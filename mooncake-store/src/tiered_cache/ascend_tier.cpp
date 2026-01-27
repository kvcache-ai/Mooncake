#include "tiered_cache/ascend_tier.h"

#include <glog/logging.h>

#include <cstring>
#include <functional>

#include "tiered_cache/copier_registry.h"
#include "tiered_cache/tiered_backend.h"

#ifdef USE_ASCEND_CACHE_TIER
#include "acl/acl.h"
#endif

namespace mooncake {

// ============================================================================
// ACL Function Wrappers - Device context management helpers
// ============================================================================

#ifdef USE_ASCEND_CACHE_TIER

// Helper function to set device context
inline bool AclSetDevice(int device_id, const char* operation_name) {
    auto ret = aclrtSetDevice(device_id);
    if (ret != ACL_SUCCESS) {
        LOG(ERROR) << operation_name
                   << ": aclrtSetDevice failed, device: " << device_id
                   << ", error: " << ret;
        return false;
    }
    return true;
}

// Helper function to allocate device memory with context management
inline void* AclMallocWithDevice(int device_id, size_t size,
                                 const char* operation_name) {
    if (!AclSetDevice(device_id, operation_name)) return nullptr;

    void* device_ptr = nullptr;
    auto ret = aclrtMalloc(&device_ptr, size, ACL_MEM_MALLOC_HUGE_FIRST);
    if (ret != ACL_SUCCESS || device_ptr == nullptr) {
        LOG(ERROR) << operation_name << ": aclrtMalloc failed on device "
                   << device_id << ", size: " << size << ", error: " << ret;
        return nullptr;
    }
    return device_ptr;
}

// Helper function to free device memory with context management
inline bool AclFreeWithDevice(int device_id, void* ptr,
                              const char* operation_name) {
    if (!ptr) return true;  // Nothing to free
    if (!AclSetDevice(device_id, operation_name)) return false;

    auto ret = aclrtFree(ptr);
    if (ret != ACL_SUCCESS) {
        LOG(ERROR) << operation_name << ": aclrtFree failed on device "
                   << device_id << ", error: " << ret;
        return false;
    }
    return true;
}

// Helper function to perform memory copy with context management
inline bool AclMemcpyWithDevice(int device_id, void* dst, size_t dst_size,
                                const void* src, size_t src_size,
                                aclrtMemcpyKind kind,
                                const char* operation_name) {
    if (!AclSetDevice(device_id, operation_name)) {
        return false;
    }

    auto ret = aclrtMemcpy(dst, dst_size, src, src_size, kind);
    if (ret != ACL_SUCCESS) {
        LOG(ERROR) << operation_name << ": aclrtMemcpy failed, error: " << ret;
        return false;
    }
    return true;
}

#endif  // USE_ASCEND_CACHE_TIER

// ============================================================================
// AscendBuffer Implementation
// ============================================================================

AscendBuffer::AscendBuffer(std::unique_ptr<AscendUnifiedPointer> unified_ptr)
    : unified_ptr_(std::move(unified_ptr)) {}

AscendBuffer::~AscendBuffer() { ReleaseMemory(); }

AscendBuffer::AscendBuffer(AscendBuffer&& other) noexcept
    : unified_ptr_(std::move(other.unified_ptr_)) {}

AscendBuffer& AscendBuffer::operator=(AscendBuffer&& other) noexcept {
    if (this != &other) {
        ReleaseMemory();
        unified_ptr_ = std::move(other.unified_ptr_);
    }
    return *this;
}

void AscendBuffer::ReleaseMemory() {
    if (unified_ptr_) {
#ifdef USE_ASCEND_CACHE_TIER
        AclFreeWithDevice(unified_ptr_->device_id, unified_ptr_->device_ptr,
                          "AscendBuffer::ReleaseMemory");
#else
        // Fallback: free host memory if Ascend is not available
        std::free(unified_ptr_->device_ptr);
#endif
        unified_ptr_.reset();
    }
}

uint64_t AscendBuffer::data() const {
    return unified_ptr_ ? reinterpret_cast<uint64_t>(unified_ptr_->device_ptr)
                        : 0;
}

std::size_t AscendBuffer::size() const {
    return unified_ptr_ ? unified_ptr_->size : 0;
}

int AscendBuffer::GetDeviceId() const {
    return unified_ptr_ ? unified_ptr_->device_id : -1;
}

void* AscendBuffer::GetDevicePtr() const {
    return unified_ptr_ ? unified_ptr_->device_ptr : nullptr;
}

const AscendUnifiedPointer* AscendBuffer::GetUnifiedPointer() const {
    return unified_ptr_.get();
}

// ============================================================================
// AscendCacheTier Implementation
// ============================================================================

AscendCacheTier::AscendCacheTier(UUID tier_id, size_t capacity,
                                 const std::vector<std::string>& tags,
                                 int device_id)
    : tier_id_(tier_id),
      capacity_(capacity),
      tags_(tags),
      device_id_(device_id) {}

AscendCacheTier::~AscendCacheTier() {
    // Note: All allocated buffers should be freed via RAII before tier
    // destruction If there are still outstanding allocations, they will be
    // cleaned up when their AscendBuffer destructors are called
    size_t remaining = current_usage_.load(std::memory_order_acquire);
    if (remaining > 0) {
        LOG(WARNING) << "AscendCacheTier " << tier_id_ << " destroyed with "
                     << remaining << " bytes still allocated";
    }
}

tl::expected<void, ErrorCode> AscendCacheTier::Init(TieredBackend* backend,
                                                    TransferEngine* engine) {
    std::lock_guard<std::mutex> lock(init_mutex_);

    if (is_initialized_) {
        return tl::expected<void, ErrorCode>{};
    }

    if (!backend) {
        LOG(ERROR) << "Init failed: backend is nullptr";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    backend_ = backend;

#ifdef USE_ASCEND_CACHE_TIER
    // Initialize ACL framework (only once, thread-safe via static
    // initialization)
    static bool acl_initialized = []() {
        aclError init_ret = aclInit(nullptr);
        if (init_ret != ACL_SUCCESS &&
            init_ret != ACL_ERROR_REPEAT_INITIALIZE) {
            LOG(ERROR) << "aclInit failed with error: " << init_ret;
            return false;
        }
        if (init_ret == ACL_ERROR_REPEAT_INITIALIZE) {
            LOG(INFO) << "ACL already initialized";
        } else {
            LOG(INFO) << "ACL initialized successfully";
            // Register a cleanup function to be called at program exit.
            std::atexit([]() {
                aclError ret = aclFinalize();
                if (ret != ACL_SUCCESS) {
                    LOG(ERROR) << "aclFinalize failed with error: " << ret;
                } else {
                    LOG(INFO) << "ACL finalized successfully.";
                }
            });
        }
        return true;
    }();

    if (!acl_initialized) {
        LOG(ERROR) << "ACL framework not initialized";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Validate device ID
    uint32_t device_count = 0;
    auto ret = aclrtGetDeviceCount(&device_count);
    if (ret != ACL_SUCCESS) {
        LOG(ERROR) << "Failed to get device count, error: " << ret;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    if (device_id_ < 0 || static_cast<uint32_t>(device_id_) >= device_count) {
        LOG(ERROR) << "Invalid device_id " << device_id_
                   << ", available devices: " << device_count;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Set device context
    if (!AclSetDevice(device_id_, "AscendCacheTier::Init")) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    LOG(INFO) << "AscendCacheTier initialized: tier_id=" << tier_id_
              << ", capacity=" << capacity_ << " bytes"
              << ", device_id=" << device_id_;
#else
    LOG(WARNING)
        << "USE_ASCEND_CACHE_TIER not defined, device operations limited";
    LOG(INFO) << "AscendCacheTier initialized (fallback mode): tier_id="
              << tier_id_ << ", capacity=" << capacity_ << " bytes";
#endif

    is_initialized_ = true;
    return tl::expected<void, ErrorCode>{};
}

tl::expected<void, ErrorCode> AscendCacheTier::Allocate(size_t size,
                                                        DataSource& data) {
    if (!is_initialized_) {
        LOG(ERROR) << "AscendCacheTier " << tier_id_ << " not initialized";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    if (size == 0) {
        LOG(ERROR) << "Cannot allocate 0 bytes";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Use CAS to atomically check and reserve space (thread-safe)
    size_t current = current_usage_.load(std::memory_order_acquire);
    do {
        if (current + size > capacity_) {
            LOG(ERROR) << "Insufficient space in tier " << tier_id_
                       << ": requested=" << size
                       << ", available=" << (capacity_ - current);
            return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
    } while (!current_usage_.compare_exchange_weak(current, current + size,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_acquire));

    // Space reserved, now allocate device memory
    auto unified_ptr = AllocateDeviceMemory(size);
    if (!unified_ptr) {
        // Allocation failed, rollback the reserved space
        current_usage_.fetch_sub(size, std::memory_order_release);
        LOG(ERROR) << "Failed to allocate device memory for tier " << tier_id_;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Create AscendBuffer and wrap in DataSource
    auto ascend_buffer = std::make_unique<AscendBuffer>(std::move(unified_ptr));
    data.buffer = std::move(ascend_buffer);
    data.type = MemoryType::ASCEND_NPU;

    VLOG(1) << "Allocated " << size << " bytes on device " << device_id_
            << ", total usage: "
            << current_usage_.load(std::memory_order_acquire);

    return tl::expected<void, ErrorCode>{};
}

tl::expected<void, ErrorCode> AscendCacheTier::Free(DataSource data) {
    if (!data.buffer) {
        LOG(WARNING) << "Attempting to free null buffer in tier " << tier_id_;
        return tl::expected<void, ErrorCode>{};
    }

    // Get the size before buffer is released
    size_t freed_size = data.buffer->size();

    // RAII: data.buffer destructor will be called when this function returns,
    // which will trigger AscendBuffer::ReleaseMemory() -> aclrtFree()

    // Update usage counter with proper memory ordering
    if (freed_size > 0) {
        current_usage_.fetch_sub(freed_size, std::memory_order_acq_rel);
        VLOG(1) << "Freed " << freed_size << " bytes from device " << device_id_
                << ", total usage: "
                << current_usage_.load(std::memory_order_acquire);
    }

    return tl::expected<void, ErrorCode>{};
}

size_t AscendCacheTier::GetUsage() const {
    return current_usage_.load(std::memory_order_acquire);
}

std::unique_ptr<AscendUnifiedPointer> AscendCacheTier::AllocateDeviceMemory(
    size_t size) {
    if (size == 0) {
        LOG(ERROR) << "AllocateDeviceMemory called with size 0";
        return nullptr;
    }

#ifdef USE_ASCEND_CACHE_TIER
    void* device_ptr =
        AclMallocWithDevice(device_id_, size, "AllocateDeviceMemory");
    if (!device_ptr) {
        return nullptr;
    }

    try {
        return std::make_unique<AscendUnifiedPointer>(
            AscendUnifiedPointer{device_ptr, device_id_, size});
    } catch (const std::bad_alloc& e) {
        LOG(ERROR) << "Failed to allocate AscendUnifiedPointer: " << e.what();
        AclFreeWithDevice(device_id_, device_ptr,
                          "AllocateDeviceMemory cleanup");
        return nullptr;
    }
#else
    // Fallback: allocate host memory if Ascend is not available
    LOG(WARNING) << "USE_ASCEND_CACHE_TIER not defined, using malloc fallback";
    void* host_ptr = std::malloc(size);
    if (!host_ptr) {
        LOG(ERROR) << "malloc failed for size " << size;
        return nullptr;
    }

    try {
        return std::make_unique<AscendUnifiedPointer>(
            AscendUnifiedPointer{host_ptr, -1, size});
    } catch (const std::bad_alloc& e) {
        LOG(ERROR) << "Failed to allocate AscendUnifiedPointer: " << e.what();
        std::free(host_ptr);
        return nullptr;
    }
#endif
}

bool AscendCacheTier::HasSpace(size_t size) const {
    return (current_usage_.load(std::memory_order_acquire) + size) <= capacity_;
}

// ============================================================================
// Data Copy Functions - Adapted for new CopyFunction signature
// ============================================================================

/**
 * @brief Copy data from Ascend NPU to DRAM.
 *
 * Extracts AscendUnifiedPointer from source buffer to get device context,
 * then performs device-to-host memory copy.
 */
tl::expected<void, ErrorCode> CopyAscendToDram(const DataSource& src,
                                               const DataSource& dst) {
    // Validate source buffer
    if (!src.buffer) {
        LOG(ERROR) << "CopyAscendToDram: source buffer is null";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate destination buffer
    if (!dst.buffer) {
        LOG(ERROR) << "CopyAscendToDram: destination buffer is null";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Get AscendUnifiedPointer from source using type-safe cast
    const AscendBuffer* ascend_src_buffer =
        dynamic_cast<const AscendBuffer*>(src.buffer.get());
    if (!ascend_src_buffer) {
        LOG(ERROR) << "CopyAscendToDram: source buffer is not an AscendBuffer";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const AscendUnifiedPointer* ascend_ptr =
        ascend_src_buffer->GetUnifiedPointer();

    if (!ascend_ptr || !ascend_ptr->device_ptr) {
        LOG(ERROR) << "CopyAscendToDram: invalid Ascend pointer";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Get destination DRAM address
    void* dest_ptr = reinterpret_cast<void*>(dst.buffer->data());
    size_t copy_size = src.buffer->size();

    // Validate destination buffer size
    if (dst.buffer->size() < copy_size) {
        LOG(ERROR) << "CopyAscendToDram: destination buffer too small ("
                   << dst.buffer->size() << " < " << copy_size << ")";
        return tl::unexpected(ErrorCode::BUFFER_OVERFLOW);
    }

#ifdef USE_ASCEND_CACHE_TIER
    // Perform copy with device context management
    if (!AclMemcpyWithDevice(ascend_ptr->device_id, dest_ptr,
                             dst.buffer->size(), ascend_ptr->device_ptr,
                             copy_size, ACL_MEMCPY_DEVICE_TO_HOST,
                             "CopyAscendToDram")) {
        return tl::unexpected(ErrorCode::DATA_COPY_FAILED);
    }

    VLOG(1) << "CopyAscendToDram: copied " << copy_size << " bytes from device "
            << ascend_ptr->device_id;
#else
    // Fallback: use memcpy if Ascend is not available
    std::memcpy(dest_ptr, ascend_ptr->device_ptr, copy_size);
#endif
    return tl::expected<void, ErrorCode>{};
}

/**
 * @brief Copy data from DRAM to Ascend NPU.
 *
 * Extracts AscendUnifiedPointer from destination buffer to get device context,
 * then performs host-to-device memory copy.
 */
tl::expected<void, ErrorCode> CopyDramToAscend(const DataSource& src,
                                               const DataSource& dst) {
    // Validate source buffer (DRAM)
    if (!src.buffer) {
        LOG(ERROR) << "CopyDramToAscend: source buffer is null";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate destination buffer (Ascend)
    if (!dst.buffer) {
        LOG(ERROR) << "CopyDramToAscend: destination buffer is null";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Get source DRAM address
    const void* src_ptr = reinterpret_cast<const void*>(src.buffer->data());
    size_t copy_size = src.buffer->size();

    // Get AscendUnifiedPointer from destination using type-safe cast
    const AscendBuffer* ascend_dst_buffer =
        dynamic_cast<const AscendBuffer*>(dst.buffer.get());
    if (!ascend_dst_buffer) {
        LOG(ERROR)
            << "CopyDramToAscend: destination buffer is not an AscendBuffer";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const AscendUnifiedPointer* ascend_ptr =
        ascend_dst_buffer->GetUnifiedPointer();

    if (!ascend_ptr || !ascend_ptr->device_ptr) {
        LOG(ERROR) << "CopyDramToAscend: invalid Ascend pointer";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate destination buffer size
    if (ascend_ptr->size < copy_size) {
        LOG(ERROR) << "CopyDramToAscend: destination buffer too small ("
                   << ascend_ptr->size << " < " << copy_size << ")";
        return tl::unexpected(ErrorCode::BUFFER_OVERFLOW);
    }

#ifdef USE_ASCEND_CACHE_TIER
    // Perform copy with device context management
    if (!AclMemcpyWithDevice(ascend_ptr->device_id, ascend_ptr->device_ptr,
                             ascend_ptr->size, src_ptr, copy_size,
                             ACL_MEMCPY_HOST_TO_DEVICE, "CopyDramToAscend")) {
        return tl::unexpected(ErrorCode::DATA_COPY_FAILED);
    }

    VLOG(1) << "CopyDramToAscend: copied " << copy_size << " bytes to device "
            << ascend_ptr->device_id;
#else
    // Fallback: use memcpy if Ascend is not available
    std::memcpy(ascend_ptr->device_ptr, src_ptr, copy_size);
#endif
    return tl::expected<void, ErrorCode>{};
}

// ============================================================================
// Static Registration of Copy Functions
// ============================================================================

// Register ASCEND_NPU <-> DRAM copy functions with the CopierRegistry
static CopierRegistrar ascend_copier_registrar(MemoryType::ASCEND_NPU,
                                               CopyAscendToDram,
                                               CopyDramToAscend);

}  // namespace mooncake
