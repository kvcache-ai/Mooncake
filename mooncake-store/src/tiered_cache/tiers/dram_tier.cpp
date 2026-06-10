#include <glog/logging.h>
#include <chrono>
#include <cstdlib>
#include <thread>

#ifdef USE_ASCEND_DRAM_TIER
#include "acl/acl.h"
#else
#include <numa.h>
#endif

#include "tiered_cache/tiers/dram_tier.h"
#include "utils.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/copier_registry.h"
#include "transfer_engine.h"

namespace mooncake {

DramCacheTier::DramCacheTier(UUID tier_id, size_t capacity,
                             const std::vector<std::string>& tags,
                             std::optional<int> numa_node,
                             BufferAllocatorType allocator_type)
    : tier_id_(tier_id),
      capacity_(capacity),
      tags_(tags),
      numa_node_(numa_node),
      allocator_type_(allocator_type),
      allocator_(nullptr),
      engine_(nullptr) {}

DramCacheTier::~DramCacheTier() {
    // Wait for all allocated buffers to be released before destroying allocator
    if (allocator_) {
        size_t allocated_size = allocator_->size();
        if (allocated_size > 0) {
            LOG(WARNING) << "DramCacheTier " << tier_id_
                         << " is being destroyed with " << allocated_size
                         << " bytes still allocated. Waiting for buffers to be "
                            "released...";

            // Wait for buffers to be released
            constexpr int kCheckIntervalMs = 100;
            int i = 0;
            while (true) {
                allocated_size = allocator_->size();
                if (allocated_size == 0) {
                    LOG(INFO) << "All buffers released for DramCacheTier "
                              << tier_id_;
                    break;
                }

                if (i == 10) {  // Log every second
                    LOG(INFO) << "DramCacheTier " << tier_id_ << " waiting for "
                              << allocated_size << " bytes to be released...";
                    i = 0;
                }

                std::this_thread::sleep_for(
                    std::chrono::milliseconds(kCheckIntervalMs));
                ++i;
            }
        }
    }

    allocator_.reset();

    if (engine_ != nullptr && memory_buffer_ != nullptr) {
        LOG(INFO) << "unregistering memory for DramCacheTier " << tier_id_;
        int rc = engine_->unregisterLocalMemory(memory_buffer_.get());
        if (rc != 0) {
            LOG(ERROR) << "Failed to unregister memory for DramCacheTier "
                       << tier_id_ << ", engine ret is " << rc;
        }
    }
}

tl::expected<void, ErrorCode> DramCacheTier::Init(TieredBackend* backend,
                                                   TransferEngine* engine) {
    backend_ = backend;
    if (engine != nullptr) engine_ = engine;

#ifdef USE_ASCEND_DRAM_TIER
    int current_device = -1;
    aclError acl_ret = aclrtGetDevice(&current_device);
    if (acl_ret != ACL_SUCCESS) {
        LOG(ERROR) << "USE_ASCEND_DRAM_TIER enabled but no ACL Device Context "
                   << "on current thread (aclrtGetDevice returned " << acl_ret
                   << "). Ensure aclInit and aclrtSetDevice/aclrtCreateContext "
                   << "have been called before DramCacheTier::Init().";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    LOG(INFO) << "ACL Device Context found (device " << current_device
              << "), using aclrtMallocHost for DRAM tier " << tier_id_;

    void* host_ptr = nullptr;
    acl_ret = aclrtMallocHost(&host_ptr, capacity_);
    if (acl_ret != ACL_SUCCESS || host_ptr == nullptr) {
        LOG(ERROR) << "aclrtMallocHost failed for " << capacity_
                   << " bytes, ACL error: " << acl_ret;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    memory_buffer_ = std::unique_ptr<char[], std::function<void(char*)>>(
        static_cast<char*>(host_ptr), [](char* p) {
            aclrtFreeHost(static_cast<void*>(p));
        });
    LOG(INFO) << "Allocated " << capacity_ << " bytes (ACL Host Pinned) "
              << "for DramCacheTier " << tier_id_;
#else
    int node = -1;

    const bool use_hugepage = (std::getenv("MC_STORE_USE_HUGEPAGE") != nullptr);
    if (use_hugepage) {
        const size_t hugepage_size = get_hugepage_size_from_env();
        const size_t mapped_size = align_up(capacity_, hugepage_size);

        struct bitmask* saved_nodemask = nullptr;
        if (numa_node_.has_value()) {
            if (numa_available() < 0) {
                LOG(ERROR) << "NUMA not available on this system.";
                return tl::unexpected(ErrorCode::INTERNAL_ERROR);
            }
            node = numa_node_.value();
            if (node < 0 || node > numa_max_node()) {
                LOG(ERROR) << "Invalid NUMA node " << node;
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            saved_nodemask = numa_get_membind();
            struct bitmask* nodemask = numa_allocate_nodemask();
            numa_bitmask_setbit(nodemask, node);
            numa_set_membind(nodemask);
            numa_free_nodemask(nodemask);
        }

        char* hp_ptr = static_cast<char*>(
            allocate_buffer_mmap_memory(mapped_size, hugepage_size));

        if (saved_nodemask != nullptr) {
            numa_set_membind(saved_nodemask);
            numa_free_nodemask(saved_nodemask);
        }

        if (!hp_ptr) {
            LOG(ERROR) << "Failed to allocate hugepage memory (" << mapped_size
                       << " bytes) for DramCacheTier " << tier_id_;
            return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        memory_buffer_ = std::unique_ptr<char[], std::function<void(char*)>>(
            hp_ptr, [mapped_size](char* p) {
                free_buffer_mmap_memory(p, mapped_size);
            });
        LOG(INFO) << "Allocated " << mapped_size << " bytes (hugepage"
                  << (node != -1 ? ", NUMA node " + std::to_string(node) : "")
                  << ") for DramCacheTier " << tier_id_;
    } else if (numa_node_.has_value()) {
        if (numa_available() < 0) {
            LOG(ERROR) << "NUMA not available on this system.";
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        node = numa_node_.value();
        if (node < 0 || node > numa_max_node()) {
            LOG(ERROR) << "Invalid NUMA node " << node;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        char* mem_ptr = static_cast<char*>(numa_alloc_onnode(capacity_, node));
        if (!mem_ptr) {
            LOG(ERROR) << "Failed to allocate " << capacity_
                       << " bytes from NUMA node " << node
                       << " for DramCacheTier " << tier_id_;
            return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        memory_buffer_ = std::unique_ptr<char[], std::function<void(char*)>>(
            mem_ptr, [](char* p) { numa_free(p, 0); });
        LOG(INFO) << "Allocated " << capacity_ << " bytes from NUMA node "
                  << node << " for DramCacheTier " << tier_id_;
    } else {
        try {
            memory_buffer_ =
                std::unique_ptr<char[], std::function<void(char*)>>(
                    new char[capacity_], [](char* p) { delete[] p; });
        } catch (const std::bad_alloc& e) {
            LOG(ERROR) << "Failed to allocate " << capacity_
                       << " bytes for DramCacheTier " << tier_id_ << ": "
                       << e.what();
            return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        LOG(INFO) << "Allocated " << capacity_ << " bytes for DramCacheTier "
                  << tier_id_;
    }
#endif
    char* mem_ptr = memory_buffer_.get();

#ifdef USE_ASCEND_DRAM_TIER
    std::string location = kWildcardLocation;
#else
    std::string location;
    if (node != -1) {
        location = "cpu:" + std::to_string(node);
    } else {
        location = kWildcardLocation;
    }
#endif

    if (engine_) {
        int rc = engine_->registerLocalMemory(mem_ptr, capacity_, location);
        if (rc != 0) {
            LOG(ERROR) << "Failed to register memory with TransferEngine for "
                          "DramCacheTier "
                       << tier_id_ << ", engine ret is " << rc;
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        } else {
            LOG(INFO)
                << "registered memory with TransferEngine for DramCacheTier "
                << tier_id_ << " at " << static_cast<void*>(mem_ptr);
        }
    }

    // Use the address of this registered block as the base_address for the
    // allocator.
    const uintptr_t base_address = reinterpret_cast<uintptr_t>(mem_ptr);
    std::string segment_name = "dram_tier_" + std::to_string(tier_id_.first) +
                               "-" + std::to_string(tier_id_.second);

    switch (allocator_type_) {
        case BufferAllocatorType::OFFSET:
            allocator_ = std::make_shared<OffsetBufferAllocator>(
                segment_name, base_address, capacity_, segment_name, tier_id_);
            break;
        case BufferAllocatorType::CACHELIB:
            allocator_ = std::make_shared<CachelibBufferAllocator>(
                segment_name, base_address, capacity_, segment_name, tier_id_);
            break;
        default:
            LOG(ERROR) << "Unsupported allocator type for DramCacheTier";
            if (engine_) {
                engine_->unregisterLocalMemory(mem_ptr);
            }
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    LOG(INFO) << "DramCacheTier " << tier_id_ << " initialized and registered "
              << capacity_ << " bytes at base address 0x" << std::hex
              << base_address;
    return tl::expected<void, ErrorCode>{};
}

size_t DramCacheTier::GetUsage() const {
    return allocator_ ? allocator_->size() : 0;
}

tl::expected<void, ErrorCode> DramCacheTier::Allocate(size_t size,
                                                      DataSource& data_source) {
    if (!allocator_) {
        LOG(ERROR) << "Allocator not initialized for DramCacheTier "
                   << tier_id_;
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto alloc_result = allocator_->allocate(size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate " << size
                   << " bytes from DramCacheTier " << tier_id_;
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    auto dram_buffer_wrapper =
        std::make_unique<DRAMBuffer>(std::move(alloc_result));
    data_source.buffer = std::move(dram_buffer_wrapper);
    data_source.type = MemoryType::DRAM;

    return tl::expected<void, ErrorCode>{};
}

tl::expected<void, ErrorCode> DramCacheTier::Free(DataSource data_source) {
    if (!data_source.buffer) {
        LOG(WARNING) << "Attempting to free null buffer in DramCacheTier "
                     << tier_id_;
    }
    // RAII will handle the deallocation when buffer_handle goes out of scope.
    return tl::expected<void, ErrorCode>{};
}

}  // namespace mooncake