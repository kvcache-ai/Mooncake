#include "device/cuda_ipc_buffer.h"

#include <cstring>
#include <limits>
#include <utility>

#if defined(USE_CUDA)
#include <dlfcn.h>
#include <unistd.h>

#include "cuda_alike.h"
#endif

namespace mooncake {
namespace device {
namespace {

tl::expected<CudaIpcBufferHandle, ErrorCode> UnsupportedCudaIpc() {
    return tl::unexpected(ErrorCode::INVALID_PARAMS);
}

#if defined(USE_CUDA)
bool AddOverflows(uint64_t a, uint64_t b) {
    return a > std::numeric_limits<uint64_t>::max() - b;
}

void ClearCudaError() { cudaGetLastError(); }

tl::expected<int32_t, ErrorCode> ResolveCudaDevice(
    const CudaIpcBufferHandle &handle) {
    if (!handle.has_device_uuid) {
        if (handle.device_id < 0) {
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        return handle.device_id;
    }

    if (cuInit(0) != CUDA_SUCCESS) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    int device_count = 0;
    if (cuDeviceGetCount(&device_count) != CUDA_SUCCESS) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    for (int device_id = 0; device_id < device_count; ++device_id) {
        CUdevice device = 0;
        CUuuid uuid{};
        if (cuDeviceGet(&device, device_id) != CUDA_SUCCESS ||
            cuDeviceGetUuid(&uuid, device) != CUDA_SUCCESS) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        static_assert(sizeof(uuid.bytes) == kCudaDeviceUuidSize,
                      "Unexpected CUDA UUID size");
        if (std::memcmp(uuid.bytes, handle.device_uuid.data(),
                        kCudaDeviceUuidSize) == 0) {
            return device_id;
        }
    }
    return tl::unexpected(ErrorCode::INVALID_PARAMS);
}

using CuMemGetAddressRangeFn = CUresult (*)(CUdeviceptr *, size_t *,
                                            CUdeviceptr);

CuMemGetAddressRangeFn LoadCuMemGetAddressRange() {
    static CuMemGetAddressRangeFn fn = [] {
        void *handle = dlopen("libcuda.so.1", RTLD_LAZY | RTLD_LOCAL);
        if (handle == nullptr) {
            handle = dlopen("libcuda.so", RTLD_LAZY | RTLD_LOCAL);
        }
        void *symbol = handle != nullptr
                           ? dlsym(handle, "cuMemGetAddressRange_v2")
                           : dlsym(RTLD_DEFAULT, "cuMemGetAddressRange_v2");
        if (symbol == nullptr) {
            symbol = handle != nullptr
                         ? dlsym(handle, "cuMemGetAddressRange")
                         : dlsym(RTLD_DEFAULT, "cuMemGetAddressRange");
        }
        return reinterpret_cast<CuMemGetAddressRangeFn>(symbol);
    }();
    return fn;
}

// Validate a possibly suballocated view before exporting or importing its base.
bool GetCudaAllocationRange(const void *ptr, size_t length, CUdeviceptr &base,
                            size_t &capacity) {
    const auto addr = reinterpret_cast<CUdeviceptr>(ptr);
    auto fn = LoadCuMemGetAddressRange();
    return fn && fn(&base, &capacity, addr) == CUDA_SUCCESS && base != 0 &&
           addr >= base && length <= capacity &&
           addr - base <= capacity - length;
}

tl::expected<int, ErrorCode> ExportAllocationDmabuf(
    const CudaIpcBufferHandle &handle, const void *ptr) {
    CUdeviceptr base = 0;
    size_t capacity = 0;
    if (!GetCudaAllocationRange(ptr, handle.size, base, capacity) ||
        reinterpret_cast<CUdeviceptr>(ptr) - base != handle.offset) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    if (AddOverflows(capacity, page_size - 1))
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    const size_t length = (capacity + page_size - 1) / page_size * page_size;
    int fd = -1;
    if (cuMemGetHandleForAddressRange(&fd, base, length,
                                      CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
                                      0) != CUDA_SUCCESS) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return fd;
}
#endif

}  // namespace

tl::expected<CudaIpcBufferHandle, ErrorCode> ExportCudaIpcBuffer(
    const void *ptr, size_t size) {
#if defined(USE_CUDA)
    static_assert(sizeof(cudaIpcMemHandle_t) == kCudaIpcHandleSize,
                  "Unexpected CUDA IPC handle size");

    if (ptr == nullptr || size == 0) return UnsupportedCudaIpc();

    cudaPointerAttributes attr{};
    if (cudaPointerGetAttributes(&attr, ptr) != cudaSuccess ||
        attr.type != cudaMemoryTypeDevice || attr.devicePointer == nullptr) {
        ClearCudaError();
        return UnsupportedCudaIpc();
    }

    // PyTorch may hand out suballocated pointers; CUDA IPC must export the
    // allocation base and carry the caller pointer offset separately.
    const auto ptr_addr = reinterpret_cast<uintptr_t>(ptr);
    CUdeviceptr base_ptr = 0;
    size_t allocation_size = 0;
    if (!GetCudaAllocationRange(ptr, size, base_ptr, allocation_size)) {
        ClearCudaError();
        return UnsupportedCudaIpc();
    }

    int current_device = -1;
    cudaGetDevice(&current_device);
    if (cudaSetDevice(attr.device) != cudaSuccess) {
        ClearCudaError();
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    cudaIpcMemHandle_t ipc_handle{};
    cudaError_t ret = cudaIpcGetMemHandle(&ipc_handle, (void *)base_ptr);
    if (current_device >= 0) cudaSetDevice(current_device);
    if (ret != cudaSuccess) {
        ClearCudaError();
        return UnsupportedCudaIpc();
    }

    CudaIpcBufferHandle exported;
    std::memcpy(exported.handle.data(), &ipc_handle, sizeof(ipc_handle));
    exported.offset = ptr_addr - base_ptr;
    exported.size = size;
    exported.device_id = attr.device;
    CUdevice device = 0;
    CUuuid uuid{};
    if (cuDeviceGet(&device, attr.device) != CUDA_SUCCESS ||
        cuDeviceGetUuid(&uuid, device) != CUDA_SUCCESS) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    static_assert(sizeof(uuid.bytes) == kCudaDeviceUuidSize,
                  "Unexpected CUDA UUID size");
    std::memcpy(exported.device_uuid.data(), uuid.bytes, kCudaDeviceUuidSize);
    exported.has_device_uuid = true;
    return exported;
#else
    (void)ptr;
    (void)size;
    return UnsupportedCudaIpc();
#endif
}

tl::expected<int, ErrorCode> ExportCudaIpcDmabuf(
    const CudaIpcBufferHandle &handle, const void *ptr) {
#if defined(USE_CUDA)
    if (ptr == nullptr || handle.size == 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto device = ResolveCudaDevice(handle);
    if (!device) return tl::unexpected(device.error());

    int current_device = -1;
    if (cudaGetDevice(&current_device) != cudaSuccess ||
        cudaSetDevice(*device) != cudaSuccess) {
        ClearCudaError();
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto exported = ExportAllocationDmabuf(handle, ptr);
    if (cudaSetDevice(current_device) != cudaSuccess) {
        ClearCudaError();
        if (exported) close(*exported);
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return exported;
#else
    (void)handle;
    (void)ptr;
    return tl::unexpected(ErrorCode::INVALID_PARAMS);
#endif
}

tl::expected<void, ErrorCode> SynchronizeCudaStream(int32_t device_id,
                                                    uintptr_t stream_handle) {
#if defined(USE_CUDA)
    int current_device = -1;
    if (cudaGetDevice(&current_device) != cudaSuccess) {
        ClearCudaError();
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    cudaError_t operation_status = cudaSetDevice(device_id);
    if (operation_status == cudaSuccess) {
        const cudaStream_t stream =
            stream_handle == 0 ? nullptr
                               : reinterpret_cast<cudaStream_t>(stream_handle);
        operation_status = cudaStreamSynchronize(stream);
    }
    if (operation_status != cudaSuccess) ClearCudaError();

    const cudaError_t restore_status = cudaSetDevice(current_device);
    if (restore_status != cudaSuccess) ClearCudaError();
    if (operation_status != cudaSuccess || restore_status != cudaSuccess) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return {};
#else
    (void)device_id;
    (void)stream_handle;
    return tl::unexpected(ErrorCode::INVALID_PARAMS);
#endif
}

CudaIpcBufferMapping::~CudaIpcBufferMapping() { Close(); }

CudaIpcBufferMapping::CudaIpcBufferMapping(
    CudaIpcBufferMapping &&other) noexcept
    : base_(std::exchange(other.base_, nullptr)),
      ptr_(std::exchange(other.ptr_, nullptr)),
      device_id_(std::exchange(other.device_id_, -1)),
      allocation_size_(std::exchange(other.allocation_size_, 0)) {}

CudaIpcBufferMapping &CudaIpcBufferMapping::operator=(
    CudaIpcBufferMapping &&other) noexcept {
    if (this != &other) {
        Close();
        std::swap(base_, other.base_);
        std::swap(ptr_, other.ptr_);
        std::swap(device_id_, other.device_id_);
        std::swap(allocation_size_, other.allocation_size_);
    }
    return *this;
}

tl::expected<CudaIpcBufferMapping, ErrorCode> CudaIpcBufferMapping::Open(
    const CudaIpcBufferHandle &handle) {
    CudaIpcBufferMapping mapping;
    auto result = mapping.OpenInto(handle);
    if (!result) return tl::unexpected(result.error());
    return mapping;
}

tl::expected<void, ErrorCode> CudaIpcBufferMapping::OpenInto(
    const CudaIpcBufferHandle &handle) {
#if defined(USE_CUDA)
    static_assert(sizeof(cudaIpcMemHandle_t) == kCudaIpcHandleSize,
                  "Unexpected CUDA IPC handle size");

    if (base_ || handle.size == 0 || AddOverflows(handle.offset, handle.size)) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto resolved_device = ResolveCudaDevice(handle);
    if (!resolved_device.has_value()) {
        return tl::unexpected(resolved_device.error());
    }

    int current_device = -1;
    cudaGetDevice(&current_device);
    if (cudaSetDevice(*resolved_device) != cudaSuccess) {
        ClearCudaError();
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    cudaIpcMemHandle_t ipc_handle{};
    std::memcpy(&ipc_handle, handle.handle.data(), sizeof(ipc_handle));

    void *base = nullptr;
    cudaError_t ret =
        cudaIpcOpenMemHandle(&base, ipc_handle, cudaIpcMemLazyEnablePeerAccess);
    CUdeviceptr allocation_base = 0;
    size_t allocation_size = 0;
    if (ret == cudaSuccess && base != nullptr) {
        base_ = base;
        device_id_ = *resolved_device;
        if (!GetCudaAllocationRange(base, handle.offset + handle.size,
                                    allocation_base, allocation_size) ||
            allocation_base != reinterpret_cast<CUdeviceptr>(base)) {
            ret = cudaErrorInvalidValue;
        }
    }
    if (current_device >= 0) cudaSetDevice(current_device);
    if (ret != cudaSuccess || base == nullptr) {
        ClearCudaError();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    ptr_ = static_cast<char *>(base) + handle.offset;
    allocation_size_ = allocation_size;
    return {};
#else
    (void)handle;
    return tl::unexpected(ErrorCode::INVALID_PARAMS);
#endif
}

tl::expected<void, ErrorCode> CudaIpcBufferMapping::Close() {
#if defined(USE_CUDA)
    if (base_ != nullptr) {
        int current_device = -1;
        const bool restore_device =
            cudaGetDevice(&current_device) == cudaSuccess;
        if (device_id_ >= 0 && cudaSetDevice(device_id_) != cudaSuccess) {
            ClearCudaError();
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        const auto result = cudaIpcCloseMemHandle(base_);
        if (restore_device) cudaSetDevice(current_device);
        if (result != cudaSuccess) {
            ClearCudaError();
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }
#endif
    Abandon();
    return {};
}

void CudaIpcBufferMapping::Abandon() noexcept {
    base_ = nullptr;
    ptr_ = nullptr;
    device_id_ = -1;
    allocation_size_ = 0;
}

}  // namespace device
}  // namespace mooncake
