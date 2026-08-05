#include "device/cuda_ipc_buffer.h"

#include <cstring>
#include <limits>
#include <utility>

#if defined(USE_CUDA)
#include <dlfcn.h>

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

CUresult GetCudaAllocationRange(CUdeviceptr *base, size_t *size,
                                CUdeviceptr ptr) {
    auto fn = LoadCuMemGetAddressRange();
    if (fn == nullptr) return CUDA_ERROR_NOT_FOUND;
    return fn(base, size, ptr);
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
    CUresult cu_ret =
        GetCudaAllocationRange(&base_ptr, &allocation_size, (CUdeviceptr)ptr);
    if (cu_ret != CUDA_SUCCESS || base_ptr == 0 || allocation_size == 0) {
        ClearCudaError();
        return UnsupportedCudaIpc();
    }

    const auto base_addr = static_cast<uintptr_t>(base_ptr);
    if (ptr_addr < base_addr) return UnsupportedCudaIpc();

    const uint64_t offset = static_cast<uint64_t>(ptr_addr - base_addr);
    const uint64_t payload_size = static_cast<uint64_t>(size);
    if (AddOverflows(offset, payload_size) ||
        offset + payload_size > allocation_size) {
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
    exported.offset = offset;
    exported.size = payload_size;
    exported.device_id = attr.device;
    return exported;
#else
    (void)ptr;
    (void)size;
    return UnsupportedCudaIpc();
#endif
}

CudaIpcBufferMapping::~CudaIpcBufferMapping() { Close(); }

CudaIpcBufferMapping::CudaIpcBufferMapping(
    CudaIpcBufferMapping &&other) noexcept
    : base_(std::exchange(other.base_, nullptr)),
      ptr_(std::exchange(other.ptr_, nullptr)),
      device_id_(std::exchange(other.device_id_, -1)) {}

CudaIpcBufferMapping &CudaIpcBufferMapping::operator=(
    CudaIpcBufferMapping &&other) noexcept {
    if (this != &other) {
        Close();
        base_ = std::exchange(other.base_, nullptr);
        ptr_ = std::exchange(other.ptr_, nullptr);
        device_id_ = std::exchange(other.device_id_, -1);
    }
    return *this;
}

tl::expected<CudaIpcBufferMapping, ErrorCode> CudaIpcBufferMapping::Open(
    const CudaIpcBufferHandle &handle) {
#if defined(USE_CUDA)
    static_assert(sizeof(cudaIpcMemHandle_t) == kCudaIpcHandleSize,
                  "Unexpected CUDA IPC handle size");

    if (handle.size == 0 || handle.device_id < 0 ||
        AddOverflows(handle.offset, handle.size)) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    int current_device = -1;
    cudaGetDevice(&current_device);
    if (cudaSetDevice(handle.device_id) != cudaSuccess) {
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
        CUresult cu_ret = GetCudaAllocationRange(
            &allocation_base, &allocation_size, (CUdeviceptr)base);
        if (cu_ret != CUDA_SUCCESS || allocation_base != (CUdeviceptr)base ||
            allocation_size == 0 || AddOverflows(handle.offset, handle.size) ||
            handle.offset + handle.size > allocation_size) {
            cudaIpcCloseMemHandle(base);
            base = nullptr;
            ret = cudaErrorInvalidValue;
        }
    }
    if (current_device >= 0) cudaSetDevice(current_device);
    if (ret != cudaSuccess || base == nullptr) {
        ClearCudaError();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto *ptr = static_cast<char *>(base) + handle.offset;
    return CudaIpcBufferMapping(base, ptr, handle.device_id);
#else
    (void)handle;
    return tl::unexpected(ErrorCode::INVALID_PARAMS);
#endif
}

void CudaIpcBufferMapping::Close() {
#if defined(USE_CUDA)
    if (base_ != nullptr) {
        int current_device = -1;
        const bool restore_device =
            cudaGetDevice(&current_device) == cudaSuccess;
        if (device_id_ >= 0 && cudaSetDevice(device_id_) != cudaSuccess) {
            ClearCudaError();
        }
        cudaIpcCloseMemHandle(base_);
        if (restore_device) cudaSetDevice(current_device);
    }
#endif
    base_ = nullptr;
    ptr_ = nullptr;
    device_id_ = -1;
}

}  // namespace device
}  // namespace mooncake
