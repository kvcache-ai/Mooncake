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

    // PCI bus ID survives CUDA_VISIBLE_DEVICES remapping across processes
    // (#4980). Prefer this over attr.device when the peer opens the handle.
    char pci_bus_id[32] = {};
    const cudaError_t pci_ret =
        cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), attr.device);

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
    if (pci_ret == cudaSuccess && pci_bus_id[0] != '\0') {
        exported.pci_bus_id = pci_bus_id;
    } else {
        ClearCudaError();
    }
    return exported;
#else
    (void)ptr;
    (void)size;
    return UnsupportedCudaIpc();
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

    // Never cudaSetDevice(handle.device_id): that ordinal is relative to the
    // peer's CUDA_VISIBLE_DEVICES and may map to a different physical GPU
    // (or an invalid index) in this process (#4980). Prefer pci_bus_id.
    if (handle.size == 0 || AddOverflows(handle.offset, handle.size)) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    int previous_device = -1;
    if (cudaGetDevice(&previous_device) != cudaSuccess || previous_device < 0) {
        ClearCudaError();
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    int open_device = previous_device;
    bool switched_device = false;
    if (!handle.pci_bus_id.empty()) {
        if (cudaDeviceGetByPCIBusId(&open_device, handle.pci_bus_id.c_str()) !=
            cudaSuccess) {
            ClearCudaError();
            // Exporter GPU is not visible under this process's
            // CUDA_VISIBLE_DEVICES.
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (open_device != previous_device) {
            if (cudaSetDevice(open_device) != cudaSuccess) {
                ClearCudaError();
                return tl::unexpected(ErrorCode::INTERNAL_ERROR);
            }
            switched_device = true;
        }
    }
    // Legacy handles without pci_bus_id: open on the caller's current device
    // (same as TENT NVLinkTransport). Do not trust handle.device_id.

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
    if (switched_device) {
        if (cudaSetDevice(previous_device) != cudaSuccess) ClearCudaError();
    }
    if (ret != cudaSuccess || base == nullptr) {
        ClearCudaError();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto *ptr = static_cast<char *>(base) + handle.offset;
    return CudaIpcBufferMapping(base, ptr, open_device);
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
