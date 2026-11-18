#pragma once

#include <cstddef>
#include <cstdint>

#include "device/cuda_ipc_buffer_handle.h"

namespace mooncake {
namespace device {

tl::expected<CudaIpcBufferHandle, ErrorCode> ExportCudaIpcBuffer(
    const void *ptr, size_t size);

// Export the complete allocation backing ptr. The caller owns the returned fd.
tl::expected<int, ErrorCode> ExportCudaIpcDmabuf(
    const CudaIpcBufferHandle &handle, const void *ptr);

tl::expected<void, ErrorCode> SynchronizeCudaStream(int32_t device_id,
                                                    uintptr_t stream_handle);

class CudaIpcBufferMapping {
   public:
    CudaIpcBufferMapping() = default;
    ~CudaIpcBufferMapping();

    CudaIpcBufferMapping(const CudaIpcBufferMapping &) = delete;
    CudaIpcBufferMapping &operator=(const CudaIpcBufferMapping &) = delete;

    CudaIpcBufferMapping(CudaIpcBufferMapping &&other) noexcept;
    CudaIpcBufferMapping &operator=(CudaIpcBufferMapping &&other) noexcept;

    static tl::expected<CudaIpcBufferMapping, ErrorCode> Open(
        const CudaIpcBufferHandle &handle);

    // Leave the CUDA mapping open until process exit when an MR may still
    // reference it after its owner shuts down.
    void Abandon() noexcept;

    void *ptr() const { return ptr_; }
    void *base() const { return base_; }
    size_t allocation_size() const { return allocation_size_; }

   private:
    CudaIpcBufferMapping(void *base, void *ptr, int32_t device_id, size_t size)
        : base_(base),
          ptr_(ptr),
          device_id_(device_id),
          allocation_size_(size) {}

    void Close();

    void *base_ = nullptr;
    void *ptr_ = nullptr;
    int32_t device_id_ = -1;
    size_t allocation_size_ = 0;
};

}  // namespace device
}  // namespace mooncake
