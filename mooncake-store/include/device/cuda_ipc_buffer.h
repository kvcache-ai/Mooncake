#pragma once

#include <cstddef>
#include <cstdint>

#include "device/cuda_ipc_buffer_handle.h"

namespace mooncake {
namespace device {

tl::expected<CudaIpcBufferHandle, ErrorCode> ExportCudaIpcBuffer(
    const void *ptr, size_t size);

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

    void *ptr() const { return ptr_; }

   private:
    CudaIpcBufferMapping(void *base, void *ptr, int32_t device_id)
        : base_(base), ptr_(ptr), device_id_(device_id) {}

    void Close();

    void *base_ = nullptr;
    void *ptr_ = nullptr;
    int32_t device_id_ = -1;
};

}  // namespace device
}  // namespace mooncake
