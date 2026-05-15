#pragma once

#include "cuda_alike.h"

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include <acl/acl_rt.h>
#endif

#include <cstddef>
#include <glog/logging.h>

namespace mooncake {
namespace gpu_staging {

// Detect whether ptr resides in accelerator device memory.
// If so, writes the device ID to *out_device_id for subsequent SetDevice.
inline bool IsDevicePointer(const void* ptr, int* out_device_id = nullptr) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    cudaPointerAttributes attr{};
    if (cudaPointerGetAttributes(&attr, ptr) == cudaSuccess &&
        attr.type == cudaMemoryTypeDevice) {
        if (out_device_id) *out_device_id = attr.device;
        return true;
    }
#elif defined(USE_HIP)
    hipPointerAttribute_t attr{};
    if (hipPointerGetAttributes(&attr, ptr) == hipSuccess &&
        attr.type == hipMemoryTypeDevice) {
        if (out_device_id) *out_device_id = attr.device;
        return true;
    }
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtPtrAttributes attr{};
    if (aclrtPointerGetAttributes(const_cast<void*>(ptr), &attr) ==
            ACL_SUCCESS &&
        attr.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        if (out_device_id) *out_device_id = static_cast<int>(attr.location.id);
        return true;
    }
#endif
    (void)ptr;
    (void)out_device_id;
    return false;
}

// Copy device memory to host. Caller must have called SetDevice first.
inline bool CopyDeviceToHost(void* dst, const void* src, size_t size) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    return cudaMemcpy(dst, src, size, cudaMemcpyDeviceToHost) == cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpy(dst, src, size, hipMemcpyDeviceToHost) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtMemcpy(dst, size, src, size, ACL_MEMCPY_DEVICE_TO_HOST) ==
           ACL_SUCCESS;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}

// Auto-direction copy: runtime determines the transfer direction from pointer
// attributes (cudaMemcpyDefault). Works for H2H, H2D, D2H, and D2D.
// Caller must have called SetDevice first when device memory is involved.
inline bool CopyAuto(void* dst, const void* src, size_t size) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    return cudaMemcpy(dst, src, size, cudaMemcpyDefault) == cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpy(dst, src, size, hipMemcpyDefault) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtPtrAttributes src_attr{}, dst_attr{};
    bool src_dev = aclrtPointerGetAttributes(const_cast<void*>(src),
                                             &src_attr) == ACL_SUCCESS &&
                   src_attr.location.type == ACL_MEM_LOCATION_TYPE_DEVICE;
    bool dst_dev = aclrtPointerGetAttributes(dst, &dst_attr) == ACL_SUCCESS &&
                   dst_attr.location.type == ACL_MEM_LOCATION_TYPE_DEVICE;
    aclrtMemcpyKind kind = ACL_MEMCPY_HOST_TO_HOST;
    if (src_dev && dst_dev)
        kind = ACL_MEMCPY_DEVICE_TO_DEVICE;
    else if (src_dev)
        kind = ACL_MEMCPY_DEVICE_TO_HOST;
    else if (dst_dev)
        kind = ACL_MEMCPY_HOST_TO_DEVICE;
    return aclrtMemcpy(dst, size, src, size, kind) == ACL_SUCCESS;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}

// Bind the calling thread to the given device context.
inline bool SetDevice(int device_id) {
    if (device_id < 0) return false;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    return cudaSetDevice(device_id) == cudaSuccess;
#elif defined(USE_HIP)
    return hipSetDevice(device_id) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtSetDevice(device_id) == ACL_SUCCESS;
#else
    return false;
#endif
}

// Copy host memory to device. Caller must have called SetDevice first.
inline bool CopyHostToDevice(void* dst, const void* src, size_t size) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    return cudaMemcpy(dst, src, size, cudaMemcpyHostToDevice) == cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpy(dst, src, size, hipMemcpyHostToDevice) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtMemcpy(dst, size, src, size, ACL_MEMCPY_HOST_TO_DEVICE) ==
           ACL_SUCCESS;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}

// Detect whether ptr resides in host (CPU) memory.
// Used together with IsDevicePointer for safe pointer-type dispatching:
//   if IsDevicePointer  -> CopyHostToDevice / CopyDeviceToHost
//   else if IsHostPointer -> memcpy
//   else                  -> reject (unknown type, e.g. non-standard allocator)
//
// Pageable host memory (not tracked by CUDA runtime) is treated as host.
inline bool IsHostPointer(const void* ptr) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    cudaPointerAttributes attr{};
    if (cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        // Query failed: pageable host memory not tracked by the runtime.
        cudaGetLastError();  // clear sticky error
        return true;
    }
    return attr.type != cudaMemoryTypeDevice;
#elif defined(USE_HIP)
    hipPointerAttribute_t attr{};
    if (hipPointerGetAttributes(&attr, ptr) != hipSuccess) {
        hipGetLastError();  // clear sticky error
        return true;
    }
    return attr.type != hipMemoryTypeDevice;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtPtrAttributes attr{};
    if (aclrtPointerGetAttributes(const_cast<void*>(ptr), &attr) !=
        ACL_SUCCESS) {
        // Query failed: likely pageable host memory not tracked by the runtime.
        return true;
    }
    return attr.location.type != ACL_MEM_LOCATION_TYPE_DEVICE;
#else
    (void)ptr;
    return true;  // CPU-only build: all pointers are host
#endif
}

// --- Stream types and async copy ---

using GpuStream =
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    cudaStream_t;
#elif defined(USE_HIP)
    hipStream_t;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtStream;
#else
    void*;
#endif

inline bool CreateStream(GpuStream* stream) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    return cudaStreamCreate(stream) == cudaSuccess;
#elif defined(USE_HIP)
    return hipStreamCreate(stream) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtCreateStream(stream) == ACL_SUCCESS;
#else
    (void)stream;
    return false;
#endif
}

inline void DestroyStream(GpuStream stream) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    cudaStreamDestroy(stream);
#elif defined(USE_HIP)
    hipStreamDestroy(stream);
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtDestroyStream(stream);
#endif
}

inline bool SyncStream(GpuStream stream) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    return cudaStreamSynchronize(stream) == cudaSuccess;
#elif defined(USE_HIP)
    return hipStreamSynchronize(stream) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtSynchronizeStream(stream) == ACL_SUCCESS;
#else
    (void)stream;
    return false;
#endif
}

inline bool CopyDeviceToHostAsync(void* dst, const void* src, size_t size,
                                  GpuStream stream) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    return cudaMemcpyAsync(dst, src, size, cudaMemcpyDeviceToHost, stream) ==
           cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpyAsync(dst, src, size, hipMemcpyDeviceToHost, stream) ==
           hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtMemcpyAsync(dst, size, src, size, ACL_MEMCPY_DEVICE_TO_HOST,
                            stream) == ACL_SUCCESS;
#else
    (void)dst;
    (void)src;
    (void)size;
    (void)stream;
    return false;
#endif
}

inline bool CopyHostToDeviceAsync(void* dst, const void* src, size_t size,
                                  GpuStream stream) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    return cudaMemcpyAsync(dst, src, size, cudaMemcpyHostToDevice, stream) ==
           cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpyAsync(dst, src, size, hipMemcpyHostToDevice, stream) ==
           hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtMemcpyAsync(dst, size, src, size, ACL_MEMCPY_HOST_TO_DEVICE,
                            stream) == ACL_SUCCESS;
#else
    (void)dst;
    (void)src;
    (void)size;
    (void)stream;
    return false;
#endif
}

// Single-stream RAII wrapper.
// Same-GPU same-direction transfers share one Copy Engine, so multiple
// streams provide no bandwidth benefit. A single stream is sufficient
// to achieve CPU-GPU overlap (submit all slices, then sync once).
class ScopedGpuStream {
   public:
    explicit ScopedGpuStream(int device_id) : device_id_(device_id) {
        if (!SetDevice(device_id_)) {
            LOG(ERROR) << "Failed to set GPU device " << device_id;
            valid_ = false;
            return;
        }
        valid_ = CreateStream(&stream_);
        if (!valid_) {
            LOG(ERROR) << "Failed to create GPU stream for device "
                       << device_id;
        }
    }

    ~ScopedGpuStream() {
        if (!valid_) return;
        SetDevice(device_id_);
        if (!synced_) {
            if (!SyncStream(stream_)) {
                LOG(WARNING)
                    << "ScopedGpuStream: implicit sync failed in destructor"
                    << " for device " << device_id_
                    << ", data may be incomplete";
            }
        }
        DestroyStream(stream_);
    }

    ScopedGpuStream(const ScopedGpuStream&) = delete;
    ScopedGpuStream& operator=(const ScopedGpuStream&) = delete;
    ScopedGpuStream(ScopedGpuStream&& other) noexcept
        : device_id_(other.device_id_),
          stream_(other.stream_),
          valid_(other.valid_),
          synced_(other.synced_) {
        other.stream_ = GpuStream{};
        other.valid_ = false;
    }
    ScopedGpuStream& operator=(ScopedGpuStream&& other) noexcept {
        if (this != &other) {
            // Clean up current stream (inline destructor logic)
            if (valid_) {
                if (!SetDevice(device_id_)) {
                    LOG(ERROR) << "ScopedGpuStream: SetDevice failed in"
                               << " move-assignment for device " << device_id_
                               << ", skipping sync";
                } else if (!synced_) {
                    SyncStream(stream_);
                }
                DestroyStream(stream_);
            }
            device_id_ = other.device_id_;
            stream_ = other.stream_;
            valid_ = other.valid_;
            synced_ = other.synced_;
            other.stream_ = GpuStream{};
            other.valid_ = false;
        }
        return *this;
    }

    GpuStream get() const { return stream_; }
    bool valid() const { return valid_; }

    bool sync() {
        if (!valid_) return false;
        SetDevice(device_id_);
        bool ok = SyncStream(stream_);
        synced_ = true;
        return ok;
    }

   private:
    int device_id_;
    GpuStream stream_{};
    bool valid_ = false;
    bool synced_ = false;
};

}  // namespace gpu_staging
}  // namespace mooncake
