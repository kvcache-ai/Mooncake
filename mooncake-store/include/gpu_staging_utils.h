#pragma once

#include "cuda_alike.h"

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include <acl/acl_rt.h>
#endif

#if defined(USE_SUNRISE)
#include <tang_runtime_api.h>
#include "sunrise_allocator.h"

#include <vector>

struct SavedTangDevice {
    int dev{-1};
    SavedTangDevice() { tangGetDevice(&dev); }
    ~SavedTangDevice() {
        if (dev >= 0) tangSetDevice(dev);
    }
    SavedTangDevice(const SavedTangDevice&) = delete;
    SavedTangDevice& operator=(const SavedTangDevice&) = delete;
};
#endif

#include <cstddef>
#include <glog/logging.h>

namespace mooncake {
namespace gpu_staging {

// Detect whether ptr resides in accelerator device memory.
// If so, writes the device ID to *out_device_id for subsequent SetDevice.
inline bool IsDevicePointer(const void* ptr, int* out_device_id) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
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
#elif defined(USE_SUNRISE)
    if (sunrise_is_device_memory_range(const_cast<void*>(ptr))) {
        if (out_device_id) *out_device_id = 0;
        return true;
    }
#endif
    (void)ptr;
    (void)out_device_id;
    return false;
}

// Copy device memory to host. Caller must have called SetDevice first.
inline bool CopyDeviceToHost(void* dst, const void* src, size_t size) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    return cudaMemcpy(dst, src, size, cudaMemcpyDeviceToHost) == cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpy(dst, src, size, hipMemcpyDeviceToHost) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtMemcpy(dst, size, src, size, ACL_MEMCPY_DEVICE_TO_HOST) ==
           ACL_SUCCESS;
#elif defined(USE_SUNRISE)
    return tangMemcpy(dst, src, size, tangMemcpyDeviceToHost) == tangSuccess;
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
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
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
#elif defined(USE_SUNRISE)
    bool src_dev = sunrise_is_device_memory_range(const_cast<void*>(src));
    bool dst_dev = sunrise_is_device_memory_range(dst);
    bool src_host_alloc = sunrise_is_host_allocated(const_cast<void*>(src));
    bool dst_host_alloc = sunrise_is_host_allocated(dst);

    if (!src_dev && !dst_dev && !src_host_alloc && !dst_host_alloc) {
        memcpy(dst, src, size);
        return true;
    }

    if (src_dev && dst_host_alloc && !dst_dev) {
        SavedTangDevice saved;
        tangSetDevice(0);
        std::vector<char> staging(size);
        if (tangMemcpy(staging.data(), src, size, tangMemcpyDeviceToHost) !=
            tangSuccess)
            return false;
        tangDeviceSynchronize();
        memcpy(dst, staging.data(), size);
        return true;
    }

    if (src_host_alloc && !src_dev && dst_dev) {
        SavedTangDevice saved;
        tangSetDevice(0);
        std::vector<char> staging(size);
        memcpy(staging.data(), src, size);
        return tangMemcpy(dst, staging.data(), size, tangMemcpyHostToDevice) ==
               tangSuccess;
    }

    if ((src_host_alloc || dst_host_alloc) && !src_dev && !dst_dev) {
        memcpy(dst, src, size);
        return true;
    }

    enum tangMemcpyKind kind = tangMemcpyHostToHost;
    if ((src_dev || src_host_alloc) && (dst_dev || dst_host_alloc))
        kind = tangMemcpyDeviceToDevice;
    else if (src_dev || src_host_alloc)
        kind = tangMemcpyDeviceToHost;
    else if (dst_dev || dst_host_alloc)
        kind = tangMemcpyHostToDevice;

    if (kind == tangMemcpyHostToHost) {
        memcpy(dst, src, size);
        return true;
    }

    SavedTangDevice saved;
    tangSetDevice(0);
    if (tangMemcpy(dst, src, size, kind) != tangSuccess) return false;
    if (dst_host_alloc || kind == tangMemcpyDeviceToHost)
        tangDeviceSynchronize();
    return true;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}

// Bind the calling thread to the given device context.
inline void SetDevice(int device_id) {
    if (device_id < 0) return;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    cudaSetDevice(device_id);
#elif defined(USE_HIP)
    hipSetDevice(device_id);
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtSetDevice(device_id);
#elif defined(USE_SUNRISE)
    tangSetDevice(device_id);
#endif
}

// Copy host memory to device. Caller must have called SetDevice first.
inline bool CopyHostToDevice(void* dst, const void* src, size_t size) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    return cudaMemcpy(dst, src, size, cudaMemcpyHostToDevice) == cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpy(dst, src, size, hipMemcpyHostToDevice) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtMemcpy(dst, size, src, size, ACL_MEMCPY_HOST_TO_DEVICE) ==
           ACL_SUCCESS;
#elif defined(USE_SUNRISE)
    return tangMemcpy(dst, src, size, tangMemcpyHostToDevice) == tangSuccess;
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
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
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
        return true;
    }
    return attr.location.type != ACL_MEM_LOCATION_TYPE_DEVICE;
#elif defined(USE_SUNRISE)
    if (sunrise_is_device_memory_range(const_cast<void*>(ptr))) {
        return false;
    }
    return true;
#else
    (void)ptr;
    return true;  // CPU-only build: all pointers are host
#endif
}
}  // namespace gpu_staging
}  // namespace mooncake
