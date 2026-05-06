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
inline bool IsDevicePointer(const void* ptr, int* out_device_id) {
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
inline void SetDevice(int device_id) {
    if (device_id < 0) return;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    cudaSetDevice(device_id);
#elif defined(USE_HIP)
    hipSetDevice(device_id);
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtSetDevice(device_id);
#endif
}

}  // namespace gpu_staging
}  // namespace mooncake
