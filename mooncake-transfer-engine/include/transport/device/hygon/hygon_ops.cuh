// Hygon DTK implementations of device-side memory ordering primitives.
// DTK accepts CUDA source but targets a non-PTX ISA, so these operations use
// CUDA-compatible intrinsics and volatile accesses instead of inline PTX.
#pragma once

#include <cuda_runtime.h>

namespace mooncake {
namespace device {

__device__ __forceinline__ int mc_ld_acquire(const int* ptr) {
#if defined(__HIP_DEVICE_COMPILE__) || defined(__HIP__) || defined(__AMDGCN__)
    int ret;
    asm volatile(
        "global_load_dword %0, %1, off glc slc\n\t"
        "s_waitcnt vmcnt(0)"
        : "=v"(ret)
        : "v"(ptr)
        : "memory"
    );
    return ret;
#else
    __threadfence_system();
    int ret = *const_cast<volatile const int*>(ptr);
    if (ret == 0) {
        ret = atomicAdd(const_cast<int*>(ptr), 0);
    }
    __threadfence_system();
    return ret;
#endif
}

__device__ __forceinline__ uint64_t mc_ld_acquire_u64(const uint64_t* ptr) {
#if defined(__HIP_DEVICE_COMPILE__) || defined(__HIP__) || defined(__AMDGCN__)
    uint64_t ret;
    asm volatile(
        "global_load_dwordx2 %0, %1, off glc slc\n\t"
        "s_waitcnt vmcnt(0)"
        : "=v"(ret)
        : "v"(ptr)
        : "memory"
    );
    return ret;
#else
    __threadfence_system();
    uint64_t ret = *const_cast<volatile const uint64_t*>(ptr);
    if (ret == 0) {
        ret = static_cast<uint64_t>(atomicAdd(
            reinterpret_cast<unsigned long long*>(const_cast<uint64_t*>(ptr)),
            0ULL));
    }
    __threadfence_system();
    return ret;
#endif
}

__device__ __forceinline__ void mc_st_release(const int* ptr, int val) {
#if defined(__HIP_DEVICE_COMPILE__) || defined(__HIP__) || defined(__AMDGCN__)
    asm volatile(
        "global_store_dword %0, %1, off glc slc\n\t"
        "s_waitcnt vmcnt(0)"
        :
        : "v"(ptr), "v"(val)
        : "memory"
    );
#else
    __threadfence_system();
    *const_cast<volatile int*>(ptr) = val;
    __threadfence_system();
#endif
}

__device__ __forceinline__ void mc_st_release_u32(const uint32_t* ptr,
                                                  uint32_t val) {
#if defined(__HIP_DEVICE_COMPILE__) || defined(__HIP__) || defined(__AMDGCN__)
    asm volatile(
        "global_store_dword %0, %1, off glc slc\n\t"
        "s_waitcnt vmcnt(0)"
        :
        : "v"(ptr), "v"(val)
        : "memory"
    );
#else
    __threadfence_system();
    *const_cast<volatile uint32_t*>(ptr) = val;
    __threadfence_system();
#endif
}

__device__ __forceinline__ void mc_st_release_u64(const uint64_t* ptr,
                                                  uint64_t val) {
#if defined(__HIP_DEVICE_COMPILE__) || defined(__HIP__) || defined(__AMDGCN__)
    asm volatile(
        "global_store_dwordx2 %0, %1, off glc slc\n\t"
        "s_waitcnt vmcnt(0)"
        :
        : "v"(ptr), "v"(val)
        : "memory"
    );
#else
    __threadfence_system();
    *const_cast<volatile uint64_t*>(ptr) = val;
    __threadfence_system();
#endif
}

__device__ __forceinline__ int mc_atomic_add_release(const int* ptr, int val) {
    __threadfence_system();
    int ret = atomicAdd(const_cast<int*>(ptr), val);
    __threadfence_system();
    return ret;
}

__device__ __forceinline__ int4 mc_ld_nc(const int4* ptr) {
    const volatile int* words = reinterpret_cast<const volatile int*>(ptr);
    int4 ret;
    ret.x = words[0];
    ret.y = words[1];
    ret.z = words[2];
    ret.w = words[3];
    return ret;
}

__device__ __forceinline__ int mc_ld_nc_s32(const int* ptr) {
    return *const_cast<volatile const int*>(ptr);
}

__device__ __forceinline__ float mc_ld_nc_f32(const float* ptr) {
    return *const_cast<volatile const float*>(ptr);
}

__device__ __forceinline__ int64_t mc_ld_nc_s64(const int64_t* ptr) {
    return *const_cast<volatile const int64_t*>(ptr);
}

__device__ __forceinline__ void mc_st_na(const int4* ptr, const int4& val) {
    volatile int* words =
        reinterpret_cast<volatile int*>(const_cast<int4*>(ptr));
    words[0] = val.x;
    words[1] = val.y;
    words[2] = val.z;
    words[3] = val.w;
}

__device__ __forceinline__ void mc_bar_init() {}

__device__ __forceinline__ void mc_bar_sync(int /*bar_id*/,
                                            int /*num_threads*/) {
    __syncthreads();
}

__device__ __forceinline__ void mc_grid_sync() {
    // Hygon kernels use split send/receive launches on the 64-lane path, so a
    // grid-wide barrier is not needed here.
}

__device__ __forceinline__ void mc_fence() { __threadfence_system(); }

__device__ __forceinline__ void mc_fence_barrier_fence() {
    mc_fence();
    mc_bar_sync(0, 0);
    mc_fence();
}

__device__ __forceinline__ uint16_t mc_bswap16(uint16_t x) {
    return static_cast<uint16_t>(((x & 0x00ffu) << 8) |
                                 ((x & 0xff00u) >> 8));
}

__device__ __forceinline__ uint32_t mc_bswap32(uint32_t x) {
    return ((x & 0x000000ffu) << 24) | ((x & 0x0000ff00u) << 8) |
           ((x & 0x00ff0000u) >> 8) | ((x & 0xff000000u) >> 24);
}

__device__ __forceinline__ uint64_t mc_bswap64(uint64_t x) {
    uint32_t hi = mc_bswap32(static_cast<uint32_t>(x >> 32));
    uint32_t lo = mc_bswap32(static_cast<uint32_t>(x));
    return (static_cast<uint64_t>(lo) << 32) | hi;
}

}  // namespace device
}  // namespace mooncake
