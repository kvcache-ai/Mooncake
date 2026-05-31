#pragma once

#include <mooncake_ep_exception.cuh>

namespace mooncake {

__device__ __forceinline__ void trap() { __trap(); }
__device__ __forceinline__ void memory_fence() { __threadfence_system(); }
__device__ __forceinline__ void memory_fence_gpu() { __threadfence(); }
__device__ __forceinline__ void memory_fence_cta() { __syncthreads(); }
__device__ __forceinline__ void st_relaxed_sys_global(const int *ptr, int val) {
    *const_cast<int *>(ptr) = val;
}
__device__ __forceinline__ void st_release_sys_global(const int *ptr, int val) {
    *const_cast<int *>(ptr) = val;
    __threadfence_system();
}
__device__ __forceinline__ void st_release_cta(const int *ptr, int val) {
    *const_cast<int *>(ptr) = val;
    __threadfence();
}
__device__ __forceinline__ int ld_acquire_sys_global(const int *ptr) {
    __threadfence_system();
    return *const_cast<volatile int *>(ptr);
}
__device__ __forceinline__ uint64_t ld_acquire_sys_global(const uint64_t *ptr) {
    __threadfence_system();
    return *const_cast<volatile uint64_t *>(ptr);
}
__device__ __forceinline__ int ld_acquire_global(const int *ptr) {
    __threadfence();
    return *const_cast<volatile int *>(ptr);
}
__device__ __forceinline__ int atomic_add_release_sys_global(const int *ptr,
                                                             int value) {
    int r = atomicAdd(const_cast<int *>(ptr), value);
    __threadfence_system();
    return r;
}
__device__ __forceinline__ int atomic_add_release_global(const int *ptr,
                                                         int value) {
    int r = atomicAdd(const_cast<int *>(ptr), value);
    __threadfence_system();
    return r;
}
__device__ __forceinline__ int ld_acquire_cta(const int *ptr) {
    return *const_cast<volatile int *>(ptr);
}
__device__ __forceinline__ int ld_volatile_global(const int *ptr) {
    return *const_cast<volatile int *>(ptr);
}
__device__ __forceinline__ float ld_volatile_global(const float *ptr) {
    return *const_cast<volatile float *>(ptr);
}
__device__ __forceinline__ int64_t ld_volatile_global(const int64_t *ptr) {
    return *const_cast<volatile int64_t *>(ptr);
}
__device__ __forceinline__ int64_t ld_volatile_global(const uint64_t *ptr) {
    return *reinterpret_cast<volatile const int64_t *>(ptr);
}
template <typename T>
__device__ __forceinline__ T ld_nc_global(const T *ptr) {
    T r;
    memcpy(&r, ptr, sizeof(T));
    return r;
}
template <>
__device__ __forceinline__ int4 ld_nc_global(const int4 *ptr) {
    int4 r;
    memcpy(&r, ptr, sizeof(int4));
    return r;
}
template <>
__device__ __forceinline__ int2 ld_nc_global(const int2 *ptr) {
    int2 r;
    memcpy(&r, ptr, sizeof(int2));
    return r;
}
template <typename T>
__device__ __forceinline__ void st_na_global(const T *ptr, const T &v) {
    memcpy(const_cast<T *>(ptr), &v, sizeof(T));
}
template <>
__device__ __forceinline__ void st_na_global(const int4 *ptr, const int4 &v) {
    memcpy(const_cast<int4 *>(ptr), &v, sizeof(int4));
}
__device__ __forceinline__ void st_na_release(const int *ptr, int val) {
    *const_cast<volatile int *>(ptr) = val;
    __threadfence_system();
}
__device__ __forceinline__ void st_na_release(const uint32_t *ptr,
                                              uint32_t val) {
    *const_cast<volatile uint32_t *>(ptr) = val;
    __threadfence_system();
}
__device__ __forceinline__ void st_na_release(const uint64_t *ptr,
                                              uint64_t val) {
    *const_cast<volatile uint64_t *>(ptr) = val;
    __threadfence_system();
}
// MUSA: __ldg not available
#ifndef __ldg
#define __ldg(ptr) (*(ptr))
#endif
// MUSA: __activemask not available
#ifndef __activemask
#define __activemask() (0xffffffff)
#endif

__forceinline__ __device__ int get_lane_id() { return threadIdx.x % 32; }

}  // namespace mooncake
