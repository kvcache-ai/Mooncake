// MACA implementations of device-side memory ordering primitives.
//
// MACA's cu-bridge compiler accepts CUDA-like intrinsics, but does not reliably
// compile the PTX acquire/release/barrier instructions used by the CUDA path.
#pragma once

#include <cuda_runtime.h>

namespace mooncake {
namespace device {

__device__ __forceinline__ int mc_ld_acquire(const int* ptr) {
    __threadfence_system();
    return *const_cast<volatile const int*>(ptr);
}

__device__ __forceinline__ uint64_t mc_ld_acquire_u64(const uint64_t* ptr) {
    __threadfence_system();
    return *const_cast<volatile const uint64_t*>(ptr);
}

__device__ __forceinline__ void mc_st_release(const int* ptr, int val) {
    *const_cast<volatile int*>(ptr) = val;
    __threadfence_system();
}

__device__ __forceinline__ void mc_st_release_u32(const uint32_t* ptr,
                                                  uint32_t val) {
    *const_cast<volatile uint32_t*>(ptr) = val;
    __threadfence_system();
}

__device__ __forceinline__ void mc_st_release_u64(const uint64_t* ptr,
                                                  uint64_t val) {
    *const_cast<volatile uint64_t*>(ptr) = val;
    __threadfence_system();
}

__device__ __forceinline__ int mc_atomic_add_release(const int* ptr, int val) {
    int ret = atomicAdd(const_cast<int*>(ptr), val);
    __threadfence_system();
    return ret;
}

__device__ __forceinline__ int4 mc_ld_nc(const int4* ptr) { return __ldg(ptr); }

__device__ __forceinline__ int mc_ld_nc_s32(const int* ptr) {
    return __ldg(ptr);
}

__device__ __forceinline__ float mc_ld_nc_f32(const float* ptr) {
    return __ldg(ptr);
}

__device__ __forceinline__ int64_t mc_ld_nc_s64(const int64_t* ptr) {
    return __ldg(ptr);
}

__device__ __forceinline__ void mc_st_na(const int4* ptr, const int4& val) {
    *const_cast<int4*>(ptr) = val;
}

__device__ __forceinline__ void mc_bar_init() {}

__device__ __forceinline__ void mc_bar_sync(int /*bar_id*/,
                                            int /*num_threads*/) {
    __syncthreads();
}

__device__ __forceinline__ void mc_grid_sync() {}

__device__ __forceinline__ void mc_fence() { __threadfence_system(); }

__device__ __forceinline__ void mc_fence_barrier_fence() {
    mc_fence();
    mc_bar_sync(0, 0);
    mc_fence();
}

__device__ __forceinline__ uint16_t mc_bswap16(uint16_t x) {
    return (uint16_t)(((x & 0x00FFu) << 8) | ((x & 0xFF00u) >> 8));
}

__device__ __forceinline__ uint32_t mc_bswap32(uint32_t x) {
    return ((x & 0x000000FFu) << 24) | ((x & 0x0000FF00u) << 8) |
           ((x & 0x00FF0000u) >> 8) | ((x & 0xFF000000u) >> 24);
}

__device__ __forceinline__ uint64_t mc_bswap64(uint64_t x) {
    uint32_t hi = mc_bswap32((uint32_t)(x >> 32));
    uint32_t lo = mc_bswap32((uint32_t)(x));
    return ((uint64_t)lo << 32) | hi;
}

}  // namespace device
}  // namespace mooncake
