// MACA implementations of device-side memory ordering primitives.
// Included by device_ops.cuh when USE_MACA is defined.
//
// MACA mxcc supports nearly all CUDA PTX instructions, but the CUDA-bridge
// compiler (cucc) used by torch.utils.cpp_extension has limited support for
// certain PTX inline-assembly instructions when they are expanded inline.
// We therefore follow the same fallback strategy as MUSA, but with a key
// advantage: MACA does support __ldg and __byte_perm intrinsics.
//
// Constraints avoided:
//   - "l" (64-bit register) -> use "r"
//   - "=f" (float register) -> use "r"
//   - "=h" (16-bit register) -> use "r"
//
// Instructions replaced by intrinsics:
//   - ld.acquire / st.release / atom.add.release -> volatile + fence
//   - bar.sync -> __syncthreads()
//   - grid.sync -> no-op (host uses separate kernel launches)
//   - trap -> __trap()
#pragma once

#include <cuda_runtime.h>

namespace mooncake {
namespace device {

// ---------------------------------------------------------------------------
// Acquire loads — emulate with volatile load + system fence
// ---------------------------------------------------------------------------
__device__ __forceinline__ int mc_ld_acquire(const int* ptr) {
    __threadfence_system();
    return *const_cast<volatile const int*>(ptr);
}

__device__ __forceinline__ uint64_t mc_ld_acquire_u64(const uint64_t* ptr) {
    __threadfence_system();
    return *const_cast<volatile const uint64_t*>(ptr);
}

// ---------------------------------------------------------------------------
// Release stores — emulate with volatile store + system fence
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Atomic add — atomicAdd + system fence
// ---------------------------------------------------------------------------
__device__ __forceinline__ int mc_atomic_add_release(const int* ptr, int val) {
    int ret = atomicAdd(const_cast<int*>(ptr), val);
    __threadfence_system();
    return ret;
}

// ---------------------------------------------------------------------------
// Non-coherent loads — MACA supports __ldg!
// ---------------------------------------------------------------------------
__device__ __forceinline__ int4 mc_ld_nc(const int4* ptr) {
    return __ldg(ptr);
}

__device__ __forceinline__ int mc_ld_nc_s32(const int* ptr) {
    return __ldg(ptr);
}

__device__ __forceinline__ float mc_ld_nc_f32(const float* ptr) {
    return __ldg(ptr);
}

__device__ __forceinline__ int64_t mc_ld_nc_s64(const int64_t* ptr) {
    return __ldg(ptr);
}

// ---------------------------------------------------------------------------
// Non-temporal stores — plain volatile store (no nt hint available)
// ---------------------------------------------------------------------------
__device__ __forceinline__ void mc_st_na(const int4* ptr, const int4& val) {
    volatile int* vp = reinterpret_cast<volatile int*>(const_cast<int4*>(ptr));
    vp[0] = val.x;
    vp[1] = val.y;
    vp[2] = val.z;
    vp[3] = val.w;
}

// ---------------------------------------------------------------------------
// Named barrier init — no-op
// ---------------------------------------------------------------------------
__device__ __forceinline__ void mc_bar_init() {}

// ---------------------------------------------------------------------------
// Named barrier — __syncthreads()
// ---------------------------------------------------------------------------
__device__ __forceinline__ void mc_bar_sync(int /*bar_id*/,
                                            int /*num_threads*/) {
    __syncthreads();
}

// ---------------------------------------------------------------------------
// Grid sync — no-op. Host always uses separate kernel launches.
// ---------------------------------------------------------------------------
__device__ __forceinline__ void mc_grid_sync() {}

// ---------------------------------------------------------------------------
// Byte-swap helpers — MACA supports __byte_perm!
// ---------------------------------------------------------------------------
__device__ __forceinline__ uint16_t mc_bswap16(uint16_t x) {
    return __byte_perm(x, x, 0x2301);
}
__device__ __forceinline__ uint32_t mc_bswap32(uint32_t x) {
    return __byte_perm(x, x, 0x0123);
}
__device__ __forceinline__ uint64_t mc_bswap64(uint64_t x) {
    uint32_t hi = __byte_perm((uint32_t)(x >> 32), 0, 0x0123);
    uint32_t lo = __byte_perm((uint32_t)(x), 0, 0x0123);
    return ((uint64_t)lo << 32) | hi;
}

}  // namespace device
}  // namespace mooncake
