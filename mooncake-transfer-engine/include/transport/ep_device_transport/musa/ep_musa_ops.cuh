// MUSA implementations of EP device-side memory ordering primitives.
// Included by ep_device_ops.cuh when MOONCAKE_EP_USE_MUSA is defined.
//
// MUSA has no PTX-style acquire/release instructions.  We emulate them with
// plain loads/stores + __threadfence_system() for cross-GPU (MTLink)
// visibility.
//
// Known MUSA SDK 4.3.3 compiler bugs to avoid:
//   - atomicAdd_system / atomicCAS_system → infinite SelectionDAG loop.
//     Use block-scope atomicAdd + __threadfence_system() instead.
//   - Named barriers (bar.sync) → not available; use __syncthreads().
//   - cooperative_groups::this_grid().sync() → not available; host uses
//     separate kernel launches (return_recv_hook=true) so grid sync is a no-op.
#pragma once

#include <musa_runtime.h>

namespace mooncake {
namespace ep {

// ---------------------------------------------------------------------------
// Acquire loads
// ---------------------------------------------------------------------------
__device__ __forceinline__ int ep_ld_acquire(const int* ptr) {
    __threadfence_system();
    return *const_cast<volatile const int*>(ptr);
}

__device__ __forceinline__ uint64_t ep_ld_acquire_u64(const uint64_t* ptr) {
    __threadfence_system();
    return *const_cast<volatile const uint64_t*>(ptr);
}

// ---------------------------------------------------------------------------
// Release stores
// ---------------------------------------------------------------------------
__device__ __forceinline__ void ep_st_release(const int* ptr, int val) {
    *const_cast<volatile int*>(ptr) = val;
    __threadfence_system();
}

__device__ __forceinline__ void ep_st_release_u32(const uint32_t* ptr,
                                                  uint32_t val) {
    *const_cast<volatile uint32_t*>(ptr) = val;
    __threadfence_system();
}

__device__ __forceinline__ void ep_st_release_u64(const uint64_t* ptr,
                                                  uint64_t val) {
    *const_cast<volatile uint64_t*>(ptr) = val;
    __threadfence_system();
}

// ---------------------------------------------------------------------------
// Atomic add — block-scope atomicAdd + system fence (avoids SDK bug)
// ---------------------------------------------------------------------------
__device__ __forceinline__ int ep_atomic_add_release(const int* ptr, int val) {
    int ret = atomicAdd(const_cast<int*>(ptr), val);
    __threadfence_system();
    return ret;
}

// ---------------------------------------------------------------------------
// Non-coherent loads — MUSA has no nc/no_allocate cache hints; use volatile.
// ---------------------------------------------------------------------------
__device__ __forceinline__ int4 ep_ld_nc(const int4* ptr) {
    return *const_cast<volatile const int4*>(ptr);
}

__device__ __forceinline__ int ep_ld_nc_s32(const int* ptr) {
    return *const_cast<volatile const int*>(ptr);
}

__device__ __forceinline__ float ep_ld_nc_f32(const float* ptr) {
    return *const_cast<volatile const float*>(ptr);
}

__device__ __forceinline__ int64_t ep_ld_nc_s64(const int64_t* ptr) {
    return *const_cast<volatile const int64_t*>(ptr);
}

// ---------------------------------------------------------------------------
// Non-temporal stores — MUSA has no nt/no_allocate hints; plain store.
// ---------------------------------------------------------------------------
__device__ __forceinline__ void ep_st_na(const int4* ptr, const int4& val) {
    *const_cast<volatile int4*>(ptr) = val;
}

// ---------------------------------------------------------------------------
// Named barrier — MUSA has no bar.sync; use __syncthreads() (CTA scope).
// The bar_id and num_threads parameters are ignored.
// ---------------------------------------------------------------------------
__device__ __forceinline__ void ep_bar_sync(int /*bar_id*/,
                                            int /*num_threads*/) {
    __syncthreads();
}

// ---------------------------------------------------------------------------
// Grid sync — not available on MUSA.  Host always uses separate kernel
// launches, so SEND and RECV never share a kernel invocation.  No-op.
// ---------------------------------------------------------------------------
__device__ __forceinline__ void ep_grid_sync() {}

// ---------------------------------------------------------------------------
// Byte-swap helpers — MUSA has no __byte_perm; implement manually.
// ---------------------------------------------------------------------------
__device__ __forceinline__ uint16_t ep_bswap16(uint16_t x) {
    return (uint16_t)(((x & 0x00FFu) << 8) | ((x & 0xFF00u) >> 8));
}
__device__ __forceinline__ uint32_t ep_bswap32(uint32_t x) {
    return ((x & 0x000000FFu) << 24) | ((x & 0x0000FF00u) << 8) |
           ((x & 0x00FF0000u) >> 8) | ((x & 0xFF000000u) >> 24);
}
__device__ __forceinline__ uint64_t ep_bswap64(uint64_t x) {
    uint32_t hi = ep_bswap32((uint32_t)(x >> 32));
    uint32_t lo = ep_bswap32((uint32_t)(x));
    return ((uint64_t)lo << 32) | hi;
}

}  // namespace ep
}  // namespace mooncake
