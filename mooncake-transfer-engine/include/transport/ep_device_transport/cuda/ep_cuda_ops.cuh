// CUDA PTX implementations of EP device-side memory ordering primitives.
// Included by ep_device_ops.cuh when MOONCAKE_EP_USE_MUSA is not defined.
#pragma once

#include <cuda_runtime.h>
#include <cooperative_groups.h>

namespace mooncake {
namespace ep {

// ---------------------------------------------------------------------------
// Acquire loads — cross-GPU visibility (sys scope)
// ---------------------------------------------------------------------------
__device__ __forceinline__ int ep_ld_acquire(const int* ptr) {
    int ret;
    asm volatile("ld.acquire.sys.global.s32 %0, [%1];" : "=r"(ret) : "l"(ptr));
    return ret;
}

__device__ __forceinline__ uint64_t ep_ld_acquire_u64(const uint64_t* ptr) {
    uint64_t ret;
    asm volatile("ld.acquire.sys.global.u64 %0, [%1];" : "=l"(ret) : "l"(ptr));
    return ret;
}

// ---------------------------------------------------------------------------
// Release stores — cross-GPU visibility (sys scope), non-temporal (no alloc)
// ---------------------------------------------------------------------------
__device__ __forceinline__ void ep_st_release(const int* ptr, int val) {
    asm volatile("st.release.sys.global.L1::no_allocate.s32 [%0], %1;"
                 :
                 : "l"(ptr), "r"(val));
}

__device__ __forceinline__ void ep_st_release_u32(const uint32_t* ptr,
                                                  uint32_t val) {
    asm volatile("st.release.sys.global.L1::no_allocate.b32 [%0], %1;"
                 :
                 : "l"(ptr), "r"(val));
}

__device__ __forceinline__ void ep_st_release_u64(const uint64_t* ptr,
                                                  uint64_t val) {
    asm volatile("st.release.sys.global.L1::no_allocate.b64 [%0], %1;"
                 :
                 : "l"(ptr), "l"(val));
}

// ---------------------------------------------------------------------------
// Atomic add — release semantics, sys scope
// ---------------------------------------------------------------------------
__device__ __forceinline__ int ep_atomic_add_release(const int* ptr, int val) {
    int ret;
    asm volatile("atom.add.release.sys.global.s32 %0, [%1], %2;"
                 : "=r"(ret)
                 : "l"(ptr), "r"(val));
    return ret;
}

// ---------------------------------------------------------------------------
// Non-coherent loads (read-only cache, no L1 alloc) — for bulk data reads
// ---------------------------------------------------------------------------
__device__ __forceinline__ int4 ep_ld_nc(const int4* ptr) {
    int4 ret;
    asm volatile(
        "ld.global.nc.L1::no_allocate.L2::256B.v4.s32 {%0,%1,%2,%3}, [%4];"
        : "=r"(ret.x), "=r"(ret.y), "=r"(ret.z), "=r"(ret.w)
        : "l"(ptr));
    return ret;
}

__device__ __forceinline__ int ep_ld_nc_s32(const int* ptr) {
    int ret;
    asm volatile("ld.global.nc.L1::no_allocate.s32 %0, [%1];"
                 : "=r"(ret)
                 : "l"(ptr));
    return ret;
}

__device__ __forceinline__ float ep_ld_nc_f32(const float* ptr) {
    float ret;
    asm volatile("ld.global.nc.L1::no_allocate.f32 %0, [%1];"
                 : "=f"(ret)
                 : "l"(ptr));
    return ret;
}

__device__ __forceinline__ int64_t ep_ld_nc_s64(const int64_t* ptr) {
    int64_t ret;
    asm volatile("ld.global.nc.L1::no_allocate.s64 %0, [%1];"
                 : "=l"(ret)
                 : "l"(ptr));
    return ret;
}

// ---------------------------------------------------------------------------
// Non-temporal stores (no L1 alloc) — for bulk data writes
// ---------------------------------------------------------------------------
__device__ __forceinline__ void ep_st_na(const int4* ptr, const int4& val) {
    asm volatile("st.global.L1::no_allocate.v4.s32 [%0], {%1,%2,%3,%4};"
                 :
                 : "l"(ptr), "r"(val.x), "r"(val.y), "r"(val.z), "r"(val.w));
}

// ---------------------------------------------------------------------------
// Named barrier (warp-group scope) — CUDA PTX bar.sync
// On MUSA this is replaced by __syncthreads() in ep_musa_ops.cuh.
// ---------------------------------------------------------------------------
__device__ __forceinline__ void ep_bar_sync(int bar_id, int num_threads) {
    asm volatile("bar.sync %0, %1;" : : "r"(bar_id), "r"(num_threads));
}

// ---------------------------------------------------------------------------
// Grid-level sync — cooperative_groups::this_grid().sync()
// On MUSA this is a no-op because the host always uses separate kernel
// launches (return_recv_hook=true), so SEND and RECV never run in the same
// kernel invocation.
// ---------------------------------------------------------------------------
__device__ __forceinline__ void ep_grid_sync() {
    cooperative_groups::this_grid().sync();
}

// ---------------------------------------------------------------------------
// Byte-swap helpers (for mlx5 big-endian WQE fields)
// ---------------------------------------------------------------------------
__device__ __forceinline__ uint16_t ep_bswap16(uint16_t x) {
    return __byte_perm(x, x, 0x2301);
}
__device__ __forceinline__ uint32_t ep_bswap32(uint32_t x) {
    return __byte_perm(x, x, 0x0123);
}
__device__ __forceinline__ uint64_t ep_bswap64(uint64_t x) {
    uint32_t hi = __byte_perm((uint32_t)(x >> 32), 0, 0x0123);
    uint32_t lo = __byte_perm((uint32_t)(x), 0, 0x0123);
    return ((uint64_t)lo << 32) | hi;
}

}  // namespace ep
}  // namespace mooncake
