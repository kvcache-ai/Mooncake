#pragma once
// ============================================================================
// mooncake_ep_device.h - Minimal platform-specific definitions
// ============================================================================
// Only contains items that torchada's source-to-source translation cannot
// handle.  Everything else uses plain CUDA API names — torchada maps them
// to MUSA equivalents at build time via SimplePorting text replacement.
// ============================================================================

#if defined(MOONCAKE_EP_USE_MUSA)

// -- FP8 types (MUSA uses different names; not in torchada mapping) ----------
#include <musa_fp8.h>
using ep_fp8_storage_t = __mt_fp8_storage_t;
using ep_fp8x2_storage_t = __mt_fp8x2_storage_t;
#if defined(__CUDACC__) || defined(__MCC__)
__device__ __forceinline__ ep_fp8x2_storage_t ep_cvt_float2_to_fp8x2(float2 x) {
    return __musa_cvt_float2_to_fp8x2(x, __MT_SATFINITE, __MT_E4M3);
}
#endif

// -- Device intrinsics (MUSA doesn't have __ldg / __activemask) --------------
#ifndef __ldg
#define __ldg(ptr) (*(ptr))
#endif
#ifndef __activemask
#define __activemask() (0xffffffff)
#endif

#if defined(__CUDACC__) || defined(__MCC__)
constexpr int kEpWarpSize = 32;
constexpr unsigned int kEpWarpMask = 0xffffffffu;
__forceinline__ __device__ int get_lane_id() { return threadIdx.x % kEpWarpSize; }
#endif

// -- Kernel launch (MUSA: no __launch_bounds__, no cooperative launch) --------
#define EP_LAUNCH_BOUNDS(max_threads, min_blocks)

#define SETUP_LAUNCH_CONFIG(num_sms, num_threads, stream) \
    dim3 _grid(num_sms);                                  \
    dim3 _block(num_threads);                             \
    cudaStream_t _stream = stream

#define LAUNCH_KERNEL(config, kernel, ...)                     \
    kernel<<<_grid, _block, 0, _stream>>>(__VA_ARGS__);        \
    {                                                          \
        auto _err = cudaGetLastError();                        \
        if (_err != cudaSuccess) {                             \
            fprintf(stderr, "[EP] kernel launch failed: %s\n", \
                    cudaGetErrorString(_err));                 \
        }                                                      \
    }

#elif defined(MOONCAKE_EP_USE_MACA)

// -- FP8 types (MACA follows CUDA-compatible names) -------------------------
#include <cuda_fp8.h>
using ep_fp8_storage_t = __nv_fp8_storage_t;
using ep_fp8x2_storage_t = __nv_fp8x2_storage_t;
#if defined(__CUDACC__) || defined(__MCC__)
__device__ __forceinline__ ep_fp8x2_storage_t ep_cvt_float2_to_fp8x2(float2 x) {
    return __nv_cvt_float2_to_fp8x2(x, __NV_SATFINITE, __NV_E4M3);
}
#endif

// -- Device intrinsics -------------------------------------------------------
#if defined(__CUDACC__) || defined(__MCC__)
constexpr int kEpWarpSize = 64;
constexpr unsigned long long kEpWarpMask = 0xffffffffffffffffull;
__forceinline__ __device__ int get_lane_id() { return threadIdx.x % kEpWarpSize; }
#endif

// -- Kernel launch (MACA: no cooperative launch for this EP smoke path) ------
#define EP_LAUNCH_BOUNDS(max_threads, min_blocks)

#define SETUP_LAUNCH_CONFIG(num_sms, num_threads, stream) \
    dim3 _grid(num_sms);                                  \
    dim3 _block(num_threads);                             \
    cudaStream_t _stream = stream

#define LAUNCH_KERNEL(config, kernel, ...)                     \
    kernel<<<_grid, _block, 0, _stream>>>(__VA_ARGS__);        \
    {                                                          \
        auto _err = cudaGetLastError();                        \
        if (_err != cudaSuccess) {                             \
            fprintf(stderr, "[EP] kernel launch failed: %s\n", \
                    cudaGetErrorString(_err));                 \
        }                                                      \
    }

#else  // !MOONCAKE_EP_USE_MUSA && !MOONCAKE_EP_USE_MACA

// -- FP8 types (CUDA native names) -------------------------------------------
#include <cuda_fp8.h>
using ep_fp8_storage_t = __nv_fp8_storage_t;
using ep_fp8x2_storage_t = __nv_fp8x2_storage_t;
#if defined(__CUDACC__) || defined(__MCC__)
__device__ __forceinline__ ep_fp8x2_storage_t ep_cvt_float2_to_fp8x2(float2 x) {
    return __nv_cvt_float2_to_fp8x2(x, __NV_SATFINITE, __NV_E4M3);
}
#endif

// -- Device intrinsics -------------------------------------------------------
#if defined(__CUDACC__) || defined(__MCC__)
constexpr int kEpWarpSize = 32;
constexpr unsigned int kEpWarpMask = 0xffffffffu;
__forceinline__ __device__ int get_lane_id() {
    int lane_id;
    asm("mov.s32 %0, %laneid;" : "=r"(lane_id));
    return lane_id;
}
#endif

// -- Kernel launch (CUDA: cooperative launch) --------------------------------
#define EP_LAUNCH_BOUNDS(max_threads, min_blocks) \
    __launch_bounds__(max_threads, min_blocks)

#define SETUP_LAUNCH_CONFIG(num_sms, num_threads, stream) \
    cudaLaunchConfig_t cfg = {                            \
        (num_sms), (num_threads), 0, stream, nullptr, 0}; \
    cudaLaunchAttribute attr[1];                          \
    attr[0].id = cudaLaunchAttributeCooperative;          \
    attr[0].val.cooperative = 1;                          \
    cfg.attrs = attr;                                     \
    cfg.numAttrs = 1

#define LAUNCH_KERNEL(config, kernel, ...) \
    CUDA_CHECK(cudaLaunchKernelEx(config, kernel, ##__VA_ARGS__))

#endif  // MOONCAKE_EP_USE_MUSA / MOONCAKE_EP_USE_MACA

// Both platforms need IB verbs
#include <infiniband/mlx5dv.h>
