#pragma once

#define NUM_MAX_NVL_PEERS 8
#define NUM_MAX_RDMA_PEERS 20
#define MAX_QP_COUNT 256
#define NUM_MAX_FIFO_SLOTS 32768
#define NUM_WORKSPACE_BYTES (32 * 1024 * 1024)
#define NUM_MAX_LOCAL_EXPERTS 1024
#define NUM_BUFFER_ALIGNMENT_BYTES 128

#define FINISHED_SUM_TAG 1024
#define NUM_CPU_TIMEOUT_SECS 100
#define NUM_TIMEOUT_CYCLES 200000000000ull  // 200G cycles ~= 100s
#define NUM_WAIT_NANOSECONDS 500

#define LOW_LATENCY_SEND_PHASE 1
#define LOW_LATENCY_RECV_PHASE 2

// Make CLion CUDA indexing work
#ifdef __CLION_IDE__
#define __CUDA_ARCH__ 900  // NOLINT(*-reserved-identifier)
#define __CUDACC_RDC__     // NOLINT(*-reserved-identifier)
#endif

// Remove Torch restrictions
#ifdef __CUDA_NO_HALF_CONVERSIONS__
#undef __CUDA_NO_HALF_CONVERSIONS__
#endif
#ifdef __CUDA_NO_HALF_OPERATORS__
#undef __CUDA_NO_HALF_OPERATORS__
#endif
#ifdef __CUDA_NO_HALF2_OPERATORS__
#undef __CUDA_NO_HALF2_OPERATORS__
#endif
#ifdef __CUDA_NO_BFLOAT16_CONVERSIONS__
#undef __CUDA_NO_BFLOAT16_CONVERSIONS__
#endif
#ifdef __CUDA_NO_BFLOAT162_OPERATORS__
#undef __CUDA_NO_BFLOAT162_OPERATORS__
#endif

#ifdef MOONCAKE_EP_USE_MUSA
#include <musa_runtime.h>
#include <musa_bf16.h>
// MUSA type aliases for CUDA compatibility
#ifdef __MUSA_ARCH__
using nv_bfloat16 = mt_bfloat16;
#elif defined(__MUSA__)
using nv_bfloat16 = mt_bfloat16;
#else
struct nv_bfloat16 {
    unsigned short __x;
};
#endif
// FP8 stubs — MUSA has no FP8; templates compile but are never instantiated
using __nv_fp8_storage_t = uint8_t;
using __nv_fp8x2_storage_t = uint16_t;
#define __NV_SATFINITE 0
#define __NV_E4M3 0
__device__ __forceinline__ __nv_fp8x2_storage_t __nv_cvt_float2_to_fp8x2(float2,
                                                                         int,
                                                                         int) {
    return 0;
}
// MUSA runtime API aliases
#define cudaMalloc musaMalloc
#define cudaMallocHost musaMallocHost
#define cudaFree musaFree
#define cudaFreeHost musaFreeHost
#define cudaMemcpy musaMemcpy
#define cudaMemcpyAsync musaMemcpyAsync
#define cudaMemset musaMemset
#define cudaMemsetAsync musaMemsetAsync
#define cudaGetDevice musaGetDevice
#define cudaGetDeviceCount musaGetDeviceCount
#define cudaSetDevice musaSetDevice
#define cudaDeviceGetAttribute musaDeviceGetAttribute
#define cudaDeviceCanAccessPeer musaDeviceCanAccessPeer
#define cudaDeviceEnablePeerAccess musaDeviceEnablePeerAccess
#define cudaGetLastError musaGetLastError
#define cudaStream_t musaStream_t
#define cudaEvent_t musaEvent_t
#define cudaIpcMemHandle_t musaIpcMemHandle_t
#define cudaIpcGetMemHandle musaIpcGetMemHandle
#define cudaIpcOpenMemHandle musaIpcOpenMemHandle
#define cudaMemcpyHostToDevice musaMemcpyHostToDevice
#define cudaMemcpyDeviceToHost musaMemcpyDeviceToHost
#define cudaMemcpyDeviceToDevice musaMemcpyDeviceToDevice
#define cudaDevAttrClockRate musaDevAttrClockRate
#define cudaErrorPeerAccessAlreadyEnabled musaErrorPeerAccessAlreadyEnabled
#define cudaSuccess musaSuccess
#else
#include <cuda_bf16.h>
#include <cuda_fp8.h>
#include <cuda_runtime.h>
#include <infiniband/mlx5dv.h>
#endif
