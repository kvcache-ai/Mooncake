#pragma once

#define NUM_MAX_NVL_PEERS 8
#define NUM_MAX_RDMA_PEERS 20
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
// When compiling .mu kernels with mcc, both host and device passes need
// mt_bfloat16 (not a stub), because __bfloat162float/__float2bfloat16
// require __mt_bfloat16 arguments.  The host stub is only needed when
// compiling pure .cpp files (buffer.cpp etc.) that never call these
// conversion functions.
#ifdef __MUSA_ARCH__
using nv_bfloat16 = mt_bfloat16;
#else
// Host-side compilation: use mt_bfloat16 if available (mcc host pass),
// otherwise fall back to a 2-byte stub (pure C++ host compiler like g++).
#ifdef __MUSA__
using nv_bfloat16 = mt_bfloat16;
#else
struct nv_bfloat16 {
    unsigned short __x;
    nv_bfloat16() = default;
    nv_bfloat16& operator=(unsigned short v) { __x = v; return *this; }
};
#endif
#endif
// MUSA FP8 stubs (MUSA does not support FP8, but templates still compile)
using __nv_fp8_storage_t = uint8_t;
using __nv_fp8x2_storage_t = uint16_t;
#define __NV_SATFINITE 0
#define __NV_E4M3 0
__device__ __forceinline__ __nv_fp8x2_storage_t __nv_cvt_float2_to_fp8x2(float2, int, int) {
    return 0;  // Never reached at runtime
}
// MUSA runtime API aliases (host-side code uses cuda* names)
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
// MUSA does not have FP8 or InfiniBand
#else
#include <cuda_bf16.h>
#include <cuda_fp8.h>
#include <cuda_runtime.h>
#include <infiniband/mlx5dv.h>
#endif
