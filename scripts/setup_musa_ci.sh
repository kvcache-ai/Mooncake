#!/usr/bin/env bash
set -euxo pipefail

apt update -y
apt install -y curl libopenblas0 python3-pip
python3 -m pip install --no-cache-dir --upgrade pip
python3 -m pip install --no-cache-dir \
  torch==2.9.0 \
  --index-url https://download.pytorch.org/whl/cpu
python3 -m pip install --no-cache-dir \
  'numpy<2' \
  torchada==0.1.66
CUDA_COMPAT_HOME="${RUNNER_TEMP:-/tmp}/mooncake-musa-cuda"
rm -rf "${CUDA_COMPAT_HOME}"
mkdir -p "${CUDA_COMPAT_HOME}/include" "${CUDA_COMPAT_HOME}/bin" "${CUDA_COMPAT_HOME}/lib64"
cat > "${CUDA_COMPAT_HOME}/include/mooncake_musa_ci_compat.h" <<'H'
#ifndef MOONCAKE_MUSA_CI_COMPAT_H
#define MOONCAKE_MUSA_CI_COMPAT_H

/* Compatibility declarations for compiling PyTorch CUDA-facing headers
 * against the MUSA SDK in this CI job.  These live in the temporary
 * CUDA_HOME shim directory and do not modify /usr/local/musa/include.
 */
#include <driver_types.h>

#ifndef __host__
#define __host__
#endif
#ifndef __device__
#define __device__
#endif
#ifndef __cudart_builtin__
#define __cudart_builtin__
#endif
#ifndef CUDARTAPI
#define CUDARTAPI
#endif

#ifndef MUSART_PI
#define MUSART_PI 3.14159265358979323846
#endif
#ifndef MUSART_THIRD
#define MUSART_THIRD 0.33333333333333333333
#endif
#ifndef MUSART_SQRT_HALF_HI
#define MUSART_SQRT_HALF_HI 0.70710678118654752440
#endif
#ifndef MUSART_SQRT_HALF_LO
#define MUSART_SQRT_HALF_LO 0.0
#endif

#endif  /* MOONCAKE_MUSA_CI_COMPAT_H */
H
cat > "${CUDA_COMPAT_HOME}/include/cuda_runtime_api.h" <<'H'
#pragma once
#include "mooncake_musa_ci_compat.h"
#include <musa_runtime_api.h>
typedef musaError_t cudaError_t;
typedef musaStream_t cudaStream_t;
typedef musaEvent_t cudaEvent_t;
typedef musaIpcEventHandle_t cudaIpcEventHandle_t;
typedef enum musaMemcpyKind cudaMemcpyKind;
typedef enum musaDeviceAttr cudaDeviceAttr;
typedef enum musaStreamCaptureMode cudaStreamCaptureMode;
typedef enum musaStreamCaptureStatus cudaStreamCaptureStatus;
typedef struct musaDeviceProp cudaDeviceProp;
typedef struct musaPointerAttributes cudaPointerAttributes;
#define cudaSuccess musaSuccess
#define cudaErrorNotReady musaErrorNotReady
#define cudaDevAttrClockRate musaDevAttrClockRate
#define cudaMemcpyHostToDevice musaMemcpyHostToDevice
#define cudaMemcpyDeviceToHost musaMemcpyDeviceToHost
#define cudaMemcpyDefault musaMemcpyDefault
#define cudaMemoryTypeDevice musaMemoryTypeDevice
#define cudaHostAllocMapped musaHostAllocMapped
#define cudaEventDefault musaEventDefault
#define cudaEventDisableTiming musaEventDisableTiming
#define cudaEventInterprocess musaEventInterprocess
#define cudaEventRecordDefault musaEventRecordDefault
#define cudaEventRecordExternal musaEventRecordExternal
#define cudaEventWaitDefault musaEventWaitDefault
#define cudaEventWaitExternal musaEventWaitExternal
#define cudaStreamCaptureStatusNone musaStreamCaptureStatusNone
#define cudaStreamCaptureStatusActive musaStreamCaptureStatusActive
#define cudaStreamCaptureStatusInvalidated musaStreamCaptureStatusInvalidated
#define cudaGetErrorString musaGetErrorString
#define cudaGetLastError musaGetLastError
#define cudaGetDevice musaGetDevice
#define cudaSetDevice musaSetDevice
#define cudaDeviceGetAttribute musaDeviceGetAttribute
#define cudaMalloc musaMalloc
#define cudaFree musaFree
#define cudaMallocHost musaMallocHost
#define cudaFreeHost musaFreeHost
#ifdef __cplusplus
template <typename T>
static inline cudaError_t cudaHostAlloc(T **ptr, size_t size, unsigned int flags) {
  return musaHostAlloc(reinterpret_cast<void **>(ptr), size, flags);
}
template <typename T>
static inline cudaError_t cudaHostGetDevicePointer(T **device_ptr, T *host_ptr, unsigned int flags) {
  return musaHostGetDevicePointer(reinterpret_cast<void **>(device_ptr), host_ptr, flags);
}
#else
static inline cudaError_t cudaHostAlloc(void **ptr, size_t size, unsigned int flags) {
  return musaHostAlloc(ptr, size, flags);
}
static inline cudaError_t cudaHostGetDevicePointer(void **device_ptr, void *host_ptr, unsigned int flags) {
  return musaHostGetDevicePointer(device_ptr, host_ptr, flags);
}
#endif
#define cudaMemset musaMemset
#define cudaMemsetAsync musaMemsetAsync
#define cudaMemcpy musaMemcpy
#define cudaMemcpyAsync musaMemcpyAsync
#define cudaPointerGetAttributes musaPointerGetAttributes
static inline cudaError_t cudaThreadExchangeStreamCaptureMode(cudaStreamCaptureMode *mode) {
  (void)mode;
  return cudaSuccess;
}
static inline cudaError_t cudaStreamIsCapturing(cudaStream_t stream, cudaStreamCaptureStatus *status) {
  (void)stream;
  if (status) *status = cudaStreamCaptureStatusNone;
  return cudaSuccess;
}
#define cudaStreamSynchronize musaStreamSynchronize
static inline cudaError_t cudaStreamQuery(cudaStream_t stream) { return musaStreamQuery(stream); }
static inline cudaError_t cudaStreamGetPriority(cudaStream_t stream, int *priority) { return musaStreamGetPriority(stream, priority); }
static inline cudaError_t cudaStreamWaitEvent(cudaStream_t stream, cudaEvent_t event, unsigned int flags) {
  return musaStreamWaitEvent(stream, event, flags);
}
static inline cudaError_t cudaDeviceGetStreamPriorityRange(int *least, int *greatest) { return musaDeviceGetStreamPriorityRange(least, greatest); }
static inline cudaError_t cudaEventCreateWithFlags(cudaEvent_t *event, unsigned int flags) { return musaEventCreateWithFlags(event, flags); }
static inline cudaError_t cudaEventDestroy(cudaEvent_t event) { return musaEventDestroy(event); }
static inline cudaError_t cudaEventRecord(cudaEvent_t event, cudaStream_t stream) { return musaEventRecord(event, stream); }
static inline cudaError_t cudaEventRecordWithFlags(cudaEvent_t event, cudaStream_t stream, unsigned int flags) { return musaEventRecordWithFlags(event, stream, flags); }
static inline cudaError_t cudaEventQuery(cudaEvent_t event) { return musaEventQuery(event); }
static inline cudaError_t cudaEventSynchronize(cudaEvent_t event) { return musaEventSynchronize(event); }
static inline cudaError_t cudaEventElapsedTime(float *ms, cudaEvent_t start, cudaEvent_t end) { return musaEventElapsedTime(ms, start, end); }
static inline cudaError_t cudaIpcGetEventHandle(cudaIpcEventHandle_t *handle, cudaEvent_t event) { return musaIpcGetEventHandle(handle, event); }
static inline cudaError_t cudaIpcOpenEventHandle(cudaEvent_t *event, cudaIpcEventHandle_t handle) { return musaIpcOpenEventHandle(event, handle); }
H
cat > "${CUDA_COMPAT_HOME}/include/cuda_runtime.h" <<'H'
#pragma once
#include <cuda_runtime_api.h>
H
cat > "${CUDA_COMPAT_HOME}/include/cuda.h" <<'H'
#pragma once
#include <musa.h>
#include <cuda_runtime_api.h>
#define CUresult MUresult
#define CUDA_SUCCESS MUSA_SUCCESS
H
cat > "${CUDA_COMPAT_HOME}/include/cuda_bf16.h" <<'H'
#pragma once
#include <stdint.h>
#ifdef __MCC__
#include <musa_bf16.h>
typedef mt_bfloat16 nv_bfloat16;
#else
typedef uint16_t nv_bfloat16;
static __host__ __device__ inline float __bfloat162float(nv_bfloat16) { return 0.0f; }
static __host__ __device__ inline nv_bfloat16 __float2bfloat16(float) { return 0; }
#endif
H
cat > "${CUDA_COMPAT_HOME}/include/cuda_fp16.h" <<'H'
#pragma once
#include <stdint.h>
struct __half { uint16_t __x; };
struct __half2 { uint32_t __x; };
typedef __half half;
typedef __half2 half2;
H
cat > "${CUDA_COMPAT_HOME}/include/cuda_fp16.hpp" <<'H'
#pragma once
#include <cuda_fp16.h>
H
cat > "${CUDA_COMPAT_HOME}/include/cuda_fp8.h" <<'H'
#pragma once
typedef unsigned char __nv_fp8_storage_t;
typedef unsigned short __nv_fp8x2_storage_t;
H
cat > "${CUDA_COMPAT_HOME}/include/cusparse.h" <<'H'
#pragma once
typedef struct cusparseContext *cusparseHandle_t;
typedef struct cusparseMatDescr *cusparseMatDescr_t;
typedef int cusparseStatus_t;
#define CUSPARSE_STATUS_SUCCESS 0
H
cat > "${CUDA_COMPAT_HOME}/include/cublas_v2.h" <<'H'
#pragma once
typedef struct cublasContext *cublasHandle_t;
typedef int cublasStatus_t;
typedef int cublasPointerMode_t;
typedef int cublasSideMode_t;
typedef int cublasFillMode_t;
typedef int cublasOperation_t;
typedef int cublasDiagType_t;
#define CUBLAS_STATUS_SUCCESS 0
H
cat > "${CUDA_COMPAT_HOME}/include/cublasLt.h" <<'H'
#pragma once
typedef struct cublasLtContext *cublasLtHandle_t;
H
cat > "${CUDA_COMPAT_HOME}/include/cusolverDn.h" <<'H'
#pragma once
#include <cusolver_common.h>
typedef struct cusolverDnContext *cusolverDnHandle_t;
typedef int cusolverStatus_t;
#define CUSOLVER_STATUS_SUCCESS 0
H
cat > "${CUDA_COMPAT_HOME}/include/cusolver_common.h" <<'H'
#pragma once
typedef int cusolverStatus_t;
#define CUSOLVER_STATUS_SUCCESS 0
H
cat > /tmp/mooncake_empty_cuda_stub.c <<'C'
void mooncake_empty_cuda_stub(void) {}
C
cc -shared -fPIC /tmp/mooncake_empty_cuda_stub.c -o "${CUDA_COMPAT_HOME}/lib64/libcudart.so"
torch_lib_dir=$(python3 - <<'PY'
import pathlib
import site
for site_dir in site.getsitepackages():
    torch_lib = pathlib.Path(site_dir) / 'torch' / 'lib'
    if torch_lib.is_dir():
        print(torch_lib)
        break
PY
)
[ -e "${torch_lib_dir}/libc10_cuda.so" ] || cc -shared -fPIC /tmp/mooncake_empty_cuda_stub.c -o "${torch_lib_dir}/libc10_cuda.so"
[ -e "${torch_lib_dir}/libtorch_cuda.so" ] || cc -shared -fPIC /tmp/mooncake_empty_cuda_stub.c -o "${torch_lib_dir}/libtorch_cuda.so"
cat > "${CUDA_COMPAT_HOME}/bin/nvcc" <<'SH'
#!/usr/bin/env bash
args=(-I"${CUDA_HOME}/include" -include "${CUDA_HOME}/include/mooncake_musa_ci_compat.h")
skip_compiler_options=0
for arg in "$@"; do
  if [[ ${skip_compiler_options} -eq 1 ]]; then
    arg="${arg%\'}"
    arg="${arg#\'}"
    args+=("${arg}")
    skip_compiler_options=0
    continue
  fi
  case "${arg}" in
    --expt-relaxed-constexpr)
      ;;
    --cuda-gpu-arch=*)
      args+=("--offload-arch=${arg#--cuda-gpu-arch=}")
      ;;
    --compiler-options)
      skip_compiler_options=1
      ;;
    *.cu)
      mu_source="${TMPDIR:-/tmp}/$(basename "${arg%.cu}").mu"
      cp "${arg}" "${mu_source}"
      args+=("${mu_source}")
      ;;
    *)
      args+=("${arg}")
      ;;
  esac
done
exec /usr/local/musa/bin/mcc "${args[@]}"
SH
chmod +x "${CUDA_COMPAT_HOME}/bin/nvcc"
echo "MUSA_HOME=/usr/local/musa" >> $GITHUB_ENV
echo "CUDA_HOME=${CUDA_COMPAT_HOME}" >> $GITHUB_ENV
echo "${CUDA_COMPAT_HOME}/bin" >> $GITHUB_PATH
echo "CPATH=/usr/local/musa/include:${CPATH}" >> $GITHUB_ENV
echo "LD_LIBRARY_PATH=${CUDA_COMPAT_HOME}/lib64:/usr/local/musa/lib:${LD_LIBRARY_PATH}" >> $GITHUB_ENV
# Keep PyTorch from eagerly loading accelerator runtime backends during
# extension builds; this job only needs the PyTorch 2.9 headers and
# torchada's CUDA-to-MUSA compile-time mapping.
echo "TORCH_DEVICE_BACKEND_AUTOLOAD=0" >> $GITHUB_ENV
echo "TORCHADA_PLATFORM=musa" >> $GITHUB_ENV
export TORCH_DEVICE_BACKEND_AUTOLOAD=0
export TORCHADA_PLATFORM=musa
python3 - <<'PY'
import torch
import torchada
print("torch", torch.__version__)
print("torchada", getattr(torchada, "__version__", "unknown"))
print("torchada platform", torchada.get_platform().value)
PY
