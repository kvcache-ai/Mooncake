#ifndef MOONCAKE_WORKER_KERNELS_CUH
#define MOONCAKE_WORKER_KERNELS_CUH

// Include the main worker header for struct definitions (Task, SegmentInfo,
// TransferGroupMeta). When compiled by mcc (__MUSA__ defined), the torch-
// dependent parts are guarded out, making this safe for the MUSA compiler.
#include <mooncake_worker.cuh>

namespace mooncake {

// Kernel function declarations — guarded so g++ doesn't see __global__
// which it can't parse. Parameters use plain C++ types (int instead of
// c10d::OpType / c10d::ReduceOp::RedOpType) so that mcc can compile them
// without torch headers.
#if defined(__CUDACC__) || defined(__MUSA__)
__global__ void enqueueTaskKernel(int opType, size_t tensorSize,
                                  int64_t broadcastRoot, int bufferOffset,
                                  uint64_t submitSequence, void* meta,
                                  Task* tasks, int numRanks,
                                  const bool* activeRanks,
                                  int* activeRanksTensor, size_t taskId);

template <typename scalar_t>
__global__ void reduceKernel(scalar_t* dst, const scalar_t* src,
                             size_t numElements, size_t numRanks, int op,
                             bool* activeRanks);
#endif

// Host-callable kernel launch wrappers (compiled by mcc/nvcc, callable from
// g++) g++ cannot compile <<<>>> syntax, so these wrappers are compiled by the
// GPU compiler and provide plain C++ functions that the host code can call.
// Both CUDA and MUSA use cudaStream_t in the declaration: on MUSA,
// cuda_alike.h typedefs cudaStream_t to musaStream_t.

void launchEnqueueTaskKernel(int opType, size_t tensorSize,
                             int64_t broadcastRoot, int bufferOffset,
                             uint64_t submitSequence, void* meta, Task* tasks,
                             int numRanks, const bool* activeRanks,
                             int* activeRanksTensor, size_t taskId,
                             cudaStream_t stream);

void launchReduceKernel_uint8(uint8_t* dst, const uint8_t* src,
                              size_t numElements, size_t numRanks, int op,
                              bool* activeRanks, cudaStream_t stream);
void launchReduceKernel_int8(int8_t* dst, const int8_t* src, size_t numElements,
                             size_t numRanks, int op, bool* activeRanks,
                             cudaStream_t stream);
void launchReduceKernel_int16(int16_t* dst, const int16_t* src,
                              size_t numElements, size_t numRanks, int op,
                              bool* activeRanks, cudaStream_t stream);
void launchReduceKernel_int32(int* dst, const int* src, size_t numElements,
                              size_t numRanks, int op, bool* activeRanks,
                              cudaStream_t stream);
void launchReduceKernel_int64(int64_t* dst, const int64_t* src,
                              size_t numElements, size_t numRanks, int op,
                              bool* activeRanks, cudaStream_t stream);
void launchReduceKernel_float(float* dst, const float* src, size_t numElements,
                              size_t numRanks, int op, bool* activeRanks,
                              cudaStream_t stream);
void launchReduceKernel_double(double* dst, const double* src,
                               size_t numElements, size_t numRanks, int op,
                               bool* activeRanks, cudaStream_t stream);
void launchReduceKernel_bool(bool* dst, const bool* src, size_t numElements,
                             size_t numRanks, int op, bool* activeRanks,
                             cudaStream_t stream);
void launchReduceKernel_bf16(void* dst, const void* src, size_t numElements,
                             size_t numRanks, int op, bool* activeRanks,
                             cudaStream_t stream);

void preloadReduceKernels();

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_KERNELS_CUH
