// mooncake_worker_kernels.cu — GPU kernel functions for PG collectives.
// Compiled by both nvcc (CUDA) and mcc (MUSA) via mooncake_worker.mu.
// This file intentionally avoids torch headers to stay compatible with mcc.

#include <mooncake_worker_kernels.cuh>

#ifdef __MUSA__
#include <musa_bf16.h>
#endif

namespace mooncake {

__global__ void enqueueTaskKernel(int opType, size_t tensorSize,
                                  int64_t broadcastRoot, int bufferOffset,
                                  uint64_t submitSequence, void* meta,
                                  Task* tasks, int numRanks,
                                  const bool* activeRanks,
                                  int* activeRanksTensor, size_t taskId) {
    tasks[taskId].opType = opType;
    tasks[taskId].tensorSize = tensorSize;
    tasks[taskId].broadcastRoot = broadcastRoot;
    tasks[taskId].bufferOffset = bufferOffset;
    tasks[taskId].submitSequence = submitSequence;
    tasks[taskId].transferGroupMeta = meta;

    __threadfence_system();
    tasks[taskId].active = true;

    while (tasks[taskId].active) {
        __threadfence_system();
    }
    for (int i = 0; i < numRanks; ++i) {
        activeRanksTensor[i] = activeRanks[i] ? 1 : 0;
    }
}

template <typename scalar_t>
__global__ void reduceKernel(scalar_t* dst, const scalar_t* src,
                             size_t numElements, size_t numRanks,
                             int op, bool* activeRanks) {
    size_t thread_idx = blockIdx.x * blockDim.x + threadIdx.x;
    size_t stride = blockDim.x * gridDim.x;
    for (size_t elem_idx = thread_idx; elem_idx < numElements;
         elem_idx += stride) {
        bool valid = false;
        float acc = 0.0f;
        for (size_t rank = 0; rank < numRanks; ++rank) {
            if (activeRanks[rank]) {
                if (!valid) {
                    acc = (float)src[rank * numElements + elem_idx];
                    valid = true;
                } else {
                    switch (op) {
                        case 0:  // SUM
                            acc += (float)src[rank * numElements + elem_idx];
                            break;
                        case 2:  // PRODUCT
                            acc *= (float)src[rank * numElements + elem_idx];
                            break;
                        case 3:  // MIN
                            acc = fminf(acc, (float)src[rank * numElements + elem_idx]);
                            break;
                        case 4:  // MAX
                            acc = fmaxf(acc, (float)src[rank * numElements + elem_idx]);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        dst[elem_idx] = (scalar_t)acc;
    }
}

}  // namespace mooncake

// Kernel launch wrappers — compiled by mcc, callable from g++ host code.
// g++ cannot compile <<<>>> syntax, so these wrappers are compiled by mcc
// and provide plain C++ functions that the host code can call.
#ifdef __MUSA__
#include <musa_runtime.h>

namespace mooncake {

void launchEnqueueTaskKernel(int opType, size_t tensorSize,
                             int64_t broadcastRoot, int bufferOffset,
                             uint64_t submitSequence, void* meta,
                             Task* tasks, int numRanks,
                             const bool* activeRanks,
                             int* activeRanksTensor, size_t taskId,
                             cudaStream_t stream) {
    enqueueTaskKernel<<<1, 1, 0, stream>>>(
        opType, tensorSize, broadcastRoot, bufferOffset, submitSequence,
        meta, tasks, numRanks, activeRanks, activeRanksTensor, taskId);
}

#define DEF_LAUNCH_REDUCE(scalar_t, suffix)                                    \
    void launchReduceKernel_##suffix(scalar_t* dst, const scalar_t* src,       \
                                    size_t numElements, size_t numRanks,       \
                                    int op, bool* activeRanks,                 \
                                    cudaStream_t stream) {                     \
        reduceKernel<<<64, 256, 0, stream>>>(dst, src, numElements, numRanks,  \
                                             op, activeRanks);                 \
    }

DEF_LAUNCH_REDUCE(uint8_t, uint8)
DEF_LAUNCH_REDUCE(int8_t, int8)
DEF_LAUNCH_REDUCE(int16_t, int16)
DEF_LAUNCH_REDUCE(int, int32)
DEF_LAUNCH_REDUCE(int64_t, int64)
DEF_LAUNCH_REDUCE(float, float)
DEF_LAUNCH_REDUCE(double, double)
DEF_LAUNCH_REDUCE(bool, bool)

// BFloat16 needs special handling since it's a struct, not a primitive
void launchReduceKernel_bf16(void* dst, const void* src,
                             size_t numElements, size_t numRanks,
                             int op, bool* activeRanks, cudaStream_t stream) {
    reduceKernel<<<64, 256, 0, stream>>>(
        (mt_bfloat16*)dst, (const mt_bfloat16*)src,
        numElements, numRanks, op, activeRanks);
}

#undef DEF_LAUNCH_REDUCE

}  // namespace mooncake
#endif  // __MUSA__
