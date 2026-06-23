// mooncake_worker.cu — GPU kernel functions and launch wrappers.
// Compiled by nvcc (CUDA) and mcc (MUSA, via mooncake_worker.mu symlink).
// The __MUSA__ branch avoids torch headers to stay compatible with mcc.

#include <mooncake_worker_kernels.cuh>

#ifdef __MUSA__
#include <musa_bf16.h>
#endif

namespace mooncake {

// ── Kernel functions ──────────────────────────────────────────────
// Both CUDA and MUSA share the same kernel bodies. Parameters use plain
// C++ types (int instead of c10d::OpType / c10d::ReduceOp::RedOpType)
// so that mcc can compile them without torch headers.

__global__ void enqueueTaskKernel(int opType, size_t tensorSize,
                                  int64_t broadcastRoot, int bufferOffset,
                                  uint64_t submitSequence, void* meta,
                                  Task* tasks, int numRanks,
                                  const bool* activeRanks,
                                  int* activeRanksTensor,
                                  int* failedRanksHostPtr, size_t taskId) {
    // Copy task into slot
    tasks[taskId].opType = opType;
    tasks[taskId].tensorSize = tensorSize;
    tasks[taskId].broadcastRoot = broadcastRoot;
    tasks[taskId].bufferOffset = bufferOffset;
    tasks[taskId].submitSequence = submitSequence;
    tasks[taskId].transferGroupMeta = meta;
    tasks[taskId].failedRanksHost = failedRanksHostPtr;

    // Publish task metadata before notifying the host worker thread.
    __threadfence_system();
    tasks[taskId].active = true;

    // Spin-wait until CPU proxy sets DONE
    while (tasks[taskId].active) {
        __threadfence_system();
    }
    for (int i = 0; i < numRanks; ++i) {
        activeRanksTensor[i] = activeRanks[i] ? 1 : 0;
    }
}

template <typename scalar_t>
__global__ void reduceKernel(scalar_t* dst, const scalar_t* src,
                             size_t numElements, size_t numRanks, int op,
                             bool* activeRanks, int* failedRanks) {
    size_t thread_idx = blockIdx.x * blockDim.x + threadIdx.x;
    size_t stride = blockDim.x * gridDim.x;

#ifdef __MUSA__
    // mt_bfloat16 lacks arithmetic and comparison operators; use float
    // accumulator and fminf/fmaxf for that type only.  Other types
    // (float, double, int64_t, etc.) use their native accumulator to
    // preserve full precision.
    constexpr bool kIsBf16 = std::is_same_v<scalar_t, mt_bfloat16>;
#else
    constexpr bool kIsBf16 = false;
#endif
    using acc_t = std::conditional_t<kIsBf16, float, scalar_t>;

    for (size_t elem_idx = thread_idx; elem_idx < numElements;
         elem_idx += stride) {
        bool valid = false;
        acc_t acc = 0;
        for (size_t rank = 0; rank < numRanks; ++rank) {
            bool shouldInclude =
                activeRanks[rank] && (!failedRanks || failedRanks[rank] == 0);
            if (shouldInclude) {
                if (!valid) {
                    if constexpr (kIsBf16) {
                        acc = (float)src[rank * numElements + elem_idx];
                    } else {
                        acc = src[rank * numElements + elem_idx];
                    }
                    valid = true;
                } else {
                    if constexpr (kIsBf16) {
                        float val = (float)src[rank * numElements + elem_idx];
                        switch (op) {
                            case 0:  // SUM
                                acc += val;
                                break;
                            case 2:  // PRODUCT
                                acc *= val;
                                break;
                            case 3:  // MIN
                                acc = fminf(acc, val);
                                break;
                            case 4:  // MAX
                                acc = fmaxf(acc, val);
                                break;
                            default:
                                break;
                        }
                    } else {
                        switch (op) {
                            case 0:  // SUM
                                acc += src[rank * numElements + elem_idx];
                                break;
                            case 2:  // PRODUCT
                                acc *= src[rank * numElements + elem_idx];
                                break;
                            case 3:  // MIN
                                acc = std::min(
                                    src[rank * numElements + elem_idx], acc);
                                break;
                            case 4:  // MAX
                                acc = std::max(
                                    src[rank * numElements + elem_idx], acc);
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
        }
        if constexpr (kIsBf16) {
            dst[elem_idx] = (scalar_t)acc;
        } else {
            dst[elem_idx] = acc;
        }
    }
}

}  // namespace mooncake

// ── Kernel launch wrappers ────────────────────────────────────────
// g++ cannot compile <<<>>> syntax, so these wrappers are compiled by
// the GPU compiler (nvcc/mcc) and provide plain C++ functions that the
// host code can call.

#ifdef __MUSA__
#include <musa_runtime.h>
#endif

namespace mooncake {

void launchEnqueueTaskKernel(int opType, size_t tensorSize,
                             int64_t broadcastRoot, int bufferOffset,
                             uint64_t submitSequence, void* meta, Task* tasks,
                             int numRanks, const bool* activeRanks,
                             int* activeRanksTensor, int* failedRanksHostPtr,
                             size_t taskId, cudaStream_t stream) {
    enqueueTaskKernel<<<1, 1, 0, stream>>>(
        opType, tensorSize, broadcastRoot, bufferOffset, submitSequence, meta,
        tasks, numRanks, activeRanks, activeRanksTensor, failedRanksHostPtr,
        taskId);
}

#define DEF_LAUNCH_REDUCE(scalar_t, suffix)                                   \
    void launchReduceKernel_##suffix(scalar_t* dst, const scalar_t* src,      \
                                     size_t numElements, size_t numRanks,     \
                                     int op, bool* activeRanks,               \
                                     int* failedRanks, cudaStream_t stream) { \
        reduceKernel<<<64, 256, 0, stream>>>(dst, src, numElements, numRanks, \
                                             op, activeRanks, failedRanks);   \
    }

DEF_LAUNCH_REDUCE(uint8_t, uint8)
DEF_LAUNCH_REDUCE(int8_t, int8)
DEF_LAUNCH_REDUCE(int16_t, int16)
DEF_LAUNCH_REDUCE(int, int32)
DEF_LAUNCH_REDUCE(int64_t, int64)
DEF_LAUNCH_REDUCE(float, float)
DEF_LAUNCH_REDUCE(double, double)
DEF_LAUNCH_REDUCE(bool, bool)

#undef DEF_LAUNCH_REDUCE

void launchReduceKernel_bf16(void* dst, const void* src, size_t numElements,
                             size_t numRanks, int op, bool* activeRanks,
                             int* failedRanks, cudaStream_t stream) {
#ifdef __MUSA__
    reduceKernel<<<64, 256, 0, stream>>>(
        (mt_bfloat16*)dst, (const mt_bfloat16*)src, numElements, numRanks, op,
        activeRanks, failedRanks);
#else
    reduceKernel<<<64, 256, 0, stream>>>(
        (at::BFloat16*)dst, (const at::BFloat16*)src, numElements, numRanks, op,
        activeRanks, failedRanks);
#endif
}

void preloadReduceKernels() {
#ifdef __MUSA__
    // MUSA: mcc has no cudaFuncGetAttributes, kernels are JIT-compiled on
    // first use.
#else
    // CUDA: preload kernels to avoid JIT compilation overhead on first use.
    auto preload = [](const char* name, auto kernel_ptr) {
        cudaFuncAttributes attr{};
        auto err = cudaFuncGetAttributes(&attr, kernel_ptr);
        TORCH_CHECK(err == cudaSuccess, "Failed to preload kernel ", name, ": ",
                    cudaGetErrorString(err));
    };
    preload("reduceKernel<uint8_t>",
            reinterpret_cast<const void*>(reduceKernel<uint8_t>));
    preload("reduceKernel<int8_t>",
            reinterpret_cast<const void*>(reduceKernel<int8_t>));
    preload("reduceKernel<int16_t>",
            reinterpret_cast<const void*>(reduceKernel<int16_t>));
    preload("reduceKernel<int>",
            reinterpret_cast<const void*>(reduceKernel<int>));
    preload("reduceKernel<int64_t>",
            reinterpret_cast<const void*>(reduceKernel<int64_t>));
    preload("reduceKernel<float>",
            reinterpret_cast<const void*>(reduceKernel<float>));
    preload("reduceKernel<double>",
            reinterpret_cast<const void*>(reduceKernel<double>));
    preload("reduceKernel<bool>",
            reinterpret_cast<const void*>(reduceKernel<bool>));
    preload("reduceKernel<BFloat16>",
            reinterpret_cast<const void*>(reduceKernel<at::BFloat16>));
#endif
}

}  // namespace mooncake
