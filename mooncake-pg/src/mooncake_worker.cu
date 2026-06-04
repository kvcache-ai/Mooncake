// mooncake_worker.cu — CUDA kernel functions and launch wrappers.
// Compiled by nvcc. Host code lives in mooncake_worker_host.cpp (shared
// with MUSA). Kernel functions use torch types (c10d::OpType, etc.)
// which are safe for nvcc but not for mcc.

#include <mooncake_worker.cuh>
#include <mooncake_worker_kernels.cuh>

// Kernel functions — compiled by nvcc (CUDA build only)
__global__ void enqueueTaskKernel(c10d::OpType opType, size_t tensorSize,
                                  int64_t broadcastRoot, int bufferOffset,
                                  uint64_t submitSequence, void* meta,
                                  Task* tasks, int numRanks,
                                  const bool* activeRanks,
                                  int* activeRanksTensor, size_t taskId) {
    // Copy task into slot
    tasks[taskId].opType = (int)opType;
    tasks[taskId].tensorSize = tensorSize;
    tasks[taskId].broadcastRoot = broadcastRoot;
    tasks[taskId].bufferOffset = bufferOffset;
    tasks[taskId].submitSequence = submitSequence;
    tasks[taskId].transferGroupMeta = meta;

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
                             size_t numElements, size_t numRanks,
                             c10d::ReduceOp::RedOpType op, bool* activeRanks) {
    size_t thread_idx = blockIdx.x * blockDim.x + threadIdx.x;
    size_t stride = blockDim.x * gridDim.x;
    for (size_t elem_idx = thread_idx; elem_idx < numElements;
         elem_idx += stride) {
        bool valid = false;
        scalar_t acc = 0;
        for (size_t rank = 0; rank < numRanks; ++rank) {
            if (activeRanks[rank]) {
                if (!valid) {
                    acc = src[rank * numElements + elem_idx];
                    valid = true;
                } else {
                    switch (op) {
                        case c10d::ReduceOp::SUM:
                            acc += src[rank * numElements + elem_idx];
                            break;
                        case c10d::ReduceOp::MIN:
                            acc = std::min(src[rank * numElements + elem_idx],
                                           acc);
                            break;
                        case c10d::ReduceOp::MAX:
                            acc = std::max(src[rank * numElements + elem_idx],
                                           acc);
                            break;
                        case c10d::ReduceOp::PRODUCT:
                            acc *= src[rank * numElements + elem_idx];
                            break;
                        default:
                            // never
                            break;
                    }
                }
            }
        }
        dst[elem_idx] = acc;
    }
}

// CUDA launch wrappers — compiled by nvcc, callable from g++ host code.
// These match the MUSA launch wrappers in mooncake_worker_kernels.cu.
// The shared host code (mooncake_worker_host.cpp) calls these uniformly.
namespace mooncake {

void launchEnqueueTaskKernel(int opType, size_t tensorSize,
                             int64_t broadcastRoot, int bufferOffset,
                             uint64_t submitSequence, void* meta,
                             Task* tasks, int numRanks,
                             const bool* activeRanks,
                             int* activeRanksTensor, size_t taskId,
                             cudaStream_t stream) {
    enqueueTaskKernel<<<1, 1, 0, stream>>>(
        (c10d::OpType)opType, tensorSize, broadcastRoot, bufferOffset,
        submitSequence, meta, tasks, numRanks, activeRanks,
        activeRanksTensor, taskId);
}

#define DEF_LAUNCH_REDUCE(scalar_t, suffix)                                    \
    void launchReduceKernel_##suffix(scalar_t* dst, const scalar_t* src,       \
                                    size_t numElements, size_t numRanks,       \
                                    int op, bool* activeRanks,                 \
                                    cudaStream_t stream) {                     \
        reduceKernel<<<64, 256, 0, stream>>>(                                  \
            dst, src, numElements, numRanks,                                   \
            (c10d::ReduceOp::RedOpType)op, activeRanks);                       \
    }

DEF_LAUNCH_REDUCE(uint8_t, uint8)
DEF_LAUNCH_REDUCE(int8_t, int8)
DEF_LAUNCH_REDUCE(int16_t, int16)
DEF_LAUNCH_REDUCE(int, int32)
DEF_LAUNCH_REDUCE(int64_t, int64)
DEF_LAUNCH_REDUCE(float, float)
DEF_LAUNCH_REDUCE(double, double)
DEF_LAUNCH_REDUCE(bool, bool)

void launchReduceKernel_bf16(void* dst, const void* src,
                             size_t numElements, size_t numRanks,
                             int op, bool* activeRanks, cudaStream_t stream) {
    reduceKernel<<<64, 256, 0, stream>>>(
        (at::BFloat16*)dst, (const at::BFloat16*)src,
        numElements, numRanks, (c10d::ReduceOp::RedOpType)op, activeRanks);
}

#undef DEF_LAUNCH_REDUCE

void preloadReduceKernels() {
    // CUDA: preload kernels to avoid JIT compilation overhead on first use.
    // MUSA preloading is a no-op in mooncake_worker_host.cpp (mcc has no
    // cudaFuncGetAttributes).
    auto preload = [](const char* name, auto kernel_ptr) {
        cudaFuncAttributes attr{};
        auto err = cudaFuncGetAttributes(&attr, kernel_ptr);
        TORCH_CHECK(err == cudaSuccess, "Failed to preload kernel ", name,
                    ": ", cudaGetErrorString(err));
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
}

}  // namespace mooncake
