// mooncake_worker.cu — GPU kernel functions and launch wrappers.
// Compiled by nvcc (CUDA) and mcc (MUSA, via mooncake_worker.mu symlink).
// The __MUSA__ branch avoids torch headers to stay compatible with mcc.

#include <mooncake_worker_kernels.cuh>
#include <transport/device/comm_device.cuh>

#include <algorithm>
#include <cstdio>
#include <cstdlib>

#ifdef __MUSA__
#include <musa_bf16.h>
#endif

namespace mooncake {

using mooncake::device::CommCtx;
using mooncake::device::make_comm_ctx;
using mooncake::device::mc_rdma_put;
using mooncake::device::mc_route_put;
using mooncake::device::mc_signal;
using mooncake::device::mc_signal_write;
using mooncake::device::mc_st_release;

static int p2pAllgatherBlockCap() {
    const char* value = std::getenv("MOONCAKE_PG_AG_BLOCKS");
    if (!value || value[0] == '\0') return 512;
    char* end = nullptr;
    long parsed = std::strtol(value, &end, 10);
    if (end == value || parsed <= 0) return 512;
    if (parsed > 512) return 512;
    return static_cast<int>(parsed);
}

static int clampP2pAllgatherBlocks(int blocks) {
    const int cap = p2pAllgatherBlockCap();
    return blocks < 1 ? 1 : (blocks > cap ? cap : blocks);
}

static int p2pEnvBlockCap(const char* primary, const char* fallback) {
    const char* value = std::getenv(primary);
    if ((!value || value[0] == '\0') && fallback != nullptr) {
        value = std::getenv(fallback);
    }
    if (!value || value[0] == '\0') return 512;
    char* end = nullptr;
    long parsed = std::strtol(value, &end, 10);
    if (end == value || parsed <= 0) return 512;
    if (parsed > 512) return 512;
    return static_cast<int>(parsed);
}

static int clampP2pReduceScatterStoreBlocks(int blocks) {
    const int cap = p2pEnvBlockCap("MOONCAKE_PG_RS_STORE_BLOCKS",
                                   "MOONCAKE_PG_RS_BLOCKS");
    return blocks < 1 ? 1 : (blocks > cap ? cap : blocks);
}

static int clampP2pReduceScatterReduceBlocks(int blocks) {
    const int cap = p2pEnvBlockCap("MOONCAKE_PG_RS_REDUCE_BLOCKS",
                                   "MOONCAKE_PG_RS_BLOCKS");
    return blocks < 1 ? 1 : (blocks > cap ? cap : blocks);
}

static bool useParallelProduceWait() {
    const char* value = std::getenv("MOONCAKE_PG_PARALLEL_PRODUCE_WAIT");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useAllgatherParallelProduceWait() {
    const char* value = std::getenv("MOONCAKE_PG_AG_PARALLEL_PRODUCE_WAIT");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool useDeviceApiCollectives() {
    return false;
}

static bool deviceApiStageSyncEnabled() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_API_STAGE_SYNC");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool deviceApiSignalDebugEnabled() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_API_SIGNAL_DEBUG");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool deviceApiCompletionPollEnabled() {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_API_COMPLETION_POLL");
    return value && value[0] != '\0' && value[0] != '0';
}

static bool deviceApiProfileEnabledForRank(int rank) {
    const char* value = std::getenv("MOONCAKE_PG_DEVICE_API_PROFILE");
    if (!value || value[0] == '\0' || value[0] == '0') return false;
    const char* rank_value = std::getenv("MOONCAKE_PG_DEVICE_API_PROFILE_RANK");
    if (!rank_value || rank_value[0] == '\0') return true;
    char* end = nullptr;
    long parsed = std::strtol(rank_value, &end, 10);
    return end != rank_value && static_cast<int>(parsed) == rank;
}

static void maybeDeviceApiStageSync(const char* op, const char* stage, int rank,
                                    uint32_t chunkIndex,
                                    cudaStream_t stream) {
    if (!deviceApiStageSyncEnabled()) return;
    std::fprintf(stderr,
                 "MOONCAKE_PG_DEVICE_API_STAGE_BEGIN op=%s stage=%s rank=%d "
                 "chunk=%u\n",
                 op, stage, rank, chunkIndex);
    cudaError_t err = cudaStreamSynchronize(stream);
    std::fprintf(stderr,
                 "MOONCAKE_PG_DEVICE_API_STAGE_%s op=%s stage=%s rank=%d "
                 "chunk=%u err=%d %s\n",
                 err == cudaSuccess ? "DONE" : "ERROR", op, stage, rank,
                 chunkIndex, static_cast<int>(err), cudaGetErrorString(err));
}

// ── Kernel functions ──────────────────────────────────────────────
// Both CUDA and MUSA share the same kernel bodies. Parameters use plain
// C++ types (int instead of c10d::OpType / c10d::ReduceOp::RedOpType)
// so that mcc can compile them without torch headers.

__global__ void enqueueTaskKernel(int opType, size_t tensorSize,
                                  int64_t broadcastRoot, int bufferOffset,
                                  uint64_t submitSequence, void* meta,
                                  Task* tasks, int numRanks,
                                  const bool* activeRanks,
                                  int* activeRanksTensor, size_t taskId) {
    // Copy task into slot
    tasks[taskId].opType = opType;
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
                             size_t numElements, size_t numRanks, int op,
                             bool* activeRanks) {
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
            if (activeRanks[rank]) {
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
                             int* activeRanksTensor, size_t taskId,
                             cudaStream_t stream) {
    enqueueTaskKernel<<<1, 1, 0, stream>>>(
        opType, tensorSize, broadcastRoot, bufferOffset, submitSequence, meta,
        tasks, numRanks, activeRanks, activeRanksTensor, taskId);
}

#define DEF_LAUNCH_REDUCE(scalar_t, suffix)                                   \
    void launchReduceKernel_##suffix(                                         \
        scalar_t* dst, const scalar_t* src, size_t numElements,               \
        size_t numRanks, int op, bool* activeRanks, cudaStream_t stream) {    \
        reduceKernel<<<64, 256, 0, stream>>>(dst, src, numElements, numRanks, \
                                             op, activeRanks);                \
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

__global__ void p2pAllgatherBaseCopyKernel(const char* input, void** peer_ptrs,
                                           const int32_t* available,
                                           size_t tensorSize, int rank,
                                           int numRanks) {
    size_t total = tensorSize * static_cast<size_t>(numRanks);
    size_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    size_t stride = blockDim.x * gridDim.x;
    for (size_t idx = tid; idx < total; idx += stride) {
        int peer = static_cast<int>(idx / tensorSize);
        if (!available[peer]) continue;
        size_t off = idx - static_cast<size_t>(peer) * tensorSize;
        char* peer_base = static_cast<char*>(peer_ptrs[peer]);
        peer_base[static_cast<size_t>(rank) * tensorSize + off] = input[off];
    }
}

__global__ void p2pAllgatherBaseSignalWaitKernel(void** peer_ptrs,
                                                 const int32_t* available,
                                                 void* local_recv_base,
                                                 int rank, int numRanks,
                                                 uint32_t sequence,
                                                 size_t signalOffset) {
    if (threadIdx.x == 0 && blockIdx.x == 0) {
        __threadfence_system();
        for (int peer = 0; peer < numRanks; ++peer) {
            if (!available[peer]) continue;
            char* peer_base = static_cast<char*>(peer_ptrs[peer]);
            uint32_t* peer_signal =
                reinterpret_cast<uint32_t*>(peer_base + signalOffset);
#ifdef __MUSA__
            peer_signal[rank] = sequence;
#else
            atomicExch_system(
                reinterpret_cast<unsigned int*>(peer_signal + rank), sequence);
#endif
        }
        __threadfence_system();

        uint32_t* local_signal = reinterpret_cast<uint32_t*>(
            static_cast<char*>(local_recv_base) + signalOffset);
        bool done = false;
        while (!done) {
            done = true;
            for (int peer = 0; peer < numRanks; ++peer) {
                if (!available[peer]) continue;
#ifdef __MUSA__
                uint32_t value = local_signal[peer];
#else
                uint32_t value = atomicAdd(
                    reinterpret_cast<unsigned int*>(local_signal + peer), 0);
#endif
                if (value < sequence) {
                    done = false;
                    break;
                }
            }
        }
    }
}

void launchP2pAllgatherBaseKernel(void* input, void* output,
                                  void* local_recv_base, void** peer_ptrs,
                                  int32_t* available, size_t tensorSize,
                                  int rank, int numRanks, uint32_t sequence,
                                  cudaStream_t stream) {
    constexpr size_t kProduceSignalOffset =
        kBufferSize - 2 * kMaxNumRanks * sizeof(uint32_t);
    constexpr size_t kConsumeSignalOffset =
        kBufferSize - kMaxNumRanks * sizeof(uint32_t);
    size_t total = tensorSize * static_cast<size_t>(numRanks);
    int threads = 256;
    int blocks = static_cast<int>((total + threads - 1) / threads);
    blocks = blocks < 1 ? 1 : (blocks > 1024 ? 1024 : blocks);
    p2pAllgatherBaseCopyKernel<<<blocks, threads, 0, stream>>>(
        static_cast<const char*>(input), peer_ptrs, available, tensorSize, rank,
        numRanks);
    p2pAllgatherBaseSignalWaitKernel<<<1, 1, 0, stream>>>(
        peer_ptrs, available, local_recv_base, rank, numRanks, sequence,
        kProduceSignalOffset);
    cudaMemcpyAsync(output, local_recv_base,
                    tensorSize * static_cast<size_t>(numRanks),
                    cudaMemcpyDeviceToDevice, stream);
    p2pAllgatherBaseSignalWaitKernel<<<1, 1, 0, stream>>>(
        peer_ptrs, available, local_recv_base, rank, numRanks, sequence,
        kConsumeSignalOffset);
}

void launchP2pAllgatherBaseCopyKernel(void* input, void** peer_ptrs,
                                      int32_t* available, size_t tensorSize,
                                      int rank, int numRanks,
                                      cudaStream_t stream) {
    size_t total = tensorSize * static_cast<size_t>(numRanks);
    int threads = 256;
    int blocks = static_cast<int>((total + threads - 1) / threads);
    blocks = blocks < 1 ? 1 : (blocks > 1024 ? 1024 : blocks);
    p2pAllgatherBaseCopyKernel<<<blocks, threads, 0, stream>>>(
        static_cast<const char*>(input), peer_ptrs, available, tensorSize, rank,
        numRanks);
}

void launchP2pAllgatherBaseFinishKernel(void* output, void* local_recv_base,
                                        void** peer_ptrs, int32_t* available,
                                        size_t tensorSize, int rank,
                                        int numRanks, uint32_t sequence,
                                        cudaStream_t stream) {
    constexpr size_t kProduceSignalOffset =
        kBufferSize - 2 * kMaxNumRanks * sizeof(uint32_t);
    constexpr size_t kConsumeSignalOffset =
        kBufferSize - kMaxNumRanks * sizeof(uint32_t);
    p2pAllgatherBaseSignalWaitKernel<<<1, 1, 0, stream>>>(
        peer_ptrs, available, local_recv_base, rank, numRanks, sequence,
        kProduceSignalOffset);
    cudaMemcpyAsync(output, local_recv_base,
                    tensorSize * static_cast<size_t>(numRanks),
                    cudaMemcpyDeviceToDevice, stream);
    p2pAllgatherBaseSignalWaitKernel<<<1, 1, 0, stream>>>(
        peer_ptrs, available, local_recv_base, rank, numRanks, sequence,
        kConsumeSignalOffset);
}

__device__ __forceinline__ void p2pAgCopyBytes(char* dst, const char* src,
                                               size_t bytes) {
    size_t tid = threadIdx.x;
    size_t stride = blockDim.x;
    size_t words = bytes / sizeof(uint64_t);
    auto* dst64 = reinterpret_cast<uint64_t*>(dst);
    const auto* src64 = reinterpret_cast<const uint64_t*>(src);
    for (size_t i = tid; i < words; i += stride) {
        dst64[i] = src64[i];
    }
    for (size_t i = words * sizeof(uint64_t) + tid; i < bytes; i += stride) {
        dst[i] = src[i];
    }
}

__device__ __forceinline__ void p2pAgCopyBytesSlice(char* dst, const char* src,
                                                    size_t offset,
                                                    size_t bytes) {
    char* slice_dst = dst + offset;
    const char* slice_src = src + offset;
    uintptr_t dst_addr = reinterpret_cast<uintptr_t>(slice_dst);
    uintptr_t src_addr = reinterpret_cast<uintptr_t>(slice_src);
    if (((dst_addr | src_addr | bytes) & (sizeof(uint64_t) - 1)) == 0) {
        p2pAgCopyBytes(slice_dst, slice_src, bytes);
        return;
    }

    size_t tid = threadIdx.x;
    size_t stride = blockDim.x;
    for (size_t i = tid; i < bytes; i += stride) {
        slice_dst[i] = slice_src[i];
    }
}

__device__ __forceinline__ void p2pAgSignalStore(uint32_t* ptr, uint32_t value,
                                                 int signalMode) {
#ifdef __MUSA__
    ptr[0] = value;
#else
    if (signalMode == 1) {
        asm volatile("st.global.release.sys.u32 [%0], %1;"
                     :
                     : "l"(ptr), "r"(value)
                     : "memory");
    } else if (signalMode == 2) {
        asm volatile("st.global.relaxed.sys.u32 [%0], %1;"
                     :
                     : "l"(ptr), "r"(value)
                     : "memory");
    } else {
        atomicExch_system(reinterpret_cast<unsigned int*>(ptr), value);
    }
#endif
}

__device__ __forceinline__ uint32_t p2pAgSignalLoad(uint32_t* ptr,
                                                    int signalMode) {
#ifdef __MUSA__
    return ptr[0];
#else
    if (signalMode == 1) {
        uint32_t value;
        asm volatile("ld.global.acquire.sys.u32 %0, [%1];"
                     : "=r"(value)
                     : "l"(ptr)
                     : "memory");
        return value;
    } else if (signalMode == 2) {
        return reinterpret_cast<volatile uint32_t*>(ptr)[0];
    }
    return atomicAdd(reinterpret_cast<unsigned int*>(ptr), 0);
#endif
}

__global__ void p2pAllgatherRingKernel(const char* input, char* output,
                                       char* local_recv_base, void** peer_ptrs,
                                       const int32_t* available,
                                       size_t tensorSize, int rank,
                                       int numRanks, uint32_t sequence,
                                       int signalMode, int numChannels) {
    const int channel = blockIdx.x;
    if (channel >= numChannels) return;

    const size_t signalSlots =
        static_cast<size_t>(numChannels) * kMaxNumRanks;
    const size_t kProduceSignalOffset =
        kBufferSize - 2 * signalSlots * sizeof(uint32_t);
    const size_t kConsumeSignalOffset =
        kBufferSize - signalSlots * sizeof(uint32_t);
    const size_t channelStart =
        (tensorSize * static_cast<size_t>(channel)) /
        static_cast<size_t>(numChannels);
    const size_t channelEnd =
        (tensorSize * static_cast<size_t>(channel + 1)) /
        static_cast<size_t>(numChannels);
    const size_t channelBytes = channelEnd - channelStart;
    const int prev = (rank + numRanks - 1) % numRanks;
    const int next = (rank + 1) % numRanks;
    if (!available[prev] || !available[next]) return;

    auto* produce = reinterpret_cast<uint32_t*>(local_recv_base +
                                                kProduceSignalOffset);
    auto* consume = reinterpret_cast<uint32_t*>(local_recv_base +
                                                kConsumeSignalOffset);
    char* next_base = static_cast<char*>(peer_ptrs[next]);
    auto* next_produce = reinterpret_cast<uint32_t*>(next_base +
                                                     kProduceSignalOffset);
    char* prev_base = static_cast<char*>(peer_ptrs[prev]);
    auto* prev_consume = reinterpret_cast<uint32_t*>(prev_base +
                                                     kConsumeSignalOffset);
    const size_t signalLane =
        static_cast<size_t>(channel) * kMaxNumRanks + static_cast<size_t>(rank);
    const size_t prevSignalLane =
        static_cast<size_t>(channel) * kMaxNumRanks + static_cast<size_t>(prev);
    const size_t nextSignalLane =
        static_cast<size_t>(channel) * kMaxNumRanks + static_cast<size_t>(next);

    // Seed the local scratch/output with this rank's own chunk.
    p2pAgCopyBytesSlice(
        local_recv_base + static_cast<size_t>(rank) * tensorSize, input,
        channelStart, channelBytes);
    p2pAgCopyBytesSlice(output + static_cast<size_t>(rank) * tensorSize, input,
                        channelStart, channelBytes);
    __syncthreads();

    // NCCL-like ring: each step forwards one rank chunk to the next peer.  A
    // per-neighbor monotonically increasing step value replaces the coarse
    // all-rank host/shm barrier used by earlier PoCs.
    for (int step = 0; step < numRanks - 1; ++step) {
        const int send_chunk = (rank - step + numRanks) % numRanks;
        const int recv_chunk = (rank - step - 1 + numRanks) % numRanks;
        const uint32_t step_seq =
            sequence * static_cast<uint32_t>(kMaxNumRanks * numChannels) +
            static_cast<uint32_t>(step * numChannels + channel + 1);

        p2pAgCopyBytesSlice(
            next_base + static_cast<size_t>(send_chunk) * tensorSize,
            local_recv_base + static_cast<size_t>(send_chunk) * tensorSize,
            channelStart, channelBytes);
        __syncthreads();
        if (threadIdx.x == 0) {
            if (signalMode == 2) __threadfence_system();
            p2pAgSignalStore(next_produce + signalLane, step_seq,
                             signalMode);
        }
        __syncthreads();
        if (threadIdx.x == 0) {
            uint32_t value = 0;
            do {
                value = p2pAgSignalLoad(produce + prevSignalLane,
                                        signalMode);
            } while (value < step_seq);
        }
        __syncthreads();
        p2pAgCopyBytesSlice(
            output + static_cast<size_t>(recv_chunk) * tensorSize,
            local_recv_base + static_cast<size_t>(recv_chunk) * tensorSize,
            channelStart, channelBytes);
        __syncthreads();
    }

    // Keep peer scratch from being overwritten by the next invocation before
    // the rank that owns that scratch has copied all chunks to user output.
    // Each rank writes data into next's scratch, so it waits for next's ack;
    // each rank receives data from prev, so it sends the ack to prev.
    if (threadIdx.x == 0) {
        if (signalMode == 2) __threadfence_system();
        p2pAgSignalStore(prev_consume + signalLane, sequence, signalMode);
        uint32_t value = 0;
        do {
            value = p2pAgSignalLoad(consume + nextSignalLane, signalMode);
        } while (value < sequence);
    }
}

void launchP2pAllgatherRingKernel(void* input, void* output,
                                  void* local_recv_base, void** peer_ptrs,
                                  int32_t* available, size_t tensorSize,
                                  int rank, int numRanks, uint32_t sequence,
                                  int signalMode, int numChannels,
                                  cudaStream_t stream) {
    p2pAllgatherRingKernel<<<numChannels, 256, 0, stream>>>(
        static_cast<const char*>(input), static_cast<char*>(output),
        static_cast<char*>(local_recv_base), peer_ptrs, available, tensorSize,
        rank, numRanks, sequence, signalMode, numChannels);
}

__device__ __forceinline__ void p2pRingCopyFloatSlice(
    float* dst, const float* src, size_t start, size_t count) {
    const size_t tid = threadIdx.x;
    const size_t stride = blockDim.x;
    for (size_t i = tid; i < count; i += stride) {
        dst[start + i] = src[start + i];
    }
}

__device__ __forceinline__ void p2pRingAddFloatSlice(
    float* dst, const float* src, size_t start, size_t count) {
    const size_t tid = threadIdx.x;
    const size_t stride = blockDim.x;
    for (size_t i = tid; i < count; i += stride) {
        dst[start + i] += src[start + i];
    }
}

__global__ void p2pAllReduceRingFloatKernel(
    float* tensor, char* local_recv_base, void** peer_ptrs,
    const int32_t* available, size_t numElements, int rank, int numRanks,
    uint32_t hostSequence, const uint32_t* sequenceSlot, int signalMode,
    int numChannels) {
    const int channel = blockIdx.x;
    if (channel >= numChannels || numRanks <= 1) return;

    const int prev = (rank + numRanks - 1) % numRanks;
    const int next = (rank + 1) % numRanks;
    if (!available[prev] || !available[next]) return;

    const uint32_t sequence = sequenceSlot ? *sequenceSlot : hostSequence;
    const size_t segmentElements =
        numElements / static_cast<size_t>(numRanks);
    const size_t tensorBytes = numElements * sizeof(float);
    const size_t channelStart =
        (segmentElements * static_cast<size_t>(channel)) /
        static_cast<size_t>(numChannels);
    const size_t channelEnd =
        (segmentElements * static_cast<size_t>(channel + 1)) /
        static_cast<size_t>(numChannels);
    const size_t channelElements = channelEnd - channelStart;
    if (channelElements == 0) return;

    float* accum = reinterpret_cast<float*>(local_recv_base);
    float* recv = reinterpret_cast<float*>(local_recv_base + tensorBytes);
    char* next_base = static_cast<char*>(peer_ptrs[next]);
    float* next_recv = reinterpret_cast<float*>(next_base + tensorBytes);
    auto* produce = reinterpret_cast<uint32_t*>(
        local_recv_base + kBufferSize -
        2 * static_cast<size_t>(numChannels) * kMaxNumRanks *
            sizeof(uint32_t));
    auto* consume = reinterpret_cast<uint32_t*>(
        local_recv_base + kBufferSize -
        static_cast<size_t>(numChannels) * kMaxNumRanks * sizeof(uint32_t));
    auto* next_produce = reinterpret_cast<uint32_t*>(
        next_base + kBufferSize -
        2 * static_cast<size_t>(numChannels) * kMaxNumRanks *
            sizeof(uint32_t));
    char* prev_base = static_cast<char*>(peer_ptrs[prev]);
    auto* prev_consume = reinterpret_cast<uint32_t*>(
        prev_base + kBufferSize -
        static_cast<size_t>(numChannels) * kMaxNumRanks * sizeof(uint32_t));
    const size_t signalLane =
        static_cast<size_t>(channel) * kMaxNumRanks + static_cast<size_t>(rank);
    const size_t prevSignalLane =
        static_cast<size_t>(channel) * kMaxNumRanks + static_cast<size_t>(prev);
    const size_t nextSignalLane =
        static_cast<size_t>(channel) * kMaxNumRanks + static_cast<size_t>(next);
    const uint32_t phaseStride =
        static_cast<uint32_t>(2 * kMaxNumRanks * numChannels + 1);
    const uint32_t baseSeq = sequence * phaseStride;

    for (int chunk = 0; chunk < numRanks; ++chunk) {
        const size_t chunkBase = static_cast<size_t>(chunk) * segmentElements;
        p2pRingCopyFloatSlice(accum + chunkBase, tensor + chunkBase,
                              channelStart, channelElements);
    }
    __syncthreads();

    for (int step = 0; step < numRanks - 1; ++step) {
        const int sendChunk = (rank - step + numRanks) % numRanks;
        const int recvChunk = (rank - step - 1 + numRanks) % numRanks;
        const size_t sendBase =
            static_cast<size_t>(sendChunk) * segmentElements;
        const size_t recvBase =
            static_cast<size_t>(recvChunk) * segmentElements;
        const uint32_t stepSeq =
            baseSeq + static_cast<uint32_t>(step * numChannels + channel + 1);

        p2pRingCopyFloatSlice(next_recv + sendBase, accum + sendBase,
                              channelStart, channelElements);
        __syncthreads();
        if (threadIdx.x == 0) {
            if (signalMode == 2) __threadfence_system();
            p2pAgSignalStore(next_produce + signalLane, stepSeq, signalMode);
        }
        __syncthreads();
        if (threadIdx.x == 0) {
            uint32_t value = 0;
            do {
                value = p2pAgSignalLoad(produce + prevSignalLane, signalMode);
            } while (value < stepSeq);
        }
        __syncthreads();
        p2pRingAddFloatSlice(accum + recvBase, recv + recvBase, channelStart,
                             channelElements);
        __syncthreads();
    }

    const int ownedChunk = (rank + 1) % numRanks;
    p2pRingCopyFloatSlice(
        tensor + static_cast<size_t>(ownedChunk) * segmentElements,
        accum + static_cast<size_t>(ownedChunk) * segmentElements,
        channelStart, channelElements);
    __syncthreads();

    for (int step = 0; step < numRanks - 1; ++step) {
        const int sendChunk = (rank + 1 - step + numRanks) % numRanks;
        const int recvChunk = (rank - step + numRanks) % numRanks;
        const size_t sendBase =
            static_cast<size_t>(sendChunk) * segmentElements;
        const size_t recvBase =
            static_cast<size_t>(recvChunk) * segmentElements;
        const uint32_t stepSeq =
            baseSeq + static_cast<uint32_t>((numRanks - 1 + step) *
                                                numChannels +
                                            channel + 1);

        p2pRingCopyFloatSlice(next_recv + sendBase, accum + sendBase,
                              channelStart, channelElements);
        __syncthreads();
        if (threadIdx.x == 0) {
            if (signalMode == 2) __threadfence_system();
            p2pAgSignalStore(next_produce + signalLane, stepSeq, signalMode);
        }
        __syncthreads();
        if (threadIdx.x == 0) {
            uint32_t value = 0;
            do {
                value = p2pAgSignalLoad(produce + prevSignalLane, signalMode);
            } while (value < stepSeq);
        }
        __syncthreads();
        p2pRingCopyFloatSlice(accum + recvBase, recv + recvBase, channelStart,
                              channelElements);
        p2pRingCopyFloatSlice(tensor + recvBase, recv + recvBase,
                              channelStart, channelElements);
        __syncthreads();
    }

    if (threadIdx.x == 0) {
        if (signalMode == 2) __threadfence_system();
        p2pAgSignalStore(prev_consume + signalLane, sequence, signalMode);
        uint32_t value = 0;
        do {
            value = p2pAgSignalLoad(consume + nextSignalLane, signalMode);
        } while (value < sequence);
    }
}

void launchP2pAllReduceRingKernel_float(
    float* tensor, void* local_recv_base, void** peer_ptrs, int32_t* available,
    size_t numElements, int rank, int numRanks, uint32_t sequence,
    int signalMode, int numChannels, cudaStream_t stream) {
    p2pAllReduceRingFloatKernel<<<numChannels, 256, 0, stream>>>(
        tensor, static_cast<char*>(local_recv_base), peer_ptrs, available,
        numElements, rank, numRanks, sequence, nullptr, signalMode,
        numChannels);
}

void launchP2pAllReduceRingGraphKernel_float(
    float* tensor, void* local_recv_base, void** peer_ptrs, int32_t* available,
    size_t numElements, int rank, int numRanks, const uint32_t* sequenceSlot,
    int signalMode, int numChannels, cudaStream_t stream) {
    p2pAllReduceRingFloatKernel<<<numChannels, 256, 0, stream>>>(
        tensor, static_cast<char*>(local_recv_base), peer_ptrs, available,
        numElements, rank, numRanks, 0, sequenceSlot, signalMode, numChannels);
}

__global__ void p2pAllgatherFifoRingKernel(const char* input, char* output,
                                           char* local_recv_base,
                                           void** peer_ptrs,
                                           const int32_t* available,
                                           size_t tensorSize, int rank,
                                           int numRanks, uint32_t sequence,
                                           int signalMode,
                                           int numChannels, int fifoSlots) {
    const int channel = blockIdx.x;
    if (channel >= numChannels) return;

    const int prev = (rank + numRanks - 1) % numRanks;
    const int next = (rank + 1) % numRanks;
    if (!available[prev] || !available[next]) return;

    const size_t channelStart =
        (tensorSize * static_cast<size_t>(channel)) /
        static_cast<size_t>(numChannels);
    const size_t channelEnd =
        (tensorSize * static_cast<size_t>(channel + 1)) /
        static_cast<size_t>(numChannels);
    const size_t channelBytes = channelEnd - channelStart;
    const size_t channelStride =
        (tensorSize + static_cast<size_t>(numChannels) - 1) /
        static_cast<size_t>(numChannels);
    const size_t dataBytes = static_cast<size_t>(numChannels) *
                             static_cast<size_t>(fifoSlots) * channelStride;
    const size_t signalSlots =
        static_cast<size_t>(numChannels) * static_cast<size_t>(fifoSlots);
    const size_t produceSignalOffset =
        kBufferSize - 2 * signalSlots * sizeof(uint32_t);
    const size_t consumeSignalOffset =
        kBufferSize - signalSlots * sizeof(uint32_t);
    if (dataBytes > produceSignalOffset) return;

    const size_t signalLane =
        static_cast<size_t>(channel) * static_cast<size_t>(fifoSlots);
    char* next_base = static_cast<char*>(peer_ptrs[next]);
    auto* produce = reinterpret_cast<uint32_t*>(local_recv_base +
                                                produceSignalOffset);
    auto* consume = reinterpret_cast<uint32_t*>(local_recv_base +
                                                consumeSignalOffset);
    auto* next_produce = reinterpret_cast<uint32_t*>(next_base +
                                                     produceSignalOffset);
    auto* next_consume = reinterpret_cast<uint32_t*>(next_base +
                                                     consumeSignalOffset);

    p2pAgCopyBytesSlice(output + static_cast<size_t>(rank) * tensorSize, input,
                        channelStart, channelBytes);
    __syncthreads();

    for (int step = 0; step < numRanks - 1; ++step) {
        const int send_chunk = (rank - step + numRanks) % numRanks;
        const int recv_chunk = (rank - step - 1 + numRanks) % numRanks;
        const int slot = step % fifoSlots;
        const size_t slotLane = signalLane + static_cast<size_t>(slot);

        if (threadIdx.x == 0 && sequence > 1) {
            uint32_t value = 0;
            do {
                value = p2pAgSignalLoad(next_consume + slotLane,
                                        signalMode);
            } while (value < sequence - 1);
        }
        __syncthreads();

        const size_t slotOffset =
            (static_cast<size_t>(channel) * static_cast<size_t>(fifoSlots) +
             static_cast<size_t>(slot)) *
            channelStride;
        char* next_fifo = next_base + slotOffset;
        if (step == 0) {
            p2pAgCopyBytes(next_fifo, input + channelStart, channelBytes);
        } else {
            const int prevSlot = (step - 1) % fifoSlots;
            const size_t prevSlotOffset =
                (static_cast<size_t>(channel) * static_cast<size_t>(fifoSlots) +
                 static_cast<size_t>(prevSlot)) *
                channelStride;
            char* local_fifo = local_recv_base + prevSlotOffset;
            p2pAgCopyBytes(next_fifo, local_fifo, channelBytes);
        }
        __syncthreads();
        if (threadIdx.x == 0) {
            if (signalMode == 2) __threadfence_system();
            p2pAgSignalStore(next_produce + slotLane, sequence, signalMode);
        }

        if (threadIdx.x == 0) {
            uint32_t value = 0;
            do {
                value = p2pAgSignalLoad(produce + slotLane, signalMode);
            } while (value < sequence);
        }
        __syncthreads();

        char* local_fifo = local_recv_base + slotOffset;
        p2pAgCopyBytes(output + static_cast<size_t>(recv_chunk) * tensorSize +
                           channelStart,
                       local_fifo, channelBytes);
        __syncthreads();
        if (threadIdx.x == 0) {
            if (signalMode == 2) __threadfence_system();
            p2pAgSignalStore(consume + slotLane, sequence, signalMode);
        }
        __syncthreads();
    }
}

void launchP2pAllgatherFifoRingKernel(void* input, void* output,
                                      void* local_recv_base, void** peer_ptrs,
                                      int32_t* available, size_t tensorSize,
                                      int rank, int numRanks,
                                      uint32_t sequence, int signalMode,
                                      int numChannels, int fifoSlots,
                                      cudaStream_t stream) {
    p2pAllgatherFifoRingKernel<<<numChannels, 256, 0, stream>>>(
        static_cast<const char*>(input), static_cast<char*>(output),
        static_cast<char*>(local_recv_base), peer_ptrs, available, tensorSize,
        rank, numRanks, sequence, signalMode, numChannels, fifoSlots);
}

__global__ void p2pAllgatherStoreWaitPrevConsumeKernel(
    char* local_recv_base, size_t consumeSignalOffset, int numRanks,
    uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0 || sequence <= 1) return;
    auto* consume = reinterpret_cast<volatile uint32_t*>(local_recv_base +
                                                         consumeSignalOffset);
    const uint32_t previous = sequence - 1;
    for (int peer = 0; peer < numRanks; ++peer) {
        while (consume[peer] < previous) {
        }
    }
}

__global__ void p2pAllgatherStoreToScratchKernel(const char* input,
                                                 void** peer_ptrs,
                                                 const int32_t* available,
                                                 size_t tensorSize, int rank,
                                                 int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;

    if (((reinterpret_cast<uintptr_t>(input) | tensorSize) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(input);
        const size_t words = tensorSize / sizeof(uint64_t);
        for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
            if (!available[peer]) continue;
            auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
            auto* dst = peer_base + static_cast<size_t>(rank) * tensorSize;
            if ((reinterpret_cast<uintptr_t>(dst) & (sizeof(uint64_t) - 1)) ==
                0) {
                auto* dst64 = reinterpret_cast<uint64_t*>(dst);
                for (size_t i = tid; i < words; i += stride) {
                    dst64[i] = src64[i];
                }
            } else {
                for (size_t i = tid; i < tensorSize; i += stride) {
                    dst[i] = input[i];
                }
            }
        }
        return;
    }

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* dst = peer_base + static_cast<size_t>(rank) * tensorSize;
        for (size_t i = tid; i < tensorSize; i += stride) {
            dst[i] = input[i];
        }
    }
}

__global__ void p2pAllgatherStoreProduceWaitKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, int rank, int numRanks, uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* peerProduce = reinterpret_cast<uint32_t*>(peer_base +
                                                        produceSignalOffset);
        p2pAgSignalStore(peerProduce + rank, sequence, 2);
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<volatile uint32_t*>(local_recv_base +
                                                         produceSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        while (produce[peer] < sequence) {
        }
    }
}

__global__ void p2pAllgatherStoreConsumeKernel(
    void** peer_ptrs, const int32_t* available, size_t consumeSignalOffset,
    int rank, int numRanks, uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* peerConsume = reinterpret_cast<uint32_t*>(peer_base +
                                                        consumeSignalOffset);
        p2pAgSignalStore(peerConsume + rank, sequence, 2);
    }
}

__global__ void p2pAllgatherStoreLocalCopyKernel(const char* local_recv_base,
                                                 char* output,
                                                 size_t bytes) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    if (((reinterpret_cast<uintptr_t>(local_recv_base) |
          reinterpret_cast<uintptr_t>(output) | bytes) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(local_recv_base);
        auto* dst64 = reinterpret_cast<uint64_t*>(output);
        const size_t words = bytes / sizeof(uint64_t);
        for (size_t i = tid; i < words; i += stride) {
            dst64[i] = src64[i];
        }
        return;
    }
    for (size_t i = tid; i < bytes; i += stride) {
        output[i] = local_recv_base[i];
    }
}

__global__ void p2pAllgatherStoreToSlottedScratchKernel(
    const char* input, void** peer_ptrs, const int32_t* available,
    size_t tensorSize, size_t slotStride, int slot, int rank, int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;

    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;
    if (((reinterpret_cast<uintptr_t>(input) | tensorSize | slotStride) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(input);
        const size_t words = tensorSize / sizeof(uint64_t);
        for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
            if (!available[peer]) continue;
            auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
            auto* dst = peer_base + slotOffset +
                        static_cast<size_t>(rank) * slotStride;
            if ((reinterpret_cast<uintptr_t>(dst) & (sizeof(uint64_t) - 1)) ==
                0) {
                auto* dst64 = reinterpret_cast<uint64_t*>(dst);
                for (size_t i = tid; i < words; i += stride) {
                    dst64[i] = src64[i];
                }
            } else {
                for (size_t i = tid; i < tensorSize; i += stride) {
                    dst[i] = input[i];
                }
            }
        }
        return;
    }

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* dst = peer_base + slotOffset + static_cast<size_t>(rank) * slotStride;
        for (size_t i = tid; i < tensorSize; i += stride) {
            dst[i] = input[i];
        }
    }
}

__global__ void p2pAllgatherStoreSlottedProduceWaitKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, int slot, int rank, int numRanks,
    uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* peerProduce = reinterpret_cast<uint32_t*>(peer_base +
                                                        produceSignalOffset);
        p2pAgSignalStore(peerProduce +
                             static_cast<size_t>(slot) * kMaxNumRanks + rank,
                         sequence, 2);
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<volatile uint32_t*>(local_recv_base +
                                                         produceSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        while (produce[static_cast<size_t>(slot) * kMaxNumRanks + peer] <
               sequence) {
        }
    }
}

__global__ void p2pSlottedReuseWaitKernel(
    const char* local_recv_base, const int32_t* available,
    size_t consumeSignalOffset, int slot, int numRanks, uint32_t sequence,
    int slots) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    if (sequence < static_cast<uint32_t>(slots)) return;

    const uint32_t previousSequence = sequence - static_cast<uint32_t>(slots);
    auto* consume = reinterpret_cast<const volatile uint32_t*>(
        local_recv_base + consumeSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        while (consume[static_cast<size_t>(slot) * kMaxNumRanks + peer] <
               previousSequence) {
        }
    }
}

__global__ void p2pSlottedConsumeNotifyKernel(
    void** peer_ptrs, const int32_t* available, size_t consumeSignalOffset,
    int slot, int rank, int numRanks, uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* peerConsume = reinterpret_cast<uint32_t*>(peer_base +
                                                       consumeSignalOffset);
        p2pAgSignalStore(peerConsume +
                             static_cast<size_t>(slot) * kMaxNumRanks + rank,
                         sequence, 2);
    }
    __threadfence_system();
}

__global__ void p2pReserveSequenceKernel(uint32_t* counter,
                                         uint32_t* sequenceSlot,
                                         uint32_t increment) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    *sequenceSlot = atomicAdd(counter, increment);
}

void launchP2pReserveSequence(uint32_t* counter, uint32_t* sequenceSlot,
                              uint32_t increment, cudaStream_t stream) {
    p2pReserveSequenceKernel<<<1, 1, 0, stream>>>(counter, sequenceSlot,
                                                  increment);
}

__global__ void p2pSlottedReuseWaitGraphKernel(
    const char* local_recv_base, const int32_t* available,
    size_t consumeSignalOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int numRanks, uint32_t* sequenceCounter,
    uint32_t reserveIncrement) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    if (chunkIndex == 0 && sequenceCounter != nullptr) {
        *const_cast<uint32_t*>(baseSequenceSlot) =
            atomicAdd(sequenceCounter, reserveIncrement);
    }
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    if (sequence < static_cast<uint32_t>(slots)) return;

    const uint32_t previousSequence = sequence - static_cast<uint32_t>(slots);
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    auto* consume = reinterpret_cast<const volatile uint32_t*>(
        local_recv_base + consumeSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        while (consume[static_cast<size_t>(slot) * kMaxNumRanks + peer] <
               previousSequence) {
        }
    }
}

__global__ void deviceApiSlottedReuseWaitGraphKernel(
    const char* local_recv_base, const bool* active_mask,
    size_t consumeSignalOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int numRanks, uint32_t* sequenceCounter,
    uint32_t reserveIncrement) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    if (sequenceCounter != nullptr && chunkIndex == 0) {
        const uint32_t old = atomicAdd(sequenceCounter, reserveIncrement);
        *const_cast<uint32_t*>(baseSequenceSlot) = old;
    }
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    if (sequence < static_cast<uint32_t>(slots)) return;

    const uint32_t previousSequence = sequence - static_cast<uint32_t>(slots);
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    auto* consume = reinterpret_cast<const volatile uint32_t*>(
        local_recv_base + consumeSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!active_mask[peer]) continue;
        while (consume[static_cast<size_t>(slot) * kMaxNumRanks + peer] <
               previousSequence) {
        }
    }
}

__global__ void deviceApiSlottedClearSignalsGraphKernel(
    char* local_recv_base, size_t produceSignalOffset,
    size_t consumeSignalOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int numRanks) {
    if (blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const int tid = threadIdx.x;
    auto* produce =
        reinterpret_cast<uint32_t*>(local_recv_base + produceSignalOffset);
    auto* consume =
        reinterpret_cast<uint32_t*>(local_recv_base + consumeSignalOffset);
    for (int peer = tid; peer < numRanks; peer += blockDim.x) {
        produce[static_cast<size_t>(slot) * kMaxNumRanks + peer] = 0;
        consume[static_cast<size_t>(slot) * kMaxNumRanks + peer] = 0;
    }
    __threadfence_system();
}

__global__ void p2pSlottedProduceWaitGraphKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* peerProduce = reinterpret_cast<uint32_t*>(peer_base +
                                                        produceSignalOffset);
        p2pAgSignalStore(peerProduce +
                             static_cast<size_t>(slot) * kMaxNumRanks + rank,
                         sequence, 2);
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<volatile uint32_t*>(local_recv_base +
                                                         produceSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        while (produce[static_cast<size_t>(slot) * kMaxNumRanks + peer] <
               sequence) {
        }
    }
}

__global__ void p2pSlottedProduceWaitDeviceApiGraphKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, rank, numRanks, 1);
    __threadfence_system();
    auto* signal_ptr = reinterpret_cast<int*>(
        local_recv_base + produceSignalOffset +
        (static_cast<size_t>(slot) * kMaxNumRanks + rank) *
            sizeof(uint32_t));
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        if (peer == rank) {
            p2pAgSignalStore(reinterpret_cast<uint32_t*>(signal_ptr), sequence,
                             2);
        } else if (mc_comm_p2p_available(comm_ctx, peer)) {
            auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
            auto* peer_produce =
                reinterpret_cast<uint32_t*>(peer_base + produceSignalOffset);
            p2pAgSignalStore(peer_produce +
                                 static_cast<size_t>(slot) * kMaxNumRanks +
                                 rank,
                             sequence, 2);
        } else {
            mc_signal(comm_ctx, peer, 0, 1, signal_ptr,
                      static_cast<int32_t>(sequence));
        }
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<uint32_t*>(local_recv_base +
                                                produceSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* signal = produce + static_cast<size_t>(slot) * kMaxNumRanks +
                       peer;
        while (p2pAgSignalLoad(signal, 2) < sequence) {
        }
    }
}

__global__ void deviceApiSlottedProduceWaitGraphKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* p2p_available,
    const bool* active_mask, void* raddrs, void* rkeys, void* qp_devctxs,
    int qps_per_rank, size_t produceSignalOffset,
    size_t rdmaRemoteSignalBaseOffset, size_t rdmaLocalAtomicBaseOffset,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots, int rank,
    int numRanks, bool debug_signal, bool poll_completion) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const CommCtx comm_ctx = make_comm_ctx(
        local_recv_base, p2p_available, peer_ptrs, raddrs, rkeys, qp_devctxs,
        local_recv_base + rdmaLocalAtomicBaseOffset,
        local_recv_base + rdmaRemoteSignalBaseOffset, rank, numRanks,
        qps_per_rank);
    __threadfence_system();
    auto* signal_ptr = reinterpret_cast<int*>(
        local_recv_base + produceSignalOffset +
        (static_cast<size_t>(slot) * kMaxNumRanks + rank) *
            sizeof(uint32_t));
    if (debug_signal) {
        printf("MOONCAKE_PG_DEVICE_API_SIGNAL_BEGIN rank=%d seq=%u base=%u "
               "chunk=%u slot=%d active=%d%d%d%d p2p=%d%d%d%d qps=%d\n",
               rank, sequence, *baseSequenceSlot, chunkIndex, slot,
               numRanks > 0 ? static_cast<int>(active_mask[0]) : -1,
               numRanks > 1 ? static_cast<int>(active_mask[1]) : -1,
               numRanks > 2 ? static_cast<int>(active_mask[2]) : -1,
               numRanks > 3 ? static_cast<int>(active_mask[3]) : -1,
               numRanks > 0 ? static_cast<int>(p2p_available[0]) : -1,
               numRanks > 1 ? static_cast<int>(p2p_available[1]) : -1,
               numRanks > 2 ? static_cast<int>(p2p_available[2]) : -1,
               numRanks > 3 ? static_cast<int>(p2p_available[3]) : -1,
               qps_per_rank);
    }
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!active_mask[peer]) continue;
        if (peer == rank) {
            p2pAgSignalStore(reinterpret_cast<uint32_t*>(signal_ptr), sequence,
                             2);
            if (debug_signal) {
                printf("MOONCAKE_PG_DEVICE_API_SIGNAL_SEND rank=%d peer=%d "
                       "path=self seq=%u chunk=%u\n",
                       rank, peer, sequence, chunkIndex);
            }
        } else if (mc_comm_p2p_available(comm_ctx, peer)) {
            auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
            auto* peer_produce =
                reinterpret_cast<uint32_t*>(peer_base + produceSignalOffset);
            p2pAgSignalStore(peer_produce +
                                 static_cast<size_t>(slot) * kMaxNumRanks +
                                 rank,
                             sequence, 2);
            if (debug_signal) {
                printf("MOONCAKE_PG_DEVICE_API_SIGNAL_SEND rank=%d peer=%d "
                       "path=p2p seq=%u chunk=%u\n",
                       rank, peer, sequence, chunkIndex);
            }
        } else {
            mc_signal_write(comm_ctx, peer, 0, qps_per_rank, signal_ptr,
                            static_cast<int32_t>(sequence), debug_signal,
                            poll_completion);
            if (debug_signal) {
                printf("MOONCAKE_PG_DEVICE_API_SIGNAL_SEND rank=%d peer=%d "
                       "path=rdma seq=%u chunk=%u\n",
                       rank, peer, sequence, chunkIndex);
            }
        }
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<uint32_t*>(local_recv_base +
                                                produceSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!active_mask[peer]) continue;
        auto* signal = produce + static_cast<size_t>(slot) * kMaxNumRanks +
                       peer;
        bool printed_wait = false;
        while (p2pAgSignalLoad(signal, 2) < sequence) {
            if (debug_signal && !printed_wait) {
                printf("MOONCAKE_PG_DEVICE_API_SIGNAL_WAIT rank=%d peer=%d "
                       "seq=%u chunk=%u observed=%u\n",
                       rank, peer, sequence, chunkIndex,
                       p2pAgSignalLoad(signal, 2));
                printed_wait = true;
            }
        }
        if (debug_signal) {
            printf("MOONCAKE_PG_DEVICE_API_SIGNAL_RECV rank=%d peer=%d "
                   "seq=%u chunk=%u observed=%u\n",
                   rank, peer, sequence, chunkIndex,
                   p2pAgSignalLoad(signal, 2));
        }
    }
}

__global__ void p2pSlottedProduceWaitGraphParallelKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks) {
    if (blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const int lane = threadIdx.x;
    __threadfence_system();
    if (lane < numRanks && available[lane]) {
        auto* peer_base = static_cast<char*>(peer_ptrs[lane]);
        auto* peerProduce =
            reinterpret_cast<uint32_t*>(peer_base + produceSignalOffset);
        p2pAgSignalStore(peerProduce +
                             static_cast<size_t>(slot) * kMaxNumRanks + rank,
                         sequence, 2);
    }
    __syncthreads();
    __threadfence_system();

    auto* produce = reinterpret_cast<uint32_t*>(local_recv_base +
                                                produceSignalOffset);
    if (lane < numRanks && available[lane]) {
        auto* signal =
            produce + static_cast<size_t>(slot) * kMaxNumRanks + lane;
        while (p2pAgSignalLoad(signal, 2) < sequence) {
        }
    }
}

__global__ void p2pSlottedConsumeNotifyGraphKernel(
    void** peer_ptrs, const int32_t* available, size_t consumeSignalOffset,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots, int rank,
    int numRanks) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* peerConsume = reinterpret_cast<uint32_t*>(peer_base +
                                                       consumeSignalOffset);
        p2pAgSignalStore(peerConsume +
                             static_cast<size_t>(slot) * kMaxNumRanks + rank,
                         sequence, 2);
    }
    __threadfence_system();
}

__global__ void p2pAllgatherStoreSlottedLocalCopyKernel(
    const char* local_recv_base, char* output, size_t tensorSize,
    size_t slotStride, int slot, int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t outputBytes = tensorSize * static_cast<size_t>(numRanks);
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;

    if (slotStride == tensorSize &&
        ((reinterpret_cast<uintptr_t>(local_recv_base) |
          reinterpret_cast<uintptr_t>(output) | outputBytes) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(local_recv_base +
                                                             slotOffset);
        auto* dst64 = reinterpret_cast<uint64_t*>(output);
        const size_t words = outputBytes / sizeof(uint64_t);
        for (size_t i = tid; i < words; i += stride) {
            dst64[i] = src64[i];
        }
        return;
    }

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        const char* src = local_recv_base + slotOffset +
                          static_cast<size_t>(peer) * slotStride;
        char* dst = output + static_cast<size_t>(peer) * tensorSize;
        if (((reinterpret_cast<uintptr_t>(src) | reinterpret_cast<uintptr_t>(dst) |
              tensorSize) &
             (sizeof(uint64_t) - 1)) == 0) {
            const auto* src64 = reinterpret_cast<const uint64_t*>(src);
            auto* dst64 = reinterpret_cast<uint64_t*>(dst);
            const size_t words = tensorSize / sizeof(uint64_t);
            for (size_t i = tid; i < words; i += stride) {
                dst64[i] = src64[i];
            }
        } else {
            for (size_t i = tid; i < tensorSize; i += stride) {
                dst[i] = src[i];
            }
        }
    }
}

__global__ void p2pSlottedConsumeNotifyDeviceApiGraphKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t consumeSignalOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, rank, numRanks, 1);
    __threadfence_system();
    auto* signal_ptr = reinterpret_cast<int*>(
        local_recv_base + consumeSignalOffset +
        (static_cast<size_t>(slot) * kMaxNumRanks + rank) *
            sizeof(uint32_t));
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        if (peer == rank) {
            p2pAgSignalStore(reinterpret_cast<uint32_t*>(signal_ptr), sequence,
                             2);
        } else if (mc_comm_p2p_available(comm_ctx, peer)) {
            auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
            auto* peer_consume =
                reinterpret_cast<uint32_t*>(peer_base + consumeSignalOffset);
            p2pAgSignalStore(peer_consume +
                                 static_cast<size_t>(slot) * kMaxNumRanks +
                                 rank,
                             sequence, 2);
        } else {
            mc_signal(comm_ctx, peer, 0, 1, signal_ptr,
                      static_cast<int32_t>(sequence));
        }
    }
    __threadfence_system();
}

__global__ void deviceApiSlottedConsumeNotifyGraphKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* p2p_available,
    const bool* active_mask, void* raddrs, void* rkeys, void* qp_devctxs,
    int qps_per_rank, size_t consumeSignalOffset,
    size_t rdmaRemoteSignalBaseOffset, size_t rdmaLocalAtomicBaseOffset,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots, int rank,
    int numRanks, bool poll_completion) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const CommCtx comm_ctx = make_comm_ctx(
        local_recv_base, p2p_available, peer_ptrs, raddrs, rkeys, qp_devctxs,
        local_recv_base + rdmaLocalAtomicBaseOffset,
        local_recv_base + rdmaRemoteSignalBaseOffset, rank, numRanks,
        qps_per_rank);
    __threadfence_system();
    auto* signal_ptr = reinterpret_cast<int*>(
        local_recv_base + consumeSignalOffset +
        (static_cast<size_t>(slot) * kMaxNumRanks + rank) *
            sizeof(uint32_t));
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!active_mask[peer]) continue;
        if (peer == rank) {
            p2pAgSignalStore(reinterpret_cast<uint32_t*>(signal_ptr), sequence,
                             2);
        } else if (mc_comm_p2p_available(comm_ctx, peer)) {
            auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
            auto* peer_consume =
                reinterpret_cast<uint32_t*>(peer_base + consumeSignalOffset);
            p2pAgSignalStore(peer_consume +
                                 static_cast<size_t>(slot) * kMaxNumRanks +
                                 rank,
                             sequence, 2);
        } else {
            mc_signal_write(comm_ctx, peer, 0, qps_per_rank, signal_ptr,
                            static_cast<int32_t>(sequence), false,
                            poll_completion);
        }
    }
    __threadfence_system();
}

__global__ void p2pSlottedConsumeNotifyDeviceApiKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t consumeSignalOffset, int slot, int rank, int numRanks,
    uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, rank, numRanks, 1);
    __threadfence_system();
    auto* signal_ptr = reinterpret_cast<int*>(
        local_recv_base + consumeSignalOffset +
        (static_cast<size_t>(slot) * kMaxNumRanks + rank) *
            sizeof(uint32_t));
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        mc_signal(comm_ctx, peer, 0, 1, signal_ptr,
                  static_cast<int32_t>(sequence));
    }
    __threadfence_system();
}

__global__ void p2pAllgatherStoreSlottedLocalCopyChunkKernel(
    const char* local_recv_base, char* output, size_t chunkBytes,
    size_t outputRankStride, size_t chunkOffset, size_t slotStride, int slot,
    int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        const char* src = local_recv_base + slotOffset +
                          static_cast<size_t>(peer) * slotStride;
        char* dst = output + static_cast<size_t>(peer) * outputRankStride +
                    chunkOffset;
        if (((reinterpret_cast<uintptr_t>(src) | reinterpret_cast<uintptr_t>(dst) |
              chunkBytes) &
             (sizeof(uint64_t) - 1)) == 0) {
            const auto* src64 = reinterpret_cast<const uint64_t*>(src);
            auto* dst64 = reinterpret_cast<uint64_t*>(dst);
            const size_t words = chunkBytes / sizeof(uint64_t);
            for (size_t i = tid; i < words; i += stride) {
                dst64[i] = src64[i];
            }
        } else {
            for (size_t i = tid; i < chunkBytes; i += stride) {
                dst[i] = src[i];
            }
        }
    }
}

__global__ void p2pAllgatherStoreToSlottedScratchGraphKernel(
    const char* input, void** peer_ptrs, const int32_t* available,
    size_t tensorSize, size_t slotStride, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;

    if (((reinterpret_cast<uintptr_t>(input) | tensorSize) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(input);
        const size_t words = tensorSize / sizeof(uint64_t);
        for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
            if (!available[peer]) continue;
            auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
            auto* dst64 = reinterpret_cast<uint64_t*>(
                peer_base + slotOffset + static_cast<size_t>(rank) * slotStride);
            for (size_t i = tid; i < words; i += stride) {
                dst64[i] = src64[i];
            }
        }
        return;
    }

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        char* dst = peer_base + slotOffset + static_cast<size_t>(rank) * slotStride;
        for (size_t i = tid; i < tensorSize; i += stride) {
            dst[i] = input[i];
        }
    }
}

__global__ void p2pAllgatherStoreToSlottedScratchDeviceApiGraphKernel(
    const char* input, char* local_recv_base, void** peer_ptrs,
    const int32_t* available, size_t tensorSize, size_t slotStride,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots, int rank,
    int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, rank, numRanks, 1);

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        void* write_dst = mc_route_put(
            comm_ctx, peer,
            local_recv_base + slotOffset + static_cast<size_t>(rank) * slotStride);
        if (write_dst == nullptr) {
            continue;
        }
        auto* dst = static_cast<char*>(write_dst);
        if (((reinterpret_cast<uintptr_t>(input) |
              reinterpret_cast<uintptr_t>(dst) | tensorSize) &
             (sizeof(uint64_t) - 1)) == 0) {
            const auto* src64 = reinterpret_cast<const uint64_t*>(input);
            auto* dst64 = reinterpret_cast<uint64_t*>(dst);
            const size_t words = tensorSize / sizeof(uint64_t);
            for (size_t i = tid; i < words; i += stride) {
                dst64[i] = src64[i];
            }
        } else {
            for (size_t i = tid; i < tensorSize; i += stride) {
                dst[i] = input[i];
            }
        }
    }
}

__global__ void deviceApiAllgatherStageLocalSlottedGraphKernel(
    const char* input, char* local_recv_base, size_t tensorSize,
    size_t slotStride, const uint32_t* baseSequenceSlot, uint32_t chunkIndex,
    int slots, int rank, int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;
    char* dst = local_recv_base + slotOffset +
                static_cast<size_t>(rank) * slotStride;

    if (((reinterpret_cast<uintptr_t>(input) | reinterpret_cast<uintptr_t>(dst) |
          tensorSize) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(input);
        auto* dst64 = reinterpret_cast<uint64_t*>(dst);
        const size_t words = tensorSize / sizeof(uint64_t);
        for (size_t i = tid; i < words; i += stride) {
            dst64[i] = src64[i];
        }
        return;
    }

    for (size_t i = tid; i < tensorSize; i += stride) {
        dst[i] = input[i];
    }
}

__global__ void deviceApiAllgatherPublishSlottedGraphKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* p2p_available,
    const bool* active_mask, void* raddrs, void* rkeys, void* qp_devctxs,
    int qps_per_rank, size_t tensorSize, size_t slotStride,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots, int rank,
    int numRanks, bool debug_signal, bool poll_completion) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;
    const CommCtx comm_ctx = make_comm_ctx(
        local_recv_base, p2p_available, peer_ptrs, raddrs, rkeys, qp_devctxs,
        local_recv_base, local_recv_base, rank, numRanks, qps_per_rank);
    const void* send_ptr = local_recv_base + slotOffset +
                           static_cast<size_t>(rank) * slotStride;

    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!active_mask[peer]) continue;
        if (peer == rank) continue;
        void* recv_ptr = local_recv_base + slotOffset +
                         static_cast<size_t>(rank) * slotStride;
        void* write_dst = mc_route_put(comm_ctx, peer, recv_ptr);
        if (write_dst == nullptr) {
            mc_rdma_put(comm_ctx, 0, peer, qps_per_rank, send_ptr, recv_ptr,
                        static_cast<uint32_t>(tensorSize), 0, debug_signal,
                        poll_completion);
        } else if (peer != rank) {
            // P2P peers were already handled by the multi-block store kernel.
        }
    }
    __threadfence_system();
}

__global__ void p2pAllgatherStoreToSlottedScratchGraphSkipSelfKernel(
    const char* input, void** peer_ptrs, const int32_t* available,
    size_t tensorSize, size_t slotStride, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;

    if (((reinterpret_cast<uintptr_t>(input) | tensorSize) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(input);
        const size_t words = tensorSize / sizeof(uint64_t);
        for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
            if (peer == rank || !available[peer]) continue;
            auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
            auto* dst64 = reinterpret_cast<uint64_t*>(
                peer_base + slotOffset + static_cast<size_t>(rank) * slotStride);
            for (size_t i = tid; i < words; i += stride) {
                dst64[i] = src64[i];
            }
        }
        return;
    }

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (peer == rank || !available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        char* dst = peer_base + slotOffset + static_cast<size_t>(rank) * slotStride;
        for (size_t i = tid; i < tensorSize; i += stride) {
            dst[i] = input[i];
        }
    }
}

__global__ void p2pAllgatherStoreSlottedLocalCopyGraphKernel(
    const char* local_recv_base, char* output, size_t tensorSize,
    size_t slotStride, const uint32_t* baseSequenceSlot, uint32_t chunkIndex,
    int slots, int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t outputBytes = tensorSize * static_cast<size_t>(numRanks);
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;

    if (slotStride == tensorSize &&
        ((reinterpret_cast<uintptr_t>(local_recv_base) |
          reinterpret_cast<uintptr_t>(output) | outputBytes) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(local_recv_base +
                                                             slotOffset);
        auto* dst64 = reinterpret_cast<uint64_t*>(output);
        const size_t words = outputBytes / sizeof(uint64_t);
        for (size_t i = tid; i < words; i += stride) {
            dst64[i] = src64[i];
        }
        return;
    }

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        const char* src = local_recv_base + slotOffset +
                          static_cast<size_t>(peer) * slotStride;
        char* dst = output + static_cast<size_t>(peer) * tensorSize;
        if (((reinterpret_cast<uintptr_t>(src) | reinterpret_cast<uintptr_t>(dst) |
              tensorSize) &
             (sizeof(uint64_t) - 1)) == 0) {
            const auto* src64 = reinterpret_cast<const uint64_t*>(src);
            auto* dst64 = reinterpret_cast<uint64_t*>(dst);
            const size_t words = tensorSize / sizeof(uint64_t);
            for (size_t i = tid; i < words; i += stride) {
                dst64[i] = src64[i];
            }
        } else {
            for (size_t i = tid; i < tensorSize; i += stride) {
                dst[i] = src[i];
            }
        }
    }
}

__global__ void p2pAllgatherStoreSlottedLocalCopyGraphSkipSelfKernel(
    const char* local_recv_base, char* output, size_t tensorSize,
    size_t slotStride, const uint32_t* baseSequenceSlot, uint32_t chunkIndex,
    int slots, int rank, int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (peer == rank) continue;
        const char* src = local_recv_base + slotOffset +
                          static_cast<size_t>(peer) * slotStride;
        char* dst = output + static_cast<size_t>(peer) * tensorSize;
        if (((reinterpret_cast<uintptr_t>(src) | reinterpret_cast<uintptr_t>(dst) |
              tensorSize) &
             (sizeof(uint64_t) - 1)) == 0) {
            const auto* src64 = reinterpret_cast<const uint64_t*>(src);
            auto* dst64 = reinterpret_cast<uint64_t*>(dst);
            const size_t words = tensorSize / sizeof(uint64_t);
            for (size_t i = tid; i < words; i += stride) {
                dst64[i] = src64[i];
            }
        } else {
            for (size_t i = tid; i < tensorSize; i += stride) {
                dst[i] = src[i];
            }
        }
    }
}

__global__ void p2pAllgatherStoreSlottedLocalCopyChunkGraphKernel(
    const char* local_recv_base, char* output, size_t chunkBytes,
    size_t outputRankStride, size_t chunkOffset, size_t slotStride,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots,
    int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStride;

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        const char* src = local_recv_base + slotOffset +
                          static_cast<size_t>(peer) * slotStride;
        char* dst = output + static_cast<size_t>(peer) * outputRankStride +
                    chunkOffset;
        if (((reinterpret_cast<uintptr_t>(src) | reinterpret_cast<uintptr_t>(dst) |
              chunkBytes) &
             (sizeof(uint64_t) - 1)) == 0) {
            const auto* src64 = reinterpret_cast<const uint64_t*>(src);
            auto* dst64 = reinterpret_cast<uint64_t*>(dst);
            const size_t words = chunkBytes / sizeof(uint64_t);
            for (size_t i = tid; i < words; i += stride) {
                dst64[i] = src64[i];
            }
        } else {
            for (size_t i = tid; i < chunkBytes; i += stride) {
                dst[i] = src[i];
            }
        }
    }
}

template <typename scalar_t>
__global__ void p2pReduceScatterStoreToSlottedScratchKernel(
    const scalar_t* input, void** peer_ptrs, const int32_t* available,
    size_t numElements, size_t slotStrideBytes, int slot, int rank,
    int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* dst = reinterpret_cast<scalar_t*>(
            peer_base + slotOffset + static_cast<size_t>(rank) * slotStrideBytes);
        const scalar_t* src = input + static_cast<size_t>(peer) * numElements;
        for (size_t i = tid; i < numElements; i += stride) {
            dst[i] = src[i];
        }
    }
}

template <typename scalar_t>
__global__ void p2pReduceScatterStoreChunkToSlottedScratchKernel(
    const scalar_t* input, void** peer_ptrs, const int32_t* available,
    size_t fullNumElements, size_t chunkOffsetElements, size_t chunkElements,
    size_t slotStrideBytes, int slot, int rank, int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* dst = reinterpret_cast<scalar_t*>(
            peer_base + slotOffset + static_cast<size_t>(rank) * slotStrideBytes);
        const scalar_t* src = input + static_cast<size_t>(peer) * fullNumElements +
                              chunkOffsetElements;
        for (size_t i = tid; i < chunkElements; i += stride) {
            dst[i] = src[i];
        }
    }
}

template <typename scalar_t>
__global__ void p2pReduceScatterStoreToSlottedScratchGraphKernel(
    const scalar_t* input, void** peer_ptrs, const int32_t* available,
    size_t numElements, size_t slotStrideBytes,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots, int rank,
    int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* dst = reinterpret_cast<scalar_t*>(
            peer_base + slotOffset + static_cast<size_t>(rank) * slotStrideBytes);
        const scalar_t* src = input + static_cast<size_t>(peer) * numElements;
        for (size_t i = tid; i < numElements; i += stride) {
            dst[i] = src[i];
        }
    }
}

template <typename scalar_t>
__global__ void p2pReduceScatterStoreChunkToSlottedScratchGraphKernel(
    const scalar_t* input, void** peer_ptrs, const int32_t* available,
    size_t fullNumElements, size_t chunkOffsetElements, size_t chunkElements,
    size_t slotStrideBytes, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* dst = reinterpret_cast<scalar_t*>(
            peer_base + slotOffset + static_cast<size_t>(rank) * slotStrideBytes);
        const scalar_t* src = input + static_cast<size_t>(peer) * fullNumElements +
                              chunkOffsetElements;
        for (size_t i = tid; i < chunkElements; i += stride) {
            dst[i] = src[i];
        }
    }
}

template <typename scalar_t>
__global__ void p2pReduceScatterStoreToSlottedScratchDeviceApiGraphKernel(
    const scalar_t* input, char* local_recv_base, void** peer_ptrs,
    const int32_t* available, size_t numElements, size_t slotStrideBytes,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots, int rank,
    int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, rank, numRanks, 1);

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        void* write_dst = mc_route_put(
            comm_ctx, peer,
            local_recv_base + slotOffset +
                static_cast<size_t>(rank) * slotStrideBytes);
        if (write_dst == nullptr) continue;
        auto* dst = static_cast<scalar_t*>(write_dst);
        const scalar_t* src = input + static_cast<size_t>(peer) * numElements;
        for (size_t i = tid; i < numElements; i += stride) {
            dst[i] = src[i];
        }
    }
}

template <typename scalar_t>
__global__ void deviceApiReduceScatterStageSendSlottedGraphKernel(
    const scalar_t* input, char* local_recv_base, size_t numElements,
    size_t slotStrideBytes, size_t rdmaSendBaseOffset,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots,
    int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const int peer = blockIdx.y;
    if (peer >= numRanks) return;
    const size_t slotOffset =
        rdmaSendBaseOffset +
        (static_cast<size_t>(slot) * static_cast<size_t>(numRanks) +
         static_cast<size_t>(peer)) *
            slotStrideBytes;
    auto* dst = reinterpret_cast<scalar_t*>(local_recv_base + slotOffset);
    const scalar_t* src = input + static_cast<size_t>(peer) * numElements;
    for (size_t i = tid; i < numElements; i += stride) {
        dst[i] = src[i];
    }
}

template <typename scalar_t>
__global__ void deviceApiReduceScatterStageSendChunkSlottedGraphKernel(
    const scalar_t* input, char* local_recv_base, size_t fullNumElements,
    size_t chunkOffsetElements, size_t chunkElements, size_t slotStrideBytes,
    size_t rdmaSendBaseOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const int peer = blockIdx.y;
    if (peer >= numRanks) return;
    const size_t slotOffset =
        rdmaSendBaseOffset +
        (static_cast<size_t>(slot) * static_cast<size_t>(numRanks) +
         static_cast<size_t>(peer)) *
            slotStrideBytes;
    auto* dst = reinterpret_cast<scalar_t*>(local_recv_base + slotOffset);
    const scalar_t* src =
        input + static_cast<size_t>(peer) * fullNumElements + chunkOffsetElements;
    for (size_t i = tid; i < chunkElements; i += stride) {
        dst[i] = src[i];
    }
}

template <typename scalar_t>
__global__ void deviceApiReduceScatterPublishSlottedGraphKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* p2p_available,
    const bool* active_mask, void* raddrs, void* rkeys, void* qp_devctxs,
    int qps_per_rank, size_t numElements, size_t slotStrideBytes,
    size_t rdmaSendBaseOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks,
    bool poll_completion) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t recvSlotOffset =
        static_cast<size_t>(slot) * static_cast<size_t>(numRanks) *
        slotStrideBytes;
    const size_t tensorBytes = numElements * sizeof(scalar_t);
    const CommCtx comm_ctx = make_comm_ctx(
        local_recv_base, p2p_available, peer_ptrs, raddrs, rkeys, qp_devctxs,
        local_recv_base, local_recv_base, rank, numRanks, qps_per_rank);

    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!active_mask[peer]) continue;
        void* recv_ptr =
            local_recv_base + recvSlotOffset +
            static_cast<size_t>(rank) * slotStrideBytes;
        void* write_dst = mc_route_put(comm_ctx, peer, recv_ptr);
        if (write_dst == nullptr) {
            const void* send_ptr =
                local_recv_base + rdmaSendBaseOffset +
                (static_cast<size_t>(slot) * static_cast<size_t>(numRanks) +
                 static_cast<size_t>(peer)) *
                    slotStrideBytes;
            mc_rdma_put(comm_ctx, 0, peer, qps_per_rank, send_ptr, recv_ptr,
                        static_cast<uint32_t>(tensorBytes), 0, false,
                        poll_completion);
        }
    }
    __threadfence_system();
}

template <typename scalar_t>
__global__ void p2pReduceScatterStoreChunkToSlottedScratchDeviceApiGraphKernel(
    const scalar_t* input, char* local_recv_base, void** peer_ptrs,
    const int32_t* available, size_t fullNumElements, size_t chunkOffsetElements,
    size_t chunkElements, size_t slotStrideBytes,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots, int rank,
    int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, rank, numRanks, 1);

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        void* write_dst = mc_route_put(
            comm_ctx, peer,
            local_recv_base + slotOffset +
                static_cast<size_t>(rank) * slotStrideBytes);
        if (write_dst == nullptr) continue;
        auto* dst = static_cast<scalar_t*>(write_dst);
        const scalar_t* src = input + static_cast<size_t>(peer) * fullNumElements +
                              chunkOffsetElements;
        for (size_t i = tid; i < chunkElements; i += stride) {
            dst[i] = src[i];
        }
    }
}

__global__ void p2pReduceScatterSlottedProduceWaitKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, int slot, int rank, int numRanks,
    uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* peerProduce = reinterpret_cast<uint32_t*>(peer_base +
                                                        produceSignalOffset);
        p2pAgSignalStore(peerProduce +
                             static_cast<size_t>(slot) * kMaxNumRanks + rank,
                         sequence, 2);
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<volatile uint32_t*>(local_recv_base +
                                                         produceSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        while (produce[static_cast<size_t>(slot) * kMaxNumRanks + peer] <
               sequence) {
        }
    }
}

__global__ void p2pSlottedProduceWaitDeviceApiKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, int slot, int rank, int numRanks,
    uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, rank, numRanks, 1);
    __threadfence_system();
    auto* signal_ptr = reinterpret_cast<int*>(
        local_recv_base + produceSignalOffset +
        (static_cast<size_t>(slot) * kMaxNumRanks + rank) *
            sizeof(uint32_t));
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        mc_signal(comm_ctx, peer, 0, 1, signal_ptr,
                  static_cast<int32_t>(sequence));
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<volatile uint32_t*>(local_recv_base +
                                                         produceSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        while (produce[static_cast<size_t>(slot) * kMaxNumRanks + peer] <
               sequence) {
        }
    }
}

template <typename scalar_t>
__global__ void p2pReduceScatterSlottedLocalReduceKernel(
    scalar_t* output, const char* local_recv_base, size_t numElements,
    size_t slotStrideBytes, int slot, int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;

#ifdef __MUSA__
    constexpr bool kIsBf16 = std::is_same_v<scalar_t, mt_bfloat16>;
#else
    constexpr bool kIsBf16 = false;
#endif
    using acc_t = std::conditional_t<kIsBf16, float, scalar_t>;

    for (size_t i = tid; i < numElements; i += stride) {
        auto* src0 = reinterpret_cast<const scalar_t*>(local_recv_base +
                                                       slotOffset);
        acc_t acc;
        if constexpr (kIsBf16) {
            acc = static_cast<float>(src0[i]);
        } else {
            acc = src0[i];
        }
        for (int peer = 1; peer < numRanks; ++peer) {
            auto* src = reinterpret_cast<const scalar_t*>(
                local_recv_base + slotOffset +
                static_cast<size_t>(peer) * slotStrideBytes);
            if constexpr (kIsBf16) {
                acc += static_cast<float>(src[i]);
            } else {
                acc += src[i];
            }
        }
        if constexpr (kIsBf16) {
            output[i] = static_cast<scalar_t>(acc);
        } else {
            output[i] = acc;
        }
    }
}

template <typename scalar_t>
__global__ void p2pReduceScatterSlottedLocalReduceChunkKernel(
    scalar_t* output, const char* local_recv_base, size_t chunkOffsetElements,
    size_t chunkElements, size_t slotStrideBytes, int slot, int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;

#ifdef __MUSA__
    constexpr bool kIsBf16 = std::is_same_v<scalar_t, mt_bfloat16>;
#else
    constexpr bool kIsBf16 = false;
#endif
    using acc_t = std::conditional_t<kIsBf16, float, scalar_t>;

    for (size_t i = tid; i < chunkElements; i += stride) {
        auto* src0 = reinterpret_cast<const scalar_t*>(local_recv_base +
                                                       slotOffset);
        acc_t acc;
        if constexpr (kIsBf16) {
            acc = static_cast<float>(src0[i]);
        } else {
            acc = src0[i];
        }
        for (int peer = 1; peer < numRanks; ++peer) {
            auto* src = reinterpret_cast<const scalar_t*>(
                local_recv_base + slotOffset +
                static_cast<size_t>(peer) * slotStrideBytes);
            if constexpr (kIsBf16) {
                acc += static_cast<float>(src[i]);
            } else {
                acc += src[i];
            }
        }
        if constexpr (kIsBf16) {
            output[chunkOffsetElements + i] = static_cast<scalar_t>(acc);
        } else {
            output[chunkOffsetElements + i] = acc;
        }
    }
}

template <typename scalar_t>
__global__ void p2pReduceScatterSlottedLocalReduceGraphKernel(
    scalar_t* output, const char* local_recv_base, size_t numElements,
    size_t slotStrideBytes, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;
#ifdef __MUSA__
    constexpr bool kIsBf16 = std::is_same_v<scalar_t, mt_bfloat16>;
#else
    constexpr bool kIsBf16 = false;
#endif
    using acc_t = std::conditional_t<kIsBf16, float, scalar_t>;

    for (size_t i = tid; i < numElements; i += stride) {
        auto* src0 = reinterpret_cast<const scalar_t*>(local_recv_base +
                                                       slotOffset);
        acc_t acc;
        if constexpr (kIsBf16) {
            acc = static_cast<float>(src0[i]);
        } else {
            acc = src0[i];
        }
        for (int peer = 1; peer < numRanks; ++peer) {
            auto* src = reinterpret_cast<const scalar_t*>(
                local_recv_base + slotOffset +
                static_cast<size_t>(peer) * slotStrideBytes);
            if constexpr (kIsBf16) {
                acc += static_cast<float>(src[i]);
            } else {
                acc += src[i];
            }
        }
        if constexpr (kIsBf16) {
            output[i] = static_cast<scalar_t>(acc);
        } else {
            output[i] = acc;
        }
    }
}

template <typename scalar_t>
__global__ void p2pReduceScatterSlottedLocalReduceChunkGraphKernel(
    scalar_t* output, const char* local_recv_base, size_t chunkOffsetElements,
    size_t chunkElements, size_t slotStrideBytes,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots,
    int numRanks) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;
#ifdef __MUSA__
    constexpr bool kIsBf16 = std::is_same_v<scalar_t, mt_bfloat16>;
#else
    constexpr bool kIsBf16 = false;
#endif
    using acc_t = std::conditional_t<kIsBf16, float, scalar_t>;

    for (size_t i = tid; i < chunkElements; i += stride) {
        auto* src0 = reinterpret_cast<const scalar_t*>(local_recv_base +
                                                       slotOffset);
        acc_t acc;
        if constexpr (kIsBf16) {
            acc = static_cast<float>(src0[i]);
        } else {
            acc = src0[i];
        }
        for (int peer = 1; peer < numRanks; ++peer) {
            auto* src = reinterpret_cast<const scalar_t*>(
                local_recv_base + slotOffset +
                static_cast<size_t>(peer) * slotStrideBytes);
            if constexpr (kIsBf16) {
                acc += static_cast<float>(src[i]);
            } else {
                acc += src[i];
            }
        }
        if constexpr (kIsBf16) {
            output[chunkOffsetElements + i] = static_cast<scalar_t>(acc);
        } else {
            output[chunkOffsetElements + i] = acc;
        }
    }
}

template <typename scalar_t>
__global__ void p2pAllReduceFusedReduceToRsScratchGraphKernel(
    char* local_recv_base, size_t segmentElements, size_t slotStrideBytes,
    const uint32_t* rsSequenceSlot, int slots, int rank, int numRanks) {
    const uint32_t rsSequence = *rsSequenceSlot;
    const int rsSlot =
        static_cast<int>(rsSequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t rsSlotOffset = static_cast<size_t>(rsSlot) *
                                static_cast<size_t>(numRanks) *
                                slotStrideBytes;
    auto* reducedDst = reinterpret_cast<scalar_t*>(
        local_recv_base + rsSlotOffset +
        static_cast<size_t>(rank) * slotStrideBytes);

#ifdef __MUSA__
    constexpr bool kIsBf16 = std::is_same_v<scalar_t, mt_bfloat16>;
#else
    constexpr bool kIsBf16 = false;
#endif
    using acc_t = std::conditional_t<kIsBf16, float, scalar_t>;

    for (size_t i = tid; i < segmentElements; i += stride) {
        auto* src0 =
            reinterpret_cast<const scalar_t*>(local_recv_base + rsSlotOffset);
        acc_t acc;
        if constexpr (kIsBf16) {
            acc = static_cast<float>(src0[i]);
        } else {
            acc = src0[i];
        }
        for (int srcPeer = 1; srcPeer < numRanks; ++srcPeer) {
            auto* src = reinterpret_cast<const scalar_t*>(
                local_recv_base + rsSlotOffset +
                static_cast<size_t>(srcPeer) * slotStrideBytes);
            if constexpr (kIsBf16) {
                acc += static_cast<float>(src[i]);
            } else {
                acc += src[i];
            }
        }
        if constexpr (kIsBf16) {
            reducedDst[i] = static_cast<scalar_t>(acc);
        } else {
            reducedDst[i] = acc;
        }
    }
}

template <typename scalar_t>
__global__ void p2pAllReduceFusedPublishReducedToAgScratchGraphKernel(
    const char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t segmentElements, size_t slotStrideBytes,
    const uint32_t* rsSequenceSlot, const uint32_t* agSequenceSlot, int slots,
    int rank, int numRanks) {
    const uint32_t rsSequence = *rsSequenceSlot;
    const uint32_t agSequence = *agSequenceSlot;
    const int rsSlot =
        static_cast<int>(rsSequence % static_cast<uint32_t>(slots));
    const int agSlot =
        static_cast<int>(agSequence % static_cast<uint32_t>(slots));
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t rsSlotOffset = static_cast<size_t>(rsSlot) *
                                static_cast<size_t>(numRanks) *
                                slotStrideBytes;
    const size_t agSlotOffset = static_cast<size_t>(agSlot) *
                                static_cast<size_t>(numRanks) *
                                slotStrideBytes;
    const auto* src = reinterpret_cast<const scalar_t*>(
        local_recv_base + rsSlotOffset +
        static_cast<size_t>(rank) * slotStrideBytes);

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* dst = reinterpret_cast<scalar_t*>(
            peer_base + agSlotOffset +
            static_cast<size_t>(rank) * slotStrideBytes);
        for (size_t i = tid; i < segmentElements; i += stride) {
            dst[i] = src[i];
        }
    }
}

template <typename scalar_t>
void launchP2pReduceScatterSlottedKernelTyped(
    scalar_t* output, const scalar_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream) {
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    constexpr int threads = 256;
    const size_t elements_per_block = threads;
    int blocks_x = static_cast<int>((numElements + elements_per_block - 1) /
                                    elements_per_block);
    const int store_blocks_x = clampP2pReduceScatterStoreBlocks(blocks_x);
    const int reduce_blocks_x = clampP2pReduceScatterReduceBlocks(blocks_x);
    dim3 store_grid(store_blocks_x, numRanks, 1);
    p2pSlottedReuseWaitKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available, consumeSignalOffset,
        slot, numRanks, sequence, slots);
    p2pReduceScatterStoreToSlottedScratchKernel<<<store_grid, threads, 0,
                                                  stream>>>(
        input, peer_ptrs, available, numElements, slotStrideBytes, slot, rank,
        numRanks);
    p2pReduceScatterSlottedProduceWaitKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, available,
        produceSignalOffset, slot, rank, numRanks, sequence);
    p2pReduceScatterSlottedLocalReduceKernel<<<reduce_blocks_x, threads, 0, stream>>>(
        output, static_cast<const char*>(local_recv_base), numElements,
        slotStrideBytes, slot, numRanks);
    p2pSlottedConsumeNotifyKernel<<<1, 1, 0, stream>>>(
        peer_ptrs, available, consumeSignalOffset, slot, rank, numRanks,
        sequence);
}

#define DEF_LAUNCH_P2P_RS_SLOTTED(scalar_t, suffix)                         \
    void launchP2pReduceScatterSlottedKernel_##suffix(                       \
        scalar_t* output, const scalar_t* input, void* local_recv_base,       \
        void** peer_ptrs, int32_t* available, size_t numElements,             \
        size_t slotStrideBytes, int slots, int rank, int numRanks,            \
        uint32_t sequence, cudaStream_t stream) {                             \
        launchP2pReduceScatterSlottedKernelTyped(                            \
            output, input, local_recv_base, peer_ptrs, available, numElements,\
            slotStrideBytes, slots, rank, numRanks, sequence, stream);        \
    }

DEF_LAUNCH_P2P_RS_SLOTTED(uint8_t, uint8)
DEF_LAUNCH_P2P_RS_SLOTTED(int8_t, int8)
DEF_LAUNCH_P2P_RS_SLOTTED(int16_t, int16)
DEF_LAUNCH_P2P_RS_SLOTTED(int, int32)
DEF_LAUNCH_P2P_RS_SLOTTED(int64_t, int64)
DEF_LAUNCH_P2P_RS_SLOTTED(float, float)
DEF_LAUNCH_P2P_RS_SLOTTED(double, double)
DEF_LAUNCH_P2P_RS_SLOTTED(bool, bool)

#undef DEF_LAUNCH_P2P_RS_SLOTTED

void launchP2pReduceScatterSlottedKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream) {
#ifdef __MUSA__
    launchP2pReduceScatterSlottedKernelTyped(
        static_cast<mt_bfloat16*>(output), static_cast<const mt_bfloat16*>(input),
        local_recv_base, peer_ptrs, available, numElements, slotStrideBytes,
        slots, rank, numRanks, sequence, stream);
#else
    launchP2pReduceScatterSlottedKernelTyped(
        static_cast<at::BFloat16*>(output), static_cast<const at::BFloat16*>(input),
        local_recv_base, peer_ptrs, available, numElements, slotStrideBytes,
        slots, rank, numRanks, sequence, stream);
#endif
}

template <typename scalar_t>
void launchP2pReduceScatterSlottedChunkedKernelTyped(
    scalar_t* output, const scalar_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t fullNumElements,
    size_t chunkElements, size_t slotStrideBytes, int slots, int rank,
    int numRanks, uint32_t baseSequence, cudaStream_t stream) {
    constexpr int threads = 256;
    size_t chunkIndex = 0;
    for (size_t offset = 0; offset < fullNumElements;
         offset += chunkElements, ++chunkIndex) {
        const size_t thisChunk = std::min(chunkElements, fullNumElements - offset);
        const uint32_t sequence =
            baseSequence + static_cast<uint32_t>(chunkIndex);
        const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
        const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                                   sizeof(uint32_t);
        const size_t produceSignalOffset = kBufferSize - signalBytes;
        const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
        int blocks_x = static_cast<int>((thisChunk + threads - 1) / threads);
        const int store_blocks_x = clampP2pReduceScatterStoreBlocks(blocks_x);
        const int reduce_blocks_x = clampP2pReduceScatterReduceBlocks(blocks_x);
        dim3 store_grid(store_blocks_x, numRanks, 1);
        p2pSlottedReuseWaitKernel<<<1, 1, 0, stream>>>(
            static_cast<const char*>(local_recv_base), available,
            consumeSignalOffset, slot, numRanks, sequence, slots);
        p2pReduceScatterStoreChunkToSlottedScratchKernel<<<store_grid, threads,
                                                           0, stream>>>(
            input, peer_ptrs, available, fullNumElements, offset, thisChunk,
            slotStrideBytes, slot, rank, numRanks);
        p2pReduceScatterSlottedProduceWaitKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, slot, rank, numRanks, sequence);
        p2pReduceScatterSlottedLocalReduceChunkKernel<<<reduce_blocks_x,
                                                        threads, 0, stream>>>(
            output, static_cast<const char*>(local_recv_base), offset,
            thisChunk, slotStrideBytes, slot, numRanks);
        p2pSlottedConsumeNotifyKernel<<<1, 1, 0, stream>>>(
            peer_ptrs, available, consumeSignalOffset, slot, rank, numRanks,
            sequence);
    }
}

#define DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED(scalar_t, suffix)                  \
    void launchP2pReduceScatterSlottedChunkedKernel_##suffix(                \
        scalar_t* output, const scalar_t* input, void* local_recv_base,       \
        void** peer_ptrs, int32_t* available, size_t fullNumElements,         \
        size_t chunkElements, size_t slotStrideBytes, int slots, int rank,    \
        int numRanks, uint32_t baseSequence, cudaStream_t stream) {           \
        launchP2pReduceScatterSlottedChunkedKernelTyped(                     \
            output, input, local_recv_base, peer_ptrs, available,             \
            fullNumElements, chunkElements, slotStrideBytes, slots, rank,     \
            numRanks, baseSequence, stream);                                 \
    }

DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED(uint8_t, uint8)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED(int8_t, int8)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED(int16_t, int16)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED(int, int32)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED(int64_t, int64)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED(float, float)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED(double, double)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED(bool, bool)

#undef DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED

void launchP2pReduceScatterSlottedChunkedKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream) {
#ifdef __MUSA__
    launchP2pReduceScatterSlottedChunkedKernelTyped(
        static_cast<mt_bfloat16*>(output), static_cast<const mt_bfloat16*>(input),
        local_recv_base, peer_ptrs, available, fullNumElements, chunkElements,
        slotStrideBytes, slots, rank, numRanks, baseSequence, stream);
#else
    launchP2pReduceScatterSlottedChunkedKernelTyped(
        static_cast<at::BFloat16*>(output), static_cast<const at::BFloat16*>(input),
        local_recv_base, peer_ptrs, available, fullNumElements, chunkElements,
        slotStrideBytes, slots, rank, numRanks, baseSequence, stream);
#endif
}

template <typename scalar_t>
void launchP2pReduceScatterSlottedGraphKernelTyped(
    scalar_t* output, const scalar_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, uint32_t* sequenceCounter,
    uint32_t reserveIncrement, cudaStream_t stream) {
#ifndef __MUSA__
    const bool profile = [rank] {
        const char* value = std::getenv("MOONCAKE_PG_RS_PROFILE");
        if (!value || value[0] == '\0' || value[0] == '0') return false;
        const char* rank_value = std::getenv("MOONCAKE_PG_RS_PROFILE_RANK");
        if (!rank_value || rank_value[0] == '\0') return true;
        char* end = nullptr;
        long parsed = std::strtol(rank_value, &end, 10);
        return end != rank_value && static_cast<int>(parsed) == rank;
    }();
    cudaEvent_t ev[6]{};
    if (profile) {
        for (auto& event : ev) {
            cudaEventCreate(&event);
        }
        cudaEventRecord(ev[0], stream);
    }
#endif
    constexpr int threads = 256;
    int blocks_x = static_cast<int>((numElements + threads - 1) / threads);
    const int store_blocks_x = clampP2pReduceScatterStoreBlocks(blocks_x);
    const int reduce_blocks_x = clampP2pReduceScatterReduceBlocks(blocks_x);
    dim3 store_grid(store_blocks_x, numRanks, 1);
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    constexpr uint32_t chunkIndex = 0;
    p2pSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available, consumeSignalOffset,
        baseSequenceSlot, chunkIndex, slots, numRanks, sequenceCounter,
        reserveIncrement);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[1], stream);
#endif
    if (useDeviceApiCollectives()) {
        p2pReduceScatterStoreToSlottedScratchDeviceApiGraphKernel<<<store_grid,
                                                                    threads, 0,
                                                                    stream>>>(
            input, static_cast<char*>(local_recv_base), peer_ptrs, available,
            numElements, slotStrideBytes, baseSequenceSlot, chunkIndex, slots,
            rank, numRanks);
    } else {
        p2pReduceScatterStoreToSlottedScratchGraphKernel<<<store_grid, threads,
                                                           0, stream>>>(
            input, peer_ptrs, available, numElements, slotStrideBytes,
            baseSequenceSlot, chunkIndex, slots, rank, numRanks);
    }
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[2], stream);
#endif
    if (useAllgatherParallelProduceWait()) {
        p2pSlottedProduceWaitGraphParallelKernel<<<1, 32, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
    } else if (useDeviceApiCollectives()) {
        p2pSlottedProduceWaitDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
    } else {
        p2pSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
    }
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[3], stream);
#endif
    p2pReduceScatterSlottedLocalReduceGraphKernel<<<reduce_blocks_x, threads, 0,
                                                    stream>>>(
        output, static_cast<const char*>(local_recv_base), numElements,
        slotStrideBytes, baseSequenceSlot, chunkIndex, slots, numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[4], stream);
#endif
    if (useDeviceApiCollectives()) {
        p2pSlottedConsumeNotifyDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            consumeSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
    } else {
        p2pSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
            peer_ptrs, available, consumeSignalOffset, baseSequenceSlot,
            chunkIndex, slots, rank, numRanks);
    }
#ifndef __MUSA__
    if (profile) {
        cudaEventRecord(ev[5], stream);
        cudaEventSynchronize(ev[5]);
        float reuse_wait_ms = 0.0f;
        float store_ms = 0.0f;
        float produce_wait_ms = 0.0f;
        float reduce_ms = 0.0f;
        float consume_ms = 0.0f;
        float total_ms = 0.0f;
        cudaEventElapsedTime(&reuse_wait_ms, ev[0], ev[1]);
        cudaEventElapsedTime(&store_ms, ev[1], ev[2]);
        cudaEventElapsedTime(&produce_wait_ms, ev[2], ev[3]);
        cudaEventElapsedTime(&reduce_ms, ev[3], ev[4]);
        cudaEventElapsedTime(&consume_ms, ev[4], ev[5]);
        cudaEventElapsedTime(&total_ms, ev[0], ev[5]);
        std::fprintf(
            stderr,
            "MOONCAKE_PG_RS_SLOTTED_GRAPH_PROFILE rank=%d ranks=%d "
            "elements=%zu slot_stride=%zu reuse_wait_us=%.3f "
            "store_us=%.3f produce_wait_us=%.3f reduce_us=%.3f "
            "consume_us=%.3f total_us=%.3f\n",
            rank, numRanks, numElements, slotStrideBytes,
            reuse_wait_ms * 1000.0f, store_ms * 1000.0f,
            produce_wait_ms * 1000.0f, reduce_ms * 1000.0f,
            consume_ms * 1000.0f, total_ms * 1000.0f);
        for (auto& event : ev) {
            cudaEventDestroy(event);
        }
    }
#endif
}

template <typename scalar_t>
void launchDeviceApiReduceScatterSlottedGraphKernelTyped(
    scalar_t* output, const scalar_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* p2p_available, bool* active_mask, void* raddrs,
    void* rkeys, void* qp_devctxs, int qps_per_rank, size_t numElements,
    size_t slotStrideBytes, size_t rdmaSendBaseOffset, int slots, int rank,
    int numRanks, const uint32_t* baseSequenceSlot, uint32_t* sequenceCounter,
    uint32_t reserveIncrement, cudaStream_t stream) {
#ifndef __MUSA__
    const bool profile = deviceApiProfileEnabledForRank(rank);
    cudaEvent_t ev[8]{};
    if (profile) {
        for (auto& event : ev) {
            cudaEventCreate(&event);
        }
        cudaEventRecord(ev[0], stream);
    }
#endif
    constexpr int threads = 256;
    int blocks_x = static_cast<int>((numElements + threads - 1) / threads);
    const int store_blocks_x = clampP2pReduceScatterStoreBlocks(blocks_x);
    const int reduce_blocks_x = clampP2pReduceScatterReduceBlocks(blocks_x);
    dim3 store_grid(store_blocks_x, numRanks, 1);
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    const size_t atomicScratchBytes = static_cast<size_t>(slots) *
                                      kMaxNumRanks * sizeof(uint32_t);
    const size_t rdmaAtomicOffset = consumeSignalOffset - atomicScratchBytes;
    constexpr uint32_t chunkIndex = 0;
    const bool debug_signal = deviceApiSignalDebugEnabled();
    const bool poll_completion = deviceApiCompletionPollEnabled();

    deviceApiSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), active_mask,
        consumeSignalOffset, baseSequenceSlot, chunkIndex, slots, numRanks,
        sequenceCounter, reserveIncrement);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[1], stream);
#endif
    p2pReduceScatterStoreToSlottedScratchDeviceApiGraphKernel<<<
        store_grid, threads, 0, stream>>>(
        input, static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
        numElements, slotStrideBytes, baseSequenceSlot, chunkIndex, slots, rank,
        numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[2], stream);
#endif
    deviceApiReduceScatterStageSendSlottedGraphKernel<scalar_t>
        <<<store_grid, threads, 0, stream>>>(
        input, static_cast<char*>(local_recv_base), numElements,
        slotStrideBytes, rdmaSendBaseOffset, baseSequenceSlot, chunkIndex,
        slots, numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[3], stream);
#endif
    deviceApiReduceScatterPublishSlottedGraphKernel<scalar_t>
        <<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
        active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank, numElements,
        slotStrideBytes, rdmaSendBaseOffset, baseSequenceSlot, chunkIndex,
        slots, rank, numRanks, poll_completion);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[4], stream);
#endif
    deviceApiSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
        active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,
        produceSignalOffset, produceSignalOffset, rdmaAtomicOffset,
        baseSequenceSlot, chunkIndex, slots, rank, numRanks, debug_signal,
        poll_completion);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[5], stream);
#endif
    p2pReduceScatterSlottedLocalReduceGraphKernel<<<reduce_blocks_x, threads, 0,
                                                    stream>>>(
        output, static_cast<const char*>(local_recv_base), numElements,
        slotStrideBytes, baseSequenceSlot, chunkIndex, slots, numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[6], stream);
#endif
    deviceApiSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
        active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,
        consumeSignalOffset, consumeSignalOffset, rdmaAtomicOffset,
        baseSequenceSlot, chunkIndex, slots, rank, numRanks, poll_completion);
#ifndef __MUSA__
    if (profile) {
        cudaEventRecord(ev[7], stream);
        cudaEventSynchronize(ev[7]);
        float us[7]{};
        float total_ms = 0.0f;
        for (int i = 0; i < 7; ++i) {
            float ms = 0.0f;
            cudaEventElapsedTime(&ms, ev[i], ev[i + 1]);
            us[i] = ms * 1000.0f;
        }
        cudaEventElapsedTime(&total_ms, ev[0], ev[7]);
        std::fprintf(
            stderr,
            "MOONCAKE_PG_DEVICE_API_PROFILE op=reduce_scatter rank=%d "
            "ranks=%d elements=%zu slot_stride=%zu reuse_wait_us=%.3f "
            "p2p_store_us=%.3f stage_send_us=%.3f rdma_publish_us=%.3f "
            "produce_wait_us=%.3f local_reduce_us=%.3f consume_us=%.3f "
            "total_us=%.3f\n",
            rank, numRanks, numElements, slotStrideBytes, us[0], us[1],
            us[2], us[3], us[4], us[5], us[6], total_ms * 1000.0f);
        for (auto& event : ev) {
            cudaEventDestroy(event);
        }
    }
#endif
}

#define DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH(scalar_t, suffix)                    \
    void launchP2pReduceScatterSlottedGraphKernel_##suffix(                  \
        scalar_t* output, const scalar_t* input, void* local_recv_base,       \
        void** peer_ptrs, int32_t* available, size_t numElements,             \
        size_t slotStrideBytes, int slots, int rank, int numRanks,            \
        const uint32_t* baseSequenceSlot, uint32_t* sequenceCounter,          \
        uint32_t reserveIncrement, cudaStream_t stream) {                    \
        launchP2pReduceScatterSlottedGraphKernelTyped(                       \
            output, input, local_recv_base, peer_ptrs, available, numElements,\
            slotStrideBytes, slots, rank, numRanks, baseSequenceSlot,         \
            sequenceCounter, reserveIncrement, stream);                      \
    }

DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH(uint8_t, uint8)
DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH(int8_t, int8)
DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH(int16_t, int16)
DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH(int, int32)
DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH(int64_t, int64)
DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH(float, float)
DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH(double, double)
DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH(bool, bool)

#undef DEF_LAUNCH_P2P_RS_SLOTTED_GRAPH

#define DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH(scalar_t, suffix)             \
    void launchDeviceApiReduceScatterSlottedGraphKernel_##suffix(            \
        scalar_t* output, const scalar_t* input, void* local_recv_base,       \
        void** peer_ptrs, int32_t* p2p_available, bool* active_mask,          \
        void* raddrs, void* rkeys, void* qp_devctxs, int qps_per_rank,        \
        size_t numElements, size_t slotStrideBytes,                           \
        size_t rdmaSendBaseOffset, int slots, int rank, int numRanks,         \
        const uint32_t* baseSequenceSlot, uint32_t* sequenceCounter,          \
        uint32_t reserveIncrement, cudaStream_t stream) {                    \
        launchDeviceApiReduceScatterSlottedGraphKernelTyped(                 \
            output, input, local_recv_base, peer_ptrs, p2p_available,         \
            active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,            \
            numElements, slotStrideBytes, rdmaSendBaseOffset, slots, rank,   \
            numRanks, baseSequenceSlot, sequenceCounter, reserveIncrement,   \
            stream);                                                         \
    }

DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH(uint8_t, uint8)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH(int8_t, int8)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH(int16_t, int16)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH(int, int32)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH(int64_t, int64)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH(float, float)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH(double, double)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH(bool, bool)

#undef DEF_LAUNCH_DEVICE_API_RS_SLOTTED_GRAPH

void launchP2pReduceScatterSlottedGraphKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement,
    cudaStream_t stream) {
#ifdef __MUSA__
    launchP2pReduceScatterSlottedGraphKernelTyped(
        static_cast<mt_bfloat16*>(output), static_cast<const mt_bfloat16*>(input),
        local_recv_base, peer_ptrs, available, numElements, slotStrideBytes,
        slots, rank, numRanks, baseSequenceSlot, sequenceCounter,
        reserveIncrement, stream);
#else
    launchP2pReduceScatterSlottedGraphKernelTyped(
        static_cast<at::BFloat16*>(output), static_cast<const at::BFloat16*>(input),
        local_recv_base, peer_ptrs, available, numElements, slotStrideBytes,
        slots, rank, numRanks, baseSequenceSlot, sequenceCounter,
        reserveIncrement, stream);
#endif
}

void launchDeviceApiReduceScatterSlottedGraphKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, bool* active_mask, void* raddrs, void* rkeys,
    void* qp_devctxs, int qps_per_rank, size_t numElements,
    size_t slotStrideBytes, size_t rdmaSendBaseOffset, int slots, int rank,
    int numRanks, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement,
    cudaStream_t stream) {
#ifdef __MUSA__
    launchDeviceApiReduceScatterSlottedGraphKernelTyped(
        static_cast<mt_bfloat16*>(output), static_cast<const mt_bfloat16*>(input),
        local_recv_base, peer_ptrs, p2p_available, active_mask, raddrs, rkeys,
        qp_devctxs, qps_per_rank, numElements, slotStrideBytes,
        rdmaSendBaseOffset, slots, rank, numRanks, baseSequenceSlot,
        sequenceCounter, reserveIncrement, stream);
#else
    launchDeviceApiReduceScatterSlottedGraphKernelTyped(
        static_cast<at::BFloat16*>(output), static_cast<const at::BFloat16*>(input),
        local_recv_base, peer_ptrs, p2p_available, active_mask, raddrs, rkeys,
        qp_devctxs, qps_per_rank, numElements, slotStrideBytes,
        rdmaSendBaseOffset, slots, rank, numRanks, baseSequenceSlot,
        sequenceCounter, reserveIncrement, stream);
#endif
}

template <typename scalar_t>
void launchP2pReduceScatterSlottedChunkedGraphKernelTyped(
    scalar_t* output, const scalar_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t fullNumElements,
    size_t chunkElements, size_t slotStrideBytes, int slots, int rank,
    int numRanks, const uint32_t* baseSequenceSlot, cudaStream_t stream) {
    constexpr int threads = 256;
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    uint32_t chunkIndex = 0;
    for (size_t offset = 0; offset < fullNumElements;
         offset += chunkElements, ++chunkIndex) {
        const size_t thisChunk = std::min(chunkElements, fullNumElements - offset);
        int blocks_x = static_cast<int>((thisChunk + threads - 1) / threads);
        const int store_blocks_x = clampP2pReduceScatterStoreBlocks(blocks_x);
        const int reduce_blocks_x = clampP2pReduceScatterReduceBlocks(blocks_x);
        dim3 store_grid(store_blocks_x, numRanks, 1);
        p2pSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<const char*>(local_recv_base), available,
            consumeSignalOffset, baseSequenceSlot, chunkIndex, slots, numRanks,
            nullptr, 0);
        if (useDeviceApiCollectives()) {
            p2pReduceScatterStoreChunkToSlottedScratchDeviceApiGraphKernel<<<
                store_grid, threads, 0, stream>>>(
                input, static_cast<char*>(local_recv_base), peer_ptrs, available,
                fullNumElements, offset, thisChunk, slotStrideBytes,
                baseSequenceSlot, chunkIndex, slots, rank, numRanks);
        } else {
            p2pReduceScatterStoreChunkToSlottedScratchGraphKernel<<<store_grid,
                                                                    threads, 0,
                                                                    stream>>>(
                input, peer_ptrs, available, fullNumElements, offset, thisChunk,
                slotStrideBytes, baseSequenceSlot, chunkIndex, slots, rank,
                numRanks);
        }
        if (useParallelProduceWait()) {
            p2pSlottedProduceWaitGraphParallelKernel<<<1, 32, 0, stream>>>(
                static_cast<char*>(local_recv_base), peer_ptrs, available,
                produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
                numRanks);
        } else if (useDeviceApiCollectives()) {
            p2pSlottedProduceWaitDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
                static_cast<char*>(local_recv_base), peer_ptrs, available,
                produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
                numRanks);
        } else {
            p2pSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
                static_cast<char*>(local_recv_base), peer_ptrs, available,
                produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
                numRanks);
        }
        p2pReduceScatterSlottedLocalReduceChunkGraphKernel<<<reduce_blocks_x,
                                                             threads, 0,
                                                             stream>>>(
            output, static_cast<const char*>(local_recv_base), offset,
            thisChunk, slotStrideBytes, baseSequenceSlot, chunkIndex, slots,
            numRanks);
        if (useDeviceApiCollectives()) {
            p2pSlottedConsumeNotifyDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
                static_cast<char*>(local_recv_base), peer_ptrs, available,
                consumeSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
                numRanks);
        } else {
            p2pSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
                peer_ptrs, available, consumeSignalOffset, baseSequenceSlot,
                chunkIndex, slots, rank, numRanks);
        }
    }
}

#define DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH(scalar_t, suffix)            \
    void launchP2pReduceScatterSlottedChunkedGraphKernel_##suffix(           \
        scalar_t* output, const scalar_t* input, void* local_recv_base,       \
        void** peer_ptrs, int32_t* available, size_t fullNumElements,         \
        size_t chunkElements, size_t slotStrideBytes, int slots, int rank,    \
        int numRanks, const uint32_t* baseSequenceSlot, cudaStream_t stream) {\
        launchP2pReduceScatterSlottedChunkedGraphKernelTyped(                \
            output, input, local_recv_base, peer_ptrs, available,             \
            fullNumElements, chunkElements, slotStrideBytes, slots, rank,     \
            numRanks, baseSequenceSlot, stream);                             \
    }

DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH(uint8_t, uint8)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH(int8_t, int8)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH(int16_t, int16)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH(int, int32)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH(int64_t, int64)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH(float, float)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH(double, double)
DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH(bool, bool)

#undef DEF_LAUNCH_P2P_RS_SLOTTED_CHUNKED_GRAPH

void launchP2pReduceScatterSlottedChunkedGraphKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, cudaStream_t stream) {
#ifdef __MUSA__
    launchP2pReduceScatterSlottedChunkedGraphKernelTyped(
        static_cast<mt_bfloat16*>(output), static_cast<const mt_bfloat16*>(input),
        local_recv_base, peer_ptrs, available, fullNumElements, chunkElements,
        slotStrideBytes, slots, rank, numRanks, baseSequenceSlot, stream);
#else
    launchP2pReduceScatterSlottedChunkedGraphKernelTyped(
        static_cast<at::BFloat16*>(output), static_cast<const at::BFloat16*>(input),
        local_recv_base, peer_ptrs, available, fullNumElements, chunkElements,
        slotStrideBytes, slots, rank, numRanks, baseSequenceSlot, stream);
#endif
}

template <typename scalar_t>
void launchDeviceApiReduceScatterSlottedChunkedGraphKernelTyped(
    scalar_t* output, const scalar_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* p2p_available, bool* active_mask, void* raddrs,
    void* rkeys, void* qp_devctxs, int qps_per_rank, size_t fullNumElements,
    size_t chunkElements, size_t slotStrideBytes, size_t rdmaSendBaseOffset,
    int slots, int rank, int numRanks, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement,
    cudaStream_t stream) {
#ifndef __MUSA__
    const bool profile = deviceApiProfileEnabledForRank(rank);
    cudaEvent_t ev[8]{};
    if (profile) {
        for (auto& event : ev) {
            cudaEventCreate(&event);
        }
    }
#endif
    constexpr int threads = 256;
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    const size_t atomicScratchBytes = static_cast<size_t>(slots) *
                                      kMaxNumRanks * sizeof(uint32_t);
    const size_t rdmaAtomicOffset = consumeSignalOffset - atomicScratchBytes;
    uint32_t chunkIndex = 0;
    const bool debug_signal = deviceApiSignalDebugEnabled();
    const bool poll_completion = deviceApiCompletionPollEnabled();

    for (size_t offset = 0; offset < fullNumElements;
         offset += chunkElements, ++chunkIndex) {
        const size_t thisChunk =
            std::min(chunkElements, fullNumElements - offset);
        int blocks_x = static_cast<int>((thisChunk + threads - 1) / threads);
        const int store_blocks_x = clampP2pReduceScatterStoreBlocks(blocks_x);
        const int reduce_blocks_x = clampP2pReduceScatterReduceBlocks(blocks_x);
        dim3 store_grid(store_blocks_x, numRanks, 1);

#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[0], stream);
#endif
        deviceApiSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<const char*>(local_recv_base), active_mask,
            consumeSignalOffset, baseSequenceSlot, chunkIndex, slots, numRanks,
            sequenceCounter, reserveIncrement);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[1], stream);
#endif
        p2pReduceScatterStoreChunkToSlottedScratchDeviceApiGraphKernel<<<
            store_grid, threads, 0, stream>>>(
            input, static_cast<char*>(local_recv_base), peer_ptrs,
            p2p_available, fullNumElements, offset, thisChunk, slotStrideBytes,
            baseSequenceSlot, chunkIndex, slots, rank, numRanks);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[2], stream);
#endif
        deviceApiReduceScatterStageSendChunkSlottedGraphKernel<scalar_t>
            <<<store_grid, threads, 0, stream>>>(
            input, static_cast<char*>(local_recv_base), fullNumElements, offset,
            thisChunk, slotStrideBytes, rdmaSendBaseOffset, baseSequenceSlot,
            chunkIndex, slots, numRanks);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[3], stream);
#endif
        deviceApiReduceScatterPublishSlottedGraphKernel<scalar_t>
            <<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
            active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank, thisChunk,
            slotStrideBytes, rdmaSendBaseOffset, baseSequenceSlot, chunkIndex,
            slots, rank, numRanks, poll_completion);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[4], stream);
#endif
        deviceApiSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
            active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,
            produceSignalOffset, produceSignalOffset, rdmaAtomicOffset,
            baseSequenceSlot, chunkIndex, slots, rank, numRanks, debug_signal,
            poll_completion);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[5], stream);
#endif
        p2pReduceScatterSlottedLocalReduceChunkGraphKernel<<<
            reduce_blocks_x, threads, 0, stream>>>(
            output, static_cast<const char*>(local_recv_base), offset,
            thisChunk, slotStrideBytes, baseSequenceSlot, chunkIndex, slots,
            numRanks);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[6], stream);
#endif
        deviceApiSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
            active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,
            consumeSignalOffset, consumeSignalOffset, rdmaAtomicOffset,
            baseSequenceSlot, chunkIndex, slots, rank, numRanks,
            poll_completion);
#ifndef __MUSA__
        if (profile) {
            cudaEventRecord(ev[7], stream);
            cudaEventSynchronize(ev[7]);
            float us[7]{};
            float total_ms = 0.0f;
            for (int i = 0; i < 7; ++i) {
                float ms = 0.0f;
                cudaEventElapsedTime(&ms, ev[i], ev[i + 1]);
                us[i] = ms * 1000.0f;
            }
            cudaEventElapsedTime(&total_ms, ev[0], ev[7]);
            std::fprintf(
                stderr,
                "MOONCAKE_PG_DEVICE_API_PROFILE op=reduce_scatter_chunked "
                "rank=%d ranks=%d chunk=%u offset_elements=%zu "
                "chunk_elements=%zu full_elements=%zu slot_stride=%zu "
                "reuse_wait_us=%.3f p2p_store_us=%.3f stage_send_us=%.3f "
                "rdma_publish_us=%.3f produce_wait_us=%.3f "
                "local_reduce_us=%.3f consume_us=%.3f total_us=%.3f\n",
                rank, numRanks, chunkIndex, offset, thisChunk, fullNumElements,
                slotStrideBytes, us[0], us[1], us[2], us[3], us[4], us[5],
                us[6], total_ms * 1000.0f);
        }
#endif
    }
#ifndef __MUSA__
    if (profile) {
        for (auto& event : ev) {
            cudaEventDestroy(event);
        }
    }
#endif
}

#define DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH(scalar_t, suffix)     \
    void launchDeviceApiReduceScatterSlottedChunkedGraphKernel_##suffix(     \
        scalar_t* output, const scalar_t* input, void* local_recv_base,       \
        void** peer_ptrs, int32_t* p2p_available, bool* active_mask,          \
        void* raddrs, void* rkeys, void* qp_devctxs, int qps_per_rank,        \
        size_t fullNumElements, size_t chunkElements,                         \
        size_t slotStrideBytes, size_t rdmaSendBaseOffset, int slots,         \
        int rank, int numRanks, const uint32_t* baseSequenceSlot,             \
        uint32_t* sequenceCounter, uint32_t reserveIncrement,                 \
        cudaStream_t stream) {                                                \
        launchDeviceApiReduceScatterSlottedChunkedGraphKernelTyped(          \
            output, input, local_recv_base, peer_ptrs, p2p_available,         \
            active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,            \
            fullNumElements, chunkElements, slotStrideBytes,                  \
            rdmaSendBaseOffset, slots, rank, numRanks, baseSequenceSlot,     \
            sequenceCounter, reserveIncrement, stream);                      \
    }

DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH(uint8_t, uint8)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH(int8_t, int8)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH(int16_t, int16)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH(int, int32)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH(int64_t, int64)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH(float, float)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH(double, double)
DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH(bool, bool)

#undef DEF_LAUNCH_DEVICE_API_RS_SLOTTED_CHUNKED_GRAPH

void launchDeviceApiReduceScatterSlottedChunkedGraphKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, bool* active_mask, void* raddrs, void* rkeys,
    void* qp_devctxs, int qps_per_rank, size_t fullNumElements,
    size_t chunkElements, size_t slotStrideBytes, size_t rdmaSendBaseOffset,
    int slots, int rank, int numRanks, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement,
    cudaStream_t stream) {
#ifdef __MUSA__
    launchDeviceApiReduceScatterSlottedChunkedGraphKernelTyped(
        static_cast<mt_bfloat16*>(output), static_cast<const mt_bfloat16*>(input),
        local_recv_base, peer_ptrs, p2p_available, active_mask, raddrs, rkeys,
        qp_devctxs, qps_per_rank, fullNumElements, chunkElements,
        slotStrideBytes, rdmaSendBaseOffset, slots, rank, numRanks,
        baseSequenceSlot, sequenceCounter, reserveIncrement, stream);
#else
    launchDeviceApiReduceScatterSlottedChunkedGraphKernelTyped(
        static_cast<at::BFloat16*>(output), static_cast<const at::BFloat16*>(input),
        local_recv_base, peer_ptrs, p2p_available, active_mask, raddrs, rkeys,
        qp_devctxs, qps_per_rank, fullNumElements, chunkElements,
        slotStrideBytes, rdmaSendBaseOffset, slots, rank, numRanks,
        baseSequenceSlot, sequenceCounter, reserveIncrement, stream);
#endif
}

template <typename scalar_t>
__global__ void deviceApiAllReducePairStageSlottedGraphKernel(
    const scalar_t* tensor, char* local_recv_base, void** peer_ptrs,
    const int32_t* p2p_available, void* raddrs, void* rkeys, void* qp_devctxs,
    int qps_per_rank, size_t offsetElements, size_t chunkElements,
    size_t slotStrideBytes, size_t dataBaseOffset,
    const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks,
    int pairLocalRank, int peerRank, bool poll_completion) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t slotOffset = dataBaseOffset + static_cast<size_t>(slot) * 2 *
                              slotStrideBytes;
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const CommCtx comm_ctx = make_comm_ctx(
        local_recv_base, p2p_available, peer_ptrs, raddrs, rkeys, qp_devctxs,
        local_recv_base, local_recv_base, rank, numRanks, qps_per_rank);

    void* write_dst = mc_route_put(
        comm_ctx, peerRank,
        local_recv_base + slotOffset +
            static_cast<size_t>(pairLocalRank) * slotStrideBytes);
    auto* local_dst = reinterpret_cast<scalar_t*>(
        local_recv_base + slotOffset +
        static_cast<size_t>(pairLocalRank) * slotStrideBytes);
    const scalar_t* src = tensor + offsetElements;

    if (write_dst != nullptr) {
        auto* dst = static_cast<scalar_t*>(write_dst);
        for (size_t i = tid; i < chunkElements; i += stride) {
            dst[i] = src[i];
        }
    } else {
        if (tid == 0) {
            for (size_t i = 0; i < chunkElements; ++i) {
                local_dst[i] = src[i];
            }
            __threadfence_system();
            mc_rdma_put(
                comm_ctx, 0, peerRank, qps_per_rank, local_dst,
                local_recv_base + slotOffset +
                    static_cast<size_t>(pairLocalRank) * slotStrideBytes,
                static_cast<uint32_t>(chunkElements * sizeof(scalar_t)), 0,
                false, poll_completion);
        }
    }
}

__global__ void deviceApiPairProduceWaitGraphKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* p2p_available,
    void* raddrs, void* rkeys, void* qp_devctxs, int qps_per_rank,
    size_t produceSignalOffset, size_t rdmaRemoteSignalBaseOffset,
    size_t rdmaLocalAtomicBaseOffset, const uint32_t* baseSequenceSlot,
    uint32_t chunkIndex, int slots, int rank, int numRanks, int peerRank,
    bool poll_completion) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const CommCtx comm_ctx = make_comm_ctx(
        local_recv_base, p2p_available, peer_ptrs, raddrs, rkeys, qp_devctxs,
        local_recv_base + rdmaLocalAtomicBaseOffset,
        local_recv_base + rdmaRemoteSignalBaseOffset, rank, numRanks,
        qps_per_rank);
    __threadfence_system();
    auto* signal_ptr = reinterpret_cast<int*>(
        local_recv_base + produceSignalOffset +
        (static_cast<size_t>(slot) * kMaxNumRanks + rank) *
            sizeof(uint32_t));
    if (mc_comm_p2p_available(comm_ctx, peerRank)) {
        auto* peer_base = static_cast<char*>(peer_ptrs[peerRank]);
        auto* peer_produce =
            reinterpret_cast<uint32_t*>(peer_base + produceSignalOffset);
        p2pAgSignalStore(peer_produce +
                             static_cast<size_t>(slot) * kMaxNumRanks + rank,
                         sequence, 2);
    } else {
        mc_signal_write(comm_ctx, peerRank, 0, qps_per_rank, signal_ptr,
                        static_cast<int32_t>(sequence), false,
                        poll_completion);
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<uint32_t*>(local_recv_base +
                                                produceSignalOffset);
    auto* signal = produce + static_cast<size_t>(slot) * kMaxNumRanks +
                   peerRank;
    while (p2pAgSignalLoad(signal, 2) < sequence) {
    }
}

__global__ void deviceApiPairReuseWaitGraphKernel(
    const char* local_recv_base, size_t consumeSignalOffset,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots,
    int peerRank) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    if (sequence < static_cast<uint32_t>(slots)) return;
    const uint32_t previousSequence = sequence - static_cast<uint32_t>(slots);
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    auto* consume = reinterpret_cast<const volatile uint32_t*>(
        local_recv_base + consumeSignalOffset);
    while (consume[static_cast<size_t>(slot) * kMaxNumRanks + peerRank] <
           previousSequence) {
    }
}

template <typename scalar_t>
__global__ void deviceApiAllReducePairAddSlottedGraphKernel(
    scalar_t* tensor, const char* local_recv_base, size_t offsetElements,
    size_t chunkElements, size_t slotStrideBytes, size_t dataBaseOffset,
    const uint32_t* baseSequenceSlot, uint32_t chunkIndex, int slots,
    int peerPairLocalRank) {
    const uint32_t sequence = *baseSequenceSlot + chunkIndex;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t slotOffset = dataBaseOffset + static_cast<size_t>(slot) * 2 *
                              slotStrideBytes;
    const auto* src = reinterpret_cast<const scalar_t*>(
        local_recv_base + slotOffset +
        static_cast<size_t>(peerPairLocalRank) * slotStrideBytes);
    scalar_t* dst = tensor + offsetElements;
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    for (size_t i = tid; i < chunkElements; i += stride) {
        dst[i] = dst[i] + src[i];
    }
}

template <typename scalar_t>
void launchDeviceApiAllReducePairExchangeSlottedChunkedGraphKernelTyped(
    scalar_t* tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, void* raddrs, void* rkeys, void* qp_devctxs,
    int qps_per_rank, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, size_t dataBaseOffset, size_t controlBaseOffset,
    int slots, int rank, int numRanks, int pairLocalRank,
    int peerPairLocalRank, int peerRank, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement,
    cudaStream_t stream) {
    constexpr int threads = 256;
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = controlBaseOffset;
    const size_t consumeSignalOffset = produceSignalOffset + signalBytes;
    const size_t atomicScratchBytes = static_cast<size_t>(slots) *
                                      kMaxNumRanks * sizeof(uint32_t);
    const size_t rdmaAtomicOffset = consumeSignalOffset + signalBytes;
    const bool poll_completion = deviceApiCompletionPollEnabled();
    uint32_t chunkIndex = 0;
    for (size_t offset = 0; offset < fullNumElements;
         offset += chunkElements, ++chunkIndex) {
        const size_t thisChunk =
            std::min(chunkElements, fullNumElements - offset);
        int blocks_x = static_cast<int>((thisChunk + threads - 1) / threads);
        blocks_x = blocks_x < 1 ? 1 : (blocks_x > 512 ? 512 : blocks_x);
        if (chunkIndex == 0 && sequenceCounter != nullptr) {
            launchP2pReserveSequence(sequenceCounter,
                                     const_cast<uint32_t*>(baseSequenceSlot),
                                     reserveIncrement, stream);
        }
        deviceApiPairReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<const char*>(local_recv_base), consumeSignalOffset,
            baseSequenceSlot, chunkIndex, slots, peerRank);
        deviceApiAllReducePairStageSlottedGraphKernel<scalar_t>
            <<<blocks_x, threads, 0, stream>>>(
            tensor, static_cast<char*>(local_recv_base), peer_ptrs,
            p2p_available, raddrs, rkeys, qp_devctxs, qps_per_rank, offset,
            thisChunk, slotStrideBytes, dataBaseOffset, baseSequenceSlot,
            chunkIndex, slots, rank, numRanks, pairLocalRank, peerRank,
            poll_completion);
        deviceApiPairProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
            raddrs, rkeys, qp_devctxs, qps_per_rank, produceSignalOffset,
            produceSignalOffset, rdmaAtomicOffset, baseSequenceSlot,
            chunkIndex, slots, rank, numRanks, peerRank, poll_completion);
        deviceApiAllReducePairAddSlottedGraphKernel<scalar_t>
            <<<blocks_x, threads, 0, stream>>>(
            tensor, static_cast<const char*>(local_recv_base), offset,
            thisChunk, slotStrideBytes, dataBaseOffset, baseSequenceSlot,
            chunkIndex, slots, peerPairLocalRank);
        deviceApiPairProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
            raddrs, rkeys, qp_devctxs, qps_per_rank, consumeSignalOffset,
            consumeSignalOffset, rdmaAtomicOffset, baseSequenceSlot,
            chunkIndex, slots, rank, numRanks, peerRank, poll_completion);
    }
}

void launchDeviceApiAllReducePairExchangeSlottedChunkedGraphKernel_float(
    float* tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, void* raddrs, void* rkeys, void* qp_devctxs,
    int qps_per_rank, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, size_t dataBaseOffset, size_t controlBaseOffset,
    int slots, int rank, int numRanks, int pairLocalRank,
    int peerPairLocalRank, int peerRank, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement,
    cudaStream_t stream) {
    launchDeviceApiAllReducePairExchangeSlottedChunkedGraphKernelTyped(
        tensor, local_recv_base, peer_ptrs, p2p_available, raddrs, rkeys,
        qp_devctxs, qps_per_rank, fullNumElements, chunkElements,
        slotStrideBytes, dataBaseOffset, controlBaseOffset, slots, rank,
        numRanks, pairLocalRank, peerPairLocalRank, peerRank, baseSequenceSlot,
        sequenceCounter, reserveIncrement, stream);
}

void launchDeviceApiAllReducePairExchangeSlottedChunkedGraphKernel_bf16(
    void* tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, void* raddrs, void* rkeys, void* qp_devctxs,
    int qps_per_rank, size_t fullNumElements, size_t chunkElements,
    size_t slotStrideBytes, size_t dataBaseOffset, size_t controlBaseOffset,
    int slots, int rank, int numRanks, int pairLocalRank,
    int peerPairLocalRank, int peerRank, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement,
    cudaStream_t stream) {
#ifdef __MUSA__
    launchDeviceApiAllReducePairExchangeSlottedChunkedGraphKernelTyped(
        static_cast<mt_bfloat16*>(tensor), local_recv_base, peer_ptrs,
        p2p_available, raddrs, rkeys, qp_devctxs, qps_per_rank,
        fullNumElements, chunkElements, slotStrideBytes, dataBaseOffset,
        controlBaseOffset, slots, rank, numRanks, pairLocalRank,
        peerPairLocalRank, peerRank, baseSequenceSlot, sequenceCounter,
        reserveIncrement, stream);
#else
    launchDeviceApiAllReducePairExchangeSlottedChunkedGraphKernelTyped(
        static_cast<at::BFloat16*>(tensor), local_recv_base, peer_ptrs,
        p2p_available, raddrs, rkeys, qp_devctxs, qps_per_rank,
        fullNumElements, chunkElements, slotStrideBytes, dataBaseOffset,
        controlBaseOffset, slots, rank, numRanks, pairLocalRank,
        peerPairLocalRank, peerRank, baseSequenceSlot, sequenceCounter,
        reserveIncrement, stream);
#endif
}

template <typename scalar_t>
__global__ void p2pAllReduceStoreToSlottedScratchKernel(
    const scalar_t* input, void** peer_ptrs, const int32_t* available,
    size_t numElements, size_t slotStrideBytes, int slot, int rank,
    int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;
    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(peer_ptrs[peer]);
        auto* dst = reinterpret_cast<scalar_t*>(
            peer_base + slotOffset + static_cast<size_t>(rank) * slotStrideBytes);
        for (size_t i = tid; i < numElements; i += stride) {
            dst[i] = input[i];
        }
    }
}

template <typename scalar_t>
__global__ void p2pAllReduceStoreToSlottedScratchDeviceApiKernel(
    const scalar_t* input, char* local_recv_base, void** peer_ptrs,
    const int32_t* available, size_t numElements, size_t slotStrideBytes,
    int slot, int rank, int numRanks) {
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(numRanks) * slotStrideBytes;
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, rank, numRanks, 1);
    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        void* write_dst = mc_route_put(
            comm_ctx, peer,
            local_recv_base + slotOffset +
                static_cast<size_t>(rank) * slotStrideBytes);
        if (write_dst == nullptr) continue;
        auto* dst = static_cast<scalar_t*>(write_dst);
        for (size_t i = tid; i < numElements; i += stride) {
            dst[i] = input[i];
        }
    }
}

template <typename scalar_t>
__global__ void p2pNodeLocalAllReduceStoreToSlottedScratchDeviceApiKernel(
    const scalar_t* input, char* local_recv_base, void** peer_ptrs,
    const int32_t* available, size_t numElements, size_t slotStrideBytes,
    const uint32_t* baseSequenceSlot, int slots, int globalRank,
    int nodeBaseRank, int localRank, int nodeSize) {
    const uint32_t sequence = *baseSequenceSlot;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t slotOffset = static_cast<size_t>(slot) *
                              static_cast<size_t>(nodeSize) * slotStrideBytes;
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, globalRank, kMaxNumRanks, 1);
    for (int localPeer = blockIdx.y; localPeer < nodeSize;
         localPeer += gridDim.y) {
        const int globalPeer = nodeBaseRank + localPeer;
        if (!available[globalPeer]) continue;
        void* write_dst = mc_route_put(
            comm_ctx, globalPeer,
            local_recv_base + slotOffset +
                static_cast<size_t>(localRank) * slotStrideBytes);
        if (write_dst == nullptr) continue;
        auto* dst = static_cast<scalar_t*>(write_dst);
        for (size_t i = tid; i < numElements; i += stride) {
            dst[i] = input[i];
        }
    }
}

__global__ void p2pNodeLocalSlottedReuseWaitKernel(
    const char* local_recv_base, const int32_t* available,
    size_t consumeSignalOffset, const uint32_t* baseSequenceSlot,
    int nodeBaseRank, int nodeSize, int slots) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot;
    if (sequence < static_cast<uint32_t>(slots)) return;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));

    const uint32_t previousSequence = sequence - static_cast<uint32_t>(slots);
    auto* consume = reinterpret_cast<const volatile uint32_t*>(
        local_recv_base + consumeSignalOffset);
    for (int localPeer = 0; localPeer < nodeSize; ++localPeer) {
        const int globalPeer = nodeBaseRank + localPeer;
        if (!available[globalPeer]) continue;
        while (consume[static_cast<size_t>(slot) * kMaxNumRanks + localPeer] <
               previousSequence) {
        }
    }
}

__global__ void p2pNodeLocalSlottedProduceWaitDeviceApiKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, const uint32_t* baseSequenceSlot, int slots,
    int globalRank, int nodeBaseRank, int localRank, int nodeSize) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, globalRank, kMaxNumRanks, 1);
    __threadfence_system();
    auto* signal_ptr = reinterpret_cast<uint32_t*>(
        local_recv_base + produceSignalOffset +
        (static_cast<size_t>(slot) * kMaxNumRanks + localRank) *
            sizeof(uint32_t));
    for (int localPeer = 0; localPeer < nodeSize; ++localPeer) {
        const int globalPeer = nodeBaseRank + localPeer;
        if (!available[globalPeer]) continue;
        if (globalPeer == globalRank) {
            p2pAgSignalStore(signal_ptr, sequence, 2);
        } else if (mc_comm_p2p_available(comm_ctx, globalPeer)) {
            auto* peer_base = static_cast<char*>(peer_ptrs[globalPeer]);
            auto* peer_produce =
                reinterpret_cast<uint32_t*>(peer_base + produceSignalOffset);
            p2pAgSignalStore(peer_produce +
                                 static_cast<size_t>(slot) * kMaxNumRanks +
                                 localRank,
                             sequence, 2);
        }
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<volatile uint32_t*>(
        local_recv_base + produceSignalOffset);
    for (int localPeer = 0; localPeer < nodeSize; ++localPeer) {
        const int globalPeer = nodeBaseRank + localPeer;
        if (!available[globalPeer]) continue;
        while (produce[static_cast<size_t>(slot) * kMaxNumRanks + localPeer] <
               sequence) {
        }
    }
}

__global__ void p2pNodeLocalSlottedConsumeNotifyDeviceApiKernel(
    char* local_recv_base, void** peer_ptrs, const int32_t* available,
    size_t consumeSignalOffset, const uint32_t* baseSequenceSlot, int slots,
    int globalRank, int nodeBaseRank, int localRank, int nodeSize) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *baseSequenceSlot;
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const CommCtx comm_ctx = make_comm_ctx(local_recv_base, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, globalRank, kMaxNumRanks, 1);
    __threadfence_system();
    auto* signal_ptr = reinterpret_cast<uint32_t*>(
        local_recv_base + consumeSignalOffset +
        (static_cast<size_t>(slot) * kMaxNumRanks + localRank) *
            sizeof(uint32_t));
    for (int localPeer = 0; localPeer < nodeSize; ++localPeer) {
        const int globalPeer = nodeBaseRank + localPeer;
        if (!available[globalPeer]) continue;
        if (globalPeer == globalRank) {
            p2pAgSignalStore(signal_ptr, sequence, 2);
        } else if (mc_comm_p2p_available(comm_ctx, globalPeer)) {
            auto* peer_base = static_cast<char*>(peer_ptrs[globalPeer]);
            auto* peer_consume =
                reinterpret_cast<uint32_t*>(peer_base + consumeSignalOffset);
            p2pAgSignalStore(peer_consume +
                                 static_cast<size_t>(slot) * kMaxNumRanks +
                                 localRank,
                             sequence, 2);
        }
    }
    __threadfence_system();
}

template <typename scalar_t>
__global__ void p2pAllReduceFusedPublishReducedToAgScratchDeviceApiGraphKernel(
    const char* local_recv_base, char* gdr_buffer, void** peer_ptrs,
    const int32_t* available, size_t segmentElements, size_t slotStrideBytes,
    const uint32_t* rsSequenceSlot, const uint32_t* agSequenceSlot, int slots,
    int rank, int numRanks) {
    const uint32_t rsSequence = *rsSequenceSlot;
    const uint32_t agSequence = *agSequenceSlot;
    const int rsSlot =
        static_cast<int>(rsSequence % static_cast<uint32_t>(slots));
    const int agSlot =
        static_cast<int>(agSequence % static_cast<uint32_t>(slots));
    const size_t tid =
        static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
    const size_t rsSlotOffset = static_cast<size_t>(rsSlot) *
                                static_cast<size_t>(numRanks) *
                                slotStrideBytes;
    const size_t agSlotOffset = static_cast<size_t>(agSlot) *
                                static_cast<size_t>(numRanks) *
                                slotStrideBytes;
    const auto* src = reinterpret_cast<const scalar_t*>(
        local_recv_base + rsSlotOffset +
        static_cast<size_t>(rank) * slotStrideBytes);
    const CommCtx comm_ctx = make_comm_ctx(gdr_buffer, available, peer_ptrs,
                                           nullptr, nullptr, nullptr, nullptr,
                                           nullptr, rank, numRanks, 1);

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        void* write_dst = mc_route_put(
            comm_ctx, peer,
            gdr_buffer + agSlotOffset +
                static_cast<size_t>(rank) * slotStrideBytes);
        if (write_dst == nullptr) continue;
        auto* dst = static_cast<scalar_t*>(write_dst);
        for (size_t i = tid; i < segmentElements; i += stride) {
            dst[i] = src[i];
        }
    }
}

template <typename scalar_t>
void launchP2pAllReduceFusedRsAgSlottedGraphKernelTyped(
    scalar_t* tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, const uint32_t* rsSequenceSlot,
    const uint32_t* agSequenceSlot, cudaStream_t stream) {
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    constexpr int threads = 256;
    int blocks_x = static_cast<int>((numElements + threads - 1) / threads);
    blocks_x = clampP2pReduceScatterStoreBlocks(blocks_x);
    const int reduce_blocks_x =
        clampP2pReduceScatterReduceBlocks(blocks_x);
    dim3 rs_store_grid(blocks_x, numRanks, 1);
    dim3 reduce_grid(reduce_blocks_x, 1, 1);
    dim3 publish_grid(blocks_x, numRanks, 1);
    dim3 ag_copy_grid(clampP2pAllgatherBlocks(blocks_x), numRanks, 1);

    p2pSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available,
        consumeSignalOffset, rsSequenceSlot, 0, slots, numRanks, nullptr, 0);
    p2pSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available,
        consumeSignalOffset, agSequenceSlot, 0, slots, numRanks, nullptr, 0);
    if (useDeviceApiCollectives()) {
        p2pReduceScatterStoreToSlottedScratchDeviceApiGraphKernel<<<
            rs_store_grid, threads, 0, stream>>>(
            tensor, static_cast<char*>(local_recv_base), peer_ptrs, available,
            numElements, slotStrideBytes, rsSequenceSlot, 0, slots, rank,
            numRanks);
        p2pSlottedProduceWaitDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, rsSequenceSlot, 0, slots, rank, numRanks);
    } else {
        p2pReduceScatterStoreToSlottedScratchGraphKernel<<<rs_store_grid, threads,
                                                           0, stream>>>(
            tensor, peer_ptrs, available, numElements, slotStrideBytes,
            rsSequenceSlot, 0, slots, rank, numRanks);
        p2pSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, rsSequenceSlot, 0, slots, rank, numRanks);
    }
    p2pAllReduceFusedReduceToRsScratchGraphKernel<scalar_t>
        <<<reduce_grid, threads, 0, stream>>>(
        static_cast<char*>(local_recv_base), numElements, slotStrideBytes,
        rsSequenceSlot, slots, rank, numRanks);
    if (useDeviceApiCollectives()) {
        p2pAllReduceFusedPublishReducedToAgScratchDeviceApiGraphKernel<scalar_t>
            <<<publish_grid, threads, 0, stream>>>(
            static_cast<const char*>(local_recv_base),
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            numElements, slotStrideBytes, rsSequenceSlot, agSequenceSlot, slots,
            rank, numRanks);
        p2pSlottedProduceWaitDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, agSequenceSlot, 0, slots, rank, numRanks);
    } else {
        p2pAllReduceFusedPublishReducedToAgScratchGraphKernel<scalar_t>
            <<<publish_grid, threads, 0, stream>>>(
            static_cast<const char*>(local_recv_base), peer_ptrs, available,
            numElements, slotStrideBytes, rsSequenceSlot, agSequenceSlot, slots,
            rank, numRanks);
        p2pSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, agSequenceSlot, 0, slots, rank, numRanks);
    }
    p2pAllgatherStoreSlottedLocalCopyGraphKernel<<<ag_copy_grid, threads, 0,
                                                   stream>>>(
        static_cast<const char*>(local_recv_base), reinterpret_cast<char*>(tensor),
        numElements * sizeof(scalar_t), slotStrideBytes, agSequenceSlot, 0,
        slots, numRanks);
    if (useDeviceApiCollectives()) {
        p2pSlottedConsumeNotifyDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            consumeSignalOffset, rsSequenceSlot, 0, slots, rank, numRanks);
        p2pSlottedConsumeNotifyDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            consumeSignalOffset, agSequenceSlot, 0, slots, rank, numRanks);
    } else {
        p2pSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
            peer_ptrs, available, consumeSignalOffset, rsSequenceSlot, 0, slots,
            rank, numRanks);
        p2pSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
            peer_ptrs, available, consumeSignalOffset, agSequenceSlot, 0, slots,
            rank, numRanks);
    }
}

void launchP2pAllReduceFusedRsAgSlottedGraphKernel_float(
    float* tensor, void* local_recv_base, void** peer_ptrs, int32_t* available,
    size_t numElements, size_t slotStrideBytes, int slots, int rank,
    int numRanks, const uint32_t* rsSequenceSlot,
    const uint32_t* agSequenceSlot, cudaStream_t stream) {
    launchP2pAllReduceFusedRsAgSlottedGraphKernelTyped(
        tensor, local_recv_base, peer_ptrs, available, numElements,
        slotStrideBytes, slots, rank, numRanks, rsSequenceSlot, agSequenceSlot,
        stream);
}

void launchP2pAllReduceFusedRsAgSlottedGraphKernel_bf16(
    void* tensor, void* local_recv_base, void** peer_ptrs, int32_t* available,
    size_t numElements, size_t slotStrideBytes, int slots, int rank,
    int numRanks, const uint32_t* rsSequenceSlot,
    const uint32_t* agSequenceSlot, cudaStream_t stream) {
#ifdef __MUSA__
    launchP2pAllReduceFusedRsAgSlottedGraphKernelTyped(
        static_cast<mt_bfloat16*>(tensor), local_recv_base, peer_ptrs,
        available, numElements, slotStrideBytes, slots, rank, numRanks,
        rsSequenceSlot, agSequenceSlot, stream);
#else
    launchP2pAllReduceFusedRsAgSlottedGraphKernelTyped(
        static_cast<at::BFloat16*>(tensor), local_recv_base, peer_ptrs,
        available, numElements, slotStrideBytes, slots, rank, numRanks,
        rsSequenceSlot, agSequenceSlot, stream);
#endif
}

template <typename scalar_t>
void launchP2pAllReduceSlottedKernelTyped(
    scalar_t* output, const scalar_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream, bool useDeviceApi) {
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    constexpr int threads = 256;
    int blocks_x = static_cast<int>((numElements + threads - 1) / threads);
    blocks_x = blocks_x < 1 ? 1 : (blocks_x > 512 ? 512 : blocks_x);
    dim3 store_grid(blocks_x, numRanks, 1);
    p2pSlottedReuseWaitKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available, consumeSignalOffset,
        slot, numRanks, sequence, slots);
    if (useDeviceApi) {
        p2pAllReduceStoreToSlottedScratchDeviceApiKernel<<<store_grid, threads,
                                                           0, stream>>>(
            input, static_cast<char*>(local_recv_base), peer_ptrs, available,
            numElements, slotStrideBytes, slot, rank, numRanks);
        p2pSlottedProduceWaitDeviceApiKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, slot, rank, numRanks, sequence);
    } else {
        p2pAllReduceStoreToSlottedScratchKernel<<<store_grid, threads, 0, stream>>>(
            input, peer_ptrs, available, numElements, slotStrideBytes, slot, rank,
            numRanks);
        p2pReduceScatterSlottedProduceWaitKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, slot, rank, numRanks, sequence);
    }
    p2pReduceScatterSlottedLocalReduceKernel<<<blocks_x, threads, 0, stream>>>(
        output, static_cast<const char*>(local_recv_base), numElements,
        slotStrideBytes, slot, numRanks);
    if (useDeviceApi) {
        p2pSlottedConsumeNotifyDeviceApiKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            consumeSignalOffset, slot, rank, numRanks, sequence);
    } else {
        p2pSlottedConsumeNotifyKernel<<<1, 1, 0, stream>>>(
            peer_ptrs, available, consumeSignalOffset, slot, rank, numRanks,
            sequence);
    }
}

template <typename scalar_t>
void launchP2pNodeLocalAllReduceSlottedKernelTyped(
    scalar_t* output, const scalar_t* input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t numElements,
    size_t slotStrideBytes, int slots, size_t controlBaseOffset,
    int globalRank, int nodeBaseRank, int localRank, int nodeSize,
    const uint32_t* baseSequenceSlot, cudaStream_t stream) {
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = controlBaseOffset;
    const size_t consumeSignalOffset = produceSignalOffset + signalBytes;
    constexpr int threads = 256;
    int blocks_x = static_cast<int>((numElements + threads - 1) / threads);
    blocks_x = blocks_x < 1 ? 1 : (blocks_x > 512 ? 512 : blocks_x);
    dim3 store_grid(blocks_x, nodeSize, 1);

    p2pNodeLocalSlottedReuseWaitKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available,
        consumeSignalOffset, baseSequenceSlot, nodeBaseRank, nodeSize, slots);
    p2pNodeLocalAllReduceStoreToSlottedScratchDeviceApiKernel<scalar_t>
        <<<store_grid, threads, 0, stream>>>(
        input, static_cast<char*>(local_recv_base), peer_ptrs, available,
        numElements, slotStrideBytes, baseSequenceSlot, slots, globalRank,
        nodeBaseRank, localRank, nodeSize);
    p2pNodeLocalSlottedProduceWaitDeviceApiKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, available,
        produceSignalOffset, baseSequenceSlot, slots, globalRank, nodeBaseRank,
        localRank, nodeSize);
    p2pReduceScatterSlottedLocalReduceGraphKernel<scalar_t>
        <<<blocks_x, threads, 0, stream>>>(
        output, static_cast<const char*>(local_recv_base), numElements,
        slotStrideBytes, baseSequenceSlot, 0, slots, nodeSize);
    p2pNodeLocalSlottedConsumeNotifyDeviceApiKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, available,
        consumeSignalOffset, baseSequenceSlot, slots, globalRank, nodeBaseRank,
        localRank, nodeSize);
}

#define DEF_LAUNCH_P2P_AR_SLOTTED(scalar_t, suffix)                         \
    void launchP2pAllReduceSlottedKernel_##suffix(                           \
        scalar_t* output, const scalar_t* input, void* local_recv_base,       \
        void** peer_ptrs, int32_t* available, size_t numElements,             \
        size_t slotStrideBytes, int slots, int rank, int numRanks,            \
        uint32_t sequence, cudaStream_t stream, bool useDeviceApi) {          \
        launchP2pAllReduceSlottedKernelTyped(                                \
            output, input, local_recv_base, peer_ptrs, available, numElements,\
            slotStrideBytes, slots, rank, numRanks, sequence, stream,         \
            useDeviceApi);                                                    \
    }

DEF_LAUNCH_P2P_AR_SLOTTED(uint8_t, uint8)
DEF_LAUNCH_P2P_AR_SLOTTED(int8_t, int8)
DEF_LAUNCH_P2P_AR_SLOTTED(int16_t, int16)
DEF_LAUNCH_P2P_AR_SLOTTED(int, int32)
DEF_LAUNCH_P2P_AR_SLOTTED(int64_t, int64)
DEF_LAUNCH_P2P_AR_SLOTTED(float, float)
DEF_LAUNCH_P2P_AR_SLOTTED(double, double)
DEF_LAUNCH_P2P_AR_SLOTTED(bool, bool)

#undef DEF_LAUNCH_P2P_AR_SLOTTED

void launchP2pAllReduceSlottedKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream,
    bool useDeviceApi) {
#ifdef __MUSA__
    launchP2pAllReduceSlottedKernelTyped(
        static_cast<mt_bfloat16*>(output), static_cast<const mt_bfloat16*>(input),
        local_recv_base, peer_ptrs, available, numElements, slotStrideBytes,
        slots, rank, numRanks, sequence, stream, useDeviceApi);
#else
    launchP2pAllReduceSlottedKernelTyped(
        static_cast<at::BFloat16*>(output), static_cast<const at::BFloat16*>(input),
        local_recv_base, peer_ptrs, available, numElements, slotStrideBytes,
        slots, rank, numRanks, sequence, stream, useDeviceApi);
#endif
}

void launchP2pNodeLocalAllReduceSlottedKernel_float(
    float* output, const float* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    size_t controlBaseOffset, int globalRank, int nodeBaseRank,
    int localRank, int nodeSize, const uint32_t* baseSequenceSlot,
    cudaStream_t stream) {
    launchP2pNodeLocalAllReduceSlottedKernelTyped(
        output, input, local_recv_base, peer_ptrs, available, numElements,
        slotStrideBytes, slots, controlBaseOffset, globalRank, nodeBaseRank,
        localRank, nodeSize, baseSequenceSlot, stream);
}

void launchP2pNodeLocalAllReduceSlottedKernel_bf16(
    void* output, const void* input, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t numElements, size_t slotStrideBytes, int slots,
    size_t controlBaseOffset, int globalRank, int nodeBaseRank,
    int localRank, int nodeSize, const uint32_t* baseSequenceSlot,
    cudaStream_t stream) {
#ifdef __MUSA__
    launchP2pNodeLocalAllReduceSlottedKernelTyped(
        static_cast<mt_bfloat16*>(output), static_cast<const mt_bfloat16*>(input),
        local_recv_base, peer_ptrs, available, numElements, slotStrideBytes,
        slots, controlBaseOffset, globalRank, nodeBaseRank, localRank, nodeSize,
        baseSequenceSlot, stream);
#else
    launchP2pNodeLocalAllReduceSlottedKernelTyped(
        static_cast<at::BFloat16*>(output), static_cast<const at::BFloat16*>(input),
        local_recv_base, peer_ptrs, available, numElements, slotStrideBytes,
        slots, controlBaseOffset, globalRank, nodeBaseRank, localRank, nodeSize,
        baseSequenceSlot, stream);
#endif
}

__global__ void p2pAllgatherDirectOutputStoreKernel(
    const char* input, void** output_peer_ptrs, const int32_t* available,
    size_t tensorSize, int rank, int numRanks) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;

    if (((reinterpret_cast<uintptr_t>(input) | tensorSize) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(input);
        const size_t words = tensorSize / sizeof(uint64_t);
        for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
            if (!available[peer]) continue;
            auto* peer_output = static_cast<char*>(output_peer_ptrs[peer]);
            auto* dst = peer_output + static_cast<size_t>(rank) * tensorSize;
            if ((reinterpret_cast<uintptr_t>(dst) & (sizeof(uint64_t) - 1)) ==
                0) {
                auto* dst64 = reinterpret_cast<uint64_t*>(dst);
                for (size_t i = tid; i < words; i += stride) {
                    dst64[i] = src64[i];
                }
            } else {
                for (size_t i = tid; i < tensorSize; i += stride) {
                    dst[i] = input[i];
                }
            }
        }
        return;
    }

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        if (!available[peer]) continue;
        auto* peer_output = static_cast<char*>(output_peer_ptrs[peer]);
        auto* dst = peer_output + static_cast<size_t>(rank) * tensorSize;
        for (size_t i = tid; i < tensorSize; i += stride) {
            dst[i] = input[i];
        }
    }
}

__global__ void p2pAllgatherDirectOutputProduceWaitKernel(
    char* local_recv_base, void** scratch_peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, int rank, int numRanks, uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(scratch_peer_ptrs[peer]);
        auto* peerProduce = reinterpret_cast<uint32_t*>(peer_base +
                                                        produceSignalOffset);
        p2pAgSignalStore(peerProduce + rank, sequence, 2);
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<volatile uint32_t*>(local_recv_base +
                                                         produceSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        while (produce[peer] < sequence) {
        }
    }
}

__global__ void p2pAllgatherDirectOutputProduceWaitGraphKernel(
    char* local_recv_base, void** scratch_peer_ptrs, const int32_t* available,
    size_t produceSignalOffset, int rank, int numRanks,
    const uint32_t* sequenceSlot) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    const uint32_t sequence = *sequenceSlot;
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        auto* peer_base = static_cast<char*>(scratch_peer_ptrs[peer]);
        auto* peerProduce = reinterpret_cast<uint32_t*>(peer_base +
                                                        produceSignalOffset);
        p2pAgSignalStore(peerProduce + rank, sequence, 2);
    }
    __threadfence_system();

    auto* produce = reinterpret_cast<volatile uint32_t*>(local_recv_base +
                                                         produceSignalOffset);
    for (int peer = 0; peer < numRanks; ++peer) {
        if (!available[peer]) continue;
        while (produce[peer] < sequence) {
        }
    }
}

void launchP2pAllgatherStoreSignalKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream) {
    constexpr size_t kProduceSignalOffset =
        kBufferSize - 2 * kMaxNumRanks * sizeof(uint32_t);
    constexpr size_t kConsumeSignalOffset =
        kBufferSize - kMaxNumRanks * sizeof(uint32_t);
#ifndef __MUSA__
    const bool profile = [rank] {
        const char* value = std::getenv("MOONCAKE_PG_STORE_SIGNAL_PROFILE");
        if (!value || value[0] == '\0' || value[0] == '0') return false;
        const char* rank_value =
            std::getenv("MOONCAKE_PG_STORE_SIGNAL_PROFILE_RANK");
        if (!rank_value || rank_value[0] == '\0') return true;
        char* end = nullptr;
        long parsed = std::strtol(rank_value, &end, 10);
        return end != rank_value && static_cast<int>(parsed) == rank;
    }();
    cudaEvent_t ev[6]{};
    if (profile) {
        for (auto& event : ev) {
            cudaEventCreate(&event);
        }
        cudaEventRecord(ev[0], stream);
    }
#endif
    p2pAllgatherStoreWaitPrevConsumeKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), kConsumeSignalOffset, numRanks,
        sequence);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[1], stream);
#endif

    constexpr int threads = 256;
    int blocks_x = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    blocks_x = blocks_x < 1 ? 1 : (blocks_x > 512 ? 512 : blocks_x);
    dim3 grid(blocks_x, numRanks, 1);
    p2pAllgatherStoreToScratchKernel<<<grid, threads, 0, stream>>>(
        static_cast<const char*>(input), peer_ptrs, available, tensorSize, rank,
        numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[2], stream);
#endif
    p2pAllgatherStoreProduceWaitKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, available,
        kProduceSignalOffset, rank, numRanks, sequence);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[3], stream);
#endif
    const size_t outputBytes = tensorSize * static_cast<size_t>(numRanks);
    int copy_blocks = static_cast<int>(
        (outputBytes + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    copy_blocks = clampP2pAllgatherBlocks(copy_blocks);
    p2pAllgatherStoreLocalCopyKernel<<<copy_blocks, threads, 0, stream>>>(
        static_cast<const char*>(local_recv_base), static_cast<char*>(output),
        outputBytes);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[4], stream);
#endif
    p2pAllgatherStoreConsumeKernel<<<1, 1, 0, stream>>>(
        peer_ptrs, available, kConsumeSignalOffset, rank, numRanks, sequence);
#ifndef __MUSA__
    if (profile) {
        cudaEventRecord(ev[5], stream);
        cudaEventSynchronize(ev[5]);
        float wait_prev_ms = 0.0f;
        float store_ms = 0.0f;
        float produce_wait_ms = 0.0f;
        float local_copy_ms = 0.0f;
        float consume_ms = 0.0f;
        float total_ms = 0.0f;
        cudaEventElapsedTime(&wait_prev_ms, ev[0], ev[1]);
        cudaEventElapsedTime(&store_ms, ev[1], ev[2]);
        cudaEventElapsedTime(&produce_wait_ms, ev[2], ev[3]);
        cudaEventElapsedTime(&local_copy_ms, ev[3], ev[4]);
        cudaEventElapsedTime(&consume_ms, ev[4], ev[5]);
        cudaEventElapsedTime(&total_ms, ev[0], ev[5]);
        std::fprintf(
            stderr,
            "MOONCAKE_PG_STORE_SIGNAL_PROFILE rank=%d seq=%u ranks=%d "
            "bytes=%zu wait_prev_us=%.3f store_us=%.3f "
            "produce_wait_us=%.3f local_copy_us=%.3f consume_us=%.3f "
            "total_us=%.3f\n",
            rank, sequence, numRanks, tensorSize, wait_prev_ms * 1000.0f,
            store_ms * 1000.0f, produce_wait_ms * 1000.0f,
            local_copy_ms * 1000.0f, consume_ms * 1000.0f,
            total_ms * 1000.0f);
        for (auto& event : ev) {
            cudaEventDestroy(event);
        }
    }
#endif
}

void launchP2pAllgatherStoreSignalSlottedKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream) {
    const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;

#ifndef __MUSA__
    const bool profile = [rank] {
        const char* value = std::getenv("MOONCAKE_PG_STORE_SIGNAL_PROFILE");
        if (!value || value[0] == '\0' || value[0] == '0') return false;
        const char* rank_value =
            std::getenv("MOONCAKE_PG_STORE_SIGNAL_PROFILE_RANK");
        if (!rank_value || rank_value[0] == '\0') return true;
        char* end = nullptr;
        long parsed = std::strtol(rank_value, &end, 10);
        return end != rank_value && static_cast<int>(parsed) == rank;
    }();
    cudaEvent_t ev[4]{};
    if (profile) {
        for (auto& event : ev) {
            cudaEventCreate(&event);
        }
        cudaEventRecord(ev[0], stream);
    }
#endif

    constexpr int threads = 256;
    int blocks_x = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    blocks_x = blocks_x < 1 ? 1 : (blocks_x > 512 ? 512 : blocks_x);
    dim3 store_grid(blocks_x, numRanks, 1);
    p2pSlottedReuseWaitKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available, consumeSignalOffset,
        slot, numRanks, sequence, slots);
    p2pAllgatherStoreToSlottedScratchKernel<<<store_grid, threads, 0, stream>>>(
        static_cast<const char*>(input), peer_ptrs, available, tensorSize,
        slotStride, slot, rank, numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[1], stream);
#endif

    p2pAllgatherStoreSlottedProduceWaitKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, available,
        produceSignalOffset, slot, rank, numRanks, sequence);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[2], stream);
#endif

    int copy_blocks = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    copy_blocks = copy_blocks < 1 ? 1 : (copy_blocks > 512 ? 512 : copy_blocks);
    dim3 copy_grid(copy_blocks, numRanks, 1);
    p2pAllgatherStoreSlottedLocalCopyKernel<<<copy_grid, threads, 0, stream>>>(
        static_cast<const char*>(local_recv_base), static_cast<char*>(output),
        tensorSize, slotStride, slot, numRanks);
    p2pSlottedConsumeNotifyKernel<<<1, 1, 0, stream>>>(
        peer_ptrs, available, consumeSignalOffset, slot, rank, numRanks,
        sequence);
#ifndef __MUSA__
    if (profile) {
        cudaEventRecord(ev[3], stream);
        cudaEventSynchronize(ev[3]);
        float store_ms = 0.0f;
        float produce_wait_ms = 0.0f;
        float local_copy_ms = 0.0f;
        float total_ms = 0.0f;
        cudaEventElapsedTime(&store_ms, ev[0], ev[1]);
        cudaEventElapsedTime(&produce_wait_ms, ev[1], ev[2]);
        cudaEventElapsedTime(&local_copy_ms, ev[2], ev[3]);
        cudaEventElapsedTime(&total_ms, ev[0], ev[3]);
        std::fprintf(
            stderr,
            "MOONCAKE_PG_STORE_SIGNAL_SLOTTED_PROFILE rank=%d seq=%u "
            "slot=%d slots=%d ranks=%d bytes=%zu slot_stride=%zu "
            "store_us=%.3f produce_wait_us=%.3f local_copy_us=%.3f "
            "total_us=%.3f\n",
            rank, sequence, slot, slots, numRanks, tensorSize, slotStride,
            store_ms * 1000.0f, produce_wait_ms * 1000.0f,
            local_copy_ms * 1000.0f, total_ms * 1000.0f);
        for (auto& event : ev) {
            cudaEventDestroy(event);
        }
    }
#endif
}

void launchP2pAllgatherStoreSignalSlottedChunkedKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t chunkBytes,
    size_t slotStride, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream) {
    constexpr int threads = 256;
    size_t chunkIndex = 0;
    for (size_t offset = 0; offset < tensorSize;
         offset += chunkBytes, ++chunkIndex) {
        const size_t thisChunk = std::min(chunkBytes, tensorSize - offset);
        const uint32_t sequence =
            baseSequence + static_cast<uint32_t>(chunkIndex);
        const int slot = static_cast<int>(sequence % static_cast<uint32_t>(slots));
        const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                                   sizeof(uint32_t);
        const size_t produceSignalOffset = kBufferSize - signalBytes;
        const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
        int blocks_x = static_cast<int>(
            (thisChunk + threads * sizeof(uint64_t) - 1) /
            (threads * sizeof(uint64_t)));
        blocks_x = clampP2pAllgatherBlocks(blocks_x);
        dim3 store_grid(blocks_x, numRanks, 1);
        p2pSlottedReuseWaitKernel<<<1, 1, 0, stream>>>(
            static_cast<const char*>(local_recv_base), available,
            consumeSignalOffset, slot, numRanks, sequence, slots);
        p2pAllgatherStoreToSlottedScratchKernel<<<store_grid, threads, 0,
                                                  stream>>>(
            static_cast<const char*>(input) + offset, peer_ptrs, available,
            thisChunk, slotStride, slot, rank, numRanks);
        p2pAllgatherStoreSlottedProduceWaitKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, slot, rank, numRanks, sequence);

        int copy_blocks = static_cast<int>(
            (thisChunk + threads * sizeof(uint64_t) - 1) /
            (threads * sizeof(uint64_t)));
        copy_blocks = clampP2pAllgatherBlocks(copy_blocks);
        dim3 copy_grid(copy_blocks, numRanks, 1);
        p2pAllgatherStoreSlottedLocalCopyChunkKernel<<<copy_grid, threads, 0,
                                                       stream>>>(
            static_cast<const char*>(local_recv_base), static_cast<char*>(output),
            thisChunk, tensorSize, offset, slotStride, slot, numRanks);
        p2pSlottedConsumeNotifyKernel<<<1, 1, 0, stream>>>(
            peer_ptrs, available, consumeSignalOffset, slot, rank, numRanks,
            sequence);
    }
}

void launchP2pAllgatherStoreSignalSlottedGraphKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    int rank, int numRanks, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement, cudaStream_t stream) {
#ifndef __MUSA__
    const bool profile = [rank] {
        const char* value = std::getenv("MOONCAKE_PG_STORE_SIGNAL_PROFILE");
        if (!value || value[0] == '\0' || value[0] == '0') return false;
        const char* rank_value =
            std::getenv("MOONCAKE_PG_STORE_SIGNAL_PROFILE_RANK");
        if (!rank_value || rank_value[0] == '\0') return true;
        char* end = nullptr;
        long parsed = std::strtol(rank_value, &end, 10);
        return end != rank_value && static_cast<int>(parsed) == rank;
    }();
    cudaEvent_t ev[6]{};
    if (profile) {
        for (auto& event : ev) {
            cudaEventCreate(&event);
        }
        cudaEventRecord(ev[0], stream);
    }
#endif
    constexpr int threads = 256;
    int blocks_x = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    blocks_x = clampP2pAllgatherBlocks(blocks_x);
    dim3 store_grid(blocks_x, numRanks, 1);
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    constexpr uint32_t chunkIndex = 0;
    p2pSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available, consumeSignalOffset,
        baseSequenceSlot, chunkIndex, slots, numRanks, sequenceCounter,
        reserveIncrement);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[1], stream);
#endif
    if (useDeviceApiCollectives()) {
        p2pAllgatherStoreToSlottedScratchDeviceApiGraphKernel<<<store_grid,
                                                                 threads, 0,
                                                                 stream>>>(
            static_cast<const char*>(input), static_cast<char*>(local_recv_base),
            peer_ptrs, available, tensorSize, slotStride, baseSequenceSlot,
            chunkIndex, slots, rank, numRanks);
    } else {
        p2pAllgatherStoreToSlottedScratchGraphKernel<<<store_grid, threads, 0,
                                                       stream>>>(
            static_cast<const char*>(input), peer_ptrs, available, tensorSize,
            slotStride, baseSequenceSlot, chunkIndex, slots, rank, numRanks);
    }
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[2], stream);
#endif
    if (useParallelProduceWait()) {
        p2pSlottedProduceWaitGraphParallelKernel<<<1, 32, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
    } else if (useDeviceApiCollectives()) {
        p2pSlottedProduceWaitDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
    } else {
        p2pSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
    }
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[3], stream);
#endif
    int copy_blocks = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    copy_blocks = clampP2pAllgatherBlocks(copy_blocks);
    dim3 copy_grid(copy_blocks, numRanks, 1);
    p2pAllgatherStoreSlottedLocalCopyGraphKernel<<<copy_grid, threads, 0,
                                                   stream>>>(
        static_cast<const char*>(local_recv_base), static_cast<char*>(output),
        tensorSize, slotStride, baseSequenceSlot, chunkIndex, slots, numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[4], stream);
#endif
    if (useDeviceApiCollectives()) {
        p2pSlottedConsumeNotifyDeviceApiGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            consumeSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
    } else {
        p2pSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
            peer_ptrs, available, consumeSignalOffset, baseSequenceSlot,
            chunkIndex, slots, rank, numRanks);
    }
#ifndef __MUSA__
    if (profile) {
        cudaEventRecord(ev[5], stream);
        cudaEventSynchronize(ev[5]);
        float reuse_wait_ms = 0.0f;
        float store_ms = 0.0f;
        float produce_wait_ms = 0.0f;
        float local_copy_ms = 0.0f;
        float consume_ms = 0.0f;
        float total_ms = 0.0f;
        cudaEventElapsedTime(&reuse_wait_ms, ev[0], ev[1]);
        cudaEventElapsedTime(&store_ms, ev[1], ev[2]);
        cudaEventElapsedTime(&produce_wait_ms, ev[2], ev[3]);
        cudaEventElapsedTime(&local_copy_ms, ev[3], ev[4]);
        cudaEventElapsedTime(&consume_ms, ev[4], ev[5]);
        cudaEventElapsedTime(&total_ms, ev[0], ev[5]);
        std::fprintf(
            stderr,
            "MOONCAKE_PG_STORE_SIGNAL_SLOTTED_GRAPH_PROFILE rank=%d "
            "ranks=%d bytes=%zu slot_stride=%zu reuse_wait_us=%.3f "
            "store_us=%.3f produce_wait_us=%.3f local_copy_us=%.3f "
            "consume_us=%.3f total_us=%.3f\n",
            rank, numRanks, tensorSize, slotStride, reuse_wait_ms * 1000.0f,
            store_ms * 1000.0f, produce_wait_ms * 1000.0f,
            local_copy_ms * 1000.0f, consume_ms * 1000.0f,
            total_ms * 1000.0f);
        for (auto& event : ev) {
            cudaEventDestroy(event);
        }
    }
#endif
}

void launchDeviceApiAllgatherStoreSignalSlottedGraphKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, bool* active_mask, void* raddrs, void* rkeys,
    void* qp_devctxs, int qps_per_rank, size_t tensorSize, size_t slotStride,
    int slots, int rank, int numRanks, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement,
    cudaStream_t stream) {
#ifndef __MUSA__
    const bool profile = deviceApiProfileEnabledForRank(rank);
    cudaEvent_t ev[8]{};
    if (profile) {
        for (auto& event : ev) {
            cudaEventCreate(&event);
        }
        cudaEventRecord(ev[0], stream);
    }
#endif
    constexpr int threads = 256;
    int blocks_x = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    blocks_x = clampP2pAllgatherBlocks(blocks_x);
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    const size_t atomicScratchBytes = static_cast<size_t>(slots) *
                                      kMaxNumRanks * sizeof(uint32_t);
    const size_t rdmaAtomicOffset = consumeSignalOffset - atomicScratchBytes;
    constexpr uint32_t chunkIndex = 0;
    const bool debug_signal = deviceApiSignalDebugEnabled();
    const bool poll_completion = deviceApiCompletionPollEnabled();

    deviceApiSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), active_mask,
        consumeSignalOffset, baseSequenceSlot, chunkIndex, slots, numRanks,
        sequenceCounter, reserveIncrement);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[1], stream);
#endif
    deviceApiAllgatherStageLocalSlottedGraphKernel<<<blocks_x, threads, 0,
                                                     stream>>>(
        static_cast<const char*>(input), static_cast<char*>(local_recv_base),
        tensorSize, slotStride, baseSequenceSlot, chunkIndex, slots, rank,
        numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[2], stream);
#endif
    dim3 p2p_store_grid(blocks_x, numRanks, 1);
    p2pAllgatherStoreToSlottedScratchDeviceApiGraphKernel<<<p2p_store_grid,
                                                             threads, 0,
                                                             stream>>>(
        static_cast<const char*>(input), static_cast<char*>(local_recv_base),
        peer_ptrs, p2p_available, tensorSize, slotStride, baseSequenceSlot,
        chunkIndex, slots, rank, numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[3], stream);
#endif
    deviceApiAllgatherPublishSlottedGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
        active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank, tensorSize,
        slotStride, baseSequenceSlot, chunkIndex, slots, rank, numRanks,
        debug_signal, poll_completion);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[4], stream);
#endif
    deviceApiSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
        active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,
        produceSignalOffset, produceSignalOffset, rdmaAtomicOffset,
        baseSequenceSlot, chunkIndex, slots, rank, numRanks, debug_signal,
        poll_completion);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[5], stream);
#endif
    dim3 copy_grid(blocks_x, numRanks, 1);
    p2pAllgatherStoreSlottedLocalCopyGraphKernel<<<copy_grid, threads, 0,
                                                   stream>>>(
        static_cast<const char*>(local_recv_base), static_cast<char*>(output),
        tensorSize, slotStride, baseSequenceSlot, chunkIndex, slots, numRanks);
#ifndef __MUSA__
    if (profile) cudaEventRecord(ev[6], stream);
#endif
    deviceApiSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
        active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,
        consumeSignalOffset, consumeSignalOffset, rdmaAtomicOffset,
        baseSequenceSlot, chunkIndex, slots, rank, numRanks, poll_completion);
#ifndef __MUSA__
    if (profile) {
        cudaEventRecord(ev[7], stream);
        cudaEventSynchronize(ev[7]);
        float us[7]{};
        float total_ms = 0.0f;
        for (int i = 0; i < 7; ++i) {
            float ms = 0.0f;
            cudaEventElapsedTime(&ms, ev[i], ev[i + 1]);
            us[i] = ms * 1000.0f;
        }
        cudaEventElapsedTime(&total_ms, ev[0], ev[7]);
        std::fprintf(
            stderr,
            "MOONCAKE_PG_DEVICE_API_PROFILE op=all_gather_base rank=%d "
            "ranks=%d bytes=%zu slot_stride=%zu reuse_wait_us=%.3f "
            "stage_local_us=%.3f p2p_store_us=%.3f rdma_publish_us=%.3f "
            "produce_wait_us=%.3f local_copy_us=%.3f consume_us=%.3f "
            "total_us=%.3f\n",
            rank, numRanks, tensorSize, slotStride, us[0], us[1], us[2],
            us[3], us[4], us[5], us[6], total_ms * 1000.0f);
        for (auto& event : ev) {
            cudaEventDestroy(event);
        }
    }
#endif
}

void launchDeviceApiAllgatherStoreSignalSlottedChunkedGraphKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, bool* active_mask, void* raddrs, void* rkeys,
    void* qp_devctxs, int qps_per_rank, size_t tensorSize, size_t chunkBytes,
    size_t slotStride, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, uint32_t* sequenceCounter,
    uint32_t reserveIncrement, cudaStream_t stream) {
#ifndef __MUSA__
    const bool profile = deviceApiProfileEnabledForRank(rank);
    cudaEvent_t ev[8]{};
    if (profile) {
        for (auto& event : ev) {
            cudaEventCreate(&event);
        }
    }
#endif
    constexpr int threads = 256;
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    const size_t atomicScratchBytes = static_cast<size_t>(slots) *
                                      kMaxNumRanks * sizeof(uint32_t);
    const size_t rdmaAtomicOffset = consumeSignalOffset - atomicScratchBytes;
    uint32_t chunkIndex = 0;
    const bool debug_signal = deviceApiSignalDebugEnabled();
    const bool poll_completion = deviceApiCompletionPollEnabled();

    for (size_t offset = 0; offset < tensorSize;
         offset += chunkBytes, ++chunkIndex) {
        const size_t thisChunk = std::min(chunkBytes, tensorSize - offset);
        int blocks_x = static_cast<int>(
            (thisChunk + threads * sizeof(uint64_t) - 1) /
            (threads * sizeof(uint64_t)));
        blocks_x = clampP2pAllgatherBlocks(blocks_x);

#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[0], stream);
#endif
        deviceApiSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<const char*>(local_recv_base), active_mask,
            consumeSignalOffset, baseSequenceSlot, chunkIndex, slots, numRanks,
            sequenceCounter, reserveIncrement);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[1], stream);
#endif
        maybeDeviceApiStageSync("ag_chunked", "reuse_wait", rank, chunkIndex,
                                stream);
        deviceApiAllgatherStageLocalSlottedGraphKernel<<<blocks_x, threads, 0,
                                                         stream>>>(
            static_cast<const char*>(input) + offset,
            static_cast<char*>(local_recv_base), thisChunk, slotStride,
            baseSequenceSlot, chunkIndex, slots, rank, numRanks);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[2], stream);
#endif
        maybeDeviceApiStageSync("ag_chunked", "stage_local", rank, chunkIndex,
                                stream);
        dim3 p2p_store_grid(blocks_x, numRanks, 1);
        p2pAllgatherStoreToSlottedScratchDeviceApiGraphKernel<<<
            p2p_store_grid, threads, 0, stream>>>(
            static_cast<const char*>(input) + offset,
            static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
            thisChunk, slotStride, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[3], stream);
#endif
        maybeDeviceApiStageSync("ag_chunked", "p2p_store", rank, chunkIndex,
                                stream);
        deviceApiAllgatherPublishSlottedGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
            active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank, thisChunk,
            slotStride, baseSequenceSlot, chunkIndex, slots, rank, numRanks,
            debug_signal, poll_completion);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[4], stream);
#endif
        maybeDeviceApiStageSync("ag_chunked", "publish", rank, chunkIndex,
                                stream);
        deviceApiSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
            active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,
            produceSignalOffset, produceSignalOffset, rdmaAtomicOffset,
            baseSequenceSlot, chunkIndex, slots, rank, numRanks, debug_signal,
            poll_completion);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[5], stream);
#endif
        maybeDeviceApiStageSync("ag_chunked", "produce_wait", rank, chunkIndex,
                                stream);
        dim3 copy_grid(blocks_x, numRanks, 1);
        p2pAllgatherStoreSlottedLocalCopyChunkGraphKernel<<<copy_grid, threads,
                                                            0, stream>>>(
            static_cast<const char*>(local_recv_base),
            static_cast<char*>(output), thisChunk, tensorSize, offset,
            slotStride, baseSequenceSlot, chunkIndex, slots, numRanks);
#ifndef __MUSA__
        if (profile) cudaEventRecord(ev[6], stream);
#endif
        maybeDeviceApiStageSync("ag_chunked", "local_copy", rank, chunkIndex,
                                stream);
        deviceApiSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, p2p_available,
            active_mask, raddrs, rkeys, qp_devctxs, qps_per_rank,
            consumeSignalOffset, consumeSignalOffset, rdmaAtomicOffset,
            baseSequenceSlot, chunkIndex, slots, rank, numRanks,
            poll_completion);
#ifndef __MUSA__
        if (profile) {
            cudaEventRecord(ev[7], stream);
            cudaEventSynchronize(ev[7]);
            float us[7]{};
            float total_ms = 0.0f;
            for (int i = 0; i < 7; ++i) {
                float ms = 0.0f;
                cudaEventElapsedTime(&ms, ev[i], ev[i + 1]);
                us[i] = ms * 1000.0f;
            }
            cudaEventElapsedTime(&total_ms, ev[0], ev[7]);
            std::fprintf(
                stderr,
                "MOONCAKE_PG_DEVICE_API_PROFILE op=all_gather_base_chunked "
                "rank=%d ranks=%d chunk=%u offset_bytes=%zu chunk_bytes=%zu "
                "full_bytes=%zu slot_stride=%zu reuse_wait_us=%.3f "
                "stage_local_us=%.3f p2p_store_us=%.3f "
                "rdma_publish_us=%.3f produce_wait_us=%.3f "
                "local_copy_us=%.3f consume_us=%.3f total_us=%.3f\n",
                rank, numRanks, chunkIndex, offset, thisChunk, tensorSize,
                slotStride, us[0], us[1], us[2], us[3], us[4], us[5], us[6],
                total_ms * 1000.0f);
        }
#endif
        maybeDeviceApiStageSync("ag_chunked", "consume_notify", rank,
                                chunkIndex, stream);
    }
#ifndef __MUSA__
    if (profile) {
        for (auto& event : ev) {
            cudaEventDestroy(event);
        }
    }
#endif
}

void launchP2pAllgatherStoreSignalSlottedGraphSkipSelfKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    int rank, int numRanks, const uint32_t* baseSequenceSlot,
    cudaStream_t stream) {
    constexpr int threads = 256;
    int blocks_x = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    blocks_x = clampP2pAllgatherBlocks(blocks_x);
    dim3 store_grid(blocks_x, numRanks, 1);
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    constexpr uint32_t chunkIndex = 0;
    p2pSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available, consumeSignalOffset,
        baseSequenceSlot, chunkIndex, slots, numRanks, nullptr, 0);
    p2pAllgatherStoreToSlottedScratchGraphSkipSelfKernel<<<store_grid, threads, 0,
                                                           stream>>>(
        static_cast<const char*>(input), peer_ptrs, available, tensorSize,
        slotStride, baseSequenceSlot, chunkIndex, slots, rank, numRanks);
    p2pSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), peer_ptrs, available,
        produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
        numRanks);
    dim3 copy_grid(blocks_x, numRanks, 1);
    p2pAllgatherStoreSlottedLocalCopyGraphSkipSelfKernel<<<copy_grid, threads,
                                                           0, stream>>>(
        static_cast<const char*>(local_recv_base), static_cast<char*>(output),
        tensorSize, slotStride, baseSequenceSlot, chunkIndex, slots, rank,
        numRanks);
    p2pSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
        peer_ptrs, available, consumeSignalOffset, baseSequenceSlot, chunkIndex,
        slots, rank, numRanks);
}

void launchP2pAllgatherStoreSignalSlottedChunkedGraphKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t chunkBytes,
    size_t slotStride, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, cudaStream_t stream) {
    constexpr int threads = 256;
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    uint32_t chunkIndex = 0;
    for (size_t offset = 0; offset < tensorSize;
         offset += chunkBytes, ++chunkIndex) {
        const size_t thisChunk = std::min(chunkBytes, tensorSize - offset);
        int blocks_x = static_cast<int>(
            (thisChunk + threads * sizeof(uint64_t) - 1) /
            (threads * sizeof(uint64_t)));
        blocks_x = clampP2pAllgatherBlocks(blocks_x);
        dim3 store_grid(blocks_x, numRanks, 1);
        p2pSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<const char*>(local_recv_base), available,
            consumeSignalOffset, baseSequenceSlot, chunkIndex, slots, numRanks,
            nullptr, 0);
        p2pAllgatherStoreToSlottedScratchGraphKernel<<<store_grid, threads, 0,
                                                       stream>>>(
            static_cast<const char*>(input) + offset, peer_ptrs, available,
            thisChunk, slotStride, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
        p2pSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
            static_cast<char*>(local_recv_base), peer_ptrs, available,
            produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
            numRanks);
        int copy_blocks = static_cast<int>(
            (thisChunk + threads * sizeof(uint64_t) - 1) /
            (threads * sizeof(uint64_t)));
        copy_blocks = clampP2pAllgatherBlocks(copy_blocks);
        dim3 copy_grid(copy_blocks, numRanks, 1);
        p2pAllgatherStoreSlottedLocalCopyChunkGraphKernel<<<copy_grid, threads,
                                                            0, stream>>>(
            static_cast<const char*>(local_recv_base), static_cast<char*>(output),
            thisChunk, tensorSize, offset, slotStride, baseSequenceSlot,
            chunkIndex, slots, numRanks);
        p2pSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
            peer_ptrs, available, consumeSignalOffset, baseSequenceSlot,
            chunkIndex, slots, rank, numRanks);
    }
}

void launchP2pAllgatherDirectOutputStoreSignalKernel(
    void* input, void** output_peer_ptrs, void* local_recv_base,
    void** scratch_peer_ptrs, int32_t* available, size_t tensorSize, int rank,
    int numRanks, uint32_t sequence, cudaStream_t stream) {
    constexpr size_t kProduceSignalOffset =
        kBufferSize - kMaxNumRanks * sizeof(uint32_t);
    constexpr int threads = 256;
    int blocks_x = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    blocks_x = blocks_x < 1 ? 1 : (blocks_x > 512 ? 512 : blocks_x);
    dim3 store_grid(blocks_x, numRanks, 1);
    p2pAllgatherDirectOutputStoreKernel<<<store_grid, threads, 0, stream>>>(
        static_cast<const char*>(input), output_peer_ptrs, available,
        tensorSize, rank, numRanks);
    p2pAllgatherDirectOutputProduceWaitKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), scratch_peer_ptrs, available,
        kProduceSignalOffset, rank, numRanks, sequence);
}

void launchP2pAllgatherDirectOutputStoreSignalGraphKernel(
    void* input, void** output_peer_ptrs, void* local_recv_base,
    void** scratch_peer_ptrs, int32_t* available, size_t tensorSize, int rank,
    int numRanks, const uint32_t* sequenceSlot, cudaStream_t stream) {
    constexpr size_t kProduceSignalOffset =
        kBufferSize - kMaxNumRanks * sizeof(uint32_t);
    constexpr int threads = 256;
    int blocks_x = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    blocks_x = blocks_x < 1 ? 1 : (blocks_x > 512 ? 512 : blocks_x);
    dim3 store_grid(blocks_x, numRanks, 1);
    p2pAllgatherDirectOutputStoreKernel<<<store_grid, threads, 0, stream>>>(
        static_cast<const char*>(input), output_peer_ptrs, available,
        tensorSize, rank, numRanks);
    p2pAllgatherDirectOutputProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), scratch_peer_ptrs, available,
        kProduceSignalOffset, rank, numRanks, sequenceSlot);
}

void launchP2pAllgatherDirectOutputSlottedGraphKernel(
    void* input, void** output_peer_ptrs, void* local_recv_base,
    void** scratch_peer_ptrs, int32_t* available, size_t tensorSize, int slots,
    int rank, int numRanks, const uint32_t* baseSequenceSlot,
    cudaStream_t stream) {
    constexpr int threads = 256;
    int blocks_x = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    blocks_x = blocks_x < 1 ? 1 : (blocks_x > 512 ? 512 : blocks_x);
    dim3 store_grid(blocks_x, numRanks, 1);
    const size_t signalBytes = static_cast<size_t>(slots) * kMaxNumRanks *
                               sizeof(uint32_t);
    const size_t produceSignalOffset = kBufferSize - signalBytes;
    const size_t consumeSignalOffset = kBufferSize - 2 * signalBytes;
    constexpr uint32_t chunkIndex = 0;
    p2pSlottedReuseWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<const char*>(local_recv_base), available, consumeSignalOffset,
        baseSequenceSlot, chunkIndex, slots, numRanks, nullptr, 0);
    p2pAllgatherDirectOutputStoreKernel<<<store_grid, threads, 0, stream>>>(
        static_cast<const char*>(input), output_peer_ptrs, available,
        tensorSize, rank, numRanks);
    p2pSlottedProduceWaitGraphKernel<<<1, 1, 0, stream>>>(
        static_cast<char*>(local_recv_base), scratch_peer_ptrs, available,
        produceSignalOffset, baseSequenceSlot, chunkIndex, slots, rank,
        numRanks);
    p2pSlottedConsumeNotifyGraphKernel<<<1, 1, 0, stream>>>(
        scratch_peer_ptrs, available, consumeSignalOffset, baseSequenceSlot,
        chunkIndex, slots, rank, numRanks);
}

__global__ void ipcAllgatherStoreKernel(const char* input,
                                        const uintptr_t* outPtrs, int rank,
                                        int numRanks, size_t tensorSize) {
    const size_t tid = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    const size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;

    if (((reinterpret_cast<uintptr_t>(input) | tensorSize) &
         (sizeof(uint64_t) - 1)) == 0) {
        const auto* src64 = reinterpret_cast<const uint64_t*>(input);
        const size_t words = tensorSize / sizeof(uint64_t);
        for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
            auto* dst = reinterpret_cast<char*>(outPtrs[peer]) +
                        static_cast<size_t>(rank) * tensorSize;
            if ((reinterpret_cast<uintptr_t>(dst) & (sizeof(uint64_t) - 1)) ==
                0) {
                auto* dst64 = reinterpret_cast<uint64_t*>(dst);
                for (size_t i = tid; i < words; i += stride) {
                    dst64[i] = src64[i];
                }
            } else {
                for (size_t i = tid; i < tensorSize; i += stride) {
                    dst[i] = input[i];
                }
            }
        }
        return;
    }

    for (int peer = blockIdx.y; peer < numRanks; peer += gridDim.y) {
        auto* dst = reinterpret_cast<char*>(outPtrs[peer]) +
                    static_cast<size_t>(rank) * tensorSize;
        for (size_t i = tid; i < tensorSize; i += stride) {
            dst[i] = input[i];
        }
    }
}

void launchIpcAllgatherStoreKernel(void* input, const uintptr_t* outPtrs,
                                   int rank, int numRanks, size_t tensorSize,
                                   cudaStream_t stream) {
    constexpr int threads = 256;
    int blocks_x = static_cast<int>(
        (tensorSize + threads * sizeof(uint64_t) - 1) /
        (threads * sizeof(uint64_t)));
    blocks_x = blocks_x < 1 ? 1 : (blocks_x > 512 ? 512 : blocks_x);
    dim3 grid(blocks_x, numRanks, 1);
    ipcAllgatherStoreKernel<<<grid, threads, 0, stream>>>(
        static_cast<const char*>(input), outPtrs, rank, numRanks, tensorSize);
}

__global__ void ipcAllgatherSignalWaitKernel(const uintptr_t* signalPtrs,
                                             int rank, int numRanks,
                                             uint32_t sequence) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    __threadfence_system();
    for (int peer = 0; peer < numRanks; ++peer) {
        auto* peerSignals = reinterpret_cast<uint32_t*>(signalPtrs[peer]);
        p2pAgSignalStore(peerSignals + rank, sequence, 2);
    }
    __threadfence_system();

    auto* localSignals =
        reinterpret_cast<volatile uint32_t*>(signalPtrs[rank]);
    for (int peer = 0; peer < numRanks; ++peer) {
        while (localSignals[peer] < sequence) {
        }
    }
}

void launchIpcAllgatherStoreSignalKernel(
    void* input, const uintptr_t* outPtrs, const uintptr_t* signalPtrs,
    int rank, int numRanks, uint32_t sequence, size_t tensorSize,
    cudaStream_t stream) {
    launchIpcAllgatherStoreKernel(input, outPtrs, rank, numRanks, tensorSize,
                                  stream);
    ipcAllgatherSignalWaitKernel<<<1, 1, 0, stream>>>(signalPtrs, rank,
                                                       numRanks, sequence);
}

void launchReduceKernel_bf16(void* dst, const void* src, size_t numElements,
                             size_t numRanks, int op, bool* activeRanks,
                             cudaStream_t stream) {
#ifdef __MUSA__
    reduceKernel<<<64, 256, 0, stream>>>((mt_bfloat16*)dst,
                                         (const mt_bfloat16*)src, numElements,
                                         numRanks, op, activeRanks);
#else
    reduceKernel<<<64, 256, 0, stream>>>((at::BFloat16*)dst,
                                         (const at::BFloat16*)src, numElements,
                                         numRanks, op, activeRanks);
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
