#ifndef MOONCAKE_WORKER_CUH
#define MOONCAKE_WORKER_CUH

#if !defined(__MUSA__)
#include <ATen/ATen.h>
#include <ATen/cuda/CUDAContext.h>
#include <c10/util/intrusive_ptr.h>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>
#include <torch/csrc/distributed/c10d/Store.hpp>
#else
// MUSA device compilation: minimal includes to avoid mcc compiler crash
#include <cstddef>
#include <cstdint>
#endif

#include <cuda_alike.h>
#include <transfer_engine.h>

#include <memory>
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake {

// Prototype upper bound for per-rank staging/scratch buffers. Generic PG paths
// still default to the historical 16MB effective chunking, while direct-P2P
// experiments may opt into a larger effective scratch region via env knobs.
static constexpr size_t kBufferSize = 128u << 20;
static constexpr size_t kDefaultBufferSize = 16u << 20;
static constexpr size_t kMaxNumRanks = 64;

struct SegmentInfo {
    uint64_t send_buffer[2], recv_buffer[2], send_sync[2], recv_sync[2],
        warmup_buffer[2];
    uint64_t p2p_credit_region;
    uint64_t p2p_ack_region;
};

struct TransferGroupMeta {
    int rank;
    int size;        // capacity: number of slots allocated (incl. inactive)
    int activeSize;  // visible group size: number of ranks that participate
    int taskCount;
    bool* activeRanks;
    bool* activeRanksDevice;
#if !defined(__MUSA__)
    at::Tensor activeRanksTensor;
#endif
    bool peerConnected[kMaxNumRanks]{};
    TransferEngine* engine;
#if !defined(__MUSA__)
    c10::intrusive_ptr<::c10d::Store> store;
#endif
    int bufferBaseIndex;
    int backendIndex;
    bool syncOnDevice = false;
    bool storeSync = false;
    TransferMetadata::SegmentID segmentIDs[kMaxNumRanks];
    SegmentInfo segmentInfos[kMaxNumRanks];
};

#if defined(__CUDACC__) || defined(__MUSA__)
__global__
#endif
    struct Task {
    volatile bool active = false;
    int opType =
        0;  // c10d::OpType as int, for ABI compatibility with kernel code
    size_t tensorSize;  // In bytes
    int64_t broadcastRoot;
    int bufferOffset;
    uint64_t submitSequence = 0;
    BatchID batchID;
    void* transferGroupMeta;
};

#if !defined(__MUSA__)
void launchReduceKernel(at::Tensor dst, size_t pos, size_t realSize, void* src,
                        size_t numRanks, c10d::ReduceOp op, bool* activeRanks,
                        cudaStream_t stream);

void launchP2pReduceScatterSlottedKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t tensorSize,
    size_t slotStride, int slots, int rank, int numRanks, uint32_t sequence,
    cudaStream_t stream);

void launchP2pReserveSequence(uint32_t* counter, uint32_t* sequenceSlot,
                              uint32_t increment, cudaStream_t stream);

void launchP2pReduceScatterSlottedGraphKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t tensorSize,
    size_t slotStride, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, uint32_t* sequenceCounter,
    uint32_t reserveIncrement, cudaStream_t stream);

void launchDeviceApiReduceScatterSlottedGraphKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* p2p_available, bool* active_mask, void* raddrs,
    void* rkeys, void* qp_devctxs, int qps_per_rank, size_t tensorSize,
    size_t slotStride, size_t rdmaSendBaseOffset, int slots, int rank,
    int numRanks, const uint32_t* baseSequenceSlot, uint32_t* sequenceCounter,
    uint32_t reserveIncrement, cudaStream_t stream);

void launchDeviceApiReduceScatterSlottedChunkedGraphKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* p2p_available, bool* active_mask, void* raddrs,
    void* rkeys, void* qp_devctxs, int qps_per_rank, size_t tensorSize,
    size_t chunkBytes, size_t slotStride, size_t rdmaSendBaseOffset, int slots,
    int rank, int numRanks, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement, cudaStream_t stream);

void launchDeviceApiAllReducePairExchangeSlottedChunkedGraphKernel(
    at::Tensor& tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, void* raddrs, void* rkeys, void* qp_devctxs,
    int qps_per_rank, size_t tensorSize, size_t chunkBytes,
    size_t slotStride, size_t dataBaseOffset, size_t controlBaseOffset,
    int slots, int rank, int numRanks, int pairLocalRank,
    int peerPairLocalRank, int peerRank, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement, cudaStream_t stream);

void launchP2pReduceScatterSlottedChunkedKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t tensorSize,
    size_t chunkBytes, size_t slotStride, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream);

void launchP2pReduceScatterSlottedChunkedGraphKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t tensorSize,
    size_t chunkBytes, size_t slotStride, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, cudaStream_t stream);

void launchP2pAllReduceSlottedKernel(
    at::Tensor& tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream,
    bool useDeviceApi = false);

void launchP2pNodeLocalAllReduceSlottedKernel(
    at::Tensor& tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    size_t controlBaseOffset, int globalRank, int nodeBaseRank,
    int localRank, int nodeSize, const uint32_t* baseSequenceSlot,
    cudaStream_t stream);

void launchP2pAllReduceRingKernel(
    at::Tensor& tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, int rank, int numRanks,
    uint32_t sequence, int signalMode, int numChannels, cudaStream_t stream);

void launchP2pAllReduceRingGraphKernel(
    at::Tensor& tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, int rank, int numRanks,
    const uint32_t* sequenceSlot, int signalMode, int numChannels,
    cudaStream_t stream);

void launchP2pAllReduceFusedRsAgSlottedGraphKernel(
    at::Tensor& tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    int rank, int numRanks, const uint32_t* rsSequenceSlot,
    const uint32_t* agSequenceSlot, cudaStream_t stream);

void launchReduceCpu(at::Tensor dst, size_t pos, size_t realSize, void* src,
                     size_t numRanks, c10d::ReduceOp op, bool* activeRanks);
void preloadReduceKernels();

void launchP2pAllgatherBaseKernel(void* input, void* output,
                                  void* local_recv_base, void** peer_ptrs,
                                  int32_t* available, size_t tensorSize,
                                  int rank, int numRanks, uint32_t sequence,
                                  cudaStream_t stream);

void launchP2pAllgatherBaseCopyKernel(void* input, void** peer_ptrs,
                                      int32_t* available, size_t tensorSize,
                                      int rank, int numRanks,
                                      cudaStream_t stream);

void launchP2pAllgatherBaseFinishKernel(void* output, void* local_recv_base,
                                        void** peer_ptrs, int32_t* available,
                                        size_t tensorSize, int rank,
                                        int numRanks, uint32_t sequence,
                                        cudaStream_t stream);

void launchP2pAllgatherRingKernel(void* input, void* output,
                                  void* local_recv_base, void** peer_ptrs,
                                  int32_t* available, size_t tensorSize,
                                  int rank, int numRanks, uint32_t sequence,
                                  int signalMode, int numChannels,
                                  cudaStream_t stream);

void launchP2pAllgatherFifoRingKernel(void* input, void* output,
                                      void* local_recv_base, void** peer_ptrs,
                                      int32_t* available, size_t tensorSize,
                                      int rank, int numRanks,
                                      uint32_t sequence, int signalMode,
                                      int numChannels, int fifoSlots,
                                      cudaStream_t stream);

void launchP2pAllgatherStoreSignalKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, int rank, int numRanks,
    uint32_t sequence, cudaStream_t stream);

void launchP2pAllgatherStoreSignalSlottedKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream);

void launchP2pAllgatherStoreSignalSlottedGraphKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    int rank, int numRanks, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement, cudaStream_t stream);

void launchDeviceApiAllgatherStoreSignalSlottedGraphKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, bool* active_mask, void* raddrs, void* rkeys,
    void* qp_devctxs, int qps_per_rank, size_t tensorSize, size_t slotStride,
    int slots, int rank, int numRanks, const uint32_t* baseSequenceSlot,
    uint32_t* sequenceCounter, uint32_t reserveIncrement, cudaStream_t stream);

void launchDeviceApiAllgatherStoreSignalSlottedChunkedGraphKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* p2p_available, bool* active_mask, void* raddrs, void* rkeys,
    void* qp_devctxs, int qps_per_rank, size_t tensorSize, size_t chunkBytes,
    size_t slotStride, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, uint32_t* sequenceCounter,
    uint32_t reserveIncrement, cudaStream_t stream);

void launchP2pAllgatherStoreSignalSlottedGraphSkipSelfKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    int rank, int numRanks, const uint32_t* baseSequenceSlot,
    cudaStream_t stream);

void launchP2pAllgatherStoreSignalSlottedChunkedKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t chunkBytes,
    size_t slotStride, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream);

void launchP2pAllgatherStoreSignalSlottedChunkedGraphKernel(
    void* input, void* output, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t chunkBytes,
    size_t slotStride, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, cudaStream_t stream);

void launchP2pAllgatherDirectOutputStoreSignalKernel(
    void* input, void** output_peer_ptrs, void* local_recv_base,
    void** scratch_peer_ptrs, int32_t* available, size_t tensorSize, int rank,
    int numRanks, uint32_t sequence, cudaStream_t stream);

void launchP2pAllgatherDirectOutputStoreSignalGraphKernel(
    void* input, void** output_peer_ptrs, void* local_recv_base,
    void** scratch_peer_ptrs, int32_t* available, size_t tensorSize, int rank,
    int numRanks, const uint32_t* sequenceSlot, cudaStream_t stream);

void launchP2pAllgatherDirectOutputSlottedGraphKernel(
    void* input, void** output_peer_ptrs, void* local_recv_base,
    void** scratch_peer_ptrs, int32_t* available, size_t tensorSize, int slots,
    int rank, int numRanks, const uint32_t* baseSequenceSlot,
    cudaStream_t stream);

void launchIpcAllgatherStoreKernel(void* input, const uintptr_t* outPtrs,
                                   int rank, int numRanks, size_t tensorSize,
                                   cudaStream_t stream);

void launchIpcAllgatherStoreSignalKernel(
    void* input, const uintptr_t* outPtrs, const uintptr_t* signalPtrs,
    int rank, int numRanks, uint32_t sequence, size_t tensorSize,
    cudaStream_t stream);

class ConnectionContext;

struct CudaTaskSubmissionToken {
    size_t task_id;
    uint64_t sequence;
};

struct PgProfileSlot {
    uint64_t sequence = 0;
    uint64_t issue_start_us = 0;
    uint64_t wait_conn_us = 0;
    uint64_t tensor_to_buffer_us = 0;
    uint64_t enqueue_us = 0;
    uint64_t buffer_to_tensor_us = 0;
    uint64_t host_issue_us = 0;
};

class MooncakeWorker {
   public:
    explicit MooncakeWorker(int cuda_device_index = -1);
    ~MooncakeWorker();

    c10::intrusive_ptr<c10d::Work> putTaskCpu(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        const std::shared_ptr<TransferGroupMeta>& meta,
        const std::shared_ptr<ConnectionContext>& connection_ctx,
        const std::function<void(void* dst, size_t pos, size_t realSize)>&
            tensorToBuffer,
        const std::function<void(void* src, size_t pos, size_t realSize)>&
            bufferToTensor);

    c10::intrusive_ptr<c10d::Work> putTaskCuda(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        const std::shared_ptr<TransferGroupMeta>& meta,
        const std::shared_ptr<ConnectionContext>& connection_ctx,
        const at::cuda::CUDAStream& issue_stream,
        const std::function<void(void* dst, size_t pos, size_t realSize,
                                 const at::cuda::CUDAStream&)>& tensorToBuffer,
        const std::function<void(void* src, size_t pos, size_t realSize,
                                 const at::cuda::CUDAStream&)>& bufferToTensor);

    void Start();

    /**
     * @brief Waits for all active collective tasks for the given backend to
     * complete.
     *
     * Used during graceful shutdown to ensure no pending collective operations
     * are active before releasing resources. Blocks until all tasks complete
     * or the timeout expires.
     *
     * @param meta The transfer group metadata identifying the backend.
     * @return True if all tasks completed within the timeout; false if timed
     * out.
     */
    bool drainTasks(const TransferGroupMeta* meta) const;

    bool waitUntilTasksSubmitted(
        const std::vector<CudaTaskSubmissionToken>& tasks,
        std::chrono::milliseconds timeout) const;

   private:
    void startWorker();

    static constexpr size_t kNumTasks_ = 4;

    static constexpr size_t kPingTimeoutMicroseconds_ = 100;
    static constexpr size_t kDrainTasksTimeoutMs = 5000;  // 5s

    std::atomic<bool> running_{false};
    std::atomic<bool> started_{false};
    int cuda_device_index_;

    Task *tasks_, *tasks_device_;
    bool hasCallback_[kNumTasks_]{};
    std::function<void()> callbacks_[kNumTasks_]{};

    int cpuTaskCount = 0;
    int cudaTaskCount = 0;
    std::atomic<uint64_t> next_cuda_task_sequence_{1};
    std::atomic<uint64_t> submitted_task_sequence_[kNumTasks_]{};
    PgProfileSlot profile_slots_[kNumTasks_]{};

    std::thread worker_thread_;
};

class MooncakeWorkerManager {
   public:
    static MooncakeWorkerManager& GetInstance() {
        // leaky singleton to avoid destructor fiasco problem
        static MooncakeWorkerManager* manager = new MooncakeWorkerManager;
        return *manager;
    }

    std::shared_ptr<MooncakeWorker> GetCPUWorker();
    std::shared_ptr<MooncakeWorker> GetCUDAWorker(int cuda_device_index);

   private:
    std::shared_ptr<MooncakeWorker> GetWorker(int worker_id);
    static constexpr int CPUWorkerID = -1;
    std::mutex manager_mutex_;
    // Keep workers alive for the entire process lifetime because their
    // detached threads must not outlive the MooncakeWorker object.
    std::unordered_map<int, std::shared_ptr<MooncakeWorker>> workers_;
};
#endif  // !defined(__MUSA__)

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_CUH
