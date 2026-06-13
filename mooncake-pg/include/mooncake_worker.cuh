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

static constexpr size_t kBufferSize = 1u << 24;
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

void launchReduceCpu(at::Tensor dst, size_t pos, size_t realSize, void* src,
                     size_t numRanks, c10d::ReduceOp op, bool* activeRanks);
void preloadReduceKernels();

class ConnectionContext;

struct CudaTaskSubmissionToken {
    size_t task_id;
    uint64_t sequence;
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
