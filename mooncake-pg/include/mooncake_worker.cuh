#ifndef MOONCAKE_WORKER_CUH
#define MOONCAKE_WORKER_CUH

#include <ATen/cuda/CUDAContext.h>
#include <cuda_bf16.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>
#include <torch/csrc/distributed/c10d/Store.hpp>
#include <transfer_engine.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace mooncake {

static constexpr size_t kBufferSize = 1u << 24;
static constexpr size_t kMaxNumRanks = 64;

struct SegmentInfo {
    uint64_t send_buffer[2], recv_buffer[2], send_sync[2], recv_sync[2],
        warmup_buffer[2];
    uint64_t p2p_send_buffer;
    uint64_t p2p_recv_buffer;
    uint64_t p2p_ctrl_send;
    uint64_t p2p_ctrl_recv;
};

struct TransferGroupMeta {
    int rank;
    int size;
    int taskCount;
    bool* activeRanks;
    bool* activeRanksDevice;
    at::Tensor activeRanksTensor;
    bool peerConnected[kMaxNumRanks]{};
    TransferEngine* engine;
    c10::intrusive_ptr<::c10d::Store> store;
    int bufferBaseIndex;
    int backendIndex;
    TransferMetadata::SegmentID segmentIDs[kMaxNumRanks];
    SegmentInfo segmentInfos[kMaxNumRanks];
};

__global__ struct Task {
    volatile bool active = false;
    c10d::OpType opType = c10d::OpType::UNKNOWN;
    size_t tensorSize;  // In bytes
    int64_t broadcastRoot;
    int bufferOffset;
    BatchID batchID;
    void* transferGroupMeta;
};

void launchReduceKernel(at::Tensor dst, size_t pos, size_t realSize, void* src,
                        size_t numRanks, c10d::ReduceOp op, bool* activeRanks,
                        cudaStream_t stream);

void launchReduceCpu(at::Tensor dst, size_t pos, size_t realSize, void* src,
                     size_t numRanks, c10d::ReduceOp op, bool* activeRanks);
void preloadReduceKernels();

class ConnectionContext;
class MooncakeWorker {
   public:
    explicit MooncakeWorker(int cuda_device_index = -1);

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
        const at::cuda::CUDAStream& stream,
        const std::function<void(void* dst, size_t pos, size_t realSize)>&
            tensorToBuffer,
        const std::function<void(void* src, size_t pos, size_t realSize)>&
            bufferToTensor);

    void Start();

    void Stop() { running_ = false; }

   private:
    void startWorker();

    static constexpr size_t kNumTasks_ = 4;

    static constexpr size_t kPingTimeoutMicroseconds_ = 100;

    bool running_ = false;
    std::atomic<bool> started_{false};
    int cuda_device_index_;

    Task *tasks_, *tasks_device_;
    bool hasCallback_[kNumTasks_]{};
    std::function<void()> callbacks_[kNumTasks_]{};

    int cpuTaskCount = 0;
    int cudaTaskCount = 0;
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

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_CUH
