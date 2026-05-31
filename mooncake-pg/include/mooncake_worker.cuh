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

namespace mooncake {

static constexpr size_t kBufferSize = 1u << 24;
static constexpr size_t kMaxNumRanks = 64;
// Number of slots in the circular buffer for P2P operations.
static constexpr size_t kP2PNumSlots = 256;
static constexpr size_t kP2PSlotSize = kBufferSize / kP2PNumSlots;

struct SegmentInfo {
    uint64_t send_buffer[2], recv_buffer[2], send_sync[2], recv_sync[2],
        warmup_buffer[2];
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
    int64_t p2pSendSeq[kMaxNumRanks]{};
    int64_t p2pRecvSeq[kMaxNumRanks]{};
    int64_t p2pSendLowestInFlight[kMaxNumRanks]{};
    int64_t p2pRecvLowestInFlight[kMaxNumRanks]{};
    int64_t p2pRecvNextExpected[kMaxNumRanks]{};
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

class MooncakeWorker {
   public:
    explicit MooncakeWorker();

    c10::intrusive_ptr<c10d::Work> putTaskCpu(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        TransferGroupMeta* meta,
        const std::function<void(void* dst, size_t pos, size_t realSize)>&
            tensorToBuffer,
        const std::function<void(void* src, size_t pos, size_t realSize)>&
            bufferToTensor);

    c10::intrusive_ptr<c10d::Work> putTaskCuda(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        TransferGroupMeta* meta, const at::cuda::CUDAStream& stream,
        const std::function<void(void* dst, size_t pos, size_t realSize)>&
            tensorToBuffer,
        const std::function<void(void* src, size_t pos, size_t realSize)>&
            bufferToTensor);

    void startWorker();

    void stopWorker() { running_ = false; }

   private:
    static constexpr size_t kNumTasks_ = 4;

    static constexpr size_t kPingTimeoutMicroseconds_ = 100;

    bool running_ = false;

    Task *tasks_, *tasks_device_;
    bool hasCallback_[kNumTasks_]{};
    std::function<void()> callbacks_[kNumTasks_]{};

    int cpuTaskCount = 0;
    int cudaTaskCount = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_CUH
