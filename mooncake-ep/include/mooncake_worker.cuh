#ifndef MOONCAKE_WORKER_CUH
#define MOONCAKE_WORKER_CUH

#include <ATen/cuda/CUDAContext.h>
#include <cuda_bf16.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>
#include <transfer_engine.h>

namespace mooncake {

struct TransferGroupMeta {
    int rank;
    int size;
    int taskCount;
    bool* activeRanks;
    bool* activeRanksDevice;
    at::Tensor activeRanksTensor;
    TransferEngine* engine;
    int bufferBaseIndex;
    std::vector<TransferMetadata::SegmentID> segmentIDs;
    std::vector<std::shared_ptr<TransferMetadata::SegmentDesc>> segmentDescs;
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

static constexpr size_t kBufferSize = 1u << 29;
static constexpr size_t kMaxNumRanks = 64;

void launchReduceKernel(at::Tensor dst, void* src, size_t numRanks,
                        c10d::ReduceOp op, bool* activeRanks,
                        cudaStream_t stream);

void launchReduceCpu(at::Tensor dst, void* src, size_t numRanks,
                     c10d::ReduceOp op);

class MooncakeWorker {
   public:
    explicit MooncakeWorker();

    c10::intrusive_ptr<c10d::Work> putTaskCpu(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        TransferGroupMeta* meta,
        const std::function<void(void* dst)>& tensorToBuffer,
        const std::function<void(void* src)>& bufferToTensor);

    c10::intrusive_ptr<c10d::Work> putTaskCuda(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        TransferGroupMeta* meta, const at::cuda::CUDAStream& stream,
        const std::function<void(void* dst)>& tensorToBuffer,
        const std::function<void(void* src)>& bufferToTensor);

    void startWorker();

    void stopWorker() { running_ = false; }

   private:
    static constexpr size_t kNumTasks_ = 4;

    bool running_ = false;

    Task *tasks_, *tasks_device_;
    bool hasCallback_[kNumTasks_]{};
    std::function<void()> callbacks_[kNumTasks_]{};

    int cpuTaskCount = 0;
    int cudaTaskCount = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_CUH
