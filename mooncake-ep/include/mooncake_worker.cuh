#ifndef MOONCAKE_WORKER_CUH
#define MOONCAKE_WORKER_CUH

#include <cuda_bf16.h>
#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>
#include <transfer_engine.h>

namespace mooncake {

__global__ struct Task {
    volatile bool active = false;
    c10d::OpType opType = c10d::OpType::UNKNOWN;
    size_t tensorSize;  // In bytes
    int64_t broadcastRoot;
    BatchID batchID;
};

void launchReduceKernel(at::Tensor dst, void* src, size_t numRanks,
                        c10d::ReduceOp op, cudaStream_t stream);

void launchReduceCpu(at::Tensor dst, void* src, size_t numRanks,
                     c10d::ReduceOp op);

class MooncakeWorker {
   public:
    explicit MooncakeWorker(TransferEngine* engine, int rank, int size);

    c10::intrusive_ptr<c10d::Work> putTaskCpu(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        const std::function<void(void* dst)>& tensorToBuffer,
        const std::function<void(void* src)>& bufferToTensor);

    c10::intrusive_ptr<c10d::Work> putTaskCuda(
        c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
        cudaStream_t stream,
        const std::function<void(void* dst)>& tensorToBuffer,
        const std::function<void(void* src)>& bufferToTensor);

    void initWorker(const std::vector<std::string>& server_names);

   private:
    static constexpr size_t kNumTasks_ = 2;

    Task *tasks_, *tasks_device_;

    int rank_, size_;

    TransferEngine* engine_;
    std::vector<TransferMetadata::SegmentID> segment_ids_;
    std::vector<std::shared_ptr<TransferMetadata::SegmentDesc>> segment_descs_;

    int taskCount = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_CUH
