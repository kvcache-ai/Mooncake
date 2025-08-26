#ifndef MOONCAKE_WORKER_CUH
#define MOONCAKE_WORKER_CUH

#include <cuda_bf16.h>
#include <cuda_runtime.h>
#include <mooncake_backend_buffer.h>
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
                        c10d::ReduceOp op, bool* brokenRanks,
                        cudaStream_t stream);

void launchReduceCpu(at::Tensor dst, void* src, size_t numRanks,
                     c10d::ReduceOp op);

class MooncakeWorker {
   public:
    explicit MooncakeWorker(TransferEngine* engine, int rank, int size,
                            at::Tensor brokenRanks);

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

    void stopWorker() { running_ = false; }

    void setBackendBuffer(BackendBuffer* buffer) { buffer_ = buffer; }

    bool* getBrokenRanks() { return brokenRanks_; }

   private:
    static constexpr size_t kNumTasks_ = 2;

    bool running_ = false;

    Task *tasks_, *tasks_device_;
    bool *brokenRanks_, *brokenRanksDevice_;
    bool hasCallback_[kNumTasks_]{};
    std::function<void()> callbacks_[kNumTasks_]{};

    int rank_, size_;
    BackendBuffer* buffer_ = nullptr;
    at::Tensor brokenRanksTensor_;

    TransferEngine* engine_;
    std::vector<TransferMetadata::SegmentID> segment_ids_;
    std::vector<std::shared_ptr<TransferMetadata::SegmentDesc>> segment_descs_;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_CUH
