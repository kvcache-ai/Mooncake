#ifndef MOONCAKE_WORKER_CUH
#define MOONCAKE_WORKER_CUH

#include <cuda_runtime.h>
#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Work.hpp>
#include <transfer_engine.h>

namespace mooncake {

__global__ enum TaskStatus {
    IDLE = 0,
    OCCUPIED = 1,
    READY = 2,
    DONE = 3,
};

__global__ struct Task {
    volatile TaskStatus status = IDLE;
    c10d::OpType opType = c10d::OpType::UNKNOWN;
    size_t tensorSize;  // In bytes
    BatchID batchID;
};

class MooncakeWorker {
   public:
    explicit MooncakeWorker(TransferEngine* engine, int rank, int size);

    c10::intrusive_ptr<c10d::Work> putTask(
        c10d::OpType opType, size_t tensorSize, cudaStream_t stream,
        const std::function<void(void* dst)>& tensorToBuffer,
        const std::function<void(void* src)>& bufferToTensor);

    void initWorker(const std::vector<std::string>& server_names);

    static constexpr size_t kNumTasks_ = 1024;

   private:
    Task *tasks_, *tasks_device_;

    int rank_, size_;

    TransferEngine* engine_;
    std::vector<TransferMetadata::SegmentID> segment_ids_;
    std::vector<std::shared_ptr<TransferMetadata::SegmentDesc>> segment_descs_;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_CUH
