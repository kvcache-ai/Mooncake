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
    TaskStatus status = IDLE;
    c10d::OpType opType = c10d::OpType::UNKNOWN;
    void* inputPtr;
    void* outputPtr;
};

class MooncakeWorker {
   public:
    explicit MooncakeWorker(TransferEngine* engine);

    c10::intrusive_ptr<c10d::Work> putTask(
        c10d::OpType opType, void* inputPtr, void* outputPtr,
        cudaStream_t stream, const std::function<void()>& asyncCallback);

    void initWorker();

   private:
    static constexpr size_t kNumTasks_ = 1024;

    Task *tasks_, *tasks_device_;

    TransferEngine* engine_;
};

}  // namespace mooncake

#endif  // MOONCAKE_WORKER_CUH
