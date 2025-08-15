#include <mooncake_backend.h>
#include <mooncake_worker.cuh>

namespace mooncake {

class MooncakeWork : public ::c10d::Work {
   public:
    MooncakeWork(c10d::OpType opType, cudaEvent_t event)
        : Work(-1, opType), event_(event) {}

    bool isCompleted() override {
        return cudaEventQuery(event_) == cudaSuccess;
    }

    bool wait(std::chrono::milliseconds timeout) override {
        return cudaEventSynchronize(event_) == cudaSuccess;
    }

   private:
    cudaEvent_t event_;
};

__device__ int findIdleTask(Task* tasks, size_t numTasks) {
    for (size_t i = 0; i < numTasks; ++i) {
        int expected = IDLE;
        if (atomicCAS((int*)&tasks[i].status, expected, OCCUPIED) == expected) {
            return i;
        }
    }
    return -1;
}

__global__ void enqueueTaskKernel(c10d::OpType opType, size_t tensorSize,
                                  Task* tasks, size_t numTasks) {
    // Find idle task
    int idx = findIdleTask(tasks, numTasks);
    assert(idx >= 0);

    // Copy task into slot
    tasks[idx].opType = opType;
    tasks[idx].tensorSize = tensorSize;

    // Mark READY
    __threadfence();  // Ensure writes visible to host
    tasks[idx].status = READY;

    // Spin-wait until CPU proxy sets DONE
    while (atomicAdd((int*)&tasks[idx].status, 0) != DONE) {
        __threadfence();
    }
    tasks[idx].status = IDLE;
}

template <typename scalar_t>
__global__ void reduceKernel(scalar_t* dst, const scalar_t* src,
                             size_t numElements, size_t numRanks) {
    size_t thread_idx = blockIdx.x * blockDim.x + threadIdx.x;
    size_t stride = blockDim.x * gridDim.x;
    for (size_t elem_idx = thread_idx; elem_idx < numElements;
         elem_idx += stride) {
        scalar_t sum = 0;
        for (size_t rank = 0; rank < numRanks; ++rank) {
            sum += src[rank * numElements + elem_idx];
        }
        dst[elem_idx] = sum;
    }
}

void launchReduceKernel(at::Tensor dst, void* src, size_t numRanks,
                        cudaStream_t stream) {
    switch (dst.scalar_type()) {
        case c10::kInt:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<int>(), (int*)src,
                                                 dst.numel(), numRanks);
            break;
        case c10::kBFloat16:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<at::BFloat16>(),
                                                 (at::BFloat16*)src,
                                                 dst.numel(), numRanks);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce dtype: ",
                                        dst.scalar_type()));
    }
}

MooncakeWorker::MooncakeWorker(TransferEngine* engine, int rank, int size)
    : engine_(engine), rank_(rank), size_(size) {
    // Pin memory for task array
    cudaHostAlloc(&tasks_, kNumTasks_ * sizeof(Task), cudaHostAllocMapped);
    cudaHostGetDevicePointer(&tasks_device_, tasks_, 0);
    for (size_t i = 0; i < kNumTasks_; ++i) {
        tasks_[i].status = IDLE;
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTask(
    c10d::OpType opType, size_t tensorSize, cudaStream_t stream,
    const std::function<void(void* dst)>& tensorToBuffer,
    const std::function<void(void* src)>& bufferToTensor) {
    tensorToBuffer((void*)segment_descs_[rank_]->buffers[0].addr);
    enqueueTaskKernel<<<1, 1, 0, stream>>>(opType, tensorSize, tasks_device_,
                                           kNumTasks_);
    bufferToTensor((void*)segment_descs_[rank_]->buffers[1].addr);
    cudaEvent_t event;
    cudaEventCreateWithFlags(&event, cudaEventDisableTiming);
    cudaEventRecord(event, stream);
    return c10::make_intrusive<MooncakeWork>(opType, event);
}

}  // namespace mooncake