#include <mooncake_backend.h>
#include <mooncake_worker.cuh>

namespace mooncake {

class MooncakeWorkCpu : public ::c10d::Work {
   public:
    MooncakeWorkCpu(c10d::OpType opType,
                    c10::intrusive_ptr<c10::ivalue::Future> future)
        : Work(-1, opType), future_(future) {}

    bool isCompleted() override { return future_->completed(); }

    bool wait(std::chrono::milliseconds timeout) override {
        future_->wait();
        return future_->completed() && !future_->hasError();
    }

   private:
    c10::intrusive_ptr<c10::ivalue::Future> future_;
};

class MooncakeWorkCuda : public ::c10d::Work {
   public:
    MooncakeWorkCuda(c10d::OpType opType, cudaEvent_t event)
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
                                  int64_t broadcastRoot, Task* tasks,
                                  size_t numTasks) {
    // Find idle task
    int idx = findIdleTask(tasks, numTasks);
    assert(idx >= 0);

    // Copy task into slot
    tasks[idx].opType = opType;
    tasks[idx].tensorSize = tensorSize;
    tasks[idx].broadcastRoot = broadcastRoot;

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

template <typename T>
void reduceCpu(T* dst, const T* src, size_t numElements,
                          size_t numRanks) {
    at::parallel_for(0, numElements, 1024, [&](int64_t begin, int64_t end) {
        for (int64_t i = begin; i < end; ++i) {
            T acc = T(0);
            for (int64_t rank = 0; rank < numRanks; ++rank) {
                acc += src[i + rank * numElements];
            }
            dst[i] = acc;
        }
    });
}

void launchReduceCpu(at::Tensor dst, void* src, size_t numRanks) {
    switch (dst.scalar_type()) {
        case c10::kInt:
            reduceCpu(dst.data_ptr<int>(), (int*)src, dst.numel(), numRanks);
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

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTaskCpu(
    c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
    const std::function<void(void* dst)>& tensorToBuffer,
    const std::function<void(void* src)>& bufferToTensor) {
    TORCH_CHECK(tensorSize * size_ < (1u << 29), "Too large!");
    auto future = c10::make_intrusive<c10::ivalue::Future>(
        c10::ListType::create(c10::TensorType::get()));
    for (size_t i = 0; i < kNumTasks_; ++i) {
        if (tasks_[i].status == IDLE) {
            tasks_[i].status = OCCUPIED;
            tasks_[i].opType = opType;
            tasks_[i].tensorSize = tensorSize;
            tasks_[i].broadcastRoot = broadcastRoot;
            tensorToBuffer(
                (void*)segment_descs_[rank_]->buffers[taskCount % 2].addr);

            std::thread([this, i, bufferToTensor, future, opType] {
                tasks_[i].status = READY;
                while (tasks_[i].status != DONE) {
                    _mm_pause();
                }
                bufferToTensor((void*)segment_descs_[rank_]
                                   ->buffers[2 + taskCount % 2]
                                   .addr);
                tasks_[i].status = IDLE;
                ++taskCount;
                future->markCompleted(c10::IValue());
            }).detach();
            return c10::make_intrusive<MooncakeWorkCpu>(opType, future);
        }
    }
    TORCH_CHECK(false, "No task slots available.");
}

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTaskCuda(
    c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
    cudaStream_t stream, const std::function<void(void* dst)>& tensorToBuffer,
    const std::function<void(void* src)>& bufferToTensor) {
    TORCH_CHECK(tensorSize * size_ < (1u << 29), "Too large!");
    tensorToBuffer((void*)segment_descs_[rank_]->buffers[taskCount % 2].addr);
    enqueueTaskKernel<<<1, 1, 0, stream>>>(opType, tensorSize, broadcastRoot,
                                           tasks_device_, kNumTasks_);
    bufferToTensor(
        (void*)segment_descs_[rank_]->buffers[2 + taskCount % 2].addr);
    ++taskCount;
    cudaEvent_t event;
    cudaEventCreateWithFlags(&event, cudaEventDisableTiming);
    cudaEventRecord(event, stream);
    return c10::make_intrusive<MooncakeWorkCuda>(opType, event);
}

}  // namespace mooncake