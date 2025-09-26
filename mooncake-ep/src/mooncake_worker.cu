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
    MooncakeWorkCuda(c10d::OpType opType, std::shared_ptr<torch::Event> event)
        : Work(-1, opType), event_(event) {}

    bool isCompleted() override { return event_->query(); }

    bool wait(std::chrono::milliseconds timeout) override {
        return true;  // This should be a no-op
    }

   private:
    std::shared_ptr<torch::Event> event_;
};

__global__ void enqueueTaskKernel(c10d::OpType opType, size_t tensorSize,
                                  int64_t broadcastRoot, int bufferOffset,
                                  void* meta, Task* tasks, int numRanks,
                                  const bool* activeRanks,
                                  int* activeRanksTensor, size_t taskId) {
    // Copy task into slot
    tasks[taskId].opType = opType;
    tasks[taskId].tensorSize = tensorSize;
    tasks[taskId].broadcastRoot = broadcastRoot;
    tasks[taskId].bufferOffset = bufferOffset;
    tasks[taskId].transferGroupMeta = meta;

    // Mark active
    __threadfence();  // Ensure writes visible to host
    tasks[taskId].active = true;

    // Spin-wait until CPU proxy sets DONE
    while (tasks[taskId].active) {
        __threadfence();
    }
    for (int i = 0; i < numRanks; ++i) {
        activeRanksTensor[i] = activeRanks[i] ? 1 : 0;
    }
}

template <typename scalar_t>
__global__ void reduceKernel(scalar_t* dst, const scalar_t* src,
                             size_t numElements, size_t numRanks,
                             bool* activeRanks) {
    size_t thread_idx = blockIdx.x * blockDim.x + threadIdx.x;
    size_t stride = blockDim.x * gridDim.x;
    for (size_t elem_idx = thread_idx; elem_idx < numElements;
         elem_idx += stride) {
        scalar_t sum = 0;
        for (size_t rank = 0; rank < numRanks; ++rank) {
            if (activeRanks[rank]) {
                sum += src[rank * numElements + elem_idx];
            }
        }
        dst[elem_idx] = sum;
    }
}

void launchReduceKernel(at::Tensor dst, void* src, size_t numRanks,
                        c10d::ReduceOp op, bool* activeRanks,
                        cudaStream_t stream) {
    TORCH_CHECK(op == c10d::ReduceOp::SUM, "Only support SUM for reduction.");
    switch (dst.scalar_type()) {
        case c10::kByte:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<uint8_t>(),
                                                 (uint8_t*)src, dst.numel(),
                                                 numRanks, activeRanks);
            break;
        case c10::kChar:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<int8_t>(),
                                                 (int8_t*)src, dst.numel(),
                                                 numRanks, activeRanks);
            break;
        case c10::kShort:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<int16_t>(),
                                                 (int16_t*)src, dst.numel(),
                                                 numRanks, activeRanks);
            break;
        case c10::kInt:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<int>(), (int*)src,
                                                 dst.numel(), numRanks,
                                                 activeRanks);
            break;
        case c10::kLong:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<int64_t>(),
                                                 (int64_t*)src, dst.numel(),
                                                 numRanks, activeRanks);
            break;
        case c10::kFloat:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<float>(),
                                                 (float*)src, dst.numel(),
                                                 numRanks, activeRanks);
            break;
        case c10::kDouble:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<double>(),
                                                 (double*)src, dst.numel(),
                                                 numRanks, activeRanks);
            break;
        case c10::kBool:
            reduceKernel<<<64, 256, 0, stream>>>(dst.data_ptr<bool>(),
                                                 (bool*)src, dst.numel(),
                                                 numRanks, activeRanks);
            break;
        case c10::kBFloat16:
            reduceKernel<<<64, 256, 0, stream>>>(
                dst.data_ptr<at::BFloat16>(), (at::BFloat16*)src, dst.numel(),
                numRanks, activeRanks);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce dtype: ",
                                        dst.scalar_type()));
    }
}

template <typename T>
T applyReduceOp(const T& a, const T& b, c10d::ReduceOp op) {
    switch (op) {
        case c10d::ReduceOp::SUM:
            return a + b;
        case c10d::ReduceOp::PRODUCT:
            return a * b;
        case c10d::ReduceOp::MIN:
            return std::min(a, b);
        case c10d::ReduceOp::MAX:
            return std::max(a, b);
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce op: ", op));
    }
}

template <typename T>
void reduceCpu(T* dst, const T* src, size_t numElements, size_t numRanks,
               c10d::ReduceOp op) {
    at::parallel_for(0, numElements, 1024, [&](int64_t begin, int64_t end) {
        for (int64_t i = begin; i < end; ++i) {
            T acc = src[i];
            for (int64_t rank = 1; rank < numRanks; ++rank) {
                acc = applyReduceOp(acc, src[i + rank * numElements], op);
            }
            dst[i] = acc;
        }
    });
}

void launchReduceCpu(at::Tensor dst, void* src, size_t numRanks,
                     c10d::ReduceOp op) {
    switch (dst.scalar_type()) {
        case c10::kByte:
            reduceCpu(dst.data_ptr<uint8_t>(), (uint8_t*)src, dst.numel(),
                      numRanks, op);
            break;
        case c10::kChar:
            reduceCpu(dst.data_ptr<int8_t>(), (int8_t*)src, dst.numel(),
                      numRanks, op);
            break;
        case c10::kShort:
            reduceCpu(dst.data_ptr<int16_t>(), (int16_t*)src, dst.numel(),
                      numRanks, op);
            break;
        case c10::kInt:
            reduceCpu(dst.data_ptr<int>(), (int*)src, dst.numel(), numRanks,
                      op);
            break;
        case c10::kLong:
            reduceCpu(dst.data_ptr<int64_t>(), (int64_t*)src, dst.numel(),
                      numRanks, op);
            break;
        case c10::kFloat:
            reduceCpu(dst.data_ptr<float>(), (float*)src, dst.numel(), numRanks,
                      op);
            break;
        case c10::kDouble:
            reduceCpu(dst.data_ptr<double>(), (double*)src, dst.numel(),
                      numRanks, op);
            break;
        case c10::kBool:
            reduceCpu(dst.data_ptr<bool>(), (bool*)src, dst.numel(), numRanks,
                      op);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce dtype: ",
                                        dst.scalar_type()));
    }
}

MooncakeWorker::MooncakeWorker() {
    // Pin memory for task array
    cudaHostAlloc(&tasks_, kNumTasks_ * sizeof(Task), cudaHostAllocMapped);
    cudaHostGetDevicePointer(&tasks_device_, tasks_, 0);
    for (size_t i = 0; i < kNumTasks_; ++i) {
        tasks_[i].active = false;
    }

    // Start worker
    startWorker();
}

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTaskCpu(
    c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
    TransferGroupMeta* meta,
    const std::function<void(void* dst)>& tensorToBuffer,
    const std::function<void(void* src)>& bufferToTensor) {
    TORCH_CHECK(tensorSize * meta->size < kBufferSize, "Too large!");
    auto future = c10::make_intrusive<c10::ivalue::Future>(
        c10::ListType::create(c10::TensorType::get()));
    // Alternately use even-odd items to maintain tasks
    int taskId = cpuTaskCount % 2;
    TORCH_CHECK(!tasks_[taskId].active);
    int bufferOffset = meta->bufferBaseIndex + meta->taskCount % 2;

    tasks_[taskId].opType = opType;
    tasks_[taskId].tensorSize = tensorSize;
    tasks_[taskId].broadcastRoot = broadcastRoot;
    tasks_[taskId].bufferOffset = bufferOffset;
    tasks_[taskId].transferGroupMeta = meta;
    tensorToBuffer(
        (void*)meta->segmentDescs[meta->rank]->buffers[bufferOffset].addr);

    hasCallback_[taskId] = true;
    callbacks_[taskId] = [this, meta, bufferToTensor, bufferOffset, future] {
        for (int i = 0; i < meta->size; ++i) {
            meta->activeRanksTensor[i] = meta->activeRanks[i] ? 1 : 0;
        }
        bufferToTensor((void*)meta->segmentDescs[meta->rank]
                           ->buffers[bufferOffset + 2]
                           .addr);
        future->markCompleted(c10::IValue());
    };

    tasks_[taskId].active = true;
    ++cpuTaskCount;
    ++meta->taskCount;
    return c10::make_intrusive<MooncakeWorkCpu>(opType, future);
}

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTaskCuda(
    c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
    TransferGroupMeta* meta, const at::cuda::CUDAStream& stream,
    const std::function<void(void* dst)>& tensorToBuffer,
    const std::function<void(void* src)>& bufferToTensor) {
    TORCH_CHECK(tensorSize * meta->size < kBufferSize, "Too large!");
    // Alternately use even-odd items to maintain tasks
    int taskId = cudaTaskCount % 2 + 2;
    int bufferOffset = meta->bufferBaseIndex + meta->taskCount % 2;
    tensorToBuffer(
        (void*)meta->segmentDescs[meta->rank]->buffers[bufferOffset].addr);

    hasCallback_[taskId] = false;
    enqueueTaskKernel<<<1, 1, 0, stream>>>(
        opType, tensorSize, broadcastRoot, bufferOffset, meta, tasks_device_,
        meta->size, meta->activeRanksDevice,
        meta->activeRanksTensor.data_ptr<int>(), taskId);
    bufferToTensor(
        (void*)meta->segmentDescs[meta->rank]->buffers[bufferOffset + 2].addr);
    ++cudaTaskCount;
    ++meta->taskCount;
    auto event = std::make_shared<torch::Event>(torch::kCUDA);
    event->record(stream);
    return c10::make_intrusive<MooncakeWorkCuda>(opType, event);
}

}  // namespace mooncake