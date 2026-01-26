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
                             c10d::ReduceOp::RedOpType op, bool* activeRanks) {
    size_t thread_idx = blockIdx.x * blockDim.x + threadIdx.x;
    size_t stride = blockDim.x * gridDim.x;
    for (size_t elem_idx = thread_idx; elem_idx < numElements;
         elem_idx += stride) {
        bool valid = false;
        scalar_t acc = 0;
        for (size_t rank = 0; rank < numRanks; ++rank) {
            if (activeRanks[rank]) {
                if (!valid) {
                    acc = src[rank * numElements + elem_idx];
                    valid = true;
                } else {
                    switch (op) {
                        case c10d::ReduceOp::SUM:
                            acc += src[rank * numElements + elem_idx];
                            break;
                        case c10d::ReduceOp::MIN:
                            acc = std::min(src[rank * numElements + elem_idx],
                                           acc);
                            break;
                        case c10d::ReduceOp::MAX:
                            acc = std::max(src[rank * numElements + elem_idx],
                                           acc);
                            break;
                        case c10d::ReduceOp::PRODUCT:
                            acc *= src[rank * numElements + elem_idx];
                            break;
                        default:
                            // never  
                    }
                }
            }
        }
        dst[elem_idx] = acc;
    }
}

void launchReduceKernel(at::Tensor dst, size_t pos, size_t realSize, void* src,
                        size_t numRanks, c10d::ReduceOp op, bool* activeRanks,
                        cudaStream_t stream) {
    TORCH_CHECK(op == c10d::ReduceOp::SUM || op == c10d::ReduceOp::MIN ||
                op == c10d::ReduceOp::MAX || op == c10d::ReduceOp::PRODUCT,
                "Only support SUM/MIN/MAX/PRODUCT for reduction.");
    auto ptr = (char*)dst.data_ptr() + pos;
    size_t num = realSize / dst.element_size();

    switch (dst.scalar_type()) {
        case c10::kByte:
            reduceKernel<<<64, 256, 0, stream>>>((uint8_t*)ptr, (uint8_t*)src,
                                                 num, numRanks, op.op_,
                                                 activeRanks);
            break;
        case c10::kChar:
            reduceKernel<<<64, 256, 0, stream>>>(
                (int8_t*)ptr, (int8_t*)src, num, numRanks, op.op_, activeRanks);
            break;
        case c10::kShort:
            reduceKernel<<<64, 256, 0, stream>>>((int16_t*)ptr, (int16_t*)src,
                                                 num, numRanks, op.op_,
                                                 activeRanks);
            break;
        case c10::kInt:
            reduceKernel<<<64, 256, 0, stream>>>((int*)ptr, (int*)src, num,
                                                 numRanks, op.op_, activeRanks);
            break;
        case c10::kLong:
            reduceKernel<<<64, 256, 0, stream>>>((int64_t*)ptr, (int64_t*)src,
                                                 num, numRanks, op.op_,
                                                 activeRanks);
            break;
        case c10::kFloat:
            reduceKernel<<<64, 256, 0, stream>>>((float*)ptr, (float*)src, num,
                                                 numRanks, op.op_, activeRanks);
            break;
        case c10::kDouble:
            reduceKernel<<<64, 256, 0, stream>>>(
                (double*)ptr, (double*)src, num, numRanks, op.op_, activeRanks);
            break;
        case c10::kBool:
            reduceKernel<<<64, 256, 0, stream>>>((bool*)ptr, (bool*)src, num,
                                                 numRanks, op.op_, activeRanks);
            break;
        case c10::kBFloat16:
            reduceKernel<<<64, 256, 0, stream>>>((at::BFloat16*)ptr,
                                                 (at::BFloat16*)src, num,
                                                 numRanks, op.op_, activeRanks);
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
               c10d::ReduceOp op, bool* activeRanks) {
    at::parallel_for(0, numElements, 1024, [&](int64_t begin, int64_t end) {
        for (int64_t i = begin; i < end; ++i) {
            bool valid = false;
            T acc = src[i];
            for (int64_t rank = 0; rank < numRanks; ++rank) {
                if (activeRanks[rank]) {
                    if (!valid) {
                        acc = src[i];
                        valid = true;
                    } else {
                        acc =
                            applyReduceOp(acc, src[i + rank * numElements], op);
                    }
                }
            }
            dst[i] = acc;
        }
    });
}

void launchReduceCpu(at::Tensor dst, size_t pos, size_t realSize, void* src,
                     size_t numRanks, c10d::ReduceOp op, bool* activeRanks) {
    auto ptr = (char*)dst.data_ptr() + pos;
    size_t num = realSize / dst.element_size();

    switch (dst.scalar_type()) {
        case c10::kByte:
            reduceCpu((uint8_t*)ptr, (uint8_t*)src, num, numRanks, op,
                      activeRanks);
            break;
        case c10::kChar:
            reduceCpu((int8_t*)ptr, (int8_t*)src, num, numRanks, op,
                      activeRanks);
            break;
        case c10::kShort:
            reduceCpu((int16_t*)ptr, (int16_t*)src, num, numRanks, op,
                      activeRanks);
            break;
        case c10::kInt:
            reduceCpu((int*)ptr, (int*)src, num, numRanks, op, activeRanks);
            break;
        case c10::kLong:
            reduceCpu((int64_t*)ptr, (int64_t*)src, num, numRanks, op,
                      activeRanks);
            break;
        case c10::kFloat:
            reduceCpu((float*)ptr, (float*)src, num, numRanks, op, activeRanks);
            break;
        case c10::kDouble:
            reduceCpu((double*)ptr, (double*)src, num, numRanks, op,
                      activeRanks);
            break;
        case c10::kBool:
            reduceCpu((bool*)ptr, (bool*)src, num, numRanks, op, activeRanks);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce dtype: ",
                                        dst.scalar_type()));
    }
}

MooncakeWorker::MooncakeWorker() {
    int deviceCount = 0;
    cudaError err = cudaGetDeviceCount(&deviceCount);
    if (!err && deviceCount > 0) {
        // Pin memory for task array
        cudaHostAlloc(&tasks_, kNumTasks_ * sizeof(Task), cudaHostAllocMapped);
        cudaHostGetDevicePointer(&tasks_device_, tasks_, 0);
    } else {
        LOG(WARNING) << "No CUDA device found. Only the `mooncake-cpu` backend "
                        "can be used.";
        tasks_ = new Task[kNumTasks_];
    }
    for (size_t i = 0; i < kNumTasks_; ++i) {
        tasks_[i].active = false;
    }

    // Start worker
    startWorker();
}

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTaskCpu(
    c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
    TransferGroupMeta* meta,
    const std::function<void(void* dst, size_t pos, size_t realSize)>&
        tensorToBuffer,
    const std::function<void(void* src, size_t pos, size_t realSize)>&
        bufferToTensor) {
    size_t chunkSize = ((kBufferSize - 1) / meta->size) & ~(size_t)7;
    auto future = c10::make_intrusive<c10::ivalue::Future>(
        c10::ListType::create(c10::TensorType::get()));

    struct IterState {
        size_t currentPos = 0;
    };
    auto state = std::make_shared<IterState>();

    auto processNextChunk = std::make_shared<std::function<void()>>();

    *processNextChunk = [this, processNextChunk, state, opType, tensorSize,
                         chunkSize, broadcastRoot, meta, tensorToBuffer,
                         bufferToTensor, future]() {
        if (state->currentPos >= tensorSize) {
            future->markCompleted(c10::IValue());
            return;
        }

        int taskId = cpuTaskCount % 2;
        TORCH_CHECK(!tasks_[taskId].active);

        size_t realSize = std::min(chunkSize, tensorSize - state->currentPos);
        int bufferOffset = meta->taskCount % 2;

        tasks_[taskId].opType = opType;
        tasks_[taskId].tensorSize = realSize;
        tasks_[taskId].broadcastRoot = broadcastRoot;
        tasks_[taskId].bufferOffset = bufferOffset;
        tasks_[taskId].transferGroupMeta = meta;
        tensorToBuffer(
            (void*)meta->segmentInfos[meta->rank].send_buffer[bufferOffset],
            state->currentPos, realSize);

        hasCallback_[taskId] = true;

        callbacks_[taskId] = [this, processNextChunk, state, meta,
                              bufferToTensor, bufferOffset, realSize,
                              future]() {
            for (int i = 0; i < meta->size; ++i) {
                meta->activeRanksTensor[i] = meta->activeRanks[i] ? 1 : 0;
            }
            bufferToTensor(
                (void*)meta->segmentInfos[meta->rank].recv_buffer[bufferOffset],
                state->currentPos, realSize);

            state->currentPos += realSize;

            (*processNextChunk)();
        };

        tasks_[taskId].active = true;
        ++cpuTaskCount;
        ++meta->taskCount;
    };

    (*processNextChunk)();

    return c10::make_intrusive<MooncakeWorkCpu>(opType, future);
}

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTaskCuda(
    c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
    TransferGroupMeta* meta, const at::cuda::CUDAStream& stream,
    const std::function<void(void* dst, size_t pos, size_t realSize)>&
        tensorToBuffer,
    const std::function<void(void* src, size_t pos, size_t realSize)>&
        bufferToTensor) {
    // TORCH_CHECK(tensorSize * meta->size < kBufferSize, "Too large!");
    //  Alternately use even-odd items to maintain tasks
    size_t chunkSize = ((kBufferSize - 1) / meta->size) & ~(size_t)7;

    for (size_t pos = 0; pos < tensorSize; pos += chunkSize) {
        size_t realSize = min(tensorSize, pos + chunkSize) - pos;
        int taskId = cudaTaskCount % 2 + 2;
        int bufferOffset = meta->taskCount % 2;
        tensorToBuffer(
            (void*)meta->segmentInfos[meta->rank].send_buffer[bufferOffset],
            pos, realSize);

        hasCallback_[taskId] = false;
        enqueueTaskKernel<<<1, 1, 0, stream>>>(
            opType, realSize, broadcastRoot, bufferOffset, meta, tasks_device_,
            meta->size, meta->activeRanksDevice,
            meta->activeRanksTensor.data_ptr<int>(), taskId);
        bufferToTensor(
            (void*)meta->segmentInfos[meta->rank].recv_buffer[bufferOffset],
            pos, realSize);

        ++cudaTaskCount;
        ++meta->taskCount;
    }

    auto event = std::make_shared<torch::Event>(torch::kCUDA);
    event->record(stream);
    return c10::make_intrusive<MooncakeWorkCuda>(opType, event);
}

}  // namespace mooncake