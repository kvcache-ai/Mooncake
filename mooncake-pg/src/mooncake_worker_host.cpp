// mooncake_worker_host.cpp — Host-side code for PG collectives.
// Compiled by g++ for both CUDA and MUSA builds. Uses kernel launch wrappers
// from mooncake_worker_kernels.cuh instead of <<<>>> syntax.

#include <memory>
#include <thread>
#include <mooncake_worker.cuh>
#include <mooncake_worker_kernels.cuh>

#include "error_types.h"

namespace mooncake {

void launchReduceKernel(void* dst, DataType dataType, size_t pos,
                        size_t realSize, void* src, size_t numRanks,
                        ReduceOp op, bool* activeRanks, cudaStream_t stream) {
    PG_ASSERT(op == ReduceOp::Sum || op == ReduceOp::Min ||
                  op == ReduceOp::Max || op == ReduceOp::Product,
              "Only support SUM/MIN/MAX/PRODUCT for reduction.");
    auto ptr = (char*)dst + pos;
    size_t num = realSize / elementSize(dataType);

    switch (dataType) {
        case DataType::Uint8:
            launchReduceKernel_uint8((uint8_t*)ptr, (uint8_t*)src, num,
                                     numRanks, (int)op, activeRanks, stream);
            break;
        case DataType::Int8:
            launchReduceKernel_int8((int8_t*)ptr, (int8_t*)src, num, numRanks,
                                    (int)op, activeRanks, stream);
            break;
        case DataType::Int16:
            launchReduceKernel_int16((int16_t*)ptr, (int16_t*)src, num,
                                     numRanks, (int)op, activeRanks, stream);
            break;
        case DataType::Int32:
            launchReduceKernel_int32((int*)ptr, (int*)src, num, numRanks,
                                     (int)op, activeRanks, stream);
            break;
        case DataType::Int64:
            launchReduceKernel_int64((int64_t*)ptr, (int64_t*)src, num,
                                     numRanks, (int)op, activeRanks, stream);
            break;
        case DataType::Float32:
            launchReduceKernel_float((float*)ptr, (float*)src, num, numRanks,
                                     (int)op, activeRanks, stream);
            break;
        case DataType::Float64:
            launchReduceKernel_double((double*)ptr, (double*)src, num, numRanks,
                                      (int)op, activeRanks, stream);
            break;
        case DataType::Bool:
            launchReduceKernel_bool((bool*)ptr, (bool*)src, num, numRanks,
                                    (int)op, activeRanks, stream);
            break;
        case DataType::Bfloat16:
            launchReduceKernel_bf16(ptr, src, num, numRanks, (int)op,
                                    activeRanks, stream);
            break;
        default:
            PG_ASSERT(false, "Unsupported reduce dtype: ", (int)dataType);
    }
}

template <typename T>
T applyReduceOp(const T& a, const T& b, ReduceOp op) {
    switch (op) {
        case ReduceOp::Sum:
            return a + b;
        case ReduceOp::Product:
            return a * b;
        case ReduceOp::Min:
            return std::min(a, b);
        case ReduceOp::Max:
            return std::max(a, b);
        default:
            PG_ASSERT(false, "Unsupported reduce op: ", (int)op);
    }
}

template <typename T>
void reduceCpu(T* dst, const T* src, size_t numElements, size_t numRanks,
               ReduceOp op, bool* activeRanks) {
    for (size_t i = 0; i < numElements; ++i) {
        bool valid = false;
        T acc{};
        for (size_t rank = 0; rank < numRanks; ++rank) {
            if (activeRanks[rank]) {
                if (!valid) {
                    acc = src[i + rank * numElements];
                    valid = true;
                } else {
                    acc = applyReduceOp(acc, src[i + rank * numElements], op);
                }
            }
        }
        dst[i] = acc;
    }
}

void launchReduceCpu(void* dst, DataType dataType, size_t pos, size_t realSize,
                     void* src, size_t numRanks, ReduceOp op,
                     bool* activeRanks) {
    auto ptr = (char*)dst + pos;
    size_t num = realSize / elementSize(dataType);

    switch (dataType) {
        case DataType::Uint8:
            reduceCpu((uint8_t*)ptr, (uint8_t*)src, num, numRanks, op,
                      activeRanks);
            break;
        case DataType::Int8:
            reduceCpu((int8_t*)ptr, (int8_t*)src, num, numRanks, op,
                      activeRanks);
            break;
        case DataType::Int16:
            reduceCpu((int16_t*)ptr, (int16_t*)src, num, numRanks, op,
                      activeRanks);
            break;
        case DataType::Int32:
            reduceCpu((int*)ptr, (int*)src, num, numRanks, op, activeRanks);
            break;
        case DataType::Int64:
            reduceCpu((int64_t*)ptr, (int64_t*)src, num, numRanks, op,
                      activeRanks);
            break;
        case DataType::Float32:
            reduceCpu((float*)ptr, (float*)src, num, numRanks, op, activeRanks);
            break;
        case DataType::Float64:
            reduceCpu((double*)ptr, (double*)src, num, numRanks, op,
                      activeRanks);
            break;
        case DataType::Bool:
            reduceCpu((bool*)ptr, (bool*)src, num, numRanks, op, activeRanks);
            break;
        default:
            PG_ASSERT(false, "Unsupported reduce dtype: ", (int)dataType);
    }
}

MooncakeWorker::MooncakeWorker(int cuda_device_index)
    : cuda_device_index_(cuda_device_index) {
    int deviceCount = 0;
    cudaError_t err = cudaGetDeviceCount(&deviceCount);
    if (!err && deviceCount > 0) {
        cudaHostAlloc(&tasks_, kNumTasks_ * sizeof(Task), cudaHostAllocMapped);
        cudaHostGetDevicePointer(&tasks_device_, tasks_, 0);
    } else {
        tasks_ = new Task[kNumTasks_];
    }

    if (cuda_device_index_ >= 0) {
        enqueue_stream_ = GpuStream::createNonBlocking(cuda_device_index_);
    }

    for (size_t i = 0; i < kNumTasks_; ++i) {
        tasks_[i].active = false;
        tasks_[i].submitSequence = 0;
        tasks_[i].failedRanksHint = nullptr;
        tasks_[i].resetFailedRanksHint = false;
        submitted_task_sequence_[i].store(0, std::memory_order_relaxed);
    }
}

MooncakeWorker::~MooncakeWorker() {
    running_ = false;
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

std::unique_ptr<WorkCompletion> MooncakeWorker::putTaskCpu(
    OpType opType, size_t tensorSize, int64_t broadcastRoot,
    const std::shared_ptr<TransferGroupMeta>& meta, int32_t* failed_ranks_hint,
    const std::function<void(void* dst, size_t pos, size_t realSize)>&
        copyToSendBuffer,
    const std::function<void(void* src, size_t pos, size_t realSize)>&
        copyFromRecvBuffer) {
    PG_ASSERT(failed_ranks_hint, "failed-ranks hint is null");
    size_t chunkSize = ((kBufferSize - 1) / meta->maxGroupSize) & ~(size_t)7;
    auto completion = std::make_shared<std::promise<void>>();
    auto future = completion->get_future().share();
    auto result = std::make_unique<WorkCompletion>(std::move(future));

    struct IterState {
        size_t currentPos = 0;
    };
    auto state = std::make_shared<IterState>();

    auto processNextChunk = std::make_shared<std::function<void()>>();
    std::weak_ptr<std::function<void()>> weakProcessNextChunk =
        processNextChunk;

    *processNextChunk = [this, weakProcessNextChunk, state, opType, tensorSize,
                         chunkSize, broadcastRoot, meta, copyToSendBuffer,
                         copyFromRecvBuffer, completion, failed_ranks_hint]() {
        auto processNextChunk = weakProcessNextChunk.lock();

        if (state->currentPos >= tensorSize) {
            completion->set_value();
            return;
        }

        int taskId = cpuTaskCount % 2;
        PG_ASSERT(!tasks_[taskId].active,
                  "collective CPU task slot is still active");
        size_t realSize = std::min(chunkSize, tensorSize - state->currentPos);
        int bufferOffset = meta->taskCount % 2;
        tasks_[taskId].opType = (int)opType;
        tasks_[taskId].dataSize = realSize;
        tasks_[taskId].broadcastRoot = broadcastRoot;
        tasks_[taskId].bufferOffset = bufferOffset;
        tasks_[taskId].submitSequence = 0;
        tasks_[taskId].failedRanksHint = failed_ranks_hint;
        tasks_[taskId].resetFailedRanksHint = state->currentPos == 0;
        tasks_[taskId].transferGroupMeta = meta.get();
        copyToSendBuffer(
            (void*)meta->segmentInfos[meta->rank].send_buffer[bufferOffset],
            state->currentPos, realSize);

        hasCallback_[taskId] = true;

        callbacks_[taskId] = [processNextChunk, state, meta, copyFromRecvBuffer,
                              bufferOffset, realSize, completion]() {
            copyFromRecvBuffer(
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

    return result;
}

void MooncakeWorker::putTaskCuda(
    OpType opType, size_t tensorSize, int64_t broadcastRoot,
    const std::shared_ptr<TransferGroupMeta>& meta, cudaStream_t issueStream,
    int32_t* failed_ranks_hint,
    const std::function<void(void* dst, size_t pos, size_t realSize,
                             cudaStream_t)>& copyToSendBuffer,
    const std::function<void(void* src, size_t pos, size_t realSize,
                             cudaStream_t)>& copyFromRecvBuffer) {
    size_t chunkSize = ((kBufferSize - 1) / meta->maxGroupSize) & ~(size_t)7;

    const GpuDeviceGuard guard(cuda_device_index_);
    const auto issue_stream =
        GpuStream::borrow(issueStream, cuda_device_index_);
    const auto& enq_stream = enqueue_stream_.value();

    GpuEvent event_start(issue_stream.deviceIndex());
    event_start.record(issue_stream);
    enq_stream.waitEvent(event_start);

    std::vector<CudaTaskSubmissionToken> submitted_tasks;
    submitted_tasks.reserve((tensorSize + chunkSize - 1) / chunkSize);
    for (size_t pos = 0; pos < tensorSize; pos += chunkSize) {
        size_t realSize = std::min(tensorSize, pos + chunkSize) - pos;
        int taskId = cudaTaskCount % 2 + 2;
        int bufferOffset = meta->taskCount % 2;

        const uint64_t taskSequence =
            next_cuda_task_sequence_.fetch_add(1, std::memory_order_relaxed);
        submitted_tasks.push_back(
            {.task_id = static_cast<size_t>(taskId), .sequence = taskSequence});
        copyToSendBuffer(
            (void*)meta->segmentInfos[meta->rank].send_buffer[bufferOffset],
            pos, realSize, enq_stream.get());

        hasCallback_[taskId] = false;

        launchEnqueueTaskKernel((int)opType, realSize, broadcastRoot,
                                bufferOffset, taskSequence, failed_ranks_hint,
                                pos == 0, meta.get(), tasks_device_, taskId,
                                enq_stream.get());
        copyFromRecvBuffer(
            (void*)meta->segmentInfos[meta->rank].recv_buffer[bufferOffset],
            pos, realSize, enq_stream.get());

        ++cudaTaskCount;
        ++meta->taskCount;
    }

    // During CUDA graph capture the kernels are recorded but not executed, so
    // waiting for the worker thread to observe them would hang.
    if (!issue_stream.isCapturing()) {
        waitUntilTasksSubmitted(submitted_tasks);
    }

    GpuEvent event_end(enq_stream.deviceIndex());
    event_end.record(enq_stream);
    issue_stream.waitEvent(event_end);
}

}  // namespace mooncake
