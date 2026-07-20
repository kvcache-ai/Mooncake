// mooncake_worker_host.cpp — Host-side code for PG collectives.
// Compiled by g++ for both CUDA and MUSA builds. Uses kernel launch wrappers
// from mooncake_worker_kernels.cuh instead of <<<>>> syntax.

#include <mooncake_backend.h>
#include <cstdio>
#include <memory>
#include <thread>
#include <mooncake_worker.cuh>
#include <mooncake_worker_kernels.cuh>
#include <work_handles.h>
#include <ATen/cuda/CUDAGraphsUtils.cuh>

#include "pg_utils.h"

namespace mooncake {

void launchReduceKernel(at::Tensor dst, size_t pos, size_t realSize, void* src,
                        size_t numRanks, c10d::ReduceOp op, bool* activeRanks,
                        int* failedRanks, cudaStream_t stream) {
    TORCH_CHECK(op == c10d::ReduceOp::SUM || op == c10d::ReduceOp::MIN ||
                    op == c10d::ReduceOp::MAX || op == c10d::ReduceOp::PRODUCT,
                "Only support SUM/MIN/MAX/PRODUCT for reduction.");
    auto ptr = (char*)dst.data_ptr() + pos;
    size_t num = realSize / dst.element_size();

    switch (dst.scalar_type()) {
        case c10::kByte:
            launchReduceKernel_uint8((uint8_t*)ptr, (uint8_t*)src, num,
                                     numRanks, (int)op, activeRanks,
                                     failedRanks, stream);
            break;
        case c10::kChar:
            launchReduceKernel_int8((int8_t*)ptr, (int8_t*)src, num, numRanks,
                                    (int)op, activeRanks, failedRanks, stream);
            break;
        case c10::kShort:
            launchReduceKernel_int16((int16_t*)ptr, (int16_t*)src, num,
                                     numRanks, (int)op, activeRanks,
                                     failedRanks, stream);
            break;
        case c10::kInt:
            launchReduceKernel_int32((int*)ptr, (int*)src, num, numRanks,
                                     (int)op, activeRanks, failedRanks, stream);
            break;
        case c10::kLong:
            launchReduceKernel_int64((int64_t*)ptr, (int64_t*)src, num,
                                     numRanks, (int)op, activeRanks,
                                     failedRanks, stream);
            break;
        case c10::kFloat:
            launchReduceKernel_float((float*)ptr, (float*)src, num, numRanks,
                                     (int)op, activeRanks, failedRanks, stream);
            break;
        case c10::kDouble:
            launchReduceKernel_double((double*)ptr, (double*)src, num, numRanks,
                                      (int)op, activeRanks, failedRanks,
                                      stream);
            break;
        case c10::kBool:
            launchReduceKernel_bool((bool*)ptr, (bool*)src, num, numRanks,
                                    (int)op, activeRanks, failedRanks, stream);
            break;
        case c10::kBFloat16:
            launchReduceKernel_bf16(ptr, src, num, numRanks, (int)op,
                                    activeRanks, failedRanks, stream);
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
               c10d::ReduceOp op, bool* activeRanks, int* failedRanks) {
    at::parallel_for(0, numElements, 1024, [&](int64_t begin, int64_t end) {
        for (int64_t i = begin; i < end; ++i) {
            bool valid = false;
            T acc{};
            for (int64_t rank = 0; rank < numRanks; ++rank) {
                bool shouldInclude = activeRanks[rank] &&
                                     (!failedRanks || failedRanks[rank] == 0);
                if (shouldInclude) {
                    if (!valid) {
                        acc = src[i + rank * numElements];
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
                     size_t numRanks, c10d::ReduceOp op, bool* activeRanks,
                     int* failedRanks) {
    auto ptr = (char*)dst.data_ptr() + pos;
    size_t num = realSize / dst.element_size();

    switch (dst.scalar_type()) {
        case c10::kByte:
            reduceCpu((uint8_t*)ptr, (uint8_t*)src, num, numRanks, op,
                      activeRanks, failedRanks);
            break;
        case c10::kChar:
            reduceCpu((int8_t*)ptr, (int8_t*)src, num, numRanks, op,
                      activeRanks, failedRanks);
            break;
        case c10::kShort:
            reduceCpu((int16_t*)ptr, (int16_t*)src, num, numRanks, op,
                      activeRanks, failedRanks);
            break;
        case c10::kInt:
            reduceCpu((int*)ptr, (int*)src, num, numRanks, op, activeRanks,
                      failedRanks);
            break;
        case c10::kLong:
            reduceCpu((int64_t*)ptr, (int64_t*)src, num, numRanks, op,
                      activeRanks, failedRanks);
            break;
        case c10::kFloat:
            reduceCpu((float*)ptr, (float*)src, num, numRanks, op, activeRanks,
                      failedRanks);
            break;
        case c10::kDouble:
            reduceCpu((double*)ptr, (double*)src, num, numRanks, op,
                      activeRanks, failedRanks);
            break;
        case c10::kBool:
            reduceCpu((bool*)ptr, (bool*)src, num, numRanks, op, activeRanks,
                      failedRanks);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce dtype: ",
                                        dst.scalar_type()));
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
        LOG(WARNING) << "No GPU device found. Only the `mooncake-cpu` backend "
                        "can be used.";
        tasks_ = new Task[kNumTasks_];
    }
    for (size_t i = 0; i < kNumTasks_; ++i) {
        tasks_[i].active = false;
        tasks_[i].submitSequence = 0;
        submitted_task_sequence_[i].store(0, std::memory_order_relaxed);
    }
}

MooncakeWorker::~MooncakeWorker() {
    running_ = false;
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTaskCpu(
    c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
    const std::shared_ptr<TransferGroupMeta>& meta,
    const std::shared_ptr<ConnectionContext>& connection_ctx,
    FailedRanks failed_ranks,
    const std::function<void(void* dst, size_t pos, size_t realSize)>&
        tensorToBuffer,
    const std::function<void(void* src, size_t pos, size_t realSize)>&
        bufferToTensor) {
    connection_ctx->waitUntilNewRanksConnected();

    size_t chunkSize = ((kBufferSize - 1) / meta->size) & ~(size_t)7;
    auto future = c10::make_intrusive<c10::ivalue::Future>(
        c10::ListType::create(c10::TensorType::get()));

    struct IterState {
        size_t currentPos = 0;
    };
    auto state = std::make_shared<IterState>();

    auto processNextChunk = std::make_shared<std::function<void()>>();
    std::weak_ptr<std::function<void()>> weakProcessNextChunk =
        processNextChunk;

    int* failedRanksPtr = failed_ranks.data();

    *processNextChunk = [this, weakProcessNextChunk, state, opType, tensorSize,
                         chunkSize, broadcastRoot, meta, tensorToBuffer,
                         bufferToTensor, future, failedRanksPtr]() {
        auto processNextChunk = weakProcessNextChunk.lock();

        if (state->currentPos >= tensorSize) {
            future->markCompleted(c10::IValue());
            return;
        }

        int taskId = cpuTaskCount % 2;
        TORCH_CHECK(!tasks_[taskId].active);

        size_t realSize = std::min(chunkSize, tensorSize - state->currentPos);
        int bufferOffset = meta->taskCount % 2;

        tasks_[taskId].opType = (int)opType;
        tasks_[taskId].tensorSize = realSize;
        tasks_[taskId].broadcastRoot = broadcastRoot;
        tasks_[taskId].bufferOffset = bufferOffset;
        tasks_[taskId].transferGroupMeta = meta.get();
        tasks_[taskId].failedRanksHost = failedRanksPtr;
        tensorToBuffer(
            (void*)meta->segmentInfos[meta->rank].send_buffer[bufferOffset],
            state->currentPos, realSize);

        hasCallback_[taskId] = true;

        callbacks_[taskId] = [this, processNextChunk, state, meta,
                              bufferToTensor, bufferOffset, realSize,
                              future]() {
            if (meta->activeRanksTensor.device().is_cpu()) {
                for (int i = 0; i < meta->size; ++i) {
                    meta->activeRanksTensor[i] = meta->activeRanks[i] ? 1 : 0;
                }
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

    return c10::make_intrusive<MooncakeWorkCpu>(opType, future, meta,
                                                std::move(failed_ranks));
}

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTaskCuda(
    c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
    const std::shared_ptr<TransferGroupMeta>& meta,
    const std::shared_ptr<ConnectionContext>& connection_ctx,
    const at::cuda::CUDAStream& issue_stream, FailedRanks failed_ranks,
    const std::function<void(void* dst, size_t pos, size_t realSize,
                             const at::cuda::CUDAStream&)>& tensorToBuffer,
    const std::function<void(void* src, size_t pos, size_t realSize,
                             const at::cuda::CUDAStream&)>& bufferToTensor) {
    connection_ctx->waitUntilNewRanksConnected();

    size_t chunkSize = ((kBufferSize - 1) / meta->size) & ~(size_t)7;

    at::cuda::CUDAStream enq_stream =
        at::cuda::getStreamFromPool(false, issue_stream.device_index());

    auto event_start = std::make_shared<torch::Event>(torch::kCUDA);
    event_start->record(issue_stream);
    event_start->block(enq_stream);

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
        tensorToBuffer(
            (void*)meta->segmentInfos[meta->rank].send_buffer[bufferOffset],
            pos, realSize, enq_stream);

        hasCallback_[taskId] = false;
        launchEnqueueTaskKernel(
            (int)opType, realSize, broadcastRoot, bufferOffset, taskSequence,
            meta.get(), tasks_device_, meta->size, meta->activeRanksDevice,
            meta->activeRanksTensor.data_ptr<int>(), failed_ranks.data(),
            taskId, enq_stream.stream());
        bufferToTensor(
            (void*)meta->segmentInfos[meta->rank].recv_buffer[bufferOffset],
            pos, realSize, enq_stream);

        ++cudaTaskCount;
        ++meta->taskCount;
    }

    auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
    event_end->record(enq_stream);

    if (opType == c10d::OpType::BARRIER) {
        return c10::make_intrusive<MooncakeBarrierWorkCuda>(
            opType, event_end, meta, this, std::move(submitted_tasks),
            std::move(failed_ranks));
    }
    return c10::make_intrusive<MooncakeWorkCuda>(opType, event_end, meta, this,
                                                 std::move(submitted_tasks),
                                                 std::move(failed_ranks));
}

}  // namespace mooncake
