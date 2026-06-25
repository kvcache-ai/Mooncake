// mooncake_worker_host.cpp — Host-side code for PG collectives.
// Compiled by g++ for both CUDA and MUSA builds. Uses kernel launch wrappers
// from mooncake_worker_kernels.cuh instead of <<<>>> syntax.

#include <mooncake_backend.h>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <memory>
#include <thread>
#include <mooncake_worker.cuh>
#include <mooncake_worker_kernels.cuh>
#include <ATen/cuda/CUDAGraphsUtils.cuh>

#include "pg_utils.h"

namespace mooncake {

namespace {

bool pgProfileEnabled() {
    static const bool enabled = [] {
        const char* value = std::getenv("MOONCAKE_PG_PROFILE");
        return value && value[0] != '\0' && value[0] != '0';
    }();
    return enabled;
}

bool pgSkipSelfEnabled() {
    static const bool enabled = [] {
        const char* value = std::getenv("MOONCAKE_PG_SKIP_SELF");
        return value && value[0] != '\0' && value[0] != '0';
    }();
    return enabled;
}

bool pgCanSkipSelf(c10d::OpType opType) {
    switch (opType) {
        case c10d::OpType::ALLREDUCE:
        case c10d::OpType::ALLGATHER:
        case c10d::OpType::_ALLGATHER_BASE:
        case c10d::OpType::_REDUCE_SCATTER_BASE:
        case c10d::OpType::ALLTOALL:
        case c10d::OpType::ALLTOALL_BASE:
            return true;
        default:
            return false;
    }
}

uint64_t profileNowUs() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration_cast<std::chrono::microseconds>(
               clock::now().time_since_epoch())
        .count();
}

}  // namespace

class MooncakeWorkCpu : public ::c10d::Work {
   public:
    MooncakeWorkCpu(c10d::OpType opType,
                    c10::intrusive_ptr<c10::ivalue::Future> future,
                    std::shared_ptr<TransferGroupMeta> meta)
        : Work(-1, opType),
          future_(std::move(future)),
          meta_(std::move(meta)) {}

    bool isCompleted() override { return future_->completed(); }

    bool wait(std::chrono::milliseconds timeout) override {
        future_->wait();
        return future_->completed() && !future_->hasError();
    }

   private:
    c10::intrusive_ptr<c10::ivalue::Future> future_;
    std::shared_ptr<TransferGroupMeta> meta_;
};

class MooncakeWorkCuda : public ::c10d::Work {
   public:
    MooncakeWorkCuda(c10d::OpType opType, std::shared_ptr<torch::Event> event,
                     std::shared_ptr<TransferGroupMeta> meta,
                     const MooncakeWorker* worker,
                     std::vector<CudaTaskSubmissionToken> submitted_tasks)
        : Work(-1, opType),
          event_(std::move(event)),
          meta_(std::move(meta)),
          worker_(worker),
          submitted_tasks_(std::move(submitted_tasks)) {}

    bool isCompleted() override { return event_->query(); }

    bool wait(std::chrono::milliseconds timeout) override {
        // Wait until the task has been submitted to TransferEngine:
        // This tries to ensure that the CUDA kernels required for the transfer
        // have been launched by the time `waitUntilTasksSubmitted` returns.
        //
        // Why is this needed? PyTorch documentation implies that collective
        // operations should be enqueued when `wait()` returns. In practice, we
        // found that violating this causes hangs.
        //
        // Our current hypothesis for the hang is: PyTorch assumes the kernels
        // needed for the transfer are already launched when `wait` returns
        // true. It may then launch subsequent operations after the collective
        // (e.g., `.cpu()`). Such operations may acquire a process-wide lock in
        // the CUDA runtime. Also, they may rely on the data produced by the
        // collective, thus causing a synchronization on enq_stream. However,
        // holding that runtime lock prevents cudaMemcpy(Async) in TE/TENT from
        // launching. This means the transfer can't finish, and enq_stream won't
        // complete. Thus, a deadlock occurs.
        // (In practice, we found that replacing all cudaMemcpyAsync in TENT
        // with cuMemcpyAsync actually alleviates this, which further suggests a
        // deadlock in the CUDA runtime. However, that change is too invasive
        // for TE/TENT, so we do not adopt it here.)
        //
        // Strictly speaking, the wait is needed for another reason: The current
        // stream will be blocked on the event below. Any subsequent work on
        // `current_stream` will wait on that event, which effectively waits for
        // the task to be done. Therefore, we must ensure all kernels needed for
        // the transfer task are launched BEFORE blocking the current stream, in
        // case TE/TENT use `current_stream` to launch those kernels (though it
        // is rare).
        //
        // Please note that this logic relies on the assumption that TE/TENT
        // will launch all CUDA operations in `submitTransfer`.
        // Unfortunately, TcpTransport in TE and TENT currently violates this
        // assumption (cudaMemcpy(Async) may be called later from a callback),
        // which can cause hangs in PG when a CUDA operation such as
        // `x.cpu().item()` follows the collective. For TE's TcpTransport, the
        // use of cudaMemcpy on the default stream may also contribute to the
        // hang.
        //
        // Besides, for CPU-only transports (like RdmaTransport),
        // waitUntilTasksSubmitted is totally unnecessary, but we keep it for
        // uniform behavior to avoid invasive changes to TE/TENT.
        bool submitted = true;
        if (at::cuda::currentStreamCaptureStatus() ==
            c10::cuda::CaptureStatus::None) {
            // Normal execution: block until tasks are submitted.
            submitted =
                worker_->waitUntilTasksSubmitted(submitted_tasks_, timeout);
        } else {
            // During CUDA graph capture, kernels are recorded but not actually
            // executed. The enqueueTaskKernel would never run, so
            // waitUntilTasksSubmitted would hang because the CPU worker thread
            // never sees task.active == true.
            //
            // Note that this also means NvlinkTransport (and TcpTransport too,
            // of course) won't work with CUDA Graphs: Kernels launched inside
            // TE/TENT can't be captured by the graph, and during replay they
            // are not ordered with the graph execution. This may trigger the
            // same deadlock described above.
        }
        if (!submitted) return false;

        // Once all tasks have been submitted, use the event to synchronize
        // the current stream and the enqueue stream, but do not wait on this
        // event.
        //
        // See PyTorch docs for more details:
        // https://docs.pytorch.org/docs/stable/distributed.html#synchronous-and-asynchronous-collective-operations
        //   "wait() - in the case of CPU collectives, will block the process
        //    until the operation is completed. In the case of CUDA collectives,
        //    will block the currently active CUDA stream until the operation
        //    is completed (but will not block the CPU)."
        auto current_stream = at::cuda::getCurrentCUDAStream();
        event_->block(current_stream);
        return true;
    }

   protected:
    std::shared_ptr<torch::Event> event_;
    std::shared_ptr<TransferGroupMeta> meta_;
    const MooncakeWorker* worker_;
    std::vector<CudaTaskSubmissionToken> submitted_tasks_;
};

class MooncakeBarrierWorkCuda : public MooncakeWorkCuda {
   public:
    using MooncakeWorkCuda::MooncakeWorkCuda;

    bool wait(std::chrono::milliseconds timeout) override {
        // Skip host-side synchronization during CUDA graph capture.
        // cudaEventSynchronize is not permitted while a stream is capturing.
        if (at::cuda::currentStreamCaptureStatus() !=
            c10::cuda::CaptureStatus::None) {
            // We still need stream-level synchronization so that subsequent
            // operations on the capture stream are ordered after the barrier
            // task on the enqueue stream.
            auto current_stream = at::cuda::getCurrentCUDAStream();
            event_->block(current_stream);
            return true;
        }

        if (timeout == kNoTimeout) {
            event_->synchronize();
            return true;
        }

        BackoffWaiter waiter(
            BackoffWaiterConfig::constantSleep(std::chrono::microseconds(10)));
        return waiter.wait_for(timeout, [this] { return event_->query(); });
    }
};

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
            launchReduceKernel_uint8((uint8_t*)ptr, (uint8_t*)src, num,
                                     numRanks, (int)op, activeRanks, stream);
            break;
        case c10::kChar:
            launchReduceKernel_int8((int8_t*)ptr, (int8_t*)src, num, numRanks,
                                    (int)op, activeRanks, stream);
            break;
        case c10::kShort:
            launchReduceKernel_int16((int16_t*)ptr, (int16_t*)src, num,
                                     numRanks, (int)op, activeRanks, stream);
            break;
        case c10::kInt:
            launchReduceKernel_int32((int*)ptr, (int*)src, num, numRanks,
                                     (int)op, activeRanks, stream);
            break;
        case c10::kLong:
            launchReduceKernel_int64((int64_t*)ptr, (int64_t*)src, num,
                                     numRanks, (int)op, activeRanks, stream);
            break;
        case c10::kFloat:
            launchReduceKernel_float((float*)ptr, (float*)src, num, numRanks,
                                     (int)op, activeRanks, stream);
            break;
        case c10::kDouble:
            launchReduceKernel_double((double*)ptr, (double*)src, num, numRanks,
                                      (int)op, activeRanks, stream);
            break;
        case c10::kBool:
            launchReduceKernel_bool((bool*)ptr, (bool*)src, num, numRanks,
                                    (int)op, activeRanks, stream);
            break;
        case c10::kBFloat16:
            launchReduceKernel_bf16(ptr, src, num, numRanks, (int)op,
                                    activeRanks, stream);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce dtype: ",
                                        dst.scalar_type()));
    }
}

void launchP2pReduceScatterSlottedKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t tensorSize,
    size_t slotStride, int slots, int rank, int numRanks, uint32_t sequence,
    cudaStream_t stream) {
    size_t num = tensorSize / output.element_size();
    switch (output.scalar_type()) {
        case c10::kByte:
            launchP2pReduceScatterSlottedKernel_uint8(
                (uint8_t*)output.data_ptr(), (const uint8_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kChar:
            launchP2pReduceScatterSlottedKernel_int8(
                (int8_t*)output.data_ptr(), (const int8_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kShort:
            launchP2pReduceScatterSlottedKernel_int16(
                (int16_t*)output.data_ptr(), (const int16_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kInt:
            launchP2pReduceScatterSlottedKernel_int32(
                (int*)output.data_ptr(), (const int*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kLong:
            launchP2pReduceScatterSlottedKernel_int64(
                (int64_t*)output.data_ptr(), (const int64_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kFloat:
            launchP2pReduceScatterSlottedKernel_float(
                (float*)output.data_ptr(), (const float*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kDouble:
            launchP2pReduceScatterSlottedKernel_double(
                (double*)output.data_ptr(), (const double*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kBool:
            launchP2pReduceScatterSlottedKernel_bool(
                (bool*)output.data_ptr(), (const bool*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kBFloat16:
            launchP2pReduceScatterSlottedKernel_bf16(
                output.data_ptr(), input.data_ptr(), local_recv_base, peer_ptrs,
                available, num, slotStride, slots, rank, numRanks, sequence,
                stream);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce_scatter dtype: ",
                                        output.scalar_type()));
    }
}

void launchP2pReduceScatterSlottedGraphKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t tensorSize,
    size_t slotStride, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, cudaStream_t stream) {
    size_t num = tensorSize / output.element_size();
    switch (output.scalar_type()) {
        case c10::kByte:
            launchP2pReduceScatterSlottedGraphKernel_uint8(
                (uint8_t*)output.data_ptr(), (const uint8_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kChar:
            launchP2pReduceScatterSlottedGraphKernel_int8(
                (int8_t*)output.data_ptr(), (const int8_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kShort:
            launchP2pReduceScatterSlottedGraphKernel_int16(
                (int16_t*)output.data_ptr(), (const int16_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kInt:
            launchP2pReduceScatterSlottedGraphKernel_int32(
                (int*)output.data_ptr(), (const int*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kLong:
            launchP2pReduceScatterSlottedGraphKernel_int64(
                (int64_t*)output.data_ptr(), (const int64_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kFloat:
            launchP2pReduceScatterSlottedGraphKernel_float(
                (float*)output.data_ptr(), (const float*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kDouble:
            launchP2pReduceScatterSlottedGraphKernel_double(
                (double*)output.data_ptr(), (const double*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kBool:
            launchP2pReduceScatterSlottedGraphKernel_bool(
                (bool*)output.data_ptr(), (const bool*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kBFloat16:
            launchP2pReduceScatterSlottedGraphKernel_bf16(
                output.data_ptr(), input.data_ptr(), local_recv_base, peer_ptrs,
                available, num, slotStride, slots, rank, numRanks,
                baseSequenceSlot, stream);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce_scatter dtype: ",
                                        output.scalar_type()));
    }
}

void launchP2pReduceScatterSlottedChunkedKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t tensorSize,
    size_t chunkBytes, size_t slotStride, int slots, int rank, int numRanks,
    uint32_t baseSequence, cudaStream_t stream) {
    TORCH_CHECK(tensorSize % output.element_size() == 0,
                "reduce_scatter tensorSize must align to element size");
    TORCH_CHECK(chunkBytes % output.element_size() == 0,
                "reduce_scatter chunkBytes must align to element size");
    size_t fullNum = tensorSize / output.element_size();
    size_t chunkNum = chunkBytes / output.element_size();
    switch (output.scalar_type()) {
        case c10::kByte:
            launchP2pReduceScatterSlottedChunkedKernel_uint8(
                (uint8_t*)output.data_ptr(), (const uint8_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequence, stream);
            break;
        case c10::kChar:
            launchP2pReduceScatterSlottedChunkedKernel_int8(
                (int8_t*)output.data_ptr(), (const int8_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequence, stream);
            break;
        case c10::kShort:
            launchP2pReduceScatterSlottedChunkedKernel_int16(
                (int16_t*)output.data_ptr(), (const int16_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequence, stream);
            break;
        case c10::kInt:
            launchP2pReduceScatterSlottedChunkedKernel_int32(
                (int*)output.data_ptr(), (const int*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequence, stream);
            break;
        case c10::kLong:
            launchP2pReduceScatterSlottedChunkedKernel_int64(
                (int64_t*)output.data_ptr(), (const int64_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequence, stream);
            break;
        case c10::kFloat:
            launchP2pReduceScatterSlottedChunkedKernel_float(
                (float*)output.data_ptr(), (const float*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequence, stream);
            break;
        case c10::kDouble:
            launchP2pReduceScatterSlottedChunkedKernel_double(
                (double*)output.data_ptr(), (const double*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequence, stream);
            break;
        case c10::kBool:
            launchP2pReduceScatterSlottedChunkedKernel_bool(
                (bool*)output.data_ptr(), (const bool*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequence, stream);
            break;
        case c10::kBFloat16:
            launchP2pReduceScatterSlottedChunkedKernel_bf16(
                output.data_ptr(), input.data_ptr(), local_recv_base, peer_ptrs,
                available, fullNum, chunkNum, slotStride, slots, rank, numRanks,
                baseSequence, stream);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce_scatter dtype: ",
                                        output.scalar_type()));
    }
}

void launchP2pReduceScatterSlottedChunkedGraphKernel(
    at::Tensor& output, at::Tensor& input, void* local_recv_base,
    void** peer_ptrs, int32_t* available, size_t tensorSize,
    size_t chunkBytes, size_t slotStride, int slots, int rank, int numRanks,
    const uint32_t* baseSequenceSlot, cudaStream_t stream) {
    TORCH_CHECK(tensorSize % output.element_size() == 0,
                "reduce_scatter tensorSize must align to element size");
    TORCH_CHECK(chunkBytes % output.element_size() == 0,
                "reduce_scatter chunkBytes must align to element size");
    size_t fullNum = tensorSize / output.element_size();
    size_t chunkNum = chunkBytes / output.element_size();
    switch (output.scalar_type()) {
        case c10::kByte:
            launchP2pReduceScatterSlottedChunkedGraphKernel_uint8(
                (uint8_t*)output.data_ptr(), (const uint8_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kChar:
            launchP2pReduceScatterSlottedChunkedGraphKernel_int8(
                (int8_t*)output.data_ptr(), (const int8_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kShort:
            launchP2pReduceScatterSlottedChunkedGraphKernel_int16(
                (int16_t*)output.data_ptr(), (const int16_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kInt:
            launchP2pReduceScatterSlottedChunkedGraphKernel_int32(
                (int*)output.data_ptr(), (const int*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kLong:
            launchP2pReduceScatterSlottedChunkedGraphKernel_int64(
                (int64_t*)output.data_ptr(), (const int64_t*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kFloat:
            launchP2pReduceScatterSlottedChunkedGraphKernel_float(
                (float*)output.data_ptr(), (const float*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kDouble:
            launchP2pReduceScatterSlottedChunkedGraphKernel_double(
                (double*)output.data_ptr(), (const double*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kBool:
            launchP2pReduceScatterSlottedChunkedGraphKernel_bool(
                (bool*)output.data_ptr(), (const bool*)input.data_ptr(),
                local_recv_base, peer_ptrs, available, fullNum, chunkNum,
                slotStride, slots, rank, numRanks, baseSequenceSlot, stream);
            break;
        case c10::kBFloat16:
            launchP2pReduceScatterSlottedChunkedGraphKernel_bf16(
                output.data_ptr(), input.data_ptr(), local_recv_base, peer_ptrs,
                available, fullNum, chunkNum, slotStride, slots, rank, numRanks,
                baseSequenceSlot, stream);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported reduce_scatter dtype: ",
                                        output.scalar_type()));
    }
}

void launchP2pAllReduceSlottedKernel(
    at::Tensor& tensor, void* local_recv_base, void** peer_ptrs,
    int32_t* available, size_t tensorSize, size_t slotStride, int slots,
    int rank, int numRanks, uint32_t sequence, cudaStream_t stream) {
    size_t num = tensorSize / tensor.element_size();
    switch (tensor.scalar_type()) {
        case c10::kByte:
            launchP2pAllReduceSlottedKernel_uint8(
                (uint8_t*)tensor.data_ptr(), (const uint8_t*)tensor.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kChar:
            launchP2pAllReduceSlottedKernel_int8(
                (int8_t*)tensor.data_ptr(), (const int8_t*)tensor.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kShort:
            launchP2pAllReduceSlottedKernel_int16(
                (int16_t*)tensor.data_ptr(), (const int16_t*)tensor.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kInt:
            launchP2pAllReduceSlottedKernel_int32(
                (int*)tensor.data_ptr(), (const int*)tensor.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kLong:
            launchP2pAllReduceSlottedKernel_int64(
                (int64_t*)tensor.data_ptr(), (const int64_t*)tensor.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kFloat:
            launchP2pAllReduceSlottedKernel_float(
                (float*)tensor.data_ptr(), (const float*)tensor.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kDouble:
            launchP2pAllReduceSlottedKernel_double(
                (double*)tensor.data_ptr(), (const double*)tensor.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kBool:
            launchP2pAllReduceSlottedKernel_bool(
                (bool*)tensor.data_ptr(), (const bool*)tensor.data_ptr(),
                local_recv_base, peer_ptrs, available, num, slotStride, slots,
                rank, numRanks, sequence, stream);
            break;
        case c10::kBFloat16:
            launchP2pAllReduceSlottedKernel_bf16(
                tensor.data_ptr(), tensor.data_ptr(), local_recv_base, peer_ptrs,
                available, num, slotStride, slots, rank, numRanks, sequence,
                stream);
            break;
        default:
            TORCH_CHECK(false, c10::str("Unsupported all_reduce dtype: ",
                                        tensor.scalar_type()));
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
            T acc{};
            for (int64_t rank = 0; rank < numRanks; ++rank) {
                if (activeRanks[rank]) {
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

    *processNextChunk = [this, weakProcessNextChunk, state, opType, tensorSize,
                         chunkSize, broadcastRoot, meta, tensorToBuffer,
                         bufferToTensor, future]() {
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

    return c10::make_intrusive<MooncakeWorkCpu>(opType, future, meta);
}

c10::intrusive_ptr<c10d::Work> MooncakeWorker::putTaskCuda(
    c10d::OpType opType, size_t tensorSize, int64_t broadcastRoot,
    const std::shared_ptr<TransferGroupMeta>& meta,
    const std::shared_ptr<ConnectionContext>& connection_ctx,
    const at::cuda::CUDAStream& issue_stream,
    const std::function<void(void* dst, size_t pos, size_t realSize,
                             const at::cuda::CUDAStream&)>& tensorToBuffer,
    const std::function<void(void* src, size_t pos, size_t realSize,
                             const at::cuda::CUDAStream&)>& bufferToTensor) {
    const bool profile = pgProfileEnabled();
    const uint64_t api_start_us = profile ? profileNowUs() : 0;
    connection_ctx->waitUntilNewRanksConnected();
    const uint64_t wait_conn_us = profile ? profileNowUs() - api_start_us : 0;

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
        uint64_t issue_start_us = 0;
        uint64_t before_us = 0;
        uint64_t tensor_to_buffer_us = 0;
        uint64_t enqueue_us = 0;
        uint64_t buffer_to_tensor_us = 0;
        if (profile) {
            issue_start_us = profileNowUs();
            before_us = issue_start_us;
        }
        tensorToBuffer(
            (void*)meta->segmentInfos[meta->rank].send_buffer[bufferOffset],
            pos, realSize, enq_stream);
        if (pgSkipSelfEnabled() && pgCanSkipSelf(opType) &&
            meta->activeRanks[meta->rank]) {
            uint64_t self_source =
                meta->segmentInfos[meta->rank].send_buffer[bufferOffset];
            if (opType == c10d::OpType::_REDUCE_SCATTER_BASE ||
                opType == c10d::OpType::ALLTOALL ||
                opType == c10d::OpType::ALLTOALL_BASE) {
                self_source += meta->rank * realSize;
            }
            uint64_t self_target =
                meta->segmentInfos[meta->rank].recv_buffer[bufferOffset] +
                meta->rank * realSize;
            cudaMemcpyAsync((void*)self_target, (void*)self_source, realSize,
                            cudaMemcpyDeviceToDevice, enq_stream);
        }
        if (profile) {
            const uint64_t now_us = profileNowUs();
            tensor_to_buffer_us = now_us - before_us;
            before_us = now_us;
        }

        hasCallback_[taskId] = false;
        launchEnqueueTaskKernel(
            (int)opType, realSize, broadcastRoot, bufferOffset, taskSequence,
            meta.get(), tasks_device_, meta->size, meta->activeRanksDevice,
            meta->activeRanksTensor.data_ptr<int>(), taskId,
            enq_stream.stream());
        if (profile) {
            const uint64_t now_us = profileNowUs();
            enqueue_us = now_us - before_us;
            before_us = now_us;
        }
        bufferToTensor(
            (void*)meta->segmentInfos[meta->rank].recv_buffer[bufferOffset],
            pos, realSize, enq_stream);
        if (profile) {
            const uint64_t now_us = profileNowUs();
            buffer_to_tensor_us = now_us - before_us;
            PgProfileSlot slot;
            slot.sequence = taskSequence;
            slot.issue_start_us = issue_start_us;
            slot.wait_conn_us = pos == 0 ? wait_conn_us : 0;
            slot.tensor_to_buffer_us = tensor_to_buffer_us;
            slot.enqueue_us = enqueue_us;
            slot.buffer_to_tensor_us = buffer_to_tensor_us;
            slot.host_issue_us = now_us - issue_start_us;
            profile_slots_[taskId] = slot;
        }

        ++cudaTaskCount;
        ++meta->taskCount;
    }

    auto event_end = std::make_shared<torch::Event>(torch::kCUDA);
    event_end->record(enq_stream);

    if (opType == c10d::OpType::BARRIER) {
        return c10::make_intrusive<MooncakeBarrierWorkCuda>(
            opType, event_end, meta, this, std::move(submitted_tasks));
    }
    return c10::make_intrusive<MooncakeWorkCuda>(opType, event_end, meta, this,
                                                 std::move(submitted_tasks));
}

}  // namespace mooncake
