#include <work_handles.h>
#include <mooncake_worker.cuh>
#include <ATen/cuda/CUDAContext.h>
#include <ATen/cuda/CUDAGraphsUtils.cuh>
#include <cuda_runtime.h>
#include "pg_utils.h"

namespace mooncake {

FailedRanks FailedRanks::allocate(int n, bool isCpu) {
    if (isCpu) {
        return {torch::zeros({n}, torch::kInt32), nullptr};
    }
    int* host_ptr = nullptr;
    int* dev_ptr = nullptr;
    cudaError_t err =
        cudaHostAlloc(&host_ptr, n * sizeof(int), cudaHostAllocMapped);
    TORCH_CHECK(err == cudaSuccess,
                "cudaHostAlloc failed: ", cudaGetErrorString(err));
    err = cudaHostGetDevicePointer(reinterpret_cast<void**>(&dev_ptr), host_ptr,
                                   0);
    TORCH_CHECK(err == cudaSuccess,
                "cudaHostGetDevicePointer failed: ", cudaGetErrorString(err));
    std::memset(host_ptr, 0, n * sizeof(int));
    auto deleter = [](void* ptr) { cudaFreeHost(ptr); };
    auto t = torch::from_blob(host_ptr, {n}, deleter,
                              torch::TensorOptions().dtype(torch::kInt32));
    return {t, dev_ptr};
}

bool MooncakeWorkCuda::wait(std::chrono::milliseconds timeout) {
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
        submitted = worker_->waitUntilTasksSubmitted(submitted_tasks_, timeout);
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

bool MooncakeBarrierWorkCuda::wait(std::chrono::milliseconds timeout) {
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

at::Tensor MooncakeWorkCuda::getFailedRanks() const {
    // Ensure the worker thread has completed the task and set failedRanksHost
    // before returning the tensor.
    if (event_ && at::cuda::currentStreamCaptureStatus() ==
                      c10::cuda::CaptureStatus::None) {
        event_->synchronize();
    }
    return failedRanks_.tensor;
}

bool MooncakeP2PWork::isCompleted() {
    return status_->load(std::memory_order_acquire) !=
           MooncakeP2PWork::Status::kPending;
}

bool MooncakeP2PWork::isSuccess() const {
    return status_->load(std::memory_order_acquire) ==
           MooncakeP2PWork::Status::kSuccess;
}

bool MooncakeP2PWork::wait(std::chrono::milliseconds timeout) {
    BackoffWaiterConfig cfg{};
    cfg.max_sleep = std::chrono::microseconds(10);
    BackoffWaiter waiter(cfg);

    bool done = false;
    if (timeout.count() > 0) {
        done = waiter.wait_for(timeout, [this] {
            return status_->load(std::memory_order_acquire) !=
                   MooncakeP2PWork::Status::kPending;
        });
    } else {
        waiter.wait([this] {
            return status_->load(std::memory_order_acquire) !=
                   MooncakeP2PWork::Status::kPending;
        });
        done = true;
    }

    if (!done) {
        return false;
    }

    return true;
}

}  // namespace mooncake
