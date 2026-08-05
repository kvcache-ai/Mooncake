#include <work_handles.h>
#include <torch_utils.h>
#include <pg_utils.h>

#include <ATen/cuda/CUDAContext.h>
#include <ATen/cuda/CUDAGraphsUtils.cuh>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <tuple>
#include <utility>

namespace mooncake {
namespace {

using CompletionHandle = std::shared_ptr<::mooncakePgCompletion>;

CompletionHandle makeCompletionHandle(mooncakePgCompletion_t completion) {
    TORCH_CHECK(completion, "Mooncake PG core returned a null completion");
    return CompletionHandle(completion, [](mooncakePgCompletion_t handle) {
        if (handle) (void)mooncakePgCompletionDestroy(handle);
    });
}

bool queryCompletion(mooncakePgCompletion_t completion) {
    int completed = 0;
    checkResult(mooncakePgCompletionIsCompleted(completion, &completed),
                "mooncakePgCompletionIsCompleted");
    return completed != 0;
}

bool waitCompletion(mooncakePgCompletion_t completion, int64_t timeout_us) {
    const auto result = mooncakePgCompletionWait(completion, timeout_us);
    if (result == mooncakePgTimeout) return false;
    checkResult(result, "mooncakePgCompletionWait");
    return true;
}

std::function<void()> makePostCompletionOnce(
    std::function<void()> post_completion) {
    if (!post_completion) return {};
    return [once = std::make_shared<std::once_flag>(),
            post_completion = std::move(post_completion)]() mutable {
        std::call_once(*once, [&] { post_completion(); });
    };
}

template <typename... Resources>
std::any makeTrackedResources(Resources&&... resources) {
    return std::any(std::make_tuple(std::forward<Resources>(resources)...));
}

}  // namespace

FailedRanksHint FailedRanksHint::allocate(int size) {
    auto options =
        torch::TensorOptions().dtype(torch::kInt32).device(torch::kCPU);
    return FailedRanksHint(torch::zeros({size}, options));
}

bool FailedRanksHint::isLocalSuccess() const {
    const auto* values = data();
    return std::all_of(values, values + tensor.numel(),
                       [](int32_t value) { return value == 0; });
}

MooncakeWorkTracker::MooncakeWorkTracker() = default;

MooncakeWorkTracker::~MooncakeWorkTracker() { shutdown(); }

void MooncakeWorkTracker::retire(
    std::any resources, std::function<bool()> ready_to_release) noexcept {
    // Work destruction only transfers ownership. In particular, it does not
    // run a post-completion callback that may perform a device copy.
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_shutdown_) return;
    retired_.push_back({std::move(resources), std::move(ready_to_release)});
}

void MooncakeWorkTracker::retainUntilShutdown(std::any resources) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_shutdown_) return;
    retained_until_shutdown_.push_back(std::move(resources));
}

void MooncakeWorkTracker::evictCompleted() noexcept {
    std::vector<RetiredResources> candidates;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (is_shutdown_ || retired_.empty()) return;
        candidates.swap(retired_);
    }

    std::vector<RetiredResources> pending;
    pending.reserve(candidates.size());
    for (auto& candidate : candidates) {
        try {
            if (candidate.ready_to_release()) continue;
        } catch (const std::exception& error) {
            TORCH_WARN(
                "MooncakeWorkTracker: failed to process retired work; "
                "resources remain retained: ",
                error.what());
        } catch (...) {
            TORCH_WARN(
                "MooncakeWorkTracker: failed to process retired work; "
                "resources remain retained: unknown exception");
        }
        pending.push_back(std::move(candidate));
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (is_shutdown_) return;
    retired_.insert(retired_.end(), std::make_move_iterator(pending.begin()),
                    std::make_move_iterator(pending.end()));
}

void MooncakeWorkTracker::shutdown() noexcept {
    std::vector<RetiredResources> retired;
    std::vector<std::any> retained_until_shutdown;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (is_shutdown_) return;
        is_shutdown_ = true;
        retired.swap(retired_);
        retained_until_shutdown.swap(retained_until_shutdown_);
    }

    // The core communicator is shut down before this method is called. Query
    // once to run any ready CPU/P2P post-completion callbacks, then release
    // all retained resources. Captured GPU operations have no host callback
    // and are kept until this point solely to cover graph replay.
    for (auto& resources : retired) {
        try {
            (void)resources.ready_to_release();
        } catch (const std::exception& error) {
            TORCH_WARN(
                "MooncakeWorkTracker: retired work callback failed during "
                "shutdown: ",
                error.what());
        } catch (...) {
            TORCH_WARN(
                "MooncakeWorkTracker: retired work callback failed during "
                "shutdown: unknown exception");
        }
    }
}

MooncakeWorkCpu::MooncakeWorkCpu(c10d::OpType opType,
                                 mooncakePgCompletion_t completion,
                                 FailedRanksHint failedRanksHint,
                                 std::shared_ptr<MooncakeWorkTracker> tracker,
                                 std::vector<at::Tensor> keepAlive,
                                 std::function<void()> postCompletion)
    : Work(-1, opType),
      completion_(makeCompletionHandle(completion)),
      failed_ranks_hint_(std::move(failedRanksHint)),
      tracker_(std::move(tracker)),
      keep_alive_(std::move(keepAlive)),
      post_completion_(makePostCompletionOnce(std::move(postCompletion))) {}

MooncakeWorkCpu::~MooncakeWorkCpu() {
    if (!tracker_) return;
    auto completion = std::move(completion_);
    auto post_completion = std::move(post_completion_);
    tracker_->retire(makeTrackedResources(std::move(failed_ranks_hint_),
                                          std::move(keep_alive_)),
                     [completion = std::move(completion),
                      post_completion = std::move(post_completion)]() mutable {
                         if (!queryCompletion(completion.get())) return false;
                         if (post_completion) post_completion();
                         return true;
                     });
}

bool MooncakeWorkCpu::isCompleted() {
    const bool completed = queryCompletion(completion_.get());
    if (completed && post_completion_) post_completion_();
    return completed;
}

bool MooncakeWorkCpu::wait(std::chrono::milliseconds) {
    // Preserve the existing CPU Work behavior: its timeout argument is
    // ignored and wait blocks until the operation completes.
    if (!waitCompletion(completion_.get(), -1)) return false;
    if (post_completion_) post_completion_();
    return true;
}

at::Tensor MooncakeWorkCpu::getFailedRanksHint() const {
    return failed_ranks_hint_.tensor;
}

bool MooncakeWorkCpu::getLocalSuccess() const {
    return failed_ranks_hint_.isLocalSuccess();
}

MooncakeWorkCuda::MooncakeWorkCuda(c10d::OpType opType,
                                   std::shared_ptr<c10::Event> event,
                                   FailedRanksHint failedRanksHint,
                                   std::shared_ptr<MooncakeWorkTracker> tracker,
                                   std::vector<at::Tensor> keepAlive)
    : Work(-1, opType),
      event_(std::move(event)),
      is_captured_(at::cuda::currentStreamCaptureStatus() !=
                   c10::cuda::CaptureStatus::None),
      failed_ranks_hint_(std::move(failedRanksHint)),
      tracker_(std::move(tracker)),
      keep_alive_(std::move(keepAlive)) {
    TORCH_CHECK(event_, "Mooncake PG Torch event is null");
}

MooncakeWorkCuda::~MooncakeWorkCuda() {
    if (!tracker_) return;
    if (is_captured_) {
        tracker_->retainUntilShutdown(makeTrackedResources(
            std::move(event_), std::move(failed_ranks_hint_),
            std::move(keep_alive_)));
        return;
    }

    auto event = std::move(event_);
    tracker_->retire(makeTrackedResources(std::move(failed_ranks_hint_),
                                          std::move(keep_alive_)),
                     [event = std::move(event)] { return event->query(); });
}

bool MooncakeWorkCuda::wait(std::chrono::milliseconds) {
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

at::Tensor MooncakeWorkCuda::getFailedRanksHint() const {
    // Ensure the worker thread has completed the task and written
    // the failed-ranks bitmap before returning the tensor.
    if (event_ && at::cuda::currentStreamCaptureStatus() ==
                      c10::cuda::CaptureStatus::None) {
        event_->synchronize();
    }
    return failed_ranks_hint_.tensor;
}

bool MooncakeWorkCuda::getLocalSuccess() const {
    if (event_ && at::cuda::currentStreamCaptureStatus() ==
                      c10::cuda::CaptureStatus::None) {
        event_->synchronize();
    }
    return failed_ranks_hint_.isLocalSuccess();
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

MooncakeP2PWork::MooncakeP2PWork(c10d::OpType opType,
                                 mooncakePgCompletion_t completion,
                                 FailedRanksHint failedRanksHint,
                                 std::shared_ptr<MooncakeWorkTracker> tracker,
                                 std::vector<at::Tensor> keepAlive,
                                 std::function<void()> postCompletion)
    : Work(-1, opType),
      completion_(makeCompletionHandle(completion)),
      failed_ranks_hint_(std::move(failedRanksHint)),
      tracker_(std::move(tracker)),
      keep_alive_(std::move(keepAlive)),
      post_completion_(makePostCompletionOnce(std::move(postCompletion))) {}

MooncakeP2PWork::~MooncakeP2PWork() {
    if (!tracker_) return;
    auto completion = std::move(completion_);
    auto failed_ranks_hint = failed_ranks_hint_;
    auto post_completion = std::move(post_completion_);
    tracker_->retire(
        makeTrackedResources(std::move(failed_ranks_hint_),
                             std::move(keep_alive_)),
        [completion = std::move(completion),
         failed_ranks_hint = std::move(failed_ranks_hint),
         post_completion = std::move(post_completion)]() mutable {
            if (!queryCompletion(completion.get())) return false;
            if (failed_ranks_hint.isLocalSuccess() && post_completion) {
                post_completion();
            }
            return true;
        });
}

bool MooncakeP2PWork::isCompleted() {
    const bool completed = queryCompletion(completion_.get());
    if (completed && failed_ranks_hint_.isLocalSuccess() && post_completion_) {
        post_completion_();
    }
    return completed;
}

bool MooncakeP2PWork::isSuccess() const {
    return queryCompletion(completion_.get()) &&
           failed_ranks_hint_.isLocalSuccess();
}

bool MooncakeP2PWork::wait(std::chrono::milliseconds timeout) {
    const int64_t timeout_us =
        timeout.count() > 0
            ? std::chrono::duration_cast<std::chrono::microseconds>(timeout)
                  .count()
            : -1;
    if (!waitCompletion(completion_.get(), timeout_us)) return false;
    if (failed_ranks_hint_.isLocalSuccess() && post_completion_) {
        post_completion_();
    }
    return true;
}

at::Tensor MooncakeP2PWork::getFailedRanksHint() const {
    return failed_ranks_hint_.tensor;
}

bool MooncakeP2PWork::getLocalSuccess() const { return isSuccess(); }

}  // namespace mooncake
