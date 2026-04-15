#pragma once

#include <atomic>
#include <condition_variable>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

#include <glog/logging.h>

#include "tiered_cache/tiered_backend.h"
#include "types.h"

namespace mooncake {

// ============================================================================
// LocalCopyPlan — describes a local memcpy operation
// ============================================================================

struct LocalCopyPlan {
    AllocationHandle source_handle;
    const char* source_ptr = nullptr;
    size_t source_size = 0;
    bool use_single_dest = false;
    void* single_dest_ptr = nullptr;
    size_t single_dest_size = 0;
    std::vector<Slice> dest_slices;
};

ErrorCode ExecuteLocalCopyPlan(const LocalCopyPlan& plan);

// ============================================================================
// AsyncMemcpyExecutor
// ============================================================================

class AsyncMemcpyExecutor {
   public:
    template <typename ResultType>
    struct BatchState {
        std::vector<ResultType> results;
        std::atomic<size_t> remaining{0};
        std::mutex done_mutex;
        std::condition_variable done_cv;
        bool done = false;
    };

    template <typename ResultType>
    struct BatchHandle {
        std::shared_ptr<BatchState<ResultType>> state;

        std::vector<ResultType> Wait() const {
            if (!state) {
                return {};
            }
            if (state->remaining.load(std::memory_order_acquire) > 0) {
                std::unique_lock<std::mutex> lock(state->done_mutex);
                auto state_ptr = state;
                state->done_cv.wait(lock,
                                    [state_ptr] { return state_ptr->done; });
            }
            return state->results;
        }
    };

    AsyncMemcpyExecutor(size_t worker_num, size_t max_queue_size);
    ~AsyncMemcpyExecutor();

    template <typename ResultType, typename TaskFn, typename ErrorFn>
    BatchHandle<ResultType> SubmitBatchTasks(
        const std::vector<size_t>& indices, TaskFn&& task_fn,
        ErrorFn&& on_error);

    template <typename ResultType, typename Fn>
    std::future<ResultType> SubmitSingleTask(Fn&& fn);

    void Shutdown();

   private:
    struct QueueTask {
        std::function<void()> run;
        std::function<void()> cancel;
    };

    void WorkerMain();

    template <typename ResultType>
    static void FinishBatchTask(
        const std::shared_ptr<BatchState<ResultType>>& state,
        size_t batch_index, ResultType result) {
        if (!state || batch_index >= state->results.size()) {
            return;
        }
        state->results[batch_index] = std::move(result);
        if (state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lock(state->done_mutex);
            state->done = true;
            state->done_cv.notify_one();
        }
    }

    size_t max_queue_size_ = 0;
    bool shutting_down_ = false;
    std::mutex mutex_;
    std::condition_variable queue_not_empty_cv_;
    std::condition_variable queue_not_full_cv_;
    std::queue<QueueTask> tasks_;
    std::vector<std::thread> workers_;
};

// ============================================================================
// Template method implementations (must be in header)
// ============================================================================

template <typename ResultType, typename TaskFn, typename ErrorFn>
AsyncMemcpyExecutor::BatchHandle<ResultType>
AsyncMemcpyExecutor::SubmitBatchTasks(const std::vector<size_t>& indices,
                                      TaskFn&& task_fn, ErrorFn&& on_error) {
    auto batch_state = std::make_shared<BatchState<ResultType>>();
    batch_state->results.reserve(indices.size());

    using TaskFnType = typename std::decay<TaskFn>::type;
    using ErrorFnType = typename std::decay<ErrorFn>::type;
    auto task_fn_ptr =
        std::make_shared<TaskFnType>(std::forward<TaskFn>(task_fn));
    auto on_error_ptr =
        std::make_shared<ErrorFnType>(std::forward<ErrorFn>(on_error));

    for (size_t index : indices) {
        batch_state->results.push_back((*on_error_ptr)(index));
    }
    batch_state->remaining.store(indices.size(), std::memory_order_relaxed);
    if (indices.empty()) {
        batch_state->done = true;
        return BatchHandle<ResultType>{batch_state};
    }

    size_t enqueued = 0;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        for (; enqueued < indices.size(); ++enqueued) {
            queue_not_full_cv_.wait(lock, [this] {
                return shutting_down_ || tasks_.size() < max_queue_size_;
            });
            if (shutting_down_) {
                break;
            }

            const size_t slot = enqueued;
            const size_t index = indices[slot];
            QueueTask queue_task;
            queue_task.run = [batch_state, task_fn_ptr, on_error_ptr, slot,
                              index]() mutable {
                ResultType result = (*on_error_ptr)(index);
                try {
                    result = (*task_fn_ptr)(index);
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Async batch task threw at index " << index
                               << ": " << e.what();
                    result = (*on_error_ptr)(index);
                } catch (...) {
                    LOG(ERROR) << "Async batch task threw unknown exception "
                                  "at index "
                               << index;
                    result = (*on_error_ptr)(index);
                }
                FinishBatchTask(batch_state, slot, std::move(result));
            };
            queue_task.cancel = [batch_state, on_error_ptr, slot, index]() {
                FinishBatchTask(batch_state, slot, (*on_error_ptr)(index));
            };
            tasks_.push(std::move(queue_task));
        }
    }

    if (enqueued > 0) {
        queue_not_empty_cv_.notify_all();
    }

    for (size_t slot = enqueued; slot < indices.size(); ++slot) {
        const size_t index = indices[slot];
        FinishBatchTask(batch_state, slot, (*on_error_ptr)(index));
    }
    return BatchHandle<ResultType>{batch_state};
}

template <typename ResultType, typename Fn>
std::future<ResultType> AsyncMemcpyExecutor::SubmitSingleTask(Fn&& fn) {
    auto promise = std::make_shared<std::promise<ResultType>>();
    auto future = promise->get_future();

    using FnType = typename std::decay<Fn>::type;
    auto fn_ptr = std::make_shared<FnType>(std::forward<Fn>(fn));

    QueueTask task;
    task.run = [promise, fn_ptr]() {
        try {
            promise->set_value((*fn_ptr)());
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    };
    task.cancel = [promise]() {
        try {
            promise->set_exception(std::make_exception_ptr(
                std::runtime_error("task cancelled")));
        } catch (...) {
        }
    };

    {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_not_full_cv_.wait(lock, [this] {
            return shutting_down_ || tasks_.size() < max_queue_size_;
        });
        if (shutting_down_) {
            task.cancel();
            return future;
        }
        tasks_.push(std::move(task));
    }
    queue_not_empty_cv_.notify_one();
    return future;
}

}  // namespace mooncake
