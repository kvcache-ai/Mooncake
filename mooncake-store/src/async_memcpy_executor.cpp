#include "async_memcpy_executor.h"

#include <algorithm>
#include <cstring>

namespace mooncake {

// ============================================================================
// ExecuteLocalCopyPlan
// ============================================================================

ErrorCode ExecuteLocalCopyPlan(const LocalCopyPlan& plan) {
    if (plan.use_single_dest) {
        if (!plan.single_dest_ptr) {
            LOG(ERROR) << "Local copy destination buffer is null";
            return ErrorCode::INVALID_PARAMS;
        }
        if (plan.single_dest_size < plan.source_size) {
            LOG(ERROR) << "Local copy destination is too small, required="
                       << plan.source_size
                       << ", provided=" << plan.single_dest_size;
            return ErrorCode::INVALID_PARAMS;
        }
        if (plan.source_size > 0) {
            std::memcpy(plan.single_dest_ptr, plan.source_ptr,
                        plan.source_size);
        }
        return ErrorCode::OK;
    }

    size_t offset = 0;
    for (const auto& slice : plan.dest_slices) {
        if (offset >= plan.source_size) break;
        const size_t copy_size =
            std::min(slice.size, plan.source_size - offset);
        if (copy_size == 0) continue;
        if (!slice.ptr) {
            LOG(ERROR) << "Local copy destination buffer is null";
            return ErrorCode::INVALID_PARAMS;
        }
        std::memcpy(slice.ptr, plan.source_ptr + offset, copy_size);
        offset += copy_size;
    }

    if (offset != plan.source_size) {
        LOG(ERROR) << "Local copy did not complete, copied=" << offset
                   << ", source_size=" << plan.source_size;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

// ============================================================================
// AsyncMemcpyExecutor
// ============================================================================

AsyncMemcpyExecutor::AsyncMemcpyExecutor(size_t worker_num,
                                         size_t max_queue_size)
    : max_queue_size_(std::max<size_t>(1, max_queue_size)) {
    workers_.reserve(std::max<size_t>(1, worker_num));
    for (size_t i = 0; i < std::max<size_t>(1, worker_num); ++i) {
        workers_.emplace_back(&AsyncMemcpyExecutor::WorkerMain, this);
    }
}

AsyncMemcpyExecutor::~AsyncMemcpyExecutor() { Shutdown(); }

void AsyncMemcpyExecutor::Shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutting_down_) {
            return;
        }
        shutting_down_ = true;
    }
    queue_not_empty_cv_.notify_all();
    queue_not_full_cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();

    std::queue<QueueTask> pending;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending.swap(tasks_);
    }
    while (!pending.empty()) {
        auto task = std::move(pending.front());
        if (task.cancel) {
            task.cancel();
        }
        pending.pop();
    }
}

void AsyncMemcpyExecutor::WorkerMain() {
    while (true) {
        QueueTask task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            queue_not_empty_cv_.wait(
                lock, [this] { return shutting_down_ || !tasks_.empty(); });
            if (shutting_down_ && tasks_.empty()) {
                return;
            }

            task = std::move(tasks_.front());
            tasks_.pop();
            queue_not_full_cv_.notify_one();
        }

        try {
            if (task.run) {
                task.run();
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "Async worker task threw exception: " << e.what();
            if (task.cancel) {
                task.cancel();
            }
        } catch (...) {
            LOG(ERROR) << "Async worker task threw unknown exception";
            if (task.cancel) {
                task.cancel();
            }
        }
    }
}

}  // namespace mooncake
