#include "async_memcpy_executor.h"

#include <algorithm>

namespace mooncake {

// ============================================================================
// AsyncMemcpyExecutor
// ============================================================================

AsyncMemcpyExecutor::AsyncMemcpyExecutor(size_t worker_num) {
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
