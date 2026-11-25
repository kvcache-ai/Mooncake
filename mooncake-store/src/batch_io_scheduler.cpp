#include "batch_io_scheduler.h"

#include <condition_variable>
#include <cstring>
#include <deque>
#include <list>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>
#if defined(__linux__) || defined(__APPLE__)
#include <pthread.h>
#endif
#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace mooncake {

class BatchIoScheduler::Impl {
   public:
    struct Batch {
        std::optional<ErrorCode> error;
        size_t remaining = 0;
        std::condition_variable done;
    };
    struct Group {
        std::optional<ErrorCode> error;
        size_t remaining = 0;
        Complete complete;
    };
    struct Task {
        std::shared_ptr<Batch> batch;
        size_t index = 0;
        size_t queue = 0;
        Execute execute;
        std::shared_ptr<Group> group;
    };
    // One queue per worker. A worker claims tasks only from its own queue;
    // affinity dispatch keeps one source's tasks on one worker.
    struct Slot {
        std::deque<Task> tasks;
        std::condition_variable ready;
    };
    tl::expected<void, ErrorCode> Init(size_t workers, Ready ready,
                                       std::chrono::milliseconds backoff,
                                       std::string_view worker_name_prefix) {
        ready_ = std::move(ready);
        backoff_ = backoff;
        slots_.resize(workers);
        for (auto& slot : slots_) slot = std::make_unique<Slot>();
        try {
            workers_.resize(workers);
            for (size_t i = 0; i < workers; ++i) {
                auto name = std::string(worker_name_prefix) + std::to_string(i);
                // Linux thread names include a terminating NUL in 16 bytes.
                if (name.size() > 15) name.resize(15);
                workers_[i] =
                    std::thread([this, i, workers, name = std::move(name)] {
#if defined(__linux__)
                        const int error =
                            pthread_setname_np(pthread_self(), name.c_str());
                        const auto tid = ::syscall(SYS_gettid);
#elif defined(__APPLE__)
                        const int error = pthread_setname_np(name.c_str());
                        const auto tid = std::this_thread::get_id();
#else
                        const int error = 0;
                        const auto tid = std::this_thread::get_id();
#endif
                        if (error != 0) {
                            LOG(WARNING)
                                << "Failed to name I/O worker: name=" << name
                                << " error=" << std::strerror(error);
                        }
                        LOG(INFO) << "I/O worker started: name=" << name
                                  << " worker_index=" << i
                                  << " workers=" << workers << " tid=" << tid;
                        Work(i);
                    });
            }
        } catch (...) {
            Shutdown();
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return {};
    }

    ~Impl() { Shutdown(); }

    tl::expected<void, ErrorCode> SubmitAndWait(
        const std::shared_ptr<Batch>& batch, std::list<Task>& tasks) {
        std::unique_lock<std::mutex> lock(mutex_);
        // A latched preparation error poisons the batch even when no task was
        // appended; report it instead of treating the batch as empty.
        if (batch->error) return tl::make_unexpected(*batch->error);
        if (tasks.empty()) return {};
        if (stopping_) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        batch->remaining = tasks.size();
        for (auto& task : tasks) {
            auto& slot = *slots_[task.queue];
            slot.tasks.push_back(std::move(task));
            slot.ready.notify_all();
        }
        batch->done.wait(lock, [&] { return batch->remaining == 0; });
        if (batch->error) return tl::make_unexpected(*batch->error);
        return {};
    }

    void Shutdown() {
        std::lock_guard<std::mutex> shutdown_lock(shutdown_mutex_);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
        }
        for (auto& slot : slots_) {
            std::lock_guard<std::mutex> lock(mutex_);
            slot->ready.notify_all();
        }
        // Workers drain cancelled nodes without executing their I/O callbacks.
        // Group completion still runs, and is part of SubmitAndWait's wait.
        for (auto& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
    }

   private:
    void Work(size_t worker_index) {
        auto& slot = *slots_[worker_index];
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
            slot.ready.wait(lock,
                            [&] { return stopping_ || !slot.tasks.empty(); });
            if (slot.tasks.empty()) return;
            // Peek the front task and pass the readiness gate before claiming
            // it; a deferred task stays queued and is retried after backoff.
            auto& front = slot.tasks.front();
            const bool cancelled =
                stopping_ || (front.group ? front.group->error.has_value()
                                          : front.batch->error.has_value());
            bool readiness_failed = false;
            if (!cancelled && ready_) {
                lock.unlock();
                bool ready = false;
                try {
                    ready = ready_();
                } catch (...) {
                    readiness_failed = true;
                    ready = true;
                }
                if (!ready) std::this_thread::sleep_for(backoff_);
                lock.lock();
                // Queue/state may have changed while checking readiness.
                if (!ready || stopping_ || slot.tasks.empty()) continue;
            }
            auto task = std::move(slot.tasks.front());
            slot.tasks.pop_front();
            tl::expected<void, ErrorCode> result;
            if (readiness_failed) {
                result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            } else if (stopping_) {
                result = tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            } else if (task.group && task.group->error) {
                result = tl::make_unexpected(*task.group->error);
            } else if (!task.group && task.batch->error) {
                result = tl::make_unexpected(*task.batch->error);
            } else {
                lock.unlock();
                try {
                    result = task.execute(task.index);
                } catch (...) {
                    result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                lock.lock();
            }
            if (!result) {
                if (!task.batch->error) task.batch->error = result.error();
                if (task.group && !task.group->error)
                    task.group->error = result.error();
            }
            if (task.group && --task.group->remaining == 0) {
                auto group_result =
                    task.group->error
                        ? tl::expected<void, ErrorCode>(
                              tl::make_unexpected(*task.group->error))
                        : tl::expected<void, ErrorCode>();
                lock.unlock();
                try {
                    result = task.group->complete(group_result);
                } catch (...) {
                    result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                lock.lock();
                if (!result && !task.batch->error)
                    task.batch->error = result.error();
            }
            auto batch = task.batch;
            // Callback owners may release staging or file reservations. Destroy
            // them outside the queue lock and before waking the caller.
            lock.unlock();
            task = {};
            lock.lock();
            if (--batch->remaining == 0) batch->done.notify_all();
        }
    }

   public:
    tl::expected<void, ErrorCode> PrepareTasks(
        const std::shared_ptr<Batch>& batch, std::list<Task>& tasks,
        size_t task_count, uint64_t affinity_key, const Execute& execute,
        const Complete& complete) {
        if (!execute) {
            batch->error = ErrorCode::INVALID_PARAMS;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        // A latched preparation error poisons the whole batch until
        // SubmitAndWait consumes it.
        if (batch->error) return tl::make_unexpected(*batch->error);
        const size_t queue = affinity_key % slots_.size();
        try {
            auto group = complete ? std::make_shared<Group>() : nullptr;
            if (group) group->complete = complete;
            for (size_t index = 0; index < task_count; ++index) {
                try {
                    tasks.push_back(Task{batch, index, queue, execute, group});
                } catch (const std::bad_alloc&) {
                    batch->error = ErrorCode::NO_AVAILABLE_HANDLE;
                    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
                } catch (...) {
                    batch->error = ErrorCode::INTERNAL_ERROR;
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                if (group) ++group->remaining;
            }
            return {};
        } catch (...) {
            batch->error = ErrorCode::INTERNAL_ERROR;
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    std::mutex mutex_;
    std::mutex shutdown_mutex_;
    std::vector<std::unique_ptr<Slot>> slots_;
    bool stopping_ = false;
    Ready ready_;
    std::chrono::milliseconds backoff_{300};
    std::vector<std::thread> workers_;
};

tl::expected<std::unique_ptr<BatchIoScheduler>, ErrorCode>
BatchIoScheduler::Create(size_t workers, Ready ready,
                         std::chrono::milliseconds backoff,
                         std::string_view worker_name_prefix) {
    if (workers == 0 || backoff.count() <= 0)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    try {
        auto impl = std::make_unique<Impl>();
        auto result =
            impl->Init(workers, std::move(ready), backoff, worker_name_prefix);
        if (!result) return tl::make_unexpected(result.error());
        return std::unique_ptr<BatchIoScheduler>(
            new BatchIoScheduler(std::move(impl)));
    } catch (...) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

BatchIoScheduler::BatchIoScheduler(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
BatchIoScheduler::~BatchIoScheduler() = default;

struct BatchIoScheduler::PendingBatch::State {
    std::shared_ptr<Impl::Batch> batch = std::make_shared<Impl::Batch>();
    std::list<Impl::Task> tasks;
};

BatchIoScheduler::PendingBatch::PendingBatch() = default;
BatchIoScheduler::PendingBatch::~PendingBatch() = default;

tl::expected<void, ErrorCode> BatchIoScheduler::PrepareTasks(
    PendingBatch& batch, size_t task_count, uint64_t affinity_key,
    const Execute& execute, const Complete& complete) {
    if (!batch.state_) batch.state_ = std::make_unique<PendingBatch::State>();
    return impl_->PrepareTasks(batch.state_->batch, batch.state_->tasks,
                               task_count, affinity_key, execute, complete);
}

tl::expected<void, ErrorCode> BatchIoScheduler::SubmitAndWait(
    PendingBatch& batch) {
    auto state = std::move(batch.state_);
    auto error = std::exchange(batch.error_, std::nullopt);
    if (error) return tl::make_unexpected(*error);
    if (!state) return {};
    return impl_->SubmitAndWait(state->batch, state->tasks);
}

void BatchIoScheduler::Shutdown() { impl_->Shutdown(); }

}  // namespace mooncake
