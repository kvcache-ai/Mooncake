#include "batch_io_scheduler.h"

#include <algorithm>
#include <condition_variable>
#include <cstring>
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
        SubBatch sub_batch;
        std::shared_ptr<Group> group;
    };
    tl::expected<void, ErrorCode> Init(size_t workers, Ready ready,
                                       std::chrono::milliseconds backoff,
                                       std::string_view worker_name_prefix) {
        ready_ = std::move(ready);
        backoff_ = backoff;
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
                        Work();
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
        if (tasks.empty()) return {};
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopping_) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        batch->remaining = tasks.size();
        queue_.splice(queue_.end(), tasks);
        changed_.notify_all();
        batch->done.wait(lock, [&] { return batch->remaining == 0; });
        if (batch->error) return tl::make_unexpected(*batch->error);
        return {};
    }

    void Shutdown() {
        std::lock_guard<std::mutex> shutdown_lock(shutdown_mutex_);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
            changed_.notify_all();
        }
        // Workers drain cancelled nodes without executing their I/O callbacks.
        // Group completion still runs, and is part of SubmitAndWait's wait.
        for (auto& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
    }

   private:
    void Work() {
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
            changed_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
            if (queue_.empty()) return;
            auto& front = queue_.front();
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
                if (!ready || stopping_ || queue_.empty()) continue;
            }
            auto task = std::move(queue_.front());
            queue_.pop_front();
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
                result = task.sub_batch.execute(task.sub_batch.slice_idx_begin,
                                                task.sub_batch.slice_idx_end);
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

    std::mutex mutex_;
    std::mutex shutdown_mutex_;
    std::condition_variable changed_;
    bool stopping_ = false;
    Ready ready_;
    std::chrono::milliseconds backoff_{300};
    std::list<Task> queue_;
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

tl::expected<void, ErrorCode> BatchIoScheduler::AddSubBatch(
    PendingBatch& batch, SubBatch&& sub_batch) {
    if (batch.error_) return tl::make_unexpected(*batch.error_);
    if (sub_batch.slice_idx_end < sub_batch.slice_idx_begin ||
        !sub_batch.execute) {
        batch.error_ = ErrorCode::INVALID_PARAMS;
    } else {
        try {
            if (!batch.state_)
                batch.state_ = std::make_unique<PendingBatch::State>();
            batch.state_->tasks.push_back(
                Impl::Task{batch.state_->batch, std::move(sub_batch), {}});
            return {};
        } catch (const std::bad_alloc&) {
            batch.error_ = ErrorCode::NO_AVAILABLE_HANDLE;
        } catch (...) {
            batch.error_ = ErrorCode::INTERNAL_ERROR;
        }
    }
    return tl::make_unexpected(*batch.error_);
}

tl::expected<void, ErrorCode> BatchIoScheduler::PrepareSubBatches(
    PendingBatch& batch, size_t slice_count, size_t sub_batch_size,
    const Execute& execute, const Complete& complete) {
    if (batch.error_) return tl::make_unexpected(*batch.error_);
    if (sub_batch_size < 4 || !execute) {
        batch.error_ = ErrorCode::INVALID_PARAMS;
        return tl::make_unexpected(*batch.error_);
    }
    try {
        auto group = complete ? std::make_shared<Impl::Group>() : nullptr;
        if (group) group->complete = complete;
        for (size_t begin = 0; begin < slice_count;) {
            size_t end = begin + std::min(sub_batch_size, slice_count - begin);
            // Merge the small tail before allocating the final task node.
            if (slice_count - end <= sub_batch_size / 4) end = slice_count;
            auto result = AddSubBatch(batch, SubBatch{begin, end, execute});
            if (!result) return result;
            if (group) {
                batch.state_->tasks.back().group = group;
                ++group->remaining;
            }
            begin = end;
        }
        return {};
    } catch (const std::bad_alloc&) {
        batch.error_ = ErrorCode::NO_AVAILABLE_HANDLE;
    } catch (...) {
        batch.error_ = ErrorCode::INTERNAL_ERROR;
    }
    return tl::make_unexpected(*batch.error_);
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
