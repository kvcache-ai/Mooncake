#include "batch_io_scheduler.h"

#include <algorithm>
#include <array>
#include <cstdlib>
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
#include <xxhash.h>
#ifdef USE_URING
#include <liburing.h>
#include "uring_submit.h"
#endif
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
    struct WriteFile {
        PrepareWriteFile prepare;
        size_t queue = 0;
        bool queued = false;
        bool initialized = false;
    };
    struct Task {
        std::shared_ptr<Batch> batch;
        SubBatch sub_batch;
        std::shared_ptr<Group> group;
        std::shared_ptr<WriteFile> write_file;
    };
    tl::expected<void, ErrorCode> Init(size_t workers, Ready ready,
                                       std::chrono::milliseconds backoff,
                                       std::string_view worker_name_prefix) {
        ready_ = std::move(ready);
        backoff_ = backoff;
        try {
            workers_.resize(workers);
            write_queues_.resize(workers);
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
        if (tasks.empty()) return {};
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopping_) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        batch->remaining = tasks.size();
        while (!tasks.empty()) {
            auto task = tasks.begin();
            if (task->write_file) {
                auto& file = *task->write_file;
                if (!file.queued) {
                    file.queue = next_write_queue_++ % workers_.size();
                    file.queued = true;
                }
                write_queues_[file.queue].splice(
                    write_queues_[file.queue].end(), tasks, task);
            } else {
                queue_.splice(queue_.end(), tasks, task);
            }
        }
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
    void FinishTask(Task& task, tl::expected<void, ErrorCode> result,
                    std::unique_lock<std::mutex>& lock) {
        if (!result) {
            if (!task.batch->error) task.batch->error = result.error();
            if (task.group && !task.group->error)
                task.group->error = result.error();
        }
        if (task.group && --task.group->remaining == 0) {
            auto group_result =
                task.group->error ? tl::expected<void, ErrorCode>(
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

#ifdef USE_URING
    // Keep this ring private to the worker and separate from synchronous
    // UringFile calls (such as a completed bucket's datasync).
    struct WriteRing {
        static constexpr size_t kDepth = 32;

        struct Slot {
            std::optional<Task> task;
            std::optional<WriteRequest> request;
            std::unique_ptr<char, decltype(&std::free)> scratch{nullptr,
                                                                &std::free};
        };

        ~WriteRing() { Reset(); }

        bool Init() {
            if (initialized) return true;
            const int ret = io_uring_queue_init(kDepth, &ring, 0);
            if (ret < 0) {
                LOG(ERROR) << "Failed to initialize async write ring: "
                           << strerror(-ret);
                return false;
            }
            initialized = true;
            return true;
        }

        void Reset() {
            if (initialized) io_uring_queue_exit(&ring);
            initialized = false;
        }

        size_t FreeSlot() const {
            for (size_t i = 0; i < slots.size(); ++i) {
                if (!slots[i].task) return i;
            }
            return slots.size();
        }

        std::span<char> Buffer(Slot& slot) {
            if (!slot.scratch) {
                void* buffer = nullptr;
                if (posix_memalign(&buffer, 4096, kWriteSqeBytes) != 0)
                    return {};
                slot.scratch.reset(static_cast<char*>(buffer));
            }
            return {slot.scratch.get(), kWriteSqeBytes};
        }

        int Wait(io_uring_cqe** cqe) {
            unsigned retries = 0;
            int ret;
            do {
                ret = io_uring_wait_cqe(&ring, cqe);
            } while ((ret == -EINTR || ret == -EAGAIN) && retries++ < 64);
            return ret;
        }

        io_uring ring{};
        std::array<Slot, kDepth> slots;
        // One extra task may prepare while every SQE is occupied. Its slot has
        // a stable address, so submitting it later needs no task/buffer move.
        Slot prepared;
        bool prepared_pending = false;
        size_t active = 0;
        bool initialized = false;
    };

    void RetireWrite(WriteRing& writer, io_uring_cqe* cqe,
                     std::unique_lock<std::mutex>& lock) {
        auto& slot = *static_cast<WriteRing::Slot*>(io_uring_cqe_get_data(cqe));
        const int status = cqe->res;
        io_uring_cqe_seen(&writer.ring, cqe);
        tl::expected<void, ErrorCode> result;
        if (status < 0 || static_cast<size_t>(status) != slot.request->length) {
            LOG(ERROR) << "Async bucket write failed: result=" << status
                       << " length=" << slot.request->length
                       << " offset=" << slot.request->offset;
            if (status < 0 && slot.request->report_error) {
                try {
                    slot.request->report_error(-status);
                } catch (...) {
                    LOG(ERROR) << "Async write error handler failed";
                }
            }
            result = tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        slot.request.reset();
        --writer.active;
        lock.lock();
        FinishTask(*slot.task, result, lock);
        slot.task.reset();
        lock.unlock();
    }

    void ReapWrite(WriteRing& writer, std::unique_lock<std::mutex>& lock) {
        lock.unlock();
        const auto submit = detail::submit_all_pending(
            [&] { return io_uring_sq_ready(&writer.ring); },
            [&](unsigned) { return io_uring_submit(&writer.ring); },
            [] { std::this_thread::yield(); });
        io_uring_cqe* cqe = nullptr;
        if (submit.error == 0 && writer.Wait(&cqe) == 0) {
            RetireWrite(writer, cqe, lock);
            lock.lock();
            return;
        }

        LOG(ERROR) << "Async write ring submission or completion failed: "
                   << "pending=" << submit.pending << " error=" << submit.error;
        // Before releasing any buffers, collect the requests that the kernel
        // accepted. Requests still in the SQ are failed with the ring reset.
        size_t submitted =
            writer.active - std::min<size_t>(writer.active, submit.pending);
        while (submitted != 0 && writer.Wait(&cqe) == 0) {
            RetireWrite(writer, cqe, lock);
            --submitted;
        }
        writer.Reset();
        writer.active = 0;
        lock.lock();
        for (auto& slot : writer.slots) {
            if (!slot.task) continue;
            slot.request.reset();
            FinishTask(*slot.task,
                       tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL), lock);
            slot.task.reset();
        }
        if (writer.prepared.task) {
            writer.prepared.request.reset();
            FinishTask(*writer.prepared.task,
                       tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL), lock);
            writer.prepared.task.reset();
        }
        writer.prepared_pending = false;
    }

    // Called without the scheduler lock. SQE ownership transfers only on
    // success, so synchronous work and preparation errors use FinishTask.
    tl::expected<void, ErrorCode> QueueWrite(WriteRing& writer, Task& task,
                                             bool& queued) {
        if (!writer.Init())
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        auto* slot = writer.active == WriteRing::kDepth
                         ? &writer.prepared
                         : &writer.slots[writer.FreeSlot()];
        Scratch scratch = [&writer, slot] { return writer.Buffer(*slot); };
        auto prepared = task.sub_batch.prepare_write(
            task.sub_batch.slice_idx_begin, scratch);
        if (!prepared) return tl::make_unexpected(prepared.error());
        if (!*prepared) return {};

        auto& request = **prepared;
        if (writer.active == WriteRing::kDepth) {
            slot->request.emplace(std::move(request));
            slot->task.emplace(std::move(task));
            writer.prepared_pending = true;
            queued = true;
            return {};
        }

        auto* sqe = io_uring_get_sqe(&writer.ring);
        if (!sqe) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        io_uring_prep_write(sqe, request.fd, request.data, request.length,
                            request.offset);
        io_uring_sqe_set_data(sqe, slot);
        slot->request.emplace(std::move(request));
        slot->task.emplace(std::move(task));
        ++writer.active;
        queued = true;
        return {};
    }

    tl::expected<void, ErrorCode> SubmitPrepared(WriteRing& writer) {
        auto& slot = writer.prepared;
        auto* sqe = io_uring_get_sqe(&writer.ring);
        if (!sqe) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        const auto& request = *slot.request;
        io_uring_prep_write(sqe, request.fd, request.data, request.length,
                            request.offset);
        io_uring_sqe_set_data(sqe, &slot);
        ++writer.active;
        writer.prepared_pending = false;
        return {};
    }
#endif

    void Work(size_t worker_index) {
#ifdef USE_URING
        WriteRing writer;
#endif
        auto& write_queue = write_queues_[worker_index];
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
#ifdef USE_URING
            if (writer.prepared_pending) {
                auto& task = *writer.prepared.task;
                tl::expected<void, ErrorCode> cancelled;
                if (stopping_) {
                    cancelled = tl::make_unexpected(
                        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                } else if (task.group && task.group->error) {
                    cancelled = tl::make_unexpected(*task.group->error);
                } else if (!task.group && task.batch->error) {
                    cancelled = tl::make_unexpected(*task.batch->error);
                }
                if (!cancelled) {
                    FinishTask(task, cancelled, lock);
                    writer.prepared = {};
                    writer.prepared_pending = false;
                    continue;
                }
                if (writer.active < WriteRing::kDepth) {
                    auto submitted = SubmitPrepared(writer);
                    if (!submitted) {
                        FinishTask(task, submitted, lock);
                        writer.prepared = {};
                        writer.prepared_pending = false;
                    }
                    continue;
                }
            }
            const bool has_work = !queue_.empty() || !write_queue.empty();
            if (writer.active != 0 &&
                ((writer.active == WriteRing::kDepth && writer.prepared.task) ||
                 !has_work || stopping_)) {
                // Refill on each completion. With a full ring and queued work,
                // prepare one task first so its copy overlaps in-flight I/O.
                ReapWrite(writer, lock);
                continue;
            }
            if (writer.active == WriteRing::kDepth && !writer.prepared.task &&
                has_work) {
                // Do not delay an already-arrived CQE behind the next copy.
                lock.unlock();
                io_uring_cqe* cqe = nullptr;
                if (io_uring_peek_cqe(&writer.ring, &cqe) == 0) {
                    RetireWrite(writer, cqe, lock);
                    lock.lock();
                    continue;
                }
                lock.lock();
            }
#endif
            changed_.wait(lock, [&] {
                return stopping_ || !queue_.empty() || !write_queue.empty();
            });
            if (queue_.empty() && write_queue.empty()) return;
            bool from_write_queue = !write_queue.empty();
            auto* front =
                from_write_queue ? &write_queue.front() : &queue_.front();
            bool cancelled =
                stopping_ || (front->group ? front->group->error.has_value()
                                           : front->batch->error.has_value());
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
#ifdef USE_URING
                if (!ready && writer.active != 0) {
                    lock.lock();
                    ReapWrite(writer, lock);
                    continue;
                }
#endif
                if (!ready) std::this_thread::sleep_for(backoff_);
                lock.lock();
                // Queue/state may have changed while checking readiness.
                if (!ready || stopping_ ||
                    (queue_.empty() && write_queue.empty()))
                    continue;
                from_write_queue = !write_queue.empty();
                front =
                    from_write_queue ? &write_queue.front() : &queue_.front();
                cancelled = front->group ? front->group->error.has_value()
                                         : front->batch->error.has_value();
            }
            if (!cancelled && front->write_file &&
                !front->write_file->initialized) {
                auto file = front->write_file;
                lock.unlock();
                auto inode = tl::expected<uint64_t, ErrorCode>(
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
                try {
                    inode = file->prepare();
                } catch (...) {
                }
                lock.lock();
                if (!inode) {
                    auto task = std::move(*front);
                    write_queue.pop_front();
                    FinishTask(task, tl::make_unexpected(inode.error()), lock);
                    continue;
                }
                if (!stopping_) {
                    file->initialized = true;
                    const uint64_t value = *inode;
                    file->queue =
                        XXH3_64bits(&value, sizeof(value)) % workers_.size();
                    if (file->queue != worker_index) {
                        auto& target = write_queues_[file->queue];
                        for (auto it = write_queue.begin();
                             it != write_queue.end();) {
                            if (it->write_file == file) {
                                auto current = it++;
                                target.splice(target.end(), write_queue,
                                              current);
                            } else {
                                ++it;
                            }
                        }
                        changed_.notify_all();
                        continue;
                    }
                }
                continue;
            }
            auto task = std::move(*front);
            if (from_write_queue) {
                write_queue.pop_front();
            } else {
                queue_.pop_front();
            }
            tl::expected<void, ErrorCode> result;
            bool queued = false;
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
#ifdef USE_URING
                    if (task.sub_batch.prepare_write) {
                        result = QueueWrite(writer, task, queued);
                    } else
#endif
                    {
                        result = task.sub_batch.execute(
                            task.sub_batch.slice_idx_begin,
                            task.sub_batch.slice_idx_end);
                    }
                } catch (...) {
                    result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                lock.lock();
            }
            if (!queued) FinishTask(task, result, lock);
        }
    }

    std::mutex mutex_;
    std::mutex shutdown_mutex_;
    std::condition_variable changed_;
    bool stopping_ = false;
    Ready ready_;
    std::chrono::milliseconds backoff_{300};
    std::list<Task> queue_;
    std::vector<std::list<Task>> write_queues_;
    size_t next_write_queue_ = 0;
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
        (!sub_batch.execute && !sub_batch.prepare_write)) {
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

tl::expected<void, ErrorCode> BatchIoScheduler::PrepareSubBatchesByBytes(
    PendingBatch& batch, std::span<const size_t> slice_sizes,
    size_t sub_batch_bytes, const Execute& execute, const Complete& complete) {
    if (batch.error_) return tl::make_unexpected(*batch.error_);
    if (sub_batch_bytes == 0 || !execute) {
        batch.error_ = ErrorCode::INVALID_PARAMS;
        return tl::make_unexpected(*batch.error_);
    }
    try {
        struct Group {
            size_t begin;
            size_t end;
            size_t bytes;
        };
        std::vector<Group> groups;
        Group current{0, 0, 0};
        for (size_t index = 0; index < slice_sizes.size(); ++index) {
            if (index > current.begin &&
                current.bytes + slice_sizes[index] > sub_batch_bytes) {
                groups.push_back(current);
                current = Group{index, index, 0};
            }
            current.end = index + 1;
            current.bytes += slice_sizes[index];
        }
        if (current.begin < current.end) groups.push_back(current);
        if (groups.size() > 1) {
            Group& previous = groups[groups.size() - 2];
            const Group tail = groups.back();
            if (tail.bytes <= sub_batch_bytes / 4) {
                previous.end = tail.end;
                groups.pop_back();
            }
        }
        auto group = complete ? std::make_shared<Impl::Group>() : nullptr;
        if (group) group->complete = complete;
        for (const auto& item : groups) {
            auto result =
                AddSubBatch(batch, SubBatch{item.begin, item.end, execute});
            if (!result) return result;
            if (group) {
                batch.state_->tasks.back().group = group;
                ++group->remaining;
            }
        }
        return {};
    } catch (const std::bad_alloc&) {
        batch.error_ = ErrorCode::NO_AVAILABLE_HANDLE;
    } catch (...) {
        batch.error_ = ErrorCode::INTERNAL_ERROR;
    }
    return tl::make_unexpected(*batch.error_);
}

tl::expected<void, ErrorCode> BatchIoScheduler::PrepareSubBatchTasks(
    PendingBatch& batch, size_t task_count, const Execute& execute,
    const Complete& complete) {
    return PrepareIndexedTasks(batch, task_count, execute, {}, {}, complete);
}

tl::expected<void, ErrorCode> BatchIoScheduler::PrepareWriteTasks(
    PendingBatch& batch, size_t task_count,
    const PrepareWriteFile& prepare_file, const PrepareWrite& prepare,
    const Complete& complete) {
#ifdef USE_URING
    return PrepareIndexedTasks(batch, task_count, {}, prepare_file, prepare,
                               complete);
#else
    batch.error_ = ErrorCode::INVALID_PARAMS;
    return tl::make_unexpected(*batch.error_);
#endif
}

tl::expected<void, ErrorCode> BatchIoScheduler::PrepareIndexedTasks(
    PendingBatch& batch, size_t task_count, const Execute& execute,
    const PrepareWriteFile& prepare_file, const PrepareWrite& prepare_write,
    const Complete& complete) {
    if (batch.error_) return tl::make_unexpected(*batch.error_);
    if ((!execute && !prepare_write) || (prepare_write && !prepare_file)) {
        batch.error_ = ErrorCode::INVALID_PARAMS;
        return tl::make_unexpected(*batch.error_);
    }
    try {
        auto group = complete ? std::make_shared<Impl::Group>() : nullptr;
        if (group) group->complete = complete;
        auto write_file =
            prepare_write ? std::make_shared<Impl::WriteFile>() : nullptr;
        if (write_file) write_file->prepare = prepare_file;
        for (size_t index = 0; index < task_count; ++index) {
            auto result = AddSubBatch(
                batch, SubBatch{index, index + 1, execute, prepare_write});
            if (!result) return result;
            if (group) {
                batch.state_->tasks.back().group = group;
                ++group->remaining;
            }
            batch.state_->tasks.back().write_file = write_file;
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
