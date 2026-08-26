// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_workers.h"

#include <algorithm>
#include <exception>
#include <utility>

#include <glog/logging.h>

namespace mooncake::tent {

Status HighPerformanceTcpAdmissionController::tryReserve(uint64_t tasks,
                                                         uint64_t bytes) {
    if (tasks == 0 || bytes == 0) {
        return Status::InvalidArgument(
            "HP TCP admission reservation must be non-zero" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (!accepting_) {
        return Status::TooManyRequests("HP TCP admission is closed" LOC_MARK);
    }
    if (tasks > max_tasks_ || bytes > max_bytes_ ||
        tasks_ > max_tasks_ - tasks || bytes_ > max_bytes_ - bytes) {
        return Status::TooManyRequests(
            "HP TCP admission limit exceeded" LOC_MARK);
    }
    tasks_ += tasks;
    bytes_ += bytes;
    return Status::OK();
}

void HighPerformanceTcpAdmissionController::release(uint64_t tasks,
                                                    uint64_t bytes) {
    bool zero = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (tasks > tasks_ || bytes > bytes_) {
            LOG(ERROR) << "HP TCP admission release underflow: release tasks="
                       << tasks << " bytes=" << bytes
                       << " outstanding tasks=" << tasks_
                       << " bytes=" << bytes_;
            tasks_ = 0;
            bytes_ = 0;
        } else {
            tasks_ -= tasks;
            bytes_ -= bytes;
        }
        zero = tasks_ == 0 && bytes_ == 0;
    }
    if (zero) zero_cv_.notify_all();
}

void HighPerformanceTcpAdmissionController::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    accepting_ = false;
}

void HighPerformanceTcpAdmissionController::reopenForTest() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (tasks_ == 0 && bytes_ == 0) accepting_ = true;
}

void HighPerformanceTcpAdmissionController::waitForZero() {
    std::unique_lock<std::mutex> lock(mutex_);
    zero_cv_.wait(lock, [&] { return tasks_ == 0 && bytes_ == 0; });
}

uint64_t HighPerformanceTcpAdmissionController::outstandingTasks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_;
}

uint64_t HighPerformanceTcpAdmissionController::outstandingBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return bytes_;
}

bool HighPerformanceTcpAdmissionController::accepting() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return accepting_;
}

HighPerformanceTcpWorkers::HighPerformanceTcpWorkers()
    : HighPerformanceTcpWorkers(Config{}) {}

HighPerformanceTcpWorkers::HighPerformanceTcpWorkers(Config config)
    : config_(config) {}

HighPerformanceTcpWorkers::~HighPerformanceTcpWorkers() { (void)stop(); }

Status HighPerformanceTcpWorkers::start() {
    std::unique_lock<std::mutex> lock(lifecycle_mutex_);
    const State observed = state_.load(std::memory_order_acquire);
    if (observed == State::kRunning) return Status::OK();
    if (observed != State::kCreated) {
        return Status::InvalidArgument(
            "HP TCP workers cannot be restarted" LOC_MARK);
    }
    if (config_.worker_count == 0 || config_.queue_capacity == 0) {
        return Status::InvalidArgument(
            "HP TCP worker count and queue capacity must be positive" LOC_MARK);
    }

    state_.store(State::kStarting, std::memory_order_release);
    ready_workers_ = 0;
    try {
        workers_.reserve(config_.worker_count);
        for (size_t i = 0; i < config_.worker_count; ++i) {
            workers_.push_back(
                std::make_unique<WorkerContext>(config_.queue_capacity));
        }
        for (size_t i = 0; i < workers_.size(); ++i) {
            workers_[i]->thread = std::thread([this, i] {
                {
                    std::lock_guard<std::mutex> ready_lock(lifecycle_mutex_);
                    ++ready_workers_;
                }
                lifecycle_cv_.notify_all();
                try {
                    workers_[i]->io.run();
                } catch (const std::exception& error) {
                    LOG(ERROR)
                        << "HP TCP worker " << i
                        << " io_context escaped exception: " << error.what();
                    state_.store(State::kFailed, std::memory_order_release);
                } catch (...) {
                    LOG(ERROR) << "HP TCP worker " << i
                               << " io_context escaped exception";
                    state_.store(State::kFailed, std::memory_order_release);
                }
            });
        }
    } catch (const std::exception& error) {
        state_.store(State::kFailed, std::memory_order_release);
        for (auto& worker : workers_) {
            worker->guard.reset();
            worker->io.stop();
        }
        lock.unlock();
        for (auto& worker : workers_) {
            if (worker->thread.joinable()) worker->thread.join();
        }
        return Status::InternalError(
            std::string("failed to start HP TCP workers: ") + error.what() +
            LOC_MARK);
    }

    lifecycle_cv_.wait(lock, [&] { return ready_workers_ == workers_.size(); });
    if (state_.load(std::memory_order_acquire) == State::kFailed) {
        lock.unlock();
        (void)stop();
        return Status::InternalError(
            "HP TCP worker failed while starting" LOC_MARK);
    }
    state_.store(State::kRunning, std::memory_order_release);
    return Status::OK();
}

bool HighPerformanceTcpWorkers::onWorkerThread() const {
    const auto current = std::this_thread::get_id();
    for (const auto& worker : workers_) {
        if (worker->thread.joinable() && worker->thread.get_id() == current) {
            return true;
        }
    }
    return false;
}

void HighPerformanceTcpWorkers::requestStop() {
    State expected = State::kRunning;
    (void)state_.compare_exchange_strong(expected, State::kStopping,
                                         std::memory_order_acq_rel);
}

void HighPerformanceTcpWorkers::ringPushLocked(WorkerContext& worker,
                                               Command&& command) {
    // Caller proved mailbox_size < fixed capacity. optional<Command> move
    // construction is non-allocating; std::function move is noexcept.
    worker.mailbox[worker.mailbox_tail].emplace(std::move(command));
    worker.mailbox_tail = (worker.mailbox_tail + 1) % worker.mailbox.size();
    ++worker.mailbox_size;
}

HighPerformanceTcpWorkers::Command HighPerformanceTcpWorkers::ringPopLocked(
    WorkerContext& worker) {
    Command command = std::move(*worker.mailbox[worker.mailbox_head]);
    worker.mailbox[worker.mailbox_head].reset();
    worker.mailbox_head = (worker.mailbox_head + 1) % worker.mailbox.size();
    --worker.mailbox_size;
    return command;
}

std::vector<HighPerformanceTcpWorkers::Command>
HighPerformanceTcpWorkers::extractPendingLocked() {
    std::vector<Command> pending;
    size_t total = 0;
    for (const auto& worker : workers_) {
        std::lock_guard<std::mutex> mailbox_lock(worker->mailbox_mutex);
        total += worker->mailbox_size;
    }
    pending.reserve(total);
    for (auto& worker : workers_) {
        std::lock_guard<std::mutex> mailbox_lock(worker->mailbox_mutex);
        while (worker->mailbox_size != 0) {
            pending.push_back(ringPopLocked(*worker));
        }
        worker->drain_posted = false;
    }
    return pending;
}

void HighPerformanceTcpWorkers::cancelPending() {
    auto pending = extractPendingLocked();
    for (auto& command : pending) {
        if (!command.cancel) continue;
        try {
            command.cancel();
        } catch (const std::exception& error) {
            LOG(ERROR) << "HP TCP pending-command cancellation threw: "
                       << error.what();
        } catch (...) {
            LOG(ERROR) << "HP TCP pending-command cancellation threw";
        }
    }
}

Status HighPerformanceTcpWorkers::stop() {
    std::unique_lock<std::mutex> lock(lifecycle_mutex_);
    const State observed = state_.load(std::memory_order_acquire);
    if (observed == State::kCreated || observed == State::kStopped) {
        return Status::OK();
    }
    if (onWorkerThread()) {
        return Status::InvalidArgument(
            "HP TCP worker cannot synchronously stop itself" LOC_MARK);
    }
    if (joining_) {
        lifecycle_cv_.wait(lock, [&] { return !joining_; });
        return Status::OK();
    }

    joining_ = true;
    state_.store(State::kStopping, std::memory_order_release);
    lock.unlock();

    // User work in the bounded mailboxes is explicitly settled before the
    // contexts are stopped. The transport is responsible for canceling and
    // quiescing already-started socket operations before calling stop().
    cancelPending();
    for (auto& worker : workers_) worker->guard.reset();
    for (auto& worker : workers_) worker->io.stop();
    for (auto& worker : workers_) {
        if (worker->thread.joinable()) worker->thread.join();
    }

    lock.lock();
    state_.store(State::kStopped, std::memory_order_release);
    joining_ = false;
    lock.unlock();
    lifecycle_cv_.notify_all();
    return Status::OK();
}

size_t HighPerformanceTcpWorkers::affinityOwner(const AffinityKey& key) const {
    size_t hash = std::hash<uint64_t>{}(key.peer);
    const auto mix = [&hash](size_t value) {
        hash ^= value + static_cast<size_t>(0x9e3779b97f4a7c15ULL) +
                (hash << 6U) + (hash >> 2U);
    };
    mix(std::hash<uint32_t>{}(key.endpoint));
    mix(std::hash<uint32_t>{}(key.lane));
    // Deliberately exclude incarnation: the same peer/lane remains on the
    // same owner across peer restarts, so enqueueOnOwner can retire the old
    // incarnation before the replacement lane is used.
    return config_.worker_count == 0 ? 0 : hash % config_.worker_count;
}

Status HighPerformanceTcpWorkers::submit(Task task) {
    if (config_.worker_count == 0) {
        return Status::InvalidArgument("HP TCP has no workers" LOC_MARK);
    }
    const size_t worker = next_worker_.fetch_add(1, std::memory_order_relaxed) %
                          config_.worker_count;
    return submitToWorker(worker, std::move(task));
}

Status HighPerformanceTcpWorkers::submitToWorker(size_t worker_id, Task task) {
    if (!task) {
        return Status::InvalidArgument(
            "cannot submit empty HP TCP task" LOC_MARK);
    }
    if (!running()) {
        return Status::InternalError("HP TCP workers are not running" LOC_MARK);
    }
    if (worker_id >= workers_.size()) {
        return Status::InvalidArgument(
            "HP TCP worker id is out of range" LOC_MARK);
    }
    return enqueue(worker_id, std::move(task));
}

Status HighPerformanceTcpWorkers::enqueue(size_t worker_id, Task task) {
    Command command;
    command.worker_id = worker_id;
    command.run = std::move(task);
    std::vector<Command> commands;
    commands.reserve(1);
    commands.push_back(std::move(command));
    return tryCommitBatch(commands, [] {});
}

Status HighPerformanceTcpWorkers::tryCommitBatch(
    std::vector<Command>& commands, const std::function<void()>& on_commit) {
    return tryCommitBatch(commands, nullptr, 0, 0, on_commit);
}

Status HighPerformanceTcpWorkers::tryCommitBatch(
    std::vector<Command>& commands,
    HighPerformanceTcpAdmissionController* admission, uint64_t reserve_tasks,
    uint64_t reserve_bytes, const std::function<void()>& on_commit) {
    if (commands.empty()) {
        if (reserve_tasks != 0 || reserve_bytes != 0) {
            return Status::InvalidArgument(
                "HP TCP empty batch cannot reserve admission" LOC_MARK);
        }
        return Status::OK();
    }
    if ((admission == nullptr) != (reserve_tasks == 0 && reserve_bytes == 0)) {
        return Status::InvalidArgument(
            "HP TCP admission reservation arguments are inconsistent" LOC_MARK);
    }
    if (admission != nullptr && (reserve_tasks == 0 || reserve_bytes == 0)) {
        return Status::InvalidArgument(
            "HP TCP admission reservation must be non-zero" LOC_MARK);
    }
    if (!running()) {
        return Status::InternalError("HP TCP workers are not running" LOC_MARK);
    }

    std::vector<size_t> owners;
    owners.reserve(commands.size());
    std::vector<size_t> required(workers_.size(), 0);
    for (const auto& command : commands) {
        if (!command.run || command.worker_id >= workers_.size()) {
            return Status::InvalidArgument(
                "invalid HP TCP batch command" LOC_MARK);
        }
        if (required[command.worker_id]++ == 0) {
            owners.push_back(command.worker_id);
        }
    }
    std::sort(owners.begin(), owners.end());

    std::vector<std::unique_lock<std::mutex>> locks;
    locks.reserve(owners.size());
    for (size_t owner : owners) {
        locks.emplace_back(workers_[owner]->mailbox_mutex);
    }

    if (!running()) {
        return Status::InternalError("HP TCP workers are stopping" LOC_MARK);
    }
    for (size_t owner : owners) {
        const auto& worker = *workers_[owner];
        if (required[owner] > config_.queue_capacity - worker.mailbox_size) {
            return Status::TooManyRequests(
                "HP TCP worker mailbox is full" LOC_MARK);
        }
    }

    std::vector<bool> wake(workers_.size(), false);
    for (size_t owner : owners) {
        wake[owner] = workers_[owner]->mailbox_size == 0 &&
                      !workers_[owner]->drain_posted;
    }

    // No mailbox has changed yet. Reserve the process-wide task/byte budget
    // under the same mailbox lock set so a failed transaction has zero visible
    // ownership side effects.
    bool admission_reserved = false;
    if (admission != nullptr) {
        Status reserved = admission->tryReserve(reserve_tasks, reserve_bytes);
        if (!reserved.ok()) return reserved;
        admission_reserved = true;
    }

    // The caller's callback is intentionally constrained to no-throw moves
    // into already-reserved storage. If that invariant is violated, roll back
    // the admission reservation before any mailbox mutation.
    if (on_commit) {
        try {
            on_commit();
        } catch (const std::exception& error) {
            if (admission_reserved) {
                admission->release(reserve_tasks, reserve_bytes);
            }
            return Status::InternalError(
                std::string("HP TCP ownership commit callback failed: ") +
                error.what() + LOC_MARK);
        } catch (...) {
            if (admission_reserved) {
                admission->release(reserve_tasks, reserve_bytes);
            }
            return Status::InternalError(
                "HP TCP ownership commit callback failed" LOC_MARK);
        }
    }

    for (auto& command : commands) {
        ringPushLocked(*workers_[command.worker_id], std::move(command));
    }
    for (size_t owner : owners) {
        if (wake[owner]) workers_[owner]->drain_posted = true;
    }
    for (auto& held : locks) held.unlock();

    try {
        for (size_t owner : owners) {
            if (wake[owner]) {
                asio::post(workers_[owner]->io,
                           [this, owner] { drain(owner); });
            }
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "HP TCP mailbox wake failed after ownership commit: "
                   << error.what();
        failAfterCommit(error.what());
        // Ownership was accepted already; returning non-OK would violate the
        // all-or-nothing contract by allowing the caller to reuse buffers.
        return Status::OK();
    }
    return Status::OK();
}

void HighPerformanceTcpWorkers::failAfterCommit(const char* reason) {
    LOG(ERROR) << "HP TCP worker group entering FAILED state: " << reason;
    state_.store(State::kFailed, std::memory_order_release);
    // Do not stop io_contexts here: already-started async operations still
    // need their cancellation/completion handlers to run. Pending mailbox
    // ownership is settled immediately; normal teardown joins contexts later.
    cancelPending();
}

void HighPerformanceTcpWorkers::drain(size_t worker_id) {
    auto& worker = *workers_[worker_id];
    for (;;) {
        Command command;
        {
            std::lock_guard<std::mutex> lock(worker.mailbox_mutex);
            if (worker.mailbox_size == 0) {
                worker.drain_posted = false;
                return;
            }
            command = ringPopLocked(worker);
        }

        if (!running()) {
            if (command.cancel) command.cancel();
            continue;
        }
        try {
            command.run(worker_id);
        } catch (const std::exception& error) {
            LOG(ERROR) << "HP TCP worker " << worker_id
                       << " command threw: " << error.what();
            if (command.cancel) command.cancel();
        } catch (...) {
            LOG(ERROR) << "HP TCP worker " << worker_id << " command threw";
            if (command.cancel) command.cancel();
        }
    }
}

Status HighPerformanceTcpWorkers::barrier() {
    if (onWorkerThread()) {
        return Status::InvalidArgument(
            "HP TCP barrier cannot run on a worker thread" LOC_MARK);
    }
    if (!controlContextAvailable()) {
        return Status::InternalError(
            "HP TCP worker contexts are unavailable" LOC_MARK);
    }

    struct Latch {
        std::mutex mutex;
        std::condition_variable cv;
        size_t remaining{0};
    };
    auto latch = std::make_shared<Latch>();
    latch->remaining = workers_.size();

    try {
        for (size_t i = 0; i < workers_.size(); ++i) {
            asio::post(workers_[i]->io, [latch] {
                std::lock_guard<std::mutex> lock(latch->mutex);
                if (--latch->remaining == 0) latch->cv.notify_all();
            });
        }
    } catch (const std::exception& error) {
        return Status::InternalError(
            std::string("HP TCP barrier post failed: ") + error.what() +
            LOC_MARK);
    }

    std::unique_lock<std::mutex> lock(latch->mutex);
    latch->cv.wait(lock, [&] { return latch->remaining == 0; });
    return Status::OK();
}

size_t HighPerformanceTcpWorkers::mailboxDepth(size_t worker_id) const {
    if (worker_id >= workers_.size()) return 0;
    const auto& worker = *workers_[worker_id];
    std::lock_guard<std::mutex> lock(worker.mailbox_mutex);
    return worker.mailbox_size;
}

asio::io_context& HighPerformanceTcpWorkers::ioContext(size_t worker_id) {
    if (worker_id >= workers_.size()) {
        throw std::out_of_range("HP TCP worker id out of range");
    }
    return workers_[worker_id]->io;
}

}  // namespace mooncake::tent
