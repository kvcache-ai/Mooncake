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
    if (failed_.load(std::memory_order_acquire)) {
        return Status::InternalError(
            "HP TCP admission accounting is failed" LOC_MARK);
    }
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
    bool notify_waiters = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (tasks > tasks_ || bytes > bytes_) {
            LOG(ERROR) << "HP TCP admission release underflow";
            // Do not manufacture a drained state. A double release is a
            // lifecycle invariant violation; preserving the counters keeps
            // shutdown from treating live work as retired.
            failed_.store(true, std::memory_order_release);
            notify_waiters = true;
        } else {
            tasks_ -= tasks;
            bytes_ -= bytes;
            notify_waiters = tasks_ == 0 && bytes_ == 0;
        }
    }
    if (notify_waiters) zero_cv_.notify_all();
}

void HighPerformanceTcpAdmissionController::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    accepting_ = false;
}

Status HighPerformanceTcpAdmissionController::waitForZero() {
    std::unique_lock<std::mutex> lock(mutex_);
    zero_cv_.wait(lock, [&] {
        return failed_.load(std::memory_order_acquire) ||
               (tasks_ == 0 && bytes_ == 0);
    });
    if (failed_.load(std::memory_order_acquire)) {
        return Status::InternalError(
            "HP TCP admission accounting is failed" LOC_MARK);
    }
    return Status::OK();
}

bool HighPerformanceTcpAdmissionController::failed() const {
    return failed_.load(std::memory_order_acquire);
}

uint64_t HighPerformanceTcpAdmissionController::outstandingTasks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return tasks_;
}

uint64_t HighPerformanceTcpAdmissionController::outstandingBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return bytes_;
}

HighPerformanceTcpWorkers::HighPerformanceTcpWorkers()
    : HighPerformanceTcpWorkers(Config{}) {}

HighPerformanceTcpWorkers::HighPerformanceTcpWorkers(Config config)
    : config_(config) {}

HighPerformanceTcpWorkers::~HighPerformanceTcpWorkers() { (void)stop(); }

Status HighPerformanceTcpWorkers::start() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (running()) return Status::OK();
    if (started_ || !workers_.empty() || config_.worker_count == 0) {
        return Status::InvalidArgument("invalid HP TCP worker state" LOC_MARK);
    }

    try {
        started_ = true;
        failed_.store(false, std::memory_order_release);
        workers_.reserve(config_.worker_count);
        for (size_t i = 0; i < config_.worker_count; ++i) {
            workers_.push_back(std::make_unique<WorkerContext>());
        }
        running_.store(true, std::memory_order_release);
        for (auto& worker : workers_) {
            worker->thread = std::thread([this, context = worker.get()] {
                for (;;) {
                    try {
                        context->io.run();
                        break;
                    } catch (const std::exception& error) {
                        LOG(ERROR) << "HP TCP io_context handler failed: "
                                   << error.what();
                        markWorkerFailed();
                    } catch (...) {
                        LOG(ERROR) << "HP TCP io_context handler failed";
                        markWorkerFailed();
                    }
                }
            });
        }
    } catch (const std::exception& error) {
        running_.store(false, std::memory_order_release);
        for (auto& worker : workers_) {
            worker->guard.reset();
            worker->io.stop();
            if (worker->thread.joinable()) worker->thread.join();
        }
        workers_.clear();
        return Status::InternalError(
            std::string("failed to start HP TCP workers: ") + error.what() +
            LOC_MARK);
    }
    return Status::OK();
}

void HighPerformanceTcpWorkers::markWorkerFailed() noexcept {
    failed_.store(true, std::memory_order_release);
    // New batches observe failed_ and are rejected. The owner loop deliberately
    // keeps running so already-committed work and teardown cancellation retain
    // their original affinity and can retire every task, session and lease.
}

bool HighPerformanceTcpWorkers::onWorkerThread() const {
    const auto current = std::this_thread::get_id();
    return std::any_of(workers_.begin(), workers_.end(), [&](const auto& w) {
        return w->thread.joinable() && w->thread.get_id() == current;
    });
}

Status HighPerformanceTcpWorkers::stop() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (workers_.empty()) return Status::OK();
    if (onWorkerThread()) {
        return Status::InvalidArgument(
            "HP TCP worker cannot synchronously stop itself" LOC_MARK);
    }
    const bool failed = hasFailedWorker();
    running_.store(false, std::memory_order_release);
    for (auto& worker : workers_) worker->guard.reset();
    for (auto& worker : workers_) worker->io.stop();
    for (auto& worker : workers_) {
        if (worker->thread.joinable()) worker->thread.join();
    }
    // Keep stopped contexts alive until their client/server owners are
    // destroyed. Those owners contain socket objects and cancellation handlers
    // that refer to these contexts, even after no worker can execute them.
    if (failed) {
        return Status::InternalError(
            "HP TCP workers stopped after an owner thread failure" LOC_MARK);
    }
    return Status::OK();
}

size_t HighPerformanceTcpWorkers::affinityOwner(uint64_t peer,
                                                uint32_t lane) const {
    size_t hash = std::hash<uint64_t>{}(peer);
    const auto mix = [&hash](size_t value) {
        hash ^= value + static_cast<size_t>(0x9e3779b97f4a7c15ULL) +
                (hash << 6U) + (hash >> 2U);
    };
    // HP TCP v1 has one endpoint. Keep its zero index in the hash so this
    // interface simplification does not change the existing worker mapping.
    mix(std::hash<uint32_t>{}(0));
    mix(std::hash<uint32_t>{}(lane));
    return config_.worker_count == 0 ? 0 : hash % config_.worker_count;
}

Status HighPerformanceTcpWorkers::tryCommitBatch(
    std::vector<Command>& commands,
    HighPerformanceTcpAdmissionController* admission, uint64_t reserve_tasks,
    uint64_t reserve_bytes, const std::function<void()>& on_commit) {
    if (commands.empty()) return Status::OK();

    std::lock_guard<std::mutex> submit_lock(submit_mutex_);
    if (hasFailedWorker()) {
        return Status::InternalError(
            "HP TCP worker owner thread has failed" LOC_MARK);
    }
    if (!running()) {
        return Status::InternalError("HP TCP workers are not running" LOC_MARK);
    }

    for (const auto& command : commands) {
        if (!command.run || command.worker_id >= workers_.size()) {
            return Status::InvalidArgument(
                "invalid HP TCP worker command" LOC_MARK);
        }
    }

    bool reserved = false;
    if (admission != nullptr) {
        CHECK_STATUS(admission->tryReserve(reserve_tasks, reserve_bytes));
        reserved = true;
    }
    try {
        if (on_commit) on_commit();
    } catch (...) {
        if (reserved) admission->release(reserve_tasks, reserve_bytes);
        return Status::InternalError("HP TCP ownership commit failed" LOC_MARK);
    }

    for (size_t i = 0; i < commands.size(); ++i) {
        Command command = std::move(commands[i]);
        const size_t owner = command.worker_id;
        const auto cancel = command.cancel;
        try {
            asio::post(workers_[owner]->io,
                       [this, command = std::move(command)]() mutable {
                           runCommand(std::move(command));
                       });
        } catch (...) {
            if (cancel) cancel();
            for (++i; i < commands.size(); ++i) {
                auto& pending = commands[i];
                if (pending.cancel) pending.cancel();
            }
            return Status::OK();
        }
    }
    return Status::OK();
}

void HighPerformanceTcpWorkers::runCommand(Command command) {
    const size_t owner = command.worker_id;
    try {
        if (running()) {
            command.run(owner);
        } else if (command.cancel) {
            command.cancel();
        }
    } catch (const std::exception& error) {
        LOG(ERROR) << "HP TCP worker command failed: " << error.what();
        if (command.cancel) command.cancel();
    } catch (...) {
        LOG(ERROR) << "HP TCP worker command failed";
        if (command.cancel) command.cancel();
    }
}

Status HighPerformanceTcpWorkers::barrier() {
    if (onWorkerThread()) {
        return Status::InvalidArgument(
            "HP TCP barrier cannot run on a worker thread" LOC_MARK);
    }
    if (workers_.empty()) return Status::OK();
    if (!running()) {
        return Status::InternalError(
            "HP TCP barrier cannot run after workers stop" LOC_MARK);
    }

    struct Latch {
        std::mutex mutex;
        std::condition_variable cv;
        size_t remaining{0};
    };
    auto latch = std::make_shared<Latch>();
    latch->remaining = workers_.size();
    try {
        for (auto& worker : workers_) {
            asio::post(worker->io, [latch] {
                std::lock_guard<std::mutex> lock(latch->mutex);
                if (--latch->remaining == 0) latch->cv.notify_all();
            });
        }
    } catch (...) {
        return Status::InternalError("HP TCP barrier post failed" LOC_MARK);
    }
    std::unique_lock<std::mutex> lock(latch->mutex);
    latch->cv.wait(lock, [&] { return latch->remaining == 0; });
    return Status::OK();
}

asio::io_context& HighPerformanceTcpWorkers::ioContext(size_t worker_id) {
    if (worker_id >= workers_.size()) {
        throw std::out_of_range("HP TCP worker id out of range");
    }
    return workers_[worker_id]->io;
}

}  // namespace mooncake::tent
