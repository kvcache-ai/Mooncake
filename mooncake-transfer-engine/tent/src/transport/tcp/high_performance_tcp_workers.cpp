// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_workers.h"
#include <exception>
#include <functional>
#include <glog/logging.h>
namespace mooncake::tent {
HighPerformanceTcpWorkers::HighPerformanceTcpWorkers()
    : HighPerformanceTcpWorkers(Config{}) {}
HighPerformanceTcpWorkers::HighPerformanceTcpWorkers(Config c) : config_(c) {}
HighPerformanceTcpWorkers::~HighPerformanceTcpWorkers() { stop(); }
Status HighPerformanceTcpWorkers::start() {
    std::unique_lock<std::mutex> g(lifecycle_mutex_);
    auto s = state_.load();
    if (s == State::kRunning) return Status::OK();
    if (s != State::kCreated)
        return Status::InvalidArgument(
            "High-performance TCP workers cannot be restarted" LOC_MARK);
    if (!config_.worker_count || !config_.queue_capacity)
        return Status::InvalidArgument(
            "HP TCP worker_count and queue_capacity must be positive" LOC_MARK);
    state_.store(State::kStarting);
    try {
        workers_.reserve(config_.worker_count);
        for (size_t i = 0; i < config_.worker_count; ++i)
            workers_.push_back(std::make_unique<WorkerContext>());
        for (size_t i = 0; i < workers_.size(); ++i)
            workers_[i]->thread =
                std::thread([this, i] { workers_[i]->io.run(); });
        state_.store(State::kRunning, std::memory_order_release);
        return Status::OK();
    } catch (const std::exception& e) {
        state_.store(State::kFailed);
        requestStopLocked();
        g.unlock();
        for (auto& w : workers_)
            if (w->thread.joinable()) w->thread.join();
        return Status::InternalError(
            std::string("Unable to start HP TCP workers: ") + e.what() +
            LOC_MARK);
    }
}
void HighPerformanceTcpWorkers::requestStopLocked() {
    for (auto& w : workers_) {
        w->guard.reset();
        w->io.stop();
    }
}
Status HighPerformanceTcpWorkers::stop() {
    std::unique_lock<std::mutex> g(lifecycle_mutex_);
    auto s = state_.load();
    if (s == State::kStopped || s == State::kCreated) return Status::OK();
    if (stop_joining_) {
        lifecycle_cv_.wait(g, [&] { return !stop_joining_; });
        return Status::OK();
    }
    stop_joining_ = true;
    state_.store(State::kStopping, std::memory_order_release);
    requestStopLocked();
    g.unlock();
    for (auto& w : workers_)
        if (w->thread.joinable()) {
            if (w->thread.get_id() == std::this_thread::get_id())
                return Status::InvalidArgument(
                    "HP TCP worker may not stop itself" LOC_MARK);
            w->thread.join();
        }
    g.lock();
    state_.store(State::kStopped, std::memory_order_release);
    stop_joining_ = false;
    g.unlock();
    lifecycle_cv_.notify_all();
    return Status::OK();
}
size_t HighPerformanceTcpWorkers::affinityOwner(const AffinityKey& k) const {
    size_t h = std::hash<uint64_t>{}(k.peer);
    h ^= std::hash<uint32_t>{}(k.endpoint + 0x9e3779b9 + (h << 6) + (h >> 2));
    h ^= std::hash<uint32_t>{}(k.lane + 0x9e3779b9 + (h << 6) + (h >> 2));
    h ^= std::hash<std::string>{}(k.incarnation);
    return config_.worker_count ? h % config_.worker_count : 0;
}
Status HighPerformanceTcpWorkers::submit(Task t) {
    if (!t)
        return Status::InvalidArgument(
            "Cannot submit empty HP TCP task" LOC_MARK);
    if (!running())
        return Status::InternalError("HP TCP workers are not running" LOC_MARK);
    return enqueue(next_worker_.fetch_add(1) % config_.worker_count,
                   std::move(t));
}
Status HighPerformanceTcpWorkers::submitToWorker(size_t id, Task t) {
    if (!t)
        return Status::InvalidArgument(
            "Cannot submit empty HP TCP task" LOC_MARK);
    if (!running())
        return Status::InternalError("HP TCP workers are not running" LOC_MARK);
    if (id >= workers_.size())
        return Status::InvalidArgument(
            "HP TCP worker id is out of range" LOC_MARK);
    return enqueue(id, std::move(t));
}
Status HighPerformanceTcpWorkers::enqueue(size_t id, Task task) {
    auto& w = *workers_[id];
    bool wake = false;
    {
        std::lock_guard<std::mutex> g(w.mailbox_mutex);
        if (!running())
            return Status::InternalError(
                "HP TCP workers are stopping" LOC_MARK);
        if (w.mailbox.size() >= config_.queue_capacity)
            return Status::TooManyRequests(
                "HP TCP worker mailbox is full" LOC_MARK);
        w.mailbox.push(std::move(task));
        if (!w.drain_posted) {
            w.drain_posted = true;
            wake = true;
        }
    }
    if (wake) {
        try {
            asio::post(w.io, [this, id] { drain(id); });
        } catch (const std::exception& e) {
            return Status::InternalError(
                std::string("HP TCP mailbox wake failed: ") + e.what() +
                LOC_MARK);
        }
    }
    return Status::OK();
}
void HighPerformanceTcpWorkers::drain(size_t id) {
    auto& w = *workers_[id];
    for (;;) {
        Task t;
        {
            std::lock_guard<std::mutex> g(w.mailbox_mutex);
            if (w.mailbox.empty()) {
                w.drain_posted = false;
                return;
            }
            t = std::move(w.mailbox.front());
            w.mailbox.pop();
        }
        try {
            t(id);
        } catch (const std::exception& e) {
            LOG(ERROR) << "HP TCP worker " << id
                       << " task failed: " << e.what();
        } catch (...) {
            LOG(ERROR) << "HP TCP worker " << id << " task failed";
        }
    }
}
asio::io_context& HighPerformanceTcpWorkers::ioContext(size_t id) {
    return workers_.at(id)->io;
}
}  // namespace mooncake::tent
