// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_WORKERS_H_
#define TENT_HIGH_PERFORMANCE_TCP_WORKERS_H_

#include <asio.hpp>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include "tent/common/status.h"

namespace mooncake::tent {
class HighPerformanceTcpWorkers {
   public:
    struct Config {
        size_t worker_count{16};
        size_t queue_capacity{256};
    };
    struct AffinityKey {
        uint64_t peer{0};
        uint32_t endpoint{0};
        uint32_t lane{0};
        std::string incarnation;
    };
    enum class State {
        kCreated,
        kStarting,
        kRunning,
        kStopping,
        kStopped,
        kFailed
    };
    using Task = std::function<void(size_t)>;
    HighPerformanceTcpWorkers();
    explicit HighPerformanceTcpWorkers(Config config);
    ~HighPerformanceTcpWorkers();
    HighPerformanceTcpWorkers(const HighPerformanceTcpWorkers&) = delete;
    HighPerformanceTcpWorkers& operator=(const HighPerformanceTcpWorkers&) =
        delete;
    Status start();
    Status stop();
    Status submit(Task task);
    Status submitToWorker(size_t worker_id, Task task);
    size_t affinityOwner(const AffinityKey& key) const;
    bool running() const {
        return state_.load(std::memory_order_acquire) == State::kRunning;
    }
    size_t workerCount() const { return config_.worker_count; }
    asio::io_context& ioContext(size_t worker_id);
    State state() const { return state_.load(std::memory_order_acquire); }

   private:
    struct WorkerContext {
        explicit WorkerContext() : guard(asio::make_work_guard(io)) {}
        asio::io_context io;
        asio::executor_work_guard<asio::io_context::executor_type> guard;
        std::mutex mailbox_mutex;
        std::queue<Task> mailbox;
        bool drain_posted{false};
        std::thread thread;
    };
    Status enqueue(size_t worker_id, Task task);
    void drain(size_t worker_id);
    void requestStopLocked();
    Config config_;
    std::vector<std::unique_ptr<WorkerContext>> workers_;
    std::atomic<State> state_{State::kCreated};
    std::atomic<size_t> next_worker_{0};
    mutable std::mutex lifecycle_mutex_;
    std::condition_variable lifecycle_cv_;
    bool stop_joining_{false};
};
}  // namespace mooncake::tent
#endif
