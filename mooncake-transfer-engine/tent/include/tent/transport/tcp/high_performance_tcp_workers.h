// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_WORKERS_H_
#define TENT_HIGH_PERFORMANCE_TCP_WORKERS_H_

#include <asio.hpp>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "tent/common/status.h"

namespace mooncake::tent {

// A single process-wide bound for accepted HP TCP work. Reservations remain
// active until the corresponding task reaches a terminal state.
class HighPerformanceTcpAdmissionController {
   public:
    HighPerformanceTcpAdmissionController(uint64_t max_tasks,
                                          uint64_t max_bytes)
        : max_tasks_(max_tasks), max_bytes_(max_bytes) {}

    Status tryReserve(uint64_t tasks, uint64_t bytes);
    void release(uint64_t tasks, uint64_t bytes);
    void close();
    void waitForZero();

   private:
    const uint64_t max_tasks_;
    const uint64_t max_bytes_;
    mutable std::mutex mutex_;
    std::condition_variable zero_cv_;
    uint64_t tasks_{0};
    uint64_t bytes_{0};
    bool accepting_{true};
};

// Thin owner-thread pool for sockets. ASIO is the queue; this class adds
// deterministic affinity and lifecycle management. Accepted transfer work is
// bounded by HighPerformanceTcpAdmissionController.
class HighPerformanceTcpWorkers {
   public:
    struct Config {
        size_t worker_count{16};
    };

    using Task = std::function<void(size_t)>;
    struct Command {
        size_t worker_id{0};
        Task run;
        std::function<void()> cancel;
    };

    explicit HighPerformanceTcpWorkers(Config config);
    ~HighPerformanceTcpWorkers();

    HighPerformanceTcpWorkers(const HighPerformanceTcpWorkers&) = delete;
    HighPerformanceTcpWorkers& operator=(const HighPerformanceTcpWorkers&) =
        delete;

    Status start();
    Status stop();
    Status tryCommitBatch(std::vector<Command>& commands,
                          HighPerformanceTcpAdmissionController* admission,
                          uint64_t reserve_tasks, uint64_t reserve_bytes,
                          const std::function<void()>& on_commit);

    Status barrier();
    size_t affinityOwner(uint64_t peer, uint32_t lane) const;
    bool running() const { return running_.load(std::memory_order_acquire); }
    size_t workerCount() const { return config_.worker_count; }
    asio::io_context& ioContext(size_t worker_id);
    bool onWorkerThread() const;

   private:
    struct WorkerContext {
        WorkerContext() : guard(asio::make_work_guard(io)) {}
        asio::io_context io;
        asio::executor_work_guard<asio::io_context::executor_type> guard;
        std::thread thread;
    };

    void runCommand(Command command);

    Config config_;
    std::vector<std::unique_ptr<WorkerContext>> workers_;
    std::atomic<bool> running_{false};
    bool started_{false};
    mutable std::mutex lifecycle_mutex_;
    mutable std::mutex submit_mutex_;
};

}  // namespace mooncake::tent

#endif  // TENT_HIGH_PERFORMANCE_TCP_WORKERS_H_
