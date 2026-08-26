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
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/status.h"

namespace mooncake::tent {

// Process-wide hard reservation for all HP TCP work that has been accepted by
// the transport but has not yet reached a terminal state. The reservation
// covers mailbox, lane queue, connect and in-flight I/O phases.
class HighPerformanceTcpAdmissionController {
   public:
    HighPerformanceTcpAdmissionController(uint64_t max_tasks,
                                          uint64_t max_bytes)
        : max_tasks_(max_tasks), max_bytes_(max_bytes) {}

    Status tryReserve(uint64_t tasks, uint64_t bytes);
    void release(uint64_t tasks, uint64_t bytes);
    void close();
    void reopenForTest();
    void waitForZero();
    uint64_t outstandingTasks() const;
    uint64_t outstandingBytes() const;
    bool accepting() const;

   private:
    const uint64_t max_tasks_;
    const uint64_t max_bytes_;
    mutable std::mutex mutex_;
    std::condition_variable zero_cv_;
    uint64_t tasks_{0};
    uint64_t bytes_{0};
    bool accepting_{true};
};

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
        kFailed,
    };

    using Task = std::function<void(size_t)>;

    struct Command {
        size_t worker_id{0};
        Task run;
        std::function<void()> cancel;
    };

    HighPerformanceTcpWorkers();
    explicit HighPerformanceTcpWorkers(Config config);
    ~HighPerformanceTcpWorkers();

    HighPerformanceTcpWorkers(const HighPerformanceTcpWorkers&) = delete;
    HighPerformanceTcpWorkers& operator=(const HighPerformanceTcpWorkers&) =
        delete;

    Status start();
    // Non-blocking state transition for owner callbacks. Normal teardown uses
    // stop() only after client/server I/O has quiesced.
    void requestStop();
    Status stop();

    Status submit(Task task);
    Status submitToWorker(size_t worker_id, Task task);

    // Whole-batch mailbox admission. Every affected mailbox is locked in
    // ascending worker-id order, all capacities are rechecked, then the
    // preallocated commands are moved into fixed-capacity rings. A non-OK
    // return leaves every mailbox unchanged. on_commit is invoked immediately
    // before the no-allocation ring commit and MUST NOT throw; the transport
    // uses it only to move shared_ptrs into an already-reserved SubBatch.
    Status tryCommitBatch(std::vector<Command>& commands,
                          const std::function<void()>& on_commit);

    // Same mailbox transaction, with the global task/byte reservation activated
    // while all affected mailboxes are locked. A non-OK return leaves both the
    // mailboxes and admission counters unchanged.
    Status tryCommitBatch(std::vector<Command>& commands,
                          HighPerformanceTcpAdmissionController* admission,
                          uint64_t reserve_tasks, uint64_t reserve_bytes,
                          const std::function<void()>& on_commit);

    // Cancel commands that have not yet reached their owner io_context.
    void cancelPending();

    // Wait until one marker has run on every worker context. The caller must
    // not be one of the worker threads.
    Status barrier();

    size_t affinityOwner(const AffinityKey& key) const;
    bool running() const {
        return state_.load(std::memory_order_acquire) == State::kRunning;
    }
    bool controlContextAvailable() const {
        const State s = state_.load(std::memory_order_acquire);
        return s == State::kRunning || s == State::kFailed;
    }
    size_t workerCount() const { return config_.worker_count; }
    size_t queueCapacity() const { return config_.queue_capacity; }
    size_t mailboxDepth(size_t worker_id) const;
    asio::io_context& ioContext(size_t worker_id);
    State state() const { return state_.load(std::memory_order_acquire); }
    bool onWorkerThread() const;

   private:
    struct WorkerContext {
        explicit WorkerContext(size_t capacity)
            : guard(asio::make_work_guard(io)), mailbox(capacity) {}

        asio::io_context io;
        asio::executor_work_guard<asio::io_context::executor_type> guard;
        mutable std::mutex mailbox_mutex;
        // Fixed-capacity ring: once start() succeeds, accepting a command does
        // not allocate and therefore cannot partially commit a batch on OOM.
        std::vector<std::optional<Command>> mailbox;
        size_t mailbox_head{0};
        size_t mailbox_tail{0};
        size_t mailbox_size{0};
        bool drain_posted{false};
        std::thread thread;
    };

    Status enqueue(size_t worker_id, Task task);
    void drain(size_t worker_id);
    void failAfterCommit(const char* reason);
    std::vector<Command> extractPendingLocked();
    void ringPushLocked(WorkerContext& worker, Command&& command);
    Command ringPopLocked(WorkerContext& worker);

    Config config_;
    std::vector<std::unique_ptr<WorkerContext>> workers_;
    std::atomic<State> state_{State::kCreated};
    std::atomic<size_t> next_worker_{0};

    mutable std::mutex lifecycle_mutex_;
    std::condition_variable lifecycle_cv_;
    size_t ready_workers_{0};
    bool joining_{false};
};

}  // namespace mooncake::tent

#endif  // TENT_HIGH_PERFORMANCE_TCP_WORKERS_H_
