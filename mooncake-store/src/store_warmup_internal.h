#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <string>
#include <thread>

#include "client_buffer.h"
#include "transfer_engine.h"

namespace mooncake::internal {

inline constexpr size_t kDefaultWarmupReadSize = 64;
inline constexpr uint64_t kDefaultWarmupTimeoutMs = 1000;
inline constexpr size_t kDefaultWarmupConcurrency = 16;
inline constexpr size_t kDefaultWarmupMaxTargets = 16;
inline constexpr size_t kMaxWarmupReadSize = 1024 * 1024;
inline constexpr uint64_t kMaxWarmupTimeoutMs = 60000;
inline constexpr size_t kMaxWarmupConcurrency = 128;
inline constexpr size_t kMaxWarmupTargets = 65536;

struct StoreWarmupOptions {
    size_t read_size = kDefaultWarmupReadSize;
    std::chrono::milliseconds timeout{kDefaultWarmupTimeoutMs};
    size_t concurrency = kDefaultWarmupConcurrency;
    size_t max_targets = kDefaultWarmupMaxTargets;
};

struct StoreWarmupStats {
    size_t target_count = 0;
    std::atomic<size_t> succeeded{0};
    std::atomic<size_t> failed{0};
    std::atomic<size_t> timed_out{0};
    std::atomic<size_t> skipped{0};
    size_t pending_cleanup = 0;
};

struct PendingWarmupBatch {
    Transport::BatchID batch_id;
    std::string segment_name;
    BufferHandle buffer;
    bool transfer_terminal = false;
    size_t release_attempts = 0;
};

enum class WarmupBatchState { kPending, kTerminal, kStatusError };

struct StoreWarmupProbeResult {
    enum class Status { kSucceeded, kFailed, kTimedOut, kSkipped };

    Status status = Status::kSkipped;
    bool pending_cleanup = false;
};

StoreWarmupOptions LoadStoreWarmupOptions();

// Owns both batch IDs and their destination buffers until Transfer Engine
// reports a terminal state and accepts freeBatchID(). The worker is started
// lazily, so disabled warmup adds no thread.
class WarmupBatchCleanup {
   public:
    using PollBatchFn = std::function<WarmupBatchState(Transport::BatchID)>;
    using ReleaseBatchFn = std::function<Status(Transport::BatchID)>;

    WarmupBatchCleanup(PollBatchFn poll_batch, ReleaseBatchFn release_batch);
    ~WarmupBatchCleanup();

    WarmupBatchCleanup(const WarmupBatchCleanup&) = delete;
    WarmupBatchCleanup& operator=(const WarmupBatchCleanup&) = delete;

    // This is the only warmup path that invokes freeBatchID(). A false return
    // means ownership must remain with this cleanup object for a later retry.
    bool TryReleaseWarmupBatch(PendingWarmupBatch& batch);
    bool ReleaseOrTrack(PendingWarmupBatch batch);
    void Track(PendingWarmupBatch batch);
    bool DrainFor(std::chrono::milliseconds timeout);
    size_t PendingCount() const;
    void Shutdown();

   private:
    void WorkerMain();

    PollBatchFn poll_batch_;
    ReleaseBatchFn release_batch_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::list<PendingWarmupBatch> pending_;
    std::thread worker_;
    bool stopping_ = false;
};

}  // namespace mooncake::internal
