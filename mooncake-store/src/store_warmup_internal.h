#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
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
// Defensive Master-side cap. A client request of 0 means "unlimited", but the
// service still returns at most this many targets.
inline constexpr uint64_t kMaxWarmupTargetsPerRequest = 1024;
inline constexpr auto kWarmupShutdownTimeout = std::chrono::seconds(5);

struct StoreWarmupOptions {
    size_t read_size = kDefaultWarmupReadSize;
    std::chrono::milliseconds timeout{kDefaultWarmupTimeoutMs};
    size_t concurrency = kDefaultWarmupConcurrency;
    size_t max_targets = kDefaultWarmupMaxTargets;
};

struct StoreWarmupStats {
    size_t discovered = 0;
    std::atomic<size_t> attempted{0};
    std::atomic<size_t> succeeded{0};
    std::atomic<size_t> failed{0};
    std::atomic<size_t> timed_out{0};
    std::atomic<size_t> skipped{0};
    size_t not_attempted = 0;
    size_t pending_cleanup = 0;
    size_t pending_transfers = 0;
    size_t pending_batch_id_releases = 0;
};

struct PendingWarmupBatch {
    Transport::BatchID batch_id;
    std::string segment_name;
    std::optional<BufferHandle> buffer;
    bool transfer_terminal = false;
    // Consecutive failures while querying the transfer status.
    size_t status_error_attempts = 0;
};

struct PendingWarmupBatchIDRelease {
    Transport::BatchID batch_id;
    std::string segment_name;
    size_t release_attempts = 0;
};

// A status-query error is not terminal: it does not prove that the transfer
// has stopped accessing its destination buffer.
enum class WarmupBatchState { kPending, kTerminal, kStatusError };

struct StoreWarmupProbeResult {
    enum class Status { kSucceeded, kFailed, kTimedOut, kSkipped };

    Status status = Status::kSkipped;
    bool pending_cleanup = false;
};

StoreWarmupOptions LoadStoreWarmupOptions();
uint64_t NormalizeWarmupMaxTargets(uint64_t requested);

// Owns destination buffers until Transfer Engine reports a terminal state,
// then retries batch-ID cleanup without retaining those buffers. The worker is
// started lazily, so disabled warmup adds no thread.
class WarmupBatchCleanup {
   public:
    using PollBatchFn = std::function<WarmupBatchState(Transport::BatchID)>;
    using ReleaseBatchFn = std::function<Status(Transport::BatchID)>;

    WarmupBatchCleanup(PollBatchFn poll_batch, ReleaseBatchFn release_batch);
    ~WarmupBatchCleanup();

    WarmupBatchCleanup(const WarmupBatchCleanup&) = delete;
    WarmupBatchCleanup& operator=(const WarmupBatchCleanup&) = delete;

    // This is the only warmup path that invokes freeBatchID().
    bool TryFreeWarmupBatchID(PendingWarmupBatchIDRelease& batch);
    bool ReleaseOrTrack(PendingWarmupBatch batch);
    void TrackPendingTransfer(PendingWarmupBatch batch);
    bool DrainFor(std::chrono::milliseconds timeout);
    size_t PendingCount() const;
    size_t PendingTransferCount() const;
    size_t PendingBatchIDReleaseCount() const;
    bool ShutdownFor(std::chrono::milliseconds timeout);

   private:
    void WorkerMain();

    PollBatchFn poll_batch_;
    ReleaseBatchFn release_batch_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::list<PendingWarmupBatch> pending_transfers_;
    std::list<PendingWarmupBatchIDRelease> pending_batch_id_releases_;
    std::thread worker_;
    size_t processing_transfers_ = 0;
    size_t processing_batch_id_releases_ = 0;
    bool stop_requested_ = false;
    bool stopped_ = false;
};

}  // namespace mooncake::internal
