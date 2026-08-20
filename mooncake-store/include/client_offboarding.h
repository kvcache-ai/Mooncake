#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "client_liveness.h"
#include "types.h"

namespace mooncake {

class MasterService;
class ScopedSegmentAccess;

struct PreparedSegmentOffboarding {
    UUID segment_id;
    std::string segment_name;
    size_t metrics_dec_capacity{0};
};

struct ClientOffboardingJob {
    UUID client_id;
    std::shared_ptr<ClientLivenessRecord> liveness;
    std::vector<PreparedSegmentOffboarding> segments;
    bool all_segments_prepared{true};
    // Set when a previously failed job is re-queued: the worker re-enumerates
    // and re-prepares the Client's Segments before committing again, because
    // Segments whose prepare failed remain mounted.
    bool needs_reprepare{false};
    uint64_t retry_count{0};
    std::chrono::steady_clock::time_point enqueued_at{
        std::chrono::steady_clock::now()};
};

// Completes terminal Client offboarding after MasterService has synchronously
// prepared the Client's Segments. Jobs are never coalesced: each one represents
// one Client incarnation and carries the record used for compare-and-erase.
//
// Failed jobs stay queued and keep the snapshot barrier raised until their
// cleanup converges: a snapshot taken while partially offboarded resources
// persist would restore them with fresh Active liveness after a failover.
class ClientOffboardingWorker {
   public:
    explicit ClientOffboardingWorker(MasterService* service)
        : service_(service) {}
    ~ClientOffboardingWorker();

    ClientOffboardingWorker(const ClientOffboardingWorker&) = delete;
    ClientOffboardingWorker& operator=(const ClientOffboardingWorker&) = delete;

    void Start();
    void Stop();
    [[nodiscard]] bool Schedule(ClientOffboardingJob job);
    [[nodiscard]] bool HasPending() const {
        return pending_jobs_.load(std::memory_order_acquire) != 0;
    }
    // Snapshots are skipped while any offboarding job is pending or being
    // retried, so partially reclaimed state is never persisted.
    [[nodiscard]] bool ShouldSkipSnapshot();

   private:
    void ThreadFunc();
    // Re-enumerates the Client's still-mounted Segments and re-prepares them.
    // Must be called with the service snapshot lock held (shared or better)
    // and with the caller's ScopedSegmentAccess: constructing a second
    // access recursively locks the segment manager and deadlocks.
    void ReprepareJob(ClientOffboardingJob& job,
                      ScopedSegmentAccess& segment_access);

    MasterService* service_;
    mutable std::mutex lifecycle_mutex_;
    std::thread thread_;
    bool running_{false};
    std::deque<ClientOffboardingJob> jobs_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<size_t> pending_jobs_{0};

    // Pause before re-attempting a batch that contained a failed job. Failure
    // causes (e.g. a Segment mid-graceful-unmount) are transient, so a short
    // fixed delay avoids a busy retry loop without starving convergence.
    static constexpr int kRetryBackoffMs = 200;
};

}  // namespace mooncake
