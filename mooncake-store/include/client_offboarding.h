#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "client_liveness.h"
#include "types.h"

namespace mooncake {

namespace test {
class MasterServiceTest;
}

// A segment whose unmount this job prepared and therefore must commit.
struct PreparedSegmentOffboarding {
    UUID segment_id;
    std::string segment_name;
    std::string transport_endpoint;
    size_t metrics_dec_capacity{0};
};

// Process-local residual work for one terminal Client incarnation. The job is
// intentionally not serializable: snapshots stay behind the pending-work
// barrier until the residual work converges. It names the incarnation only:
// every attempt enumerates the segments the client still owns, which is stable
// because an OFFLINE session rejects registration until the job removes it.
struct ClientOffboardingJob {
    UUID client_id;
    ClientSessionSharedPtr retired_session;
    std::vector<PreparedSegmentOffboarding> prepared_segments;
    bool metadata_cleanup_accepted{false};
    bool local_ssd_unregistered{false};
    uint64_t retry_count{0};
    std::chrono::steady_clock::time_point next_attempt_at{
        std::chrono::steady_clock::now()};
    std::chrono::steady_clock::time_point enqueued_at{
        std::chrono::steady_clock::now()};
};

// Runs offboarding jobs on its own thread and retries, with backoff, the ones
// that have not converged. What a job does is the processor's business; the
// snapshot barrier is not tracked here either: it is the OFFLINE session
// itself, which stays registered until its job removes it.
class ClientOffboardingWorker {
   public:
    // Returns whether the job converged; false reschedules it. The processor
    // runs on the worker thread and may update the job between attempts. Its
    // owner must outlive Stop.
    using Processor = std::function<bool(ClientOffboardingJob&)>;

    explicit ClientOffboardingWorker(Processor process)
        : process_(std::move(process)) {}
    ~ClientOffboardingWorker();

    ClientOffboardingWorker(const ClientOffboardingWorker&) = delete;
    ClientOffboardingWorker& operator=(const ClientOffboardingWorker&) = delete;

    // Stop drops the jobs that are still queued or backing off.
    void Start();
    void Stop();
    // Requires a started worker.
    void Schedule(ClientOffboardingJob job);

   private:
    friend class test::MasterServiceTest;

    void ThreadFunc();
    void CompleteJob(const ClientOffboardingJob& job);
    void DropJob(const ClientOffboardingJob& job, const char* reason);
    static std::chrono::seconds RetryDelay(uint64_t retry_count);
    static bool ShouldAlert(uint64_t retry_count) {
        return retry_count >= kAlertRetryThreshold;
    }

    static constexpr uint64_t kAlertRetryThreshold = 10;

    const Processor process_;
    std::thread thread_;
    bool running_{false};
    std::deque<ClientOffboardingJob> jobs_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

}  // namespace mooncake
