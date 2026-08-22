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

struct PendingSegmentOffboarding {
    UUID segment_id;
    std::string segment_name;
    std::string transport_endpoint;
};

struct PreparedSegmentOffboarding {
    UUID segment_id;
    std::string segment_name;
    std::string transport_endpoint;
    size_t metrics_dec_capacity{0};
};

// Process-local residual work for one terminal Client incarnation. The job is
// intentionally not serializable: snapshots stay behind the pending-work
// barrier until the residual work converges.
struct ClientOffboardingJob {
    UUID client_id;
    std::shared_ptr<ClientLivenessRecord> liveness;
    std::vector<PendingSegmentOffboarding> pending_prepare_segments;
    std::vector<PreparedSegmentOffboarding> prepared_segments;
    bool metadata_cleanup_accepted{false};
    bool local_ssd_unregistered{false};
    uint64_t retry_count{0};
    std::chrono::steady_clock::time_point next_attempt_at{
        std::chrono::steady_clock::now()};
    std::chrono::steady_clock::time_point enqueued_at{
        std::chrono::steady_clock::now()};
};

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

   private:
    void ThreadFunc();
    void CompleteJob(const ClientOffboardingJob& job);
    void DropJob(const ClientOffboardingJob& job, const char* reason);
    static std::chrono::seconds RetryDelay(uint64_t retry_count);

    MasterService* service_;
    std::thread thread_;
    bool running_{false};
    std::deque<ClientOffboardingJob> jobs_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<size_t> pending_jobs_{0};
};

}  // namespace mooncake
