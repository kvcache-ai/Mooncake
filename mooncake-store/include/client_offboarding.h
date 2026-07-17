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
    std::chrono::steady_clock::time_point enqueued_at{
        std::chrono::steady_clock::now()};
};

// Completes terminal Client offboarding after MasterService has synchronously
// prepared the Client's Segments. Jobs are never coalesced: each one represents
// one Client incarnation and carries the record used for compare-and-erase.
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

    MasterService* service_;
    mutable std::mutex lifecycle_mutex_;
    std::thread thread_;
    bool running_{false};
    std::deque<ClientOffboardingJob> jobs_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<size_t> pending_jobs_{0};
};

}  // namespace mooncake
