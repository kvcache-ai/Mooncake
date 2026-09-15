#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

#include "client_offboarding.h"

namespace mooncake {

class ClientRegistry;
class ClientRegistryTestPeer;

// Registry implementation detail. The worker knows only how to retry a
// stateful cleanup operation; it never borrows a MasterService pointer.
class ClientOffboardingWorker final {
   public:
    using ProcessJob = std::function<bool(ClientOffboardingJob&)>;
    explicit ClientOffboardingWorker(ProcessJob process, bool account_metrics)
        : process_(std::move(process)), account_metrics_(account_metrics) {}
    ~ClientOffboardingWorker();
    ClientOffboardingWorker(const ClientOffboardingWorker&) = delete;
    ClientOffboardingWorker& operator=(const ClientOffboardingWorker&) = delete;

    void Start();
    void Stop();
    bool HasPending() const {
        return pending_jobs_.load(std::memory_order_acquire) != 0;
    }

   private:
    friend class ClientRegistry;
    friend class ClientRegistryTestPeer;
    void ReserveJob();
    void ScheduleReserved(ClientOffboardingJob job);
    void ThreadFunc();
    void CompleteJob(const ClientOffboardingJob& job);
    void DropJob(const ClientOffboardingJob& job, const char* reason);
    static std::chrono::seconds RetryDelay(uint64_t retry_count);
    static bool ShouldAlert(uint64_t retry_count) {
        return retry_count >= kAlertRetryThreshold;
    }
    static constexpr uint64_t kAlertRetryThreshold = 10;

    ProcessJob process_;
    const bool account_metrics_;
    std::thread thread_;
    bool running_{false};
    std::deque<ClientOffboardingJob> jobs_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<size_t> pending_jobs_{0};
};

}  // namespace mooncake
