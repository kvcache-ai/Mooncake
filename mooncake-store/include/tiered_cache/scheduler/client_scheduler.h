#pragma once

#include <memory>
#include <thread>
#include <atomic>
#include <vector>
#include <unordered_map>
#include "tiered_cache/scheduler/scheduler_policy.h"
#include "tiered_cache/scheduler/stats_collector.h"
#include "types.h"

#include <json/value.h>

namespace mooncake {

class TieredBackend;  // Forward declaration
class CacheTier;

/**
 * @class ClientScheduler
 * @brief Coordinates statistics collection, policy execution, and action
 * application.
 */
class ClientScheduler {
   public:
    ClientScheduler(TieredBackend* backend, const Json::Value& config);
    ~ClientScheduler();

    // Lifecycle management
    void Start();
    void Stop();

    // Register a managed tier
    void RegisterTier(CacheTier* tier);

    // Incoming event hook (thread-safe)
    void OnAccess(const std::string& key);

    // Called when a key is deleted
    void OnDelete(const std::string& key);

   private:
    // Background worker loop
    void WorkerLoop();

    // Execute generated actions
    void ExecuteActions(const std::vector<SchedAction>& actions);

   private:
    TieredBackend* backend_;
    std::unique_ptr<SchedulerPolicy> policy_;
    std::unique_ptr<StatsCollector> stats_collector_;

    std::atomic<bool> running_{false};
    std::thread worker_thread_;

    // Local view of tiers for policy input
    std::unordered_map<UUID, CacheTier*> tiers_;

    // Configuration
    int loop_interval_ms_ = 1000;
};

}  // namespace mooncake
