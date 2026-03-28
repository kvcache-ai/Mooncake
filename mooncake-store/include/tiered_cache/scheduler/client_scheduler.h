#pragma once

#include <array>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string_view>
#include "mutex.h"
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

    // Called when a replica is committed or updated
    void OnCommit(const std::string& key, UUID tier_id, size_t size_bytes);

    // Called when a key or a replica is deleted
    void OnDelete(const std::string& key,
                  std::optional<UUID> tier_id = std::nullopt);

    // Called when allocation fails due to insufficient space
    // Returns true if reclaim freed enough space for an immediate retry
    bool OnAllocationFailure(UUID tier_id, size_t required_bytes);

   private:
    struct PlannedReclaim {
        struct Step {
            SchedAction action;
            size_t size_bytes = 0;
        };

        std::vector<Step> steps;
        size_t target_reclaim_bytes = 0;
    };

    // Background worker loop
    void WorkerLoop();

    // Execute generated actions
    void ExecuteActions(const std::vector<SchedAction>& actions);

    // Trigger immediate eviction for a tier (sync mode)
    bool TriggerSyncEviction(UUID tier_id, size_t required_bytes);

    // Reclaim pre-replicated cold replicas without copying data on the
    // allocation failure path.
    bool TryFastReclaim(UUID tier_id, size_t required_bytes);

    PlannedReclaim BuildReclaimPlan(
        UUID tier_id, const std::unordered_map<UUID, TierStats>& tier_stats,
        const std::vector<KeyContext>& active_keys,
        bool require_existing_replica, size_t required_bytes) const;

    size_t ExecuteReclaimPlan(const PlannedReclaim& plan);
    bool HasAvailableBytes(UUID tier_id, size_t required_bytes) const;
    std::optional<UUID> SelectDemotionTier(UUID source_tier_id) const;

    // Build policy input from the latest stats snapshot and scheduler cache
    std::vector<KeyContext> BuildActiveKeys(
        const AccessStats& access_stats,
        std::optional<UUID> pinned_tier_id = std::nullopt);

    // Build a fresh tier stats map for policy execution
    std::unordered_map<UUID, TierStats> CollectTierStats() const;

    struct CachedKeyState {
        size_t size_bytes = 0;
        std::vector<UUID> current_locations;
    };

    struct KeyCacheShard {
        mutable Mutex mutex;
        std::unordered_map<std::string, CachedKeyState> key_cache
            GUARDED_BY(mutex);
        std::unordered_map<UUID, std::unordered_set<std::string>>
            tier_resident_keys GUARDED_BY(mutex);
    };

    static constexpr size_t kKeyCacheShardCount = 16;

    static size_t KeyCacheShardIndex(std::string_view key);
    KeyCacheShard& GetKeyCacheShard(std::string_view key);
    const KeyCacheShard& GetKeyCacheShard(std::string_view key) const;

    size_t EstimateActiveKeyReserve(const AccessStats& access_stats,
                                    std::optional<UUID> pinned_tier_id) const;
    void AppendHotKeys(const AccessStats& access_stats,
                       std::vector<KeyContext>& active_keys,
                       std::unordered_set<std::string>& seen_keys) const;
    void AppendPinnedTierKeys(UUID pinned_tier_id,
                              std::vector<KeyContext>& active_keys,
                              std::unordered_set<std::string>& seen_keys) const;
    std::optional<KeyContext> BuildKeyContextLocked(
        const std::string& key, const CachedKeyState& state,
        const AccessStats& access_stats,
        const AccessStatEntry* stat_entry = nullptr) const;
    size_t GetCachedKeySize(const std::string& key) const;

    void TrackReplicaLocked(KeyCacheShard& shard, const std::string& key,
                            UUID tier_id, size_t size_bytes)
        REQUIRES(shard.mutex);
    bool RemoveReplicaLocked(KeyCacheShard& shard, const std::string& key,
                             std::optional<UUID> tier_id) REQUIRES(shard.mutex);

   private:
    TieredBackend* backend_;
    std::unique_ptr<SchedulerPolicy> policy_;
    std::unique_ptr<StatsCollector> stats_collector_;

    std::atomic<bool> running_{false};
    std::thread worker_thread_;

    // Local view of tiers for policy input
    std::unordered_map<UUID, CacheTier*> tiers_;

    // Scheduler-side metadata cache to avoid full backend scans each cycle
    std::array<KeyCacheShard, kKeyCacheShardCount> key_cache_shards_;
    std::optional<UUID> fast_tier_id_;

    // Configuration
    int loop_interval_ms_ = 1000;
    size_t stats_snapshot_limit_ = detail::DefaultSnapshotLimit();
    enum class EvictionMode { SYNC, ASYNC };
    EvictionMode eviction_mode_ = EvictionMode::ASYNC;

    // Used to wake the worker thread immediately on Stop().
    std::mutex cv_mutex_;
    std::condition_variable cv_;
};

}  // namespace mooncake
