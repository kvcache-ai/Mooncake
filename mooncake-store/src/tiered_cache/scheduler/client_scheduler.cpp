#include "tiered_cache/scheduler/client_scheduler.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/tiers/cache_tier.h"
#include "tiered_cache/scheduler/lru_policy.h"
#include "tiered_cache/scheduler/lru_stats_collector.h"
#include "tiered_cache/scheduler/simple_policy.h"
#include <algorithm>
#include <chrono>
#include <unordered_set>
#include <glog/logging.h>

namespace mooncake {

ClientScheduler::ClientScheduler(TieredBackend* backend,
                                 const Json::Value& config)
    : backend_(backend) {
    std::string policy_type = "SIMPLE";  // Default
    size_t stats_shards = detail::DefaultStatsShardCount();
    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("policy")) {
        policy_type = config["scheduler"]["policy"].asString();
    }

    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("stats_shards")) {
        const auto configured_shards =
            config["scheduler"]["stats_shards"].asUInt64();
        if (configured_shards > 0) {
            stats_shards = static_cast<size_t>(configured_shards);
        }
    }

    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("stats_snapshot_limit")) {
        const auto configured_limit =
            config["scheduler"]["stats_snapshot_limit"].asUInt64();
        stats_snapshot_limit_ = static_cast<size_t>(configured_limit);
    }

    // Read eviction mode configuration
    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("eviction_mode")) {
        std::string mode = config["scheduler"]["eviction_mode"].asString();
        if (mode == "sync") {
            eviction_mode_ = EvictionMode::SYNC;
            LOG(INFO) << "Eviction mode: SYNC (immediate)";
        } else {
            eviction_mode_ = EvictionMode::ASYNC;
            LOG(INFO) << "Eviction mode: ASYNC (periodic)";
        }
    }

    if (policy_type == "LRU") {
        // Initialize LRU components
        stats_collector_ = std::make_unique<LRUStatsCollector>(
            stats_shards, stats_snapshot_limit_);

        // LRU Policy Configuration
        LRUPolicy::Config lru_config;
        if (config.isMember("scheduler")) {
            const auto& sched_conf = config["scheduler"];
            if (sched_conf.isMember("high_watermark"))
                lru_config.high_watermark =
                    sched_conf["high_watermark"].asDouble();
            if (sched_conf.isMember("low_watermark"))
                lru_config.low_watermark =
                    sched_conf["low_watermark"].asDouble();
        }

        auto lru_policy = std::make_unique<LRUPolicy>(lru_config);
        policy_ = std::move(lru_policy);
        LOG(INFO) << "ClientScheduler initialized with LRU Policy";
    } else {
        // Default: SIMPLE
        stats_collector_ = std::make_unique<SimpleStatsCollector>(
            0.5, stats_shards, stats_snapshot_limit_);

        // Simple Policy Configuration
        SimplePolicy::Config simple_config;
        simple_config.promotion_threshold =
            5.0;  // Default to 5.0 for tests (matches previous hardcoded value)
        if (config.isMember("scheduler")) {
            const auto& sched_conf = config["scheduler"];
            if (sched_conf.isMember("promotion_threshold"))
                simple_config.promotion_threshold =
                    sched_conf["promotion_threshold"].asDouble();
        }

        auto simple_policy = std::make_unique<SimplePolicy>(simple_config);
        policy_ = std::move(simple_policy);
        LOG(INFO) << "ClientScheduler initialized with Simple Policy";
    }
}

ClientScheduler::~ClientScheduler() { Stop(); }

void ClientScheduler::RegisterTier(CacheTier* tier) {
    tiers_[tier->GetTierId()] = tier;

    // Auto-Configuration: If tier is DRAM, set it as Fast Tier
    if (tier->GetMemoryType() == MemoryType::DRAM) {
        if (policy_) {
            fast_tier_id_ = tier->GetTierId();
            policy_->SetFastTier(tier->GetTierId());
            LOG(INFO) << "Set Fast Tier to " << tier->GetTierId();
        }
    }
}

void ClientScheduler::Start() {
    if (running_) return;
    running_ = true;
    worker_thread_ = std::thread(&ClientScheduler::WorkerLoop, this);
}

void ClientScheduler::Stop() {
    running_ = false;
    cv_.notify_all();  // Wake the worker thread immediately.
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void ClientScheduler::OnAccess(const std::string& key) {
    if (stats_collector_) {
        stats_collector_->RecordAccess(key);
    }
}

void ClientScheduler::OnCommit(const std::string& key, UUID tier_id,
                               size_t size_bytes) {
    std::lock_guard<std::mutex> lock(key_cache_mutex_);
    TrackReplicaLocked(key, tier_id, size_bytes);
}

void ClientScheduler::OnDelete(const std::string& key,
                               std::optional<UUID> tier_id) {
    bool remove_stats = false;
    {
        std::lock_guard<std::mutex> lock(key_cache_mutex_);
        RemoveReplicaLocked(key, tier_id);
        remove_stats = !tier_id.has_value() || key_cache_.count(key) == 0;
    }

    if (remove_stats && stats_collector_) {
        stats_collector_->RemoveKey(key);
    }
}

bool ClientScheduler::OnAllocationFailure(UUID tier_id) {
    if (eviction_mode_ == EvictionMode::SYNC) {
        LOG(INFO) << "Allocation failed on tier " << tier_id
                  << ", triggering SYNC eviction";
        TriggerSyncEviction(tier_id);
        return true;
    } else {
        VLOG(2) << "Allocation failed on tier " << tier_id
                << ", ASYNC mode - will handle in next cycle";
        return false;
    }
}

void ClientScheduler::WorkerLoop() {
    while (running_) {
        // Use condition_variable so Stop() can wake us immediately.
        {
            std::unique_lock<std::mutex> lk(cv_mutex_);
            cv_.wait_for(lk, std::chrono::milliseconds(loop_interval_ms_),
                         [this] { return !running_.load(); });
        }
        if (!running_) break;

        // 1. Collect Stats
        auto access_stats = stats_collector_->GetSnapshot();

        // 2. Build Policy Context
        auto active_keys = BuildActiveKeys(access_stats, fast_tier_id_);
        if (active_keys.empty()) continue;

        auto tier_stats_map = CollectTierStats();

        // 3. Make Decision
        auto actions = policy_->Decide(tier_stats_map, active_keys);

        // 4. Execute Actions
        if (!actions.empty()) {
            ExecuteActions(actions);
        }
    }
}

std::unordered_map<UUID, TierStats> ClientScheduler::CollectTierStats() const {
    std::unordered_map<UUID, TierStats> tier_stats_map;
    for (const auto& [id, tier] : tiers_) {
        tier_stats_map[id] = {tier->GetCapacity(), tier->GetUsage()};
    }
    return tier_stats_map;
}

std::vector<KeyContext> ClientScheduler::BuildActiveKeys(
    const AccessStats& access_stats, std::optional<UUID> pinned_tier_id) {
    std::vector<KeyContext> active_keys;
    std::unordered_set<std::string> seen_keys;

    {
        std::lock_guard<std::mutex> lock(key_cache_mutex_);

        size_t reserved_size = access_stats.hot_keys.size();
        if (pinned_tier_id.has_value()) {
            auto resident_it = tier_resident_keys_.find(pinned_tier_id.value());
            if (resident_it != tier_resident_keys_.end()) {
                reserved_size += resident_it->second.size();
            }
        }

        active_keys.reserve(reserved_size);
        seen_keys.reserve(reserved_size);

        for (const auto& stat_entry : access_stats.hot_keys) {
            auto cache_it = key_cache_.find(stat_entry.key);
            if (cache_it == key_cache_.end() ||
                cache_it->second.current_locations.empty()) {
                continue;
            }

            KeyContext key_ctx;
            key_ctx.key = stat_entry.key;
            key_ctx.current_locations = cache_it->second.current_locations;
            key_ctx.size_bytes = cache_it->second.size_bytes;

            if (access_stats.metric == AccessStatMetric::kRecentHeat) {
                key_ctx.recent_heat_score = stat_entry.recent_heat_score;
            } else if (access_stats.metric == AccessStatMetric::kRecencyRank) {
                key_ctx.recency_rank = stat_entry.recency_rank;
            }

            seen_keys.insert(stat_entry.key);
            active_keys.push_back(std::move(key_ctx));
        }

        if (!pinned_tier_id.has_value()) {
            return active_keys;
        }

        auto resident_it = tier_resident_keys_.find(pinned_tier_id.value());
        if (resident_it == tier_resident_keys_.end()) {
            return active_keys;
        }

        for (const auto& key : resident_it->second) {
            if (seen_keys.count(key) > 0) {
                continue;
            }

            auto cache_it = key_cache_.find(key);
            if (cache_it == key_cache_.end() ||
                cache_it->second.current_locations.empty()) {
                continue;
            }

            active_keys.push_back(KeyContext{
                key,
                0.0,
                0,
                cache_it->second.current_locations,
                cache_it->second.size_bytes,
            });
        }
    }

    return active_keys;
}

void ClientScheduler::TrackReplicaLocked(const std::string& key, UUID tier_id,
                                         size_t size_bytes) {
    auto& state = key_cache_[key];
    state.size_bytes = size_bytes;

    const auto existing_it = std::find(state.current_locations.begin(),
                                       state.current_locations.end(), tier_id);
    if (existing_it == state.current_locations.end()) {
        state.current_locations.push_back(tier_id);
    }

    tier_resident_keys_[tier_id].insert(key);
}

void ClientScheduler::RemoveReplicaLocked(const std::string& key,
                                          std::optional<UUID> tier_id) {
    auto cache_it = key_cache_.find(key);
    if (cache_it == key_cache_.end()) {
        return;
    }

    if (!tier_id.has_value()) {
        for (const auto& resident_tier : cache_it->second.current_locations) {
            auto resident_it = tier_resident_keys_.find(resident_tier);
            if (resident_it != tier_resident_keys_.end()) {
                resident_it->second.erase(key);
            }
        }
        key_cache_.erase(cache_it);
        return;
    }

    auto& current_locations = cache_it->second.current_locations;
    current_locations.erase(
        std::remove(current_locations.begin(), current_locations.end(),
                    tier_id.value()),
        current_locations.end());

    auto resident_it = tier_resident_keys_.find(tier_id.value());
    if (resident_it != tier_resident_keys_.end()) {
        resident_it->second.erase(key);
    }

    if (current_locations.empty()) {
        key_cache_.erase(cache_it);
    }
}

void ClientScheduler::ExecuteActions(const std::vector<SchedAction>& actions) {
    // Execute in two phases: EVICT first, then MIGRATE
    // This ensures space is freed before attempting promotions

    // Phase 1: Execute all EVICT actions
    for (const auto& action : actions) {
        if (!running_) return;  // Fast exit on shutdown
        if (action.type == SchedAction::Type::EVICT) {
            if (!action.source_tier_id.has_value()) continue;

            // Execute Eviction (Delete from specific tier)
            auto res =
                backend_->Delete(action.key, action.source_tier_id.value());
            if (!res) {
                LOG(ERROR) << "Eviction failed for key: " << action.key
                           << ", error: " << res.error();
            } else {
                VLOG(1) << "Evicted key: " << action.key << " from tier "
                        << action.source_tier_id.value();
            }
        }
    }

    // Phase 2: Execute all MIGRATE actions
    std::unordered_set<UUID> tiers_needing_eviction;

    for (const auto& action : actions) {
        if (!running_) return;  // Fast exit on shutdown
        if (action.type == SchedAction::Type::MIGRATE) {
            // Check validity
            if (!action.source_tier_id.has_value() ||
                !action.target_tier_id.has_value()) {
                continue;
            }

            // Execute Transfer (Migration/Promotion)
            auto res =
                backend_->Transfer(action.key, action.source_tier_id.value(),
                                   action.target_tier_id.value());

            if (!res) {
                // Log error
                if (res.error() == ErrorCode::CAS_FAILED) {
                    LOG(INFO) << "Transfer aborted due to concurrent "
                                 "modification (CAS Failed) for key: "
                              << action.key;
                } else if (res.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                    // Insufficient space - mark tier for eviction
                    VLOG(2) << "Transfer skipped due to insufficient space for "
                               "key: "
                            << action.key << ", will trigger eviction";
                    tiers_needing_eviction.insert(
                        action.target_tier_id.value());
                } else {
                    LOG(ERROR) << "Transfer failed for key: " << action.key
                               << ", error: " << res.error();
                }
                // For MVP: ignore
            } else {
                // Transfer successful, delete from source (Move semantics)
                auto del_res =
                    backend_->Delete(action.key, action.source_tier_id.value());
                if (!del_res) {
                    // Log warning: Failed to clean up source
                }
            }
        }
    }

    // Phase 3: If any tier ran out of space, handle based on eviction mode
    if (!tiers_needing_eviction.empty()) {
        if (eviction_mode_ == EvictionMode::SYNC) {
            // Sync mode: trigger immediate eviction
            VLOG(1) << "Triggering SYNC eviction for "
                    << tiers_needing_eviction.size() << " tier(s)";
            for (const auto& tier_id : tiers_needing_eviction) {
                TriggerSyncEviction(tier_id);
            }
        } else {
            // Async mode: rely on next scheduling cycle
            VLOG(1) << "ASYNC eviction mode: will handle in next cycle for "
                    << tiers_needing_eviction.size() << " tier(s)";
        }
    }
}

void ClientScheduler::TriggerSyncEviction(UUID tier_id) {
    // Collect current stats
    auto tier_stats = CollectTierStats();

    // Get active keys from stats collector
    auto access_stats = stats_collector_->GetSnapshot();
    auto active_keys = BuildActiveKeys(access_stats, tier_id);
    if (active_keys.empty()) {
        LOG(WARNING) << "No active keys for sync eviction";
        return;
    }

    // Force policy to generate eviction actions
    auto evict_actions = policy_->Decide(tier_stats, active_keys);

    // Filter to EVICT or MIGRATE-away actions for the target tier
    std::vector<SchedAction> filtered_actions;
    for (const auto& action : evict_actions) {
        bool is_eviction_from_target = false;

        if (action.type == SchedAction::Type::EVICT &&
            action.source_tier_id.has_value() &&
            action.source_tier_id.value() == tier_id) {
            is_eviction_from_target = true;
        } else if (action.type == SchedAction::Type::MIGRATE &&
                   action.source_tier_id.has_value() &&
                   action.source_tier_id.value() == tier_id &&
                   action.target_tier_id.has_value() &&
                   action.target_tier_id.value() != tier_id) {
            // MIGRATE away from target tier also frees space
            is_eviction_from_target = true;
        }

        if (is_eviction_from_target) {
            filtered_actions.push_back(action);
        }
    }

    if (filtered_actions.empty()) {
        LOG(WARNING) << "No eviction candidates found for tier " << tier_id;
        return;
    }

    // Execute evictions
    for (const auto& action : filtered_actions) {
        if (action.type == SchedAction::Type::EVICT) {
            // Direct eviction
            auto del_res = backend_->Delete(action.key, tier_id);
            if (!del_res) {
                LOG(WARNING) << "Failed to evict key: " << action.key;
            }
        } else if (action.type == SchedAction::Type::MIGRATE) {
            // Migrate to another tier (also frees space)
            auto transfer_res =
                backend_->Transfer(action.key, action.source_tier_id.value(),
                                   action.target_tier_id.value());
            if (transfer_res) {
                // Transfer succeeded - delete from source to free space
                auto del_res =
                    backend_->Delete(action.key, action.source_tier_id.value());
                if (!del_res) {
                    LOG(WARNING) << "Failed to delete source after migration: "
                                 << action.key;
                }
            } else {
                LOG(WARNING) << "Failed to migrate key: " << action.key;
            }
        }
    }
}

}  // namespace mooncake
