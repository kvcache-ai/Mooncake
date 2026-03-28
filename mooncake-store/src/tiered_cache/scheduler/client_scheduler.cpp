#include "tiered_cache/scheduler/client_scheduler.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/tiers/cache_tier.h"
#include "tiered_cache/scheduler/lru_policy.h"
#include "tiered_cache/scheduler/lru_stats_collector.h"
#include "tiered_cache/scheduler/simple_policy.h"
#include <algorithm>
#include <chrono>
#include <functional>
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
    auto& shard = GetKeyCacheShard(key);
    MutexLocker lock(&shard.mutex);
    TrackReplicaLocked(shard, key, tier_id, size_bytes);
}

void ClientScheduler::OnDelete(const std::string& key,
                               std::optional<UUID> tier_id) {
    bool remove_stats = false;
    {
        auto& shard = GetKeyCacheShard(key);
        MutexLocker lock(&shard.mutex);
        remove_stats = RemoveReplicaLocked(shard, key, tier_id);
    }

    if (remove_stats && stats_collector_) {
        stats_collector_->RemoveKey(key);
    }
}

bool ClientScheduler::OnAllocationFailure(UUID tier_id, size_t required_bytes) {
    if (TryFastReclaim(tier_id, required_bytes)) {
        LOG(INFO) << "Allocation failed on tier " << tier_id
                  << ", reclaimed pre-replicated cold replicas";
        return true;
    }

    if (eviction_mode_ == EvictionMode::SYNC) {
        LOG(INFO) << "Allocation failed on tier " << tier_id
                  << ", triggering SYNC eviction";
        return TriggerSyncEviction(tier_id, required_bytes);
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
        if (!policy_) {
            LOG(ERROR) << "ClientScheduler worker has no policy configured";
            continue;
        }

        auto decision = policy_->Decide(tier_stats_map, active_keys);
        if (!decision) {
            LOG(ERROR) << "Scheduler policy decide failed, error: "
                       << decision.error();
            continue;
        }
        const auto& actions = decision.value();

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
    const size_t reserved_size =
        EstimateActiveKeyReserve(access_stats, pinned_tier_id);
    std::vector<KeyContext> active_keys;
    std::unordered_set<std::string> seen_keys;
    active_keys.reserve(reserved_size);
    seen_keys.reserve(reserved_size);

    AppendHotKeys(access_stats, active_keys, seen_keys);
    if (pinned_tier_id.has_value()) {
        AppendPinnedTierKeys(pinned_tier_id.value(), active_keys, seen_keys);
    }
    return active_keys;
}

size_t ClientScheduler::KeyCacheShardIndex(std::string_view key) {
    return std::hash<std::string_view>{}(key) % kKeyCacheShardCount;
}

ClientScheduler::KeyCacheShard& ClientScheduler::GetKeyCacheShard(
    std::string_view key) {
    return key_cache_shards_[KeyCacheShardIndex(key)];
}

const ClientScheduler::KeyCacheShard& ClientScheduler::GetKeyCacheShard(
    std::string_view key) const {
    return key_cache_shards_[KeyCacheShardIndex(key)];
}

size_t ClientScheduler::EstimateActiveKeyReserve(
    const AccessStats& access_stats, std::optional<UUID> pinned_tier_id) const {
    size_t reserved_size = access_stats.hot_keys.size();
    if (!pinned_tier_id.has_value()) {
        return reserved_size;
    }

    for (const auto& shard : key_cache_shards_) {
        MutexLocker lock(&shard.mutex);
        auto resident_it =
            shard.tier_resident_keys.find(pinned_tier_id.value());
        if (resident_it != shard.tier_resident_keys.end()) {
            reserved_size += resident_it->second.size();
        }
    }

    return reserved_size;
}

void ClientScheduler::AppendHotKeys(
    const AccessStats& access_stats, std::vector<KeyContext>& active_keys,
    std::unordered_set<std::string>& seen_keys) const {
    for (const auto& stat_entry : access_stats.hot_keys) {
        const auto& shard = GetKeyCacheShard(stat_entry.key);
        MutexLocker lock(&shard.mutex);
        auto cache_it = shard.key_cache.find(stat_entry.key);
        if (cache_it == shard.key_cache.end()) {
            continue;
        }

        auto key_ctx = BuildKeyContextLocked(stat_entry.key, cache_it->second,
                                             access_stats, &stat_entry);
        if (!key_ctx.has_value()) {
            continue;
        }

        seen_keys.insert(stat_entry.key);
        active_keys.push_back(std::move(key_ctx.value()));
    }
}

void ClientScheduler::AppendPinnedTierKeys(
    UUID pinned_tier_id, std::vector<KeyContext>& active_keys,
    std::unordered_set<std::string>& seen_keys) const {
    const AccessStats empty_stats{};
    for (const auto& shard : key_cache_shards_) {
        MutexLocker lock(&shard.mutex);
        auto resident_it = shard.tier_resident_keys.find(pinned_tier_id);
        if (resident_it == shard.tier_resident_keys.end()) {
            continue;
        }

        for (const auto& key : resident_it->second) {
            if (seen_keys.count(key) > 0) {
                continue;
            }

            auto cache_it = shard.key_cache.find(key);
            if (cache_it == shard.key_cache.end()) {
                continue;
            }

            auto key_ctx =
                BuildKeyContextLocked(key, cache_it->second, empty_stats);
            if (!key_ctx.has_value()) {
                continue;
            }

            seen_keys.insert(key);
            active_keys.push_back(std::move(key_ctx.value()));
        }
    }
}

std::optional<KeyContext> ClientScheduler::BuildKeyContextLocked(
    const std::string& key, const CachedKeyState& state,
    const AccessStats& access_stats, const AccessStatEntry* stat_entry) const {
    if (state.current_locations.empty()) {
        return std::nullopt;
    }

    KeyContext key_ctx;
    key_ctx.key = key;
    key_ctx.current_locations = state.current_locations;
    key_ctx.size_bytes = state.size_bytes;
    if (stat_entry != nullptr) {
        if (access_stats.metric == AccessStatMetric::kRecentHeat) {
            key_ctx.recent_heat_score = stat_entry->recent_heat_score;
        } else if (access_stats.metric == AccessStatMetric::kRecencyRank) {
            key_ctx.recency_rank = stat_entry->recency_rank;
        }
    }
    return key_ctx;
}

size_t ClientScheduler::GetCachedKeySize(const std::string& key) const {
    const auto& shard = GetKeyCacheShard(key);
    MutexLocker lock(&shard.mutex);
    auto key_it = shard.key_cache.find(key);
    if (key_it == shard.key_cache.end() || key_it->second.size_bytes == 0) {
        return 1;
    }
    return key_it->second.size_bytes;
}

void ClientScheduler::TrackReplicaLocked(KeyCacheShard& shard,
                                         const std::string& key, UUID tier_id,
                                         size_t size_bytes) {
    auto& state = shard.key_cache[key];
    state.size_bytes = size_bytes;

    const auto existing_it = std::find(state.current_locations.begin(),
                                       state.current_locations.end(), tier_id);
    if (existing_it == state.current_locations.end()) {
        state.current_locations.push_back(tier_id);
    }

    shard.tier_resident_keys[tier_id].insert(key);
}

bool ClientScheduler::RemoveReplicaLocked(KeyCacheShard& shard,
                                          const std::string& key,
                                          std::optional<UUID> tier_id) {
    auto cache_it = shard.key_cache.find(key);
    if (cache_it == shard.key_cache.end()) {
        return true;
    }

    if (!tier_id.has_value()) {
        for (const auto& resident_tier : cache_it->second.current_locations) {
            auto resident_it = shard.tier_resident_keys.find(resident_tier);
            if (resident_it != shard.tier_resident_keys.end()) {
                resident_it->second.erase(key);
            }
        }
        shard.key_cache.erase(cache_it);
        return true;
    }

    auto& current_locations = cache_it->second.current_locations;
    current_locations.erase(
        std::remove(current_locations.begin(), current_locations.end(),
                    tier_id.value()),
        current_locations.end());

    auto resident_it = shard.tier_resident_keys.find(tier_id.value());
    if (resident_it != shard.tier_resident_keys.end()) {
        resident_it->second.erase(key);
    }

    if (current_locations.empty()) {
        shard.key_cache.erase(cache_it);
        return true;
    }
    return false;
}

void ClientScheduler::ExecuteActions(const std::vector<SchedAction>& actions) {
    // Execute in three phases: EVICT first, then MIGRATE, then REPLICATE.
    // This keeps the fast path biased towards freeing space before background
    // copy work.

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
    std::unordered_map<UUID, size_t> tiers_needing_eviction;

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
                                   action.target_tier_id.value(), false);

            if (!res) {
                // Log error
                if (res.error() == ErrorCode::CAS_FAILED) {
                    LOG(INFO) << "Transfer aborted due to concurrent "
                                 "modification (CAS Failed) for key: "
                              << action.key;
                } else if (res.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                    // Insufficient space - mark tier for eviction
                    const size_t key_size = GetCachedKeySize(action.key);
                    VLOG(2) << "Transfer skipped due to insufficient space for "
                               "key: "
                            << action.key << ", will trigger eviction";
                    auto& required_bytes =
                        tiers_needing_eviction[action.target_tier_id.value()];
                    required_bytes = std::max(required_bytes, key_size);
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
            for (const auto& [tier_id, required_bytes] :
                 tiers_needing_eviction) {
                TriggerSyncEviction(tier_id, required_bytes);
            }
        } else {
            // Async mode: rely on next scheduling cycle
            VLOG(1) << "ASYNC eviction mode: will handle in next cycle for "
                    << tiers_needing_eviction.size() << " tier(s)";
        }
    }

    // Phase 4: Prepare cold replicas in lower tiers without deleting the fast
    // copy. This builds a reclaimable window before the tier reaches the high
    // watermark.
    for (const auto& action : actions) {
        if (action.type != SchedAction::Type::REPLICATE ||
            !action.source_tier_id.has_value() ||
            !action.target_tier_id.has_value()) {
            continue;
        }

        uint64_t start_version = 0;
        auto source_handle = backend_->Get(action.key, action.source_tier_id,
                                           false, &start_version);
        if (!source_handle) {
            if (source_handle.error() != ErrorCode::INVALID_KEY &&
                source_handle.error() != ErrorCode::TIER_NOT_FOUND) {
                LOG(ERROR) << "Failed to prepare replica for key: "
                           << action.key
                           << ", error: " << source_handle.error();
            }
            continue;
        }

        auto copy_res = backend_->CopyData(
            action.key, source_handle.value()->loc.data,
            action.target_tier_id.value(), start_version, false);
        if (!copy_res) {
            if (copy_res.error() == ErrorCode::CAS_FAILED ||
                copy_res.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                VLOG(2) << "Replica preparation skipped for key: " << action.key
                        << ", error: " << copy_res.error();
            } else {
                LOG(ERROR) << "Replica preparation failed for key: "
                           << action.key << ", error: " << copy_res.error();
            }
            continue;
        }

        VLOG(1) << "Prepared reclaimable replica for key: " << action.key
                << " from tier " << action.source_tier_id.value() << " to tier "
                << action.target_tier_id.value();
    }
}

bool ClientScheduler::TriggerSyncEviction(UUID tier_id, size_t required_bytes) {
    auto tier_stats = CollectTierStats();
    auto access_stats = stats_collector_->GetSnapshot();
    auto active_keys = BuildActiveKeys(access_stats, tier_id);
    auto plan = BuildReclaimPlan(tier_id, tier_stats, active_keys, false,
                                 required_bytes);

    if (plan.steps.empty()) {
        LOG(WARNING) << "No sync reclaim candidates found for tier " << tier_id;
        return false;
    }

    const size_t reclaimed_bytes = ExecuteReclaimPlan(plan);
    const bool enough_space = HasAvailableBytes(tier_id, required_bytes);
    if (!enough_space) {
        VLOG(1) << "Sync reclaim on tier " << tier_id << " freed "
                << reclaimed_bytes << " bytes, still short for "
                << required_bytes << " bytes";
    }
    return reclaimed_bytes >= plan.target_reclaim_bytes && enough_space;
}

bool ClientScheduler::TryFastReclaim(UUID tier_id, size_t required_bytes) {
    auto tier_stats = CollectTierStats();
    auto access_stats = stats_collector_->GetSnapshot();
    auto active_keys = BuildActiveKeys(access_stats, tier_id);
    auto plan = BuildReclaimPlan(tier_id, tier_stats, active_keys, true,
                                 required_bytes);
    if (plan.steps.empty()) {
        return false;
    }

    const size_t reclaimed_bytes = ExecuteReclaimPlan(plan);
    const bool enough_space = HasAvailableBytes(tier_id, required_bytes);
    return reclaimed_bytes >= plan.target_reclaim_bytes && enough_space;
}

ClientScheduler::PlannedReclaim ClientScheduler::BuildReclaimPlan(
    UUID tier_id, const std::unordered_map<UUID, TierStats>& tier_stats,
    const std::vector<KeyContext>& active_keys, bool require_existing_replica,
    size_t required_bytes) const {
    PlannedReclaim plan;

    auto tier_it = tier_stats.find(tier_id);
    if (tier_it == tier_stats.end()) {
        return plan;
    }

    const size_t total_capacity = tier_it->second.total_capacity_bytes;
    const size_t used_capacity = tier_it->second.used_capacity_bytes;
    const size_t available_bytes =
        (total_capacity > used_capacity) ? (total_capacity - used_capacity) : 0;
    const size_t reclaim_needed = (required_bytes > available_bytes)
                                      ? (required_bytes - available_bytes)
                                      : 1;

    plan.target_reclaim_bytes = reclaim_needed;
    plan.steps.reserve(active_keys.size());

    const auto demotion_tier_id =
        require_existing_replica ? std::nullopt : SelectDemotionTier(tier_id);

    size_t planned_reclaim_bytes = 0;
    for (auto it = active_keys.rbegin(); it != active_keys.rend(); ++it) {
        const auto tier_pos = std::find(it->current_locations.begin(),
                                        it->current_locations.end(), tier_id);
        if (tier_pos == it->current_locations.end()) {
            continue;
        }

        PlannedReclaim::Step step;
        step.action.key = it->key;
        step.action.source_tier_id = tier_id;
        step.size_bytes = it->size_bytes;

        const bool has_other_replica = it->current_locations.size() > 1;
        if (require_existing_replica) {
            if (!has_other_replica) {
                continue;
            }
            step.action.type = SchedAction::Type::EVICT;
        } else if (has_other_replica) {
            step.action.type = SchedAction::Type::EVICT;
        } else if (demotion_tier_id.has_value()) {
            step.action.type = SchedAction::Type::MIGRATE;
            step.action.target_tier_id = demotion_tier_id.value();
        } else {
            step.action.type = SchedAction::Type::EVICT;
        }

        plan.steps.push_back(std::move(step));
        planned_reclaim_bytes += it->size_bytes;
        if (planned_reclaim_bytes >= plan.target_reclaim_bytes) {
            break;
        }
    }

    return plan;
}

size_t ClientScheduler::ExecuteReclaimPlan(const PlannedReclaim& plan) {
    size_t reclaimed_bytes = 0;

    for (const auto& step : plan.steps) {
        const auto& action = step.action;
        if (!action.source_tier_id.has_value()) {
            continue;
        }

        if (action.type == SchedAction::Type::EVICT) {
            auto delete_res =
                backend_->Delete(action.key, action.source_tier_id.value());
            if (!delete_res) {
                continue;
            }
        } else if (action.type == SchedAction::Type::MIGRATE) {
            if (!action.target_tier_id.has_value()) {
                continue;
            }

            auto transfer_res =
                backend_->Transfer(action.key, action.source_tier_id.value(),
                                   action.target_tier_id.value(), false);
            if (!transfer_res) {
                continue;
            }

            auto delete_res =
                backend_->Delete(action.key, action.source_tier_id.value());
            if (!delete_res) {
                continue;
            }
        } else {
            continue;
        }

        reclaimed_bytes += step.size_bytes;
        if (reclaimed_bytes >= plan.target_reclaim_bytes) {
            break;
        }
    }

    return reclaimed_bytes;
}

bool ClientScheduler::HasAvailableBytes(UUID tier_id,
                                        size_t required_bytes) const {
    auto tier_it = tiers_.find(tier_id);
    if (tier_it == tiers_.end() || !tier_it->second) {
        return false;
    }

    const size_t capacity = tier_it->second->GetCapacity();
    const size_t usage = tier_it->second->GetUsage();
    const size_t available_bytes = (capacity > usage) ? (capacity - usage) : 0;
    return available_bytes >= required_bytes;
}

std::optional<UUID> ClientScheduler::SelectDemotionTier(
    UUID source_tier_id) const {
    const auto tier_views = backend_->GetTierViews();
    const TierView* source_view = nullptr;
    for (const auto& tier_view : tier_views) {
        if (tier_view.id == source_tier_id) {
            source_view = &tier_view;
            break;
        }
    }
    if (!source_view) {
        return std::nullopt;
    }

    const TierView* best_lower_priority = nullptr;
    const TierView* best_fallback = nullptr;
    for (const auto& tier_view : tier_views) {
        if (tier_view.id == source_tier_id) {
            continue;
        }

        if (!best_fallback || tier_view.priority > best_fallback->priority) {
            best_fallback = &tier_view;
        }

        if (tier_view.priority >= source_view->priority) {
            continue;
        }

        if (!best_lower_priority ||
            tier_view.priority > best_lower_priority->priority) {
            best_lower_priority = &tier_view;
        }
    }

    if (best_lower_priority) {
        return best_lower_priority->id;
    }
    if (best_fallback) {
        return best_fallback->id;
    }
    return std::nullopt;
}

}  // namespace mooncake
