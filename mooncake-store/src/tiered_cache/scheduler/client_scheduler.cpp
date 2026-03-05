#include "tiered_cache/scheduler/client_scheduler.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/tiers/cache_tier.h"
#include "tiered_cache/scheduler/lru_policy.h"
#include "tiered_cache/scheduler/lru_stats_collector.h"
#include "tiered_cache/scheduler/simple_policy.h"
#include <chrono>
#include <unordered_set>
#include <glog/logging.h>

namespace mooncake {

ClientScheduler::ClientScheduler(TieredBackend* backend,
                                 const Json::Value& config)
    : backend_(backend) {
    std::string policy_type = "SIMPLE";  // Default
    if (config.isMember("scheduler") &&
        config["scheduler"].isMember("policy")) {
        policy_type = config["scheduler"]["policy"].asString();
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
        stats_collector_ = std::make_unique<LRUStatsCollector>();

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
        stats_collector_ = std::make_unique<SimpleStatsCollector>();

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
        if (auto* p = dynamic_cast<LRUPolicy*>(policy_.get())) {
            p->SetFastTier(tier->GetTierId());
            LOG(INFO) << "Set Fast Tier (LRU) to " << tier->GetTierId();
        } else if (auto* p = dynamic_cast<SimplePolicy*>(policy_.get())) {
            p->SetFastTier(tier->GetTierId());
            LOG(INFO) << "Set Fast Tier (Simple) to " << tier->GetTierId();
        } else {
            LOG(ERROR) << "Failed to cast policy to set Fast Tier";
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
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void ClientScheduler::OnAccess(const std::string& key) {
    if (stats_collector_) {
        stats_collector_->RecordAccess(key);
    }
}

void ClientScheduler::OnDelete(const std::string& key) {
    if (stats_collector_) {
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
        std::this_thread::sleep_for(
            std::chrono::milliseconds(loop_interval_ms_));
        if (!running_) break;

        // 1. Collect Stats
        auto access_stats = stats_collector_->GetSnapshot();
        if (access_stats.hot_keys.empty()) continue;

        // 2. Build Policy Context
        std::vector<KeyContext> active_keys;
        active_keys.reserve(access_stats.hot_keys.size());

        for (const auto& [key, score] : access_stats.hot_keys) {
            KeyContext ctx;
            ctx.key = key;
            ctx.heat_score = score;

            // Check if key exists before attempting to get its handle
            ctx.current_locations = backend_->GetReplicaTierIds(key);
            if (ctx.current_locations.empty()) {
                // Key has been deleted, skip it
                continue;
            }

            // Get size information from the allocation handle
            auto handle = backend_->Get(key, std::nullopt, false);
            if (handle.has_value() && handle.value()->loc.data.buffer) {
                ctx.size_bytes = handle.value()->loc.data.buffer->size();
            }

            active_keys.push_back(std::move(ctx));
        }

        std::unordered_map<UUID, TierStats> tier_stats_map;
        for (const auto& [id, tier] : tiers_) {
            tier_stats_map[id] = {tier->GetCapacity(), tier->GetUsage()};
        }

        // 3. Make Decision
        auto actions = policy_->Decide(tier_stats_map, active_keys);

        // 4. Execute Actions
        if (!actions.empty()) {
            ExecuteActions(actions);
        }
    }
}

void ClientScheduler::ExecuteActions(const std::vector<SchedAction>& actions) {
    // Execute in two phases: EVICT first, then MIGRATE
    // This ensures space is freed before attempting promotions

    // Phase 1: Execute all EVICT actions
    for (const auto& action : actions) {
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
    std::unordered_map<UUID, TierStats> tier_stats;
    for (const auto& [tid, tier] : tiers_) {
        TierStats stats;
        stats.total_capacity_bytes = tier->GetCapacity();
        stats.used_capacity_bytes = tier->GetUsage();
        tier_stats[tid] = stats;
    }

    // Get active keys from stats collector
    auto access_stats = stats_collector_->GetSnapshot();

    if (access_stats.hot_keys.empty()) {
        LOG(WARNING) << "No active keys for sync eviction";
        return;
    }

    // Build KeyContext list
    std::vector<KeyContext> active_keys;
    active_keys.reserve(access_stats.hot_keys.size());

    for (const auto& [key, score] : access_stats.hot_keys) {
        KeyContext ctx;
        ctx.key = key;
        ctx.heat_score = score;

        ctx.current_locations = backend_->GetReplicaTierIds(key);
        if (ctx.current_locations.empty()) {
            continue;  // Key deleted
        }

        auto handle = backend_->Get(key, std::nullopt, false);
        if (handle.has_value() && handle.value()->loc.data.buffer) {
            ctx.size_bytes = handle.value()->loc.data.buffer->size();
        }

        active_keys.push_back(ctx);
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
