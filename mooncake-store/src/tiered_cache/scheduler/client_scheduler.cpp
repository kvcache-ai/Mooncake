#include "tiered_cache/scheduler/client_scheduler.h"
#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/cache_tier.h"
#include "tiered_cache/scheduler/simple_policy.h"
#include <chrono>

namespace mooncake {

ClientScheduler::ClientScheduler(TieredBackend* backend) : backend_(backend) {
    // Initialize default components MVP
    stats_collector_ = std::make_unique<SimpleStatsCollector>();

    // Default simple policy
    SimplePolicy::Config config;
    config.promotion_threshold = 5.0;  // Low threshold for easier testing
    auto simple_policy = std::make_unique<SimplePolicy>(config);
    // Note: Fast Tier needs to be set after tiers are registered or configured
    policy_ = std::move(simple_policy);
}

ClientScheduler::~ClientScheduler() { Stop(); }

void ClientScheduler::RegisterTier(CacheTier* tier) {
    tiers_[tier->GetTierId()] = tier;

    // MVP Auto-Configuration: If tier is DRAM, set it as Fast Tier for
    // SimplePolicy
    if (tier->GetMemoryType() == MemoryType::DRAM) {
        if (auto* p = dynamic_cast<SimplePolicy*>(policy_.get())) {
            p->SetFastTier(tier->GetTierId());
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
            ctx.current_locations = backend_->GetReplicaTierIds(key);
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
}

}  // namespace mooncake
