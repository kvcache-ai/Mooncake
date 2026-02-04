#pragma once

#include "tiered_cache/scheduler/scheduler_policy.h"
#include <algorithm>
#include <iostream>

namespace mooncake {

/**
 * @class SimplePolicy
 * @brief MVP Policy: Promote hot data to a designated "Fast Tier"
 */
class SimplePolicy : public SchedulerPolicy {
   public:
    struct Config {
        double promotion_threshold =
            10.0;  // Min access count/score to trigger promotion
    };

    explicit SimplePolicy(Config config) : config_(config) {}

    // Configure which tier is considered "Fast" (Target for promotion)
    void SetFastTier(UUID id) { fast_tier_id_ = id; }

    std::vector<SchedAction> Decide(
        const std::unordered_map<UUID, TierStats>& tier_stats,
        const std::vector<KeyContext>& active_keys) override {
        std::vector<SchedAction> actions;

        if (!fast_tier_id_.has_value()) {
            return actions;  // No target to promote to
        }

        UUID target_id = fast_tier_id_.value();

        // Check if target tier has space (Simple check, ignore fragmentation
        // for MVP) In a real policy, we might trigger Eviction if full.
        auto it = tier_stats.find(target_id);
        if (it == tier_stats.end()) {
            return actions;  // Target tier stats missing
        }

        // MVP Logic: Iterate active keys and promote if hot enough and not
        // already in target
        for (const auto& key_ctx : active_keys) {
            if (key_ctx.heat_score < config_.promotion_threshold) {
                continue;
            }

            // Check if already in target tier
            bool already_in_target = false;
            for (const auto& loc : key_ctx.current_locations) {
                if (loc == target_id) {
                    already_in_target = true;
                    break;
                }
            }

            if (already_in_target) {
                continue;
            }

            // Generate Promotion Action
            // We need a source tier. Ideally pick the slowest one.
            // For MVP, if it has locations, pick the first one as source.
            if (!key_ctx.current_locations.empty()) {
                SchedAction action;
                action.type = SchedAction::Type::MIGRATE;
                action.key = key_ctx.key;
                action.source_tier_id = key_ctx.current_locations[0];
                action.target_tier_id = target_id;
                actions.push_back(action);
            }
        }

        return actions;
    }

   private:
    Config config_;
    std::optional<UUID> fast_tier_id_;
};

}  // namespace mooncake
