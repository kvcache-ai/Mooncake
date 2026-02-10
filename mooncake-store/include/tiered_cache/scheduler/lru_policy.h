#pragma once

#include <algorithm>
#include <unordered_set>
#include <vector>
#include "tiered_cache/scheduler/scheduler_policy.h"

namespace mooncake {

/**
 * @class LRUPolicy
 * @brief Heat-based promotion/eviction policy with watermark control.
 *
 * All keys are sorted by heat score. The hottest keys (up to target capacity)
 * should be in fast tier, the rest in slow tier.
 */
class LRUPolicy : public SchedulerPolicy {
   public:
    struct Config {
        double high_watermark = 0.90;  // Trigger scheduling when above this
        double low_watermark = 0.70;   // Target usage after scheduling
    };

    explicit LRUPolicy(Config config) : config_(config) {}

    void SetFastTier(UUID id) { fast_tier_id_ = id; }

    std::vector<SchedAction> Decide(
        const std::unordered_map<UUID, TierStats>& tier_stats,
        const std::vector<KeyContext>& active_keys) override {
        std::vector<SchedAction> actions;

        if (!fast_tier_id_.has_value() || active_keys.empty()) {
            return actions;
        }

        UUID fast_id = fast_tier_id_.value();

        auto it = tier_stats.find(fast_id);
        if (it == tier_stats.end()) {
            return actions;
        }

        const TierStats& stats = it->second;
        if (stats.total_capacity_bytes == 0) {
            return actions;
        }

        double usage_ratio = static_cast<double>(stats.used_capacity_bytes) /
                             static_cast<double>(stats.total_capacity_bytes);

        // Only trigger scheduling when usage > high_watermark or < low_watermark
        if (usage_ratio <= config_.high_watermark &&
            usage_ratio >= config_.low_watermark) {
            return actions;
        }

        // Find slow tier ID for migrations
        std::optional<UUID> slow_id;
        for (const auto& [tid, tstats] : tier_stats) {
            if (tid != fast_id) {
                slow_id = tid;
                break;
            }
        }

        // Target: fill fast tier to low_watermark with hottest keys
        size_t target_usage = static_cast<size_t>(
            stats.total_capacity_bytes * config_.low_watermark);

        // Step 1: Determine which keys SHOULD be in fast tier
        // active_keys is assumed sorted by heat (hottest first)
        std::vector<const KeyContext*> should_be_in_fast;
        size_t accumulated_size = 0;

        for (const auto& key_ctx : active_keys) {
            if (accumulated_size + key_ctx.size_bytes <= target_usage) {
                should_be_in_fast.push_back(&key_ctx);
                accumulated_size += key_ctx.size_bytes;
            }
        }

        // Step 2: Compare with current state and generate actions
        // Build set of keys that should be in fast tier
        std::unordered_set<std::string> should_be_in_fast_set;
        for (const auto* ctx : should_be_in_fast) {
            should_be_in_fast_set.insert(ctx->key);
        }

        // Find keys to evict (in fast tier but shouldn't be)
        // Find keys to promote (should be in fast tier but aren't)
        for (const auto& key_ctx : active_keys) {
            bool is_in_fast = false;
            for (auto loc : key_ctx.current_locations) {
                if (loc == fast_id) {
                    is_in_fast = true;
                    break;
                }
            }

            bool should_be = (should_be_in_fast_set.count(key_ctx.key) > 0);

            if (is_in_fast && !should_be) {
                // Evict: in fast tier but shouldn't be
                bool has_other_copy = (key_ctx.current_locations.size() > 1);
                SchedAction action;
                if (has_other_copy) {
                    action.type = SchedAction::Type::EVICT;
                    action.key = key_ctx.key;
                    action.source_tier_id = fast_id;
                } else if (slow_id.has_value()) {
                    action.type = SchedAction::Type::MIGRATE;
                    action.key = key_ctx.key;
                    action.source_tier_id = fast_id;
                    action.target_tier_id = slow_id.value();
                } else {
                    continue;
                }
                actions.push_back(action);
            } else if (!is_in_fast && should_be && !key_ctx.current_locations.empty()) {
                // Promote: should be in fast tier but isn't
                SchedAction action;
                action.type = SchedAction::Type::MIGRATE;
                action.key = key_ctx.key;
                action.source_tier_id = key_ctx.current_locations[0];
                action.target_tier_id = fast_id;
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
