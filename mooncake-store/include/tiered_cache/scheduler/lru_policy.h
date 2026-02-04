#pragma once

#include <algorithm>
#include <iostream>
#include <vector>
#include "tiered_cache/scheduler/scheduler_policy.h"

namespace mooncake {

/**
 * @class LRUPolicy
 * @brief Promotes MRU keys and Evicts LRU keys based on Capacity Watermarks.
 */
class LRUPolicy : public SchedulerPolicy {
   public:
    struct Config {
        double high_watermark = 0.90;  // Trigger eviction at 90%
        double low_watermark = 0.80;   // Evict down to 80%
    };

    explicit LRUPolicy(Config config) : config_(config) {}

    // Configure which tier is considered "Fast" (Target for promotion / Source for eviction)
    void SetFastTier(UUID id) { fast_tier_id_ = id; }

    std::vector<SchedAction> Decide(
        const std::unordered_map<UUID, TierStats>& tier_stats,
        const std::vector<KeyContext>& active_keys) override {
        std::vector<SchedAction> actions;

        if (!fast_tier_id_.has_value()) {
            return actions;
        }

        UUID fast_id = fast_tier_id_.value();

        // 1. Check Fast Tier Status
        auto it = tier_stats.find(fast_id);
        if (it == tier_stats.end()) {
            return actions;
        }

        const TierStats& stats = it->second;
        double usage_ratio = 0.0;
        if (stats.total_capacity_bytes > 0) {
            usage_ratio = static_cast<double>(stats.used_capacity_bytes) /
                          static_cast<double>(stats.total_capacity_bytes);
        }

        // 2. Promotion Logic (MRU -> LRU)
        // Calculate available space for promotion (up to low_watermark to leave room)
        size_t promotion_budget = 0;
        size_t target_max_usage = static_cast<size_t>(
            stats.total_capacity_bytes * config_.low_watermark);
        if (stats.used_capacity_bytes < target_max_usage) {
            promotion_budget = target_max_usage - stats.used_capacity_bytes;
        }

        size_t promoted_bytes = 0;

        // Iterate active_keys (assumed ordered by MRU first)
        for (const auto& key_ctx : active_keys) {
            // Stop if we've exhausted the promotion budget
            if (promoted_bytes >= promotion_budget) {
                break;
            }

            bool in_fast = false;
            for (auto loc : key_ctx.current_locations) {
                if (loc == fast_id) in_fast = true;
            }

            if (!in_fast && !key_ctx.current_locations.empty()) {
                // Check if this key fits in remaining budget
                if (key_ctx.size_bytes > 0 &&
                    promoted_bytes + key_ctx.size_bytes > promotion_budget) {
                    continue;  // Skip this key, try smaller ones
                }

                // Not in Fast Tier. Promote it!
                SchedAction action;
                action.type = SchedAction::Type::MIGRATE;
                action.key = key_ctx.key;
                action.source_tier_id = key_ctx.current_locations[0]; // Pick first source
                action.target_tier_id = fast_id;
                actions.push_back(action);

                promoted_bytes += key_ctx.size_bytes;
            }
        }

        // 3. Eviction Logic (Capacity Control)
        if (usage_ratio > config_.high_watermark) {
            size_t target_usage = static_cast<size_t>(
                stats.total_capacity_bytes * config_.low_watermark);
            size_t bytes_to_free = 0;
            if (stats.used_capacity_bytes > target_usage) {
                bytes_to_free = stats.used_capacity_bytes - target_usage;
            }

            size_t freed_so_far = 0;

            // Iterate in REVERSE (LRU -> MRU) for eviction
            for (auto list_it = active_keys.rbegin(); list_it != active_keys.rend();
                 ++list_it) {
                if (freed_so_far >= bytes_to_free) break;

                const auto& key_ctx = *list_it;
                bool in_fast = false;
                for (auto loc : key_ctx.current_locations) {
                    if (loc == fast_id) in_fast = true;
                }

                if (in_fast) {
                    // Evict from Fast Tier
                    SchedAction action;
                    // Ideally we Move to Slow if available, or just Delete if no other tier?
                    // MVP: Look for another location. If exists, we can just delete Fast copy.
                    // If not exists, we MIGRATE to slow tier (if available).
                    // For simplicity in this tiered setup, let's assume we MIGRATE to "Slow" if possible.

                    // Find a non-fast tier
                    // Note: In current simple config, we don't have easy access to "Slow Tier ID" unless we stored it.
                    // But active_keys only tells us where it IS.
                    // If it's ONLY in Fast Tier, we must Move (Migrate) it out.
                    // If it's in Both, we Just Delete Fast copy.

                    bool has_other_copy = (key_ctx.current_locations.size() > 1);

                    if (has_other_copy) {
                        // Already elsewhere, safe to EVICT (Delete) from Fast
                        action.type = SchedAction::Type::EVICT;
                        action.key = key_ctx.key;
                        action.source_tier_id = fast_id;
                        actions.push_back(action);

                        // Use actual size from KeyContext
                        freed_so_far += key_ctx.size_bytes;
                    } else {
                        // Needs Migration to Slow Tier.
                        // Issue: We don't know Slow Tier ID here easily without iterating all stats or passing it in.
                        // Let's rely on finding a tier that isn't fast.
                        std::optional<UUID> slow_id;
                        for(const auto& [tid, tstats] : tier_stats) {
                            if (tid != fast_id) {
                                slow_id = tid;
                                break;
                            }
                        }

                        if (slow_id.has_value()) {
                            action.type = SchedAction::Type::MIGRATE;
                            action.key = key_ctx.key;
                            action.source_tier_id = fast_id;
                            action.target_tier_id = slow_id.value();
                            actions.push_back(action);
                            freed_so_far += key_ctx.size_bytes;
                        }
                    }
                }
            }
        }

        return actions;
    }

   private:
    Config config_;
    std::optional<UUID> fast_tier_id_;
};

}  // namespace mooncake
