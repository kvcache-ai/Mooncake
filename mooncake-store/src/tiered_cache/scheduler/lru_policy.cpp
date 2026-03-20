#include "tiered_cache/scheduler/lru_policy.h"

#include <glog/logging.h>

#include <algorithm>
#include <unordered_set>

namespace mooncake {

LRUPolicy::LRUPolicy(Config config) : config_(config) {}

void LRUPolicy::SetFastTier(UUID id) { fast_tier_id_ = id; }

bool LRUPolicy::IsFastTier(UUID id) const {
    return fast_tier_id_.has_value() && fast_tier_id_.value() == id;
}

tl::expected<std::vector<SchedAction>, ErrorCode> LRUPolicy::Decide(
    const std::unordered_map<UUID, TierStats>& tier_stats,
    const std::vector<KeyContext>& active_keys) {
    std::vector<SchedAction> actions;

    if (active_keys.empty()) {
        return actions;
    }

    if (!fast_tier_id_.has_value()) {
        LOG(ERROR) << "LRUPolicy::Decide requires a configured fast tier, "
                      "but none is set";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const UUID fast_id = fast_tier_id_.value();
    const auto tier_it = tier_stats.find(fast_id);
    if (tier_it == tier_stats.end()) {
        LOG(ERROR) << "LRUPolicy::Decide missing stats for fast tier "
                   << fast_id;
        return tl::make_unexpected(ErrorCode::TIER_NOT_FOUND);
    }

    const TierStats& stats = tier_it->second;
    if (stats.total_capacity_bytes == 0) {
        LOG(ERROR) << "LRUPolicy::Decide fast tier " << fast_id
                   << " has zero capacity";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const double usage_ratio = static_cast<double>(stats.used_capacity_bytes) /
                               static_cast<double>(stats.total_capacity_bytes);
    const bool above_high = usage_ratio > config_.high_watermark;
    const bool pre_demote_window = usage_ratio > config_.low_watermark &&
                                   usage_ratio <= config_.high_watermark;

    std::optional<UUID> slow_id;
    for (const auto& [tier_id, tier_stats_entry] : tier_stats) {
        static_cast<void>(tier_stats_entry);
        if (tier_id != fast_id) {
            slow_id = tier_id;
            break;
        }
    }

    const size_t target_usage =
        static_cast<size_t>(stats.total_capacity_bytes * config_.low_watermark);

    std::vector<const KeyContext*> should_be_in_fast;
    should_be_in_fast.reserve(active_keys.size());
    size_t accumulated_size = 0;
    for (const auto& key_ctx : active_keys) {
        if (accumulated_size + key_ctx.size_bytes <= target_usage) {
            should_be_in_fast.push_back(&key_ctx);
            accumulated_size += key_ctx.size_bytes;
        }
    }

    std::unordered_set<std::string> should_be_in_fast_set;
    should_be_in_fast_set.reserve(should_be_in_fast.size());
    for (const auto* ctx : should_be_in_fast) {
        should_be_in_fast_set.insert(ctx->key);
    }

    for (const auto& key_ctx : active_keys) {
        const bool is_in_fast =
            std::find(key_ctx.current_locations.begin(),
                      key_ctx.current_locations.end(),
                      fast_id) != key_ctx.current_locations.end();
        const bool should_be = should_be_in_fast_set.find(key_ctx.key) !=
                               should_be_in_fast_set.end();

        if (is_in_fast && !should_be) {
            const bool has_other_copy = key_ctx.current_locations.size() > 1;
            SchedAction action;
            if (above_high) {
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
                    action.type = SchedAction::Type::EVICT;
                    action.key = key_ctx.key;
                    action.source_tier_id = fast_id;
                }
                actions.push_back(std::move(action));
            } else if (pre_demote_window && !has_other_copy &&
                       slow_id.has_value()) {
                action.type = SchedAction::Type::REPLICATE;
                action.key = key_ctx.key;
                action.source_tier_id = fast_id;
                action.target_tier_id = slow_id.value();
                actions.push_back(std::move(action));
            }
            continue;
        }

        if (is_in_fast || !should_be || pre_demote_window) {
            continue;
        }

        if (key_ctx.current_locations.empty()) {
            LOG(ERROR) << "LRUPolicy::Decide found key without any replica "
                          "locations: key="
                       << key_ctx.key;
            continue;
        }

        SchedAction action;
        action.type = SchedAction::Type::MIGRATE;
        action.key = key_ctx.key;
        action.source_tier_id = key_ctx.current_locations.front();
        action.target_tier_id = fast_id;
        actions.push_back(std::move(action));
    }

    return actions;
}

}  // namespace mooncake
