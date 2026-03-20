#include "tiered_cache/scheduler/simple_policy.h"

#include <glog/logging.h>

#include <algorithm>

namespace mooncake {

SimplePolicy::SimplePolicy(Config config) : config_(config) {}

void SimplePolicy::SetFastTier(UUID id) { fast_tier_id_ = id; }

tl::expected<std::vector<SchedAction>, ErrorCode> SimplePolicy::Decide(
    const std::unordered_map<UUID, TierStats>& tier_stats,
    const std::vector<KeyContext>& active_keys) {
    std::vector<SchedAction> actions;

    if (active_keys.empty()) {
        return actions;
    }

    if (!fast_tier_id_.has_value()) {
        LOG(ERROR) << "SimplePolicy::Decide requires a configured fast tier, "
                      "but none is set";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const UUID target_id = fast_tier_id_.value();
    const auto tier_it = tier_stats.find(target_id);
    if (tier_it == tier_stats.end()) {
        LOG(ERROR) << "SimplePolicy::Decide missing stats for fast tier "
                   << target_id;
        return tl::make_unexpected(ErrorCode::TIER_NOT_FOUND);
    }

    for (const auto& key_ctx : active_keys) {
        if (key_ctx.recent_heat_score < config_.promotion_threshold) {
            continue;
        }

        const bool already_in_target =
            std::find(key_ctx.current_locations.begin(),
                      key_ctx.current_locations.end(),
                      target_id) != key_ctx.current_locations.end();
        if (already_in_target) {
            continue;
        }

        if (key_ctx.current_locations.empty()) {
            LOG(ERROR) << "SimplePolicy::Decide found hot key without any "
                          "replica locations: key="
                       << key_ctx.key;
            continue;
        }

        SchedAction action;
        action.type = SchedAction::Type::MIGRATE;
        action.key = key_ctx.key;
        action.source_tier_id = key_ctx.current_locations.front();
        action.target_tier_id = target_id;
        actions.push_back(std::move(action));
    }

    return actions;
}

}  // namespace mooncake
