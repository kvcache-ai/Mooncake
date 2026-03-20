#pragma once

#include "tiered_cache/scheduler/scheduler_policy.h"

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

    explicit SimplePolicy(Config config);

    // Configure which tier is considered "Fast" (Target for promotion)
    void SetFastTier(UUID id) override;

    tl::expected<std::vector<SchedAction>, ErrorCode> Decide(
        const std::unordered_map<UUID, TierStats>& tier_stats,
        const std::vector<KeyContext>& active_keys) override;

   private:
    Config config_;
    std::optional<UUID> fast_tier_id_;
};

}  // namespace mooncake
