#pragma once

#include "tiered_cache/scheduler/scheduler_policy.h"

namespace mooncake {

/**
 * @class LRUPolicy
 * @brief Recency-based promotion/eviction policy with watermark control.
 *
 * All keys are sorted by recency. The most recently used keys (up to target
 * capacity) should be in fast tier, the rest in slow tier.
 */
class LRUPolicy : public SchedulerPolicy {
   public:
    struct Config {
        double high_watermark = 0.90;  // Trigger scheduling when above this
        double low_watermark = 0.70;   // Target usage after scheduling
    };

    explicit LRUPolicy(Config config);

    void SetFastTier(UUID id) override;

    bool IsFastTier(UUID id) const;

    tl::expected<std::vector<SchedAction>, ErrorCode> Decide(
        const std::unordered_map<UUID, TierStats>& tier_stats,
        const std::vector<KeyContext>& active_keys) override;

   private:
    Config config_;
    std::optional<UUID> fast_tier_id_;
};

}  // namespace mooncake
