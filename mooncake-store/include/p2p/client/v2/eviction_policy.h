#pragma once

// EvictionPolicy: when to reclaim, and how much.
//
// It is deliberately stateless about blocks. It never sees a key, a
// registration or a pinned flag -- only bytes and time for one tiler -- so it
// cannot become a second opinion about what exists. Its whole output is a
// ReclaimPlan; the EvictEngine decides nothing about timing and the policy
// decides nothing about victims.
//
// The first implementation is a dynamic watermark controller. A single fixed
// target is wrong in both directions: set it low and an idle node throws away
// a cache it could have kept, set it high and a write burst hits the hard
// limit before reclamation has caught up. So the target moves with the write
// rate, between a user-configured floor and an idle ceiling:
//
//   0 < base_target <= dynamic_target <= idle_target < limit_watermark <= 1
//
// base_target is the operator's contract: the controller may keep MORE free
// space than asked for when the node is busy, never less.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @struct TierCapacitySnapshot
 * @brief The four kinds of "used" a controller has to tell apart.
 *
 * Collapsing them is how a reclaim loop starts lying: detached-but-pinned
 * bytes look reclaimable and are not, reserved bytes look free and are not,
 * and a controller that only reads indexed bytes will spin against a tier
 * whose space is held by readers.
 */
struct TierCapacitySnapshot {
    size_t capacity = 0;
    /** Visible in the BlockIndex. */
    size_t indexed_bytes = 0;
    /** Not yet returned to the pool, whatever the index says. */
    size_t physical_used_bytes = 0;
    /** Promised to a PreWrite or an in-flight Put. */
    size_t reserved_bytes = 0;
    /** Destination space owed to migrations already scheduled. */
    size_t pending_migration_bytes = 0;
    /** Detached but still referenced by a reader or a pinned lease. */
    size_t retired_pinned_bytes = 0;
};

/**
 * @enum ReclaimUrgency
 */
enum class ReclaimUrgency : uint8_t {
    /** Nothing to do. */
    kNone,
    /** Above the trigger watermark; reclaim in the background. */
    kBackground,
    /** At or above the limit, or an allocation just failed. */
    kForeground,
};

const char* ToString(ReclaimUrgency urgency);

/**
 * @struct ReclaimPlan
 */
struct ReclaimPlan {
    ReclaimUrgency urgency = ReclaimUrgency::kNone;
    /** Bytes this round should try to reclaim. 0 when urgency is kNone. */
    size_t target_bytes = 0;
    /** How long to wait before asking again, when nothing is to be done. */
    std::chrono::milliseconds next_check{0};

    // Reported for observability; the engine does not branch on these.
    double dynamic_target = 0.0;
    double trigger_watermark = 0.0;
    double usage_ratio = 0.0;
    double write_bytes_per_second = 0.0;

    bool ShouldReclaim() const { return urgency != ReclaimUrgency::kNone; }
};

/**
 * @struct EvictionPolicyConfig
 */
struct EvictionPolicyConfig {
    /** Resolved by CreateEvictionPolicy. Only "dynamic_watermark" today. */
    std::string type = "dynamic_watermark";

    /** The operator's floor. Reclamation never targets less free space. */
    double base_target_watermark = 0.8;
    /** The ceiling an idle tier is allowed to drift up to. */
    double idle_target_watermark = 0.9;
    /** Hard line: at or above it, reclamation is foreground. */
    double limit_watermark = 0.95;
    /**
     * Added to the dynamic target to get the start line. Without it a tier
     * hovering at the target would start and stop a reclaim round on every
     * commit.
     */
    double watermark_hysteresis = 0.02;

    /** EWMA half-life for the write-rate estimate. */
    std::chrono::milliseconds ewma_half_life{500};
    /** How often the background controller re-plans while idle. */
    std::chrono::milliseconds controller_interval{200};
    /**
     * How far ahead to project the write rate when deciding whether the tier
     * is about to cross the limit. 0 disables projection.
     */
    std::chrono::milliseconds headroom_horizon{200};
};

tl::expected<void, ErrorCode> ValidateEvictionPolicyConfig(
    const EvictionPolicyConfig& config);

/**
 * @struct EvictionPolicyStats
 */
struct EvictionPolicyStats {
    double dynamic_target = 0.0;
    double trigger_watermark = 0.0;
    double write_bytes_per_second = 0.0;
    uint64_t plans = 0;
    uint64_t background_triggers = 0;
    uint64_t foreground_triggers = 0;
};

/**
 * @class EvictionPolicy
 */
class EvictionPolicy {
   public:
    virtual ~EvictionPolicy() = default;

    /**
     * @brief Feed the write-rate estimator.
     *
     * Called on commit with the committed size. Bytes rather than keys: a
     * thousand small keys and one large object load a tier very differently,
     * and only the byte rate predicts when it will fill.
     */
    virtual void RecordWrite(size_t bytes) = 0;

    /** Plan for the current state of the tier. */
    virtual ReclaimPlan Plan(const TierCapacitySnapshot& snapshot) = 0;

    /**
     * @brief Plan for an allocation that just failed.
     *
     * Always foreground, and the target is at least the failed size: a caller
     * is blocked on this.
     */
    virtual ReclaimPlan PlanForAllocationFailure(
        const TierCapacitySnapshot& snapshot, size_t allocation_size) = 0;

    virtual EvictionPolicyStats Stats() const = 0;
};

tl::expected<std::unique_ptr<EvictionPolicy>, ErrorCode> CreateEvictionPolicy(
    const EvictionPolicyConfig& config, std::shared_ptr<Clock> clock);

}  // namespace mooncake::v2
