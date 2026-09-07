#include "p2p/client/v2/eviction_policy.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {

const char* ToString(ReclaimUrgency urgency) {
    switch (urgency) {
        case ReclaimUrgency::kNone:
            return "none";
        case ReclaimUrgency::kBackground:
            return "background";
        case ReclaimUrgency::kForeground:
            return "foreground";
    }
    return "unknown";
}

namespace {

constexpr double kLn2 = 0.6931471805599453;
constexpr char kDynamicWatermark[] = "dynamic_watermark";

template <typename Duration>
double Seconds(Duration duration) {
    return std::chrono::duration<double>(duration).count();
}

size_t SaturatingAdd(size_t lhs, size_t rhs) {
    // A wrap here would report a nearly empty tier and stop reclamation on a
    // full one, so the pathological snapshot is pinned at "completely full"
    // rather than allowed to fold around zero.
    if (lhs > std::numeric_limits<size_t>::max() - rhs) {
        return std::numeric_limits<size_t>::max();
    }
    return lhs + rhs;
}

/**
 * @brief The bytes that stand between the tier and its target.
 *
 * Section 4.2 says to count committed, reserved, pending write and pending
 * migration bytes. `reserved_bytes` already is the pending-write figure (a
 * PreWrite or an in-flight Put), so the sum is indexed + reserved + pending
 * migration -- with one deviation: the committed term is
 * max(indexed_bytes, physical_used_bytes), not indexed_bytes.
 *
 * Indexed bytes drop the instant a pinned block is detached, while the pool
 * still holds the space until the last reader goes away (section 4.3). A
 * controller reading only the index would watch usage fall, conclude the round
 * succeeded and stop -- and the very next allocation would still fail because
 * nothing was physically freed. Taking the max keeps those retired-pinned
 * bytes visible; where the tier reports no physical figure it degrades to the
 * doc's formula exactly.
 *
 * retired_pinned_bytes is deliberately NOT added on top: it is a subset of
 * physical_used_bytes (detached blocks are, by definition, not yet returned to
 * the pool), and adding it would count the same space twice and drive usage
 * past 1.0 on a half-empty tier.
 */
/**
 * @brief A byte count from a computed double: never negative, never a cast
 *        whose result is undefined.
 *
 * `used` can be a saturated value from a broken snapshot and the projection
 * adds a rate to it, so the argument is not bounded by anything the caller
 * controls. Converting a double at or above 2^64 to size_t is undefined
 * behaviour rather than merely a wrong number. NaN falls out as zero.
 */
size_t ToBytes(double value) {
    constexpr double kMaxBytes =
        static_cast<double>(std::numeric_limits<size_t>::max());
    if (!(value > 0.0)) return 0;
    if (value >= kMaxBytes) return std::numeric_limits<size_t>::max();
    return static_cast<size_t>(value);
}

size_t UsedBytes(const TierCapacitySnapshot& snapshot) {
    const size_t committed =
        std::max(snapshot.indexed_bytes, snapshot.physical_used_bytes);
    return SaturatingAdd(SaturatingAdd(committed, snapshot.reserved_bytes),
                         snapshot.pending_migration_bytes);
}

/**
 * @brief Bytes eviction could actually get back.
 *
 * Deliberately narrower than UsedBytes, which answers "how full is this tier"
 * and rightly counts everything. Reserved bytes are promised to a writer,
 * pending-migration bytes to a copy already scheduled, and retired-pinned
 * bytes are detached and held by a reader -- eviction cannot free any of them.
 * Sizing a round from the wider number asks the engine for bytes that cannot
 * come back, and it then re-plans immediately and asks again, forever.
 */
size_t ReclaimableBytes(const TierCapacitySnapshot& snapshot) {
    const size_t committed =
        std::max(snapshot.indexed_bytes, snapshot.physical_used_bytes);
    return committed - std::min(committed, snapshot.retired_pinned_bytes);
}

/**
 * @class DynamicWatermarkEvictionPolicy
 * @brief The controller of section 4.2: one write-rate estimate, one target
 *        moving inside [base_target, idle_target], one trigger above it.
 *
 * Every field lives behind one mutex because RecordWrite runs on commit
 * threads while the EvictEngine's controller thread plans, and both touch the
 * write-rate accumulator. The lock is only ever held across arithmetic -- no
 * callback, no index lookup, no allocation -- so it cannot take part in the
 * lock order that section 9 invariant 9 constrains.
 */
class DynamicWatermarkEvictionPolicy final : public EvictionPolicy {
   public:
    DynamicWatermarkEvictionPolicy(const EvictionPolicyConfig& config,
                                   std::shared_ptr<Clock> clock)
        : config_(config),
          clock_(std::move(clock)),
          // A half-life is what an operator can reason about; the integrator
          // needs the matching time constant.
          tau_seconds_(Seconds(config.ewma_half_life) / kLn2),
          // How long the tier is on its own: worst case the controller does
          // not look again for one interval, plus however far ahead it was
          // asked to see. Whatever arrives in that window has to fit in the
          // free space the target leaves behind, which is why the target is
          // sized against it.
          window_seconds_(Seconds(config.controller_interval) +
                          Seconds(config.headroom_horizon)),
          // "Startup uses base_target": with no sample yet the estimator
          // cannot tell an idle node from one that is about to be hammered,
          // and only the floor is safe in both readings.
          dynamic_target_(config.base_target_watermark),
          last_decay_(clock_->Now()),
          last_slew_(last_decay_) {}

    void RecordWrite(size_t bytes) override {
        if (bytes == 0) return;
        std::lock_guard<std::mutex> lock(mu_);
        DecayLocked(clock_->Now());
        accumulated_bytes_ += static_cast<double>(bytes);
    }

    ReclaimPlan Plan(const TierCapacitySnapshot& snapshot) override {
        std::lock_guard<std::mutex> lock(mu_);
        const auto now = clock_->Now();
        DecayLocked(now);
        SlewTargetLocked(snapshot.capacity, now);
        ReclaimPlan plan = BuildPlanLocked(snapshot, now);
        ++plans_;
        CountTriggerLocked(plan.urgency);
        return plan;
    }

    ReclaimPlan PlanForAllocationFailure(const TierCapacitySnapshot& snapshot,
                                         size_t allocation_size) override {
        std::lock_guard<std::mutex> lock(mu_);
        const auto now = clock_->Now();
        DecayLocked(now);
        SlewTargetLocked(snapshot.capacity, now);
        ReclaimPlan plan = BuildPlanLocked(snapshot, now);
        // The watermarks do not get a vote here. A caller is blocked, so the
        // round has to free at least the size that just failed on top of
        // whatever the target already asked for -- reclaiming less than the
        // failed size cannot unblock it.
        plan.urgency = ReclaimUrgency::kForeground;
        plan.target_bytes = std::max(plan.target_bytes, allocation_size);
        plan.next_check = std::chrono::milliseconds{0};
        // Open the hysteresis latch as well: the background plans that follow
        // must keep reclaiming down to the target instead of stopping the
        // moment usage slips back under the trigger.
        in_reclaim_ = true;
        ++plans_;
        // Counted per call, unlike the edge-counted plan triggers below: each
        // failed allocation is its own event, and collapsing a run of them
        // would hide exactly the situation this counter exists to expose.
        ++foreground_triggers_;
        last_urgency_ = ReclaimUrgency::kForeground;
        return plan;
    }

    EvictionPolicyStats Stats() const override {
        std::lock_guard<std::mutex> lock(mu_);
        EvictionPolicyStats stats;
        stats.dynamic_target = dynamic_target_;
        stats.trigger_watermark = TriggerLocked();
        // Decayed to *now* without mutating: a rate that only moved when
        // someone happened to call Plan() would keep reporting a busy tier
        // long after the writes stopped.
        stats.write_bytes_per_second = RateLocked(clock_->Now());
        stats.plans = plans_;
        stats.background_triggers = background_triggers_;
        stats.foreground_triggers = foreground_triggers_;
        return stats;
    }

   private:
    /**
     * @brief The write accumulator, decayed to `now`.
     *
     * Same integrator as MultiLRUPolicy::RefreshEvictWatermark -- accumulate
     * committed bytes, multiply by exp(-dt/tau) -- with two deliberate
     * divergences. That one keeps the raw accumulator and compares it against
     * capacity to get a dimensionless load score, which bakes the window into
     * the capacity; here the accumulator is divided by tau, which turns it
     * into an actual bytes/second figure. The header reports a rate, and
     * projecting over headroom_horizon needs a rate rather than a score. And
     * that one decays only on its periodic pass, batching bytes in between,
     * while this decays on every read so an idle tier's estimate falls to zero
     * even if nobody ever writes again.
     */
    double DecayedBytesLocked(Clock::time_point now) const {
        const double dt = Seconds(now - last_decay_);
        // dt <= 0 covers both "two events in the same tick" and a clock that
        // was not monotonic; neither may amplify the estimate.
        if (dt <= 0.0) return accumulated_bytes_;
        return accumulated_bytes_ * std::exp(-dt / tau_seconds_);
    }

    double RateLocked(Clock::time_point now) const {
        // A sustained r bytes/s settles the accumulator at r * tau, so this
        // division is what makes the estimate independent of the half-life.
        return DecayedBytesLocked(now) / tau_seconds_;
    }

    void DecayLocked(Clock::time_point now) {
        accumulated_bytes_ = DecayedBytesLocked(now);
        last_decay_ = now;
    }

    double TriggerLocked() const {
        return std::min(dynamic_target_ + config_.watermark_hysteresis,
                        config_.limit_watermark);
    }

    /** The target the current write rate justifies, before smoothing. */
    double LoadTargetLocked(size_t capacity, Clock::time_point now) const {
        if (capacity == 0) {
            // Nothing to divide by, and the floor is the only answer that
            // cannot overstate how much the tier may keep.
            return config_.base_target_watermark;
        }
        // Free space is sized to the bytes expected during one reaction
        // window: if the write stream will claim 5% of the tier before the
        // controller can act again, keep 5% more free than an idle tier would.
        const double incoming =
            RateLocked(now) * window_seconds_ / static_cast<double>(capacity);
        return std::clamp(config_.idle_target_watermark - incoming,
                          config_.base_target_watermark,
                          config_.idle_target_watermark);
    }

    void SlewTargetLocked(size_t capacity, Clock::time_point now) {
        const double dt = Seconds(now - last_slew_);
        last_slew_ = now;
        const double load_target = LoadTargetLocked(capacity, now);
        if (load_target <= dynamic_target_) {
            // Down is immediate. The rate driving it is already smoothed, and
            // damping the descent on top of that is how a burst reaches the
            // hard limit before the target has finished getting out of its
            // way.
            dynamic_target_ = load_target;
        } else if (dt > 0.0) {
            // Up is smoothed on the same half-life: handing the cache back
            // the instant one interval looks quiet costs a second reclaim
            // round as soon as the load returns.
            dynamic_target_ += (1.0 - std::exp(-dt / tau_seconds_)) *
                               (load_target - dynamic_target_);
        }
        // The band is enforced here rather than inferred from the arithmetic
        // above. base_target is the operator's contract and limit_watermark is
        // the hard line; a rounding error that let the target drift outside
        // them would break both silently.
        dynamic_target_ =
            std::clamp(dynamic_target_, config_.base_target_watermark,
                       config_.idle_target_watermark);
    }

    ReclaimPlan BuildPlanLocked(const TierCapacitySnapshot& snapshot,
                                Clock::time_point now) {
        ReclaimPlan plan;
        plan.dynamic_target = dynamic_target_;
        plan.trigger_watermark = TriggerLocked();
        plan.write_bytes_per_second = RateLocked(now);

        if (snapshot.capacity == 0) {
            // A tier that reports no capacity holds nothing this controller
            // can plan against, and every ratio below would come back NaN --
            // which compares false against every watermark and would quietly
            // disable the limit check. Say "nothing to do" and come back.
            LOG_FIRST_N(WARNING, 1)
                << "EvictionPolicy planning against a zero-capacity tier; "
                   "no reclamation is possible";
            in_reclaim_ = false;
            plan.next_check = config_.controller_interval;
            return plan;
        }

        const double capacity = static_cast<double>(snapshot.capacity);
        const double used = static_cast<double>(UsedBytes(snapshot));
        // Not clamped to 1: an over-committed tier should read as
        // over-committed instead of merely full.
        plan.usage_ratio = used / capacity;

        const double horizon_seconds = Seconds(config_.headroom_horizon);
        const double projected_used =
            used + plan.write_bytes_per_second * horizon_seconds;
        const bool at_limit = plan.usage_ratio >= config_.limit_watermark;
        const bool crossing =
            horizon_seconds > 0.0 &&
            projected_used >= config_.limit_watermark * capacity;

        const double target_used = dynamic_target_ * capacity;
        double excess = used - target_used;
        const double reclaimable =
            static_cast<double>(ReclaimableBytes(snapshot));
        if (at_limit || crossing) {
            plan.urgency = ReclaimUrgency::kForeground;
            if (crossing) {
                // Reclaim ahead of the write stream. Freeing only what is
                // over the target today leaves the projected bytes free to
                // push the tier through the limit anyway, which is the whole
                // reason the horizon exists.
                excess = std::max(excess, projected_used - target_used);
            }
            in_reclaim_ = true;
        } else if (plan.usage_ratio >= plan.trigger_watermark ||
                   (in_reclaim_ && plan.usage_ratio > dynamic_target_)) {
            // The second clause is the hysteresis latch: a round that started
            // at the trigger keeps running until usage is back at the target.
            // Without it the gap between the two would do nothing and a tier
            // sitting on the target would start and stop a round per commit.
            plan.urgency = ReclaimUrgency::kBackground;
            in_reclaim_ = true;
        } else {
            in_reclaim_ = false;
        }

        if (plan.urgency == ReclaimUrgency::kNone) {
            plan.next_check = config_.controller_interval;
            return plan;
        }

        // Never ask for more than eviction can supply, and never for more than
        // the tier holds. Both bounds matter: the horizon projection can
        // otherwise order a reclaim larger than the whole tier, and a tier
        // whose space is entirely reserved or pinned would be asked to free
        // bytes that no victim can release.
        excess = std::min(excess, reclaimable);
        excess = std::min(excess, capacity);
        plan.target_bytes = ToBytes(excess);

        if (plan.target_bytes == 0) {
            // Over the line, but with nothing an eviction round could free:
            // every byte is reserved, pinned or in flight. Saying "reclaim 0
            // bytes, ask me again immediately" is a live-lock -- the round
            // frees nothing, usage does not move, and the controller spins.
            // Report kNone and wait out the interval instead; the situation
            // resolves when a reader or a writer finishes, not when this
            // function is called again.
            plan.urgency = ReclaimUrgency::kNone;
            in_reclaim_ = false;
            plan.next_check = config_.controller_interval;
            return plan;
        }
        // A round is about to run; the engine re-plans when it finishes
        // rather than sleeping on an answer it already knows is stale.
        plan.next_check = std::chrono::milliseconds{0};
        return plan;
    }

    void CountTriggerLocked(ReclaimUrgency urgency) {
        // Edges, not plans: while a round runs next_check is zero, so a
        // per-plan counter would report the controller's loop rate instead of
        // how often reclamation actually had to be started.
        if (urgency != last_urgency_) {
            if (urgency == ReclaimUrgency::kBackground) {
                ++background_triggers_;
            } else if (urgency == ReclaimUrgency::kForeground) {
                ++foreground_triggers_;
            }
        }
        last_urgency_ = urgency;
    }

    mutable std::mutex mu_;
    const EvictionPolicyConfig config_;
    const std::shared_ptr<Clock> clock_;
    const double tau_seconds_;
    const double window_seconds_;

    double accumulated_bytes_ = 0.0;
    double dynamic_target_;
    Clock::time_point last_decay_;
    Clock::time_point last_slew_;
    /** True between crossing the trigger and falling back to the target. */
    bool in_reclaim_ = false;
    ReclaimUrgency last_urgency_ = ReclaimUrgency::kNone;
    uint64_t plans_ = 0;
    uint64_t background_triggers_ = 0;
    uint64_t foreground_triggers_ = 0;
};

}  // namespace

tl::expected<void, ErrorCode> ValidateEvictionPolicyConfig(
    const EvictionPolicyConfig& config) {
    // One chain, checked link by link so the log names the link that broke:
    //   0 < base_target <= idle_target < limit_watermark <= 1.
    // Every comparison is written as a negated positive, which also rejects a
    // NaN: NaN compares false against everything, so a plain `x <= 0.0` test
    // would wave it through into arithmetic that never triggers reclamation.
    if (!(config.base_target_watermark > 0.0)) {
        LOG(ERROR) << "eviction_policy.base_target_watermark must be greater "
                      "than zero, got "
                   << config.base_target_watermark;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!(config.idle_target_watermark >= config.base_target_watermark)) {
        LOG(ERROR) << "eviction_policy.idle_target_watermark must be at least "
                      "base_target_watermark ("
                   << config.base_target_watermark << "), got "
                   << config.idle_target_watermark;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!(config.limit_watermark > config.idle_target_watermark)) {
        // Strictly greater: an idle target sitting on the limit means the
        // controller is allowed to consider a tier healthy at the exact point
        // where allocation starts failing.
        LOG(ERROR) << "eviction_policy.limit_watermark must be greater than "
                      "idle_target_watermark ("
                   << config.idle_target_watermark << "), got "
                   << config.limit_watermark;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!(config.limit_watermark <= 1.0)) {
        LOG(ERROR) << "eviction_policy.limit_watermark must be at most 1, got "
                   << config.limit_watermark;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!(config.watermark_hysteresis >= 0.0)) {
        // Zero is legal and means "trigger sits on the target". Negative puts
        // the start line below the stop line: a round that can never stop.
        LOG(ERROR) << "eviction_policy.watermark_hysteresis must not be "
                      "negative, got "
                   << config.watermark_hysteresis;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.ewma_half_life <= std::chrono::milliseconds::zero()) {
        // It becomes the divisor of the rate estimate.
        LOG(ERROR) << "eviction_policy.ewma_half_life must be positive, got "
                   << config.ewma_half_life.count() << "ms";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.controller_interval <= std::chrono::milliseconds::zero()) {
        // An idle controller sleeps for it; zero would spin a core.
        LOG(ERROR) << "eviction_policy.controller_interval must be positive, "
                      "got "
                   << config.controller_interval.count() << "ms";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.headroom_horizon < std::chrono::milliseconds::zero()) {
        // Zero disables the projection, as the header documents; a negative
        // horizon would project the tier backwards in time.
        LOG(ERROR) << "eviction_policy.headroom_horizon must not be negative, "
                      "got "
                   << config.headroom_horizon.count() << "ms";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<std::unique_ptr<EvictionPolicy>, ErrorCode> CreateEvictionPolicy(
    const EvictionPolicyConfig& config, std::shared_ptr<Clock> clock) {
    auto valid = ValidateEvictionPolicyConfig(config);
    if (!valid) return tl::make_unexpected(valid.error());
    if (!clock) {
        // The controller reads time on every write and every plan; falling
        // back to a real clock here would make the whole component untestable
        // without sleeping.
        LOG(ERROR) << "CreateEvictionPolicy needs a clock";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.type != kDynamicWatermark) {
        LOG(ERROR) << "Unknown eviction policy type '" << config.type << "'";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return std::make_unique<DynamicWatermarkEvictionPolicy>(config,
                                                            std::move(clock));
}

}  // namespace mooncake::v2
