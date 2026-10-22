#include "p2p/client/v2/eviction_policy.h"

// Time is injected. Every watermark, half-life and horizon in this file moves
// because FakeClock was advanced, never because a thread waited: the
// controller is a time-driven feedback loop, and a test that slept would
// assert on whatever the machine happened to be doing.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

using namespace std::chrono_literals;

/**
 * @class FakeClock
 * @brief The only time source in this file. Its origin is unrelated to the
 *        real steady clock, so a test that accidentally depended on wall time
 *        would fail rather than pass slowly.
 */
class FakeClock final : public Clock {
   public:
    time_point Now() const override {
        return now_.load(std::memory_order_acquire);
    }

    void SetNow(time_point now) { now_.store(now, std::memory_order_release); }

    void Advance(std::chrono::milliseconds delta) {
        now_.store(now_.load(std::memory_order_acquire) + delta,
                   std::memory_order_release);
    }

   private:
    std::atomic<time_point> now_{Clock::time_point{} + 24h};
};

// Round numbers: a watermark times this capacity is an exact byte count, so
// the reclaim targets below can be equalities instead of tolerances.
constexpr size_t kCapacity = 1'000'000;

constexpr double kBase = 0.6;
constexpr double kIdle = 0.9;
constexpr double kLimit = 0.95;
constexpr auto kInterval = 200ms;
constexpr auto kHalfLife = 100ms;

/** A band wide enough that movement inside it is visible. */
EvictionPolicyConfig BandConfig() {
    EvictionPolicyConfig config;
    config.base_target_watermark = kBase;
    config.idle_target_watermark = kIdle;
    config.limit_watermark = kLimit;
    config.watermark_hysteresis = 0.02;
    config.ewma_half_life = kHalfLife;
    config.controller_interval = kInterval;
    config.headroom_horizon = 200ms;
    return config;
}

/**
 * @brief base == idle pins the dynamic target.
 *
 * The watermark tests are about the trigger, the latch and the limit, and a
 * target that also moved would make every expected byte count depend on the
 * estimator's arithmetic rather than on the behaviour under test.
 */
EvictionPolicyConfig PinnedConfig() {
    EvictionPolicyConfig config = BandConfig();
    config.base_target_watermark = 0.8;
    config.idle_target_watermark = 0.8;
    config.watermark_hysteresis = 0.05;
    return config;
}

TierCapacitySnapshot UsedSnapshot(size_t capacity, size_t used) {
    TierCapacitySnapshot snapshot;
    snapshot.capacity = capacity;
    // The common case: nothing detached, so the index and the pool agree.
    snapshot.indexed_bytes = used;
    snapshot.physical_used_bytes = used;
    return snapshot;
}

class EvictionPolicyTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("EvictionPolicyTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override { clock_ = std::make_shared<FakeClock>(); }

    std::unique_ptr<EvictionPolicy> Policy(const EvictionPolicyConfig& config) {
        auto policy = CreateEvictionPolicy(config, clock_);
        CHECK(policy.has_value()) << "test setup: CreateEvictionPolicy failed";
        return std::move(policy.value());
    }

    /** What the EvictEngine does on a tier nobody is writing to. */
    std::vector<double> RunIdle(EvictionPolicy& policy, int steps,
                                const TierCapacitySnapshot& snapshot) {
        std::vector<double> targets;
        for (int i = 0; i < steps; ++i) {
            clock_->Advance(kInterval);
            targets.push_back(policy.Plan(snapshot).dynamic_target);
        }
        return targets;
    }

    /** A steady write stream: commit, then the controller looks. */
    std::vector<double> RunLoad(EvictionPolicy& policy, int steps, size_t bytes,
                                std::chrono::milliseconds step,
                                const TierCapacitySnapshot& snapshot) {
        std::vector<double> targets;
        for (int i = 0; i < steps; ++i) {
            clock_->Advance(step);
            policy.RecordWrite(bytes);
            targets.push_back(policy.Plan(snapshot).dynamic_target);
        }
        return targets;
    }

    std::shared_ptr<FakeClock> clock_;
};

// A restart must not hand the tier the idle allowance it has not earned: with
// no sample yet the estimator cannot tell a quiet node from one that is about
// to be hammered, and starting at idle_target would let the first burst after
// a restart reach the hard limit before the controller had a single reading.
TEST_F(EvictionPolicyTest, StartupSitsOnTheOperatorFloorWithNoWriteRate) {
    auto policy = Policy(BandConfig());

    const EvictionPolicyStats before = policy->Stats();
    EXPECT_DOUBLE_EQ(before.dynamic_target, kBase);
    EXPECT_DOUBLE_EQ(before.write_bytes_per_second, 0.0);
    EXPECT_EQ(before.plans, 0u);
    EXPECT_EQ(before.background_triggers, 0u);
    EXPECT_EQ(before.foreground_triggers, 0u);

    const ReclaimPlan plan = policy->Plan(UsedSnapshot(kCapacity, 0));
    EXPECT_EQ(plan.urgency, ReclaimUrgency::kNone);
    EXPECT_FALSE(plan.ShouldReclaim());
    EXPECT_EQ(plan.target_bytes, 0u);
    EXPECT_DOUBLE_EQ(plan.dynamic_target, kBase);
    // Nothing to do means "sleep one controller interval", not "spin".
    EXPECT_EQ(plan.next_check, kInterval);
}

// A busy tier that keeps only base_target free is a tier that hits the hard
// limit mid-burst: reclamation cannot outrun a write stream it starts racing
// only once the stream has already filled the headroom. If the target stopped
// falling under load, every burst would end in foreground eviction on the
// request path.
TEST_F(EvictionPolicyTest, SustainedWriteLoadDrivesTheTargetToTheFloor) {
    auto policy = Policy(BandConfig());
    const TierCapacitySnapshot snapshot = UsedSnapshot(kCapacity, 100'000);

    // Earn the idle allowance first, otherwise the fall has nowhere to start:
    // startup already sits on the floor.
    const std::vector<double> idle = RunIdle(*policy, 15, snapshot);
    ASSERT_NEAR(idle.back(), kIdle, 1e-3);

    const std::vector<double> loaded =
        RunLoad(*policy, 10, 200'000, 50ms, snapshot);
    for (size_t i = 1; i < loaded.size(); ++i) {
        // Monotone: a rising write rate may only ever ask for more free space.
        EXPECT_LE(loaded[i], loaded[i - 1]);
    }
    EXPECT_DOUBLE_EQ(loaded.back(), kBase);
    EXPECT_GT(policy->Stats().write_bytes_per_second, 0.0);
}

// The mirror image: a target stuck at the floor after the burst ends throws
// away cache the node could have kept, which is the fixed-watermark failure
// this controller exists to remove. The recovery has to be smooth, because a
// target that jumped straight back up would order a fresh reclaim round the
// moment the load returned.
TEST_F(EvictionPolicyTest, AnIdleTierDriftsBackUpTowardsTheCeiling) {
    auto policy = Policy(BandConfig());
    const TierCapacitySnapshot snapshot = UsedSnapshot(kCapacity, 100'000);

    const std::vector<double> loaded =
        RunLoad(*policy, 10, 200'000, 50ms, snapshot);
    ASSERT_DOUBLE_EQ(loaded.back(), kBase);

    const std::vector<double> recovering = RunIdle(*policy, 15, snapshot);
    for (size_t i = 1; i < recovering.size(); ++i) {
        EXPECT_GE(recovering[i], recovering[i - 1]);
    }
    // Smooth, not a step: the first interval of quiet must not already be at
    // the ceiling.
    EXPECT_LT(recovering.front(), kIdle);
    EXPECT_GT(recovering.back(), recovering.front());
    EXPECT_NEAR(recovering.back(), kIdle, 1e-3);
    // And the write-rate estimate got there on the injected clock alone.
    EXPECT_LT(policy->Stats().write_bytes_per_second, 1.0);
}

// base_target is the operator's contract -- reclamation may keep more free
// space than asked for, never less -- and idle_target is the ceiling that
// keeps the target strictly under the limit. A target outside the band means
// either a tier that ignores its configured watermark or a controller whose
// trigger has climbed past the hard limit and can no longer fire.
TEST_F(EvictionPolicyTest, TheTargetNeverLeavesTheConfiguredBand) {
    auto policy = Policy(BandConfig());

    // A deterministic walk over bursts, quiet stretches, a full tier and a
    // tier that reports no capacity at all.
    const size_t writes[] = {0, 1u << 20, 4096, 0, 1u << 22, 0, 0, 64};
    const int millis[] = {1, 500, 7, 3000, 40, 250, 5, 900};
    const size_t used[] = {0, 999'999, 500'000, 850'000};
    const size_t caps[] = {kCapacity, kCapacity, 0, kCapacity};

    for (int i = 0; i < 200; ++i) {
        clock_->Advance(std::chrono::milliseconds{millis[i % 8]});
        policy->RecordWrite(writes[i % 8]);
        const ReclaimPlan plan =
            policy->Plan(UsedSnapshot(caps[i % 4], used[i % 4]));
        ASSERT_GE(plan.dynamic_target, kBase) << "step " << i;
        ASSERT_LE(plan.dynamic_target, kIdle) << "step " << i;
        ASSERT_GE(plan.trigger_watermark, plan.dynamic_target) << "step " << i;
        ASSERT_LE(plan.trigger_watermark, kLimit) << "step " << i;
        ASSERT_TRUE(std::isfinite(plan.usage_ratio)) << "step " << i;
        ASSERT_TRUE(std::isfinite(plan.write_bytes_per_second)) << "step " << i;
    }
}

// The gap between trigger and target is the whole point of the hysteresis.
// Without the latch a tier parked just above its target would start a reclaim
// round on one commit and abandon it on the next, so the engine would spend
// its time setting up and tearing down rounds that free nothing, and the
// eviction index would be walked over and over for no reclaimed bytes.
TEST_F(EvictionPolicyTest, AReclaimRoundRunsFromTheTriggerDownToTheTarget) {
    auto policy = Policy(PinnedConfig());  // target 0.8, trigger 0.85.

    auto plan_at = [&](size_t used) {
        clock_->Advance(1ms);
        return policy->Plan(UsedSnapshot(kCapacity, used));
    };

    // Above the target but below the trigger: no round starts.
    EXPECT_EQ(plan_at(840'000).urgency, ReclaimUrgency::kNone);
    // Crossing the trigger starts one.
    const ReclaimPlan started = plan_at(860'000);
    EXPECT_EQ(started.urgency, ReclaimUrgency::kBackground);
    EXPECT_EQ(started.target_bytes, 60'000u);
    EXPECT_NEAR(started.trigger_watermark, 0.85, 1e-12);
    // Back under the trigger, still above the target: the round continues.
    EXPECT_EQ(plan_at(840'000).urgency, ReclaimUrgency::kBackground);
    EXPECT_EQ(plan_at(810'000).urgency, ReclaimUrgency::kBackground);
    // Reaching the target ends it.
    EXPECT_EQ(plan_at(790'000).urgency, ReclaimUrgency::kNone);
    EXPECT_EQ(policy->Stats().background_triggers, 1u);

    // Hovering between the target and the trigger must not restart anything,
    // however many commits land.
    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(plan_at(i % 2 == 0 ? 840'000 : 820'000).urgency,
                  ReclaimUrgency::kNone);
    }
    EXPECT_EQ(policy->Stats().background_triggers, 1u);
    EXPECT_EQ(policy->Stats().foreground_triggers, 0u);
}

// At or above the limit the tier is one allocation away from failing, so the
// round cannot wait behind background work. A background answer here would
// leave request-path Puts queued behind whatever the engine happened to be
// doing.
TEST_F(EvictionPolicyTest, ReachingTheLimitIsForeground) {
    auto policy = Policy(PinnedConfig());

    const ReclaimPlan plan = policy->Plan(UsedSnapshot(kCapacity, 960'000));
    EXPECT_EQ(plan.urgency, ReclaimUrgency::kForeground);
    // Down to the target, not merely back under the limit.
    EXPECT_EQ(plan.target_bytes, 160'000u);
    EXPECT_EQ(plan.next_check, 0ms);
    EXPECT_DOUBLE_EQ(plan.usage_ratio, 0.96);
    EXPECT_EQ(policy->Stats().foreground_triggers, 1u);
}

// Waiting for the limit to arrive is waiting too long when the write stream
// will cross it inside one horizon: the reclaim round takes time the incoming
// bytes will not give it. The control case proves the projection fired and
// not the raw usage.
TEST_F(EvictionPolicyTest, AProjectedLimitCrossingIsForeground) {
    auto policy = Policy(PinnedConfig());
    const TierCapacitySnapshot snapshot = UsedSnapshot(kCapacity, 900'000);

    // 200 KB in flight over a 200ms horizon is far more than the 50 KB of
    // headroom left under the limit.
    policy->RecordWrite(200'000);
    const ReclaimPlan projected = policy->Plan(snapshot);
    EXPECT_EQ(projected.urgency, ReclaimUrgency::kForeground);
    // The target covers the incoming bytes, not just today's excess.
    EXPECT_GT(projected.target_bytes, 100'000u);

    EvictionPolicyConfig no_horizon = PinnedConfig();
    no_horizon.headroom_horizon = 0ms;
    auto blind = Policy(no_horizon);
    blind->RecordWrite(200'000);
    const ReclaimPlan unprojected = blind->Plan(snapshot);
    EXPECT_EQ(unprojected.urgency, ReclaimUrgency::kBackground);
    EXPECT_EQ(unprojected.target_bytes, 100'000u);
}

// A caller is blocked on this allocation. Anything less than the failed size
// leaves it blocked after a full reclaim round, and a background answer would
// let it wait behind the periodic loop's sleep.
TEST_F(EvictionPolicyTest, AllocationFailureIsForegroundAndCoversTheSize) {
    auto policy = Policy(PinnedConfig());

    // Well below every watermark: the watermarks do not get a vote.
    const ReclaimPlan idle_tier = policy->PlanForAllocationFailure(
        UsedSnapshot(kCapacity, 100'000), 4096);
    EXPECT_EQ(idle_tier.urgency, ReclaimUrgency::kForeground);
    EXPECT_TRUE(idle_tier.ShouldReclaim());
    EXPECT_GE(idle_tier.target_bytes, 4096u);
    EXPECT_EQ(idle_tier.next_check, 0ms);

    // Above the target, the two demands add up rather than replace each other.
    const ReclaimPlan busy_tier = policy->PlanForAllocationFailure(
        UsedSnapshot(kCapacity, 900'000), 4096);
    EXPECT_EQ(busy_tier.urgency, ReclaimUrgency::kForeground);
    EXPECT_GE(busy_tier.target_bytes, 4096u);
    EXPECT_EQ(busy_tier.target_bytes, 100'000u);

    // Every blocked caller is its own event; collapsing a run of them would
    // hide the storm this counter exists to show.
    EXPECT_EQ(policy->Stats().foreground_triggers, 2u);
}

// A misconfigured or not-yet-sized tier reports zero capacity. Dividing by it
// yields NaN, and NaN compares false against every watermark -- the limit
// check would silently stop firing on every tier that shared this policy.
TEST_F(EvictionPolicyTest, AZeroCapacityTierYieldsFiniteRatiosAndNoReclaim) {
    auto policy = Policy(BandConfig());
    TierCapacitySnapshot snapshot;
    snapshot.capacity = 0;
    snapshot.indexed_bytes = 4096;
    snapshot.physical_used_bytes = 4096;
    snapshot.reserved_bytes = 2048;
    snapshot.pending_migration_bytes = 1024;
    snapshot.retired_pinned_bytes = 512;

    const ReclaimPlan plan = policy->Plan(snapshot);
    EXPECT_EQ(plan.urgency, ReclaimUrgency::kNone);
    EXPECT_EQ(plan.target_bytes, 0u);
    EXPECT_EQ(plan.next_check, kInterval);
    EXPECT_TRUE(std::isfinite(plan.usage_ratio));
    EXPECT_TRUE(std::isfinite(plan.dynamic_target));
    EXPECT_TRUE(std::isfinite(plan.trigger_watermark));
    EXPECT_TRUE(std::isfinite(plan.write_bytes_per_second));

    // The blocked-caller path still answers, and still answers finitely.
    const ReclaimPlan failure =
        policy->PlanForAllocationFailure(snapshot, 8192);
    EXPECT_EQ(failure.urgency, ReclaimUrgency::kForeground);
    EXPECT_GE(failure.target_bytes, 8192u);
    EXPECT_TRUE(std::isfinite(failure.usage_ratio));
}

// Detaching a pinned block removes it from the index but not from the pool
// (section 4.3). A controller that believed the index alone would watch usage
// fall, declare the round finished and stop -- while the pool stayed full and
// the next allocation failed anyway.
TEST_F(EvictionPolicyTest, RetiredPinnedBytesKeepTheTierLookingFull) {
    auto policy = Policy(PinnedConfig());
    TierCapacitySnapshot snapshot;
    snapshot.capacity = kCapacity;
    // 600 KB was detached and is still held by readers.
    snapshot.indexed_bytes = 300'000;
    snapshot.physical_used_bytes = 900'000;
    snapshot.retired_pinned_bytes = 600'000;

    const ReclaimPlan plan = policy->Plan(snapshot);
    EXPECT_DOUBLE_EQ(plan.usage_ratio, 0.9);
    EXPECT_EQ(plan.urgency, ReclaimUrgency::kBackground);
    EXPECT_EQ(plan.target_bytes, 100'000u);
}

// Reserved and pending-migration bytes are already spoken for. Leaving them
// out would let the controller admit a burst into space that is owed to an
// in-flight Put or to a migration whose destination is already scheduled.
TEST_F(EvictionPolicyTest, ReservedAndPendingMigrationBytesCountAsUsed) {
    auto policy = Policy(PinnedConfig());
    TierCapacitySnapshot snapshot;
    snapshot.capacity = kCapacity;
    snapshot.indexed_bytes = 700'000;
    snapshot.physical_used_bytes = 700'000;
    snapshot.reserved_bytes = 100'000;
    snapshot.pending_migration_bytes = 60'000;

    const ReclaimPlan plan = policy->Plan(snapshot);
    EXPECT_DOUBLE_EQ(plan.usage_ratio, 0.86);
    EXPECT_EQ(plan.urgency, ReclaimUrgency::kBackground);
    EXPECT_EQ(plan.target_bytes, 60'000u);
}

// The estimate must be a function of the injected clock only. If it read the
// real clock, the half-life would depend on how long the test process was
// descheduled -- and in production a tier would look busy for as long as the
// wall clock said so after the writes had stopped.
TEST_F(EvictionPolicyTest, TheWriteRateHalvesEveryHalfLifeOnTheFakeClock) {
    auto policy = Policy(BandConfig());
    policy->RecordWrite(1u << 20);

    const double initial = policy->Stats().write_bytes_per_second;
    ASSERT_GT(initial, 0.0);
    // Two reads at the same instant must agree exactly; a wall-clock read
    // would drift between them.
    EXPECT_DOUBLE_EQ(policy->Stats().write_bytes_per_second, initial);

    clock_->Advance(kHalfLife);
    EXPECT_NEAR(policy->Stats().write_bytes_per_second, initial / 2.0,
                initial * 1e-9);

    clock_->Advance(10 * kHalfLife);
    EXPECT_LT(policy->Stats().write_bytes_per_second, initial * 1e-3);
}

// Every link of 0 < base <= idle < limit <= 1 is load-bearing. A broken chain
// reaches the controller as a trigger above the hard limit (reclamation that
// never starts), a stop line above the start line (a round that never ends)
// or a division by zero in the estimator -- all of them at runtime, on a node
// that is already under memory pressure.
TEST_F(EvictionPolicyTest, ValidateRejectsEveryBrokenLinkInTheChain) {
    EXPECT_TRUE(
        ValidateEvictionPolicyConfig(EvictionPolicyConfig{}).has_value());
    EXPECT_TRUE(ValidateEvictionPolicyConfig(BandConfig()).has_value());
    // base == idle is legal: it simply pins the target.
    EXPECT_TRUE(ValidateEvictionPolicyConfig(PinnedConfig()).has_value());

    auto rejects = [](auto mutate) {
        EvictionPolicyConfig config = BandConfig();
        mutate(config);
        auto result = ValidateEvictionPolicyConfig(config);
        EXPECT_FALSE(result.has_value());
        if (!result) EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_FALSE(CreateEvictionPolicy(config, std::make_shared<FakeClock>())
                         .has_value());
    };

    rejects([](auto& c) { c.base_target_watermark = 0.0; });
    rejects([](auto& c) { c.base_target_watermark = -0.1; });
    rejects([](auto& c) { c.base_target_watermark = std::nan(""); });
    // base above idle inverts the band.
    rejects([](auto& c) { c.base_target_watermark = 0.95; });
    rejects([](auto& c) { c.idle_target_watermark = 0.5; });
    // idle must stay strictly under the limit.
    rejects([](auto& c) { c.idle_target_watermark = kLimit; });
    rejects([](auto& c) { c.idle_target_watermark = 0.99; });
    rejects([](auto& c) { c.limit_watermark = kIdle; });
    rejects([](auto& c) { c.limit_watermark = 0.5; });
    rejects([](auto& c) { c.limit_watermark = 1.01; });
    rejects([](auto& c) { c.limit_watermark = std::nan(""); });
    // A negative hysteresis puts the start line below the stop line.
    rejects([](auto& c) { c.watermark_hysteresis = -0.01; });
    // The half-life divides the rate estimate.
    rejects([](auto& c) { c.ewma_half_life = 0ms; });
    rejects([](auto& c) { c.ewma_half_life = -1ms; });
    // A zero interval turns the idle controller into a spin loop.
    rejects([](auto& c) { c.controller_interval = 0ms; });
    rejects([](auto& c) { c.controller_interval = -1ms; });
    // A negative horizon projects the tier backwards in time.
    rejects([](auto& c) { c.headroom_horizon = -1ms; });

    // The boundaries themselves are legal and must not be rounded away.
    EvictionPolicyConfig full_limit = BandConfig();
    full_limit.limit_watermark = 1.0;
    EXPECT_TRUE(ValidateEvictionPolicyConfig(full_limit).has_value());
    EvictionPolicyConfig no_hysteresis = BandConfig();
    no_hysteresis.watermark_hysteresis = 0.0;
    EXPECT_TRUE(ValidateEvictionPolicyConfig(no_hysteresis).has_value());
    EvictionPolicyConfig no_horizon = BandConfig();
    no_horizon.headroom_horizon = 0ms;
    EXPECT_TRUE(ValidateEvictionPolicyConfig(no_horizon).has_value());
}

// A factory that fell back to a default policy on an unknown type, or to the
// real clock on a null one, would turn a typo in a config file into a silent
// change of eviction behaviour on a live node.
TEST_F(EvictionPolicyTest, CreateRejectsAnUnknownTypeAndAMissingClock) {
    EvictionPolicyConfig unknown = BandConfig();
    unknown.type = "lru";
    auto rejected = CreateEvictionPolicy(unknown, clock_);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::INVALID_PARAMS);

    EvictionPolicyConfig empty_type = BandConfig();
    empty_type.type.clear();
    EXPECT_FALSE(CreateEvictionPolicy(empty_type, clock_).has_value());

    auto no_clock = CreateEvictionPolicy(BandConfig(), nullptr);
    ASSERT_FALSE(no_clock.has_value());
    EXPECT_EQ(no_clock.error(), ErrorCode::INVALID_PARAMS);

    EXPECT_TRUE(CreateEvictionPolicy(BandConfig(), clock_).has_value());
}

// Stats is what an operator sees when a tier is misbehaving. A trigger that
// disagreed with the plans, or counters that stood still, would send them
// looking at the engine for a decision the policy made.
TEST_F(EvictionPolicyTest, StatsAgreeWithThePlansTheyDescribe) {
    auto policy = Policy(PinnedConfig());

    const ReclaimPlan quiet = policy->Plan(UsedSnapshot(kCapacity, 100'000));
    const ReclaimPlan busy = policy->Plan(UsedSnapshot(kCapacity, 960'000));
    ASSERT_EQ(quiet.urgency, ReclaimUrgency::kNone);
    ASSERT_EQ(busy.urgency, ReclaimUrgency::kForeground);

    const EvictionPolicyStats stats = policy->Stats();
    EXPECT_EQ(stats.plans, 2u);
    EXPECT_EQ(stats.foreground_triggers, 1u);
    EXPECT_EQ(stats.background_triggers, 0u);
    EXPECT_DOUBLE_EQ(stats.dynamic_target, busy.dynamic_target);
    EXPECT_DOUBLE_EQ(stats.trigger_watermark, busy.trigger_watermark);
    EXPECT_DOUBLE_EQ(stats.write_bytes_per_second, busy.write_bytes_per_second);
}

TEST_F(EvictionPolicyTest, ToStringNamesEveryUrgency) {
    // The engine logs these; an "unknown" in a reclaim log is a dead end.
    EXPECT_STREQ(ToString(ReclaimUrgency::kNone), "none");
    EXPECT_STREQ(ToString(ReclaimUrgency::kBackground), "background");
    EXPECT_STREQ(ToString(ReclaimUrgency::kForeground), "foreground");
}

}  // namespace

// ---------------------------------------------------------------------------
// The plan must be actionable, or the controller live-locks
// ---------------------------------------------------------------------------

// A plan that says "reclaim" and then asks for zero bytes, with next_check of
// zero, is an absorbing state: the round frees nothing, usage does not move,
// and the controller re-plans immediately -- forever. It has to report kNone
// and wait out the interval instead.
TEST_F(EvictionPolicyTest, APlanNeverAsksForZeroBytesWhileClaimingToReclaim) {
    EvictionPolicyConfig config = PinnedConfig();
    config.watermark_hysteresis = 0.0;  // trigger == target
    config.headroom_horizon = 0ms;
    auto policy = Policy(config);

    // Exactly on the target, so usage >= trigger fires while the excess over
    // the target is zero.
    const ReclaimPlan plan = policy->Plan(
        UsedSnapshot(kCapacity, static_cast<size_t>(0.8 * kCapacity)));
    if (plan.ShouldReclaim()) {
        EXPECT_GT(plan.target_bytes, 0U)
            << "a reclaim plan that asks for nothing cannot make progress";
    } else {
        EXPECT_EQ(plan.next_check, config.controller_interval);
    }
}

// Retired-but-pinned bytes are detached and held by a reader. They count
// towards "how full is this tier" and towards nothing else: eviction cannot
// free them, so a target sized from them is a target no round can meet.
TEST_F(EvictionPolicyTest, PinnedBytesAreNotOfferedAsReclaimTarget) {
    auto policy = Policy(PinnedConfig());

    TierCapacitySnapshot snapshot;
    snapshot.capacity = kCapacity;
    snapshot.indexed_bytes = 0;
    snapshot.physical_used_bytes = 980'000;
    snapshot.retired_pinned_bytes = 980'000;  // every byte held by a reader

    const ReclaimPlan plan = policy->Plan(snapshot);
    EXPECT_FALSE(plan.ShouldReclaim())
        << "nothing here is reclaimable, so a round would free zero bytes and "
           "the controller would spin";
    EXPECT_GT(plan.next_check.count(), 0);
}

// Same shape, reserved rather than pinned: the space is promised to a writer
// that has not committed yet, and eviction has nothing to take.
TEST_F(EvictionPolicyTest, ReservedBytesAreNotOfferedAsReclaimTarget) {
    auto policy = Policy(PinnedConfig());

    TierCapacitySnapshot snapshot;
    snapshot.capacity = kCapacity;
    snapshot.indexed_bytes = 0;
    snapshot.physical_used_bytes = 0;
    snapshot.reserved_bytes = 990'000;

    const ReclaimPlan plan = policy->Plan(snapshot);
    EXPECT_FALSE(plan.ShouldReclaim());
    EXPECT_GT(plan.next_check.count(), 0);
}

// The horizon projection adds future writes to the excess, which can exceed
// the tier itself. Ordering a reclaim larger than capacity tells the engine to
// walk its whole index for bytes that were never there.
TEST_F(EvictionPolicyTest, TheTargetNeverExceedsTheTier) {
    EvictionPolicyConfig config = PinnedConfig();
    config.headroom_horizon = 10s;  // a long projection
    auto policy = Policy(config);

    // A heavy write burst, so the projected bytes dwarf the tier.
    for (int i = 0; i < 50; ++i) {
        policy->RecordWrite(kCapacity);
        clock_->Advance(1ms);
    }
    const ReclaimPlan plan = policy->Plan(
        UsedSnapshot(kCapacity, static_cast<size_t>(0.96 * kCapacity)));
    EXPECT_LE(plan.target_bytes, kCapacity);
    // And it is still a real request: this tier genuinely is over the limit.
    EXPECT_TRUE(plan.ShouldReclaim());
    EXPECT_GT(plan.target_bytes, 0U);
}

// A blocked caller still gets a round even when the snapshot says nothing is
// reclaimable: the allocation may have raced a reader that has since finished,
// and refusing to try would fail a request that could have succeeded. This is
// bounded by the caller's own round budget, not by this function.
TEST_F(EvictionPolicyTest, AnAllocationFailureAlwaysGetsAForegroundRound) {
    auto policy = Policy(PinnedConfig());

    TierCapacitySnapshot snapshot;
    snapshot.capacity = kCapacity;
    snapshot.physical_used_bytes = 980'000;
    snapshot.retired_pinned_bytes = 980'000;

    const ReclaimPlan plan = policy->PlanForAllocationFailure(snapshot, 4096);
    EXPECT_EQ(plan.urgency, ReclaimUrgency::kForeground);
    EXPECT_GE(plan.target_bytes, 4096U);
}

}  // namespace mooncake::v2
