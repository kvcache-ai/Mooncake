// Component tests for MovementTracker (design doc section 6.3, and the
// "every exit path settles" invariant of section 9).
//
// Time is injected: every assertion below about a cooldown or a residency
// window is made against the clock the tracker was handed, so an
// implementation that reached for steady_clock::now() would fail here instead
// of passing by luck.
//
// Nothing in this file constructs a block. The tracker deliberately knows
// nothing about block contents or locations -- it is handed a dedup identity
// and a key, and the tests speak the same language.

#include "p2p/client/v2/movement_tracker.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

using namespace std::chrono_literals;

/**
 * @class FakeClock
 * @brief The only time source in this file. It starts well away from the
 *        epoch so that "never moved" (a default-constructed time_point) stays
 *        distinguishable from "moved at the current instant".
 */
class FakeClock final : public Clock {
   public:
    time_point Now() const override {
        return now_.load(std::memory_order_acquire);
    }

    void Advance(std::chrono::milliseconds delta) {
        now_.store(now_.load(std::memory_order_acquire) + delta,
                   std::memory_order_release);
    }

   private:
    std::atomic<time_point> now_{Clock::time_point{} + 24h};
};

/**
 * @class StopOnReadClock
 * @brief Calls Stop() the first time the tracker asks it for the time.
 *
 * TryAcquire reads the clock after it has tested the stop flag and before it
 * takes the shard lock, so this reproduces -- with no threads and no sleeping,
 * hence no flakiness -- the preemption where a Stop() lands in the middle of
 * an acquire that is already past the flag.
 */
class StopOnReadClock final : public Clock {
   public:
    void Attach(MovementTracker* tracker) { tracker_ = tracker; }

    time_point Now() const override {
        if (tracker_ != nullptr && !fired_.exchange(true)) tracker_->Stop();
        return Clock::time_point{} + 24h;
    }

   private:
    MovementTracker* tracker_ = nullptr;
    mutable std::atomic<bool> fired_{false};
};

constexpr UUID kFastTiler{0xFA57, 0x0001};
constexpr UUID kSlowTiler{0x5104, 0x0002};

constexpr std::chrono::milliseconds kCooldown{2000};
constexpr std::chrono::milliseconds kResidency{1000};

}  // namespace

class MovementTrackerTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag once;
        std::call_once(once, [] {
            google::InitGoogleLogging("MovementTrackerTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        clock_ = std::make_shared<FakeClock>();
        MovementTrackerConfig config;
        config.shard_count = 4;
        config.cooldown = kCooldown;
        config.minimum_residency = kResidency;
        tracker_ = MakeTracker(config);
    }

    std::unique_ptr<MovementTracker> MakeTracker(
        const MovementTrackerConfig& config) {
        EXPECT_TRUE(ValidateMovementTrackerConfig(config).has_value());
        return std::make_unique<MovementTracker>(config, clock_);
    }

    // Derived from the key so that two calls for the same key and the same
    // pair of tiers produce the same identity -- which is precisely what the
    // dedup check is supposed to notice.
    static MovementDedupKey DedupFor(std::string_view key, const UUID& source,
                                     const UUID& destination) {
        const uint64_t seq =
            static_cast<uint64_t>(std::hash<std::string_view>{}(key));
        MovementDedupKey dedup;
        dedup.registration_id = RegistrationId{0, seq};
        dedup.source_block_id = BlockId{source, seq, 1};
        dedup.source_tiler = source;
        dedup.destination_tiler = destination;
        return dedup;
    }

    // What the same name looks like after a delete and a fresh Put: a new
    // registration and a new block, so it is a different movement even though
    // the tiers and the key string are unchanged.
    static MovementDedupKey RecreatedDedupFor(std::string_view key,
                                              const UUID& source,
                                              const UUID& destination) {
        MovementDedupKey dedup = DedupFor(key, source, destination);
        dedup.registration_id.shard_sequence += 1;
        dedup.source_block_id.generation += 1;
        return dedup;
    }

    tl::expected<MovementLease, MovementRejection> Acquire(
        std::string_view key,
        MovementDirection direction = MovementDirection::kOffload,
        const UUID& source = kFastTiler, const UUID& destination = kSlowTiler) {
        return tracker_->TryAcquire(key, DedupFor(key, source, destination),
                                    direction);
    }

    std::shared_ptr<FakeClock> clock_;
    std::unique_ptr<MovementTracker> tracker_;
};

// A config that would divide by zero or bound nothing has to be rejected at
// startup: the tracker is constructed once, and every later decision assumes
// these fields are sane.
TEST_F(MovementTrackerTest, ValidateConfigRejectsUnusableValues) {
    EXPECT_TRUE(
        ValidateMovementTrackerConfig(MovementTrackerConfig{}).has_value());

    MovementTrackerConfig no_shards;
    no_shards.shard_count = 0;
    auto shards_result = ValidateMovementTrackerConfig(no_shards);
    ASSERT_FALSE(shards_result.has_value());
    EXPECT_EQ(shards_result.error(), ErrorCode::INVALID_PARAMS);

    MovementTrackerConfig unbounded;
    unbounded.max_tracked_keys = 0;
    EXPECT_FALSE(ValidateMovementTrackerConfig(unbounded).has_value());

    MovementTrackerConfig negative;
    negative.cooldown = -1ms;
    EXPECT_FALSE(ValidateMovementTrackerConfig(negative).has_value());

    // Zero is a legal "gate off" value, not an error: a benchmark asks for it
    // deliberately.
    MovementTrackerConfig eager;
    eager.cooldown = 0ms;
    eager.minimum_residency = 0ms;
    EXPECT_TRUE(ValidateMovementTrackerConfig(eager).has_value());
}

// These strings end up in operator-facing logs and counters; renaming one
// silently would break whatever dashboard greps for it.
TEST_F(MovementTrackerTest, EnumNamesAreStable) {
    EXPECT_STREQ(ToString(MovementDirection::kOffload), "offload");
    EXPECT_STREQ(ToString(MovementDirection::kOnboard), "onboard");
    EXPECT_STREQ(ToString(MovementRejection::kInflight), "inflight");
    EXPECT_STREQ(ToString(MovementRejection::kCooldown), "cooldown");
    EXPECT_STREQ(ToString(MovementRejection::kMinimumResidency),
                 "minimum_residency");
    EXPECT_STREQ(ToString(MovementRejection::kStopped), "stopped");
    EXPECT_STREQ(ToString(MovementRejection::kInvalid), "invalid");
}

// The reason the component exists: without it two consumers (or one consumer
// run twice) would emit two commands for one block, and the second copy would
// race the first for the same destination.
TEST_F(MovementTrackerTest, SecondAcquireIsRejectedAsInflight) {
    auto first = Acquire("alpha");
    ASSERT_TRUE(first.has_value());

    auto second = Acquire("alpha");
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error(), MovementRejection::kInflight);

    auto stats = tracker_->Stats();
    EXPECT_EQ(stats.inflight, 1u);
    EXPECT_EQ(stats.acquired, 1u);
    EXPECT_EQ(stats.rejected_inflight, 1u);
    EXPECT_EQ(stats.rejected_cooldown, 0u);
    EXPECT_EQ(stats.rejected_residency, 0u);
}

// The dedup slot must be reusable after a settle. If it were not, one failed
// movement would make the key permanently unmovable.
TEST_F(MovementTrackerTest, AcquirableAgainAfterSettle) {
    auto first = Acquire("alpha");
    ASSERT_TRUE(first.has_value());
    first->Settle(/*moved=*/false);
    EXPECT_EQ(tracker_->Stats().inflight, 0u);

    auto second = Acquire("alpha");
    EXPECT_TRUE(second.has_value());

    auto stats = tracker_->Stats();
    EXPECT_EQ(stats.acquired, 2u);
    EXPECT_EQ(stats.settled_unmoved, 1u);
    EXPECT_EQ(stats.settled_moved, 0u);
}

// A successful move must hold the key still for a while: without the
// cooldown, a block sitting at the offload threshold would be shuttled
// between tiers on every policy pass.
TEST_F(MovementTrackerTest, SuccessfulMoveStartsCooldownThatExpires) {
    auto lease = Acquire("beta");
    ASSERT_TRUE(lease.has_value());
    lease->Settle(/*moved=*/true);

    auto blocked = Acquire("beta");
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), MovementRejection::kCooldown);

    // One millisecond short is still inside the window; an implementation
    // using <= or the wrong endpoint would let this through.
    clock_->Advance(kCooldown - 1ms);
    auto still_blocked = Acquire("beta");
    ASSERT_FALSE(still_blocked.has_value());
    EXPECT_EQ(still_blocked.error(), MovementRejection::kCooldown);

    clock_->Advance(1ms);
    EXPECT_TRUE(Acquire("beta").has_value());
    EXPECT_EQ(tracker_->Stats().rejected_cooldown, 2u);
}

// A movement that did not happen must not suppress the retry that is supposed
// to follow it: charging a cooldown for a failure would turn one transient
// error into `cooldown` milliseconds of doing nothing about a hot key.
TEST_F(MovementTrackerTest, UnmovedSettleStartsNoCooldown) {
    auto lease = Acquire("gamma");
    ASSERT_TRUE(lease.has_value());
    lease->Settle(/*moved=*/false);

    auto retry = Acquire("gamma");
    EXPECT_TRUE(retry.has_value());
    EXPECT_EQ(tracker_->Stats().rejected_cooldown, 0u);
}

// Onboard and offload have different natural timescales; sharing one cooldown
// would mean a block that was just onboarded could not be offloaded again
// even under memory pressure.
TEST_F(MovementTrackerTest, CooldownIsPerDirection) {
    auto offload =
        Acquire("delta", MovementDirection::kOffload, kFastTiler, kSlowTiler);
    ASSERT_TRUE(offload.has_value());
    offload->Settle(/*moved=*/true);

    auto onboard =
        Acquire("delta", MovementDirection::kOnboard, kSlowTiler, kFastTiler);
    EXPECT_TRUE(onboard.has_value());

    // The direction that did move is still held back.
    auto again =
        Acquire("delta", MovementDirection::kOffload, kFastTiler, kSlowTiler);
    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(again.error(), MovementRejection::kCooldown);
}

// Moving a block that just landed wastes the copy that put it there, so the
// residency clock starts at the commit and is measured for the source tier.
TEST_F(MovementTrackerTest, MinimumResidencyBlocksAFreshCommit) {
    tracker_->OnCommitted("epsilon", kFastTiler);

    auto blocked = Acquire("epsilon");
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), MovementRejection::kMinimumResidency);

    clock_->Advance(kResidency - 1ms);
    auto still_blocked = Acquire("epsilon");
    ASSERT_FALSE(still_blocked.has_value());
    EXPECT_EQ(still_blocked.error(), MovementRejection::kMinimumResidency);

    clock_->Advance(1ms);
    EXPECT_TRUE(Acquire("epsilon").has_value());
    EXPECT_EQ(tracker_->Stats().rejected_residency, 2u);
}

// A block committed before this tracker existed, or one whose record the cap
// dropped, has no commit timestamp. Blocking on that would make it immovable
// for the life of the process, because nothing re-announces an old commit.
TEST_F(MovementTrackerTest, MissingResidencyRecordDoesNotBlock) {
    EXPECT_TRUE(Acquire("zeta").has_value());

    // A commit on some other tier is equally irrelevant: residency is asked
    // about the tier the block is being moved off.
    tracker_->OnCommitted("eta", kSlowTiler);
    EXPECT_TRUE(
        Acquire("eta", MovementDirection::kOffload, kFastTiler, kSlowTiler)
            .has_value());
    EXPECT_EQ(tracker_->Stats().rejected_residency, 0u);
}

// After the last replica is gone the key's policy state is meaningless. Left
// behind, it would both leak memory and hold a cooldown against a key that a
// later Put recreates.
TEST_F(MovementTrackerTest, OnDeletedForgetsTheKey) {
    tracker_->OnCommitted("theta", kFastTiler);
    clock_->Advance(kResidency);
    auto lease = Acquire("theta");
    ASSERT_TRUE(lease.has_value());
    lease->Settle(/*moved=*/true);
    auto blocked = Acquire("theta");
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), MovementRejection::kCooldown);

    tracker_->OnDeleted("theta");
    EXPECT_EQ(tracker_->Stats().tracked_keys, 0u);
    EXPECT_TRUE(Acquire("theta").has_value());

    // A delete that races an in-flight movement forgets the record but keeps
    // the dedup slot -- the movement is still running, and only its own
    // settle may free it. (This test used to assert inflight == 0 here, which
    // recorded the bug: the slot was dropped and could be handed out twice.)
    // The outstanding lease must still settle against a tracker that no
    // longer knows the key.
    tracker_->OnDeleted("theta");
    {
        auto racing = Acquire("iota");
        ASSERT_TRUE(racing.has_value());
        tracker_->OnDeleted("iota");
        EXPECT_EQ(tracker_->Stats().tracked_keys, 0u);
        EXPECT_EQ(tracker_->Stats().inflight, 1u);
        racing->Settle(/*moved=*/true);
        EXPECT_EQ(tracker_->Stats().inflight, 0u);
    }
    // The settle recorded no cooldown: the block it moved is gone, and the
    // record that would have carried the cooldown went with it.
    EXPECT_TRUE(Acquire("iota").has_value());
}

// The whole reason the lease is RAII: the executor has many early returns, and
// one that forgets to settle would wedge that key's dedup slot forever.
TEST_F(MovementTrackerTest, DroppedLeaseIsSettledByItsDestructor) {
    {
        auto lease = Acquire("kappa");
        ASSERT_TRUE(lease.has_value());
        EXPECT_EQ(tracker_->Stats().inflight, 1u);
    }

    auto stats = tracker_->Stats();
    EXPECT_EQ(stats.inflight, 0u);
    EXPECT_EQ(stats.settled_unmoved, 1u);
    EXPECT_EQ(stats.settled_moved, 0u);

    // Dropped means "no outcome", so no cooldown was earned and the retry is
    // allowed straight away.
    EXPECT_TRUE(Acquire("kappa").has_value());
}

// Overwriting a live lease must release it first. If it did not, the
// overwritten key's dedup slot would be lost with no owner left to free it.
TEST_F(MovementTrackerTest, MoveAssignmentSettlesTheOverwrittenLease) {
    auto first = Acquire("lambda");
    ASSERT_TRUE(first.has_value());
    auto second = Acquire("mu");
    ASSERT_TRUE(second.has_value());
    MovementLease lambda_lease = std::move(first.value());
    MovementLease mu_lease = std::move(second.value());
    EXPECT_EQ(tracker_->Stats().inflight, 2u);

    lambda_lease = std::move(mu_lease);
    auto after_assign = tracker_->Stats();
    EXPECT_EQ(after_assign.inflight, 1u);
    EXPECT_EQ(after_assign.settled_unmoved, 1u);
    EXPECT_TRUE(lambda_lease.Key() == DedupFor("mu", kFastTiler, kSlowTiler));

    // The surviving lease still names "mu", so settling it must charge mu's
    // cooldown and leave lambda free.
    lambda_lease.Settle(/*moved=*/true);
    auto blocked = Acquire("mu");
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), MovementRejection::kCooldown);
    EXPECT_TRUE(Acquire("lambda").has_value());
    EXPECT_EQ(tracker_->Stats().settled_moved, 1u);
}

// A moved-from lease that still released would settle a movement someone else
// owns, freeing a dedup slot while its command is still running.
TEST_F(MovementTrackerTest, MoveConstructionSettlesOnlyOnce) {
    auto acquired = Acquire("nu");
    ASSERT_TRUE(acquired.has_value());
    {
        MovementLease first = std::move(acquired.value());
        EXPECT_FALSE(static_cast<bool>(acquired.value()));
        MovementLease second = std::move(first);
        EXPECT_FALSE(static_cast<bool>(first));
        EXPECT_TRUE(static_cast<bool>(second));
        EXPECT_EQ(tracker_->Stats().inflight, 1u);
        second.Settle(/*moved=*/true);
        // A second settle on the same object is legal and must do nothing.
        second.Settle(/*moved=*/false);
    }

    auto stats = tracker_->Stats();
    EXPECT_EQ(stats.inflight, 0u);
    EXPECT_EQ(stats.settled_moved, 1u);
    EXPECT_EQ(stats.settled_unmoved, 0u);
}

// Shutdown closes admission only. Refusing the settle as well would leave the
// last few in-flight movements holding dedup slots that nothing frees, and
// section 9's invariant 10 says every path settles.
TEST_F(MovementTrackerTest, StopRejectsAcquiresButOutstandingLeaseSettles) {
    auto lease = Acquire("xi");
    ASSERT_TRUE(lease.has_value());

    tracker_->Stop();

    auto rejected = Acquire("omicron");
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), MovementRejection::kStopped);
    // Even the key that is already moving is refused, and for the shutdown
    // reason rather than the dedup reason.
    auto same_key = Acquire("xi");
    ASSERT_FALSE(same_key.has_value());
    EXPECT_EQ(same_key.error(), MovementRejection::kStopped);

    lease->Settle(/*moved=*/true);
    auto stats = tracker_->Stats();
    EXPECT_EQ(stats.inflight, 0u);
    EXPECT_EQ(stats.settled_moved, 1u);
}

// The record map is fed by every commit, so it has to be bounded. Forgetting
// a cooldown makes the policy slightly more eager, which is survivable;
// forgetting a record that has a lease out is not -- its settle would then
// record no cooldown at all and the key could immediately move back.
TEST_F(MovementTrackerTest, CapEvictsSettledRecordsNeverInflightOnes) {
    MovementTrackerConfig config;
    // One shard so the per-shard share is exactly max_tracked_keys.
    config.shard_count = 1;
    config.max_tracked_keys = 4;
    config.cooldown = kCooldown;
    config.minimum_residency = kResidency;
    tracker_ = MakeTracker(config);

    auto pinned = tracker_->TryAcquire(
        "pinned", DedupFor("pinned", kFastTiler, kSlowTiler),
        MovementDirection::kOffload);
    ASSERT_TRUE(pinned.has_value());

    for (int i = 0; i < 10; ++i) {
        tracker_->OnCommitted("filler-" + std::to_string(i), kFastTiler);
        clock_->Advance(1ms);
    }

    auto stats = tracker_->Stats();
    EXPECT_EQ(stats.tracked_keys, 4u);
    EXPECT_GE(stats.evicted_records, 1u);
    EXPECT_EQ(stats.inflight, 1u);

    // The most recently touched filler is the last one a trim would pick, so
    // its residency window must still be remembered.
    auto fresh = Acquire("filler-9");
    ASSERT_FALSE(fresh.has_value());
    EXPECT_EQ(fresh.error(), MovementRejection::kMinimumResidency);

    // The pinned record survived every trim: if it had not, this settle would
    // have found no record, stored no cooldown, and the re-acquire below
    // would succeed.
    pinned->Settle(/*moved=*/true);
    auto blocked = Acquire("pinned");
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), MovementRejection::kCooldown);
}

// A delete does not stop a copy that is already running. Freeing its dedup
// slot here would hand the same source block to a second command, and the
// first settle to arrive would then free the second movement's slot -- one
// block, three concurrent movements, which is the exact race this component
// exists to prevent.
TEST_F(MovementTrackerTest, DeleteDoesNotFreeALiveLeasesDedupSlot) {
    auto lease = Acquire("rho");
    ASSERT_TRUE(lease.has_value());

    tracker_->OnDeleted("rho");
    auto after_delete = tracker_->Stats();
    // The policy state is meaningless now; the movement is not.
    EXPECT_EQ(after_delete.tracked_keys, 0u);
    EXPECT_EQ(after_delete.inflight, 1u);

    // Same registration, same source block: this *is* the movement that is
    // already running, so proposing it again must still be refused.
    auto duplicate = Acquire("rho");
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), MovementRejection::kInflight);

    // And the settle frees exactly one slot: its own.
    lease->Settle(/*moved=*/false);
    EXPECT_EQ(tracker_->Stats().inflight, 0u);
    EXPECT_TRUE(Acquire("rho").has_value());
}

// A settle names its key by string, but the key may have been deleted and put
// again while the movement ran. Charging that cooldown to whatever answers to
// the name now would hold back a block this movement never touched.
TEST_F(MovementTrackerTest, SettleAfterDeleteDoesNotChargeTheRecreatedKey) {
    tracker_->OnCommitted("sigma", kFastTiler);
    clock_->Advance(kResidency);
    auto lease = Acquire("sigma");
    ASSERT_TRUE(lease.has_value());

    // The block being moved is deleted and the name is immediately put again:
    // a different block, with its own residency clock.
    tracker_->OnDeleted("sigma");
    tracker_->OnCommitted("sigma", kFastTiler);
    clock_->Advance(kResidency);

    lease->Settle(/*moved=*/true);
    EXPECT_EQ(tracker_->Stats().settled_moved, 1u);

    auto fresh = Acquire("sigma");
    EXPECT_TRUE(fresh.has_value())
        << "the new incarnation was rejected as " << ToString(fresh.error())
        << " for a move of the old block";
}

// The cap may never drop a record with a lease out, because that lease is
// about to record a cooldown into it. A stale settle that decremented the
// wrong record's inflight count would quietly unpin it and hand it to the
// trim -- and the successful move would then leave no cooldown at all.
TEST_F(MovementTrackerTest, StaleSettleCannotUnpinALiveRecord) {
    MovementTrackerConfig config;
    // One shard so the per-shard share is exactly max_tracked_keys.
    config.shard_count = 1;
    config.max_tracked_keys = 2;
    config.cooldown = kCooldown;
    config.minimum_residency = kResidency;
    tracker_ = MakeTracker(config);

    const MovementDedupKey old_block = DedupFor("tau", kFastTiler, kSlowTiler);
    const MovementDedupKey new_block =
        RecreatedDedupFor("tau", kFastTiler, kSlowTiler);

    auto stale =
        tracker_->TryAcquire("tau", old_block, MovementDirection::kOffload);
    ASSERT_TRUE(stale.has_value());
    tracker_->OnDeleted("tau");

    // The name comes back and a genuinely live movement starts on the new
    // block.
    auto live =
        tracker_->TryAcquire("tau", new_block, MovementDirection::kOffload);
    ASSERT_TRUE(live.has_value());

    // The stale settle lands while the live lease is out.
    stale->Settle(/*moved=*/false);

    // Cap pressure. The live record must not be a candidate.
    for (int i = 0; i < 8; ++i) {
        tracker_->OnCommitted("filler-" + std::to_string(i), kFastTiler);
        clock_->Advance(1ms);
    }
    EXPECT_EQ(tracker_->Stats().tracked_keys, 2u);

    live->Settle(/*moved=*/true);
    auto blocked =
        tracker_->TryAcquire("tau", new_block, MovementDirection::kOffload);
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), MovementRejection::kCooldown);
}

// The dedup key names the source tier twice, and a caller that fills the
// block id but leaves the redundant copy at its default must not thereby turn
// the residency gate off: an all-zero tier matches no commit record, and "no
// record" is deliberately read as "residency satisfied".
TEST_F(MovementTrackerTest, ResidencyGateSurvivesAnOmittedSourceTiler) {
    tracker_->OnCommitted("upsilon", kFastTiler);

    MovementDedupKey dedup;
    dedup.registration_id = RegistrationId{0, 11};
    dedup.source_block_id = BlockId{kFastTiler, 11, 1};
    dedup.destination_tiler = kSlowTiler;
    ASSERT_TRUE(IsZeroUUID(dedup.source_tiler));

    auto blocked =
        tracker_->TryAcquire("upsilon", dedup, MovementDirection::kOffload);
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), MovementRejection::kMinimumResidency);
    EXPECT_EQ(tracker_->Stats().rejected_residency, 1u);

    clock_->Advance(kResidency);
    EXPECT_TRUE(
        tracker_->TryAcquire("upsilon", dedup, MovementDirection::kOffload)
            .has_value());
}

// With no source tier anywhere in the key there is nothing to ask the
// residency question about. That is a wiring bug, and it is reported as one
// instead of being answered "go ahead".
TEST_F(MovementTrackerTest, DedupKeyWithNoSourceTierIsRejected) {
    MovementDedupKey dedup;
    dedup.registration_id = RegistrationId{0, 12};
    dedup.source_block_id = BlockId{UUID{0, 0}, 12, 1};
    dedup.destination_tiler = kSlowTiler;

    auto rejected =
        tracker_->TryAcquire("phi", dedup, MovementDirection::kOffload);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), MovementRejection::kInvalid);

    auto stats = tracker_->Stats();
    EXPECT_EQ(stats.rejected_invalid, 1u);
    // Nothing is remembered about a key the tracker refused to answer for.
    EXPECT_EQ(stats.acquired, 0u);
    EXPECT_EQ(stats.inflight, 0u);
    EXPECT_EQ(stats.tracked_keys, 0u);
}

// Stop() has to mean it: an acquire that read the flag a moment before the
// shutdown must not mint a lease afterwards, or the owner's drain step counts
// down to zero while a new movement is starting behind it.
TEST_F(MovementTrackerTest, StopDuringAnAcquireIsSeenBeforeTheLeaseIsMinted) {
    auto clock = std::make_shared<StopOnReadClock>();
    MovementTrackerConfig config;
    config.shard_count = 4;
    config.cooldown = kCooldown;
    config.minimum_residency = kResidency;
    MovementTracker tracker(config, clock);
    clock->Attach(&tracker);

    auto rejected =
        tracker.TryAcquire("chi", DedupFor("chi", kFastTiler, kSlowTiler),
                           MovementDirection::kOffload);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), MovementRejection::kStopped);
    EXPECT_EQ(tracker.Stats().inflight, 0u);
    EXPECT_EQ(tracker.Stats().acquired, 0u);
}

}  // namespace mooncake::v2
