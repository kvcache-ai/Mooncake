// Component tests for FrequencyTracker.
//
// Everything here runs on a manual clock. Heat is a decaying quantity, so a
// test that cannot move time can only ever assert that counting up works --
// which is precisely the property the old implementation had and the reason it
// was wrong: a raw total made a key that was hot an hour ago permanently
// outrank one that is hot now.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "p2p/client/v2/frequency_tracker.h"

namespace mooncake::v2 {
namespace {

using namespace std::chrono_literals;

/**
 * @class ManualClock
 * @brief Time only moves when a test says so.
 */
class ManualClock final : public Clock {
   public:
    time_point Now() const override {
        std::lock_guard<std::mutex> lock(mu_);
        return now_;
    }
    void Advance(std::chrono::milliseconds delta) {
        std::lock_guard<std::mutex> lock(mu_);
        now_ += delta;
    }

   private:
    mutable std::mutex mu_;
    time_point now_{std::chrono::steady_clock::time_point{} +
                    std::chrono::hours(1)};
};

RegistrationId MakeId(uint32_t shard, uint64_t sequence) {
    RegistrationId id;
    id.registry_shard = shard;
    id.shard_sequence = sequence;
    return id;
}

}  // namespace

class FrequencyTrackerTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("FrequencyTrackerTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override { clock_ = std::make_shared<ManualClock>(); }

    std::unique_ptr<FrequencyTracker> Make(FrequencyTrackerConfig config = {}) {
        return std::make_unique<FrequencyTracker>(config, clock_);
    }

    std::shared_ptr<ManualClock> clock_;
};

// ---------------------------------------------------------------------------
// Decay
// ---------------------------------------------------------------------------

// Without this, a threshold once crossed is crossed forever: a key read a
// thousand times yesterday outranks one being read now, and onboarding fires
// on demand that no longer exists.
TEST_F(FrequencyTrackerTest, HeatHalvesOverOneHalfLife) {
    FrequencyTrackerConfig config;
    config.half_life = 1000ms;
    auto tracker = Make(config);
    const RegistrationId id = MakeId(1, 1);

    for (int i = 0; i < 8; ++i) tracker->RecordAccess(id, "decay");
    const double hot = tracker->Get(id, "decay").heat;
    EXPECT_DOUBLE_EQ(hot, 8.0);

    clock_->Advance(1000ms);
    EXPECT_NEAR(tracker->Get(id, "decay").heat, 4.0, 1e-6);
    clock_->Advance(1000ms);
    EXPECT_NEAR(tracker->Get(id, "decay").heat, 2.0, 1e-6);
    clock_->Advance(10s);
    EXPECT_LT(tracker->Get(id, "decay").heat, 0.01);
}

// A steady reader must not lose ground to a burst that has since gone quiet.
TEST_F(FrequencyTrackerTest, ASustainedReaderOutranksAnOldBurst) {
    FrequencyTrackerConfig config;
    config.half_life = 1000ms;
    auto tracker = Make(config);
    const RegistrationId burst_id = MakeId(1, 1);
    const RegistrationId steady_id = MakeId(1, 2);

    for (int i = 0; i < 20; ++i) tracker->RecordAccess(burst_id, "burst");
    for (int second = 0; second < 10; ++second) {
        clock_->Advance(1000ms);
        tracker->RecordAccess(steady_id, "steady");
    }

    const AccessStats stats = tracker->Snapshot(std::nullopt);
    ASSERT_GE(stats.hot_keys.size(), 2U);
    EXPECT_EQ(stats.hot_keys[0].key, "steady");
    EXPECT_EQ(stats.hot_keys[0].recency_rank, 0U);
}

// Get answers the decayed value but must not itself be a touch, or a policy
// that polls the tracker would keep the key it is deciding about warm.
TEST_F(FrequencyTrackerTest, GetDoesNotCountAsATouch) {
    auto tracker = Make();
    const RegistrationId id = MakeId(1, 1);
    tracker->RecordAccess(id, "peek");
    const uint64_t touches = tracker->Get(id, "peek").raw_touches;
    tracker->Get(id, "peek");
    tracker->Get(id, "peek");
    EXPECT_EQ(tracker->Get(id, "peek").raw_touches, touches);
}

// ---------------------------------------------------------------------------
// Reads versus writes
// ---------------------------------------------------------------------------

// The defect this separation exists to fix: the onboard decision used to read
// a counter that commits also bumped, so writing a key made it look like
// demand for that key on a slow tier.
TEST_F(FrequencyTrackerTest, ACommitCountsTowardsHeatButNotTowardsReadHeat) {
    auto tracker = Make();
    const RegistrationId id = MakeId(1, 1);

    tracker->OnCommit(id, "written");
    FrequencySnapshot after_commit = tracker->Get(id, "written");
    EXPECT_DOUBLE_EQ(after_commit.heat, 1.0);
    EXPECT_DOUBLE_EQ(after_commit.read_heat, 0.0);

    tracker->RecordAccess(id, "written");
    FrequencySnapshot after_read = tracker->Get(id, "written");
    EXPECT_DOUBLE_EQ(after_read.heat, 2.0);
    EXPECT_DOUBLE_EQ(after_read.read_heat, 1.0);
}

// A freshly written key must still be visible to hot-key recovery, which is
// why a commit counts towards heat at all.
TEST_F(FrequencyTrackerTest, AFreshlyCommittedKeyAppearsInTheSnapshot) {
    auto tracker = Make();
    tracker->OnCommit(MakeId(1, 1), "fresh");
    const AccessStats stats = tracker->Snapshot(std::nullopt);
    ASSERT_EQ(stats.hot_keys.size(), 1U);
    EXPECT_EQ(stats.hot_keys[0].key, "fresh");
    EXPECT_GT(stats.hot_keys[0].recent_heat_score, 0.0);
}

// RecordAccess returns the state including this access, which is what lets the
// access path record before publishing and still guarantee a consumer sees the
// access it is reacting to.
TEST_F(FrequencyTrackerTest, RecordAccessReturnsTheStateIncludingItself) {
    auto tracker = Make();
    const RegistrationId id = MakeId(1, 1);
    FrequencySnapshot first = tracker->RecordAccess(id, "self");
    EXPECT_FALSE(first.missing);
    EXPECT_DOUBLE_EQ(first.read_heat, 1.0);
    FrequencySnapshot second = tracker->RecordAccess(id, "self");
    EXPECT_DOUBLE_EQ(second.read_heat, 2.0);
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

// Delete-then-recreate of the same name is a different object. Inheriting the
// dead key's heat would pull a brand new, unread object up a tier.
TEST_F(FrequencyTrackerTest, ANewRegistrationForTheSameNameStartsCold) {
    auto tracker = Make();
    const RegistrationId first = MakeId(1, 1);
    const RegistrationId second = MakeId(1, 2);

    for (int i = 0; i < 10; ++i) tracker->RecordAccess(first, "recycled");
    EXPECT_DOUBLE_EQ(tracker->Get(first, "recycled").heat, 10.0);

    tracker->RecordAccess(second, "recycled");
    EXPECT_DOUBLE_EQ(tracker->Get(second, "recycled").heat, 1.0);
    // The old identity no longer has a record at all.
    EXPECT_TRUE(tracker->Get(first, "recycled").missing);
}

// A delete for the previous incarnation must not erase the live one's heat.
TEST_F(FrequencyTrackerTest, OnDeleteIsScopedToItsRegistration) {
    auto tracker = Make();
    const RegistrationId first = MakeId(1, 1);
    const RegistrationId second = MakeId(1, 2);

    tracker->RecordAccess(first, "raced");
    tracker->RecordAccess(second, "raced");  // recreated
    tracker->OnDelete(first, "raced");       // late delete of the old one

    EXPECT_FALSE(tracker->Get(second, "raced").missing);
    EXPECT_EQ(tracker->TrackedKeyCount(), 1U);
}

TEST_F(FrequencyTrackerTest, OnDeleteRemovesItsOwnRecord) {
    auto tracker = Make();
    const RegistrationId id = MakeId(1, 1);
    tracker->RecordAccess(id, "gone");
    tracker->OnDelete(id, "gone");
    EXPECT_TRUE(tracker->Get(id, "gone").missing);
    EXPECT_EQ(tracker->TrackedKeyCount(), 0U);
}

// The eviction path learns a key's last replica is gone without holding its
// identity, so the name-only form has to work.
TEST_F(FrequencyTrackerTest, RemoveByNameForgetsWhateverRegistrationIsThere) {
    auto tracker = Make();
    tracker->RecordAccess(MakeId(1, 7), "anon");
    tracker->Remove("anon");
    EXPECT_EQ(tracker->TrackedKeyCount(), 0U);
}

TEST_F(FrequencyTrackerTest, AnUnknownKeyReportsMissing) {
    auto tracker = Make();
    FrequencySnapshot snapshot = tracker->Get(MakeId(1, 1), "never-seen");
    EXPECT_TRUE(snapshot.missing);
    EXPECT_DOUBLE_EQ(snapshot.heat, 0.0);
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

TEST_F(FrequencyTrackerTest, SnapshotIsHottestFirstWithARankAndATieBreak) {
    auto tracker = Make();
    tracker->RecordAccess(MakeId(1, 1), "cold");
    for (int i = 0; i < 5; ++i) tracker->RecordAccess(MakeId(1, 2), "hot");
    // Equal scores must still come out in a stable order, or a differential
    // comparison between two implementations is a coin toss.
    tracker->RecordAccess(MakeId(1, 3), "also-cold");

    const AccessStats stats = tracker->Snapshot(std::nullopt);
    ASSERT_EQ(stats.hot_keys.size(), 3U);
    EXPECT_EQ(stats.hot_keys[0].key, "hot");
    EXPECT_EQ(stats.hot_keys[1].key, "also-cold");
    EXPECT_EQ(stats.hot_keys[2].key, "cold");
    EXPECT_EQ(stats.hot_keys[0].recency_rank, 0U);
    EXPECT_EQ(stats.hot_keys[2].recency_rank, 2U);
    EXPECT_EQ(stats.metric, AccessStatMetric::kFrequency);
}

// hot_key_num == 0 is HARecoveryManager's "everything you track", and it still
// has to be bounded or recovery would build an unbounded vector.
TEST_F(FrequencyTrackerTest, ZeroMeansEverythingButStillRespectsTheCap) {
    FrequencyTrackerConfig config;
    config.max_snapshot_keys = 3;
    auto tracker = Make(config);
    for (int i = 0; i < 10; ++i) {
        tracker->RecordAccess(MakeId(1, static_cast<uint64_t>(i)),
                              "key" + std::to_string(i));
    }
    const AccessStats stats = tracker->Snapshot(0);
    EXPECT_EQ(stats.hot_keys.size(), 3U);
    EXPECT_EQ(tracker->TruncatedSnapshotCount(), 1U);
}

TEST_F(FrequencyTrackerTest, AnUntruncatedSnapshotIsNotCounted) {
    auto tracker = Make();
    tracker->RecordAccess(MakeId(1, 1), "only");
    tracker->Snapshot(std::nullopt);
    EXPECT_EQ(tracker->TruncatedSnapshotCount(), 0U);
}

// A key whose heat has decayed to nothing is not a hot key, and reporting it
// would let recovery prioritise objects nobody has touched in an hour.
TEST_F(FrequencyTrackerTest, FullyDecayedKeysDropOutOfTheSnapshot) {
    FrequencyTrackerConfig config;
    config.half_life = 100ms;
    auto tracker = Make(config);
    tracker->RecordAccess(MakeId(1, 1), "faded");
    clock_->Advance(10s);
    EXPECT_TRUE(tracker->Snapshot(std::nullopt).hot_keys.empty());
}

TEST_F(FrequencyTrackerTest, ClearForgetsEverything) {
    auto tracker = Make();
    tracker->RecordAccess(MakeId(1, 1), "a");
    tracker->RecordAccess(MakeId(1, 2), "b");
    tracker->Clear();
    EXPECT_EQ(tracker->TrackedKeyCount(), 0U);
    EXPECT_TRUE(tracker->Snapshot(std::nullopt).hot_keys.empty());
}

// ---------------------------------------------------------------------------
// Bounded memory
// ---------------------------------------------------------------------------

// Without a cap the map holds one entry per key ever seen, for the process
// lifetime -- on a store whose whole point is churn.
TEST_F(FrequencyTrackerTest, TheTrackedSetIsBounded) {
    FrequencyTrackerConfig config;
    config.shard_count = 1;
    config.max_tracked_keys = 16;
    config.half_life = 100ms;
    config.entry_ttl = 1s;
    auto tracker = Make(config);

    for (int i = 0; i < 200; ++i) {
        tracker->RecordAccess(MakeId(1, static_cast<uint64_t>(i)),
                              "churn" + std::to_string(i));
        clock_->Advance(50ms);
    }
    EXPECT_LE(tracker->TrackedKeyCount(), 32U);
    EXPECT_GT(tracker->EvictedRecordCount(), 0U);
}

// The cap must not throw away what is actually hot.
TEST_F(FrequencyTrackerTest, TheHotKeySurvivesTheCap) {
    FrequencyTrackerConfig config;
    config.shard_count = 1;
    config.max_tracked_keys = 16;
    config.half_life = 10s;
    config.entry_ttl = 1s;
    auto tracker = Make(config);
    const RegistrationId hot = MakeId(9, 9);

    for (int i = 0; i < 100; ++i) {
        for (int touch = 0; touch < 4; ++touch) {
            tracker->RecordAccess(hot, "sticky");
        }
        tracker->RecordAccess(MakeId(1, static_cast<uint64_t>(i)),
                              "churn" + std::to_string(i));
        clock_->Advance(20ms);
    }
    EXPECT_FALSE(tracker->Get(hot, "sticky").missing);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

TEST_F(FrequencyTrackerTest, ConfigValidationRejectsUnusableValues) {
    EXPECT_TRUE(ValidateFrequencyTrackerConfig({}).has_value());

    FrequencyTrackerConfig no_shards;
    no_shards.shard_count = 0;
    EXPECT_FALSE(ValidateFrequencyTrackerConfig(no_shards).has_value());

    FrequencyTrackerConfig no_snapshot;
    no_snapshot.max_snapshot_keys = 0;
    EXPECT_FALSE(ValidateFrequencyTrackerConfig(no_snapshot).has_value());

    FrequencyTrackerConfig zero_half_life;
    zero_half_life.half_life = 0ms;
    EXPECT_FALSE(ValidateFrequencyTrackerConfig(zero_half_life).has_value());

    FrequencyTrackerConfig zero_ttl;
    zero_ttl.entry_ttl = 0ms;
    EXPECT_FALSE(ValidateFrequencyTrackerConfig(zero_ttl).has_value());

    FrequencyTrackerConfig zero_threshold;
    zero_threshold.expiry_threshold = 0.0;
    EXPECT_FALSE(ValidateFrequencyTrackerConfig(zero_threshold).has_value());

    FrequencyTrackerConfig zero_cap;
    zero_cap.max_tracked_keys = 0;
    EXPECT_FALSE(ValidateFrequencyTrackerConfig(zero_cap).has_value());
}

}  // namespace mooncake::v2
