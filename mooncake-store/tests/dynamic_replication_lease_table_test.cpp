#include "dynamic_replication_lease_table.h"

#include <chrono>
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

// The publication generation the proposals in these tests belong to. Most cases
// are about the table itself, so one non-zero value is enough; the one about
// replacement uses a second.
constexpr uint64_t kGeneration = 7;

// A lease for `proposal_id` with the deadline given outright, so a suite
// decides when it expires.
ReplicaActionLease MakeLease(const UUID& proposal_id, const std::string& key,
                             int64_t expire_at_ms_epoch) {
    ReplicaActionLease lease;
    lease.proposal_id = proposal_id;
    lease.lease_id = proposal_id;
    lease.key = key;
    lease.expire_at_ms_epoch = expire_at_ms_epoch;
    return lease;
}

int64_t EpochMillis(std::chrono::system_clock::time_point tp) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               tp.time_since_epoch())
        .count();
}

TEST(DynamicReplicationLeaseTableTest, IsKeyedByProposalId) {
    DynamicReplicationLeaseTable table;
    EXPECT_TRUE(table.Empty());

    // An in-flight proposal keeps the table non-empty until it is removed or
    // expires.
    const UUID proposal{1, 2};
    table.Put(proposal, MakeLease(proposal, "k1", 0), kGeneration);
    EXPECT_FALSE(table.Empty());

    auto found = table.Find(proposal);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->key, "k1");

    // An unknown proposal finds nothing.
    EXPECT_FALSE(table.Find(UUID{9, 9}).has_value());

    EXPECT_TRUE(table.Remove(proposal));
    EXPECT_FALSE(table.Remove(proposal));
    EXPECT_FALSE(table.Find(proposal).has_value());
    EXPECT_TRUE(table.Empty());
}

TEST(DynamicReplicationLeaseTableTest, ReplaceMovesTheProposalToItsNewKey) {
    DynamicReplicationLeaseTable table;
    const UUID proposal{1, 2};
    table.Put(proposal, MakeLease(proposal, "k1", 0), kGeneration);
    table.Put(proposal, MakeLease(proposal, "k2", 0), kGeneration);

    // Retracting the old key must not touch a lease that no longer sits under
    // it, and the key the lease moved to is the one that retracts it.
    table.EraseForObject("k1", kGeneration);
    EXPECT_TRUE(table.Find(proposal).has_value());

    table.EraseForObject("k2", kGeneration);
    EXPECT_FALSE(table.Find(proposal).has_value());
}

TEST(DynamicReplicationLeaseTableTest,
     ErasingForAnObjectDropsItsProposalsOnly) {
    DynamicReplicationLeaseTable table;
    const UUID first{1, 2};
    const UUID second{3, 4};
    const UUID other_key{5, 6};
    table.Put(first, MakeLease(first, "k1", 0), kGeneration);
    // Several proposals may be in flight for one key.
    table.Put(second, MakeLease(second, "k1", 0), kGeneration);
    table.Put(other_key, MakeLease(other_key, "k2", 0), kGeneration);

    table.EraseForObject("k1", kGeneration);

    EXPECT_FALSE(table.Find(first).has_value());
    EXPECT_FALSE(table.Find(second).has_value());
    EXPECT_TRUE(table.Find(other_key).has_value());

    // Nothing left to retract.
    table.EraseForObject("k1", kGeneration);
    EXPECT_TRUE(table.Find(other_key).has_value());
}

TEST(DynamicReplicationLeaseTableTest,
     ErasingForAnObjectSparesANewerGeneration) {
    DynamicReplicationLeaseTable table;
    constexpr uint64_t kNewer = kGeneration + 1;
    const UUID older{1, 2};
    const UUID newer{3, 4};
    table.Put(older, MakeLease(older, "k1", 0), kGeneration);
    table.Put(newer, MakeLease(newer, "k1", 0), kNewer);

    // A teardown of the older object retracts that object's proposal and leaves
    // the one a newer object registered: the client is still acting on it.
    table.EraseForObject("k1", kGeneration);
    EXPECT_FALSE(table.Find(older).has_value());
    EXPECT_TRUE(table.Find(newer).has_value());

    // A generation that registered nothing leaves the table alone, and 0 is
    // rejected outright.
    table.EraseForObject("k1", kNewer + 1);
    table.EraseForObject("k1", 0);
    EXPECT_TRUE(table.Find(newer).has_value());

    table.EraseForObject("k1", kNewer);
    EXPECT_FALSE(table.Find(newer).has_value());
    EXPECT_TRUE(table.Empty());
}

TEST(DynamicReplicationLeaseTableTest, ErasingExpiredKeepsTheLiveOnes) {
    DynamicReplicationLeaseTable table;
    const auto now = std::chrono::system_clock::now();
    const UUID expired{1, 2};
    const UUID live{3, 4};
    // One key on purpose: the sweep has to drop the expired proposal without
    // taking the live one, or its key, out with it.
    table.Put(expired, MakeLease(expired, "k1", EpochMillis(now) - 1),
              kGeneration);
    table.Put(live, MakeLease(live, "k1", EpochMillis(now) + 60'000),
              kGeneration);

    table.EraseExpired(now);

    EXPECT_FALSE(table.Find(expired).has_value());
    EXPECT_TRUE(table.Find(live).has_value());

    // The key still retracts what is left under it.
    table.EraseForObject("k1", kGeneration);
    EXPECT_FALSE(table.Find(live).has_value());
}

TEST(DynamicReplicationLeaseTableTest, SweepKeepsALeaseExtendedPastTheOldOne) {
    DynamicReplicationLeaseTable table;
    const auto now = std::chrono::system_clock::now();
    const UUID proposal{1, 2};
    table.Put(proposal, MakeLease(proposal, "k1", EpochMillis(now) + 1'000),
              kGeneration);
    const int64_t extended = EpochMillis(now) + 60'000;
    table.Put(proposal, MakeLease(proposal, "k1", extended), kGeneration);

    // The first deadline is still queued; it must not take the live lease out.
    table.EraseExpired(now + std::chrono::seconds(10));
    auto found = table.Find(proposal);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->expire_at_ms_epoch, extended);

    table.EraseExpired(now + std::chrono::minutes(2));
    EXPECT_FALSE(table.Find(proposal).has_value());
    EXPECT_TRUE(table.Empty());
}

TEST(DynamicReplicationLeaseTableTest, SweepToleratesARemovedLeasesDeadline) {
    DynamicReplicationLeaseTable table;
    const auto now = std::chrono::system_clock::now();
    const UUID proposal{1, 2};
    table.Put(proposal, MakeLease(proposal, "k1", EpochMillis(now) + 1'000),
              kGeneration);
    ASSERT_TRUE(table.Remove(proposal));

    // The deadline is still queued even though the lease is gone.
    table.EraseExpired(now + std::chrono::seconds(10));
    EXPECT_TRUE(table.Empty());
}

}  // namespace
}  // namespace mooncake
