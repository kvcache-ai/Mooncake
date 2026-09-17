#include "dynamic_replication_lease_table.h"

#include <chrono>
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

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
    table.Put(proposal, MakeLease(proposal, "k1", 0));
    EXPECT_FALSE(table.Empty());

    auto found = table.Find(proposal);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->key, "k1");

    // An unknown proposal finds nothing, and the by-key view agrees.
    EXPECT_FALSE(table.Find(UUID{9, 9}).has_value());
    EXPECT_TRUE(table.HasLeaseForObjectForTest("k1"));
    EXPECT_FALSE(table.HasLeaseForObjectForTest("k2"));

    EXPECT_TRUE(table.Remove(proposal));
    EXPECT_FALSE(table.Remove(proposal));
    EXPECT_FALSE(table.Find(proposal).has_value());
    EXPECT_TRUE(table.Empty());
}

TEST(DynamicReplicationLeaseTableTest, ReplaceMovesTheProposalToItsNewKey) {
    DynamicReplicationLeaseTable table;
    const UUID proposal{1, 2};
    table.Put(proposal, MakeLease(proposal, "k1", 0));
    table.Put(proposal, MakeLease(proposal, "k2", 0));

    EXPECT_FALSE(table.HasLeaseForObjectForTest("k1"));
    EXPECT_TRUE(table.HasLeaseForObjectForTest("k2"));

    // Retracting the old key must not touch the lease it no longer holds.
    table.EraseForObject("k1");
    EXPECT_TRUE(table.Find(proposal).has_value());
}

TEST(DynamicReplicationLeaseTableTest,
     ErasingForAnObjectDropsItsProposalsOnly) {
    DynamicReplicationLeaseTable table;
    const UUID first{1, 2};
    const UUID second{3, 4};
    const UUID other_key{5, 6};
    table.Put(first, MakeLease(first, "k1", 0));
    table.Put(second, MakeLease(second, "k1", 0));  // several may be in flight
    table.Put(other_key, MakeLease(other_key, "k2", 0));

    table.EraseForObject("k1");

    EXPECT_FALSE(table.Find(first).has_value());
    EXPECT_FALSE(table.Find(second).has_value());
    EXPECT_TRUE(table.Find(other_key).has_value());

    table.EraseForObject("k1");  // nothing left to retract
    EXPECT_TRUE(table.Find(other_key).has_value());
}

TEST(DynamicReplicationLeaseTableTest, ErasingExpiredKeepsTheLiveOnes) {
    DynamicReplicationLeaseTable table;
    const auto now = std::chrono::system_clock::now();
    const UUID expired{1, 2};
    const UUID live{3, 4};
    table.Put(expired, MakeLease(expired, "k1", EpochMillis(now) - 1));
    table.Put(live, MakeLease(live, "k2", EpochMillis(now) + 60'000));

    table.EraseExpired(now);

    EXPECT_FALSE(table.Find(expired).has_value());
    EXPECT_TRUE(table.Find(live).has_value());
    EXPECT_FALSE(table.HasLeaseForObjectForTest("k1"));
}

TEST(DynamicReplicationLeaseTableTest, SweepKeepsALeaseExtendedPastTheOldOne) {
    DynamicReplicationLeaseTable table;
    const auto now = std::chrono::system_clock::now();
    const UUID proposal{1, 2};
    table.Put(proposal, MakeLease(proposal, "k1", EpochMillis(now) + 1'000));
    const int64_t extended = EpochMillis(now) + 60'000;
    table.Put(proposal, MakeLease(proposal, "k1", extended));

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
    table.Put(proposal, MakeLease(proposal, "k1", EpochMillis(now) + 1'000));
    ASSERT_TRUE(table.Remove(proposal));

    // The deadline is still queued even though the lease is gone.
    table.EraseExpired(now + std::chrono::seconds(10));
    EXPECT_TRUE(table.Empty());
}

}  // namespace
}  // namespace mooncake
