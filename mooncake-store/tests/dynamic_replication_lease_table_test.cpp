#include "dynamic_replication_lease_table.h"

#include <chrono>
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

// Distinct proposal ids, and an explicit absolute deadline.
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

    // An in-flight proposal keeps the table non-empty, which is why the tenant
    // aggregate has to consult it alongside the object route.
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

TEST(DynamicReplicationLeaseTableTest, ErasingForAnObjectLeavesOtherKeysAlone) {
    DynamicReplicationLeaseTable table;
    const UUID first{1, 2};
    const UUID second{3, 4};
    table.Put(first, MakeLease(first, "k1", 0));
    table.Put(second, MakeLease(second, "k2", 0));

    table.EraseForObject("k1");

    EXPECT_FALSE(table.Find(first).has_value());
    EXPECT_TRUE(table.Find(second).has_value());
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
}

}  // namespace
}  // namespace mooncake
