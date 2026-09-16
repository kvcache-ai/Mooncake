#include "object_index.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// --- Object route ---

TEST(ObjectIndexTest, InsertPublishesTheEntryUnderItsKey) {
    ObjectIndex store;
    EXPECT_EQ(store.ObjectCount(), 0u);

    auto e1 = test::MakeObjectEntry("k1");
    EXPECT_TRUE(store.Insert(e1));
    EXPECT_TRUE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 1u);

    auto entry = store.Get("k1");
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.get(), e1.get());  // the route hands back the same entry
    EXPECT_EQ(entry->key(), "k1");
    EXPECT_EQ(store.Get("missing"), nullptr);
}

TEST(ObjectIndexTest, EraseIfHonoursEntryIdentity) {
    ObjectIndex store;
    auto e1 = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(e1));

    // The route points at e1, so a stale pointer may not erase it.
    auto replacement = test::MakeObjectEntry("k1");
    EXPECT_FALSE(store.EraseIf("k1", replacement.get()));
    EXPECT_TRUE(store.Contains("k1"));

    EXPECT_TRUE(store.EraseIf("k1", e1.get()));
    EXPECT_FALSE(store.EraseIf("k1", e1.get()));  // slot already gone
    EXPECT_FALSE(store.Contains("k1"));
    EXPECT_EQ(store.ObjectCount(), 0u);
}

TEST(ObjectIndexTest, InsertAssignsGenerationsAndIsCurrentTracksReplacement) {
    ObjectIndex store;
    auto e1 = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(e1));
    EXPECT_GT(e1->generation(), 0u);  // publication assigns a generation
    EXPECT_TRUE(store.IsCurrent("k1", e1.get()));

    EXPECT_TRUE(store.EraseIf("k1", e1.get()));
    EXPECT_FALSE(store.IsCurrent("k1", e1.get()));

    // A replacement of the same key gets a fresh, higher generation; the
    // stale instance is never current again.
    auto e2 = test::MakeObjectEntry("k1");
    EXPECT_EQ(e2->generation(), 0u);  // unpublished
    ASSERT_TRUE(store.Insert(e2));
    EXPECT_GT(e2->generation(), e1->generation());
    EXPECT_TRUE(store.IsCurrent("k1", e2.get()));
    EXPECT_FALSE(store.IsCurrent("k1", e1.get()));
}

TEST(ObjectIndexTest, DuplicateInsertIsRejected) {
    ObjectIndex store;
    auto winner = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(winner));

    // The loser must neither clobber the routed entry nor look published: it
    // never reaches the route, so it keeps generation 0.
    auto loser = test::MakeObjectEntry("k1");
    EXPECT_FALSE(store.Insert(loser));
    EXPECT_EQ(loser->generation(), 0u);
    EXPECT_EQ(store.ObjectCount(), 1u);
    EXPECT_EQ(store.Get("k1").get(), winner.get());
}

TEST(ObjectIndexTest, SnapshotObjectsEnumeratesEveryEntry) {
    ObjectIndex store;
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k1")));
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k2", "g1")));
    ASSERT_TRUE(store.Insert(test::MakeObjectEntry("k3", "g1")));

    std::vector<std::string> keys;
    for (const auto& entry : store.SnapshotObjects()) {
        keys.push_back(entry->key());
    }
    // Enumeration order is the map's, so compare as a set.
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys, (std::vector<std::string>{"k1", "k2", "k3"}));
}

// --- Dynamic-replication lease table ---

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

TEST(ObjectIndexTest, LeaseTableIsKeyedByProposalId) {
    ObjectIndex store;
    const UUID proposal{1, 2};
    store.PutDynamicReplicationLease(proposal, MakeLease(proposal, "k1", 0));

    auto found = store.FindDynamicReplicationLease(proposal);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->key, "k1");

    // An unknown proposal finds nothing, and the by-key view agrees.
    EXPECT_FALSE(store.FindDynamicReplicationLease(UUID{9, 9}).has_value());
    EXPECT_TRUE(store.HasDynamicReplicationLeaseForKeyForTest("k1"));
    EXPECT_FALSE(store.HasDynamicReplicationLeaseForKeyForTest("k2"));

    EXPECT_TRUE(store.RemoveDynamicReplicationLease(proposal));
    EXPECT_FALSE(store.RemoveDynamicReplicationLease(proposal));
    EXPECT_FALSE(store.FindDynamicReplicationLease(proposal).has_value());
}

TEST(ObjectIndexTest, ErasingLeasesForAnObjectLeavesOtherKeysAlone) {
    ObjectIndex store;
    const UUID first{1, 2};
    const UUID second{3, 4};
    store.PutDynamicReplicationLease(first, MakeLease(first, "k1", 0));
    store.PutDynamicReplicationLease(second, MakeLease(second, "k2", 0));

    store.EraseDynamicReplicationLeasesForObject("k1");

    EXPECT_FALSE(store.FindDynamicReplicationLease(first).has_value());
    EXPECT_TRUE(store.FindDynamicReplicationLease(second).has_value());
}

TEST(ObjectIndexTest, ErasingExpiredLeasesKeepsTheLiveOnes) {
    ObjectIndex store;
    const auto now = std::chrono::system_clock::now();
    const UUID expired{1, 2};
    const UUID live{3, 4};
    store.PutDynamicReplicationLease(
        expired, MakeLease(expired, "k1", EpochMillis(now) - 1));
    store.PutDynamicReplicationLease(
        live, MakeLease(live, "k2", EpochMillis(now) + 60'000));

    store.EraseExpiredDynamicReplicationLeases(now);

    EXPECT_FALSE(store.FindDynamicReplicationLease(expired).has_value());
    EXPECT_TRUE(store.FindDynamicReplicationLease(live).has_value());
}

TEST(ObjectIndexTest, EmptyCoversTheRouteAndTheLeaseTable) {
    ObjectIndex store;
    EXPECT_TRUE(store.Empty());

    auto entry = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(entry));
    EXPECT_FALSE(store.Empty());
    ASSERT_TRUE(store.EraseIf("k1", entry.get()));
    EXPECT_TRUE(store.Empty());

    // A proposal that can still land keeps the index non-empty even with no
    // routed object, so a reclamation check must consult this table too.
    const UUID proposal{1, 2};
    store.PutDynamicReplicationLease(
        proposal,
        MakeLease(proposal, "k1",
                  EpochMillis(std::chrono::system_clock::now()) + 60'000));
    EXPECT_FALSE(store.Empty());
}

// --- Accessors ---

TEST(ObjectIndexTest, WithObjectVisitsThePresentKeyOnly) {
    ObjectIndex store;
    auto singleton = test::MakeObjectEntry("k1");
    ASSERT_TRUE(store.Insert(singleton));
    auto& raw = singleton->metadata();

    // Absent key -> the callback must not run at all.
    store.WithObject("missing", [](ObjectMetadata&) { FAIL(); });

    // Present key -> it runs against the routed entry's own envelope.
    bool called = false;
    store.WithObject("k1", [&](ObjectMetadata& m) {
        called = true;
        EXPECT_EQ(&m, &raw);
    });
    EXPECT_TRUE(called);
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
