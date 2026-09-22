#include "metadata/tenant.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace metadata {
namespace {

// The lease the entry currently holds, read through the entry's own lock.
std::shared_ptr<Lease> LeaseOf(const std::shared_ptr<ObjectEntry>& entry) {
    return entry->WithSharedAccess(
        [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
            return metadata.lease_;
        });
}

bool IsProcessing(const std::shared_ptr<ObjectEntry>& entry) {
    return entry->WithSharedAccess(
        [](const ObjectMetadata&, const ObjectEntry::State& state) {
            return state.is_processing;
        });
}

// A replica-action lease for the entry's key and the given proposal, so it can
// be registered against the entry without tripping either check.
ReplicaActionLease LeaseFor(const std::shared_ptr<ObjectEntry>& entry,
                            const UUID& proposal_id) {
    ReplicaActionLease lease;
    lease.proposal_id = proposal_id;
    lease.lease_id = proposal_id;
    lease.key = entry->key();
    return lease;
}

TEST(TenantTest, InsertObjectWiresTheGroupLeaseAndJoinsTheGroup) {
    Tenant tenant;

    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    auto second = test::MakeObjectEntry("k2", "g1");
    ASSERT_TRUE(tenant.InsertObject(second));

    EXPECT_EQ(tenant.ObjectCount(), 2u);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 2u);

    // Both members of the group point at the one shared lease, so the group has
    // a single deadline.
    auto first_lease = LeaseOf(first);
    auto second_lease = LeaseOf(second);
    ASSERT_NE(first_lease, nullptr);
    EXPECT_EQ(first_lease.get(), second_lease.get());
}

TEST(TenantTest, InsertObjectLeavesAnUngroupedEntryOnItsOwnLease) {
    Tenant tenant;

    auto singleton = test::MakeObjectEntry("k1");
    ASSERT_TRUE(tenant.InsertObject(singleton));

    EXPECT_EQ(tenant.ObjectCount(), 1u);
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());

    // An ungrouped entry keeps the never-granted lease the envelope was
    // constructed with: present, and expired.
    ASSERT_NE(LeaseOf(singleton), nullptr);
    EXPECT_TRUE(singleton->WithSharedAccess(
        [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
            return metadata.IsLeaseExpired();
        }));
}

TEST(TenantTest, InsertObjectRejectsAKeyThatIsAlreadyRouted) {
    Tenant tenant;
    ASSERT_TRUE(tenant.InsertObject(test::MakeObjectEntry("k1", "g1")));

    // The second insert is rejected and registers nothing: no new route slot,
    // and no membership in its own group.
    EXPECT_FALSE(tenant.InsertObject(test::MakeObjectEntry("k1", "g2")));
    EXPECT_EQ(tenant.ObjectCount(), 1u);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 1u);
    EXPECT_TRUE(tenant.GroupMembers("g2").empty());
}

TEST(TenantTest, EraseObjectIfHonoursTheEntryIdentity) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "");
    ASSERT_TRUE(tenant.InsertObject(entry));
    EXPECT_EQ(tenant.Get("k1"), entry);

    // A different handle for the same key does not erase the routed entry.
    EXPECT_FALSE(tenant.EraseObjectIf(test::MakeObjectEntry("k1")));
    EXPECT_TRUE(tenant.ContainsObject("k1"));

    EXPECT_TRUE(tenant.EraseObjectIf(entry));
    EXPECT_FALSE(tenant.ContainsObject("k1"));
    EXPECT_EQ(tenant.Get("k1"), nullptr);
    EXPECT_FALSE(tenant.EraseObjectIf(entry));
}

TEST(TenantTest, RemoveObjectDropsRouteGroupAndLeasesTogether) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(entry));
    tenant.PutDynamicReplicationLease(entry, UUID{7, 8},
                                      LeaseFor(entry, UUID{7, 8}));
    ASSERT_FALSE(tenant.Empty());

    EXPECT_TRUE(tenant.RemoveObject(entry));

    // One call drops what a teardown otherwise drops by hand: the route slot,
    // the group membership and the leases still in flight for that key.
    EXPECT_FALSE(tenant.ContainsObject("k1"));
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());
    EXPECT_FALSE(tenant.FindDynamicReplicationLease(UUID{7, 8}).has_value());
    EXPECT_TRUE(tenant.Empty());
}

TEST(TenantTest, RemoveObjectRequiresTheEntryStillOnTheRoute) {
    Tenant tenant;
    auto published = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(published));

    // A different handle for the same key is not what the route publishes, so
    // the slot survives and the caller learns nothing was removed.
    EXPECT_FALSE(tenant.RemoveObject(test::MakeObjectEntry("k1", "g1")));
    EXPECT_TRUE(tenant.ContainsObject("k1"));
    EXPECT_EQ(tenant.Get("k1"), published);

    // The published entry itself is removed.
    EXPECT_TRUE(tenant.RemoveObject(published));
    EXPECT_FALSE(tenant.ContainsObject("k1"));

    // A null handle changes nothing.
    EXPECT_FALSE(tenant.RemoveObject(std::shared_ptr<ObjectEntry>{}));
}

TEST(TenantTest, RemoveObjectLeavesTheReplacementUntouched) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    tenant.PutDynamicReplicationLease(first, UUID{7, 8},
                                      LeaseFor(first, UUID{7, 8}));
    ASSERT_TRUE(tenant.RemoveObject(first));

    auto second = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(second));
    const auto second_lease = LeaseOf(second);
    ASSERT_NE(second_lease, nullptr);
    tenant.PutDynamicReplicationLease(second, UUID{9, 10},
                                      LeaseFor(second, UUID{9, 10}));

    // The route publishes the newer object, so the older handle owns nothing
    // here: neither the membership nor the lease the newer object registered
    // may be dropped.
    EXPECT_FALSE(tenant.RemoveObject(first));
    EXPECT_EQ(tenant.Get("k1"), second);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 1u);
    EXPECT_TRUE(tenant.FindDynamicReplicationLease(UUID{9, 10}).has_value());
    EXPECT_EQ(LeaseOf(second).get(), second_lease.get());
}

TEST(TenantTest, RemoveObjectIfGenerationRejectsAStaleGeneration) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    const uint64_t stale = first->generation();
    ASSERT_TRUE(tenant.RemoveObject(first));

    auto second = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(second));
    tenant.PutDynamicReplicationLease(second, UUID{9, 10},
                                      LeaseFor(second, UUID{9, 10}));

    // The recorded generation is no longer published, so nothing is touched.
    EXPECT_FALSE(tenant.RemoveObjectIfGeneration("k1", stale));
    EXPECT_EQ(tenant.Get("k1"), second);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 1u);
    EXPECT_TRUE(tenant.FindDynamicReplicationLease(UUID{9, 10}).has_value());

    // The current generation tears the object down completely.
    EXPECT_TRUE(tenant.RemoveObjectIfGeneration("k1", second->generation()));
    EXPECT_FALSE(tenant.ContainsObject("k1"));
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());
    EXPECT_FALSE(tenant.FindDynamicReplicationLease(UUID{9, 10}).has_value());

    // A generation that was never published never matches.
    EXPECT_FALSE(tenant.RemoveObjectIfGeneration("k1", 0));
}

TEST(TenantTest, IsCurrentRejectsTheUnpublishedGeneration) {
    Tenant tenant;
    auto unpublished = test::MakeObjectEntry("k1", "g1");
    EXPECT_EQ(unpublished->generation(), 0u);
    EXPECT_FALSE(tenant.IsCurrent("k1", unpublished->generation()));

    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    const uint64_t first_generation = first->generation();
    ASSERT_NE(first_generation, 0u);
    EXPECT_TRUE(tenant.IsCurrent("k1", first_generation));

    ASSERT_TRUE(tenant.RemoveObject(first));
    EXPECT_FALSE(tenant.IsCurrent("k1", first_generation));

    auto second = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(second));
    EXPECT_FALSE(tenant.IsCurrent("k1", first_generation));
    EXPECT_TRUE(tenant.IsCurrent("k1", second->generation()));
}

TEST(TenantTest, WithPublishedObjectRunsOnlyOnThePublishedEntry) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));

    bool ran = false;
    EXPECT_TRUE(tenant.WithPublishedObject(
        "k1", [&](ObjectMetadata&, ObjectEntry::State& state) {
            ran = true;
            state.is_processing = true;
        }));
    EXPECT_TRUE(ran);
    EXPECT_TRUE(IsProcessing(first));

    // After the key is published again, the same call lands on the newer entry,
    // never on the handle a caller kept from before.
    ASSERT_TRUE(tenant.RemoveObject(first));
    auto second = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(second));
    EXPECT_TRUE(tenant.WithPublishedObject(
        "k1", [](ObjectMetadata&, ObjectEntry::State& state) {
            state.is_processing = false;
        }));
    EXPECT_FALSE(IsProcessing(second));
    // The handle from before was not the one that changed.
    EXPECT_TRUE(IsProcessing(first));

    // Nothing is routed under the key any more.
    ASSERT_TRUE(tenant.RemoveObject(second));
    EXPECT_FALSE(tenant.WithPublishedObject(
        "k1", [](ObjectMetadata&, ObjectEntry::State&) {}));

    // A torn-down entry is not a target either.
    auto third = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(third));
    third->WithExclusiveAccess([](ObjectMetadata&, ObjectEntry::State& state) {
        state.is_torn_down = true;
    });
    EXPECT_FALSE(tenant.WithPublishedObject(
        "k1", [](ObjectMetadata&, ObjectEntry::State&) {}));
}

TEST(TenantTest, UnregisterGroupMemberRequiresTheCapturedGeneration) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(entry));
    const uint64_t first_generation = entry->generation();

    // The same entry is published again after a teardown, which renumbers it in
    // place: the membership it just registered carries the new generation.
    ASSERT_TRUE(tenant.RemoveObject(entry));
    ASSERT_TRUE(tenant.InsertObject(entry));
    const uint64_t second_generation = entry->generation();
    ASSERT_NE(second_generation, first_generation);
    ASSERT_EQ(tenant.GroupMembers("g1").size(), 1u);

    // A caller that captured the generation of the first publication leaves the
    // membership, and the lease the entry holds, alone: both belong to the
    // publication that is routed now.
    tenant.UnregisterGroupMember(entry, first_generation);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 1u);
    EXPECT_NE(LeaseOf(entry), nullptr);

    tenant.UnregisterGroupMember(entry, second_generation);
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());
}

TEST(TenantTest, AGroupKeepsOneLeaseAcrossReplacementOfItsKeys) {
    // A member's lease is decided by the group it belongs to, and the group is
    // replaced only when its last member leaves. Both halves are checked here
    // without concurrency, because a concurrent reader cannot decide which
    // group instance a listed member belongs to: the listing and the handle it
    // resolves can straddle a group being dropped and rebuilt, and then the two
    // members legitimately carry the lease of different group instances.
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    auto second = test::MakeObjectEntry("k2", "g1");
    ASSERT_TRUE(tenant.InsertObject(second));
    auto group_lease = LeaseOf(first);
    ASSERT_NE(group_lease, nullptr);
    EXPECT_EQ(LeaseOf(second).get(), group_lease.get());

    // Replacing one key of the group re-registers that member and keeps both on
    // the group's one lease.
    ASSERT_TRUE(tenant.RemoveObject(first));
    auto replacement = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(replacement));
    ASSERT_EQ(tenant.GroupMembers("g1").size(), 2u);
    EXPECT_EQ(LeaseOf(replacement).get(), group_lease.get());
    EXPECT_EQ(LeaseOf(second).get(), group_lease.get());

    // The group, and its lease, go with the last member; a later publication
    // gets the fresh lease of a new group.
    ASSERT_TRUE(tenant.RemoveObject(replacement));
    ASSERT_TRUE(tenant.RemoveObject(second));
    ASSERT_TRUE(tenant.GroupMembers("g1").empty());

    auto late = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(late));
    ASSERT_NE(LeaseOf(late), nullptr);
    EXPECT_NE(LeaseOf(late).get(), group_lease.get());
}

TEST(TenantTest, RemoveObjectClearsAGenerationWhoseSlotIsAlreadyGone) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(entry));
    tenant.PutDynamicReplicationLease(entry, UUID{7, 8},
                                      LeaseFor(entry, UUID{7, 8}));
    ASSERT_FALSE(tenant.Empty());

    // What a publish that failed after taking the slot leaves behind: the slot
    // is rolled back, the membership and the lease it registered are not. The
    // teardown reports that it did not erase the slot, and still clears them.
    ASSERT_TRUE(tenant.EraseObjectIf(entry));
    EXPECT_FALSE(tenant.RemoveObject(entry));
    EXPECT_FALSE(tenant.ContainsObject("k1"));
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());
    EXPECT_FALSE(tenant.FindDynamicReplicationLease(UUID{7, 8}).has_value());
    EXPECT_TRUE(tenant.Empty());
}

TEST(TenantTest, AGroupSurvivesConcurrentReplacementOfItsKeys) {
    Tenant tenant;
    constexpr int kWriterRounds = 2000;

    // Writers publish both keys of one group and tear their own publication
    // down, so a teardown of one generation runs while another is registered.
    //
    // Nothing is asserted while they run: a reader cannot tell which group
    // instance a listed member belongs to, because the listing and the handle
    // it resolves can straddle a group being dropped and rebuilt, and the two
    // members then legitimately carry the lease of different instances. What a
    // broken race leaves behind is a resting state instead — a member record
    // dropped for an object that is still routed, a group dropped with members,
    // a lease lost — so it is checked once the writers are done and no one is
    // publishing.
    std::vector<std::thread> writers;
    for (int i = 0; i < 4; ++i) {
        writers.emplace_back([&] {
            for (int round = 0; round < kWriterRounds; ++round) {
                const std::string key = (round % 2 == 0) ? "k1" : "k2";
                auto entry = test::MakeObjectEntry(key, "g1");
                if (tenant.InsertObject(entry)) {
                    tenant.PutDynamicReplicationLease(
                        entry, UUID{7, 8}, LeaseFor(entry, UUID{7, 8}));
                }
                (void)tenant.RemoveObject(entry);
            }
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }

    // Whatever is routed now has to agree with the group table, and one group
    // has to hold one lease.
    std::shared_ptr<Lease> group_lease;
    for (const auto& entry : tenant.SnapshotObjects()) {
        const std::string& group_id = entry->group_id();
        ASSERT_FALSE(group_id.empty());
        const auto members = tenant.GroupMembers(group_id);
        EXPECT_NE(std::find(members.begin(), members.end(), entry->key()),
                  members.end());
        auto lease = LeaseOf(entry);
        ASSERT_NE(lease, nullptr);
        if (group_lease == nullptr) {
            group_lease = std::move(lease);
        } else {
            EXPECT_EQ(group_lease.get(), lease.get());
        }
    }
    EXPECT_TRUE(tenant.Empty());
}

TEST(TenantTest, EmptyTracksObjectsGroupsAndLeases) {
    Tenant tenant;
    EXPECT_TRUE(tenant.Empty());

    auto grouped = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(grouped));
    EXPECT_FALSE(tenant.Empty());

    // Erasing the route slot leaves the membership behind, so the tenant is
    // still non-empty until that is dropped too.
    ASSERT_TRUE(tenant.EraseObjectIf(grouped));
    EXPECT_FALSE(tenant.Empty());
    tenant.UnregisterGroupMember(grouped, grouped->generation());
    EXPECT_TRUE(tenant.Empty());
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());

    // A lease in flight is state of its own.
    tenant.PutDynamicReplicationLease(grouped, UUID{7, 8},
                                      LeaseFor(grouped, UUID{7, 8}));
    EXPECT_FALSE(tenant.Empty());
    EXPECT_TRUE(tenant.RemoveDynamicReplicationLease(UUID{7, 8}));
    EXPECT_TRUE(tenant.Empty());
}

TEST(TenantTest, RebuildGroupStateRegroupsTheSameMembers) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    auto second = test::MakeObjectEntry("k2", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    ASSERT_TRUE(tenant.InsertObject(second));
    // A restored tenant starts without membership, so the rebuild is what
    // re-registers these members.
    tenant.UnregisterGroupMember(first, first->generation());
    tenant.UnregisterGroupMember(second, second->generation());
    ASSERT_TRUE(tenant.GroupMembers("g1").empty());

    tenant.RebuildGroupState();

    // Membership is back, and the rebuilt members share one lease again.
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 2u);
    auto rebuilt_first = LeaseOf(first);
    auto rebuilt_second = LeaseOf(second);
    ASSERT_NE(rebuilt_first, nullptr);
    EXPECT_EQ(rebuilt_first.get(), rebuilt_second.get());
}

TEST(TenantTest, ResetDynamicReplicationStateClearsTheLeasesItHolds) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(entry));

    // What a replica-action proposal leaves behind: one lease in flight, a
    // cooldown before the next action, and the proposal the entry waits on.
    tenant.PutDynamicReplicationLease(entry, UUID{7, 8},
                                      LeaseFor(entry, UUID{7, 8}));
    entry->WithExclusiveAccess([](ObjectMetadata&, ObjectEntry::State& state) {
        state.dynamic_replication_pending = DynamicReplicaPending{};
        state.dynamic_replication_cooldown =
            std::chrono::steady_clock::now() + std::chrono::seconds(1);
    });
    ASSERT_FALSE(tenant.Empty());

    entry->WithExclusiveAccess([&](ObjectMetadata&, ObjectEntry::State& state) {
        tenant.ResetDynamicReplicationState(state, entry);
    });

    // The reset drops the lease and the cooldown and leaves the object routed,
    // which is what separates this path from RemoveObject.
    EXPECT_TRUE(tenant.ContainsObject("k1"));
    EXPECT_FALSE(tenant.FindDynamicReplicationLease(UUID{7, 8}).has_value());
    EXPECT_TRUE(entry->WithSharedAccess(
        [](const ObjectMetadata&, const ObjectEntry::State& state) {
            return !state.dynamic_replication_pending.has_value() &&
                   state.dynamic_replication_cooldown ==
                       std::chrono::steady_clock::time_point{};
        }));
    EXPECT_FALSE(tenant.Empty());
}

TEST(TenantTest, PromotionCandidateKeysTrackWhatWasIndexed) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(entry));
    const uint64_t generation = entry->generation();
    EXPECT_TRUE(tenant.PromotionCandidateKeys().empty());

    tenant.IndexPromotionCandidate("k1", generation);
    tenant.IndexPromotionCandidate("k2", generation);
    tenant.IndexPromotionCandidate("k1", generation);
    EXPECT_EQ(tenant.PromotionCandidateKeys().size(), 2u);

    // An older generation does not unindex what this one indexed.
    tenant.UnindexPromotionCandidate("k2", generation + 1);
    EXPECT_EQ(tenant.PromotionCandidateKeys().size(), 2u);

    tenant.UnindexPromotionCandidate("k1", generation);
    auto keys = tenant.PromotionCandidateKeys();
    ASSERT_EQ(keys.size(), 1u);
    EXPECT_EQ(keys[0], "k2");
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
