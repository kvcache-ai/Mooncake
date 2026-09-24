#include "metadata/tenant.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
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

enum class TearDownResult { kRemoved, kAlreadyClaimed, kLostSlot };

// Tears `entry` down the way a caller must: claim it under its own lock, then
// remove it while still holding that lock. A claimed entry is still routed, so
// failing to remove it (`kLostSlot`) means the route lost track of it.
TearDownResult TearDownObject(Tenant& tenant,
                              const std::shared_ptr<ObjectEntry>& entry) {
    return entry->WithExclusiveAccess(
        [&](ObjectMetadata&, ObjectEntry::State& state) {
            if (state.is_torn_down) {
                return TearDownResult::kAlreadyClaimed;
            }
            state.is_torn_down = true;
            return tenant.RemoveObject(entry) ? TearDownResult::kRemoved
                                              : TearDownResult::kLostSlot;
        });
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

TEST(TenantTest, RemoveObjectDropsTheSlotAndTheMembershipTogether) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(entry));
    ASSERT_FALSE(tenant.Empty());

    // The slot names this entry, so the membership is its own and a teardown
    // drops both in one call.
    EXPECT_TRUE(tenant.RemoveObject(entry));

    EXPECT_FALSE(tenant.ContainsObject("k1"));
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());
    EXPECT_TRUE(tenant.Empty());

    // The slot is gone, so a repeated teardown has nothing left to take.
    EXPECT_FALSE(tenant.RemoveObject(entry));
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

TEST(TenantTest, RemoveObjectLeavesAReplacementAndItsMembershipUntouched) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    ASSERT_TRUE(tenant.RemoveObject(first));

    auto second = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(second));
    const auto second_lease = LeaseOf(second);
    ASSERT_NE(second_lease, nullptr);

    // The slot holds a newer publication of the same key, so the older handle
    // owns nothing under it: the membership the newer object registered stays.
    EXPECT_FALSE(tenant.RemoveObject(first));
    EXPECT_EQ(tenant.Get("k1"), second);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 1u);
    EXPECT_EQ(LeaseOf(second).get(), second_lease.get());
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

TEST(TenantTest, UnregisterGroupMemberDropsOnlyTheEntryItIsGiven) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    auto second = test::MakeObjectEntry("k2", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    ASSERT_TRUE(tenant.InsertObject(second));
    const auto group_lease = LeaseOf(first);
    ASSERT_NE(group_lease, nullptr);
    ASSERT_EQ(LeaseOf(second).get(), group_lease.get());

    // The entry names both the group and the member to drop, so the group keeps
    // its other member and the one lease both of them hold.
    tenant.UnregisterGroupMember(first);
    ASSERT_EQ(tenant.GroupMembers("g1").size(), 1u);
    EXPECT_EQ(tenant.GroupMembers("g1")[0], "k2");
    EXPECT_FALSE(tenant.Empty());
    EXPECT_EQ(LeaseOf(second).get(), group_lease.get());

    // The member is already gone, so a repeat leaves the group as it is.
    tenant.UnregisterGroupMember(first);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 1u);

    tenant.UnregisterGroupMember(second);
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

TEST(TenantTest, SameKeyChurnKeepsTheRouteAndTheGroupConsistent) {
    // Two keys of one group are published and torn down over and over, while
    // other threads tear down whatever handle they resolve, mutate the
    // published entry and watch both keys at once. Every teardown follows the
    // caller protocol (`TearDownObject`), which is what the route relies on.
    Tenant tenant;
    const std::vector<std::string> keys = {"k1", "k2"};
    constexpr int kWriters = 4;
    constexpr int kWriterRounds = 20000;

    std::atomic<bool> writers_done{false};
    std::atomic<int> violations{0};
    std::atomic<uint64_t> published{0};
    std::atomic<uint64_t> torn_down_by_others{0};
    std::atomic<uint64_t> mutations{0};
    std::atomic<uint64_t> pairs_observed{0};
    const auto violation = [&] {
        violations.fetch_add(1, std::memory_order_relaxed);
    };
    const auto is_member = [&](const std::string& key) {
        const auto members = tenant.GroupMembers("g1");
        return std::find(members.begin(), members.end(), key) != members.end();
    };

    std::vector<std::thread> threads;
    // Writers publish a fresh entry and tear their own down, unless another
    // thread got to it first.
    for (int w = 0; w < kWriters; ++w) {
        threads.emplace_back([&, w] {
            for (int round = 0; round < kWriterRounds; ++round) {
                auto entry = test::MakeObjectEntry(keys[(round + w) % 2], "g1");
                if (!tenant.InsertObject(entry)) {
                    continue;
                }
                published.fetch_add(1, std::memory_order_relaxed);
                if (TearDownObject(tenant, entry) ==
                    TearDownResult::kLostSlot) {
                    violation();
                }
            }
        });
    }
    // Stale removers tear down whatever the route hands them, which may be an
    // entry a writer is still wiring or has just torn down itself.
    for (int r = 0; r < 2; ++r) {
        threads.emplace_back([&, r] {
            for (uint64_t i = r; !writers_done.load(std::memory_order_relaxed);
                 ++i) {
                const auto entry = tenant.Get(keys[i % 2]);
                if (entry == nullptr) {
                    continue;
                }
                switch (TearDownObject(tenant, entry)) {
                    case TearDownResult::kRemoved:
                        torn_down_by_others.fetch_add(
                            1, std::memory_order_relaxed);
                        break;
                    case TearDownResult::kLostSlot:
                        violation();
                        break;
                    case TearDownResult::kAlreadyClaimed:
                        break;
                }
            }
        });
    }
    // Mutators only ever run on the published, live entry, and that entry is a
    // member of its group for as long as they hold its lock.
    for (int m = 0; m < 2; ++m) {
        threads.emplace_back([&, m] {
            for (uint64_t i = m; !writers_done.load(std::memory_order_relaxed);
                 ++i) {
                const std::string& key = keys[i % 2];
                (void)tenant.WithPublishedObject(
                    key, [&](ObjectMetadata&, ObjectEntry::State& state) {
                        mutations.fetch_add(1, std::memory_order_relaxed);
                        if (state.is_torn_down || !is_member(key)) {
                            violation();
                        }
                        state.is_processing = !state.is_processing;
                    });
            }
        });
    }
    // An observer holds both keys at once (always k1 before k2, and no other
    // thread holds two entry locks). Two live members of one group joined the
    // same group instance, so they carry one lease; a publication observed
    // half-wired would carry the lease it was constructed with instead.
    threads.emplace_back([&] {
        while (!writers_done.load(std::memory_order_relaxed)) {
            const auto first = tenant.Get("k1");
            const auto second = tenant.Get("k2");
            if (first == nullptr || second == nullptr) {
                continue;
            }
            first->WithSharedAccess([&](const ObjectMetadata& first_metadata,
                                        const ObjectEntry::State& first_state) {
                second->WithSharedAccess(
                    [&](const ObjectMetadata& second_metadata,
                        const ObjectEntry::State& second_state) {
                        if (first_state.is_torn_down ||
                            second_state.is_torn_down) {
                            return;
                        }
                        pairs_observed.fetch_add(1, std::memory_order_relaxed);
                        if (first_metadata.lease_ != second_metadata.lease_ ||
                            !is_member("k1") || !is_member("k2")) {
                            violation();
                        }
                    });
            });
        }
    });

    for (int w = 0; w < kWriters; ++w) {
        threads[w].join();
    }
    writers_done.store(true, std::memory_order_relaxed);
    for (size_t t = kWriters; t < threads.size(); ++t) {
        threads[t].join();
    }

    EXPECT_EQ(violations.load(), 0);
    // Every racing path actually ran, so the run was not trivially quiet.
    EXPECT_GT(published.load(), 0u);
    EXPECT_GT(torn_down_by_others.load(), 0u);
    EXPECT_GT(mutations.load(), 0u);
    EXPECT_GT(pairs_observed.load(), 0u);

    // At rest, the group lists exactly the keys that are routed: no member
    // outlived its object, and no routed object lost its membership.
    std::vector<std::string> routed;
    for (const auto& entry : tenant.SnapshotObjects()) {
        routed.push_back(entry->key());
    }
    auto members = tenant.GroupMembers("g1");
    std::sort(routed.begin(), routed.end());
    std::sort(members.begin(), members.end());
    EXPECT_EQ(members, routed);

    for (const auto& entry : tenant.SnapshotObjects()) {
        EXPECT_EQ(TearDownObject(tenant, entry), TearDownResult::kRemoved);
    }
    EXPECT_TRUE(tenant.Empty());
}

TEST(TenantTest, EmptyTracksObjectsAndGroups) {
    Tenant tenant;
    EXPECT_TRUE(tenant.Empty());

    auto grouped = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(grouped));
    EXPECT_FALSE(tenant.Empty());

    // Removing the object drops its membership with the slot.
    ASSERT_TRUE(tenant.RemoveObject(grouped));
    EXPECT_TRUE(tenant.Empty());
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());
}

TEST(TenantTest, RebuildGroupStateRegroupsTheSameMembers) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    auto second = test::MakeObjectEntry("k2", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    ASSERT_TRUE(tenant.InsertObject(second));
    // A restored tenant starts without membership, so the rebuild is what
    // re-registers these members.
    tenant.UnregisterGroupMember(first);
    tenant.UnregisterGroupMember(second);
    ASSERT_TRUE(tenant.GroupMembers("g1").empty());

    tenant.RebuildGroupState();

    // Membership is back, and the rebuilt members share one lease again.
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 2u);
    auto rebuilt_first = LeaseOf(first);
    auto rebuilt_second = LeaseOf(second);
    ASSERT_NE(rebuilt_first, nullptr);
    EXPECT_EQ(rebuilt_first.get(), rebuilt_second.get());
}

}  // namespace
}  // namespace metadata
}  // namespace mooncake
