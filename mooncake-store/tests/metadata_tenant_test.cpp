#include "metadata/tenant.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <atomic>
#include <chrono>
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
    return test::ObjectEntryTestPeer::WithSharedAccess(
        *entry, [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
            return metadata.lease_;
        });
}

bool IsProcessing(const std::shared_ptr<ObjectEntry>& entry) {
    return test::ObjectEntryTestPeer::WithSharedAccess(
        *entry, [](const ObjectMetadata&, const ObjectEntry::State& state) {
            return state.is_processing;
        });
}

enum class TearDownResult { kRemoved, kNotHeld, kLostSlot };

// Tears `entry` down the way a caller must, under its write hold. An entry the
// tenant will not hold (not published, or already torn down) is `kNotHeld`. A
// held entry is still routed while it releases and gone once the teardown
// returns; anything else (`kLostSlot`) means the route lost track of it.
TearDownResult TearDownObject(Tenant& tenant,
                              const std::shared_ptr<ObjectEntry>& entry) {
    auto hold = tenant.WriteHold(entry);
    if (!hold) {
        return TearDownResult::kNotHeld;
    }
    bool routed_while_releasing = false;
    const bool torn_down = tenant.TearDownObject(*hold, [&] {
        routed_while_releasing = tenant.Get(entry->key()) == entry;
    });
    return torn_down && routed_while_releasing &&
                   tenant.Get(entry->key()) != entry
               ? TearDownResult::kRemoved
               : TearDownResult::kLostSlot;
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
    EXPECT_TRUE(test::ObjectEntryTestPeer::WithSharedAccess(
        *singleton,
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

TEST(TenantTest, TearDownObjectDropsTheSlotAndTheMembershipTogether) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(entry));
    ASSERT_FALSE(tenant.Empty());

    // The slot names this entry, so the membership is its own and a teardown
    // drops both in one call.
    EXPECT_EQ(TearDownObject(tenant, entry), TearDownResult::kRemoved);

    EXPECT_FALSE(tenant.ContainsObject("k1"));
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());
    EXPECT_TRUE(tenant.Empty());

    // The claim is taken, so the entry is not held again.
    EXPECT_EQ(TearDownObject(tenant, entry), TearDownResult::kNotHeld);
}

TEST(TenantTest, TearDownObjectRunsOnceUnderOneHold) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1");
    ASSERT_TRUE(tenant.InsertObject(entry));

    auto hold = tenant.WriteHold(entry);
    ASSERT_TRUE(hold.has_value());
    int releases = 0;
    EXPECT_TRUE(tenant.TearDownObject(*hold, [&] { ++releases; }));
    // A second eraser under the same hold finds the claim taken and releases
    // nothing again.
    EXPECT_FALSE(tenant.TearDownObject(*hold, [&] { ++releases; }));
    EXPECT_EQ(releases, 1);
    EXPECT_FALSE(tenant.ContainsObject("k1"));
}

TEST(TenantTest, TearDownObjectRemovesOnlyTheEntryOnTheRoute) {
    Tenant tenant;
    auto published = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(published));
    const auto published_lease = LeaseOf(published);

    // A different handle for the same key is not what the route publishes, so
    // it is not held, and the slot and membership of the published one stay.
    EXPECT_EQ(TearDownObject(tenant, test::MakeObjectEntry("k1", "g1")),
              TearDownResult::kNotHeld);
    EXPECT_EQ(tenant.Get("k1"), published);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 1u);
    EXPECT_EQ(LeaseOf(published).get(), published_lease.get());

    // The published entry itself is removed.
    EXPECT_EQ(TearDownObject(tenant, published), TearDownResult::kRemoved);
    EXPECT_FALSE(tenant.ContainsObject("k1"));
}

TEST(TenantTest, TearDownObjectLeavesAReplacementAndItsMembershipUntouched) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(first));
    ASSERT_EQ(TearDownObject(tenant, first), TearDownResult::kRemoved);

    auto second = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(second));
    const auto second_lease = LeaseOf(second);
    ASSERT_NE(second_lease, nullptr);

    // The slot holds a newer publication of the same key, so the older handle
    // owns nothing under it: the membership the newer object registered stays.
    EXPECT_EQ(TearDownObject(tenant, first), TearDownResult::kNotHeld);
    EXPECT_EQ(tenant.Get("k1"), second);
    EXPECT_EQ(tenant.GroupMembers("g1").size(), 1u);
    EXPECT_EQ(LeaseOf(second).get(), second_lease.get());
}

// A hold is made only on the entry the route publishes: never on one not yet
// published, one torn down, or a kept handle whose key was published again.
TEST(TenantTest, HoldIsMadeOnlyOnThePublishedEntry) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1");
    EXPECT_FALSE(tenant.WriteHold(first).has_value());
    EXPECT_FALSE(tenant.ReadHold("k1").has_value());

    ASSERT_TRUE(tenant.InsertObject(first));
    {
        auto hold = tenant.WriteHold("k1");
        ASSERT_TRUE(hold.has_value());
        EXPECT_EQ(hold->handle(), first);
        hold->state().is_processing = true;
    }
    EXPECT_TRUE(IsProcessing(first));
    {
        const auto hold = tenant.ReadHold(first);
        ASSERT_TRUE(hold.has_value());
        EXPECT_TRUE(hold->state().is_processing);
    }

    ASSERT_EQ(TearDownObject(tenant, first), TearDownResult::kRemoved);
    EXPECT_FALSE(tenant.WriteHold(first).has_value());
    EXPECT_FALSE(tenant.ReadHold("k1").has_value());

    auto second = test::MakeObjectEntry("k1");
    ASSERT_TRUE(tenant.InsertObject(second));
    EXPECT_FALSE(tenant.ReadHold(first).has_value());
    auto hold = tenant.WriteHold("k1");
    ASSERT_TRUE(hold.has_value());
    EXPECT_EQ(hold->handle(), second);
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
    ASSERT_EQ(TearDownObject(tenant, first), TearDownResult::kRemoved);
    auto replacement = test::MakeObjectEntry("k1", "g1");
    ASSERT_TRUE(tenant.InsertObject(replacement));
    ASSERT_EQ(tenant.GroupMembers("g1").size(), 2u);
    EXPECT_EQ(LeaseOf(replacement).get(), group_lease.get());
    EXPECT_EQ(LeaseOf(second).get(), group_lease.get());

    // The group, and its lease, go with the last member; a later publication
    // gets the fresh lease of a new group.
    ASSERT_EQ(TearDownObject(tenant, replacement), TearDownResult::kRemoved);
    ASSERT_EQ(TearDownObject(tenant, second), TearDownResult::kRemoved);
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
    // The other threads yield between rounds: std::shared_mutex prefers
    // readers on glibc, and readers that never pause starve the writers on a
    // machine with few cores. The cap only guards against a hang.
    constexpr uint64_t kOtherRounds = 200000;

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
                // Leave the entry published for a moment, so the other
                // threads meet it.
                std::this_thread::yield();
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
            for (uint64_t i = r; i < kOtherRounds &&
                                 !writers_done.load(std::memory_order_relaxed);
                 ++i) {
                std::this_thread::yield();
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
                    case TearDownResult::kNotHeld:
                        break;
                }
            }
        });
    }
    // Mutators only ever run on the published, live entry, and that entry is a
    // member of its group for as long as they hold its lock.
    for (int m = 0; m < 2; ++m) {
        threads.emplace_back([&, m] {
            for (uint64_t i = m; i < kOtherRounds &&
                                 !writers_done.load(std::memory_order_relaxed);
                 ++i) {
                std::this_thread::yield();
                const std::string& key = keys[i % 2];
                if (auto hold = tenant.WriteHold(key)) {
                    mutations.fetch_add(1, std::memory_order_relaxed);
                    if (!is_member(key)) {
                        violation();
                    }
                    hold->state().is_processing = !hold->state().is_processing;
                }
            }
        });
    }
    // An observer holds both keys at once (always k1 before k2, and no other
    // thread holds two entry locks). Two live members of one group joined the
    // same group instance, so they carry one lease; a publication observed
    // half-wired would carry the lease it was constructed with instead.
    threads.emplace_back([&] {
        for (uint64_t i = 0;
             i < kOtherRounds && !writers_done.load(std::memory_order_relaxed);
             ++i) {
            std::this_thread::yield();
            const auto first = tenant.Get("k1");
            const auto second = tenant.Get("k2");
            if (first == nullptr || second == nullptr) {
                continue;
            }
            const auto first_hold = tenant.ReadHold(first);
            if (!first_hold) {
                continue;
            }
            const auto second_hold = tenant.ReadHold(second);
            if (!second_hold) {
                continue;
            }
            pairs_observed.fetch_add(1, std::memory_order_relaxed);
            if (first_hold->metadata().lease_ !=
                    second_hold->metadata().lease_ ||
                !is_member("k1") || !is_member("k2")) {
                violation();
            }
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
    std::vector<std::shared_ptr<ObjectEntry>> entries;
    for (auto object : tenant.ReadCursor()) {
        routed.push_back(object.key());
        entries.push_back(object.handle());
    }
    auto members = tenant.GroupMembers("g1");
    std::sort(routed.begin(), routed.end());
    std::sort(members.begin(), members.end());
    EXPECT_EQ(members, routed);

    for (const auto& entry : entries) {
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

    // Tearing the object down drops its membership with the slot.
    ASSERT_EQ(TearDownObject(tenant, grouped), TearDownResult::kRemoved);
    EXPECT_TRUE(tenant.Empty());
    EXPECT_TRUE(tenant.GroupMembers("g1").empty());
}

// Starts a primary write on `entry`. A published entry is listed once its
// write hold is released; one not yet published only carries the work, which
// InsertObject lists.
void StartWrite(Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry) {
    if (auto hold = tenant.WriteHold(entry)) {
        hold->state().is_processing = true;
        return;
    }
    entry->WithUnpublished([](ObjectMetadata&, ObjectEntry::State& state) {
        state.is_processing = true;
    });
}

void FinishWrite(Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry) {
    auto hold = tenant.WriteHold(entry);
    ASSERT_TRUE(hold.has_value());
    hold->state().is_processing = false;
}

std::vector<std::string> SortedInFlightKeys(const Tenant& tenant) {
    auto keys = tenant.InFlightKeys();
    std::sort(keys.begin(), keys.end());
    return keys;
}

TEST(TenantTest, InFlightListsOnlyPublishedEntriesWithWork) {
    Tenant tenant;
    auto idle = test::MakeObjectEntry("idle");
    auto busy = test::MakeObjectEntry("busy");
    ASSERT_TRUE(tenant.InsertObject(idle));
    ASSERT_TRUE(tenant.InsertObject(busy));

    {
        auto hold = tenant.WriteHold(busy);
        ASSERT_TRUE(hold.has_value());
        hold->state().is_processing = true;
        // Listed as the hold is released, not before.
        EXPECT_TRUE(tenant.InFlightKeys().empty());
    }
    EXPECT_EQ(SortedInFlightKeys(tenant), (std::vector<std::string>{"busy"}));
    // A write hold on an entry with no work lists nothing.
    {
        auto hold = tenant.WriteHold(idle);
        ASSERT_TRUE(hold.has_value());
    }
    // Starting work again lists the entry once.
    StartWrite(tenant, busy);

    EXPECT_EQ(SortedInFlightKeys(tenant), (std::vector<std::string>{"busy"}));

    FinishWrite(tenant, busy);
    EXPECT_TRUE(tenant.InFlightKeys().empty());
}

TEST(TenantTest, InsertObjectTracksAnEntryPublishedWithWork) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1");
    // Work started before publication is tracked by the publication itself.
    StartWrite(tenant, entry);
    EXPECT_TRUE(tenant.InFlightKeys().empty());

    ASSERT_TRUE(tenant.InsertObject(entry));
    EXPECT_EQ(SortedInFlightKeys(tenant), (std::vector<std::string>{"k1"}));

    // A rejected duplicate is not published, so it is not listed either.
    auto duplicate = test::MakeObjectEntry("k1");
    StartWrite(tenant, duplicate);
    ASSERT_FALSE(tenant.InsertObject(duplicate));
    EXPECT_EQ(tenant.InFlightKeys().size(), 1u);

    FinishWrite(tenant, entry);
}

TEST(TenantTest, AnEntryStaysListedWhileAnyWorkRemains) {
    Tenant tenant;
    auto entry = test::MakeObjectEntry("k1");
    ASSERT_TRUE(tenant.InsertObject(entry));

    {
        auto hold = tenant.WriteHold(entry);
        ASSERT_TRUE(hold.has_value());
        hold->state().is_processing = true;
        hold->state().offloading_task =
            OffloadingTask{1, std::chrono::system_clock::now(), {}};
    }

    // The write finished but the offload has not, so the entry stays listed.
    FinishWrite(tenant, entry);
    EXPECT_EQ(SortedInFlightKeys(tenant), (std::vector<std::string>{"k1"}));

    {
        auto hold = tenant.WriteHold(entry);
        ASSERT_TRUE(hold.has_value());
        hold->state().offloading_task.reset();
    }
    EXPECT_TRUE(tenant.InFlightKeys().empty());
}

TEST(TenantTest, TearDownTakesAnEntryOffTheInFlightList) {
    Tenant tenant;
    auto first = test::MakeObjectEntry("k1");
    auto second = test::MakeObjectEntry("k2");
    ASSERT_TRUE(tenant.InsertObject(first));
    ASSERT_TRUE(tenant.InsertObject(second));
    StartWrite(tenant, first);
    StartWrite(tenant, second);

    // The entry still carries its write, yet leaves the list with its slot.
    ASSERT_EQ(TearDownObject(tenant, first), TearDownResult::kRemoved);
    EXPECT_EQ(SortedInFlightKeys(tenant), (std::vector<std::string>{"k2"}));

    // Work started on a torn-down entry is never listed.
    StartWrite(tenant, first);
    EXPECT_EQ(SortedInFlightKeys(tenant), (std::vector<std::string>{"k2"}));
    // The handle can outlive the tenant's interest in it.
    first.reset();

    FinishWrite(tenant, second);
}

TEST(TenantTest, DestroyingATenantReleasesItsInFlightEntries) {
    auto entry = test::MakeObjectEntry("k1");
    {
        Tenant tenant;
        ASSERT_TRUE(tenant.InsertObject(entry));
        StartWrite(tenant, entry);
        ASSERT_EQ(tenant.InFlightKeys().size(), 1u);
    }
    // The caller's handle outlives the tenant; the list let go of its hook
    // first, so the entry is destroyed unlinked.
    entry.reset();
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
