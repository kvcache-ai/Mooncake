#include "group_index.h"

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(GroupIndexTest, AddMemberCreatesAndSharesOneLeasePerGroup) {
    GroupIndex index;

    const auto a1 = index.AddMember("g1", "k1");
    const auto a2 = index.AddMember("g1", "k2");
    const auto b = index.AddMember("g2", "k1");

    ASSERT_NE(a1, nullptr);
    EXPECT_EQ(a1.get(), a2.get());  // same group -> same Lease
    EXPECT_NE(a1.get(), b.get());   // different group -> its own
}

TEST(GroupIndexTest, UngroupedMemberHasNoLease) {
    GroupIndex index;

    // An empty group_id is the ungrouped case: nothing is registered, so two
    // ungrouped objects can never end up sharing one lease.
    EXPECT_EQ(index.AddMember("", "k1"), nullptr);
    EXPECT_TRUE(index.Empty());
}

TEST(GroupIndexTest, RepeatedMemberKeepsTheOneLease) {
    GroupIndex index;
    const auto first = index.AddMember("g1", "k1");
    ASSERT_NE(first, nullptr);

    // A repeat is not an error and does not disturb the group: the caller gets
    // the same lease back and the membership stays single.
    const auto again = index.AddMember("g1", "k1");
    EXPECT_EQ(first.get(), again.get());
    EXPECT_EQ(index.Members("g1").size(), 1u);
}

TEST(GroupIndexTest, AddRemoveGroupMembers) {
    GroupIndex index;

    EXPECT_NE(index.AddMember("g1", "k1"), nullptr);
    EXPECT_NE(index.AddMember("g1", "k2"), nullptr);
    EXPECT_FALSE(index.Empty());

    auto members = index.Members("g1");
    EXPECT_EQ(members.size(), 2u);

    EXPECT_TRUE(index.RemoveMember("g1", "k1"));
    auto after = index.Members("g1");
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after[0], "k2");

    EXPECT_TRUE(index.RemoveMember("g1", "k2"));
    // The member is already gone.
    EXPECT_FALSE(index.RemoveMember("g1", "k2"));
    EXPECT_TRUE(index.Empty());
}

TEST(GroupIndexTest, EmptyGroupIsDroppedOnLastMemberRemoved) {
    GroupIndex index;
    const auto before = index.AddMember("g1", "k1");
    ASSERT_NE(before, nullptr);

    EXPECT_TRUE(index.RemoveMember("g1", "k1"));
    EXPECT_TRUE(index.Members("g1").empty());
    EXPECT_TRUE(index.Empty());

    // The dropped group is not revived: the re-add materializes a fresh group
    // with a new Lease.
    const auto after = index.AddMember("g1", "k1");
    ASSERT_NE(after, nullptr);
    EXPECT_NE(before.get(), after.get());
}

TEST(GroupIndexTest, MembershipIsASetOfKeysUnderTheOneGroupLease) {
    GroupIndex index;

    // Membership is a set of keys, so the second registration of a key neither
    // duplicates the member nor hands out a second lease.
    const auto lease = index.AddMember("g1", "k1");
    ASSERT_NE(lease, nullptr);
    const auto again = index.AddMember("g1", "k1");
    EXPECT_EQ(again.get(), lease.get());
    EXPECT_EQ(index.Members("g1").size(), 1u);

    // A key the group never held is not a member, so removing it drops
    // nothing and leaves the group materialized.
    EXPECT_FALSE(index.RemoveMember("g1", "k2"));
    EXPECT_FALSE(index.RemoveMember("g2", "k1"));
    EXPECT_EQ(index.Members("g1").size(), 1u);
    EXPECT_FALSE(index.Empty());

    // The last member takes the group with it, and a later registration builds
    // a group whose lease is not the dropped one.
    EXPECT_TRUE(index.RemoveMember("g1", "k1"));
    EXPECT_TRUE(index.Members("g1").empty());
    EXPECT_TRUE(index.Empty());

    const auto rebuilt = index.AddMember("g1", "k1");
    ASSERT_NE(rebuilt, nullptr);
    EXPECT_NE(rebuilt.get(), lease.get());
    EXPECT_EQ(index.Members("g1").size(), 1u);
}

TEST(GroupIndexTest, ConcurrentMembershipAcrossStripesStaysConsistent) {
    // Few stripes, so distinct groups share a stripe and collide. Each thread
    // owns its member keys, so it knows exactly which memberships are its own
    // and which lease each of them was handed.
    StripedGroupIndex<4> index;
    constexpr size_t kThreads = 8;
    constexpr size_t kGroups = 8;
    constexpr size_t kMembersPerThread = 4;
    constexpr int kRounds = 20000;

    using Membership =
        std::map<std::pair<size_t, size_t>, std::shared_ptr<Lease>>;
    std::vector<Membership> held(kThreads);
    std::atomic<int> violations{0};
    const auto group_name = [](size_t group) {
        return "g" + std::to_string(group);
    };
    const auto member_name = [](size_t thread, size_t member) {
        return "t" + std::to_string(thread) + "_m" + std::to_string(member);
    };

    std::vector<std::thread> threads;
    for (size_t t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937 rng(static_cast<unsigned>(t));
            Membership& mine = held[t];
            for (int round = 0; round < kRounds; ++round) {
                const size_t group = rng() % kGroups;
                const size_t member = rng() % kMembersPerThread;
                const auto it = mine.find({group, member});
                if (it != mine.end()) {
                    if (!index.RemoveMember(group_name(group),
                                            member_name(t, member))) {
                        violations.fetch_add(1, std::memory_order_relaxed);
                    }
                    mine.erase(it);
                    continue;
                }
                auto lease =
                    index.AddMember(group_name(group), member_name(t, member));
                // Another of this thread's members kept the group alive the
                // whole time, so the new member joined that same instance.
                for (const auto& [slot, other] : mine) {
                    if (slot.first == group && other != lease) {
                        violations.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                if (lease == nullptr) {
                    violations.fetch_add(1, std::memory_order_relaxed);
                }
                mine.emplace(std::make_pair(group, member), std::move(lease));
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(violations.load(), 0);

    // At rest, each group lists exactly the members the threads still hold,
    // and every one of them still holds the group's current lease.
    bool any_member = false;
    for (size_t group = 0; group < kGroups; ++group) {
        std::set<std::string> expected;
        for (size_t t = 0; t < kThreads; ++t) {
            for (const auto& [slot, lease] : held[t]) {
                if (slot.first != group) {
                    continue;
                }
                expected.insert(member_name(t, slot.second));
                EXPECT_EQ(index.AddMember(group_name(group),
                                          member_name(t, slot.second)),
                          lease);
            }
        }
        const auto members = index.Members(group_name(group));
        EXPECT_EQ(std::set<std::string>(members.begin(), members.end()),
                  expected)
            << group_name(group);
        any_member = any_member || !expected.empty();
    }
    EXPECT_EQ(index.Empty(), !any_member);

    // Dropping every remaining member drops every group.
    for (size_t t = 0; t < kThreads; ++t) {
        for (const auto& [slot, lease] : held[t]) {
            EXPECT_TRUE(index.RemoveMember(group_name(slot.first),
                                           member_name(t, slot.second)));
        }
    }
    EXPECT_TRUE(index.Empty());
}

}  // namespace
}  // namespace mooncake
