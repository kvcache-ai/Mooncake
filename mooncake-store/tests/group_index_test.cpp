#include "group_index.h"

#include <string>

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
    EXPECT_FALSE(index.RemoveMember("g1", "k2"));  // already gone
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

}  // namespace
}  // namespace mooncake
