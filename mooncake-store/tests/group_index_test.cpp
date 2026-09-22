#include "group_index.h"

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

// The publication generation a membership is registered under. Most of these
// tests are about the table itself, so one value that is not 0 is enough; the
// ones about replacement use a second one.
constexpr uint64_t kGeneration = 7;

TEST(GroupIndexTest, AddMemberCreatesAndSharesOneLeasePerGroup) {
    GroupIndex index;

    const auto a1 = index.AddMember("g1", "k1", kGeneration);
    const auto a2 = index.AddMember("g1", "k2", kGeneration);
    const auto b = index.AddMember("g2", "k1", kGeneration);

    ASSERT_NE(a1, nullptr);
    EXPECT_EQ(a1.get(), a2.get());  // same group -> same Lease
    EXPECT_NE(a1.get(), b.get());   // different group -> its own
}

TEST(GroupIndexTest, UngroupedMemberHasNoLease) {
    GroupIndex index;

    // An empty group_id is the ungrouped case: nothing is registered, so two
    // ungrouped objects can never end up sharing one lease.
    EXPECT_EQ(index.AddMember("", "k1", kGeneration), nullptr);
    EXPECT_TRUE(index.Empty());
}

TEST(GroupIndexTest, RepeatedMemberKeepsTheOneLease) {
    GroupIndex index;
    const auto first = index.AddMember("g1", "k1", kGeneration);
    ASSERT_NE(first, nullptr);

    // A repeat is not an error and does not disturb the group: the caller gets
    // the same lease back and the membership stays single.
    const auto again = index.AddMember("g1", "k1", kGeneration);
    EXPECT_EQ(first.get(), again.get());
    EXPECT_EQ(index.Members("g1").size(), 1u);
}

TEST(GroupIndexTest, AddRemoveGroupMembers) {
    GroupIndex index;

    EXPECT_NE(index.AddMember("g1", "k1", kGeneration), nullptr);
    EXPECT_NE(index.AddMember("g1", "k2", kGeneration), nullptr);
    EXPECT_FALSE(index.Empty());

    auto members = index.Members("g1");
    EXPECT_EQ(members.size(), 2u);

    EXPECT_TRUE(index.RemoveMember("g1", "k1", kGeneration));
    auto after = index.Members("g1");
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after[0], "k2");

    EXPECT_TRUE(index.RemoveMember("g1", "k2", kGeneration));
    // The member is already gone.
    EXPECT_FALSE(index.RemoveMember("g1", "k2", kGeneration));
    EXPECT_TRUE(index.Empty());
}

TEST(GroupIndexTest, EmptyGroupIsDroppedOnLastMemberRemoved) {
    GroupIndex index;
    const auto before = index.AddMember("g1", "k1", kGeneration);
    ASSERT_NE(before, nullptr);

    EXPECT_TRUE(index.RemoveMember("g1", "k1", kGeneration));
    EXPECT_TRUE(index.Members("g1").empty());
    EXPECT_TRUE(index.Empty());

    // The dropped group is not revived: the re-add materializes a fresh group
    // with a new Lease.
    const auto after = index.AddMember("g1", "k1", kGeneration);
    ASSERT_NE(after, nullptr);
    EXPECT_NE(before.get(), after.get());
}

TEST(GroupIndexTest, RemoveMemberRequiresTheGenerationItRegistered) {
    GroupIndex index;
    constexpr uint64_t kNewer = kGeneration + 1;

    // The older generation registers the member, then a newer one re-registers
    // it: the record carries the newer generation now, and the repeat still
    // yields the group's one lease.
    const auto older = index.AddMember("g1", "k1", kGeneration);
    ASSERT_NE(older, nullptr);
    const auto newer = index.AddMember("g1", "k1", kNewer);
    EXPECT_EQ(older.get(), newer.get());

    // Only the generation in the record removes it: the older one leaves it,
    // and so do a generation that registered nothing and the rejected 0.
    EXPECT_FALSE(index.RemoveMember("g1", "k1", kGeneration));
    EXPECT_FALSE(index.RemoveMember("g1", "k1", kNewer + 1));
    EXPECT_FALSE(index.RemoveMember("g1", "k1", 0));
    EXPECT_EQ(index.Members("g1").size(), 1u);
    EXPECT_FALSE(index.Empty());

    // The generation in the record removes it, and takes the group with its
    // last member.
    EXPECT_TRUE(index.RemoveMember("g1", "k1", kNewer));
    EXPECT_TRUE(index.Members("g1").empty());
    EXPECT_TRUE(index.Empty());
}

}  // namespace
}  // namespace mooncake
