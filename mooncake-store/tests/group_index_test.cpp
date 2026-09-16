#include "group_index.h"

#include <chrono>
#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(GroupIndexTest, AddMemberCreatesAndSharesOneLeasePerGroup) {
    GroupIndex index;

    auto a1 = index.AddMember("g1", "k1");
    auto a2 = index.AddMember("g1", "k2");
    auto b = index.AddMember("g2", "k1");

    ASSERT_NE(a1, nullptr);
    EXPECT_EQ(a1.get(), a2.get());  // same group -> same shared Lease
    EXPECT_NE(a1.get(), b.get());   // different group -> distinct Lease
}

TEST(GroupIndexTest, AddRemoveGroupMembers) {
    GroupIndex index;

    EXPECT_NE(index.AddMember("g1", "k1"), nullptr);
    EXPECT_NE(index.AddMember("g1", "k2"), nullptr);

    auto members = index.Members("g1");
    EXPECT_EQ(members.size(), 2u);

    EXPECT_TRUE(index.RemoveMember("g1", "k1"));
    auto after = index.Members("g1");
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after[0], "k2");
}

TEST(GroupIndexTest, DuplicateMemberIsRejected) {
    GroupIndex index;
    EXPECT_NE(index.AddMember("g1", "k1"), nullptr);
    // Already registered -> nullptr (the caller reports already-exists).
    EXPECT_EQ(index.AddMember("g1", "k1"), nullptr);
    EXPECT_EQ(index.Members("g1").size(), 1u);
}

TEST(GroupIndexTest, EmptyGroupIsDroppedOnLastMemberRemoved) {
    GroupIndex index;
    auto before = index.AddMember("g1", "k1");
    ASSERT_NE(before, nullptr);

    EXPECT_TRUE(index.RemoveMember("g1", "k1"));
    EXPECT_TRUE(index.Members("g1").empty());

    // The dropped group is not revived: the re-add materializes a fresh group
    // with a new Lease.
    auto after = index.AddMember("g1", "k1");
    ASSERT_NE(after, nullptr);
    EXPECT_NE(before.get(), after.get());
}

TEST(GroupIndexTest, SharedLeaseWiresGroupAllOrNoneExpiry) {
    GroupIndex index;
    auto g1 = index.AddMember("g1", "k1");
    ASSERT_NE(g1, nullptr);
    ASSERT_NE(index.AddMember("g1", "k2"), nullptr);

    // Distinct groups get independent shared leases.
    auto g2 = index.AddMember("g2", "k1");
    ASSERT_NE(g2, nullptr);
    EXPECT_NE(g1.get(), g2.get());

    // All-or-none: every member of the group shares the one Lease, so a live
    // shared lease protects the whole group and one deadline expires it all.
    const auto now = std::chrono::system_clock::now();

    g1->GrantReadLease(std::chrono::milliseconds(10'000));
    EXPECT_FALSE(g1->IsExpired(now));

    g1->SetDeadline(now);
    EXPECT_TRUE(g1->IsExpired(now));
}

}  // namespace
}  // namespace mooncake
