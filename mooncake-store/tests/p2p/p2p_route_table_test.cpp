#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "p2p/master/p2p_route_table.h"

namespace mooncake {
namespace {

const UUID kClientA{1, 1};
const UUID kClientB{2, 2};
const UUID kSharedSegment{9, 9};

P2PRouteLocation Location(const UUID& client, const UUID& segment) {
    return P2PRouteLocation{.client_id = client, .segment_id = segment};
}

TEST(P2PRouteTableTest, PublishAppendAndWithdrawLastRoute) {
    P2PRouteTable table;
    const auto location_a = Location(kClientA, UUID{11, 11});
    const auto location_b = Location(kClientB, UUID{12, 12});

    auto first = table.Publish("key", 1024, location_a);
    ASSERT_TRUE(first.has_value());
    EXPECT_TRUE(first->created_key);
    EXPECT_EQ(table.GetRouteKeyCount(), 1);

    auto second = table.Publish("key", 1024, location_b);
    ASSERT_TRUE(second.has_value());
    EXPECT_FALSE(second->created_key);
    auto route = table.GetRoute("key");
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->object_size, 1024);
    EXPECT_EQ(route->locations.size(), 2);

    auto remove_first = table.Withdraw("key", location_a);
    ASSERT_TRUE(remove_first.has_value());
    EXPECT_FALSE(remove_first->removed_key);
    auto remove_last = table.Withdraw("key", location_b);
    ASSERT_TRUE(remove_last.has_value());
    EXPECT_TRUE(remove_last->removed_key);
    EXPECT_FALSE(table.RouteExists("key"));
    EXPECT_EQ(table.GetRouteKeyCount(), 0);
}

TEST(P2PRouteTableTest, RejectsInvalidSizeAndDuplicateLocation) {
    P2PRouteTable table;
    const auto location = Location(kClientA, UUID{11, 11});

    auto zero_size = table.Publish("key", 0, location);
    ASSERT_FALSE(zero_size.has_value());
    EXPECT_EQ(zero_size.error(), ErrorCode::INVALID_PARAMS);

    ASSERT_TRUE(table.Publish("key", 1024, location).has_value());
    auto duplicate = table.Publish("key", 1024, location);
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::REPLICA_ALREADY_EXISTS);

    auto size_mismatch =
        table.Publish("key", 2048, Location(kClientB, UUID{12, 12}));
    ASSERT_FALSE(size_mismatch.has_value());
    EXPECT_EQ(size_mismatch.error(), ErrorCode::INVALID_PARAMS);
}

TEST(P2PRouteTableTest, CountsUniqueClientsForRouteLimit) {
    P2PRouteTable table;
    ASSERT_TRUE(
        table.Publish("key", 1024, Location(kClientA, UUID{11, 11}),
                      /*max_client_per_key=*/1)
            .has_value());
    EXPECT_TRUE(
        table.Publish("key", 1024, Location(kClientA, UUID{12, 12}),
                      /*max_client_per_key=*/1)
            .has_value());

    auto second_client = table.Publish(
        "key", 1024, Location(kClientB, UUID{13, 13}),
        /*max_client_per_key=*/1);
    ASSERT_FALSE(second_client.has_value());
    EXPECT_EQ(second_client.error(), ErrorCode::REPLICA_NUM_EXCEEDED);
}

TEST(P2PRouteTableTest, WithdrawPreconditionFailureKeepsRoute) {
    P2PRouteTable table;
    const UUID segment_id{11, 11};
    const auto location = Location(kClientA, segment_id);
    ASSERT_TRUE(table.Publish("key", 1024, location).has_value());

    auto result = table.Withdraw(
        "key", location, [] { return ErrorCode::INTERNAL_ERROR; });
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(table.RouteExists("key"));
}

TEST(P2PRouteTableTest, WithdrawSkipsPreconditionForMissingTarget) {
    P2PRouteTable table;
    const auto location = Location(kClientA, UUID{11, 11});
    const auto other_location = Location(kClientA, UUID{12, 12});
    ASSERT_TRUE(table.Publish("key", 1024, location).has_value());

    bool precondition_called = false;
    const auto precondition = [&] {
        precondition_called = true;
        return ErrorCode::OK;
    };
    auto missing_key = table.Withdraw("missing", location, precondition);
    ASSERT_FALSE(missing_key.has_value());
    EXPECT_EQ(missing_key.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_FALSE(precondition_called);

    auto missing_location =
        table.Withdraw("key", other_location, precondition);
    ASSERT_FALSE(missing_location.has_value());
    EXPECT_EQ(missing_location.error(), ErrorCode::REPLICA_NOT_FOUND);
    EXPECT_FALSE(precondition_called);
    EXPECT_TRUE(table.RouteExists("key"));
}

TEST(P2PRouteTableTest, CleanupUsesClientAndSegmentIdentity) {
    P2PRouteTable table;
    const auto location_a = Location(kClientA, kSharedSegment);
    const auto location_b = Location(kClientB, kSharedSegment);
    ASSERT_TRUE(table.Publish("shared", 1024, location_a).has_value());
    ASSERT_TRUE(table.Publish("shared", 1024, location_b).has_value());
    ASSERT_TRUE(table.Publish("only-a", 1024, location_a).has_value());

    auto cleanup = table.RemoveLocation(location_a);
    EXPECT_EQ(cleanup.removed_routes, 2);
    EXPECT_EQ(cleanup.removed_key_count, 1);
    EXPECT_FALSE(table.RouteExists("only-a"));

    auto shared = table.GetRoute("shared");
    ASSERT_TRUE(shared.has_value());
    ASSERT_EQ(shared->locations.size(), 1);
    EXPECT_EQ(shared->locations.front(), location_b);
}

TEST(P2PRouteTableTest, RepeatedCleanupLeavesNoDanglingReverseKeys) {
    P2PRouteTable table;
    const auto location = Location(kClientA, UUID{11, 11});

    for (size_t i = 0; i < 2000; ++i) {
        std::string key = "route-" + std::to_string(i);
        ASSERT_TRUE(table.Publish(key, i + 1, location).has_value());
    }
    EXPECT_EQ(table.GetRouteKeyCount(), 2000);

    auto cleanup = table.RemoveLocation(location);
    EXPECT_EQ(cleanup.removed_routes, 2000);
    EXPECT_EQ(cleanup.removed_key_count, 2000);
    EXPECT_EQ(table.GetRouteKeyCount(), 0);

    auto second_cleanup = table.RemoveLocation(location);
    EXPECT_EQ(second_cleanup.removed_routes, 0);
    EXPECT_EQ(second_cleanup.removed_key_count, 0);
}

}  // namespace
}  // namespace mooncake
