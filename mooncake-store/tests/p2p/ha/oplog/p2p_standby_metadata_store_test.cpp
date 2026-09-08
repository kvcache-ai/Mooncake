#include <gtest/gtest.h>

#include "p2p/ha/oplog/p2p_standby_metadata_store.h"

namespace mooncake {
namespace {

P2PRouteLocation Location(UUID client, UUID segment) {
    return P2PRouteLocation{.client_id = client, .segment_id = segment};
}

P2PSegment Segment(UUID id, std::string name = "segment") {
    return P2PSegment{.id = id,
                      .name = std::move(name),
                      .size = 4096,
                      .priority = 1,
                      .tags = {},
                      .memory_type = MemoryType::DRAM,
                      .usage = 0};
}

TEST(P2PStandbyMetadataStoreTest, PublishesAndWithdrawsRouteLocations) {
    P2PStandbyMetadataStore store;
    const auto first = Location({1, 1}, {10, 10});
    const auto second = Location({2, 2}, {20, 20});
    ASSERT_TRUE(store.PublishRoute("key", first, 1024, 1));
    ASSERT_TRUE(store.PublishRoute("key", second, 1024, 2));

    auto route = store.GetRoute("key");
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->object_size, 1024);
    EXPECT_EQ(route->last_sequence_id, 2);
    EXPECT_EQ(route->locations.size(), 2);

    store.WithdrawRoute("key", first);
    ASSERT_EQ(store.GetRoute("key")->locations.size(), 1);
    store.WithdrawRoute("key", second);
    EXPECT_FALSE(store.RouteExists("key"));
}

TEST(P2PStandbyMetadataStoreTest, RejectsRouteSizeMismatch) {
    P2PStandbyMetadataStore store;
    ASSERT_TRUE(store.PublishRoute("key", Location({1, 1}, {10, 10}), 1024, 1));
    EXPECT_FALSE(
        store.PublishRoute("key", Location({2, 2}, {20, 20}), 2048, 2));
    ASSERT_EQ(store.GetRoute("key")->locations.size(), 1);
}

TEST(P2PStandbyMetadataStoreTest, RejectsZeroSizeWithoutCreatingRoute) {
    P2PStandbyMetadataStore store;
    EXPECT_FALSE(store.PublishRoute("key", Location({1, 1}, {10, 10}), 0, 1));
    EXPECT_FALSE(store.RouteExists("key"));
    EXPECT_EQ(store.GetRouteKeyCount(), 0);
}

TEST(P2PStandbyMetadataStoreTest, DuplicatePublishIsIdempotent) {
    P2PStandbyMetadataStore store;
    const auto location = Location({1, 1}, {10, 10});
    ASSERT_TRUE(store.PublishRoute("key", location, 1024, 1));
    ASSERT_TRUE(store.PublishRoute("key", location, 1024, 2));
    auto route = store.GetRoute("key");
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->locations.size(), 1);
    EXPECT_EQ(route->last_sequence_id, 2);
}

TEST(P2PStandbyMetadataStoreTest, UnmountUsesFullLocationIdentity) {
    P2PStandbyMetadataStore store;
    const UUID shared_segment{10, 10};
    const auto first = Location({1, 1}, shared_segment);
    const auto second = Location({2, 2}, shared_segment);
    store.RegisterClient(first.client_id, "10.0.0.1", 5001,
                         {Segment(shared_segment, "first")});
    store.RegisterClient(second.client_id, "10.0.0.2", 5002,
                         {Segment(shared_segment, "second")});
    ASSERT_TRUE(store.PublishRoute("key", first, 1024, 1));
    ASSERT_TRUE(store.PublishRoute("key", second, 1024, 2));

    store.UnmountSegment(first);
    auto route = store.GetRoute("key");
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route->locations.size(), 1);
    EXPECT_EQ(route->locations.front(), second);
    EXPECT_TRUE(store.GetClientInfo(first.client_id)->segments.empty());
    EXPECT_EQ(store.GetClientInfo(second.client_id)->segments.size(), 1);
}

TEST(P2PStandbyMetadataStoreTest, UnregisterCascadesOnlyOwnedRoutes) {
    P2PStandbyMetadataStore store;
    const auto first = Location({1, 1}, {10, 10});
    const auto second = Location({2, 2}, {20, 20});
    store.RegisterClient(first.client_id, "10.0.0.1", 5001, {});
    store.RegisterClient(second.client_id, "10.0.0.2", 5002, {});
    ASSERT_TRUE(store.PublishRoute("shared", first, 1024, 1));
    ASSERT_TRUE(store.PublishRoute("shared", second, 1024, 2));
    ASSERT_TRUE(store.PublishRoute("first-only", first, 512, 3));

    store.UnregisterClient(first.client_id);
    EXPECT_FALSE(store.GetClientInfo(first.client_id).has_value());
    EXPECT_FALSE(store.RouteExists("first-only"));
    ASSERT_TRUE(store.RouteExists("shared"));
    EXPECT_EQ(store.GetRoute("shared")->locations.front(), second);
}

TEST(P2PStandbyMetadataStoreTest, SnapshotExportAndRestoreUseRouteSchema) {
    P2PStandbyMetadataStore source;
    const auto location = Location({1, 1}, {10, 10});
    source.RegisterClient(location.client_id, "10.0.0.1", 5001,
                          {Segment(location.segment_id)});
    ASSERT_TRUE(source.PublishRoute("key", location, 1024, 7));

    auto exported = source.ExportMetadata();
    ASSERT_EQ(exported.routes.size(), 1);
    ASSERT_EQ(exported.clients.size(), 1);

    P2PStandbyMetadataStore restored;
    for (const auto& [id, client] : exported.clients) {
        restored.RegisterClient(id, client.ip_address, client.rpc_port,
                                client.segments);
    }
    for (const auto& [key, route] : exported.routes) {
        restored.RestoreRoute(key, route);
    }
    EXPECT_EQ(restored.GetRoutes(), exported.routes);
}

TEST(P2PStandbyMetadataStoreTest, RegistrationPreservesEarlyMountedSegments) {
    P2PStandbyMetadataStore store;
    const UUID client{1, 1};
    const auto early = Segment({10, 10}, "early");
    const auto later = Segment({20, 20}, "later");
    store.MountSegment(client, early);
    store.RegisterClient(client, "10.0.0.1", 5001, {early, later});
    store.RegisterClient(client, "10.0.0.2", 5002, {early});
    store.MountSegment(client, later);

    auto info = store.GetClientInfo(client);
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->ip_address, "10.0.0.2");
    EXPECT_EQ(info->rpc_port, 5002);
    ASSERT_EQ(info->segments.size(), 2);
    EXPECT_EQ(info->segments[0].id, early.id);
    EXPECT_EQ(info->segments[1].id, later.id);
}

TEST(P2PStandbyMetadataStoreTest, MissingClientStillCascadesOwnedLocations) {
    P2PStandbyMetadataStore store;
    const auto first = Location({1, 1}, {10, 10});
    const auto second = Location({2, 2}, {20, 20});
    ASSERT_TRUE(store.PublishRoute("key", first, 1024, 1));
    ASSERT_TRUE(store.PublishRoute("key", second, 1024, 2));

    store.UnmountSegment(first);
    auto route = store.GetRoute("key");
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route->locations.size(), 1);
    EXPECT_EQ(route->locations.front(), second);
    store.UnregisterClient(second.client_id);
    store.UnregisterClient(second.client_id);
    store.WithdrawRoute("missing", first);
    EXPECT_TRUE(store.GetRoutes().empty());
    EXPECT_TRUE(store.GetClients().empty());
}

TEST(P2PStandbyMetadataStoreTest, ClearRemovesClientsAndRoutes) {
    P2PStandbyMetadataStore store;
    const auto location = Location({1, 1}, {10, 10});
    store.RegisterClient(location.client_id, "10.0.0.1", 5001,
                         {Segment(location.segment_id)});
    ASSERT_TRUE(store.PublishRoute("key", location, 1024, 1));
    store.RemoveAllMetadata();
    EXPECT_TRUE(store.ListRouteKeys().empty());
    EXPECT_TRUE(store.ListClientIds().empty());
    EXPECT_FALSE(store.GetClientInfo(location.client_id).has_value());
}

}  // namespace
}  // namespace mooncake
