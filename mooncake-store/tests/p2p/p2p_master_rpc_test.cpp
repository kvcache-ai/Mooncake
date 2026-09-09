#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "p2p/master/p2p_master_client.h"
#include "p2p/master/p2p_rpc_service.h"
#include "utils.h"

namespace mooncake::testing {
namespace {

// Exercise the concrete client/server boundary without the public Client
// facade or client data path in the link graph.
class P2PMasterRpcTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("p2p_master_rpc_test");
        FLAGS_logtostderr = true;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        P2PMasterConfig config;
        config.metrics.enable_reporting = false;
        config.metrics.http_port = 0;
        config.routes.max_clients_per_key = 0;
        config.client_lifecycle.live_ttl_seconds = 60;
        config.client_lifecycle.crashed_ttl_seconds = 180;
        service_ = std::make_unique<P2PMasterRpcService>(config, kViewVersion);

        const int port = getFreeTcpPort();
        ASSERT_GT(port, 0);
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            2, port, "127.0.0.1", std::chrono::seconds(0), true);
        RegisterP2PRpcService(*server_, *service_);
        auto started = server_->async_start();
        ASSERT_FALSE(started.hasResult()) << "P2P RPC server failed to start";

        client_id_ = generate_uuid();
        client_ = std::make_unique<P2PMasterClient>(client_id_);
        ASSERT_EQ(client_->Connect("127.0.0.1:" + std::to_string(port)),
                  ErrorCode::OK);
        first_ = MakeSegment("first");
        second_ = MakeSegment("second");
        auto registered =
            client_->RegisterClient({.client_id = client_id_,
                                     .segments = {first_, second_},
                                     .ip_address = "127.0.0.1",
                                     .rpc_port = kClientPort});
        ASSERT_TRUE(registered.has_value()) << registered.error();
        EXPECT_EQ(*registered, kViewVersion);
        ASSERT_TRUE(client_->CompleteRouteSync(client_id_).has_value());
    }

    void TearDown() override {
        client_.reset();
        if (server_) {
            server_->stop();
            server_.reset();
        }
        service_.reset();
    }

    static P2PSegment MakeSegment(std::string name) {
        P2PSegment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.size = kSegmentSize;
        segment.memory_type = MemoryType::DRAM;
        return segment;
    }

    auto Publish(std::string_view key, const UUID& segment_id,
                 uint64_t size = 1024) {
        return client_->PublishRoute({.key = key,
                                      .object_size = size,
                                      .client_id = client_id_,
                                      .segment_id = segment_id});
    }

    static constexpr ViewVersionId kViewVersion = 42;
    static constexpr uint16_t kClientPort = 50052;
    static constexpr size_t kSegmentSize = 8192;
    std::unique_ptr<P2PMasterRpcService> service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::unique_ptr<P2PMasterClient> client_;
    UUID client_id_;
    P2PSegment first_;
    P2PSegment second_;
};

TEST_F(P2PMasterRpcTest, MixedSyncPreservesItemErrorsAndReadOrder) {
    ASSERT_TRUE(Publish("existing", first_.id).has_value());
    {
        // Request string_views must not become stored route-key references.
        const std::string new_key = "new-key";
        P2PBatchSyncRoutesRequest request;
        request.client_id = client_id_;
        request.publish_operations = {
            {.key = new_key, .object_size = 2048, .segment_id = first_.id},
            {.key = "bad-segment",
             .object_size = 1024,
             .segment_id = generate_uuid()},
            {.key = "cycle", .object_size = 1024, .segment_id = second_.id}};
        request.withdraw_operations = {
            {.key = "existing", .segment_id = first_.id},
            {.key = "missing", .segment_id = first_.id},
            {.key = "cycle", .segment_id = second_.id}};
        auto synced = client_->BatchSyncRoutes(request);
        ASSERT_TRUE(synced.has_value()) << synced.error();
        EXPECT_EQ(
            synced->publish_results,
            (std::vector<ErrorCode>{ErrorCode::OK, ErrorCode::SEGMENT_NOT_FOUND,
                                    ErrorCode::OK}));
        EXPECT_EQ(synced->withdraw_results,
                  (std::vector<ErrorCode>{ErrorCode::OK, ErrorCode::OK,
                                          ErrorCode::OK}));
    }

    const auto routes = client_->BatchGetReadRoute(
        {"new-key", "existing", "bad-segment", "cycle", "new-key"}, {});
    ASSERT_EQ(routes.size(), 5u);
    for (size_t index : {0u, 4u}) {
        ASSERT_TRUE(routes[index].has_value()) << routes[index].error();
        ASSERT_EQ(routes[index]->size(), 1u);
        const auto& route = routes[index]->front();
        EXPECT_EQ(route.client_id, client_id_);
        EXPECT_EQ(route.segment_id, first_.id);
        EXPECT_EQ(route.ip_address, "127.0.0.1");
        EXPECT_EQ(route.rpc_port, kClientPort);
        EXPECT_EQ(route.object_size, 2048u);
    }
    for (size_t index : {1u, 2u, 3u}) {
        ASSERT_FALSE(routes[index].has_value());
        EXPECT_EQ(routes[index].error(), ErrorCode::OBJECT_NOT_FOUND);
    }

    auto regex = client_->GetReadRouteByRegex("^new-key$");
    ASSERT_TRUE(regex.has_value()) << regex.error();
    ASSERT_EQ(regex->size(), 1u);
    ASSERT_EQ(regex->count("new-key"), 1u);
    ASSERT_EQ(regex->at("new-key").size(), 1u);
    EXPECT_EQ(regex->at("new-key").front().object_size, 2048u);
    auto invalid_regex = client_->GetReadRouteByRegex("[");
    ASSERT_FALSE(invalid_regex.has_value());
    EXPECT_EQ(invalid_regex.error(), ErrorCode::INVALID_PARAMS);

    auto async_routes = async_simple::coro::syncAwait(
        client_->AsyncGetReadRoute("new-key", {}));
    ASSERT_TRUE(async_routes.has_value()) << async_routes.error();
    ASSERT_EQ(async_routes->size(), 1u);
    EXPECT_EQ(async_routes->front().object_size, 2048u);
}

TEST_F(P2PMasterRpcTest, LifecycleAndBatchWithdrawPreserveResultOrder) {
    ASSERT_TRUE(Publish("shared", first_.id).has_value());
    ASSERT_TRUE(Publish("shared", second_.id).has_value());
    auto withdrawn = client_->BatchWithdrawRoute(
        {.key = "shared",
         .client_id = client_id_,
         .segment_ids = {first_.id, generate_uuid(), second_.id}});
    ASSERT_EQ(withdrawn.size(), 3u);
    EXPECT_TRUE(withdrawn[0].has_value());
    ASSERT_FALSE(withdrawn[1].has_value());
    EXPECT_EQ(withdrawn[1].error(), ErrorCode::REPLICA_NOT_FOUND);
    EXPECT_TRUE(withdrawn[2].has_value());

    const auto extra = MakeSegment("extra");
    ASSERT_TRUE(client_->MountSegment(extra).has_value());
    ASSERT_TRUE(Publish("unmounted", extra.id).has_value());
    ASSERT_TRUE(Publish("unregistered", first_.id).has_value());
    ASSERT_TRUE(client_->UnmountSegment(extra.id).has_value());
    auto exists =
        client_->BatchExistKey({"shared", "unmounted", "unregistered"});
    ASSERT_EQ(exists.size(), 3u);
    for (const auto& result : exists) {
        ASSERT_TRUE(result.has_value()) << result.error();
    }
    EXPECT_FALSE(*exists[0]);
    EXPECT_FALSE(*exists[1]);
    EXPECT_TRUE(*exists[2]);

    auto heartbeat = client_->Heartbeat({.client_id = client_id_});
    ASSERT_TRUE(heartbeat.has_value()) << heartbeat.error();
    EXPECT_EQ(heartbeat->view_version, kViewVersion);
    EXPECT_EQ(heartbeat->status, P2PClientStatus::HEALTH);
    auto status = client_->QueryClientStatus(client_id_);
    ASSERT_TRUE(status.has_value()) << status.error();
    EXPECT_EQ(*status, P2PClientStatus::HEALTH);
    auto unregistered = client_->UnregisterClient(client_id_);
    ASSERT_TRUE(unregistered.has_value()) << unregistered.error();
    EXPECT_EQ(*unregistered, kViewVersion);
    auto missing = client_->GetReadRoute("unregistered", {});
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
    status = client_->QueryClientStatus(client_id_);
    ASSERT_TRUE(status.has_value()) << status.error();
    EXPECT_EQ(*status, P2PClientStatus::UNDEFINED);
}

TEST_F(P2PMasterRpcTest, BatchWriteReturnsAlignedCandidateAndValidationErrors) {
    P2PBatchGetWriteRouteRequest request;
    request.client_id = client_id_;
    request.keys = {"fits", "too-large"};
    request.object_sizes = {1024, 4 * kSegmentSize};
    request.config.remote_weight = 0.0;
    auto candidates = client_->BatchGetWriteRoute(request);
    ASSERT_TRUE(candidates.has_value()) << candidates.error();
    ASSERT_EQ(candidates->responses.size(), 2u);
    EXPECT_EQ(candidates->error_codes,
              (std::vector<ErrorCode>{ErrorCode::OK,
                                      ErrorCode::NO_AVAILABLE_CANDIDATE}));
    ASSERT_EQ(candidates->responses[0].size(), 1u);
    EXPECT_EQ(candidates->responses[0][0].client_id, client_id_);
    EXPECT_TRUE(candidates->responses[1].empty());

    request.object_sizes.pop_back();
    auto invalid = client_->BatchGetWriteRoute(request);
    ASSERT_TRUE(invalid.has_value()) << invalid.error();
    ASSERT_EQ(invalid->responses.size(), request.keys.size());
    EXPECT_EQ(invalid->error_codes,
              (std::vector<ErrorCode>{ErrorCode::INVALID_PARAMS,
                                      ErrorCode::INVALID_PARAMS}));
    for (const auto& response : invalid->responses) {
        EXPECT_TRUE(response.empty());
    }
}

}  // namespace
}  // namespace mooncake::testing
