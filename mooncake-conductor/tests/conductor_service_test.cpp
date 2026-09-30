#include "conductor/kvevent/conductor_service.h"

#include <gtest/gtest.h>

#include "conductor/kvevent/event_manager.h"
#include "event_manager_test_peer.h"

namespace mooncake::conductor::kvevent {
namespace {

TEST(ConductorServiceTest, QueryOnEmptyIndexReturnsEmpty) {
    EventManager manager({}, /*http_server_port=*/0);
    QueryRequest req;
    req.context.model_name = "m";
    req.context.block_size = 16;
    req.token_ids = {1, 2, 3};
    auto result = manager.GetService().Query(req);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->instances.empty());
}

TEST(ConductorServiceTest, ListServicesReflectsActiveConfigs) {
    EventManager manager({}, /*http_server_port=*/0);
    EXPECT_TRUE(manager.GetService().ListServices().empty());

    common::ServiceConfig svc;
    svc.instance_id = "inst-1";
    svc.tenant_id = "default";
    EventManagerTestPeer::FakeActiveService(
        manager, MakeServiceKey("inst-1", "default", 0), svc);
    auto services = manager.GetService().ListServices();
    ASSERT_EQ(services.size(), 1u);
    EXPECT_EQ(services[0].instance_id, "inst-1");
}

TEST(ConductorServiceTest, GlobalViewInitiallyEmpty) {
    EventManager manager({}, /*http_server_port=*/0);
    EXPECT_EQ(manager.GetService().GetGlobalView().context_count, 0);
}

TEST(ConductorServiceTest, RegisterRejectsInvalidConfig) {
    EventManager manager({}, /*http_server_port=*/0);
    common::ServiceConfig svc;  // missing instance_id/endpoint/block_size etc.
    auto result = manager.GetService().Register(svc);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST(ConductorServiceTest, RegisterNormalizesEmptyTenantId) {
    EventManager manager({}, /*http_server_port=*/0);
    // The RPC channel reaches the service layer directly, bypassing the HTTP
    // parsing layer, so Register must normalize an empty tenant_id to
    // "default" (matching the HTTP and static-config channels). The endpoint
    // just needs to point at a free port: ZMQ connect is lazy.
    common::ServiceConfig svc;
    svc.endpoint = "tcp://127.0.0.1:29987";
    svc.publisher_kind = common::PublisherKind::kVllm;
    svc.model_name = "m";
    svc.instance_id = "inst-no-tenant";
    svc.tenant_id = "";
    svc.dp_rank = 0;
    svc.block_size = 16;
    svc.hash_profile.strategy = "vllm_v1";
    svc.hash_profile.algorithm = "sha256_cbor";
    svc.hash_profile.python_hash_seed = "0";
    svc.hash_profile.index_projection = "low64_be";

    auto result = manager.GetService().Register(svc);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->instance_id, "inst-no-tenant");
    EXPECT_TRUE(result->is_new);

    const auto services = manager.GetService().ListServices();
    ASSERT_EQ(services.size(), 1u);
    EXPECT_EQ(services[0].instance_id, "inst-no-tenant");
    EXPECT_EQ(services[0].tenant_id, "default");
    manager.Stop();
}

TEST(ConductorServiceTest, UnregisterMissingServiceReturnsNotFound) {
    EventManager manager({}, /*http_server_port=*/0);
    auto result =
        manager.GetService().Unregister("no-such-instance", "default", 0);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::SERVICE_NOT_FOUND);
}

TEST(ConductorServiceTest, RpcServerStartsAndReportsPort) {
    EventManager manager({}, /*http_server_port=*/0, /*rpc_server_port=*/0);
    // rpc_port=0 disables RPC, so StartRPCServer is a no-op.
    EXPECT_TRUE(manager.StartRPCServer());
    EXPECT_EQ(manager.RpcPort(), 0);
    manager.Stop();
}

TEST(ConductorServiceTest, UnregisterRemovesFakedService) {
    EventManager manager({}, /*http_server_port=*/0);
    // UnsubscribeFromService runs indexer cleanup with the stored config, so
    // the faked service must be field-valid (same as the /unregister HTTP
    // cases in event_manager_test.cpp).
    common::ServiceConfig svc;
    svc.endpoint = "tcp://127.0.0.1:20001";
    svc.model_name = "m";
    svc.instance_id = "inst-1";
    svc.tenant_id = "default";
    svc.dp_rank = 0;
    svc.block_size = 16;
    const std::string key = MakeServiceKey("inst-1", "default", 0);
    EventManagerTestPeer::FakeSubscriber(manager, key);
    EventManagerTestPeer::FakeActiveService(manager, key, svc);

    auto result = manager.GetService().Unregister("inst-1", "default", 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->removed_key, key);
    EXPECT_TRUE(manager.GetService().ListServices().empty());
}

}  // namespace
}  // namespace mooncake::conductor::kvevent
