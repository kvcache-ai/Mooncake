#include <gtest/gtest.h>

#define protected public
#include "p2p_client_service.h"
#undef protected

namespace mooncake {
namespace testing {

namespace {

P2PClientService MakeClient() {
    return P2PClientService("P2PHANDSHAKE", /*http_port=*/0,
                            /*enable_http_server=*/false);
}

}  // namespace

TEST(P2PClientHATest, RecognizesRedisMasterEntryAsHAMode) {
    auto client = MakeClient();

    EXPECT_FALSE(client.IsHAMode("127.0.0.1:50051"));
    EXPECT_TRUE(client.IsHAMode("etcd://127.0.0.1:2379"));
    EXPECT_TRUE(client.IsHAMode("redis://127.0.0.1:6379"));
}

TEST(P2PClientHATest, UsesDefaultClusterIdForRedisDiscovery) {
    P2PClientConfig config;
    EXPECT_EQ(config.redis_cluster_id, DEFAULT_CLUSTER_ID);

    auto client = MakeClient();
    client.SetMasterDiscoveryConfig(config);

    EXPECT_EQ(client.master_discovery_config_.redis_cluster_id,
              DEFAULT_CLUSTER_ID);
}

TEST(P2PClientHATest, EmptyClusterIdFallsBackToDefaultClusterId) {
    auto client = MakeClient();
    P2PClientConfig config;
    config.redis_cluster_id.clear();

    client.SetMasterDiscoveryConfig(config);

    EXPECT_EQ(client.master_discovery_config_.redis_cluster_id,
              DEFAULT_CLUSTER_ID);
}

TEST(P2PClientHATest, StoresRedisDiscoveryConfig) {
    auto client = MakeClient();
    P2PClientConfig config;
    config.redis_cluster_id = "test-cluster";
    config.redis_password = "test-password";
    config.redis_db_index = 3;
    config.redis_master_view_ttl_sec = 9;
    config.redis_heartbeat_interval_sec = 4;

    client.SetMasterDiscoveryConfig(config);

    EXPECT_EQ(client.master_discovery_config_.redis_cluster_id,
              config.redis_cluster_id);
    EXPECT_EQ(client.master_discovery_config_.redis_password,
              config.redis_password);
    EXPECT_EQ(client.master_discovery_config_.redis_db_index,
              config.redis_db_index);
    EXPECT_EQ(client.master_discovery_config_.redis_master_view_ttl_sec,
              config.redis_master_view_ttl_sec);
    EXPECT_EQ(client.master_discovery_config_.redis_heartbeat_interval_sec,
              config.redis_heartbeat_interval_sec);
}

TEST(P2PClientHATest, InvalidRedisDiscoveryConfigReturnsError) {
    auto client = MakeClient();
    P2PClientConfig config;
    std::string master_address;

    config.redis_db_index = -1;
    client.SetMasterDiscoveryConfig(config);
    EXPECT_EQ(
        client.ResolveMasterAddress("redis://127.0.0.1:6379", master_address),
        ErrorCode::INVALID_PARAMS);

    config.redis_db_index = 0;
    config.redis_master_view_ttl_sec = 0;
    client.SetMasterDiscoveryConfig(config);
    EXPECT_EQ(
        client.ResolveMasterAddress("redis://127.0.0.1:6379", master_address),
        ErrorCode::INVALID_PARAMS);

    config.redis_master_view_ttl_sec = 5;
    config.redis_heartbeat_interval_sec = 5;
    client.SetMasterDiscoveryConfig(config);
    EXPECT_EQ(
        client.ResolveMasterAddress("redis://127.0.0.1:6379", master_address),
        ErrorCode::INVALID_PARAMS);
}

TEST(P2PClientHATest, RedisDiscoveryInvalidEndpointReturnsError) {
    auto client = MakeClient();
    std::string master_address;

    auto err = client.ResolveMasterAddress("redis://127.0.0.1:not-a-port",
                                           master_address);

#ifdef STORE_USE_REDIS
    EXPECT_EQ(err, ErrorCode::INTERNAL_ERROR);
#else
    EXPECT_EQ(err, ErrorCode::INVALID_PARAMS);
#endif
    EXPECT_TRUE(master_address.empty());
}

}  // namespace testing
}  // namespace mooncake
