#include <gtest/gtest.h>

#define protected public
#include "p2p_client_service.h"
#undef protected

#ifdef STORE_USE_REDIS
#include "redis_helper.h"
#endif

namespace mooncake {
namespace testing {

namespace {

P2PClientService MakeClient() {
    return P2PClientService("P2PHANDSHAKE", /*http_port=*/0,
                            /*enable_http_server=*/false);
}

class FailingMasterViewHelper : public MasterViewHelper {
   public:
    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version) override {
        (void)master_address;
        (void)version;
        return ErrorCode::INTERNAL_ERROR;
    }
};

#ifdef STORE_USE_REDIS
std::string RedisMasterViewKey(const std::string& cluster_id) {
    std::string normalized_cluster_id = cluster_id;
    if (!normalized_cluster_id.empty() && normalized_cluster_id.back() != '/') {
        normalized_cluster_id += '/';
    }
    return "mooncake:{" + normalized_cluster_id + "}master_view";
}

void WriteRedisMasterViewOrSkip(const std::string& cluster_id,
                                const std::string& master_address,
                                ViewVersionId version) {
    redisContext* ctx = redisConnect("127.0.0.1", 6379);
    if (!ctx || ctx->err) {
        if (ctx) redisFree(ctx);
        GTEST_SKIP() << "Redis is not available on 127.0.0.1:6379";
    }

    std::string key = RedisMasterViewKey(cluster_id);
    std::string value =
        RedisHelper::SerializeLeaderValue(master_address, version, 30);
    RedisReplyPtr reply(static_cast<redisReply*>(
        redisCommand(ctx, "SET %b %b EX 30", key.data(), key.size(),
                     value.data(), value.size())));
    redisFree(ctx);

    ASSERT_TRUE(reply);
    ASSERT_EQ(reply->type, REDIS_REPLY_STATUS);
}

void DeleteRedisMasterView(const std::string& cluster_id) {
    redisContext* ctx = redisConnect("127.0.0.1", 6379);
    if (!ctx || ctx->err) {
        if (ctx) redisFree(ctx);
        return;
    }
    std::string key = RedisMasterViewKey(cluster_id);
    RedisReplyPtr reply(static_cast<redisReply*>(
        redisCommand(ctx, "DEL %b", key.data(), key.size())));
    redisFree(ctx);
}
#endif

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

#ifdef STORE_USE_REDIS
TEST(P2PClientHATest, RecreatesMasterViewHelperAfterResolveFailure) {
    auto client = MakeClient();
    P2PClientConfig config;
    config.redis_cluster_id = "p2p-client-ha-reconnect-test";
    client.SetMasterDiscoveryConfig(config);

    const std::string master_server_entry = "redis://127.0.0.1:6379";
    const std::string expected_master_address = "127.0.0.1:51051";
    DeleteRedisMasterView(config.redis_cluster_id);

    client.master_view_helper_ = std::make_unique<FailingMasterViewHelper>();
    client.master_view_helper_entry_ = master_server_entry;

    std::string master_address;
    auto err = client.ResolveMasterAddress(master_server_entry, master_address);

    EXPECT_EQ(err, ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(master_address.empty());
    EXPECT_EQ(client.master_view_helper_, nullptr);
    EXPECT_TRUE(client.master_view_helper_entry_.empty());

    WriteRedisMasterViewOrSkip(config.redis_cluster_id, expected_master_address,
                               7);
    err = client.ResolveMasterAddress(master_server_entry, master_address);

    EXPECT_EQ(err, ErrorCode::OK);
    EXPECT_EQ(master_address, expected_master_address);
    EXPECT_NE(client.master_view_helper_, nullptr);
    EXPECT_EQ(client.master_view_helper_entry_, master_server_entry);

    DeleteRedisMasterView(config.redis_cluster_id);
}
#endif

}  // namespace testing
}  // namespace mooncake
