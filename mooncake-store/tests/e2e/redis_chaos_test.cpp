#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client_wrapper.h"
#include "e2e_utils.h"
#include "process_handler.h"
#include "redis_master_view_helper.h"
#include "types.h"

#include <hiredis/hiredis.h>

FLAG_master_path;
FLAG_out_dir;
DEFINE_string(redis_endpoint, "127.0.0.1:6379",
              "Redis endpoint for Redis master failover test");
DEFINE_string(redis_cluster_id, "redis_chaos_test",
              "Redis cluster ID for Redis master failover test");
DEFINE_int32(redis_master_view_ttl_sec, 5,
             "Redis master view TTL for Redis master failover test");
DEFINE_int32(redis_heartbeat_interval_sec, 2,
             "Redis heartbeat interval for Redis master failover test");

constexpr int kMasterPortBase = 51051;
constexpr int kMasterNum = 3;
constexpr int kClientPortBase = 52051;

namespace mooncake {
namespace testing {

namespace {

std::pair<std::string, int> ParseRedisEndpoint() {
    std::string host = "127.0.0.1";
    int port = 6379;
    auto colon_pos = FLAGS_redis_endpoint.rfind(':');
    if (colon_pos != std::string::npos) {
        host = FLAGS_redis_endpoint.substr(0, colon_pos);
        port = std::stoi(FLAGS_redis_endpoint.substr(colon_pos + 1));
    }
    return {host, port};
}

std::string RedisClusterIdWithSlash() {
    std::string cluster_id = FLAGS_redis_cluster_id;
    if (!cluster_id.empty() && cluster_id.back() != '/') {
        cluster_id += '/';
    }
    return cluster_id;
}

std::string MasterViewKey() {
    return "mooncake:{" + RedisClusterIdWithSlash() + "}master_view";
}

std::string MasterEpochKey() {
    return "mooncake:{" + RedisClusterIdWithSlash() + "}master_epoch";
}

bool DeleteRedisKey(const std::string& key) {
    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    if (!ctx || ctx->err) {
        if (ctx) redisFree(ctx);
        return false;
    }

    redisReply* reply = static_cast<redisReply*>(
        redisCommand(ctx, "DEL %b", key.data(), key.size()));
    bool deleted =
        reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
    if (reply) freeReplyObject(reply);
    redisFree(ctx);
    return deleted;
}

void CleanupRedisKeys() {
    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    if (!ctx || ctx->err) {
        if (ctx) redisFree(ctx);
        return;
    }

    std::string master_view_key = MasterViewKey();
    std::string master_epoch_key = MasterEpochKey();
    redisReply* reply = static_cast<redisReply*>(redisCommand(
        ctx, "DEL %b %b", master_view_key.data(), master_view_key.size(),
        master_epoch_key.data(), master_epoch_key.size()));
    if (reply) freeReplyObject(reply);
    redisFree(ctx);
}

int MasterIndexFromAddress(const std::string& master_address) {
    auto colon_pos = master_address.rfind(':');
    if (colon_pos == std::string::npos) {
        return -1;
    }
    int port = std::stoi(master_address.substr(colon_pos + 1));
    return port - kMasterPortBase;
}

}  // namespace

class RedisChaosTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("RedisChaosTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        CleanupRedisKeys();
        master_view_helper_ = std::make_unique<RedisMasterViewHelper>(
            FLAGS_redis_cluster_id, FLAGS_redis_endpoint, "",
            /*db_index=*/0, FLAGS_redis_master_view_ttl_sec,
            FLAGS_redis_heartbeat_interval_sec);
        ASSERT_EQ(master_view_helper_->Connect(), ErrorCode::OK);

        MasterRunnerConfig config;
        config.election_backend = "redis";
        config.redis_endpoint = FLAGS_redis_endpoint;
        config.redis_master_view_ttl_sec = FLAGS_redis_master_view_ttl_sec;
        config.redis_heartbeat_interval_sec =
            FLAGS_redis_heartbeat_interval_sec;
        config.cluster_id = FLAGS_redis_cluster_id;
        config.rpc_address = "127.0.0.1";

        for (int i = 0; i < kMasterNum; ++i) {
            masters_.emplace_back(std::make_unique<MasterProcessHandler>(
                FLAGS_master_path, config, kMasterPortBase + i, i,
                FLAGS_out_dir));
            ASSERT_TRUE(masters_.back()->start());
        }
    }

    void TearDown() override {
        masters_.clear();
        master_view_helper_.reset();
        CleanupRedisKeys();
    }

    bool WaitForLeader(int& leader_index, ViewVersionId& version,
                       std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::string master_address;
            ViewVersionId next_version = 0;
            auto err = master_view_helper_->GetMasterView(master_address,
                                                          next_version);
            if (err == ErrorCode::OK) {
                int index = MasterIndexFromAddress(master_address);
                if (index >= 0 && index < kMasterNum) {
                    leader_index = index;
                    version = next_version;
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    bool WaitForNewLeader(int old_leader_index, ViewVersionId old_version,
                          int& new_leader_index, ViewVersionId& new_version) {
        auto timeout =
            std::chrono::seconds(FLAGS_redis_master_view_ttl_sec * 8);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (WaitForLeader(new_leader_index, new_version,
                              std::chrono::seconds(1)) &&
                new_leader_index != old_leader_index &&
                new_version > old_version) {
                return true;
            }
        }
        return false;
    }

    bool WaitForNewerView(ViewVersionId old_version, int& leader_index,
                          ViewVersionId& version) {
        auto timeout =
            std::chrono::seconds(FLAGS_redis_master_view_ttl_sec * 8);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (WaitForLeader(leader_index, version, std::chrono::seconds(1)) &&
                version > old_version) {
                return true;
            }
        }
        return false;
    }

    void WaitForLeaderServiceReady() {
        std::this_thread::sleep_for(
            std::chrono::seconds(FLAGS_redis_master_view_ttl_sec + 1));
    }

    std::shared_ptr<ClientTestWrapper> CreateRedisClient(
        int index, std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            auto client = ClientTestWrapper::CreateClientWrapper(
                "127.0.0.1:" + std::to_string(kClientPortBase + index),
                "P2PHANDSHAKE", "tcp", "", "redis://" + FLAGS_redis_endpoint,
                /*local_buffer_size=*/1024 * 1024 * 128, FLAGS_redis_cluster_id,
                /*enable_http_server=*/false);
            if (client.has_value()) {
                return client.value();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return nullptr;
    }

    bool WaitForClientPutGet(ClientTestWrapper& client,
                             const std::string& key_prefix,
                             const std::string& value,
                             std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        int attempt = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            std::string key = key_prefix + "_" + std::to_string(attempt++);
            if (client.Put(key, value) == ErrorCode::OK) {
                std::string got;
                if (client.Get(key, got) == ErrorCode::OK && got == value) {
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return false;
    }

    std::vector<std::unique_ptr<MasterProcessHandler>> masters_;
    std::unique_ptr<RedisMasterViewHelper> master_view_helper_;
};

TEST_F(RedisChaosTest, LeaderKilledFailover) {
    int leader_index = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader_index, version, std::chrono::seconds(10)));
    ASSERT_GE(leader_index, 0);
    ASSERT_LT(leader_index, kMasterNum);

    ASSERT_TRUE(masters_[leader_index]->kill());

    int new_leader_index = -1;
    ViewVersionId new_version = 0;
    ASSERT_TRUE(
        WaitForNewLeader(leader_index, version, new_leader_index, new_version));
    EXPECT_NE(new_leader_index, leader_index);
    EXPECT_GT(new_version, version);
}

TEST_F(RedisChaosTest, LeaderKeyDeletedTriggersReElection) {
    int leader_index = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader_index, version, std::chrono::seconds(10)));
    ASSERT_GE(leader_index, 0);
    ASSERT_LT(leader_index, kMasterNum);

    ASSERT_TRUE(DeleteRedisKey(MasterViewKey()));

    int next_leader_index = -1;
    ViewVersionId next_version = 0;
    ASSERT_TRUE(WaitForNewerView(version, next_leader_index, next_version));
    EXPECT_GE(next_leader_index, 0);
    EXPECT_LT(next_leader_index, kMasterNum);
    EXPECT_GT(next_version, version);
}

TEST_F(RedisChaosTest, ClientRedisDiscoverySurvivesLeaderFailover) {
    int leader_index = -1;
    ViewVersionId version = 0;
    ASSERT_TRUE(WaitForLeader(leader_index, version, std::chrono::seconds(10)));
    WaitForLeaderServiceReady();

    auto client = CreateRedisClient(/*index=*/0, std::chrono::seconds(30));
    ASSERT_NE(client, nullptr);

    void* buffer = nullptr;
    ASSERT_EQ(client->Mount(1024 * 1024 * 16, buffer), ErrorCode::OK);
    ASSERT_TRUE(WaitForClientPutGet(*client, "redis_failover_before", "value",
                                    std::chrono::seconds(5)));

    ASSERT_TRUE(masters_[leader_index]->kill());

    int new_leader_index = -1;
    ViewVersionId new_version = 0;
    ASSERT_TRUE(
        WaitForNewLeader(leader_index, version, new_leader_index, new_version));
    WaitForLeaderServiceReady();

    ASSERT_TRUE(WaitForClientPutGet(*client, "redis_failover_after", "value2",
                                    std::chrono::seconds(60)));
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
