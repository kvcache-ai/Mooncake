#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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
DEFINE_int32(redis_master_view_ttl_sec, 2,
             "Redis master view TTL for Redis master failover test");
DEFINE_int32(redis_heartbeat_interval_sec, 1,
             "Redis heartbeat interval for Redis master failover test");

constexpr int kMasterPortBase = 51051;
constexpr int kMasterNum = 3;

namespace mooncake {
namespace testing {

namespace {

void CleanupRedisKeys() {
    std::string host = "127.0.0.1";
    int port = 6379;
    auto colon_pos = FLAGS_redis_endpoint.rfind(':');
    if (colon_pos != std::string::npos) {
        host = FLAGS_redis_endpoint.substr(0, colon_pos);
        port = std::stoi(FLAGS_redis_endpoint.substr(colon_pos + 1));
    }

    redisContext* ctx = redisConnect(host.c_str(), port);
    if (!ctx || ctx->err) {
        if (ctx) redisFree(ctx);
        return;
    }

    std::string cluster_id = FLAGS_redis_cluster_id;
    if (!cluster_id.empty() && cluster_id.back() != '/') {
        cluster_id += '/';
    }
    std::string master_view_key = "mooncake:{" + cluster_id + "}master_view";
    std::string master_epoch_key = "mooncake:{" + cluster_id + "}master_epoch";

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

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
