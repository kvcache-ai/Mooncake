#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

#include "types.h"

#ifdef STORE_USE_REDIS
#include "redis_helper.h"
#include <hiredis/hiredis.h>

namespace mooncake {
namespace testing {

DEFINE_string(redis_endpoint, "127.0.0.1:6379",
              "Redis endpoint for helper test");
DEFINE_int32(redis_ttl_sec, 2, "Short TTL for testing fast key expiration");
DEFINE_string(cluster_id, "test_helper", "Cluster ID for Redis helper test");

class RedisHelperTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("RedisHelperTest");
        google::SetVLOGLevel("*", 1);
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override { CleanupRedisKeys(); }

    void TearDown() override { CleanupRedisKeys(); }

   private:
    // Delete both master_view and master_epoch keys directly.
    // master_epoch has no TTL (INCR key), so it persists across tests
    // and would block subsequent ElectLeader calls if not cleaned up.
    // Both SetUp and TearDown call this to ensure isolation even if a
    // test's KeepLeader thread is still renewing the key at exit.
    static void CleanupRedisKeys() {
        std::string host = "127.0.0.1";
        int port = 6379;
        auto colon_pos = FLAGS_redis_endpoint.rfind(':');
        if (colon_pos != std::string::npos) {
            host = FLAGS_redis_endpoint.substr(0, colon_pos);
            port = std::stoi(FLAGS_redis_endpoint.substr(colon_pos + 1));
        }
        redisContext* ctx = redisConnect(host.c_str(), port);
        if (ctx && !ctx->err) {
            std::string master_view_key =
                "mooncake:{" + FLAGS_cluster_id + "/}master_view";
            std::string master_epoch_key =
                "mooncake:{" + FLAGS_cluster_id + "/}master_epoch";
            redisReply* r = (redisReply*)redisCommand(
                ctx, "DEL %b %b", master_view_key.data(),
                master_view_key.size(), master_epoch_key.data(),
                master_epoch_key.size());
            if (r) freeReplyObject(r);
            redisFree(ctx);
        } else {
            if (ctx) redisFree(ctx);
        }
    }
};

// === Test 1: Connect ===

TEST_F(RedisHelperTest, Connect) {
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                       FLAGS_redis_ttl_sec, 1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    // Second connection (empty password = no auth) should also work
    RedisHelper helper2(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                        FLAGS_redis_ttl_sec, 1);
    ASSERT_EQ(ErrorCode::OK, helper2.Connect());
}

// === Test 2: ElectLeader + GetMasterView ===

TEST_F(RedisHelperTest, ElectLeaderAndGetMasterView) {
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                       FLAGS_redis_ttl_sec, 1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    std::string master_address = "10.0.0.1:50051";
    ViewVersionId version = 0;
    int lease_id = 0;

    helper.ElectLeader(master_address, version, lease_id);
    ASSERT_GT(version, 0);
    ASSERT_GT(lease_id, 0);

    // Read back via a separate helper
    RedisHelper reader(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                       FLAGS_redis_ttl_sec, 1);
    ASSERT_EQ(ErrorCode::OK, reader.Connect());

    std::string got_address;
    ViewVersionId got_version = 0;
    ASSERT_EQ(ErrorCode::OK, reader.GetMasterView(got_address, got_version));
    ASSERT_EQ(got_address, master_address);
    ASSERT_EQ(got_version, version);
}

// === Test 3: KeepLeader keeps key alive past TTL ===

TEST_F(RedisHelperTest, KeepLeaderRenewsTTL) {
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                       FLAGS_redis_ttl_sec, 1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    std::string master_address = "10.0.0.2:50051";
    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader(master_address, version, lease_id);

    // Start keep-alive in background
    std::thread keep_alive_thread([&]() { helper.KeepLeader(lease_id); });

    // Wait longer than TTL — if KeepLeader works, the key should still exist
    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_redis_ttl_sec * 3));

    std::string got_address;
    ViewVersionId got_version = 0;
    EXPECT_EQ(ErrorCode::OK, helper.GetMasterView(got_address, got_version));
    EXPECT_EQ(got_address, master_address);

    // Cancel keep-alive and join
    helper.CancelKeepAlive();
    keep_alive_thread.join();
}

// === Test 4: Key expires after KeepLeader stops ===

TEST_F(RedisHelperTest, KeyExpiresAfterKeepLeaderStops) {
    const int short_ttl = 1;
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    std::string master_address = "10.0.0.3:50051";
    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader(master_address, version, lease_id);

    // Don't start KeepLeader — let the key expire naturally
    std::this_thread::sleep_for(std::chrono::seconds(short_ttl * 3));

    // Key should have expired
    std::string got_address;
    ViewVersionId got_version = 0;
    EXPECT_NE(ErrorCode::OK, helper.GetMasterView(got_address, got_version));
}

// === Test 5: ElectLeader waits for key expiry, then wins ===

TEST_F(RedisHelperTest, ElectLeaderWaitsForKeyExpiry) {
    const int short_ttl = 1;

    // First helper: elect and let key expire (no KeepLeader)
    RedisHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                      1);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    std::string first_addr = "10.0.0.4:50051";
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader(first_addr, first_version, first_lease);

    // Second helper: try to elect — should block until key expires
    RedisHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, second.Connect());
    ViewVersionId second_version = 0;
    int second_lease = 0;

    std::thread elect_thread([&]() {
        second.ElectLeader("10.0.0.5:50051", second_version, second_lease);
    });

    // Should complete within short_ttl * 3 seconds
    elect_thread.join();
    ASSERT_GT(second_version, 0);
    // Epoch should be strictly increasing
    ASSERT_GT(second_version, first_version);
}

// === Test 6: Epoch monotonicity ===

TEST_F(RedisHelperTest, EpochMonotonicallyIncreases) {
    const int short_ttl = 1;
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    ViewVersionId prev_epoch = 0;
    for (int i = 0; i < 3; i++) {
        ViewVersionId version = 0;
        int lease_id = 0;
        helper.ElectLeader("10.0.0.6:50051", version, lease_id);
        ASSERT_GT(version, prev_epoch);
        prev_epoch = version;
        // Let key expire for next iteration
        std::this_thread::sleep_for(std::chrono::seconds(short_ttl * 3));
    }
}

// === Test 7: CancelKeepAlive stops KeepLeader promptly ===

TEST_F(RedisHelperTest, CancelKeepAliveStopsPromptly) {
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                       FLAGS_redis_ttl_sec, 1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    std::string master_address = "10.0.0.7:50051";
    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader(master_address, version, lease_id);

    std::thread keep_alive_thread([&]() { helper.KeepLeader(lease_id); });

    // Give keep-alive a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto start = std::chrono::steady_clock::now();
    helper.CancelKeepAlive();
    keep_alive_thread.join();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();

    // Should complete within a few seconds, not hang
    EXPECT_LT(elapsed, 5000);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#else  // STORE_USE_REDIS

// Stub main when STORE_USE_REDIS is not defined
int main() { return 0; }

#endif  // STORE_USE_REDIS