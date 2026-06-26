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

// === Test 8: CreateConnection with invalid port ===
// Covers: redis_helper.cpp:69-74 (port parse exception, host-only endpoint)

TEST_F(RedisHelperTest, CreateConnectionInvalidPort) {
    RedisHelper helper(FLAGS_cluster_id, "127.0.0.1:notaport", "", 0,
                       FLAGS_redis_ttl_sec, 1);
    // Connect should fail because port is not a number (stoi throws)
    EXPECT_NE(ErrorCode::OK, helper.Connect());
}

TEST_F(RedisHelperTest, CreateConnectionHostOnlyEndpoint) {
    // Endpoint without colon → host-only, port defaults to 6379
    RedisHelper helper(FLAGS_cluster_id, "127.0.0.1", "", 0,
                       FLAGS_redis_ttl_sec, 1);
    EXPECT_EQ(ErrorCode::OK, helper.Connect());
}

TEST_F(RedisHelperTest, CreateConnectionUnreachable) {
    // Use a port that nobody listens on — connect should fail
    RedisHelper helper(FLAGS_cluster_id, "127.0.0.1:16379", "", 0,
                       FLAGS_redis_ttl_sec, 1);
    EXPECT_NE(ErrorCode::OK, helper.Connect());
}

// === Test 9: Connect called twice (replaces existing connections) ===
// Covers: redis_helper.cpp:130-131, 145-146

TEST_F(RedisHelperTest, ConnectTwice) {
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                       FLAGS_redis_ttl_sec, 1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());
    // Second Connect should free old contexts and create new ones
    EXPECT_EQ(ErrorCode::OK, helper.Connect());
}

// === Test 10: GetMasterView without Connect ===
// Covers: redis_helper.cpp:510

TEST_F(RedisHelperTest, GetMasterViewWithoutConnect) {
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                       FLAGS_redis_ttl_sec, 1);
    // Don't call Connect — election_ctx_ is null
    std::string addr;
    ViewVersionId ver = 0;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, helper.GetMasterView(addr, ver));
}

// === Test 11: ElectLeader with existing invalid (unparsable) value ===
// Covers: redis_helper.cpp:232-233

TEST_F(RedisHelperTest, ElectLeaderUnparsableExistingValue) {
    const int short_ttl = 2;
    // Write garbage into the master_view key directly
    std::string master_view_key =
        "mooncake:{" + FLAGS_cluster_id + "/}master_view";
    redisContext* ctx = redisConnect(
        FLAGS_redis_endpoint.substr(0, FLAGS_redis_endpoint.rfind(':')).c_str(),
        std::stoi(
            FLAGS_redis_endpoint.substr(FLAGS_redis_endpoint.rfind(':') + 1)));
    ASSERT_TRUE(ctx && !ctx->err);
    redisReply* r = (redisReply*)redisCommand(
        ctx, "SET %b %b EX %d", master_view_key.data(), master_view_key.size(),
        "garbage", 7, short_ttl);
    ASSERT_TRUE(r != nullptr);
    freeReplyObject(r);

    // ElectLeader should still work — it sees an unparsable leader value
    // and waits for it to expire, then wins election
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader("10.0.0.8:50051", version, lease_id);
    ASSERT_GT(version, 0);

    redisFree(ctx);
}

// === Test 12: ElectLeader sees existing leader, waits, then wins ===
// Covers: redis_helper.cpp:218 (TryElectOnce failed → loop back)

TEST_F(RedisHelperTest, ElectLeaderContendedThenWin) {
    const int short_ttl = 1;

    // First helper: elect as leader
    RedisHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                      1);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.9:50051", first_version, first_lease);

    // Second helper: try to elect while key exists — will watch until expiry
    // Then TryElectOnce may race and lose once before winning
    RedisHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, second.Connect());

    ViewVersionId second_version = 0;
    int second_lease = 0;
    // This blocks until first's key expires and second wins
    second.ElectLeader("10.0.0.10:50051", second_version, second_lease);
    ASSERT_GT(second_version, first_version);
}

// === Test 13: GetMasterView with no leader key ===
// Covers: redis_helper.cpp:532-536

TEST_F(RedisHelperTest, GetMasterViewNoLeader) {
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                       FLAGS_redis_ttl_sec, 1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    // No election has happened, so master_view key should not exist
    std::string addr;
    ViewVersionId ver = 0;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, helper.GetMasterView(addr, ver));
}

// === Test 14: GetMasterView with corrupted value in Redis ===
// Covers: redis_helper.cpp:532-536 (ParseLeaderValue failure path)

TEST_F(RedisHelperTest, GetMasterViewCorruptedValue) {
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0,
                       FLAGS_redis_ttl_sec, 1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    // Write corrupt value directly
    std::string master_view_key =
        "mooncake:{" + FLAGS_cluster_id + "/}master_view";
    std::string host =
        FLAGS_redis_endpoint.substr(0, FLAGS_redis_endpoint.rfind(':'));
    int port = std::stoi(
        FLAGS_redis_endpoint.substr(FLAGS_redis_endpoint.rfind(':') + 1));
    redisContext* ctx = redisConnect(host.c_str(), port);
    ASSERT_TRUE(ctx && !ctx->err);
    redisReply* r = (redisReply*)redisCommand(
        ctx, "SET %b %b EX %d", master_view_key.data(), master_view_key.size(),
        "not-json", 8, FLAGS_redis_ttl_sec);
    ASSERT_TRUE(r != nullptr);
    freeReplyObject(r);
    redisFree(ctx);

    std::string addr;
    ViewVersionId ver = 0;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, helper.GetMasterView(addr, ver));
}

// === Test 15: ParseLeaderValue unit tests (no Redis needed) ===
// Covers: redis_helper.cpp:600-601, 606

TEST_F(RedisHelperTest, ParseLeaderValueInvalidJson) {
    std::string addr;
    ViewVersionId epoch = 0;
    EXPECT_FALSE(RedisHelper::ParseLeaderValue("not-json", addr, epoch));
}

TEST_F(RedisHelperTest, ParseLeaderValueMissingFields) {
    std::string addr;
    ViewVersionId epoch = 0;
    // Valid JSON but missing "address" field
    EXPECT_FALSE(RedisHelper::ParseLeaderValue(R"({"epoch":1})", addr, epoch));
    // Has address but missing "epoch"
    EXPECT_FALSE(RedisHelper::ParseLeaderValue(
        R"({"address":"10.0.0.1:50051"})", addr, epoch));
    // Has both but epoch is not integer
    EXPECT_FALSE(RedisHelper::ParseLeaderValue(
        R"({"address":"10.0.0.1:50051","epoch":"not_int"})", addr, epoch));
}

// === Test 16: KeepLeader loses leadership when key is overwritten ===
// Covers: redis_helper.cpp:477-479, 484

TEST_F(RedisHelperTest, KeepLeaderLosesLeadership) {
    const int short_ttl = 2;
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    std::string master_address = "10.0.0.11:50051";
    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader(master_address, version, lease_id);

    // Overwrite the leader key with a different value (simulate another node
    // taking over)
    std::string master_view_key =
        "mooncake:{" + FLAGS_cluster_id + "/}master_view";
    std::string host =
        FLAGS_redis_endpoint.substr(0, FLAGS_redis_endpoint.rfind(':'));
    int port = std::stoi(
        FLAGS_redis_endpoint.substr(FLAGS_redis_endpoint.rfind(':') + 1));
    redisContext* ctx = redisConnect(host.c_str(), port);
    ASSERT_TRUE(ctx && !ctx->err);
    redisReply* r = (redisReply*)redisCommand(
        ctx, "SET %b %b EX %d", master_view_key.data(), master_view_key.size(),
        R"({"address":"10.0.0.99:50051","epoch":999,"ts":0,"ttl":2})", 57,
        short_ttl);
    ASSERT_TRUE(r != nullptr);
    freeReplyObject(r);
    redisFree(ctx);

    // KeepLeader should detect the key no longer matches and exit
    auto start = std::chrono::steady_clock::now();
    helper.KeepLeader(lease_id);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    // Should exit promptly (within heartbeat interval + margin), not hang
    EXPECT_LT(elapsed, (short_ttl + 5) * 1000);
}

// === Test 17: WatchLeader polling fallback ===
// Covers: redis_helper.cpp:379-413 (polling path when SUBSCRIBE fails)
// We use CLIENT KILL to break subscribe_ctx_ so SUBSCRIBE command fails,
// forcing WatchLeader to fall through to the pure polling path.

TEST_F(RedisHelperTest, WatchLeaderPollingFallback) {
    const int short_ttl = 3;

    // First helper: elect as leader (key lives for short_ttl seconds)
    RedisHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                      1);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.12:50051", first_version, first_lease);

    // Second helper: Connect, then break its subscribe connection
    RedisHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, second.Connect());

    // Kill all normal client connections to break subscribe_ctx_
    std::string host =
        FLAGS_redis_endpoint.substr(0, FLAGS_redis_endpoint.rfind(':'));
    int port = std::stoi(
        FLAGS_redis_endpoint.substr(FLAGS_redis_endpoint.rfind(':') + 1));
    redisContext* admin = redisConnect(host.c_str(), port);
    ASSERT_TRUE(admin && !admin->err);
    redisReply* kr =
        (redisReply*)redisCommand(admin, "CLIENT KILL TYPE normal");
    if (kr) freeReplyObject(kr);
    redisFree(admin);

    // ElectLeader will reconnect election_ctx_, then find the leader key,
    // then WatchLeader will try SUBSCRIBE on broken subscribe_ctx_ → fail
    // → fall back to pure polling → first's key expires → polling detects
    // → second wins election
    ViewVersionId second_version = 0;
    int second_lease = 0;
    second.ElectLeader("10.0.0.13:50051", second_version, second_lease);
    ASSERT_GT(second_version, first_version);
}

// === Test 18: Reconnect after connection loss ===
// Covers: redis_helper.cpp:552-566, 192-199, 171-179
// We use CLIENT KILL to sever the election connection, then trigger an
// operation that retries.

TEST_F(RedisHelperTest, ReconnectAfterConnectionLoss) {
    const int short_ttl = 2;
    RedisHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    std::string master_address = "10.0.0.14:50051";
    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader(master_address, version, lease_id);
    ASSERT_GT(version, 0);

    // Start KeepLeader to test reconnection
    std::thread keep_alive_thread([&]() { helper.KeepLeader(lease_id); });

    // Let keep-alive run at least one cycle
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Kill the redis client connections to force a reconnect
    std::string host =
        FLAGS_redis_endpoint.substr(0, FLAGS_redis_endpoint.rfind(':'));
    int port = std::stoi(
        FLAGS_redis_endpoint.substr(FLAGS_redis_endpoint.rfind(':') + 1));
    redisContext* admin = redisConnect(host.c_str(), port);
    ASSERT_TRUE(admin && !admin->err);

    // CLIENT KILL to drop all normal clients except our admin connection
    redisReply* r = (redisReply*)redisCommand(admin, "CLIENT KILL TYPE normal");
    if (r) freeReplyObject(r);
    redisFree(admin);

    // KeepLeader should attempt reconnection and continue or exit gracefully
    std::this_thread::sleep_for(std::chrono::seconds(short_ttl + 2));

    helper.CancelKeepAlive();
    keep_alive_thread.join();
}

// === Test 19: ElectLeader detects cancel via WatchLeader ===
// Covers: redis_helper.cpp:171-179 (election_ctx_ null → retry)
// ElectLeader's while(true) loop doesn't check cancel_requested_ directly,
// but WatchLeader does. We create a scenario where ElectLeader enters
// WatchLeader (which can be cancelled), covering the connect-retry path
// at lines 171-179 along the way.

TEST_F(RedisHelperTest, ElectLeaderCancelledViaWatchLeader) {
    const int short_ttl = 2;

    // First helper: elect as leader and keep it alive
    RedisHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                      1);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.15:50051", first_version, first_lease);

    std::thread first_keep([&]() { first.KeepLeader(first_lease); });

    // Second helper: will block in ElectLeader → WatchLeader
    RedisHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, second.Connect());
    ViewVersionId second_version = 0;
    int second_lease = 0;

    std::thread elect_thread([&]() {
        second.ElectLeader("10.0.0.16:50051", second_version, second_lease);
    });

    // Wait for second to enter WatchLeader
    std::this_thread::sleep_for(std::chrono::seconds(1));
    second.CancelKeepAlive();

    // Cancel first so key expires, allowing second to win
    first.CancelKeepAlive();
    first_keep.join();
    elect_thread.join();

    ASSERT_GT(second_version, first_version);
}

// === Test 20: Subscribe loop processes message ===
// Covers: redis_helper.cpp:352-359 (message parsing in subscribe loop)

TEST_F(RedisHelperTest, SubscribeReceivesVacantMessage) {
    const int short_ttl = 2;

    // First helper: elect as leader with a short TTL
    RedisHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                      1);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.16:50051", first_version, first_lease);

    // Start KeepLeader so the key stays alive
    std::thread first_keep([&]() { first.KeepLeader(first_lease); });

    // Second helper: elect — will block in WatchLeader subscribing
    RedisHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint, "", 0, short_ttl,
                       1);
    ASSERT_EQ(ErrorCode::OK, second.Connect());
    ViewVersionId second_version = 0;
    int second_lease = 0;

    std::thread elect_thread([&]() {
        second.ElectLeader("10.0.0.17:50051", second_version, second_lease);
    });

    // Wait a moment for second to enter WatchLeader subscribe loop
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Cancel first's keep-alive → first's key will expire →
    // second's subscribe loop should detect "vacant" message
    first.CancelKeepAlive();
    first_keep.join();

    // Second should win election
    elect_thread.join();
    ASSERT_GT(second_version, first_version);
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