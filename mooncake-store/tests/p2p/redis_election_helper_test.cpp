#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#include "types.h"

#ifdef STORE_USE_REDIS
#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/ha_status.h"
#include "p2p/ha/redis_election_helper.h"
#include "p2p/ha/p2p_master_view.h"
#include "redis_test_utils.h"

// Keep this include after standard dependencies; the private runner is used to
// exercise the same master-view startup path without launching the RPC loop.
#define private public
#include "p2p/master/p2p_master.h"
#undef private
#include <hiredis/hiredis.h>

namespace mooncake {
class RedisElectionHelperTestPeer {
   public:
    static bool PollLeaderOnce(RedisElectionHelper& helper,
                               redisContext*& context) {
        return helper.PollLeaderOnce(context);
    }

    // Called before starting election; identify only this helper's socket.
    static long long SubscriptionClientId(RedisElectionHelper& helper) {
        std::lock_guard<std::mutex> lock(helper.subscribe_mutex_);
        if (!helper.subscribe_ctx_) return 0;
        RedisReplyPtr reply(
            (redisReply*)redisCommand(helper.subscribe_ctx_, "CLIENT ID"));
        return reply && reply->type == REDIS_REPLY_INTEGER ? reply->integer : 0;
    }
};
}  // namespace mooncake

namespace mooncake {
namespace testing {

DEFINE_string(redis_endpoint, "127.0.0.1:6379",
              "Redis endpoint for Redis test");
DEFINE_string(redis_username, "",
              "Redis ACL username for Redis test. Empty uses password-only "
              "AUTH.");
DEFINE_string(redis_password, "", "Redis password for Redis test");
DEFINE_int32(redis_ttl_sec, 2, "Short TTL for testing fast key expiration");
DEFINE_string(cluster_id, "test_helper", "Cluster ID for Redis test");
DEFINE_bool(redis_supports_client_kill, true,
            "Whether the Redis-compatible backend supports CLIENT KILL.");

// ============================================================
// Helper: clean up Redis keys between tests
// ============================================================

static std::pair<std::string, int> ParseRedisEndpoint() {
    std::string host = "127.0.0.1";
    int port = 6379;
    auto colon_pos = FLAGS_redis_endpoint.rfind(':');
    if (colon_pos != std::string::npos) {
        host = FLAGS_redis_endpoint.substr(0, colon_pos);
        port = std::stoi(FLAGS_redis_endpoint.substr(colon_pos + 1));
    }
    return {host, port};
}

static void CleanupRedisKeys() {
    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    if (AuthenticateRedisContext(ctx, FLAGS_redis_username,
                                 FLAGS_redis_password)) {
        std::string master_view_key =
            "mooncake:{" + FLAGS_cluster_id + "}:master_view";
        std::string master_epoch_key =
            "mooncake:{" + FLAGS_cluster_id + "}:master_epoch";
        redisReply* r = (redisReply*)redisCommand(
            ctx, "DEL %b %b", master_view_key.data(), master_view_key.size(),
            master_epoch_key.data(), master_epoch_key.size());
        if (r) freeReplyObject(r);
        redisFree(ctx);
    } else {
        if (ctx) redisFree(ctx);
    }
}

// ============================================================
// RedisElectionHelperTest — direct RedisElectionHelper API tests
// ============================================================

class RedisElectionHelperTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("RedisElectionHelperTest");
        google::SetVLOGLevel("*", 1);
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override { CleanupRedisKeys(); }
    void TearDown() override { CleanupRedisKeys(); }

    void ExpectMasterStartupFailure(const P2PMasterConfig& config,
                                    const std::string& role,
                                    const std::string& error) {
        struct ResetDiagnostics {
            ~ResetDiagnostics() {
                HAStatusManager::instance().Reset();
                P2PMaster::shutdown_requested_.store(false);
            }
        } reset;
        HAStatusManager::instance().Reset();
        P2PMaster master(config);
        auto result = std::async(std::launch::async,
                                 [&master] { return master.RunWithHA(); });
        const auto completed = result.wait_for(std::chrono::seconds(10));
        if (completed != std::future_status::ready) {
            // Bound a regression that accidentally reaches the serving loop.
            P2PMaster::RequestShutdown();
        }
        EXPECT_EQ(completed, std::future_status::ready);
        EXPECT_EQ(result.get(), -1);
        const auto status = HAStatusManager::instance().Snapshot();
        EXPECT_EQ(status.role, role);
        EXPECT_EQ(status.state, "FAILED");
        EXPECT_FALSE(status.snapshot_ready);
        EXPECT_NE(status.last_error.find(error), std::string::npos)
            << status.last_error;
        const auto doctor = HAStatusManager::instance().GetDoctorReport();
        const auto failed = std::find_if(
            doctor.issues.begin(), doctor.issues.end(),
            [](const auto& issue) { return issue.code == "HA_FAILED"; });
        ASSERT_NE(failed, doctor.issues.end());
        EXPECT_EQ(failed->message, status.last_error);
    }
};

struct ScopedElectionDiagnostics {
    ScopedElectionDiagnostics() {
        auto& status = HAStatusManager::instance();
        status.Reset();
        status.Configure(true, FLAGS_cluster_id, "10.0.0.2:50051");
        status.SetRoleState("standby", "WATCHING");
    }
    ~ScopedElectionDiagnostics() { HAStatusManager::instance().Reset(); }
};

static bool WaitForDoctorIssue(const std::string& code) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        const auto report = HAStatusManager::instance().GetDoctorReport();
        if (std::any_of(
                report.issues.begin(), report.issues.end(),
                [&code](const auto& issue) { return issue.code == code; })) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

static std::string WrongRedisPassword() {
    return FLAGS_redis_password + "-wrong-for-auth-failure-test";
}

static P2PMasterConfig MakeRedisMasterConfig(
    const std::string& redis_password) {
    P2PMasterConfig config;
    config.rpc.port = 0;
    config.rpc.thread_num = 1;
    config.rpc.address = "127.0.0.1";
    config.metrics.enable_reporting = false;
    config.metrics.http_port = 0;
    config.ha.enabled = true;
    config.ha.election_backend = ElectionBackend::REDIS;
    config.redis.endpoint = FLAGS_redis_endpoint;
    config.redis.username = FLAGS_redis_username;
    config.redis.password = redis_password;
    config.redis.master_view_ttl_seconds = FLAGS_redis_ttl_sec;
    config.redis.heartbeat_interval_seconds = 1;
    config.cluster_id = FLAGS_cluster_id;
    return config;
}

TEST_F(RedisElectionHelperTest, Connect) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    // Second connection (empty password = no auth) should also work
    RedisElectionHelper helper2(FLAGS_cluster_id, FLAGS_redis_endpoint,
                                FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                                FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper2.Connect());
}

TEST_F(RedisElectionHelperTest, ConnectFailsWithWrongPassword) {
    RedisElectionHelper reachable(FLAGS_cluster_id, FLAGS_redis_endpoint,
                                  FLAGS_redis_password, 0, FLAGS_redis_ttl_sec,
                                  1, FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, reachable.Connect());

    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               WrongRedisPassword(), 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);

    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, helper.Connect());
}

TEST_F(RedisElectionHelperTest, MasterRunWithHAFailsWithWrongRedisPassword) {
    RedisElectionHelper reachable(FLAGS_cluster_id, FLAGS_redis_endpoint,
                                  FLAGS_redis_password, 0, FLAGS_redis_ttl_sec,
                                  1, FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, reachable.Connect());

    HAStatusManager::instance().Reset();
    P2PMaster master(MakeRedisMasterConfig(WrongRedisPassword()));
    EXPECT_EQ(-1, master.RunWithHA());
    const auto status = HAStatusManager::instance().Snapshot();
    EXPECT_EQ(status.role, "starting");
    EXPECT_EQ(status.state, "FAILED");
    EXPECT_NE(HAStatusManager::instance().SerializeJson().find(
                  "Failed to initialize P2P master view"),
              std::string::npos);
    HAStatusManager::instance().Reset();
}

TEST_F(RedisElectionHelperTest, MasterRpcStartupFailureSurvivesCleanup) {
    asio::io_context io;
    asio::ip::tcp::acceptor occupied(io,
                                     {asio::ip::make_address("127.0.0.1"), 0});
    auto config = MakeRedisMasterConfig(FLAGS_redis_password);
    config.rpc.port = occupied.local_endpoint().port();
    ExpectMasterStartupFailure(config, "primary",
                               "Failed to start P2P master RPC server, port=" +
                                   std::to_string(config.rpc.port));
}

TEST_F(RedisElectionHelperTest, HeartbeatRpcStartupFailureSurvivesCleanup) {
    asio::io_context io;
    asio::ip::tcp::acceptor occupied(io,
                                     {asio::ip::make_address("127.0.0.1"), 0});
    auto config = MakeRedisMasterConfig(FLAGS_redis_password);
    config.rpc.heartbeat_port = occupied.local_endpoint().port();
    ExpectMasterStartupFailure(config, "primary",
                               "Failed to start heartbeat RPC server, port=" +
                                   std::to_string(config.rpc.heartbeat_port));
}

TEST_F(RedisElectionHelperTest, StandbyStartupFailureRecordsReason) {
    asio::io_context io;
    asio::ip::tcp::acceptor occupied(io,
                                     {asio::ip::make_address("127.0.0.1"), 0});
    auto config = MakeRedisMasterConfig(FLAGS_redis_password);
    config.oplog.enabled = true;
    config.oplog.store_type = "redis";
    config.ha.snapshot_service_port = occupied.local_endpoint().port();
    ExpectMasterStartupFailure(config, "standby",
                               "Failed to start P2P standby");
}

TEST_F(RedisElectionHelperTest, PrimarySnapshotPortConflictRecordsReason) {
    asio::io_context io;
    asio::ip::tcp::acceptor reserved(io,
                                     {asio::ip::make_address("127.0.0.1"), 0});
    auto config = MakeRedisMasterConfig(FLAGS_redis_password);
    config.oplog.enabled = true;
    config.oplog.store_type = "redis";
    config.ha.snapshot_service_port = reserved.local_endpoint().port();
    config.rpc.port = config.ha.snapshot_service_port;
    reserved.close();
    ExpectMasterStartupFailure(
        config, "primary",
        "P2P primary snapshot fallback port conflicts with active RPC port");
}

TEST_F(RedisElectionHelperTest, AutoTrimStartupFailureRecordsReason) {
    auto config = MakeRedisMasterConfig(FLAGS_redis_password);
    config.oplog.enabled = true;
    config.oplog.store_type = "redis";
    config.oplog.enable_auto_trim = true;
    config.oplog.auto_trim_interval_sec = 0;
    ExpectMasterStartupFailure(config, "primary",
                               "Failed to start Redis OpLog auto trim worker");
}

// === Test 2: ElectLeader + GetMasterView ===

TEST_F(RedisElectionHelperTest, ElectLeaderAndGetMasterView) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    std::string master_address = "10.0.0.1:50051";
    ViewVersionId version = 0;
    int lease_id = 0;

    helper.ElectLeader(master_address, version, lease_id);
    ASSERT_GT(version, 0);
    ASSERT_GT(lease_id, 0);

    P2PRedisMasterView reader(FLAGS_cluster_id, FLAGS_redis_endpoint,
                              FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                              FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, reader.Connect());

    std::string got_address;
    ViewVersionId got_version = 0;
    ASSERT_EQ(ErrorCode::OK, reader.GetMasterView(got_address, got_version));
    ASSERT_EQ(got_address, master_address);
    ASSERT_EQ(got_version, version);

    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(ctx, FLAGS_redis_username,
                                         FLAGS_redis_password));
    std::string master_view_key =
        "mooncake:{" + FLAGS_cluster_id + "}:master_view";
    RedisReplyPtr raw((redisReply*)redisCommand(
        ctx, "GET %b", master_view_key.data(), master_view_key.size()));
    ASSERT_TRUE(raw && raw->type == REDIS_REPLY_STRING);
    EXPECT_NE(std::string(raw->str, raw->len).find("instance_id"),
              std::string::npos);
    redisFree(ctx);
}

TEST_F(RedisElectionHelperTest, ElectLeaderUsesConfiguredInstanceId) {
    const std::string instance_id = "master-instance-for-election";
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username, instance_id);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader("10.0.0.10:50051", version, lease_id);
    ASSERT_GT(version, 0u);
    ASSERT_GT(lease_id, 0);

    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(ctx, FLAGS_redis_username,
                                         FLAGS_redis_password));
    std::string master_view_key =
        "mooncake:{" + FLAGS_cluster_id + "}:master_view";
    RedisReplyPtr raw((redisReply*)redisCommand(
        ctx, "GET %b", master_view_key.data(), master_view_key.size()));
    ASSERT_TRUE(raw && raw->type == REDIS_REPLY_STRING);
    const std::string value(raw->str, raw->len);
    EXPECT_NE(value.find("\"instance_id\":\"" + instance_id + "\""),
              std::string::npos);
    redisFree(ctx);
}

TEST_F(RedisElectionHelperTest, ShouldElectBlocksUntilReady) {
    ScopedElectionDiagnostics diagnostics;
    std::atomic<bool> ready{false};
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    helper.SetShouldElect([&ready] {
        const bool can_elect = ready.load();
        HAStatusManager::instance().SetElectionReadinessBlocked(
            !can_elect, 100, std::nullopt, 10);
        return can_elect;
    });
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    ViewVersionId version = 0;
    int lease_id = 0;
    std::promise<void> elected;
    auto elected_future = elected.get_future();
    std::thread elect_thread([&] {
        helper.ElectLeader("10.0.0.2:50051", version, lease_id);
        elected.set_value();
    });

    EXPECT_EQ(elected_future.wait_for(std::chrono::milliseconds(300)),
              std::future_status::timeout);
    EXPECT_TRUE(WaitForDoctorIssue("ELECTION_READINESS_BLOCKED"));
    const auto waiting_report = HAStatusManager::instance().GetDoctorReport();
    EXPECT_EQ(waiting_report.issues.size(), 1u);
    for (const auto& issue : waiting_report.issues) {
        EXPECT_EQ(issue.code, "ELECTION_READINESS_BLOCKED");
        EXPECT_EQ(issue.severity, "warning");
        EXPECT_NE(issue.message.find("latest_sequence_id=unknown"),
                  std::string::npos);
    }

    std::string current_leader;
    ViewVersionId current_version = 0;
    EXPECT_NE(helper.GetMasterView(current_leader, current_version),
              ErrorCode::OK);

    ready.store(true);
    if (elected_future.wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        helper.CancelElection();
        elect_thread.join();
        FAIL() << "Timed out waiting for guarded Redis leader election";
    }
    elect_thread.join();
    EXPECT_GT(version, 0u);
    EXPECT_GT(lease_id, 0);
    EXPECT_TRUE(HAStatusManager::instance().GetDoctorReport().issues.empty());
}

TEST_F(RedisElectionHelperTest, CancellationClearsOnlyReadinessDiagnostics) {
    ScopedElectionDiagnostics diagnostics;
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    helper.SetShouldElect([] {
        HAStatusManager::instance().SetElectionReadinessBlocked(
            true, 100, std::nullopt, 10);
        return false;
    });
    ASSERT_EQ(ErrorCode::OK, helper.Connect());
    ViewVersionId version = 0;
    int lease_id = 0;
    std::thread elect_thread(
        [&] { helper.ElectLeader("10.0.0.2:50051", version, lease_id); });
    // Use nonfatal assertions until the worker is cancelled and joined.
    EXPECT_TRUE(WaitForDoctorIssue("ELECTION_READINESS_BLOCKED"));
    HAStatusManager::instance().SetDoctorIssue("ELECTION_FAILED",
                                               "Previous Redis failure");
    helper.CancelElection();
    elect_thread.join();

    EXPECT_EQ(version, 0u);
    EXPECT_EQ(lease_id, 0);
    const auto report = HAStatusManager::instance().GetDoctorReport();
    ASSERT_EQ(report.issues.size(), 1u);
    EXPECT_EQ(report.issues[0].code, "ELECTION_FAILED");
    EXPECT_EQ(report.issues[0].severity, "error");
    EXPECT_EQ(report.issues[0].message, "Previous Redis failure");
}

TEST_F(RedisElectionHelperTest, ContentionDoesNotAdvanceEpoch) {
    RedisElectionHelper leader(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, leader.Connect());
    ViewVersionId leader_version = 0;
    int leader_lease = 0;
    leader.ElectLeader("10.0.0.20:50051", leader_version, leader_lease);

    RedisElectionHelper contender(FLAGS_cluster_id, FLAGS_redis_endpoint,
                                  FLAGS_redis_password, 0, FLAGS_redis_ttl_sec,
                                  1, FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, contender.Connect());
    ViewVersionId contender_version = 0;
    int contender_lease = 0;
    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(ctx, FLAGS_redis_username,
                                         FLAGS_redis_password));
    std::thread election_thread([&] {
        contender.ElectLeader("10.0.0.21:50051", contender_version,
                              contender_lease);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string epoch_key = "mooncake:{" + FLAGS_cluster_id + "}:master_epoch";
    RedisReplyPtr epoch((redisReply*)redisCommand(
        ctx, "GET %b", epoch_key.data(), epoch_key.size()));
    redisFree(ctx);

    contender.CancelElection();
    election_thread.join();
    ASSERT_TRUE(epoch && epoch->type == REDIS_REPLY_STRING);
    EXPECT_EQ(std::stoll(std::string(epoch->str, epoch->len)), leader_version);
    EXPECT_EQ(contender_version, 0);
    EXPECT_EQ(contender_lease, 0);
}

TEST_F(RedisElectionHelperTest, ElectionScriptPublishesEvent) {
    auto [host, port] = ParseRedisEndpoint();
    redisContext* subscriber = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(subscriber, FLAGS_redis_username,
                                         FLAGS_redis_password));
    timeval timeout{3, 0};
    ASSERT_EQ(redisSetTimeout(subscriber, timeout), REDIS_OK);

    std::string channel = "mooncake:" + FLAGS_cluster_id + "leader_event";
    RedisReplyPtr subscribe_reply((redisReply*)redisCommand(
        subscriber, "SUBSCRIBE %b", channel.data(), channel.size()));
    ASSERT_TRUE(subscribe_reply && subscribe_reply->type == REDIS_REPLY_ARRAY);
    ASSERT_GE(subscribe_reply->elements, 3U);
    ASSERT_STREQ(subscribe_reply->element[0]->str, "subscribe");
    ASSERT_EQ(std::string(subscribe_reply->element[1]->str,
                          subscribe_reply->element[1]->len),
              channel);

    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());
    const std::string address = "10.0.0.22:50051";
    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader(address, version, lease_id);

    redisReply* raw_message = nullptr;
    ASSERT_EQ(redisGetReply(subscriber, (void**)&raw_message), REDIS_OK);
    RedisReplyPtr message(raw_message);
    ASSERT_TRUE(message && message->type == REDIS_REPLY_ARRAY);
    ASSERT_GE(message->elements, 3U);
    ASSERT_STREQ(message->element[0]->str, "message");
    EXPECT_EQ(std::string(message->element[1]->str, message->element[1]->len),
              channel);
    EXPECT_EQ(std::string(message->element[2]->str, message->element[2]->len),
              "elected:" + address + ":" + std::to_string(version));
    redisFree(subscriber);
}

// === Test 3: KeepLeader keeps key alive past TTL ===

TEST_F(RedisElectionHelperTest, ReleaseLeaderWakesContenderBeforeTTL) {
    RedisElectionHelper leader(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, 30, 1,
                               FLAGS_redis_username);
    RedisElectionHelper contender(FLAGS_cluster_id, FLAGS_redis_endpoint,
                                  FLAGS_redis_password, 0, 30, 1,
                                  FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, leader.Connect());
    ASSERT_EQ(ErrorCode::OK, contender.Connect());
    ViewVersionId version = 0;
    int lease = 0;
    leader.ElectLeader("10.0.0.30:50051", version, lease);

    auto [host, port] = ParseRedisEndpoint();
    auto ctx = std::unique_ptr<redisContext, decltype(&redisFree)>(
        redisConnect(host.c_str(), port), redisFree);
    ASSERT_TRUE(AuthenticateRedisContext(ctx.get(), FLAGS_redis_username,
                                         FLAGS_redis_password));
    const std::string channel = "mooncake:" + FLAGS_cluster_id + "leader_event";
    ViewVersionId next_version = 0;
    int next_lease = 0;
    auto elected = std::async(std::launch::async, [&] {
        contender.ElectLeader("10.0.0.31:50051", next_version, next_lease);
    });
    bool subscribed = false;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        RedisReplyPtr reply((redisReply*)redisCommand(
            ctx.get(), "PUBSUB NUMSUB %b", channel.data(), channel.size()));
        if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements == 2 &&
            reply->element[1]->type == REDIS_REPLY_INTEGER &&
            reply->element[1]->integer > 0) {
            subscribed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(subscribed);
    EXPECT_EQ(ErrorCode::OK, leader.ReleaseLeader());
    const auto result = elected.wait_for(std::chrono::seconds(3));
    contender.CancelElection();
    elected.get();
    EXPECT_EQ(result, std::future_status::ready);
    EXPECT_GT(next_version, version);
    EXPECT_GT(next_lease, 0);
}

TEST_F(RedisElectionHelperTest, ReleaseLeaderPreservesReplacementView) {
    RedisElectionHelper leader(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, 30, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, leader.Connect());
    EXPECT_EQ(ErrorCode::OK, leader.ReleaseLeader());
    ViewVersionId version = 0;
    int lease = 0;
    const std::string address = "10.0.0.32:50051";
    leader.ElectLeader(address, version, lease);

    auto [host, port] = ParseRedisEndpoint();
    auto ctx = std::unique_ptr<redisContext, decltype(&redisFree)>(
        redisConnect(host.c_str(), port), redisFree);
    ASSERT_TRUE(AuthenticateRedisContext(ctx.get(), FLAGS_redis_username,
                                         FLAGS_redis_password));
    const std::string key = "mooncake:{" + FLAGS_cluster_id + "}:master_view";
    const std::string replacement =
        RedisElectionHelper::SerializeLeaderValue(address, version + 1, 30);
    RedisReplyPtr reply((redisReply*)redisCommand(
        ctx.get(), "SET %b %b EX 30", key.data(), key.size(),
        replacement.data(), replacement.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_STATUS);
    EXPECT_EQ(ErrorCode::OK, leader.ReleaseLeader());
    reply.reset(
        (redisReply*)redisCommand(ctx.get(), "GET %b", key.data(), key.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_STRING);
    EXPECT_EQ(std::string(reply->str, reply->len), replacement);
    EXPECT_EQ(ErrorCode::OK, leader.ReleaseLeader());
}

// === Test 3: KeepLeader keeps key alive past TTL ===

TEST_F(RedisElectionHelperTest, ReleaseLeaderReportsRedisErrorAndCanRetry) {
    RedisElectionHelper leader(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, 30, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, leader.Connect());
    ViewVersionId version = 0;
    int lease = 0;
    leader.ElectLeader("10.0.0.33:50051", version, lease);
    auto [host, port] = ParseRedisEndpoint();
    auto ctx = std::unique_ptr<redisContext, decltype(&redisFree)>(
        redisConnect(host.c_str(), port), redisFree);
    ASSERT_TRUE(AuthenticateRedisContext(ctx.get(), FLAGS_redis_username,
                                         FLAGS_redis_password));
    const std::string key = "mooncake:{" + FLAGS_cluster_id + "}:master_view";
    RedisReplyPtr reply(
        (redisReply*)redisCommand(ctx.get(), "GET %b", key.data(), key.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_STRING);
    const std::string original(reply->str, reply->len);
    reply.reset(
        (redisReply*)redisCommand(ctx.get(), "DEL %b", key.data(), key.size()));
    reply.reset((redisReply*)redisCommand(ctx.get(), "LPUSH %b wrongtype",
                                          key.data(), key.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_INTEGER);
    EXPECT_EQ(leader.ReleaseLeader(), ErrorCode::INTERNAL_ERROR);
    reply.reset((redisReply*)redisCommand(ctx.get(), "LLEN %b", key.data(),
                                          key.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_INTEGER);
    EXPECT_EQ(reply->integer, 1);
    reply.reset((redisReply*)redisCommand(ctx.get(), "SET %b %b EX 30",
                                          key.data(), key.size(),
                                          original.data(), original.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_STATUS);
    EXPECT_EQ(leader.ReleaseLeader(), ErrorCode::OK);
    reply.reset((redisReply*)redisCommand(ctx.get(), "EXISTS %b", key.data(),
                                          key.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_INTEGER);
    EXPECT_EQ(reply->integer, 0);
}

TEST_F(RedisElectionHelperTest, KeepLeaderRenewsTTL) {
    HAMetricManager::instance().set_election_last_successful_renew_timestamp_ms(
        0);
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
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
    EXPECT_GT(HAMetricManager::instance()
                  .get_election_last_successful_renew_timestamp_ms(),
              0);

    // Cancel keep-alive and join
    helper.CancelKeepAlive();
    keep_alive_thread.join();
}

// === Test 4: Key expires after KeepLeader stops ===

TEST_F(RedisElectionHelperTest, KeyExpiresAfterKeepLeaderStops) {
    const int short_ttl = 1;
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
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

TEST_F(RedisElectionHelperTest, ElectLeaderWaitsForKeyExpiry) {
    const int short_ttl = 1;

    // First helper: elect and let key expire (no KeepLeader)
    RedisElectionHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint,
                              FLAGS_redis_password, 0, short_ttl, 1,
                              FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    std::string first_addr = "10.0.0.4:50051";
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader(first_addr, first_version, first_lease);

    // Second helper: try to elect — should block until key expires
    RedisElectionHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, second.Connect());
    ViewVersionId second_version = 0;
    int second_lease = 0;

    // Use a future with timeout to prevent blocking forever if the
    // Redis key does not expire as expected.
    std::promise<void> done;
    auto future = done.get_future();
    std::thread elect_thread([&]() {
        second.ElectLeader("10.0.0.5:50051", second_version, second_lease);
        done.set_value();
    });

    // short_ttl=1, key should expire in ~2s; allow 10s safety margin
    if (future.wait_for(std::chrono::seconds(10)) !=
        std::future_status::ready) {
        second.CancelElection();
        elect_thread.join();
        FAIL() << "Timed out waiting for Redis leader election";
    }
    elect_thread.join();
    ASSERT_GT(second_version, 0);
    // Epoch should be strictly increasing
    ASSERT_GT(second_version, first_version);
}

// === Test 6: Epoch monotonicity ===

TEST_F(RedisElectionHelperTest, EpochMonotonicallyIncreases) {
    const int short_ttl = 1;
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
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

TEST_F(RedisElectionHelperTest, CancelKeepAliveStopsPromptly) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 5,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    std::string master_address = "10.0.0.7:50051";
    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader(master_address, version, lease_id);

    std::thread keep_alive_thread([&]() { helper.KeepLeader(lease_id); });

    // Give keep-alive a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto start = std::chrono::steady_clock::now();
    helper.CancelKeepAlive();
    keep_alive_thread.join();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();

    EXPECT_LT(elapsed, 1000);
}

TEST_F(RedisElectionHelperTest, CancelBeforeKeepAliveIsNotLost) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, 30, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(helper.Connect(), ErrorCode::OK);
    ViewVersionId version = 0;
    int lease = 0;
    helper.ElectLeader("10.0.0.34:50051", version, lease);
    helper.CancelKeepAlive();
    auto worker =
        std::async(std::launch::async, [&] { helper.KeepLeader(lease); });
    const auto result = worker.wait_for(std::chrono::seconds(1));
    helper.Shutdown();
    worker.get();
    EXPECT_EQ(result, std::future_status::ready);
    EXPECT_EQ(helper.ReleaseLeader(), ErrorCode::OK);
    std::string address;
    EXPECT_NE(helper.GetMasterView(address, version), ErrorCode::OK);
}

TEST_F(RedisElectionHelperTest, RejectsConcurrentKeepAlive) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 5,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader("10.0.0.23:50051", version, lease_id);
    std::thread first_keep_alive([&] { helper.KeepLeader(lease_id); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto second_keep_alive =
        std::async(std::launch::async, [&] { helper.KeepLeader(lease_id); });
    EXPECT_EQ(second_keep_alive.wait_for(std::chrono::milliseconds(200)),
              std::future_status::ready);

    helper.CancelKeepAlive();
    first_keep_alive.join();
}

TEST_F(RedisElectionHelperTest, ShutdownWaitsForKeepAlive) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 5,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader("10.0.0.21:50051", version, lease_id);
    std::thread keep_alive_thread([&] { helper.KeepLeader(lease_id); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    const auto start = std::chrono::steady_clock::now();
    helper.Shutdown();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    keep_alive_thread.join();

    EXPECT_LT(elapsed, std::chrono::milliseconds(1000));
}

// === Test 8: CreateConnection with invalid port ===
// Covers: redis_election_helper.cpp:69-74 (port parse exception, host-only
// endpoint)

TEST_F(RedisElectionHelperTest, CreateConnectionInvalidPort) {
    RedisElectionHelper helper(FLAGS_cluster_id, "127.0.0.1:notaport", "", 0,
                               FLAGS_redis_ttl_sec, 1);
    // Connect should fail because port is not a number (stoi throws)
    EXPECT_NE(ErrorCode::OK, helper.Connect());
}

TEST_F(RedisElectionHelperTest, CreateConnectionHostOnlyEndpoint) {
    // Endpoint defaults belong to the configuration layer. RedisUtil requires
    // an explicit port.
    RedisElectionHelper helper(FLAGS_cluster_id, "127.0.0.1", "", 0,
                               FLAGS_redis_ttl_sec, 1);
    EXPECT_NE(ErrorCode::OK, helper.Connect());
}

TEST(RedisUtilTest, ParseEndpoint) {
    std::string host;
    int port = 0;

    EXPECT_TRUE(RedisUtil::ParseEndpoint("redis.example.com:6379", host, port));
    EXPECT_EQ("redis.example.com", host);
    EXPECT_EQ(6379, port);

    EXPECT_TRUE(RedisUtil::ParseEndpoint("[::1]:6380", host, port));
    EXPECT_EQ("::1", host);
    EXPECT_EQ(6380, port);
}

TEST(RedisUtilTest, RejectsEndpointWithoutExplicitPort) {
    for (const char* endpoint : {"", "127.0.0.1", "[::1]", "::1"}) {
        std::string host;
        int port = 0;
        EXPECT_FALSE(RedisUtil::ParseEndpoint(endpoint, host, port))
            << endpoint;
    }
}

TEST_F(RedisElectionHelperTest, CreateConnectionUnreachable) {
    // Use a port that nobody listens on — connect should fail
    RedisElectionHelper helper(FLAGS_cluster_id, "127.0.0.1:16379", "", 0,
                               FLAGS_redis_ttl_sec, 1);
    EXPECT_NE(ErrorCode::OK, helper.Connect());
}

// === Test 9: Connect called twice (replaces existing connections) ===
// Covers: redis_election_helper.cpp:130-131, 145-146

TEST_F(RedisElectionHelperTest, ConnectTwice) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());
    // Second Connect should free old contexts and create new ones
    EXPECT_EQ(ErrorCode::OK, helper.Connect());
}

// === Test 10: GetMasterView without Connect ===
// Covers: redis_election_helper.cpp:510

TEST_F(RedisElectionHelperTest, GetMasterViewWithoutConnect) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    // Don't call Connect — election_ctx_ is null
    std::string addr;
    ViewVersionId ver = 0;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, helper.GetMasterView(addr, ver));
}

// === Test 11: ElectLeader with existing invalid (unparsable) value ===
// Covers: redis_election_helper.cpp:232-233

TEST_F(RedisElectionHelperTest, ElectLeaderUnparsableExistingValue) {
    const int short_ttl = 2;
    // Write garbage into the master_view key directly
    std::string master_view_key =
        "mooncake:{" + FLAGS_cluster_id + "}:master_view";
    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(ctx, FLAGS_redis_username,
                                         FLAGS_redis_password));
    RedisReplyPtr r((redisReply*)redisCommand(
        ctx, "SET %b %b EX %d", master_view_key.data(), master_view_key.size(),
        "garbage", 7, short_ttl));
    ASSERT_TRUE(r != nullptr);

    // ElectLeader should still work — it sees an unparsable leader value
    // and waits for it to expire, then wins election
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader("10.0.0.8:50051", version, lease_id);
    ASSERT_GT(version, 0);

    redisFree(ctx);
}

// === Test 12: ElectLeader sees existing leader, waits, then wins ===
// Covers: redis_election_helper.cpp:218 (TryElectOnce failed → loop back)

TEST_F(RedisElectionHelperTest, ElectLeaderContendedThenWin) {
    const int short_ttl = 1;

    // First helper: elect as leader
    RedisElectionHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint,
                              FLAGS_redis_password, 0, short_ttl, 1,
                              FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.9:50051", first_version, first_lease);

    // Second helper: try to elect while key exists — will watch until expiry
    // Then TryElectOnce may race and lose once before winning
    RedisElectionHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, second.Connect());

    ViewVersionId second_version = 0;
    int second_lease = 0;
    // This blocks until first's key expires and second wins
    second.ElectLeader("10.0.0.10:50051", second_version, second_lease);
    ASSERT_GT(second_version, first_version);
}

// === Test 13: GetMasterView with no leader key ===
// Covers: redis_election_helper.cpp:532-536

TEST_F(RedisElectionHelperTest, GetMasterViewNoLeader) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    // No election has happened, so master_view key should not exist
    std::string addr;
    ViewVersionId ver = 0;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, helper.GetMasterView(addr, ver));
}

// === Test 14: GetMasterView with corrupted value in Redis ===
// Covers: redis_election_helper.cpp:532-536 (ParseLeaderValue failure path)

TEST_F(RedisElectionHelperTest, GetMasterViewCorruptedValue) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    // Write corrupt value directly
    std::string master_view_key =
        "mooncake:{" + FLAGS_cluster_id + "}:master_view";
    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(ctx, FLAGS_redis_username,
                                         FLAGS_redis_password));
    RedisReplyPtr r((redisReply*)redisCommand(
        ctx, "SET %b %b EX %d", master_view_key.data(), master_view_key.size(),
        "not-json", 8, FLAGS_redis_ttl_sec));
    ASSERT_TRUE(r != nullptr);
    redisFree(ctx);

    std::string addr;
    ViewVersionId ver = 0;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, helper.GetMasterView(addr, ver));
}

// === Test 15: ParseLeaderValue unit tests (no Redis needed) ===
// Covers: redis_election_helper.cpp:600-601, 606

TEST_F(RedisElectionHelperTest, ParseLeaderValueInvalidJson) {
    std::string addr;
    ViewVersionId epoch = 0;
    EXPECT_FALSE(
        RedisElectionHelper::ParseLeaderValue("not-json", addr, epoch));
}

TEST_F(RedisElectionHelperTest, ParseLeaderValueMissingFields) {
    std::string addr;
    ViewVersionId epoch = 0;
    // Valid JSON but missing "address" field
    EXPECT_FALSE(
        RedisElectionHelper::ParseLeaderValue(R"({"epoch":1})", addr, epoch));
    // Has address but missing "epoch"
    EXPECT_FALSE(RedisElectionHelper::ParseLeaderValue(
        R"({"address":"10.0.0.1:50051"})", addr, epoch));
    // Has both but epoch is not integer
    EXPECT_FALSE(RedisElectionHelper::ParseLeaderValue(
        R"({"address":"10.0.0.1:50051","epoch":"not_int"})", addr, epoch));
}

// === Test 16: KeepLeader loses leadership when key is overwritten ===
// Covers: redis_election_helper.cpp:477-479, 484

TEST_F(RedisElectionHelperTest, KeepLeaderLosesLeadership) {
    const auto before_renew_failures =
        HAMetricManager::instance().get_election_renew_failures_total();
    auto& ha_status = HAStatusManager::instance();
    ha_status.Reset();
    ha_status.Configure(true, FLAGS_cluster_id, "test-primary");
    const int short_ttl = 2;
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    std::string master_address = "10.0.0.11:50051";
    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader(master_address, version, lease_id);

    // Overwrite the leader key with a different value (simulate another node
    // taking over)
    std::string master_view_key =
        "mooncake:{" + FLAGS_cluster_id + "}:master_view";
    auto [host, port] = ParseRedisEndpoint();
    redisContext* ctx = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(ctx, FLAGS_redis_username,
                                         FLAGS_redis_password));
    RedisReplyPtr r((redisReply*)redisCommand(
        ctx, "SET %b %b EX %d", master_view_key.data(), master_view_key.size(),
        R"({"address":"10.0.0.99:50051","epoch":999,"ts":0,"ttl":2})", 57,
        short_ttl));
    ASSERT_TRUE(r != nullptr);
    redisFree(ctx);

    // KeepLeader should detect the key no longer matches and exit
    auto start = std::chrono::steady_clock::now();
    helper.KeepLeader(lease_id);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    // Should exit promptly (within heartbeat interval + margin), not hang
    EXPECT_LT(elapsed, (short_ttl + 5) * 1000);
    EXPECT_EQ(before_renew_failures + 1,
              HAMetricManager::instance().get_election_renew_failures_total());
    EXPECT_NE(ha_status.SerializeDoctorJson().find("LEADERSHIP_LOST"),
              std::string::npos);
    ha_status.Reset();
}

// === Test 17: WatchLeader polling fallback ===
// Covers: redis_election_helper.cpp:379-413 (polling path when SUBSCRIBE fails)
// We use CLIENT KILL to break subscribe_ctx_ so SUBSCRIBE command fails,
// forcing WatchLeader to fall through to the pure polling path.

TEST_F(RedisElectionHelperTest, PollLeaderOnceRecordsAndClearsWatchFailure) {
    auto& ha_status = HAStatusManager::instance();
    ha_status.Reset();
    ha_status.Configure(true, FLAGS_cluster_id, "test-standby");
    ha_status.SetRoleState("standby", "WATCHING");

    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    auto [host, port] = ParseRedisEndpoint();
    redisContext* admin = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(admin, FLAGS_redis_username,
                                         FLAGS_redis_password));
    const std::string key = "mooncake:{" + FLAGS_cluster_id + "}:master_view";

    RedisReplyPtr reply(
        (redisReply*)redisCommand(admin, "DEL %b", key.data(), key.size()));
    reply.reset((redisReply*)redisCommand(admin, "LPUSH %b wrongtype",
                                          key.data(), key.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_INTEGER);

    redisContext* context = nullptr;
    EXPECT_FALSE(RedisElectionHelperTestPeer::PollLeaderOnce(helper, context));
    EXPECT_EQ(context, nullptr);
    EXPECT_NE(ha_status.SerializeDoctorJson().find("ELECTION_WATCH_FAILED"),
              std::string::npos);

    const auto leader_value = RedisElectionHelper::SerializeLeaderValue(
        "primary:50051", 1, FLAGS_redis_ttl_sec);
    reply.reset(
        (redisReply*)redisCommand(admin, "DEL %b", key.data(), key.size()));
    ASSERT_TRUE(reply);
    reply.reset((redisReply*)redisCommand(admin, "SET %b %b", key.data(),
                                          key.size(), leader_value.data(),
                                          leader_value.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_STATUS);
    EXPECT_FALSE(RedisElectionHelperTestPeer::PollLeaderOnce(helper, context));
    ASSERT_NE(context, nullptr);
    EXPECT_EQ(ha_status.SerializeDoctorJson().find("ELECTION_WATCH_FAILED"),
              std::string::npos);

    reply.reset(
        (redisReply*)redisCommand(admin, "DEL %b", key.data(), key.size()));
    ASSERT_TRUE(reply && reply->type == REDIS_REPLY_INTEGER);
    EXPECT_TRUE(RedisElectionHelperTestPeer::PollLeaderOnce(helper, context));
    ASSERT_NE(context, nullptr);

    redisFree(context);
    redisFree(admin);
    ha_status.Reset();
}

TEST_F(RedisElectionHelperTest, WatchLeaderPollingFallback) {
    if (!FLAGS_redis_supports_client_kill) {
        GTEST_SKIP() << "Redis-compatible backend does not support CLIENT KILL";
    }

    const int short_ttl = 3;

    // First helper: elect as leader (key lives for short_ttl seconds)
    RedisElectionHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint,
                              FLAGS_redis_password, 0, short_ttl, 1,
                              FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.12:50051", first_version, first_lease);

    // Second helper: Connect, then break its subscribe connection
    RedisElectionHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, second.Connect());

    // Kill all normal client connections to break subscribe_ctx_
    auto [host, port] = ParseRedisEndpoint();
    redisContext* admin = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(admin, FLAGS_redis_username,
                                         FLAGS_redis_password));
    RedisReplyPtr kr(
        (redisReply*)redisCommand(admin, "CLIENT KILL TYPE normal"));
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
// Covers: redis_election_helper.cpp:552-566, 192-199, 171-179
// We use CLIENT KILL to sever the election connection, then trigger an
// operation that retries.

TEST_F(RedisElectionHelperTest, ReconnectAfterConnectionLoss) {
    if (!FLAGS_redis_supports_client_kill) {
        GTEST_SKIP() << "Redis-compatible backend does not support CLIENT KILL";
    }

    const int short_ttl = 2;
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
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
    auto [host, port] = ParseRedisEndpoint();
    redisContext* admin = redisConnect(host.c_str(), port);
    ASSERT_TRUE(AuthenticateRedisContext(admin, FLAGS_redis_username,
                                         FLAGS_redis_password));

    // CLIENT KILL to drop all normal clients except our admin connection
    RedisReplyPtr r(
        (redisReply*)redisCommand(admin, "CLIENT KILL TYPE normal"));
    redisFree(admin);

    // KeepLeader should reconnect, atomically renew, and retain leadership.
    std::this_thread::sleep_for(std::chrono::seconds(short_ttl + 2));

    std::string current_address;
    ViewVersionId current_version = 0;
    EXPECT_EQ(helper.GetMasterView(current_address, current_version),
              ErrorCode::OK);
    EXPECT_EQ(current_address, master_address);
    EXPECT_EQ(current_version, version);

    helper.CancelKeepAlive();
    keep_alive_thread.join();
}

// === Test 19: ElectLeader detects cancel via WatchLeader ===
// Covers: redis_election_helper.cpp:171-179 (election_ctx_ null → retry)
// ElectLeader can be cancelled while waiting in WatchLeader.

TEST_F(RedisElectionHelperTest, ElectLeaderCancelledViaWatchLeader) {
    const int short_ttl = 2;

    // First helper: elect as leader and keep it alive
    RedisElectionHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint,
                              FLAGS_redis_password, 0, short_ttl, 1,
                              FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.15:50051", first_version, first_lease);

    std::thread first_keep([&]() { first.KeepLeader(first_lease); });

    // Second helper: will block in ElectLeader → WatchLeader
    RedisElectionHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, second.Connect());
    ViewVersionId second_version = 0;
    int second_lease = 0;

    std::thread elect_thread([&]() {
        second.ElectLeader("10.0.0.16:50051", second_version, second_lease);
    });

    // Wait for second to enter WatchLeader
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto cancel_start = std::chrono::steady_clock::now();
    second.CancelElection();
    elect_thread.join();
    auto cancel_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - cancel_start);

    // Clean up the first leader.
    first.CancelKeepAlive();
    first_keep.join();

    ASSERT_EQ(0, second_version);
    EXPECT_LT(cancel_elapsed, std::chrono::milliseconds(1000));
}

TEST_F(RedisElectionHelperTest, ConnectResetsElectionCancellation) {
    RedisElectionHelper helper(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, FLAGS_redis_ttl_sec, 1,
                               FLAGS_redis_username);
    helper.CancelElection();
    ASSERT_EQ(ErrorCode::OK, helper.Connect());

    ViewVersionId version = 0;
    int lease_id = 0;
    helper.ElectLeader("10.0.0.18:50051", version, lease_id);

    EXPECT_GT(version, 0);
    EXPECT_GT(lease_id, 0);
}

TEST_F(RedisElectionHelperTest, ConnectWaitsForActiveWatch) {
    const int short_ttl = 2;
    RedisElectionHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint,
                              FLAGS_redis_password, 0, short_ttl, 1,
                              FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.19:50051", first_version, first_lease);
    std::thread first_keep([&] { first.KeepLeader(first_lease); });

    RedisElectionHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, second.Connect());
    ViewVersionId second_version = 0;
    int second_lease = 0;
    std::thread elect_thread([&] {
        second.ElectLeader("10.0.0.20:50051", second_version, second_lease);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto reconnect =
        std::async(std::launch::async, [&] { return second.Connect(); });
    EXPECT_EQ(reconnect.wait_for(std::chrono::milliseconds(200)),
              std::future_status::timeout);

    second.CancelElection();
    elect_thread.join();
    ASSERT_EQ(reconnect.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(reconnect.get(), ErrorCode::OK);

    first.CancelKeepAlive();
    first_keep.join();
    EXPECT_EQ(second_version, 0);
}

TEST_F(RedisElectionHelperTest, ConnectDoesNotUndoElectionCancellation) {
    const int short_ttl = 2;
    RedisElectionHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint,
                              FLAGS_redis_password, 0, short_ttl, 1,
                              FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.21:50051", first_version, first_lease);
    std::thread first_keep([&] { first.KeepLeader(first_lease); });

    RedisElectionHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, second.Connect());
    ViewVersionId second_version = 0;
    int second_lease = 0;
    std::thread elect_thread([&] {
        second.ElectLeader("10.0.0.22:50051", second_version, second_lease);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    second.CancelElection();
    auto reconnect =
        std::async(std::launch::async, [&] { return second.Connect(); });
    elect_thread.join();

    ASSERT_EQ(reconnect.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(reconnect.get(), ErrorCode::OK);
    EXPECT_EQ(second_version, 0);

    first.CancelKeepAlive();
    first_keep.join();
}

// === Test 20: Watch detects an expired leader ===

TEST_F(RedisElectionHelperTest, WatchLeaderDetectsExpiredLeader) {
    const int short_ttl = 2;

    // First helper: elect as leader with a short TTL
    RedisElectionHelper first(FLAGS_cluster_id, FLAGS_redis_endpoint,
                              FLAGS_redis_password, 0, short_ttl, 1,
                              FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, first.Connect());
    ViewVersionId first_version = 0;
    int first_lease = 0;
    first.ElectLeader("10.0.0.16:50051", first_version, first_lease);

    // Start KeepLeader so the key stays alive
    std::thread first_keep([&]() { first.KeepLeader(first_lease); });

    // Second helper: elect — will block in WatchLeader subscribing
    RedisElectionHelper second(FLAGS_cluster_id, FLAGS_redis_endpoint,
                               FLAGS_redis_password, 0, short_ttl, 1,
                               FLAGS_redis_username);
    ASSERT_EQ(ErrorCode::OK, second.Connect());
    ViewVersionId second_version = 0;
    int second_lease = 0;

    std::thread elect_thread([&]() {
        second.ElectLeader("10.0.0.17:50051", second_version, second_lease);
    });

    // Wait a moment for second to enter WatchLeader subscribe loop
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Cancel first's keep-alive. The subscription watch's polling backup
    // detects expiration and lets the second helper compete for leadership.
    first.CancelKeepAlive();
    first_keep.join();

    // Second should win election
    elect_thread.join();
    ASSERT_GT(second_version, first_version);
}

class RedisElectionWatchRetryTest : public RedisElectionHelperTest,
                                    public ::testing::WithParamInterface<bool> {
   protected:
    template <typename Predicate>
    bool WaitUntil(Predicate predicate) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        do {
            if (predicate()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } while (std::chrono::steady_clock::now() < deadline);
        return false;
    }

    bool WatchFailed() {
        return HAStatusManager::instance().SerializeDoctorJson().find(
                   "ELECTION_WATCH_FAILED") != std::string::npos;
    }

    std::map<long long, std::string> ClientCommands() {
        RedisReplyPtr reply(
            (redisReply*)redisCommand(admin_.get(), "CLIENT LIST"));
        if (!reply || reply->type != REDIS_REPLY_STRING) return {};
        std::map<long long, std::string> clients;
        std::istringstream lines(std::string(reply->str, reply->len));
        std::string line;
        while (std::getline(lines, line)) {
            std::istringstream fields(line);
            std::string field, command;
            long long id = 0;
            while (fields >> field) {
                if (field.starts_with("id=")) {
                    id = std::stoll(field.substr(3));
                } else if (field.starts_with("cmd=")) {
                    command = field.substr(4);
                }
            }
            if (id > 0) clients.emplace(id, std::move(command));
        }
        return clients;
    }

    long long PollingClientId() {
        long long polling_id = 0;
        for (const auto& [id, command] : ClientCommands()) {
            if (command == "get" && !clients_before_watch_.contains(id)) {
                // Never guess a target when more than one client matches.
                if (polling_id != 0) return 0;
                polling_id = id;
            }
        }
        return polling_id;
    }

    long long SubscriberCount() {
        // Mooncake's channel name differs from kvpool's hash-tagged channel.
        const auto channel = "mooncake:" + FLAGS_cluster_id + "leader_event";
        RedisReplyPtr reply((redisReply*)redisCommand(
            admin_.get(), "PUBSUB NUMSUB %b", channel.data(), channel.size()));
        return reply && reply->type == REDIS_REPLY_ARRAY &&
                       reply->elements == 2 && reply->element[1] &&
                       reply->element[1]->type == REDIS_REPLY_INTEGER
                   ? reply->element[1]->integer
                   : -1;
    }

    uint64_t PollingFallbacks() {
        std::istringstream metrics(
            HAMetricManager::instance().serialize_metrics());
        const std::string prefix = "ha_election_polling_fallbacks_total ";
        std::string line;
        while (std::getline(metrics, line)) {
            if (line.starts_with(prefix)) {
                return std::stoull(line.substr(prefix.size()));
            }
        }
        return 0;
    }

    void KillClient(long long id) {
        ASSERT_GT(id, 0);
        RedisReplyPtr reply(
            (redisReply*)redisCommand(admin_.get(), "CLIENT KILL ID %lld", id));
        ASSERT_TRUE(reply && reply->type == REDIS_REPLY_INTEGER);
        ASSERT_EQ(reply->integer, 1);
    }

    void SetUp() override {
        RedisElectionHelperTest::SetUp();
        auto& status = HAStatusManager::instance();
        status.Reset();
        status.Configure(true, FLAGS_cluster_id, "test-standby");
        status.SetRoleState("standby", "WATCHING");
        if (GetParam() && !FLAGS_redis_supports_client_kill) {
            GTEST_SKIP() << "Polling fallback requires CLIENT KILL";
        }
        auto [host, port] = ParseRedisEndpoint();
        admin_.reset(redisConnect(host.c_str(), port));
        ASSERT_TRUE(AuthenticateRedisContext(admin_.get(), FLAGS_redis_username,
                                             FLAGS_redis_password));
        key_ = "mooncake:{" + FLAGS_cluster_id + "}:master_view";
        leader_value_ = RedisElectionHelper::SerializeLeaderValue(
            "old-primary:50051", 1, 30);
        ASSERT_NO_FATAL_FAILURE(RestorePollingGet());
        helper_ = std::make_unique<RedisElectionHelper>(
            FLAGS_cluster_id, FLAGS_redis_endpoint, FLAGS_redis_password, 0, 1,
            1, FLAGS_redis_username);
        ASSERT_EQ(helper_->Connect(), ErrorCode::OK);
        clients_before_watch_ = ClientCommands();
        ASSERT_FALSE(clients_before_watch_.empty());
        const auto fallbacks = PollingFallbacks();
        if (GetParam()) {
            // Break only the unopened subscription to force polling fallback.
            ASSERT_NO_FATAL_FAILURE(KillClient(
                RedisElectionHelperTestPeer::SubscriptionClientId(*helper_)));
        }
        election_ = std::async(std::launch::async, [&] {
            helper_->ElectLeader("new-primary:50051", version_, lease_);
        });
        ASSERT_TRUE(WaitUntil([&] {
            return PollingClientId() > 0 &&
                   (GetParam() ? PollingFallbacks() > fallbacks &&
                                     SubscriberCount() == 0
                               : SubscriberCount() == 1);
        }));
        EXPECT_FALSE(WatchFailed());
    }

    void TearDown() override {
        // Cancel before waiting, including after fatal setup/test assertions.
        if (helper_) helper_->CancelElection();
        if (election_.valid()) election_.get();
        helper_.reset();
        HAStatusManager::instance().Reset();
        RedisElectionHelperTest::TearDown();
    }

    void BreakPollingGet() {
        // Change type atomically: no absent-key window may trigger election.
        RedisReplyPtr reply((redisReply*)redisCommand(
            admin_.get(), "EVAL %s 1 %b",
            "redis.call('DEL', KEYS[1]); "
            "return redis.call('LPUSH', KEYS[1], 'wrongtype')",
            key_.data(), key_.size()));
        ASSERT_TRUE(reply && reply->type == REDIS_REPLY_INTEGER);
    }

    void RestorePollingGet() {
        RedisReplyPtr reply((redisReply*)redisCommand(
            admin_.get(), "SET %b %b EX 30", key_.data(), key_.size(),
            leader_value_.data(), leader_value_.size()));
        ASSERT_TRUE(reply && reply->type == REDIS_REPLY_STATUS);
    }

    void ExpireLeaderAndExpectElection() {
        EXPECT_EQ(election_.wait_for(std::chrono::milliseconds(0)),
                  std::future_status::timeout);
        // Expiration sends no leader_event notification.
        RedisReplyPtr reply((redisReply*)redisCommand(
            admin_.get(), "PEXPIRE %b 100", key_.data(), key_.size()));
        ASSERT_TRUE(reply && reply->type == REDIS_REPLY_INTEGER &&
                    reply->integer == 1);
        ASSERT_EQ(election_.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        election_.get();
        EXPECT_GT(version_, 0);
        EXPECT_GT(lease_, 0);
        EXPECT_FALSE(WatchFailed());
    }

    std::unique_ptr<redisContext, decltype(&redisFree)> admin_{nullptr,
                                                               redisFree};
    std::unique_ptr<RedisElectionHelper> helper_;
    std::future<void> election_;
    std::map<long long, std::string> clients_before_watch_;
    std::string key_;
    std::string leader_value_;
    ViewVersionId version_{0};
    int lease_{0};
};

TEST_P(RedisElectionWatchRetryTest, RecoversGetErrorsWithoutNotification) {
    ASSERT_NO_FATAL_FAILURE(BreakPollingGet());
    ASSERT_TRUE(WaitUntil([&] { return WatchFailed(); }));
    // Reconnecting successfully cannot clear a still-failing GET.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_TRUE(WatchFailed());
    auto& status = HAStatusManager::instance();
    EXPECT_NE(status.SerializeDoctorJson().find("WRONGTYPE"),
              std::string::npos);
    status.SetDoctorIssue("OPLOG_READ_FAILED", "independent read failure");
    ASSERT_NO_FATAL_FAILURE(RestorePollingGet());
    ASSERT_TRUE(WaitUntil([&] { return !WatchFailed(); }));
    EXPECT_NE(status.SerializeDoctorJson().find("OPLOG_READ_FAILED"),
              std::string::npos);
    EXPECT_EQ(SubscriberCount(), GetParam() ? 0 : 1);
    ASSERT_NO_FATAL_FAILURE(ExpireLeaderAndExpectElection());
}

TEST_P(RedisElectionWatchRetryTest, CancellationClearsWatchFailure) {
    ASSERT_NO_FATAL_FAILURE(BreakPollingGet());
    ASSERT_TRUE(WaitUntil([&] { return WatchFailed(); }));
    helper_->CancelElection();
    ASSERT_EQ(election_.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    election_.get();
    EXPECT_EQ(version_, 0);
    EXPECT_EQ(lease_, 0);
    EXPECT_FALSE(WatchFailed());
}

TEST_P(RedisElectionWatchRetryTest, RecoversLostPollingConnection) {
    if (!FLAGS_redis_supports_client_kill) {
        GTEST_SKIP() << "Connection loss requires CLIENT KILL";
    }
    const auto polling_id = PollingClientId();
    // Leave the election, Pub/Sub and unrelated clients connected.
    ASSERT_NO_FATAL_FAILURE(KillClient(polling_id));
    ASSERT_TRUE(WaitUntil([&] { return WatchFailed(); }));
    ASSERT_TRUE(WaitUntil([&] {
        const auto recovered_id = PollingClientId();
        return recovered_id > 0 && recovered_id != polling_id && !WatchFailed();
    }));
    EXPECT_EQ(SubscriberCount(), GetParam() ? 0 : 1);
    ASSERT_NO_FATAL_FAILURE(ExpireLeaderAndExpectElection());
}

INSTANTIATE_TEST_SUITE_P(WatchModes, RedisElectionWatchRetryTest,
                         ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                             return info.param ? "Polling" : "PubSub";
                         });

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
