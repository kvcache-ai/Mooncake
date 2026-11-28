#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <limits>
#include <optional>
#include <vector>
#include <string>
#include <thread>

#include <hiredis/hiredis.h>

#include "ha/leadership/leader_coordinator_factory.h"
#include "ha/leadership/backends/redis/redis_leader_coordinator.h"
#include "ha/common/redis/redis_test_utils.h"
#include "ha/leadership/high_availability_test_fixture.h"
#include "types.h"

namespace mooncake {
namespace testing {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for HA integration tests, e.g. 127.0.0.1:6379");

namespace {

ha::HABackendSpec MakeRedisBackendSpec(const std::string& endpoint,
                                       const std::string& cluster_namespace) {
    return ha::HABackendSpec{
        .type = ha::HABackendType::REDIS,
        .connstring = endpoint,
        .cluster_namespace = cluster_namespace,
        .ha_redis_expected_replicas_per_shard = 1,
    };
}

std::unique_ptr<ha::LeaderCoordinator> CreateRedisCoordinatorOrNull(
    const std::string& endpoint, const std::string& cluster_namespace) {
    auto coordinator = ha::CreateLeaderCoordinator(
        MakeRedisBackendSpec(endpoint, cluster_namespace));
    if (!coordinator) {
        return nullptr;
    }
    return std::move(coordinator.value());
}

std::string MakeRedisTestClusterNamespace(const std::string& suffix) {
    return "ha-redis-test-" + suffix + "-" + UuidToString(generate_uuid());
}

class RedisLeaderPolicyTest : public ::testing::Test {
   protected:
    using Coordinator = ha::backends::redis::RedisLeaderCoordinator;

    void SetUp() override {
        for (const auto* name :
             {"MC_REDIS_DB_INDEX", "MC_REDIS_PASSWORD", "MC_REDIS_USERNAME",
              "MC_REDIS_CLUSTER_ENABLED", "MC_REDIS_TOPOLOGY_REFRESH_MS",
              "MC_REDIS_CONNECT_TIMEOUT_MS", "MC_REDIS_COMMAND_TIMEOUT_MS",
              "MC_REDIS_MAX_REDIRECTS", "MC_REDIS_DURABILITY_PROFILE",
              "MC_REDIS_REPLICATION_ACK_TIMEOUT_MS",
              "MC_REDIS_AOF_ACK_TIMEOUT_MS"}) {
            const auto* value = std::getenv(name);
            environment_.emplace_back(
                name, value ? std::optional<std::string>(value) : std::nullopt);
            ASSERT_EQ(0, ::unsetenv(name));
        }
        ASSERT_EQ(0, ::setenv("MC_REDIS_TOPOLOGY_REFRESH_MS", "0", 1));
    }

    void TearDown() override {
        for (const auto& [name, value] : environment_) {
            if (value) {
                EXPECT_EQ(0, ::setenv(name.c_str(), value->c_str(), 1));
            } else {
                EXPECT_EQ(0, ::unsetenv(name.c_str()));
            }
        }
    }

   private:
    std::vector<std::pair<std::string, std::optional<std::string>>>
        environment_;
};

TEST_F(RedisLeaderPolicyTest, RejectsUnsafeLeaseBudgetBeforeConnecting) {
    for (const auto* profile : {"memory", "persistent"}) {
        ASSERT_EQ(0, ::setenv("MC_REDIS_DURABILITY_PROFILE", profile, 1));
        for (const int64_t ttl : {int64_t{0}, int64_t{-1}, int64_t{2},
                                  std::numeric_limits<int64_t>::max()}) {
            auto spec = MakeRedisBackendSpec("127.0.0.1:1", "budget");
            spec.leader_lease_ttl_sec = ttl;
            Coordinator coordinator(spec);
            EXPECT_EQ(ErrorCode::INVALID_PARAMS, coordinator.Connect());
        }
    }
    ASSERT_EQ(0, ::setenv("MC_REDIS_DURABILITY_PROFILE", "persistent", 1));
    auto spec = MakeRedisBackendSpec("127.0.0.1:1", "budget");
    spec.leader_lease_ttl_sec = 4;  // Two 1s waits equal half the TTL.
    Coordinator coordinator(spec);
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, coordinator.Connect());
}

TEST_F(RedisLeaderPolicyTest, UsesConfiguredLeaseAndMemoryAckTimeout) {
    ASSERT_EQ(0, ::setenv("MC_REDIS_REPLICATION_ACK_TIMEOUT_MS", "321", 1));
    ScriptedRedisPeer peer(
        {"*2\r\n:1\r\n:7\r\n", ":1\r\n", ":1\r\n", ":1\r\n"});
    auto spec = MakeRedisBackendSpec(peer.endpoint(), "ttl");
    spec.leader_lease_ttl_sec = 9;
    Coordinator coordinator(spec);
    ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
    const auto acquired = coordinator.TryAcquireLeadership("leader");
    ASSERT_TRUE(acquired);
    ASSERT_TRUE(acquired->session);
    EXPECT_EQ(std::chrono::seconds(9), acquired->session->lease_ttl);
    auto invalid = *acquired->session;
    invalid.lease_ttl = std::chrono::milliseconds(1);
    EXPECT_FALSE(coordinator.RenewLeadership(invalid));
    EXPECT_EQ(ErrorCode::OK, coordinator.ReleaseLeadership(*acquired->session));
    peer.Join();
    ASSERT_EQ(4u, peer.commands().size());
    EXPECT_EQ("9000", peer.commands()[0][6]);
    for (const size_t index : {1u, 3u})
        EXPECT_EQ((std::vector<std::string>{"WAIT", "1", "321"}),
                  peer.commands()[index]);
}

TEST_F(RedisLeaderPolicyTest, MemoryAckMustMatchExactlyAndOwnerCannotCommit) {
    for (const auto* ack : {":0\r\n", ":2\r\n", "-ERR WAIT denied\r\n"}) {
        ScriptedRedisPeer peer({"*2\r\n:1\r\n:7\r\n", ack, "OWNER"});
        Coordinator coordinator(
            MakeRedisBackendSpec(peer.endpoint(), "unknown"));
        ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
        EXPECT_FALSE(coordinator.TryAcquireLeadership("leader"));
        EXPECT_EQ(Coordinator::OwnershipReconciliation::MatchingUnconfirmed,
                  coordinator.LastOwnershipReconciliation());
        peer.Join();
        ASSERT_EQ(3u, peer.commands().size());
        EXPECT_EQ("HGET", peer.commands()[2][0]);
    }
}

TEST_F(RedisLeaderPolicyTest, UnconfirmedRenewAndReleaseRemainErrors) {
    ScriptedRedisPeer peer({"*2\r\n:1\r\n:7\r\n", ":1\r\n", ":1\r\n", ":0\r\n",
                            "OWNER", ":1\r\n", ":0\r\n", "$-1\r\n"});
    Coordinator coordinator(MakeRedisBackendSpec(peer.endpoint(), "renew"));
    ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
    const auto acquired = coordinator.TryAcquireLeadership("leader");
    ASSERT_TRUE(acquired);
    ASSERT_TRUE(acquired->session);
    EXPECT_FALSE(coordinator.RenewLeadership(*acquired->session));
    EXPECT_EQ(Coordinator::OwnershipReconciliation::MatchingUnconfirmed,
              coordinator.LastOwnershipReconciliation());
    EXPECT_FALSE(coordinator.StartLeadershipMonitor(
        *acquired->session, [](ha::LeadershipLossReason) {}));
    EXPECT_NE(ErrorCode::OK, coordinator.ReleaseLeadership(*acquired->session));
    EXPECT_EQ(Coordinator::OwnershipReconciliation::Lost,
              coordinator.LastOwnershipReconciliation());
}

TEST_F(RedisLeaderPolicyTest,
       BackgroundAckFailureStopsLeaderDespiteOwnerMatch) {
    ScriptedRedisPeer peer({"*2\r\n:1\r\n:7\r\n", ":1\r\n", ":1\r\n", ":1\r\n",
                            ":1\r\n", ":0\r\n", "OWNER", ":1\r\n", ":1\r\n"});
    Coordinator coordinator(
        MakeRedisBackendSpec(peer.endpoint(), "monitor-ack"));
    ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
    const auto acquired = coordinator.TryAcquireLeadership("leader");
    ASSERT_TRUE(acquired);
    ASSERT_TRUE(acquired->session);
    ASSERT_TRUE(coordinator.RenewLeadership(*acquired->session).value());
    std::promise<ha::LeadershipLossReason> lost;
    auto future = lost.get_future();
    auto monitor = coordinator.StartLeadershipMonitor(
        *acquired->session,
        [&lost](ha::LeadershipLossReason reason) { lost.set_value(reason); });
    ASSERT_TRUE(monitor);
    ASSERT_EQ(std::future_status::ready,
              future.wait_for(std::chrono::seconds(4)));
    EXPECT_EQ(ha::LeadershipLossReason::kRenewError, future.get());
    EXPECT_EQ(Coordinator::OwnershipReconciliation::MatchingUnconfirmed,
              coordinator.LastOwnershipReconciliation());
    EXPECT_FALSE(coordinator.RenewLeadership(*acquired->session).value());
    EXPECT_EQ(ErrorCode::OK, coordinator.ReleaseLeadership(*acquired->session));
}

std::vector<std::string> PersistentStartupReplies() {
    return {"$21\r\nredis_version:7.2.0\r\n\r\n",
            "$15\r\naof_enabled:1\r\n\r\n", "*2\r\n:0\r\n:0\r\n"};
}

TEST_F(RedisLeaderPolicyTest,
       PersistentAcquireRequiresExactLocalAndReplicaAof) {
    ASSERT_EQ(0, ::setenv("MC_REDIS_DURABILITY_PROFILE", "persistent", 1));
    ASSERT_EQ(0, ::setenv("MC_REDIS_AOF_ACK_TIMEOUT_MS", "123", 1));
    for (const auto* aof :
         {"*2\r\n:0\r\n:1\r\n", "*2\r\n:1\r\n:0\r\n", "*2\r\n:1\r\n:2\r\n",
          "*2\r\n:2\r\n:1\r\n", "-NOPERM denied\r\n", "*1\r\n:1\r\n"}) {
        auto replies = PersistentStartupReplies();
        replies.insert(replies.end(),
                       {"*2\r\n:1\r\n:7\r\n", ":1\r\n", aof, "OWNER"});
        ScriptedRedisPeer peer(std::move(replies));
        Coordinator coordinator(MakeRedisBackendSpec(peer.endpoint(), "aof"));
        ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
        EXPECT_FALSE(coordinator.TryAcquireLeadership("leader"));
        EXPECT_EQ(Coordinator::OwnershipReconciliation::MatchingUnconfirmed,
                  coordinator.LastOwnershipReconciliation());
        peer.Join();
        ASSERT_EQ(7u, peer.commands().size());
        EXPECT_EQ("EVAL", peer.commands()[3][0]);
        EXPECT_EQ("WAIT", peer.commands()[4][0]);
        EXPECT_EQ((std::vector<std::string>{"WAITAOF", "1", "1", "123"}),
                  peer.commands()[5]);
        EXPECT_EQ("HGET", peer.commands()[6][0]);
    }
}

TEST_F(RedisLeaderPolicyTest, PersistentRenewAndReleaseCannotSkipFsync) {
    ASSERT_EQ(0, ::setenv("MC_REDIS_DURABILITY_PROFILE", "persistent", 1));
    auto replies = PersistentStartupReplies();
    replies.insert(replies.end(),
                   {"*2\r\n:1\r\n:7\r\n", ":1\r\n", "*2\r\n:1\r\n:1\r\n",
                    ":1\r\n", ":1\r\n", "*2\r\n:0\r\n:1\r\n", "OWNER", ":1\r\n",
                    ":1\r\n", "*2\r\n:1\r\n:0\r\n", "$-1\r\n"});
    ScriptedRedisPeer peer(std::move(replies));
    Coordinator coordinator(MakeRedisBackendSpec(peer.endpoint(), "aof-renew"));
    ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
    const auto acquired = coordinator.TryAcquireLeadership("leader");
    ASSERT_TRUE(acquired);
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquired->status);
    ASSERT_TRUE(acquired->session);
    EXPECT_FALSE(coordinator.RenewLeadership(*acquired->session));
    EXPECT_EQ(Coordinator::OwnershipReconciliation::MatchingUnconfirmed,
              coordinator.LastOwnershipReconciliation());
    EXPECT_NE(ErrorCode::OK, coordinator.ReleaseLeadership(*acquired->session));
    EXPECT_EQ(Coordinator::OwnershipReconciliation::Lost,
              coordinator.LastOwnershipReconciliation());
}

TEST_F(RedisLeaderPolicyTest, PersistentWaitShortfallNeverAttemptsFsync) {
    ASSERT_EQ(0, ::setenv("MC_REDIS_DURABILITY_PROFILE", "persistent", 1));
    auto replies = PersistentStartupReplies();
    replies.insert(replies.end(), {"*2\r\n:1\r\n:7\r\n", ":0\r\n", "OWNER"});
    ScriptedRedisPeer peer(std::move(replies));
    Coordinator coordinator(MakeRedisBackendSpec(peer.endpoint(), "aof-wait"));
    ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
    EXPECT_FALSE(coordinator.TryAcquireLeadership("leader"));
    peer.Join();
    ASSERT_EQ(6u, peer.commands().size());
    EXPECT_EQ("WAIT", peer.commands()[4][0]);
    EXPECT_EQ("HGET", peer.commands()[5][0]);
}

TEST_F(RedisLeaderPolicyTest, PersistentReleaseConfirmsBothAcknowledgements) {
    ASSERT_EQ(0, ::setenv("MC_REDIS_DURABILITY_PROFILE", "persistent", 1));
    auto replies = PersistentStartupReplies();
    replies.insert(replies.end(),
                   {"*2\r\n:1\r\n:7\r\n", ":1\r\n", "*2\r\n:1\r\n:1\r\n",
                    ":1\r\n", ":1\r\n", "*2\r\n:1\r\n:1\r\n"});
    ScriptedRedisPeer peer(std::move(replies));
    Coordinator coordinator(
        MakeRedisBackendSpec(peer.endpoint(), "aof-release"));
    ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
    const auto acquired = coordinator.TryAcquireLeadership("leader");
    ASSERT_TRUE(acquired);
    ASSERT_TRUE(acquired->session);
    EXPECT_EQ(ErrorCode::OK, coordinator.ReleaseLeadership(*acquired->session));
    peer.Join();
    ASSERT_EQ(9u, peer.commands().size());
    for (size_t base : {3u, 6u}) {
        EXPECT_EQ("EVAL", peer.commands()[base][0]);
        EXPECT_EQ("WAIT", peer.commands()[base + 1][0]);
        EXPECT_EQ("WAITAOF", peer.commands()[base + 2][0]);
    }
}

TEST_F(RedisLeaderPolicyTest, PersistentStartupFailureNeverWritesLease) {
    ASSERT_EQ(0, ::setenv("MC_REDIS_DURABILITY_PROFILE", "persistent", 1));
    for (bool aof_enabled : {false, true}) {
        auto replies = PersistentStartupReplies();
        if (aof_enabled) {
            replies[2] = "-ERR unknown command 'WAITAOF'\r\n";
        } else {
            replies[1] = "$15\r\naof_enabled:0\r\n\r\n";
            replies.resize(2);
        }
        ScriptedRedisPeer peer(std::move(replies));
        Coordinator coordinator(
            MakeRedisBackendSpec(peer.endpoint(), "aof-start"));
        EXPECT_NE(ErrorCode::OK, coordinator.Connect());
        peer.Join();
        for (const auto& command : peer.commands())
            EXPECT_NE("EVAL", command[0]);
    }
}

}  // namespace

TEST_F(HighAvailabilityTest, RedisBasicMasterViewOperations) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace = MakeRedisTestClusterNamespace("basic");
    auto coordinator =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(coordinator, nullptr);

    auto initial_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(initial_view.has_value());
    EXPECT_FALSE(initial_view->has_value());

    auto acquire = coordinator->TryAcquireLeadership("127.0.0.1:8899");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());

    auto current_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(current_view.has_value());
    ASSERT_TRUE(current_view->has_value());
    EXPECT_EQ("127.0.0.1:8899", current_view->value().leader_address);
    EXPECT_EQ(acquire->session->view.view_version,
              current_view->value().view_version);

    auto renewed = coordinator->RenewLeadership(*acquire->session);
    ASSERT_TRUE(renewed.has_value());
    EXPECT_TRUE(renewed.value());

    auto contender =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(contender, nullptr);
    auto contended = contender->TryAcquireLeadership("127.0.0.1:9900");
    ASSERT_TRUE(contended.has_value());
    EXPECT_EQ(ha::AcquireLeadershipStatus::CONTENDED, contended->status);
    ASSERT_TRUE(contended->observed_view.has_value());
    EXPECT_EQ(acquire->session->view.view_version,
              contended->observed_view->view_version);

    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(*acquire->session));

    auto released_view = contender->ReadCurrentView();
    ASSERT_TRUE(released_view.has_value());
    EXPECT_FALSE(released_view->has_value());
}

TEST_F(HighAvailabilityTest, RedisLeadershipMonitorReportsDeletedView) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace = MakeRedisTestClusterNamespace("monitor");
    auto coordinator =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("127.0.0.1:9911");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    std::promise<ha::LeadershipLossReason> loss_promise;
    auto loss_reported = std::make_shared<std::atomic<bool>>(false);
    auto loss_future = loss_promise.get_future();
    auto monitor = coordinator->StartLeadershipMonitor(
        session,
        [&loss_promise, loss_reported](ha::LeadershipLossReason reason) {
            bool expected = false;
            if (loss_reported->compare_exchange_strong(expected, true)) {
                loss_promise.set_value(reason);
            }
        });
    ASSERT_TRUE(monitor.has_value());

    auto redis = ConnectRedisForTest(FLAGS_redis_endpoint);
    ASSERT_TRUE(redis.has_value());
    const auto master_view_key = BuildRedisMasterViewKey(cluster_namespace);
    RedisReplyPtr delete_reply(static_cast<redisReply*>(
        redisCommand(redis->get(), "DEL %b", master_view_key.data(),
                     master_view_key.size())));
    ASSERT_NE(delete_reply, nullptr);
    ASSERT_NE(delete_reply->type, REDIS_REPLY_ERROR);

    ASSERT_EQ(loss_future.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    EXPECT_EQ(ha::LeadershipLossReason::kLostLeadership, loss_future.get());

    monitor.value()->Stop();
    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));
}

TEST_F(HighAvailabilityTest, RedisLeadershipMonitorIgnoresExplicitRelease) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace = MakeRedisTestClusterNamespace("release");
    auto coordinator =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("127.0.0.1:9922");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    auto callback_fired = std::make_shared<std::atomic<bool>>(false);
    auto monitor = coordinator->StartLeadershipMonitor(
        session, [callback_fired](ha::LeadershipLossReason) {
            callback_fired->store(true);
        });
    ASSERT_TRUE(monitor.has_value());

    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_FALSE(callback_fired->load());
}

TEST_F(HighAvailabilityTest, RedisCanRestartRenewAfterExplicitRelease) {
    if (FLAGS_redis_endpoint.empty()) {
        GTEST_SKIP() << "Redis endpoint is not configured";
    }

    const auto cluster_namespace =
        MakeRedisTestClusterNamespace("restart-renew");
    auto coordinator =
        CreateRedisCoordinatorOrNull(FLAGS_redis_endpoint, cluster_namespace);
    ASSERT_NE(coordinator, nullptr);

    auto first_acquire = coordinator->TryAcquireLeadership("127.0.0.1:9933");
    ASSERT_TRUE(first_acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, first_acquire->status);
    ASSERT_TRUE(first_acquire->session.has_value());

    auto first_renew = coordinator->RenewLeadership(*first_acquire->session);
    ASSERT_TRUE(first_renew.has_value());
    ASSERT_TRUE(first_renew.value());

    ASSERT_EQ(ErrorCode::OK,
              coordinator->ReleaseLeadership(*first_acquire->session));

    auto second_acquire = coordinator->TryAcquireLeadership("127.0.0.1:9944");
    ASSERT_TRUE(second_acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, second_acquire->status);
    ASSERT_TRUE(second_acquire->session.has_value());

    auto second_renew = coordinator->RenewLeadership(*second_acquire->session);
    ASSERT_TRUE(second_renew.has_value());
    ASSERT_TRUE(second_renew.value());

    ASSERT_EQ(ErrorCode::OK,
              coordinator->ReleaseLeadership(*second_acquire->session));
}

}  // namespace testing
}  // namespace mooncake
