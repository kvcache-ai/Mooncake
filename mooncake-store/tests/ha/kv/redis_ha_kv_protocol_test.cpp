#include "ha/kv/redis_ha_kv_backend.h"

#include <gtest/gtest.h>
#include <hiredis/hiredis.h>

#include <atomic>
#include <cstdlib>
#include <optional>
#include <thread>
#include <tuple>
#include <utility>

#include "ha/kv/ha_kv_backend_factory.h"
#include "ha/common/redis/redis_test_utils.h"
#include "ha/leadership/backends/redis/redis_leader_coordinator.h"

namespace mooncake::test {
namespace {

class RedisHaKvProtocolTest : public ::testing::Test {
   protected:
    void SetUp() override {
        for (const auto* name :
             {"MC_REDIS_DB_INDEX", "MC_REDIS_PASSWORD", "MC_REDIS_USERNAME",
              "MC_REDIS_CLUSTER_ENABLED", "MC_REDIS_TOPOLOGY_REFRESH_MS",
              "MC_REDIS_CONNECT_TIMEOUT_MS", "MC_REDIS_COMMAND_TIMEOUT_MS",
              "MC_REDIS_MAX_REDIRECTS"}) {
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

using mooncake::testing::ScriptedRedisPeer;
using mooncake::testing::ShardsTopology;
using mooncake::testing::Topology;

TEST_F(RedisHaKvProtocolTest, StartupFailuresRetainStructuredReason) {
    using Client = ha::common::redis::RedisClusterClient;
    for (bool shards : {false, true}) {
        for (const auto& [response, reason] :
             std::vector<std::pair<std::string, Client::RoutingError>>{
                 {shards ? ShardsTopology(1) : Topology(1),
                  Client::RoutingError::ReplicaCountMismatch},
                 {"*0\r\n", Client::RoutingError::InvalidTopology},
                 {"-NOPERM denied\r\n", Client::RoutingError::ServerError},
                 {"", Client::RoutingError::Transport}}) {
            ScriptedRedisPeer peer({response}, {}, false, shards);
            Client client;
            const auto result =
                client.ConnectDetailed("redis+cluster://" + peer.endpoint(), 2);
            EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.error);
            EXPECT_EQ(reason, result.routing_error);
        }
    }
    Client client;
    auto result = client.ConnectDetailed("redis+cluster://127.0.0.1:1", 0);
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error);
    EXPECT_EQ(Client::RoutingError::InvalidConfiguration, result.routing_error);
    result = client.ConnectDetailed("redis+cluster://127.0.0.1:1", 2);
    EXPECT_EQ(Client::RoutingError::Transport, result.routing_error);
    ScriptedRedisPeer peer({ShardsTopology()}, {}, false, true);
    result = client.ConnectDetailed("redis+cluster://" + peer.endpoint(), 2);
    EXPECT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(Client::RoutingError::None, result.routing_error);
}

TEST_F(RedisHaKvProtocolTest, StartupSemanticErrorSurvivesUnreachableSeed) {
    using Client = ha::common::redis::RedisClusterClient;
    ScriptedRedisPeer peer({ShardsTopology(1)}, {}, false, true);
    Client client;
    const auto result = client.ConnectDetailed(
        "redis+cluster://" + peer.endpoint() + ",127.0.0.1:1", 2);
    EXPECT_EQ(Client::RoutingError::ReplicaCountMismatch, result.routing_error);
}

TEST_F(RedisHaKvProtocolTest,
       StartupCallersLogClassificationWithoutConnectionString) {
    for (bool coordinator : {false, true}) {
        ScriptedRedisPeer peer({ShardsTopology(1)}, {}, false, true);
        ::testing::internal::CaptureStderr();
        ErrorCode error;
        if (coordinator) {
            ha::backends::redis::RedisLeaderCoordinator backend(
                {.type = ha::HABackendType::REDIS,
                 .connstring = "redis+cluster://" + peer.endpoint(),
                 .ha_redis_expected_replicas_per_shard = 2});
            error = backend.Connect();
        } else {
            RedisHaKvBackend backend({.replicas_required = 2});
            error = backend.Connect("redis+cluster://" + peer.endpoint());
        }
        const auto log = ::testing::internal::GetCapturedStderr();
        EXPECT_NE(ErrorCode::OK, error);
        EXPECT_NE(std::string::npos, log.find("replica-count-mismatch"));
        EXPECT_EQ(std::string::npos, log.find(peer.endpoint()));
    }
}

TEST_F(RedisHaKvProtocolTest, SlotsRejectsConflictingCrossRangeMembership) {
    using Client = ha::common::redis::RedisClusterClient;
    const auto first = Topology(2, "PORT", 0, 8191);
    std::vector<std::string> conflicts;
    // Same primary identity, different endpoint.
    conflicts.push_back(Topology(2, "6381", 8192, 16383));
    // Same endpoint, different primary identity.
    conflicts.push_back(Topology(2, "PORT", 8192, 16383, 1));
    auto changed = Topology(2, "PORT", 8192, 16383);
    changed.replace(changed.find("\r\na\r\n"), 5, "\r\nx\r\n");
    conflicts.push_back(changed);
    // Different primary shares a replica with the first shard.
    changed = Topology(2, "6381", 8192, 16383);
    changed.replace(changed.find("\r\np\r\n"), 5, "\r\nq\r\n");
    conflicts.push_back(changed);
    // A replica appears as a primary elsewhere.
    changed = Topology(2, "6381", 8192, 16383, 1);
    changed.replace(changed.find("\r\nq\r\n"), 5, "\r\na\r\n");
    conflicts.push_back(changed);
    // Same primary has a different, otherwise unique replica set.
    changed = Topology(2, "PORT", 8192, 16383, 1);
    changed.replace(changed.find("\r\nq\r\n"), 5, "\r\np\r\n");
    conflicts.push_back(changed);
    for (const auto& second : conflicts) {
        ScriptedRedisPeer peer({"*2\r\n" + first.substr(4) + second.substr(4)});
        Client client;
        const auto result =
            client.ConnectDetailed("redis+cluster://" + peer.endpoint(), 2);
        EXPECT_EQ(Client::RoutingError::InvalidTopology, result.routing_error);
        EXPECT_NE(ErrorCode::OK, result.error);
    }
}

TEST_F(RedisHaKvProtocolTest,
       SlotsAcceptsConsistentDisjointRangesForOnePrimary) {
    const auto topology = "*2\r\n" + Topology(2, "PORT", 0, 8191).substr(4) +
                          Topology(2, "PORT", 8192, 16383).substr(4);
    ScriptedRedisPeer peer(
        {topology, topology, "*1\r\n:1\r\n", ":2\r\n", topology});
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + peer.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/bar/key", "value").status);
}

TEST_F(RedisHaKvProtocolTest, ShardsIsPreferredAndConfirmsOnWriteConnection) {
    const auto topology = ShardsTopology();
    ScriptedRedisPeer peer(
        {topology, topology, "*1\r\n:1\r\n", ":2\r\n", topology}, {}, false,
        true);
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + peer.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/bar/key", "value").status);
    peer.Join();
    ASSERT_EQ(5u, peer.commands().size());
    for (int i : {0, 1, 4})
        EXPECT_EQ((std::vector<std::string>{"CLUSTER", "SHARDS"}),
                  peer.commands()[i]);
    EXPECT_EQ("EVAL", peer.commands()[2][0]);
    EXPECT_EQ("WAIT", peer.commands()[3][0]);
}

TEST_F(RedisHaKvProtocolTest, ShardsIncludesSlotlessAskTarget) {
    ScriptedRedisPeer target({"+OK\r\n", "*1\r\n:1\r\n", ":2\r\n"}, {}, false,
                             true);
    const auto port = target.endpoint().substr(target.endpoint().find(':') + 1);
    const auto topology = "*2\r\n" + ShardsTopology().substr(4) +
                          ShardsTopology(2, port, true, "q").substr(4);
    ScriptedRedisPeer seed(
        {topology, topology, "-ASK 5061 " + target.endpoint() + "\r\n",
         topology, topology},
        {}, false, true);
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + seed.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/bar/key", "value").status);
    target.Join();
    ASSERT_EQ(3u, target.commands().size());
    EXPECT_EQ("ASKING", target.commands()[0][0]);
    EXPECT_EQ("EVAL", target.commands()[1][0]);
    EXPECT_EQ("WAIT", target.commands()[2][0]);
}

TEST_F(RedisHaKvProtocolTest, ShardsErrorsDoNotFallBackToSlots) {
    const auto duplicate = "*2\r\n" + ShardsTopology().substr(4) +
                           ShardsTopology(2, "6381", true).substr(4);
    for (const auto& response :
         {std::string("-NOPERM no permissions\r\n"), std::string("*0\r\n"),
          Topology(), ShardsTopology(1), duplicate}) {
        // If the client wrongly falls back, this valid SLOTS reply would
        // make Connect succeed and record the extra command.
        ScriptedRedisPeer peer({response, Topology()}, {}, false, true);
        RedisHaKvBackend backend({.replicas_required = 2});
        EXPECT_NE(ErrorCode::OK,
                  backend.Connect("redis+cluster://" + peer.endpoint()));
        peer.Join();
        ASSERT_EQ(1u, peer.commands().size());
        EXPECT_EQ((std::vector<std::string>{"CLUSTER", "SHARDS"}),
                  peer.commands()[0]);
    }
}

TEST_F(RedisHaKvProtocolTest, UnsupportedShardsFallsBackAndCachesCapability) {
    ScriptedRedisPeer peer({Topology(), Topology(), "$-1\r\n"});
    ha::common::redis::RedisClusterClient client;
    ASSERT_EQ(ErrorCode::OK,
              client.Connect("redis+cluster://" + peer.endpoint(), 2));
    auto lease = client.Execute({"GET", "key"}, "key", false);
    ASSERT_TRUE(lease.reply);
    EXPECT_EQ(REDIS_REPLY_NIL, lease.reply->type);
    peer.Join();
    EXPECT_EQ(1u, peer.shards_probes());
    ASSERT_EQ(3u, peer.commands().size());
    EXPECT_EQ((std::vector<std::string>{"CLUSTER", "SLOTS"}),
              peer.commands()[0]);
}

TEST_F(RedisHaKvProtocolTest, CachedTopologyAvoidsRefreshUntilExpiry) {
    ASSERT_EQ(0, setenv("MC_REDIS_TOPOLOGY_REFRESH_MS", "100", 1));
    ScriptedRedisPeer peer(
        {Topology(), "$-1\r\n", "$-1\r\n", Topology(), "$-1\r\n"});
    ha::common::redis::RedisClusterClient client;
    ASSERT_EQ(ErrorCode::OK,
              client.Connect("redis+cluster://" + peer.endpoint(), 2));
    for (int i = 0; i < 2; ++i) {
        auto lease = client.Execute({"GET", "key"}, "key", false);
        ASSERT_TRUE(lease.reply);
        EXPECT_EQ(REDIS_REPLY_NIL, lease.reply->type);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    auto lease = client.Execute({"GET", "key"}, "key", false);
    ASSERT_TRUE(lease.reply);
    EXPECT_EQ(REDIS_REPLY_NIL, lease.reply->type);
    peer.Join();
    ASSERT_EQ(5u, peer.commands().size());
    EXPECT_EQ("CLUSTER", peer.commands()[3][0]);
}

TEST_F(RedisHaKvProtocolTest, RedirectFailuresAreClassifiedWithoutReplay) {
    using Client = ha::common::redis::RedisClusterClient;
    for (const auto& [reply, expected] :
         std::vector<std::pair<std::string, Client::RoutingError>>{
             {"-MOVED invalid nowhere\r\n",
              Client::RoutingError::MalformedRedirect},
             {"-ASK 1 127.0.0.1:1234\r\n",
              Client::RoutingError::RedirectSlotMismatch},
             {"-ASK 5061 127.0.0.1:1234\r\n",
              Client::RoutingError::RedirectLimit}}) {
        ASSERT_EQ(0, setenv("MC_REDIS_MAX_REDIRECTS", "0", 1));
        ScriptedRedisPeer peer({Topology(), Topology(), reply});
        Client client;
        ASSERT_EQ(ErrorCode::OK,
                  client.Connect("redis+cluster://" + peer.endpoint(), 2));
        auto lease = client.Execute({"SET", "{bar}", "v"}, "{bar}", true);
        EXPECT_EQ(expected, lease.routing_error);
        EXPECT_FALSE(lease.may_have_executed);
        EXPECT_NE(ErrorCode::OK, lease.error);
    }
}

TEST_F(RedisHaKvProtocolTest, SameCountDifferentReplicaIdentityIsUnknown) {
    auto changed = Topology();
    changed.replace(changed.find("\r\na\r\n"), 5, "\r\nx\r\n");
    ScriptedRedisPeer peer(
        {Topology(), Topology(), "*1\r\n:1\r\n", ":2\r\n", changed});
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + peer.endpoint()));
    EXPECT_EQ(KvWriteStatus::kOutcomeUnknown,
              backend.PutWithResult("/oplog/bar/key", "value").status);
}

TEST_F(RedisHaKvProtocolTest, FiveHopLimitIsBounded) {
    using Client = ha::common::redis::RedisClusterClient;
    {
        // Each ASK target is a known primary in the immutable topology.
        // Six distinct targets exceed five redirects.
        std::vector<std::unique_ptr<ScriptedRedisPeer>> targets;
        std::string next = "127.0.0.1:1";
        for (int i = 0; i < 5; ++i) {
            targets.push_back(
                std::make_unique<ScriptedRedisPeer>(std::vector<std::string>{
                    "+OK\r\n", "-ASK 5061 " + next + "\r\n"}));
            next = targets.back()->endpoint();
        }
        std::string topology =
            "*6\r\n" + Topology(2, "PORT", 0, 8191).substr(4);
        for (size_t i = 0; i < targets.size(); ++i) {
            const auto& endpoint = targets[i]->endpoint();
            topology +=
                Topology(2, endpoint.substr(endpoint.find(':') + 1), 8192 + i,
                         i == targets.size() - 1 ? 16383 : 8192 + i, i + 1)
                    .substr(4);
        }
        std::vector<std::string> replies{topology, topology,
                                         "-ASK 5061 " + next + "\r\n"};
        for (int i = 0; i < 5; ++i) replies.push_back(topology);
        ScriptedRedisPeer seed(replies);
        Client client;
        ASSERT_EQ(ErrorCode::OK,
                  client.Connect("redis+cluster://" + seed.endpoint(), 2));
        auto lease = client.Execute({"SET", "{bar}", "v"}, "{bar}", true);
        EXPECT_EQ(Client::RoutingError::RedirectLimit, lease.routing_error);
        EXPECT_FALSE(lease.may_have_executed);
        for (auto& target : targets) {
            target->Join();
            ASSERT_EQ(2u, target->commands().size());
        }
    }
}

TEST_F(RedisHaKvProtocolTest, MultiNodeRedirectLoopDoesNotReplay) {
    using Client = ha::common::redis::RedisClusterClient;
    ScriptedRedisPeer target({"+OK\r\n", "ASK_VALUE"});
    const auto port = target.endpoint().substr(target.endpoint().find(':') + 1);
    const auto topology = "*2\r\n" + Topology(2, "PORT", 0, 8191).substr(4) +
                          Topology(2, port, 8192, 16383, 1).substr(4);
    ScriptedRedisPeer seed({topology, topology,
                            "-ASK 5061 " + target.endpoint() + "\r\n", topology,
                            topology});
    Client client;
    ASSERT_EQ(ErrorCode::OK,
              client.Connect("redis+cluster://" + seed.endpoint(), 2));
    auto lease =
        client.Execute({"SET", "{bar}", seed.endpoint()}, "{bar}", true);
    EXPECT_EQ(Client::RoutingError::RedirectLoop, lease.routing_error);
    EXPECT_FALSE(lease.may_have_executed);
    seed.Join();
    target.Join();
    EXPECT_EQ(5u, seed.commands().size());
    EXPECT_EQ(2u, target.commands().size());
}

TEST_F(RedisHaKvProtocolTest, MovedRoutesOverActualIpv6Connection) {
    ScriptedRedisPeer target({"*1\r\n:1\r\n", ":2\r\n"}, {}, true);
    auto topology = Topology(2, target.endpoint().substr(6));
    topology.replace(topology.find("$9\r\n127.0.0.1"), 13, "$3\r\n::1");
    // Redis may emit the redirect as an unbracketed IPv6 literal.
    ScriptedRedisPeer seed(
        {Topology(), Topology(),
         "-MOVED 5061 ::1:" + target.endpoint().substr(6) + "\r\n", topology,
         topology});
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + seed.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/bar/key", "value").status);
    target.Join();
    ASSERT_EQ(2u, target.commands().size());
    EXPECT_EQ("WAIT", target.commands()[1][0]);
}

TEST_F(RedisHaKvProtocolTest, AskSupportsPrimaryWithoutOwnedSlots) {
    ScriptedRedisPeer target(
        {"NODES", "+OK\r\n", "*1\r\n:1\r\n", ":2\r\n", "NODES"});
    ScriptedRedisPeer seed({Topology(), Topology(),
                            "-ASK 5061 " + target.endpoint() + "\r\n",
                            Topology(), Topology()});
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + seed.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/bar/key", "value").status);
    target.Join();
    ASSERT_EQ(5u, target.commands().size());
    EXPECT_EQ((std::vector<std::string>{"CLUSTER", "NODES"}),
              target.commands()[0]);
    EXPECT_EQ("ASKING", target.commands()[1][0]);
    EXPECT_EQ("EVAL", target.commands()[2][0]);
    EXPECT_EQ("WAIT", target.commands()[3][0]);
}

TEST_F(RedisHaKvProtocolTest,
       MatchingOwnerCannotReplaceMissingAcknowledgments) {
    ScriptedRedisPeer peer({Topology(), Topology(), "*2\r\n:1\r\n:7\r\n",
                            ":1\r\n", Topology(), Topology(), "OWNER"});
    using Coordinator = ha::backends::redis::RedisLeaderCoordinator;
    Coordinator coordinator({.type = ha::HABackendType::REDIS,
                             .connstring = "redis+cluster://" + peer.endpoint(),
                             .cluster_namespace = "clusterA",
                             .ha_redis_expected_replicas_per_shard = 2});
    ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
    EXPECT_FALSE(coordinator.TryAcquireLeadership("127.0.0.1:50051"));
    EXPECT_EQ(Coordinator::OwnershipReconciliation::MatchingUnconfirmed,
              coordinator.LastOwnershipReconciliation());
}

TEST_F(RedisHaKvProtocolTest, HaFactoryRequiresExplicitReplicaPolicy) {
    auto backend = CreateHaKvBackend(
        {.type = ha::HABackendType::REDIS, .connstring = "127.0.0.1:1"});
    ASSERT_FALSE(backend);
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, backend.error());
}

TEST_F(RedisHaKvProtocolTest, ClusterUsesConfiguredShardReplicaCount) {
    for (int ack : {1, 2}) {
        ScriptedRedisPeer peer({Topology(), Topology(), "*1\r\n:1\r\n",
                                ":" + std::to_string(ack) + "\r\n",
                                Topology()});
        RedisHaKvBackend backend({.replicas_required = 2});
        ASSERT_EQ(ErrorCode::OK,
                  backend.Connect("redis+cluster://" + peer.endpoint()));
        const auto result =
            backend.PutWithResult("/oplog/clusterA/key", "value");
        EXPECT_EQ(ack == 2 ? KvWriteStatus::kCommitted
                           : KvWriteStatus::kOutcomeUnknown,
                  result.status);
        peer.Join();
        ASSERT_EQ(5u, peer.commands().size());
        EXPECT_EQ("EVAL", peer.commands()[2][0]);
        EXPECT_EQ((std::vector<std::string>{"WAIT", "2", "1000"}),
                  peer.commands()[3]);
    }
}

TEST_F(RedisHaKvProtocolTest, ClusterRejectsMissingReplicasAndIncompleteSlots) {
    for (const auto& topology :
         {Topology(1), Topology(2, "PORT", 1), Topology(2, "PORT", 0, 16382),
          std::string("*0\r\n")}) {
        ScriptedRedisPeer peer({topology, topology});
        RedisHaKvBackend backend({.replicas_required = 2});
        EXPECT_NE(ErrorCode::OK,
                  backend.Connect("redis+cluster://" + peer.endpoint()));
        peer.Join();
        for (const auto& command : peer.commands())
            EXPECT_EQ((std::vector<std::string>{"CLUSTER", "SLOTS"}), command);
    }
}

TEST_F(RedisHaKvProtocolTest,
       ClusterRejectsOverlappingSlotsAndMissingNodeIdentity) {
    const auto overlap = "*2\r\n" + Topology(2, "PORT", 0, 8192).substr(4) +
                         Topology(2, "PORT", 8192, 16383).substr(4);
    auto missing_id = Topology();
    missing_id.replace(missing_id.find("$1\r\np\r\n"), 7, "$0\r\n\r\n");
    for (const auto& topology : {overlap, missing_id}) {
        ScriptedRedisPeer peer({topology, topology});
        RedisHaKvBackend backend({.replicas_required = 2});
        EXPECT_NE(ErrorCode::OK,
                  backend.Connect("redis+cluster://" + peer.endpoint()));
    }
}

TEST_F(RedisHaKvProtocolTest, DiscoveryOnlyClientCannotWrite) {
    ScriptedRedisPeer peer({Topology(), Topology(), "$5\r\nvalue\r\n"});
    ha::common::redis::RedisClusterClient client;
    ASSERT_EQ(ErrorCode::OK,
              client.Connect("redis+cluster://" + peer.endpoint(), 1, true));
    {
        auto rejected =
            client.Execute({"SET", "{bar}", "value"}, "{bar}", true);
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, rejected.error);
        EXPECT_FALSE(rejected.may_have_executed);
    }
    auto read = client.Execute({"GET", "{bar}"}, "{bar}", false);
    ASSERT_NE(nullptr, read.reply);
    EXPECT_EQ(REDIS_REPLY_STRING, read.reply->type);
    EXPECT_EQ("value", std::string(read.reply->str, read.reply->len));
}

TEST_F(RedisHaKvProtocolTest, ClusterMembershipChangeAfterWriteIsUnknown) {
    ScriptedRedisPeer peer({Topology(), Topology(), "*1\r\n:1\r\n", ":2\r\n",
                            Topology(1), Topology(1)});
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + peer.endpoint()));
    EXPECT_EQ(KvWriteStatus::kOutcomeUnknown,
              backend.PutWithResult("/oplog/clusterA/key", "value").status);
    peer.Join();
    size_t writes = 0;
    for (const auto& command : peer.commands()) writes += command[0] == "EVAL";
    EXPECT_EQ(1u, writes);
}

TEST_F(RedisHaKvProtocolTest, ClusterRedirectFailureIsBeforeExecution) {
    const auto slot =
        ha::common::redis::RedisClusterClient::HashSlot("{clusterA}");
    for (const auto& redirect :
         {"-MOVED " + std::to_string(slot) + " 127.0.0.1:PORT\r\n",
          std::string("-ASK 1 127.0.0.1:1234\r\n"),
          std::string("-MOVED invalid 127.0.0.1:1234\r\n")}) {
        ScriptedRedisPeer peer({Topology(), Topology(), redirect, Topology()});
        RedisHaKvBackend backend({.replicas_required = 2});
        ASSERT_EQ(ErrorCode::OK,
                  backend.Connect("redis+cluster://" + peer.endpoint()));
        EXPECT_EQ(KvWriteStatus::kFailedBeforeCommit,
                  backend.PutWithResult("/oplog/clusterA/key", "value").status);
    }
}

TEST_F(RedisHaKvProtocolTest, ClusterRejectsNonzeroDbAndCrossSlotLua) {
    EXPECT_EQ(0, ::setenv("MC_REDIS_DB_INDEX", "1", 1));
    RedisHaKvBackend backend;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              backend.Connect("redis+cluster://127.0.0.1:1"));
    EXPECT_EQ(0, ::unsetenv("MC_REDIS_DB_INDEX"));
    ScriptedRedisPeer peer({Topology()});
    ha::common::redis::RedisClusterClient client;
    ASSERT_EQ(ErrorCode::OK,
              client.Connect("redis+cluster://" + peer.endpoint(), 2));
    auto result =
        client.Execute({"EVAL", "return 1", "2", "{a}", "{b}"}, "{a}", true);
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error);
    EXPECT_FALSE(result.may_have_executed);
}

TEST_F(RedisHaKvProtocolTest, AskUsesAdjacentAskingAndWriteThenWaitOnTarget) {
    ScriptedRedisPeer target({"+OK\r\n", "*1\r\n:1\r\n", ":2\r\n"});
    const auto port = target.endpoint().substr(target.endpoint().find(':') + 1);
    const auto topology = "*2\r\n" + Topology(2, "PORT", 0, 8191).substr(4) +
                          Topology(2, port, 8192, 16383, 1).substr(4);
    ScriptedRedisPeer seed({topology, topology,
                            "-ASK 5061 " + target.endpoint() + "\r\n", topology,
                            topology});
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + seed.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/bar/key", "value").status);
    target.Join();
    ASSERT_EQ(3u, target.commands().size());
    EXPECT_EQ((std::vector<std::string>{"ASKING"}), target.commands()[0]);
    EXPECT_EQ("EVAL", target.commands()[1][0]);
    EXPECT_EQ((std::vector<std::string>{"WAIT", "2", "1000"}),
              target.commands()[2]);
}

TEST_F(RedisHaKvProtocolTest,
       MovedRefreshesOwnerBeforeRetryingUnexecutedWrite) {
    ScriptedRedisPeer target({"*1\r\n:1\r\n", ":2\r\n"});
    const auto port = target.endpoint().substr(target.endpoint().find(':') + 1);
    ScriptedRedisPeer seed({Topology(), Topology(),
                            "-MOVED 5061 " + target.endpoint() + "\r\n",
                            Topology(2, port), Topology(2, port)});
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + seed.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/bar/key", "value").status);
    target.Join();
    ASSERT_EQ(2u, target.commands().size());
    EXPECT_EQ("EVAL", target.commands()[0][0]);
    EXPECT_EQ("WAIT", target.commands()[1][0]);
}

TEST_F(RedisHaKvProtocolTest, ClusterAcceptsSecondSeedWhenFirstIsUnavailable) {
    ScriptedRedisPeer peer({Topology()});
    RedisHaKvBackend backend({.replicas_required = 2});
    EXPECT_EQ(ErrorCode::OK, backend.Connect("redis+cluster://127.0.0.1:1," +
                                             peer.endpoint()));
}

TEST_F(RedisHaKvProtocolTest,
       RefreshCanUseDiscoveredPrimaryAfterSeedDisconnects) {
    ScriptedRedisPeer target(
        {Topology(), "*1\r\n:1\r\n", ":2\r\n", Topology()});
    const auto port = target.endpoint().substr(target.endpoint().find(':') + 1);
    ScriptedRedisPeer seed({Topology(2, port), ""});
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK,
              backend.Connect("redis+cluster://" + seed.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/bar/key", "value").status);
}

TEST_F(RedisHaKvProtocolTest,
       ClusterLeaderAcquisitionRequiresAllAcknowledgments) {
    for (int ack : {1, 2}) {
        ScriptedRedisPeer peer({Topology(), Topology(), "*2\r\n:1\r\n:7\r\n",
                                ":" + std::to_string(ack) + "\r\n", Topology(),
                                Topology(), "$-1\r\n"});
        ha::backends::redis::RedisLeaderCoordinator coordinator(
            {.type = ha::HABackendType::REDIS,
             .connstring = "redis+cluster://" + peer.endpoint(),
             .cluster_namespace = "clusterA",
             .ha_redis_expected_replicas_per_shard = 2});
        ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
        auto acquired = coordinator.TryAcquireLeadership("127.0.0.1:50051");
        EXPECT_EQ(ack == 2, acquired.has_value());
        if (acquired)
            EXPECT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquired->status);
    }
}

TEST_F(RedisHaKvProtocolTest, RedisHashSlotsFollowHashTagRules) {
    using Client = ha::common::redis::RedisClusterClient;
    EXPECT_EQ(12739, Client::HashSlot("123456789"));
    EXPECT_EQ(12182, Client::HashSlot("foo"));
    EXPECT_EQ(5061, Client::HashSlot("bar"));
    EXPECT_EQ(Client::HashSlot("user1000"),
              Client::HashSlot("{user1000}.following"));
    EXPECT_EQ(Client::HashSlot("foo"), Client::HashSlot("a{foo}b{bar}"));
    EXPECT_EQ(Client::HashSlot("{foo"), Client::HashSlot("a{{foo}}"));
    EXPECT_NE(Client::HashSlot("foo"), Client::HashSlot("a{}b{foo}"));
}

TEST_F(RedisHaKvProtocolTest,
       MissingRenewalAcknowledgmentNotifiesLeadershipLoss) {
    ScriptedRedisPeer peer({"*2\r\n:1\r\n:7\r\n", ":2\r\n", ":1\r\n", ":2\r\n",
                            ":1\r\n", ":1\r\n", "$-1\r\n", ":1\r\n", ":2\r\n"});
    ha::backends::redis::RedisLeaderCoordinator coordinator(
        {.type = ha::HABackendType::REDIS,
         .connstring = peer.endpoint(),
         .cluster_namespace = "renewal",
         .ha_redis_expected_replicas_per_shard = 2});
    ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
    auto acquired = coordinator.TryAcquireLeadership("127.0.0.1:50051");
    ASSERT_TRUE(acquired.has_value());
    ASSERT_TRUE(acquired->session.has_value());
    auto renewed = coordinator.RenewLeadership(*acquired->session);
    ASSERT_TRUE(renewed.has_value());
    ASSERT_TRUE(*renewed);
    std::atomic<bool> lost{false};
    auto monitor = coordinator.StartLeadershipMonitor(
        *acquired->session,
        [&](ha::LeadershipLossReason) { lost.store(true); });
    ASSERT_TRUE(monitor.has_value());
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(4);
    while (!lost.load() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_TRUE(lost.load());
    EXPECT_EQ(ErrorCode::OK, coordinator.ReleaseLeadership(*acquired->session));
}

TEST_F(RedisHaKvProtocolTest, RealClusterKvAndLeadershipAcrossAllShards) {
    const auto* seeds = std::getenv("MC_TEST_REDIS_CLUSTER_SEEDS");
    if (!seeds)
        GTEST_SKIP()
            << "Set MC_TEST_REDIS_CLUSTER_SEEDS for the nine-node test";
    RedisHaKvBackend backend({.replicas_required = 2});
    ASSERT_EQ(ErrorCode::OK, backend.Connect(seeds));
    for (const auto& tag : {"b", "c", "a"}) {
        const auto cluster = std::string(tag);
        const auto key =
            "/oplog/" + cluster + "/task2-" + UuidToString(generate_uuid());
        const auto result = backend.PutWithResult(key, "value");
        ASSERT_EQ(KvWriteStatus::kCommitted, result.status);
        EXPECT_EQ(2u, result.replicas_acked);
        std::string value;
        ASSERT_EQ(ErrorCode::OK, backend.Get(key, value));
        EXPECT_EQ("value", value);
        KvTxn txn;
        txn.compares.push_back({.key = key,
                                .kind = KvCompareKind::kValueEquals,
                                .expected_value = "value"});
        txn.puts.push_back({.key = key, .value = "updated"});
        ASSERT_EQ(ErrorCode::OK, backend.Txn(txn));
        std::vector<KvPair> pairs;
        ASSERT_EQ(ErrorCode::OK, backend.Range(key, key + "0", 10, pairs));
        ASSERT_EQ(1u, pairs.size());
        EXPECT_EQ("updated", pairs[0].value);
        ASSERT_EQ(ErrorCode::OK, backend.DeleteRange(key, key + "0"));
        ha::backends::redis::RedisLeaderCoordinator coordinator(
            {.type = ha::HABackendType::REDIS,
             .connstring = seeds,
             .cluster_namespace = cluster,
             .ha_redis_expected_replicas_per_shard = 2});
        ASSERT_EQ(ErrorCode::OK, coordinator.Connect());
        const auto acquired =
            coordinator.TryAcquireLeadership("127.0.0.1:50051");
        ASSERT_TRUE(acquired.has_value());
        ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquired->status);
        ASSERT_TRUE(acquired->session.has_value());
        auto view = coordinator.ReadCurrentView();
        ASSERT_TRUE(view.has_value());
        ASSERT_TRUE(view->has_value());
        EXPECT_EQ("127.0.0.1:50051", (*view)->leader_address);
        const auto renewed = coordinator.RenewLeadership(*acquired->session);
        ASSERT_TRUE(renewed.has_value());
        EXPECT_TRUE(*renewed);
        EXPECT_EQ(ErrorCode::OK,
                  coordinator.ReleaseLeadership(*acquired->session));
    }
    RedisHaKvBackend mismatch({.replicas_required = 3});
    EXPECT_NE(ErrorCode::OK, mismatch.Connect(seeds));
}

TEST_F(RedisHaKvProtocolTest, RealClusterReshardingAndReplicaOnlySeed) {
    const auto* seeds = std::getenv("MC_TEST_REDIS_CLUSTER_SEEDS");
    if (!seeds)
        GTEST_SKIP() << "Requires the private nine-node Cluster fixture";
    using namespace ha::common::redis;
    std::string seed(seeds + std::string("redis+cluster://").size());
    seed = seed.substr(0, seed.find(','));
    auto control = ConnectRedis(seed);
    ASSERT_TRUE(control);
    RedisReplyPtr slots(static_cast<redisReply*>(
        redisCommand(control->get(), "CLUSTER SLOTS")));
    ASSERT_TRUE(slots);
    ASSERT_EQ(REDIS_REPLY_ARRAY, slots->type);
    struct Primary {
        std::string endpoint, id;
    };
    std::vector<Primary> primaries;
    std::string source_id, replica_seed;
    const auto endpoint = [](const redisReply* node) {
        return std::string(node->element[0]->str) + ":" +
               std::to_string(node->element[1]->integer);
    };
    for (size_t i = 0; i < slots->elements; ++i) {
        const auto* shard = slots->element[i];
        ASSERT_GE(shard->elements, 5u);
        primaries.push_back(
            {endpoint(shard->element[2]), shard->element[2]->element[2]->str});
        if (shard->element[0]->integer <= 5061 &&
            shard->element[1]->integer >= 5061) {
            source_id = primaries.back().id;
            replica_seed = endpoint(shard->element[3]);
        }
    }
    ASSERT_FALSE(source_id.empty());
    auto source =
        std::find_if(primaries.begin(), primaries.end(),
                     [&](const auto& p) { return p.id == source_id; });
    auto target =
        std::find_if(primaries.begin(), primaries.end(),
                     [&](const auto& p) { return p.id != source_id; });
    ASSERT_NE(primaries.end(), target);
    auto source_context = ConnectRedis(source->endpoint);
    auto target_context = ConnectRedis(target->endpoint);
    ASSERT_TRUE(source_context);
    ASSERT_TRUE(target_context);
    ASSERT_EQ(0, setenv("MC_REDIS_TOPOLOGY_REFRESH_MS", "60000", 1));
    RedisClusterClient client;
    ASSERT_EQ(ErrorCode::OK,
              client.Connect("redis+cluster://" + replica_seed, 2));
    const auto command = [](redisContext* context, const char* state,
                            const std::string& id) {
        RedisReplyPtr reply(static_cast<redisReply*>(redisCommand(
            context, "CLUSTER SETSLOT 5061 %s %s", state, id.c_str())));
        return reply && reply->type == REDIS_REPLY_STATUS;
    };
    ASSERT_TRUE(command(target_context->get(), "IMPORTING", source_id));
    ASSERT_TRUE(command(source_context->get(), "MIGRATING", target->id));
    const auto key = "{bar}:reshard:" + UuidToString(generate_uuid());
    {
        auto lease = client.Execute({"SET", key, "during-ask"}, key, true);
        ASSERT_TRUE(lease.reply);
        ASSERT_EQ(REDIS_REPLY_STATUS, lease.reply->type);
        EXPECT_EQ(target->endpoint, lease.endpoint);
        auto ack = client.Wait(lease, 2, std::chrono::milliseconds(1000));
        ASSERT_TRUE(ack);
        EXPECT_EQ(2u, *ack);
    }
    // No old keys occupy this slot in the private fixture at this point.
    for (const auto& primary : primaries) {
        auto context = ConnectRedis(primary.endpoint);
        ASSERT_TRUE(context);
        ASSERT_TRUE(command(context->get(), "NODE", target->id));
    }
    {
        auto lease = client.Execute({"GET", key}, key, false);
        ASSERT_TRUE(lease.reply);
        ASSERT_EQ(REDIS_REPLY_STRING, lease.reply->type);
        EXPECT_EQ("during-ask",
                  std::string(lease.reply->str, lease.reply->len));
        EXPECT_EQ(target->endpoint, lease.endpoint);
    }
    RedisReplyPtr removed(static_cast<redisReply*>(
        redisCommand(target_context->get(), "DEL %s", key.c_str())));
    ASSERT_TRUE(removed);
}

TEST_F(RedisHaKvProtocolTest, RealClusterReplicaPauseAndPrimaryFailover) {
    const auto* seeds = std::getenv("MC_TEST_REDIS_CLUSTER_SEEDS");
    if (!seeds)
        GTEST_SKIP() << "Requires the private nine-node Cluster fixture";
    std::string seed(seeds);
    seed.erase(0, std::string("redis+cluster://").size());
    seed = seed.substr(0, seed.find(','));
    auto control = ha::common::redis::ConnectRedis(seed);
    ASSERT_TRUE(control.has_value());
    ha::common::redis::RedisReplyPtr slots(static_cast<redisReply*>(
        redisCommand(control->get(), "CLUSTER SLOTS")));
    ASSERT_NE(nullptr, slots);
    ASSERT_EQ(REDIS_REPLY_ARRAY, slots->type);
    std::string replica_endpoint;
    for (size_t i = 0; i < slots->elements; ++i) {
        const auto* shard = slots->element[i];
        if (shard->element[0]->integer <= 5061 &&
            shard->element[1]->integer >= 5061) {
            ASSERT_GE(shard->elements, 5u);
            const auto* replica = shard->element[3];
            replica_endpoint = std::string(replica->element[0]->str,
                                           replica->element[0]->len) +
                               ":" +
                               std::to_string(replica->element[1]->integer);
        }
    }
    ASSERT_FALSE(replica_endpoint.empty());
    auto replica = ha::common::redis::ConnectRedis(replica_endpoint);
    ASSERT_TRUE(replica.has_value());
    RedisHaKvBackend backend(
        {.replicas_required = 2,
         .replica_ack_timeout = std::chrono::milliseconds(100)});
    ASSERT_EQ(ErrorCode::OK, backend.Connect(seeds));
    ha::common::redis::RedisReplyPtr paused(static_cast<redisReply*>(
        redisCommand(replica->get(), "CLIENT PAUSE 1500 ALL")));
    ASSERT_NE(nullptr, paused);
    const auto key = "/oplog/bar/fault-" + UuidToString(generate_uuid());
    const auto result = backend.PutWithResult(key, "unconfirmed");
    EXPECT_EQ(KvWriteStatus::kOutcomeUnknown, result.status);
    std::string value;
    EXPECT_EQ(ErrorCode::OK, backend.Get(key, value));
    EXPECT_EQ("unconfirmed", value);
    // The paused replica resumes automatically; request a controlled promotion.
    std::this_thread::sleep_for(std::chrono::milliseconds(1600));
    ha::common::redis::RedisReplyPtr failover(static_cast<redisReply*>(
        redisCommand(replica->get(), "CLUSTER FAILOVER")));
    ASSERT_NE(nullptr, failover);
    ASSERT_EQ(REDIS_REPLY_STATUS, failover->type);
    bool promoted = false;
    bool recovered = false;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline) {
        ha::common::redis::RedisReplyPtr role(
            static_cast<redisReply*>(redisCommand(replica->get(), "ROLE")));
        promoted = role && role->type == REDIS_REPLY_ARRAY &&
                   std::string(role->element[0]->str) == "master";
        if (promoted && backend.PutWithResult(key, "after-failover").status ==
                            KvWriteStatus::kCommitted) {
            recovered = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    EXPECT_TRUE(promoted);
    ASSERT_TRUE(recovered);
    EXPECT_EQ(ErrorCode::OK, backend.Get(key, value));
    EXPECT_EQ("after-failover", value);
    EXPECT_EQ(ErrorCode::OK, backend.DeleteRange(key, key + "0"));
}

TEST_F(RedisHaKvProtocolTest, EveryWriteWaitsOnTheExecutionConnection) {
    for (int operation = 0; operation < 3; ++operation) {
        ScriptedRedisPeer peer({"*1\r\n:1\r\n", ":2\r\n"});
        RedisHaKvBackend backend(
            {.replicas_required = 2,
             .replica_ack_timeout = std::chrono::milliseconds(40)});
        ASSERT_EQ(ErrorCode::OK, backend.Connect(peer.endpoint()));
        KvWriteResult result;
        const std::string key = "/oplog/clusterA/key";
        if (operation == 0) {
            result = backend.PutWithResult(key, std::string("a\0b", 3));
        } else if (operation == 1) {
            KvTxn txn;
            txn.puts.push_back({.key = key, .value = "value"});
            result = backend.TxnWithResult(txn);
        } else {
            result = backend.DeleteRangeWithResult(key, key + "0");
        }
        EXPECT_EQ(KvWriteStatus::kCommitted, result.status);
        EXPECT_EQ(2u, result.replicas_acked);
        peer.Join();
        ASSERT_EQ(2u, peer.commands().size());
        EXPECT_EQ("EVAL", peer.commands()[0][0]);
        EXPECT_EQ((std::vector<std::string>{"WAIT", "2", "40"}),
                  peer.commands()[1]);
    }
}

TEST_F(RedisHaKvProtocolTest, FactoryDefaultRequiresExactlyOneReplica) {
    for (size_t acked : {0u, 1u, 2u}) {
        for (int operation = 0; operation < 3; ++operation) {
            SCOPED_TRACE(::testing::Message()
                         << "acked=" << acked << " operation=" << operation);
            ScriptedRedisPeer peer(
                {"*1\r\n:1\r\n", ":" + std::to_string(acked) + "\r\n"});
            auto backend =
                CreateHaKvBackend({.type = ha::HABackendType::REDIS,
                                   .connstring = peer.endpoint(),
                                   .cluster_namespace = "clusterA",
                                   .ha_redis_expected_replicas_per_shard = 1});
            ASSERT_TRUE(backend.has_value());
            EXPECT_EQ(1u,
                      (*backend)->GetWriteDurabilityPolicy().replicas_required);
            const std::string key = "/oplog/clusterA/key";
            KvWriteResult result;
            if (operation == 0) {
                result = (*backend)->PutWithResult(key, "value");
            } else if (operation == 1) {
                KvTxn txn;
                txn.puts.push_back({.key = key, .value = "value"});
                result = (*backend)->TxnWithResult(txn);
            } else {
                result = (*backend)->DeleteRangeWithResult(key, key + "0");
            }
            EXPECT_EQ(acked == 1 ? KvWriteStatus::kCommitted
                                 : KvWriteStatus::kOutcomeUnknown,
                      result.status);
            EXPECT_EQ(acked, result.replicas_acked);
            EXPECT_EQ(1u, result.replicas_required);
            peer.Join();
            ASSERT_EQ(2u, peer.commands().size());
            EXPECT_EQ("EVAL", peer.commands()[0][0]);
            EXPECT_EQ((std::vector<std::string>{"WAIT", "1", "1000"}),
                      peer.commands()[1]);
        }
    }
}

TEST_F(RedisHaKvProtocolTest, FactoryLegacyWritesUseTheSameReplicaPolicy) {
    for (size_t acked : {0u, 1u, 2u}) {
        for (int operation = 0; operation < 3; ++operation) {
            SCOPED_TRACE(::testing::Message()
                         << "acked=" << acked << " operation=" << operation);
            ScriptedRedisPeer peer(
                {"*1\r\n:1\r\n", ":" + std::to_string(acked) + "\r\n"});
            auto backend =
                CreateHaKvBackend({.type = ha::HABackendType::REDIS,
                                   .connstring = peer.endpoint(),
                                   .cluster_namespace = "clusterA",
                                   .ha_redis_expected_replicas_per_shard = 1});
            ASSERT_TRUE(backend.has_value());
            const std::string key = "/oplog/clusterA/key";
            ErrorCode error;
            if (operation == 0) {
                error = (*backend)->Put(key, "value");
            } else if (operation == 1) {
                KvTxn txn;
                txn.puts.push_back({.key = key, .value = "value"});
                error = (*backend)->Txn(txn);
            } else {
                error = (*backend)->DeleteRange(key, key + "0");
            }
            EXPECT_EQ(
                acked == 1 ? ErrorCode::OK : ErrorCode::ETCD_OPERATION_ERROR,
                error);
            peer.Join();
            ASSERT_EQ(2u, peer.commands().size());
            EXPECT_EQ("EVAL", peer.commands()[0][0]);
            EXPECT_EQ((std::vector<std::string>{"WAIT", "1", "1000"}),
                      peer.commands()[1]);
        }
    }
}

TEST_F(RedisHaKvProtocolTest,
       ScriptRepliesDistinguishCompareRejectionAndUnknown) {
    const std::vector<std::pair<std::string, KvWriteStatus>> cases{
        {"*1\r\n:0\r\n", KvWriteStatus::kCompareFailed},
        {"*1\r\n:-1\r\n", KvWriteStatus::kRejected},
        {"*1\r\n:-2\r\n", KvWriteStatus::kFailedBeforeCommit},
        {"-MOVED 1 127.0.0.1:1234\r\n", KvWriteStatus::kFailedBeforeCommit},
        {"-ERR Error running script after a write\r\n",
         KvWriteStatus::kOutcomeUnknown},
        {":1\r\n", KvWriteStatus::kOutcomeUnknown},
        {"*2\r\n:1\r\n:1\r\n", KvWriteStatus::kOutcomeUnknown},
        {"*1\r\n$1\r\n1\r\n", KvWriteStatus::kOutcomeUnknown},
        {"*1\r\n:99\r\n", KvWriteStatus::kOutcomeUnknown},
        {"", KvWriteStatus::kOutcomeUnknown},
    };
    for (const auto& [response, expected] : cases) {
        ScriptedRedisPeer peer({response});
        RedisHaKvBackend backend;
        ASSERT_EQ(ErrorCode::OK, backend.Connect(peer.endpoint()));
        KvTxn txn;
        txn.compares.push_back({.key = "/oplog/clusterA/key",
                                .kind = KvCompareKind::kKeyNotExists,
                                .expected_value = ""});
        txn.puts.push_back({.key = "/oplog/clusterA/key", .value = "value"});
        EXPECT_EQ(expected, backend.TxnWithResult(txn).status);
        peer.Join();
        EXPECT_EQ(1u, peer.commands().size());
    }
}

TEST_F(RedisHaKvProtocolTest, BadOrLostWaitReplyIsUnknownWithoutReplayingLua) {
    for (const auto& response : {":0\r\n", ":-1\r\n", "$1\r\n1\r\n",
                                 "-ERR unsupported WAIT\r\n", ""}) {
        ScriptedRedisPeer peer({"*1\r\n:1\r\n", response});
        RedisHaKvBackend backend;
        ASSERT_EQ(ErrorCode::OK, backend.Connect(peer.endpoint()));
        const auto result =
            backend.PutWithResult("/oplog/clusterA/key", "value");
        EXPECT_EQ(KvWriteStatus::kOutcomeUnknown, result.status);
        EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.ToLegacyError());
        peer.Join();
        ASSERT_EQ(2u, peer.commands().size());
        EXPECT_EQ("EVAL", peer.commands()[0][0]);
        EXPECT_EQ("WAIT", peer.commands()[1][0]);
    }
}

TEST_F(RedisHaKvProtocolTest, LuaRetainsTheNormalCommandTimeout) {
    ScriptedRedisPeer peer({"*1\r\n:1\r\n", ":1\r\n"},
                           {std::chrono::milliseconds(1500)});
    RedisHaKvBackend backend(
        {.replicas_required = 1,
         .replica_ack_timeout = std::chrono::milliseconds(20)});
    ASSERT_EQ(ErrorCode::OK, backend.Connect(peer.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/clusterA/key", "value").status);
}

TEST_F(RedisHaKvProtocolTest, ReadsRetainNormalTimeoutAfterEachWriteOutcome) {
    const std::string key = "/oplog/clusterA/key";
    for (const auto& [script_reply, wait_reply, expected] :
         std::vector<std::tuple<std::string, std::string, KvWriteStatus>>{
             {"*1\r\n:1\r\n", ":1\r\n", KvWriteStatus::kCommitted},
             {"*1\r\n:1\r\n", ":0\r\n", KvWriteStatus::kOutcomeUnknown},
             {"*1\r\n:1\r\n", "-ERR WAIT failed\r\n",
              KvWriteStatus::kOutcomeUnknown},
             {"*1\r\n:0\r\n", "", KvWriteStatus::kCompareFailed},
         }) {
        for (const bool range : {false, true}) {
            SCOPED_TRACE(::testing::Message()
                         << "status=" << static_cast<int>(expected)
                         << " range=" << range);
            std::vector<std::string> replies{script_reply};
            if (!wait_reply.empty()) replies.push_back(wait_reply);
            std::vector<std::chrono::milliseconds> delays(replies.size());
            replies.push_back(range ? "*2\r\n$19\r\n/oplog/clusterA/key\r\n"
                                      "$5\r\nvalue\r\n"
                                    : "$5\r\nvalue\r\n");
            delays.push_back(std::chrono::milliseconds(1500));
            ScriptedRedisPeer peer(std::move(replies), std::move(delays));
            RedisHaKvBackend backend(
                {.replicas_required = 1,
                 .replica_ack_timeout = std::chrono::milliseconds(20)});
            ASSERT_EQ(ErrorCode::OK, backend.Connect(peer.endpoint()));
            EXPECT_EQ(expected, backend.PutWithResult(key, "value").status);
            if (range) {
                std::vector<KvPair> values;
                ASSERT_EQ(ErrorCode::OK,
                          backend.Range(key, key + "0", 1, values));
                ASSERT_EQ(1u, values.size());
                EXPECT_EQ(key, values[0].key);
                EXPECT_EQ("value", values[0].value);
            } else {
                std::string value;
                EXPECT_EQ(ErrorCode::OK, backend.Get(key, value));
                EXPECT_EQ("value", value);
            }
        }
    }
}

TEST_F(RedisHaKvProtocolTest, LongWaitCanExceedTheNormalCommandTimeout) {
    ScriptedRedisPeer peer(
        {"*1\r\n:1\r\n", ":1\r\n"},
        {std::chrono::milliseconds(0), std::chrono::milliseconds(3500)});
    RedisHaKvBackend backend(
        {.replicas_required = 1,
         .replica_ack_timeout = std::chrono::milliseconds(4000)});
    ASSERT_EQ(ErrorCode::OK, backend.Connect(peer.endpoint()));
    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("/oplog/clusterA/key", "value").status);
}

TEST_F(RedisHaKvProtocolTest, LongWaitDoesNotExtendTheNextCommandTimeout) {
    ScriptedRedisPeer peer(
        {"*1\r\n:1\r\n", ":1\r\n", "$5\r\nvalue\r\n"},
        {std::chrono::milliseconds(0), std::chrono::milliseconds(0),
         std::chrono::milliseconds(3500)});
    RedisHaKvBackend backend(
        {.replicas_required = 1,
         .replica_ack_timeout = std::chrono::milliseconds(4000)});
    ASSERT_EQ(ErrorCode::OK, backend.Connect(peer.endpoint()));
    const std::string key = "/oplog/clusterA/key";
    ASSERT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult(key, "value").status);
    std::string value;
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, backend.Get(key, value));
}

}  // namespace
}  // namespace mooncake::test
