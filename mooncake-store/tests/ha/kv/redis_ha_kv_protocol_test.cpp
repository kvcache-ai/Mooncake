#include "ha/kv/redis_ha_kv_backend.h"

#include <gtest/gtest.h>
#include <hiredis/hiredis.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <optional>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <utility>

#include "ha/kv/ha_kv_backend_factory.h"

namespace mooncake::test {
namespace {

class RedisHaKvProtocolTest : public ::testing::Test {
   protected:
    void SetUp() override {
        for (const auto* name :
             {"MC_REDIS_DB_INDEX", "MC_REDIS_PASSWORD", "MC_REDIS_USERNAME"}) {
            const auto* value = std::getenv(name);
            environment_.emplace_back(
                name, value ? std::optional<std::string>(value) : std::nullopt);
            ASSERT_EQ(0, ::unsetenv(name));
        }
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

// A scripted RESP peer exercises the real hiredis command/reply boundary.
// Only one connection is accepted, so EVAL and WAIT must share that socket.
class ScriptedRedisPeer {
   public:
    explicit ScriptedRedisPeer(
        std::vector<std::string> replies,
        std::vector<std::chrono::milliseconds> delays = {})
        : replies_(std::move(replies)), delays_(std::move(delays)) {
        listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (listener_ < 0 ||
            ::bind(listener_, reinterpret_cast<sockaddr*>(&address),
                   sizeof(address)) != 0 ||
            ::listen(listener_, 1) != 0) {
            if (listener_ >= 0) ::close(listener_);
            throw std::runtime_error("Cannot create scripted Redis listener");
        }
        socklen_t length = sizeof(address);
        if (::getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                          &length) != 0) {
            ::close(listener_);
            throw std::runtime_error("Cannot read scripted Redis port");
        }
        endpoint_ = "127.0.0.1:" + std::to_string(ntohs(address.sin_port));
        worker_ = std::thread([this] { Serve(); });
    }

    ~ScriptedRedisPeer() {
        ::shutdown(listener_, SHUT_RDWR);
        ::close(listener_);
        const int client = client_.load();
        if (client >= 0) ::shutdown(client, SHUT_RDWR);
        if (worker_.joinable()) worker_.join();
    }

    const std::string& endpoint() const { return endpoint_; }
    void Join() { worker_.join(); }
    const std::vector<std::vector<std::string>>& commands() const {
        return commands_;
    }

   private:
    void Serve() {
        const int client = ::accept(listener_, nullptr, nullptr);
        if (client < 0) return;
        client_.store(client);
        timeval timeout{2, 0};
        ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                     sizeof(timeout));
        auto* reader = redisReaderCreate();
        for (size_t index = 0; index < replies_.size(); ++index) {
            const auto& response = replies_[index];
            void* raw = nullptr;
            while (raw == nullptr) {
                if (redisReaderGetReply(reader, &raw) != REDIS_OK) break;
                if (raw != nullptr) break;
                char bytes[4096];
                const auto count = ::recv(client, bytes, sizeof(bytes), 0);
                if (count <= 0 ||
                    redisReaderFeed(reader, bytes,
                                    static_cast<size_t>(count)) != REDIS_OK)
                    break;
            }
            if (raw == nullptr) break;
            ha::common::redis::RedisReplyPtr command(
                static_cast<redisReply*>(raw));
            std::vector<std::string> args;
            if (command->type == REDIS_REPLY_ARRAY) {
                for (size_t i = 0; i < command->elements; ++i) {
                    const auto* arg = command->element[i];
                    if (arg->type == REDIS_REPLY_STRING) {
                        args.emplace_back(arg->str, arg->len);
                    }
                }
            }
            commands_.push_back(std::move(args));
            if (response.empty()) break;  // Lose the reply after receiving it.
            if (index < delays_.size()) {
                std::this_thread::sleep_for(delays_[index]);
            }
            size_t sent = 0;
            while (sent < response.size()) {
                const auto count = ::send(client, response.data() + sent,
                                          response.size() - sent, MSG_NOSIGNAL);
                if (count <= 0) break;
                sent += static_cast<size_t>(count);
            }
        }
        redisReaderFree(reader);
        client_.store(-1);
        ::close(client);
    }

    int listener_{-1};
    std::atomic<int> client_{-1};
    std::string endpoint_;
    std::vector<std::string> replies_;
    std::vector<std::chrono::milliseconds> delays_;
    std::vector<std::vector<std::string>> commands_;
    std::thread worker_;
};

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
            auto backend = CreateHaKvBackend({.type = ha::HABackendType::REDIS,
                                              .connstring = peer.endpoint(),
                                              .cluster_namespace = "clusterA"});
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
            auto backend = CreateHaKvBackend({.type = ha::HABackendType::REDIS,
                                              .connstring = peer.endpoint(),
                                              .cluster_namespace = "clusterA"});
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
