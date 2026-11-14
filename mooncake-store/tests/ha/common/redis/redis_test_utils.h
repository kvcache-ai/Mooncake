#pragma once

#include <string>
#include <string_view>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <hiredis/hiredis.h>
#include <ylt/util/tl/expected.hpp>

#include "ha/common/redis/redis_connection.h"
#include "ha/ha_types.h"
#include "types.h"

namespace mooncake {
namespace testing {

using RedisContextPtr = ha::common::redis::RedisContextPtr;
using RedisReplyPtr = ha::common::redis::RedisReplyPtr;

inline tl::expected<RedisContextPtr, ErrorCode> ConnectRedisForTest(
    std::string_view endpoint) {
    return ha::common::redis::ConnectRedis(endpoint);
}

inline std::string BuildRedisScopedKey(
    const ha::ClusterNamespace& cluster_namespace, std::string_view suffix) {
    const auto hash_tag = ha::common::redis::SanitizeHashTagComponent(
        std::string(cluster_namespace));
    return "mooncake-store/{" + hash_tag + "}/" + std::string(suffix);
}

inline std::string BuildRedisMasterViewKey(
    const ha::ClusterNamespace& cluster_namespace) {
    return BuildRedisScopedKey(cluster_namespace, "master_view");
}

inline std::string BuildRedisSnapshotLatestKey(
    const ha::ClusterNamespace& cluster_namespace) {
    return BuildRedisScopedKey(cluster_namespace, "snapshot/latest");
}

inline std::string BuildRedisSnapshotIndexKey(
    const ha::ClusterNamespace& cluster_namespace) {
    return BuildRedisScopedKey(cluster_namespace, "snapshot/index");
}

// A scripted RESP peer exercises the real hiredis command/reply boundary.
// Only one connection is accepted, so EVAL and WAIT must share that socket.
class ScriptedRedisPeer {
   public:
    explicit ScriptedRedisPeer(
        std::vector<std::string> replies,
        std::vector<std::chrono::milliseconds> delays = {}, bool ipv6 = false,
        bool supports_shards = false)
        : replies_(std::move(replies)),
          delays_(std::move(delays)),
          supports_shards_(supports_shards) {
        listener_ = ::socket(ipv6 ? AF_INET6 : AF_INET, SOCK_STREAM, 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        sockaddr_in6 address6{};
        address6.sin6_family = AF_INET6;
        address6.sin6_addr = in6addr_loopback;
        auto* bind_address = ipv6 ? reinterpret_cast<sockaddr*>(&address6)
                                  : reinterpret_cast<sockaddr*>(&address);
        socklen_t length = ipv6 ? sizeof(address6) : sizeof(address);
        if (listener_ < 0 || ::bind(listener_, bind_address, length) != 0 ||
            ::listen(listener_, 1) != 0) {
            if (listener_ >= 0) ::close(listener_);
            throw std::runtime_error("Cannot create scripted Redis listener");
        }
        if (::getsockname(listener_, bind_address, &length) != 0) {
            ::close(listener_);
            throw std::runtime_error("Cannot read scripted Redis port");
        }
        const auto port = ntohs(ipv6 ? address6.sin6_port : address.sin_port);
        endpoint_ = (ipv6 ? "[::1]:" : "127.0.0.1:") + std::to_string(port);
        for (auto& reply : replies_) {
            size_t pos;
            while ((pos = reply.find(":PORT\r\n")) != std::string::npos)
                reply.replace(pos, 7, ":" + std::to_string(port) + "\r\n");
        }
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
    size_t shards_probes() const { return shards_probes_; }
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
        for (size_t index = 0; index < replies_.size();) {
            auto response = replies_[index];
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
            const bool legacy_probe =
                !supports_shards_ &&
                args == std::vector<std::string>{"CLUSTER", "SHARDS"};
            if (legacy_probe) {
                ++shards_probes_;
                response =
                    "-ERR unknown subcommand 'SHARDS'. Try CLUSTER HELP.\r\n";
            } else {
                commands_.push_back(std::move(args));
            }
            if (response == "ASK_VALUE") {
                response = "-ASK 5061 " + commands_.back().back() + "\r\n";
            }
            if (response == "OWNER") {
                for (const auto& previous : commands_) {
                    if (previous.size() > 2 && previous[0] == "EVAL") {
                        const auto& token = previous.back();
                        response = "$" + std::to_string(token.size()) + "\r\n" +
                                   token + "\r\n";
                        break;
                    }
                }
            }
            if (response == "NODES") {
                const auto nodes =
                    "p " + endpoint_ +
                    "@12345 myself,master - 0 0 1 connected\n"
                    "a 127.0.0.1:6000@16000 slave p 0 0 1 connected\n"
                    "b 127.0.0.1:6001@16001 slave p 0 0 1 connected\n";
                response = "$" + std::to_string(nodes.size()) + "\r\n" + nodes +
                           "\r\n";
            }
            if (response.empty()) break;  // Lose the reply after receiving it.
            if (!legacy_probe && index < delays_.size()) {
                std::this_thread::sleep_for(delays_[index]);
            }
            size_t sent = 0;
            while (sent < response.size()) {
                const auto count = ::send(client, response.data() + sent,
                                          response.size() - sent, MSG_NOSIGNAL);
                if (count <= 0) break;
                sent += static_cast<size_t>(count);
            }
            if (!legacy_probe) ++index;
        }
        redisReaderFree(reader);
        ::shutdown(listener_, SHUT_RDWR);
        client_.store(-1);
        ::close(client);
    }

    int listener_{-1};
    std::atomic<int> client_{-1};
    std::string endpoint_;
    std::vector<std::string> replies_;
    std::vector<std::chrono::milliseconds> delays_;
    std::vector<std::vector<std::string>> commands_;
    bool supports_shards_;
    size_t shards_probes_ = 0;
    std::thread worker_;
};

inline std::string Topology(int replicas = 2, const std::string& port = "PORT",
                            int first = 0, int last = 16383, int shard = 0) {
    std::string reply = "*1\r\n*" + std::to_string(3 + replicas) +
                        "\r\n:" + std::to_string(first) +
                        "\r\n:" + std::to_string(last) +
                        "\r\n*3\r\n$9\r\n127.0.0.1\r\n:" + port + "\r\n$1\r\n" +
                        std::string(1, 'p' + shard) + "\r\n";
    for (int i = 0; i < replicas; ++i) {
        reply += "*3\r\n$9\r\n127.0.0.1\r\n:" +
                 std::to_string(6000 + 100 * shard + i) + "\r\n$1\r\n" +
                 std::string(1, 'a' + 2 * shard + i) + "\r\n";
    }
    return reply;
}

inline std::string ShardsTopology(int replicas = 2,
                                  const std::string& port = "PORT",
                                  bool slotless = false,
                                  const std::string& id = "p") {
    const auto bulk = [](const std::string& s) {
        return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
    };
    std::string result = "*1\r\n*4\r\n" + bulk("slots") +
                         (slotless ? "*0\r\n" : "*2\r\n:0\r\n:16383\r\n") +
                         bulk("nodes") + "*" + std::to_string(1 + replicas) +
                         "\r\n";
    for (int i = 0; i <= replicas; ++i) {
        result +=
            "*10\r\n" + bulk("id") + bulk(id + std::to_string(i)) +
            bulk("endpoint") + bulk("127.0.0.1") + bulk("port") + ":" +
            (i == 0 ? port : std::to_string((id == "p" ? 6000 : 7000) + i)) +
            "\r\n" + bulk("role") + bulk(i == 0 ? "master" : "replica") +
            bulk("health") + bulk("online");
    }
    return result;
}

}  // namespace testing
}  // namespace mooncake
