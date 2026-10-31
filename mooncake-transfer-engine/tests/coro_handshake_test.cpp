// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common.h"
#include "error.h"
#include "handshake_rpc.h"
#include "plugin.h"
#include "rpc_config.h"
#include "transfer_metadata_plugin.h"

#include <fstream>
#include <sstream>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/struct_pack.hpp>
#include <ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp>

int HandshakeNotRegistered(int) { return 0; }

namespace mooncake {
int CallRawHandshakeForTest(HandShakePlugin *plugin, const std::string &host,
                            uint16_t port, int func_id,
                            const std::string &attachment,
                            std::string &response) {
    auto *coro = dynamic_cast<CoroHandShakePlugin *>(plugin);
    if (coro == nullptr) return ERR_INVALID_ARGUMENT;
    return coro->CallRaw(host, port, func_id, attachment, response);
}
}  // namespace mooncake

namespace {

struct ForceCoroTransport {
    ForceCoroTransport() {
        setenv("MC_HANDSHAKE_TRANSPORT", "coro", 1);
        setenv("MC_HANDSHAKE_MAX_LENGTH", "1048576", 0);
        setenv("MC_HANDSHAKE_MAX_CONCURRENCY", "2", 1);
        setenv("MC_HANDSHAKE_MAX_POOLS", "2", 1);
        setenv("MC_HANDSHAKE_MAX_INFLIGHT_BYTES", "1200000", 1);
        setenv("MC_HANDSHAKE_RPC_TIMEOUT_SECONDS", "5", 1);
        setenv("MC_HANDSHAKE_IDLE_TTL_SECONDS", "1", 1);
        setenv("MC_HANDSHAKE_CONNECT_TIMEOUT", "1", 1);
        setenv("MC_HANDSHAKE_RPC_THREADS", "2", 1);
    }
};

ForceCoroTransport g_force_coro_transport;

int ListenLoopback(uint16_t &port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) !=
        0) {
        close(fd);
        return -1;
    }
    sockaddr_in bound{};
    socklen_t length = sizeof(bound);
    if (getsockname(fd, reinterpret_cast<sockaddr *>(&bound), &length) != 0) {
        close(fd);
        return -1;
    }
    port = ntohs(bound.sin_port);
    return fd;
}

class CoroServer {
   public:
    void Start() {
        port_ = 0;
        fd_ = ListenLoopback(port_);
        ASSERT_GE(fd_, 0);
        plugin_ = mooncake::CreateCoroHandshakePlugin();
        plugin_->registerOnProbeCallBack(
            [](const Json::Value &, Json::Value &local) {
                local = Json::Value(Json::objectValue);
                return 0;
            });
        ASSERT_EQ(plugin_->startDaemon(port_, fd_), 0);
        fd_ = -1;
    }

    ~CoroServer() {
        plugin_.reset();
        if (fd_ >= 0) close(fd_);
    }

    mooncake::HandShakePlugin &plugin() { return *plugin_; }
    uint16_t port() const { return port_; }
    std::shared_ptr<mooncake::HandShakePlugin> shared() { return plugin_; }

   private:
    std::shared_ptr<mooncake::HandShakePlugin> plugin_;
    uint16_t port_ = 0;
    int fd_ = -1;
};

Json::Value Object() { return Json::Value(Json::objectValue); }

int CountEstablished(uint16_t port) {
    std::ifstream input("/proc/net/tcp");
    std::string line;
    std::getline(input, line);
    int count = 0;
    while (std::getline(input, line)) {
        std::istringstream fields(line);
        std::string index;
        std::string local;
        std::string remote;
        std::string state;
        fields >> index >> local >> remote >> state;
        if (state != "01") continue;
        auto port_of = [](const std::string &address) {
            const auto colon = address.rfind(':');
            if (colon == std::string::npos) return 0u;
            return static_cast<unsigned>(
                std::stoul(address.substr(colon + 1), nullptr, 16));
        };
        if (port_of(local) == port || port_of(remote) == port) ++count;
    }
    return count;
}

int CountThreads() {
    std::ifstream input("/proc/self/status");
    std::string key;
    int value = 0;
    while (input >> key) {
        if (key == "Threads:") {
            input >> value;
            return value;
        }
    }
    return 0;
}

}  // namespace

TEST(CoroHandshake, ProbeMetadataNotifyAndConnection) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &peer, Json::Value &local) {
            EXPECT_EQ(peer["name"].asString(), "remote");
            local["name"] = "local";
            return 0;
        });
    server.plugin().registerOnNotifyCallBack(
        [](const Json::Value &peer, Json::Value &local) {
            EXPECT_EQ(peer["notify_msg"].asString(), "ping");
            local["name"] = "srv";
            local["notify_msg"] = "pong";
            return 0;
        });
    server.plugin().registerOnConnectionCallBack(
        [](const Json::Value &peer, Json::Value &local) {
            EXPECT_EQ(peer["local_nic_path"].asString(), "nic0");
            local["reply_msg"] = "";
            local["peer_nic_path"] = "nic0";
            return 0;
        });

    Json::Value request = Object();
    Json::Value response;
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), request,
                                        response),
              0);

    request["name"] = "remote";
    EXPECT_EQ(server.plugin().exchangeMetadata("127.0.0.1", server.port(),
                                               request, response),
              0);
    EXPECT_EQ(response["name"].asString(), "local");

    request = Object();
    request["name"] = "cli";
    request["notify_msg"] = "ping";
    EXPECT_EQ(server.plugin().sendNotify("127.0.0.1", server.port(), request,
                                         response),
              0);
    EXPECT_EQ(response["notify_msg"].asString(), "pong");

    request = Object();
    request["local_nic_path"] = "nic0";
    EXPECT_EQ(
        server.plugin().send("127.0.0.1", server.port(), request, response), 0);
    EXPECT_EQ(response["peer_nic_path"].asString(), "nic0");
}

TEST(CoroHandshake, MetadataOffloadUsesASmallThreadPool) {
    const int before = CountThreads();
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &, Json::Value &local) {
            local = Object();
            return 0;
        });
    Json::Value response;
    ASSERT_EQ(server.plugin().exchangeMetadata("127.0.0.1", server.port(),
                                               Object(), response),
              0);
    const int after = CountThreads();
    // The offload pool is 2 threads. The rejected alternative adds one thread
    // per CPU on the first Connection or Metadata callback and keeps them.
    EXPECT_LT(after - before, 16);
    EXPECT_LT(after, 64);
}

TEST(CoroHandshake, ConnectionCallbackCanBeRegisteredAfterListen) {
    CoroServer server;
    server.Start();
    Json::Value response;
    EXPECT_NE(
        server.plugin().send("127.0.0.1", server.port(), Object(), response),
        0);
    server.plugin().registerOnConnectionCallBack(
        [](const Json::Value &, Json::Value &local) {
            local["reply_msg"] = "";
            return 0;
        });
    EXPECT_EQ(
        server.plugin().send("127.0.0.1", server.port(), Object(), response),
        0);
}

TEST(CoroHandshake, RepeatedStartRequiresTheSameEndpoint) {
    CoroServer server;
    server.Start();
    EXPECT_EQ(server.plugin().startDaemon(server.port(), -1), 0);
    uint16_t other = 0;
    int fd = ListenLoopback(other);
    ASSERT_GE(fd, 0);
    EXPECT_EQ(server.plugin().startDaemon(other, fd), ERR_INVALID_ARGUMENT);
    close(fd);
}

TEST(CoroHandshake, IllegalTransportDoesNotReturnNull) {
    setenv("MC_HANDSHAKE_TRANSPORT", "quic", 1);
    auto plugin = mooncake::HandShakePlugin::Create("P2PHANDSHAKE");
    ASSERT_NE(plugin, nullptr);
    EXPECT_EQ(plugin->startDaemon(1, -1), ERR_INVALID_ARGUMENT);
    Json::Value response;
    EXPECT_EQ(plugin->sendProbe("127.0.0.1", 1, Object(), response),
              ERR_INVALID_ARGUMENT);
    setenv("MC_HANDSHAKE_TRANSPORT", "coro", 1);
}

TEST(CoroHandshake, LocalOversizeDoesNotConnect) {
    uint16_t port = 0;
    int fd = ListenLoopback(port);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(listen(fd, 1), 0);
    std::atomic<bool> accepted{false};
    std::thread acceptor([&] {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(fd, &fds);
        timeval wait{};
        wait.tv_sec = 1;
        if (select(fd + 1, &fds, nullptr, nullptr, &wait) > 0) accepted = true;
    });
    auto client = mooncake::CreateCoroHandshakePlugin();
    std::string response;
    const std::string payload(mooncake::getHandshakeMaxLength() + 1, 'x');
    const auto started = std::chrono::steady_clock::now();
    EXPECT_EQ(mooncake::CallRawHandshakeForTest(client.get(), "127.0.0.1", port,
                                                1, payload, response),
              ERR_INVALID_ARGUMENT);
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::seconds(1));
    acceptor.join();
    EXPECT_FALSE(accepted.load());
    close(fd);
}

TEST(CoroHandshake, OversizedHeaderIsClosedBeforeTheBody) {
    CoroServer server;
    server.Start();
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(server.port());
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(
        connect(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)),
        0);

    coro_rpc::protocol::coro_rpc_protocol::req_header header{};
    header.magic = coro_rpc::protocol::coro_rpc_protocol::magic_number;
    header.version = 0;
    header.length = 0;
    header.attach_length =
        static_cast<uint32_t>(mooncake::getHandshakeMaxLength() + 1);
    std::string bytes;
    struct_pack::serialize_to<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        bytes, header);
    ASSERT_EQ(send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(bytes.size()));

    const auto started = std::chrono::steady_clock::now();
    char scratch;
    const ssize_t n = recv(fd, &scratch, 1, 0);
    EXPECT_LE(std::chrono::steady_clock::now() - started,
              std::chrono::seconds(2));
    EXPECT_LE(n, 0);
    close(fd);

    Json::Value response;
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        response),
              0);
}

TEST(CoroHandshake, BlockedConnectionDoesNotStallProbe) {
    CoroServer server;
    server.Start();
    std::promise<void> release;
    auto wait = release.get_future().share();
    std::atomic<int> entered{0};
    server.plugin().registerOnConnectionCallBack(
        [&](const Json::Value &, Json::Value &local) {
            entered.fetch_add(1);
            wait.wait();
            local = Object();
            return 0;
        });
    auto first = std::async(std::launch::async, [&] {
        Json::Value response;
        return server.plugin().send("127.0.0.1", server.port(), Object(),
                                    response);
    });
    auto second = std::async(std::launch::async, [&] {
        Json::Value response;
        return server.plugin().send("127.0.0.1", server.port(), Object(),
                                    response);
    });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (entered.load() < 2 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(entered.load(), 2);

    const auto probe_started = std::chrono::steady_clock::now();
    Json::Value response;
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        response),
              0);
    EXPECT_LT(std::chrono::steady_clock::now() - probe_started,
              std::chrono::milliseconds(1000));

    Json::Value ignored;
    EXPECT_EQ(
        server.plugin().send("127.0.0.1", server.port(), Object(), ignored),
        ERR_TOO_MANY_REQUESTS);
    release.set_value();
    EXPECT_EQ(first.get(), 0);
    EXPECT_EQ(second.get(), 0);
}

TEST(CoroHandshake, CallbackExceptionStaysInsideTheServer) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnConnectionCallBack(
        [](const Json::Value &, Json::Value &) -> int {
            throw std::runtime_error("boom");
        });
    Json::Value response;
    EXPECT_EQ(
        server.plugin().send("127.0.0.1", server.port(), Object(), response),
        ERR_METADATA);
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        response),
              0);
}

TEST(CoroHandshake, MalformedJsonIsRejected) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &, Json::Value &local) {
            local = Object();
            return 0;
        });
    std::string response;
    EXPECT_EQ(
        mooncake::CallRawHandshakeForTest(server.shared().get(), "127.0.0.1",
                                          server.port(), 1, "{", response),
        ERR_MALFORMED_JSON);
    Json::Value probe;
    EXPECT_EQ(
        server.plugin().sendProbe("127.0.0.1", server.port(), Object(), probe),
        0);
}

TEST(CoroHandshake, RestartDropsTheStalePooledConnection) {
    uint16_t port = 0;
    int fd = ListenLoopback(port);
    ASSERT_GE(fd, 0);
    auto server = mooncake::CreateCoroHandshakePlugin();
    server->registerOnProbeCallBack(
        [](const Json::Value &, Json::Value &local) {
            local = Object();
            return 0;
        });
    ASSERT_EQ(server->startDaemon(port, fd), 0);
    auto client = mooncake::CreateCoroHandshakePlugin();
    Json::Value response;
    ASSERT_EQ(client->sendProbe("127.0.0.1", port, Object(), response), 0);
    server.reset();

    fd = ListenLoopback(port);
    ASSERT_GE(fd, 0);
    server = mooncake::CreateCoroHandshakePlugin();
    server->registerOnProbeCallBack(
        [](const Json::Value &, Json::Value &local) {
            local = Object();
            return 0;
        });
    ASSERT_EQ(server->startDaemon(port, fd), 0);
    EXPECT_EQ(client->sendProbe("127.0.0.1", port, Object(), response), 0);
}

TEST(CoroHandshake, PoolLimitRejectsANewPeerWhileLeasesAreActive) {
    auto accept_and_hold = [](uint16_t *port, std::atomic<int> *ready) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        int on = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address));
        sockaddr_in bound{};
        socklen_t length = sizeof(bound);
        getsockname(fd, reinterpret_cast<sockaddr *>(&bound), &length);
        *port = ntohs(bound.sin_port);
        listen(fd, 4);
        ready->fetch_add(1);
        int conn = accept(fd, nullptr, nullptr);
        if (conn >= 0) {
            std::this_thread::sleep_for(std::chrono::seconds(3));
            close(conn);
        }
        close(fd);
    };
    uint16_t first_port = 0;
    uint16_t second_port = 0;
    std::atomic<int> ready{0};
    std::thread first(accept_and_hold, &first_port, &ready);
    std::thread second(accept_and_hold, &second_port, &ready);
    while (ready.load() < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    auto client = mooncake::CreateCoroHandshakePlugin();
    auto hold = [](mooncake::HandShakePlugin &plugin, uint16_t port) {
        Json::Value response;
        plugin.sendProbe("127.0.0.1", port, Object(), response);
    };
    auto left =
        std::async(std::launch::async, hold, std::ref(*client), first_port);
    auto right =
        std::async(std::launch::async, hold, std::ref(*client), second_port);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const auto started = std::chrono::steady_clock::now();
    Json::Value response;
    EXPECT_EQ(client->sendProbe("127.0.0.1", 1, Object(), response),
              ERR_TOO_MANY_REQUESTS);
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::milliseconds(500));
    left.wait();
    right.wait();
    first.join();
    second.join();
}

TEST(CoroHandshake, ByteBudgetRejectsASecondLargeFrame) {
    CoroServer server;
    server.Start();
    std::promise<void> release;
    auto wait = release.get_future().share();
    std::atomic<bool> entered{false};
    server.plugin().registerOnMetadataCallBack(
        [&](const Json::Value &, Json::Value &local) {
            entered.store(true);
            wait.wait();
            local = Object();
            return 0;
        });
    Json::Value body;
    body["blob"] = std::string(700000, 'a');
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    const std::string payload = Json::writeString(builder, body);
    auto first = std::async(std::launch::async, [&] {
        std::string response;
        return mooncake::CallRawHandshakeForTest(server.shared().get(),
                                                 "127.0.0.1", server.port(), 1,
                                                 payload, response);
    });
    for (int i = 0; i < 50 && !entered.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(entered.load());
    auto second = std::async(std::launch::async, [&] {
        std::string response;
        return mooncake::CallRawHandshakeForTest(server.shared().get(),
                                                 "127.0.0.1", server.port(), 1,
                                                 payload, response);
    });
    ASSERT_EQ(second.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(second.get(), ERR_SOCKET);
    release.set_value();
    EXPECT_EQ(first.get(), 0);
    std::string again;
    EXPECT_EQ(
        mooncake::CallRawHandshakeForTest(server.shared().get(), "127.0.0.1",
                                          server.port(), 1, payload, again),
        0);
    Json::Value probe;
    EXPECT_EQ(
        server.plugin().sendProbe("127.0.0.1", server.port(), Object(), probe),
        0);
}

TEST(CoroHandshake, UnknownRouteReleasesTheByteBudget) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &, Json::Value &local) {
            local = Object();
            return 0;
        });
    Json::Value body;
    body["blob"] = std::string(700000, 'b');
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    const std::string payload = Json::writeString(builder, body);
    coro_rpc::coro_rpc_client client;
    auto connected = async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(server.port())));
    ASSERT_FALSE(connected);
    client.set_req_attachment(payload);
    auto missed =
        async_simple::coro::syncAwait(client.call<HandshakeNotRegistered>(1));
    EXPECT_FALSE(missed.has_value());
    std::string response;
    EXPECT_EQ(
        mooncake::CallRawHandshakeForTest(server.shared().get(), "127.0.0.1",
                                          server.port(), 1, payload, response),
        0);
}

struct RestoreEnv {
    explicit RestoreEnv(const char *name, const char *value) : name_(name) {
        const char *current = std::getenv(name);
        if (current != nullptr) previous_ = current;
        setenv(name, value, 1);
    }
    ~RestoreEnv() { setenv(name_.c_str(), previous_.c_str(), 1); }
    std::string name_;
    std::string previous_;
};

TEST(CoroHandshake, ActiveHandshakesPerPeerStopAtTheRetainCap) {
    RestoreEnv concurrency("MC_HANDSHAKE_MAX_CONCURRENCY", "256");
    CoroServer server;
    server.Start();
    std::promise<void> release;
    auto wait = release.get_future().share();
    std::atomic<int> entered{0};
    server.plugin().registerOnMetadataCallBack(
        [&](const Json::Value &, Json::Value &local) {
            entered.fetch_add(1);
            wait.wait();
            local = Object();
            return 0;
        });
    constexpr int kCap = 128;
    std::atomic<int> started{0};
    std::vector<std::future<int>> calls;
    calls.reserve(kCap);
    for (int i = 0; i < kCap; ++i) {
        calls.push_back(std::async(std::launch::async, [&] {
            started.fetch_add(1);
            Json::Value response;
            return server.plugin().exchangeMetadata("127.0.0.1", server.port(),
                                                    Object(), response);
        }));
    }
    for (int i = 0; i < 200 && started.load() < kCap; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_EQ(started.load(), kCap);
    // Two offload threads run the callbacks. The other calls still hold a
    // connection while they wait for one of those threads.
    for (int i = 0; i < 50 && entered.load() < 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GE(entered.load(), 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto extra = std::async(std::launch::async, [&] {
        Json::Value response;
        return server.plugin().exchangeMetadata("127.0.0.1", server.port(),
                                                Object(), response);
    });
    ASSERT_EQ(extra.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(extra.get(), ERR_TOO_MANY_REQUESTS);
    release.set_value();
    for (auto &call : calls) EXPECT_EQ(call.get(), 0);
}

TEST(CoroHandshake, IdleCapClosesConnectionsAboveTheLimit) {
    RestoreEnv concurrency("MC_HANDSHAKE_MAX_CONCURRENCY", "16");
    RestoreEnv idle_cap("MC_HANDSHAKE_MAX_IDLE_PER_PEER", "2");
    CoroServer server;
    server.Start();
    std::atomic<int> entered{0};
    std::promise<void> release;
    auto wait = release.get_future().share();
    server.plugin().registerOnMetadataCallBack(
        [&](const Json::Value &, Json::Value &local) {
            entered.fetch_add(1);
            wait.wait();
            local = Object();
            return 0;
        });
    std::vector<std::future<int>> calls;
    for (int i = 0; i < 8; ++i) {
        calls.push_back(std::async(std::launch::async, [&] {
            Json::Value response;
            return server.plugin().exchangeMetadata("127.0.0.1", server.port(),
                                                    Object(), response);
        }));
    }
    for (int i = 0; i < 100 && entered.load() < 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    release.set_value();
    for (auto &call : calls) ASSERT_EQ(call.get(), 0);
    // Two kept connections, each with a client and a server socket.
    EXPECT_EQ(CountEstablished(server.port()), 4);
}

TEST(CoroHandshake, IdleConnectionsExpireWithoutAnotherCall) {
    CoroServer server;
    server.Start();
    Json::Value response;
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        response),
              0);
    EXPECT_EQ(CountEstablished(server.port()), 2);
    // TTL is 1s and the sweep runs about once a second. No further call.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    EXPECT_EQ(CountEstablished(server.port()), 0);
    EXPECT_EQ(server.plugin().sendProbe("127.0.0.1", server.port(), Object(),
                                        response),
              0);
}

TEST(CoroHandshake, LimitParserRejectsSignsAndJunk) {
    EXPECT_EQ(mooncake::ParseHandshakeUint64("1048576", 42), 1048576u);
    EXPECT_EQ(mooncake::ParseHandshakeUint64("-1", 42), 42u);
    EXPECT_EQ(mooncake::ParseHandshakeUint64("+1", 42), 1u);
    EXPECT_EQ(mooncake::ParseHandshakeUint64("12abc", 42), 42u);
    EXPECT_EQ(mooncake::ParseHandshakeUint64("0", 42), 42u);
    EXPECT_EQ(mooncake::ParseHandshakeUint64("18446744073709551616", 42), 42u);
    EXPECT_EQ(mooncake::ParseHandshakeUint64(nullptr, 42), 42u);
}

TEST(CoroHandshake, OversizedResponseIsNotReturned) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &, Json::Value &local) {
            local["blob"] = std::string(mooncake::getHandshakeMaxLength(), 'z');
            return 0;
        });
    Json::Value response;
    const int rc = server.plugin().exchangeMetadata("127.0.0.1", server.port(),
                                                    Object(), response);
    EXPECT_TRUE(rc == ERR_INVALID_ARGUMENT || rc == ERR_SOCKET);
    EXPECT_FALSE(response.isMember("blob"));
}

TEST(CoroHandshake, MetadataFailureIsAnRpcError) {
    CoroServer server;
    server.Start();
    server.plugin().registerOnMetadataCallBack(
        [](const Json::Value &, Json::Value &) { return -1; });
    Json::Value response;
    EXPECT_EQ(server.plugin().exchangeMetadata("127.0.0.1", server.port(),
                                               Object(), response),
              ERR_METADATA);
}

TEST(CoroHandshake, AdoptFailureLeavesTheCallerAbleToClose) {
    int fd = open("/dev/null", O_RDONLY);
    ASSERT_GE(fd, 0);
    auto plugin = mooncake::CreateCoroHandshakePlugin();
    EXPECT_NE(plugin->startDaemon(1, fd), 0);
    EXPECT_EQ(close(fd), -1);
    EXPECT_EQ(errno, EBADF);
}

TEST(CoroHandshake, UnresolvedHostIsDnsError) {
    auto client = mooncake::CreateCoroHandshakePlugin();
    Json::Value response;
    EXPECT_EQ(client->sendProbe("no-such-host.invalid", 1, Object(), response),
              ERR_DNS);
}

TEST(CoroHandshake, RefusedConnectStaysASocketError) {
    uint16_t port = 0;
    int fd = ListenLoopback(port);
    ASSERT_GE(fd, 0);
    auto client = mooncake::CreateCoroHandshakePlugin();
    Json::Value response;
    EXPECT_EQ(client->sendProbe("127.0.0.1", port, Object(), response),
              ERR_SOCKET);
    close(fd);
}
