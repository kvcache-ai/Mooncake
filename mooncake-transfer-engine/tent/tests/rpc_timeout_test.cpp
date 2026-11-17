// Copyright 2026 KVCache.AI
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

// Per-call RPC timeouts and the peer_unreachable classification. Loopback only.

#include <arpa/inet.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <async_simple/coro/SyncAwait.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "tent/rpc/rpc.h"

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

constexpr int kSlowFunc = 6001;
constexpr int kThrowFunc = 6002;
constexpr int kEchoFunc = 6003;
constexpr auto kHandlerDelay = 1500ms;

void registerEcho(CoroRpcAgent& server) {
    ASSERT_TRUE(server
                    .registerFunction(kEchoFunc,
                                      [](const std::string_view& request,
                                         std::string& response) {
                                          response = std::string(request);
                                      })
                    .ok());
}

RpcCallOptions flagging(bool* unreachable,
                        std::chrono::milliseconds timeout = -1ms) {
    RpcCallOptions options;
    options.timeout = timeout;
    options.peer_unreachable = unreachable;
    return options;
}

int listenLoopback(uint16_t* port, int backlog) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(addr);
    if (fd < 0 || bind(fd, reinterpret_cast<sockaddr*>(&addr), len) != 0 ||
        listen(fd, backlog) != 0 ||
        getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0)
        return -1;
    *port = ntohs(addr.sin_port);
    return fd;
}

class RpcTimeoutTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Inline: an offloaded handler would outlive a timed-out call and
        // delay the next test's handler on the shared blocking executor.
        auto slow = [](const std::string_view& request, std::string& response) {
            std::this_thread::sleep_for(kHandlerDelay);
            response = std::string(request);
        };
        auto fail = [](const std::string_view&, std::string&) {
            throw std::runtime_error("handler failed");
        };
        ASSERT_TRUE(server_.registerFunction(kSlowFunc, slow).ok());
        ASSERT_TRUE(server_.registerFunction(kThrowFunc, fail).ok());
        registerEcho(server_);
        uint16_t port = 0;
        ASSERT_TRUE(server_.start(port).ok());
        addr_ = "127.0.0.1:" + std::to_string(port);
    }

    CoroRpcAgent server_;
    CoroRpcAgent client_;
    std::string addr_;
};

TEST_F(RpcTimeoutTest, PerCallTimeoutFailsFastAndFlagsThePeer) {
    bool unreachable = false;
    const auto options = flagging(&unreachable, 300ms);

    std::string response;
    const auto started = Clock::now();
    const Status status =
        client_.call(addr_, kSlowFunc, "slow", response, options);
    const auto took = Clock::now() - started;
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsRpcServiceError()) << status.ToString();
    EXPECT_TRUE(unreachable);
    EXPECT_LT(took, 1200ms);
    std::this_thread::sleep_for(kHandlerDelay);
}

// Control: without a per-call timeout the same handler answers.
TEST_F(RpcTimeoutTest, DefaultTimeoutLetsASlowHandlerAnswer) {
    bool unreachable = false;
    const auto options = flagging(&unreachable);

    std::string response;
    const Status status =
        client_.call(addr_, kSlowFunc, "slow", response, options);
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(response, "slow");
    EXPECT_FALSE(unreachable);
}

TEST_F(RpcTimeoutTest, AHandlerErrorIsNotUnreachable) {
    bool unreachable = false;
    const auto options = flagging(&unreachable);

    std::string response;
    const Status status =
        client_.call(addr_, kThrowFunc, "boom", response, options);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(unreachable);
}

TEST_F(RpcTimeoutTest, APooledConnectionTimeoutIsUnreachable) {
    std::string response;
    ASSERT_TRUE(client_.call(addr_, kEchoFunc, "warm", response).ok());

    bool unreachable = false;
    const auto options = flagging(&unreachable, 300ms);
    const Status status =
        client_.call(addr_, kSlowFunc, "slow", response, options);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(unreachable);
    std::this_thread::sleep_for(kHandlerDelay);
}

TEST(RpcTimeoutPoolTest, AStalePooledConnectionIsNotUnreachable) {
    auto server = std::make_unique<CoroRpcAgent>();
    registerEcho(*server);
    uint16_t port = 0;
    ASSERT_TRUE(server->start(port).ok());
    const std::string addr = "127.0.0.1:" + std::to_string(port);

    CoroRpcAgent client;
    std::string response;
    ASSERT_TRUE(client.call(addr, kEchoFunc, "warm", response).ok());

    // The sleep lets the client socket see the FIN before the restart.
    server.reset();
    std::this_thread::sleep_for(100ms);
    server = std::make_unique<CoroRpcAgent>();
    registerEcho(*server);
    uint16_t same_port = port;
    ASSERT_TRUE(server->start(same_port).ok());
    ASSERT_EQ(same_port, port) << "port drifted";

    bool unreachable = false;
    const auto options = flagging(&unreachable);
    const Status stale =
        client.call(addr, kEchoFunc, "stale", response, options);
    EXPECT_FALSE(stale.ok()) << "expected the stale connection to fail";
    EXPECT_FALSE(unreachable) << stale.ToString();

    response.clear();
    const Status fresh = client.call(addr, kEchoFunc, "fresh", response);
    EXPECT_TRUE(fresh.ok()) << fresh.ToString();
    EXPECT_EQ(response, "fresh");
}

// The first attempt times out and the retry is answered: only the last attempt
// may mark the peer. ylt deals connections to its two io threads in turn, so
// the retry's fresh one is not stuck behind the sleeping handler.
TEST(RpcTimeoutPoolTest, RetryOnceWithOptionsDoesNotFlagAnAnsweringPeer) {
    std::atomic<int> calls{0};
    CoroRpcAgent server;
    ASSERT_TRUE(server
                    .registerFunction(kSlowFunc,
                                      [&calls](const std::string_view& request,
                                               std::string& response) {
                                          if (calls++ == 0)
                                              std::this_thread::sleep_for(
                                                  kHandlerDelay);
                                          response = std::string(request);
                                      })
                    .ok());
    registerEcho(server);
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port, false, 2).ok());
    const std::string addr = "127.0.0.1:" + std::to_string(port);

    CoroRpcAgent client;
    std::string response;
    ASSERT_TRUE(client.call(addr, kEchoFunc, "warm", response).ok());

    bool unreachable = false;
    const auto started = Clock::now();
    auto [status, echoed] = async_simple::coro::syncAwait(client.callCoroutine(
        addr, kSlowFunc, "retry", /*retry_rpc_error_once=*/true,
        flagging(&unreachable, 300ms)));
    const auto took = Clock::now() - started;
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(echoed, "retry");
    EXPECT_EQ(calls.load(), 2);
    EXPECT_LT(took, 1200ms);
    EXPECT_FALSE(unreachable);
}

// A full accept queue drops SYNs; the connect timeout bounds the dial.
TEST(RpcTimeoutPoolTest, PerCallConnectTimeoutBoundsAnUnansweredDial) {
    uint16_t port = 0;
    const int listener = listenLoopback(&port, 0);
    ASSERT_GE(listener, 0);
    const std::string addr = "127.0.0.1:" + std::to_string(port);
    sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = htons(port);

    std::vector<int> fillers;
    for (int i = 0; i < 8; ++i) {
        const int fd = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(fd, 0);
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
        (void)connect(fd, reinterpret_cast<sockaddr*>(&bind_addr),
                      sizeof(bind_addr));
        fillers.push_back(fd);
    }
    std::this_thread::sleep_for(100ms);

    CoroRpcAgent client;
    bool unreachable = false;
    const auto options = flagging(&unreachable, 300ms);
    std::string response;
    const auto started = Clock::now();
    const Status status = client.call(addr, kEchoFunc, "x", response, options);
    const auto took = Clock::now() - started;
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(unreachable);
    EXPECT_LT(took, 3s) << "dial was not bounded";

    for (int fd : fillers) close(fd);
    close(listener);
}

// The first dial times out on a full accept queue while a real server takes
// over the port, so the retry is answered: a dial that timed out on an earlier
// attempt must not leave the peer marked.
TEST(RpcTimeoutPoolTest, RetryOnceAfterAConnectTimeoutDoesNotFlagThePeer) {
    uint16_t port = 0;
    const int listener = listenLoopback(&port, 0);
    ASSERT_GE(listener, 0);
    sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = htons(port);

    std::vector<int> fillers;
    for (int i = 0; i < 8; ++i) {
        const int fd = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(fd, 0);
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
        (void)connect(fd, reinterpret_cast<sockaddr*>(&bind_addr),
                      sizeof(bind_addr));
        fillers.push_back(fd);
    }
    std::this_thread::sleep_for(100ms);

    CoroRpcAgent server;
    registerEcho(server);
    uint16_t same_port = port;
    bool started = false;
    std::thread swapper([&] {
        std::this_thread::sleep_for(100ms);
        for (int fd : fillers) close(fd);
        close(listener);
        started = server.start(same_port).ok();
    });
    CoroRpcAgent client;
    bool unreachable = false;
    const auto begin = Clock::now();
    auto [status, echoed] = async_simple::coro::syncAwait(client.callCoroutine(
        "127.0.0.1:" + std::to_string(port), kEchoFunc, "late",
        /*retry_rpc_error_once=*/true, flagging(&unreachable, 500ms)));
    const auto took = Clock::now() - begin;
    swapper.join();
    ASSERT_TRUE(started);
    ASSERT_EQ(same_port, port) << "port drifted";
    EXPECT_GE(took, 450ms) << "the first dial did not time out";
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(echoed, "late");
    EXPECT_FALSE(unreachable);
}

TEST(RpcTimeoutPoolTest, AFreshConnectionClosedWithoutAnswerIsNotUnreachable) {
    uint16_t port = 0;
    const int listener = listenLoopback(&port, 8);
    ASSERT_GE(listener, 0);
    std::thread closer([listener] {
        const int conn = accept(listener, nullptr, nullptr);
        if (conn >= 0) close(conn);
    });
    CoroRpcAgent client;
    bool unreachable = false;
    std::string response;
    const Status status =
        client.call("127.0.0.1:" + std::to_string(port), kEchoFunc, "x",
                    response, flagging(&unreachable, 2000ms));
    closer.join();
    close(listener);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.IsRpcConnectionError()) << status.ToString();
    EXPECT_FALSE(unreachable) << status.ToString();
}

// Nothing listens on port 1: a refused connect is not unreachable.
TEST_F(RpcTimeoutTest, AConnectRefusalIsNotUnreachable) {
    bool unreachable = false;
    const auto options = flagging(&unreachable);

    std::string response;
    const auto started = Clock::now();
    const Status status =
        client_.call("127.0.0.1:1", kSlowFunc, "x", response, options);
    EXPECT_TRUE(status.IsRpcConnectionError()) << status.ToString();
    EXPECT_FALSE(unreachable);
    EXPECT_LT(Clock::now() - started, 5s);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
