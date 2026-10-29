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

// A peer restart invalidates every connection pooled for that peer, not just
// the one that happens to notice first. These tests pin what a failed pooled
// call does to the rest of the pool.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <mutex>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/rpc/rpc.h"

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;
using std::chrono::steady_clock;

constexpr int kEchoFunc = 5001;
constexpr int kBarrierFunc = 5002;
constexpr int kThrowFunc = 5003;
constexpr int kSideEffectThenThrowFunc = 5004;

// Counts how many times the peer actually ran the handler.
std::atomic<int> g_side_effects{0};

// How many connections the burst below parks in the pool. Two is what these
// tests need: enough to tell a whole-pool flush from a single-client discard,
// and low enough that the burst is genuinely concurrent on any offload pool.
constexpr int kPooled = 2;

std::atomic<int> g_in_flight{0};

class RpcReconnectTest : public ::testing::Test {
   protected:
    void SetUp() override {
        g_in_flight.store(0);
        g_side_effects.store(0);
    }

    void StartServer(uint16_t port) {
        server_ = std::make_unique<CoroRpcAgent>();
        ASSERT_TRUE(server_
                        ->registerFunction(kEchoFunc,
                                           [](const std::string_view& request,
                                              std::string& response) {
                                               response = std::string(request);
                                           })
                        .ok());
        // Parks until the whole burst has arrived, so the calls are genuinely
        // concurrent and cannot share one pooled connection.
        ASSERT_TRUE(
            server_
                ->registerFunction(
                    kBarrierFunc,
                    [](const std::string_view& request, std::string& response) {
                        g_in_flight.fetch_add(1);
                        auto deadline = steady_clock::now() + 3s;
                        while (g_in_flight.load() < kPooled &&
                               steady_clock::now() < deadline) {
                            std::this_thread::sleep_for(1ms);
                        }
                        response = std::string(request);
                    },
                    /*offload=*/true)
                .ok());
        // Runs a side effect and only then fails, standing in for the
        // control-plane handlers that mutate state before something goes
        // wrong on the way back.
        ASSERT_TRUE(
            server_
                ->registerFunction(kSideEffectThenThrowFunc,
                                   [](const std::string_view&, std::string&) {
                                       g_side_effects.fetch_add(1);
                                       throw std::runtime_error(
                                           "after the side effect");
                                   })
                .ok());
        // Fails on the server side, with the connection intact.
        ASSERT_TRUE(server_
                        ->registerFunction(
                            kThrowFunc,
                            [](const std::string_view&, std::string&) {
                                throw std::runtime_error("handler failed");
                            })
                        .ok());
        uint16_t requested = port;
        port_ = port;
        ASSERT_TRUE(server_->start(port_).ok());
        // start() takes the port by reference and picks a random one if the
        // bind fails. A silent change would move the pool key and make these
        // tests pass without exercising anything.
        if (requested != 0) ASSERT_EQ(port_, requested) << "port drifted";
        addr_ = "127.0.0.1:" + std::to_string(port_);
    }

    void StopServer() { server_.reset(); }

    // The server destructor joins its io threads, so the port is free when
    // this returns; the sleep lets the client sockets see the FIN first, so
    // "stale" is deterministic rather than racing the restart.
    void RestartServerSamePort() {
        StopServer();
        std::this_thread::sleep_for(100ms);
        g_in_flight.store(0);
        StartServer(port_);
    }

    // Parks kPooled concurrent calls in the server, so the client has to open
    // one connection each and pools all of them.
    void FillPool(CoroRpcAgent& client) {
        std::atomic<int> ok_count{0};
        std::vector<std::thread> burst;
        for (int i = 0; i < kPooled; ++i) {
            burst.emplace_back([&] {
                std::string response;
                if (client.call(addr_, kBarrierFunc, "warm", response).ok()) {
                    ok_count.fetch_add(1);
                }
            });
        }
        for (auto& t : burst) t.join();
        ASSERT_EQ(ok_count.load(), kPooled) << "burst did not fill the pool";
    }

    std::unique_ptr<CoroRpcAgent> server_;
    uint16_t port_ = 0;
    std::string addr_;
};

// The point of the change. After a restart every pooled connection is stale.
// The first call to use one fails - a socket only reports the peer's death
// when it is written to - but that single failure must take the rest of the
// pool with it, so the next call connects fresh and succeeds. Without the
// flush the caller pays one failed call per stale connection, and the retry
// budget above (TcpTransport: max_retry_count) runs out before the pool does.
TEST_F(RpcReconnectTest, StalePoolIsFlushedSoTheNextCallSucceeds) {
    StartServer(0);
    CoroRpcAgent client;
    ASSERT_NO_FATAL_FAILURE(FillPool(client));

    RestartServerSamePort();

    std::string response;
    auto first = client.call(addr_, kEchoFunc, "after-restart", response);
    EXPECT_FALSE(first.ok()) << "expected the stale connection to fail";

    response.clear();
    auto second = client.call(addr_, kEchoFunc, "after-restart", response);
    EXPECT_TRUE(second.ok()) << second.ToString();
    EXPECT_EQ(response, "after-restart");
}

// Guard, not a regression test: it passes with and without the flush, because
// a flushed pool simply reconnects. It is here to pin the behaviour a reviewer
// would ask about - a failure the peer replied with says nothing about the
// connection, and the pool is shared with every other caller of this address,
// so a throwing handler must not cost them their connections.
TEST_F(RpcReconnectTest, HandlerFailureLeavesThePoolAlone) {
    StartServer(0);
    CoroRpcAgent client;
    ASSERT_NO_FATAL_FAILURE(FillPool(client));

    std::string response;
    EXPECT_FALSE(client.call(addr_, kThrowFunc, "boom", response).ok());

    // The pooled connections are still good, and the server is still up.
    for (int i = 0; i < kPooled; ++i) {
        response.clear();
        auto status = client.call(addr_, kEchoFunc, "still-fine", response);
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(response, "still-fine");
    }
}

// Guard, not a regression test: a peer that is genuinely gone must keep
// failing, so the flush cannot turn an unreachable server into a false
// success or a retry storm.
TEST_F(RpcReconnectTest, DeadPeerStillFails) {
    StartServer(0);
    CoroRpcAgent client;
    ASSERT_NO_FATAL_FAILURE(FillPool(client));

    StopServer();

    std::string response;
    for (int i = 0; i < 3; ++i) {
        EXPECT_FALSE(client.call(addr_, kEchoFunc, "gone", response).ok());
    }
}

// A failed call is reported, not retried. Several RPCs share this path and
// mutate remote state - Pin acquires a stage buffer, Unpin releases one,
// Delegate starts a transfer, Notify runs a user callback - and an
// unsuccessful result does not tell us the peer skipped the handler: the
// error code for "never sent" and for "ran, but the reply was lost" is the
// same io_error. Retrying here would turn those into at-least-once.
TEST_F(RpcReconnectTest, AFailedCallIsNotRetried) {
    StartServer(0);
    CoroRpcAgent client;
    ASSERT_NO_FATAL_FAILURE(FillPool(client));

    std::string response;
    EXPECT_FALSE(
        client.call(addr_, kSideEffectThenThrowFunc, "once", response).ok());
    EXPECT_EQ(g_side_effects.load(), 1)
        << "the peer ran the handler " << g_side_effects.load() << " times";
}

// Several callers notice the same restart at once. Each flushes at most the
// generation it drew from, so the connection one caller has already
// re-established is not thrown away by another's later failure, and every
// caller recovers on its next attempt.
//
// Guard, not a regression test: what the generation changes is how many
// reconnects happen, and the agent exposes no connection count to assert on.
// What is checked here is the part that is observable - nobody is left
// permanently broken.
TEST_F(RpcReconnectTest, ConcurrentStaleCallsAllRecover) {
    StartServer(0);
    CoroRpcAgent client;
    ASSERT_NO_FATAL_FAILURE(FillPool(client));

    RestartServerSamePort();

    constexpr int kCallers = 4;
    auto round = [&](const char* payload) {
        std::atomic<int> ok_count{0};
        std::vector<std::thread> callers;
        for (int i = 0; i < kCallers; ++i) {
            callers.emplace_back([&] {
                std::string response;
                if (client.call(addr_, kEchoFunc, payload, response).ok() &&
                    response == payload) {
                    ok_count.fetch_add(1);
                }
            });
        }
        for (auto& t : callers) t.join();
        return ok_count.load();
    };

    // The first round draws the stale connections; some or all of it fails.
    (void)round("first");
    // By the second round every caller is on a fresh connection.
    EXPECT_EQ(round("second"), kCallers);
}

// A thread-local control client can outlive many services, each restarted on
// another port. The peer's FIN is only observed on the next RPC to that same
// address, so an unbounded address cache retains all of these idle sockets.
TEST(RpcPoolLifetimeTest, AddressChurnDoesNotAccumulateIdleSockets) {
#ifdef __linux__
    auto fdCount = [] {
        return std::distance(
            std::filesystem::directory_iterator("/proc/self/fd"),
            std::filesystem::directory_iterator{});
    };
    CoroRpcAgent client;
    auto churn = [&] {
        for (int i = 0; i < 256; ++i) {
            CoroRpcAgent server;
            ASSERT_TRUE(
                server
                    .registerFunction(kEchoFunc,
                                      [](const std::string_view& request,
                                         std::string& response) {
                                          response = std::string(request);
                                      })
                    .ok());
            uint16_t port = 0;
            ASSERT_TRUE(server.start(port).ok());
            std::string response;
            const auto address = "127.0.0.1:" + std::to_string(port);
            // As in getSegmentDesc(), a read may retry a stale pooled call.
            auto status = client.call(address, kEchoFunc, "echo", response);
            if (status.IsRpcServiceError())
                status = client.call(address, kEchoFunc, "echo", response);
            ASSERT_TRUE(status.ok()) << status.ToString();
            ASSERT_EQ(response, "echo");
        }
    };
    churn();
    ASSERT_FALSE(HasFatalFailure());
    const auto first = fdCount();
    churn();
    ASSERT_FALSE(HasFatalFailure());
    // Leave room for asynchronous close callbacks, but not another cache's
    // worth of old peer sockets. This checks resources, not map internals.
    const auto deadline = steady_clock::now() + 2s;
    while (fdCount() > first + 32 && steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    EXPECT_LE(fdCount(), first + 32);
#else
    GTEST_SKIP() << "File descriptor accounting requires /proc/self/fd";
#endif
}

TEST(RpcPoolLifetimeTest, EvictionPreservesConcurrentCalls) {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false, release = false;
    CoroRpcAgent server, client;
    ASSERT_TRUE(
        server
            .registerFunction(
                kBarrierFunc,
                [&](const std::string_view& request, std::string& response) {
                    std::unique_lock lock(mutex);
                    entered = true;
                    cv.notify_all();
                    if (!cv.wait_for(lock, 10s, [&] { return release; }))
                        throw std::runtime_error("eviction test gate timeout");
                    response = std::string(request);
                },
                true)
            .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port).ok());
    auto reply =
        std::make_shared<std::promise<std::pair<Status, std::string>>>();
    auto future = reply->get_future();
    const auto address = "127.0.0.1:" + std::to_string(port);
    client.callAsync(address, kBarrierFunc, "held",
                     [reply](auto status, auto body) {
                         reply->set_value({std::move(status), std::move(body)});
                     });
    {
        std::unique_lock lock(mutex);
        EXPECT_TRUE(cv.wait_for(lock, 3s, [&] { return entered; }));
    }
    // Shared agent: address lookup, LRU updates and late lease returns race
    // with other calls. More distinct live destinations than the cache can
    // retain force eviction of the held call's pool.
    std::atomic<int> successes{0};
    std::vector<std::thread> callers;
    for (int lane = 0; lane < 4; ++lane) {
        callers.emplace_back([&] {
            for (int i = 0; i < 80; ++i) {
                CoroRpcAgent peer;
                EXPECT_TRUE(
                    peer.registerFunction(kEchoFunc,
                                          [](const std::string_view& request,
                                             std::string& response) {
                                              response = std::string(request);
                                          })
                        .ok());
                uint16_t other_port = 0;
                auto started = peer.start(other_port);
                EXPECT_TRUE(started.ok()) << started.ToString();
                if (!started.ok()) break;
                std::string response;
                auto other = "127.0.0.1:" + std::to_string(other_port);
                auto status =
                    client.call(other, kEchoFunc, "parallel", response);
                if (status.IsRpcServiceError())
                    status =
                        client.call(other, kEchoFunc, "parallel", response);
                EXPECT_TRUE(status.ok()) << status.ToString();
                if (status.ok() && response == "parallel") ++successes;
            }
        });
    }
    for (auto& caller : callers) caller.join();
    {
        std::lock_guard lock(mutex);
        release = true;
    }
    cv.notify_all();
    ASSERT_EQ(future.wait_for(12s), std::future_status::ready);
    auto [status, response] = future.get();
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(response, "held");
    EXPECT_EQ(successes.load(), 320);
    response.clear();
    auto again = client.call(address, kBarrierFunc, "after-eviction", response);
    EXPECT_TRUE(again.ok()) << again.ToString();
    EXPECT_EQ(response, "after-eviction");
}

TEST(RpcPoolLifetimeTest, SameAgentCanStartAfterStop) {
    CoroRpcAgent server;
    ASSERT_TRUE(server
                    .registerFunction(kEchoFunc,
                                      [](const std::string_view& request,
                                         std::string& response) {
                                          response = std::string(request);
                                      })
                    .ok());
    uint16_t port = 0;
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(server.start(port).ok());
        CoroRpcAgent client;
        std::string response;
        ASSERT_TRUE(client
                        .call("127.0.0.1:" + std::to_string(port), kEchoFunc,
                              "restarted", response)
                        .ok());
        EXPECT_EQ(response, "restarted");
        ASSERT_TRUE(server.stop().ok());
        ASSERT_TRUE(server.stop().ok());
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
