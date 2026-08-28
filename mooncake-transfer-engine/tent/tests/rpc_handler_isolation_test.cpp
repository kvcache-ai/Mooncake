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

// A slow RPC handler must not stall the rest of the control plane.
//
// The server runs one io_context (kRpcThreads == 1) and every connection's
// coroutine is scheduled on it, so an inline handler pins that thread for its
// whole duration -- for every peer, not just its own. onDelegate does exactly
// that: it runs a full transfer to completion. offload=true moves the handler
// off the io_context so the event loop keeps serving.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>

#include "tent/rpc/rpc.h"

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;

constexpr int kSlowFunc = 4001;
constexpr int kFastFunc = 4002;
constexpr int kThrowingFunc = 4003;

// Long enough that a serialized fast call is unambiguously distinguishable
// from a concurrent one, short enough to keep the test quick.
constexpr auto kSlowHandlerDuration = 800ms;
// A concurrent fast call only pays connect + round trip. Generous enough to
// survive a loaded CI box, still far below kSlowHandlerDuration.
constexpr auto kFastCallBudget = 400ms;

class RpcHandlerIsolationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        server_ = std::make_unique<CoroRpcAgent>();
        ASSERT_TRUE(
            server_
                ->registerFunction(
                    kSlowFunc,
                    [this](const std::string_view&, std::string& response) {
                        slow_running_.store(true);
                        std::this_thread::sleep_for(kSlowHandlerDuration);
                        slow_running_.store(false);
                        response = "slow";
                    },
                    /*offload=*/true)
                .ok());
        ASSERT_TRUE(
            server_
                ->registerFunction(
                    kFastFunc, [](const std::string_view&,
                                  std::string& response) { response = "fast"; })
                .ok());
        ASSERT_TRUE(server_
                        ->registerFunction(
                            kThrowingFunc,
                            [](const std::string_view&, std::string&) {
                                throw std::runtime_error("handler blew up");
                            },
                            /*offload=*/true)
                        .ok());
        uint16_t port = 0;
        ASSERT_TRUE(server_->start(port).ok());
        addr_ = "127.0.0.1:" + std::to_string(port);
    }

    void TearDown() override {
        if (server_) server_->stop();
    }

    std::unique_ptr<CoroRpcAgent> server_;
    std::string addr_;
    std::atomic<bool> slow_running_{false};
};

TEST_F(RpcHandlerIsolationTest, SlowOffloadedHandlerDoesNotStallOtherRpcs) {
    CoroRpcAgent slow_client;
    CoroRpcAgent fast_client;

    std::atomic<bool> slow_done{false};
    std::thread slow_thread([&] {
        std::string response;
        auto status = slow_client.call(addr_, kSlowFunc, "", response);
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(response, "slow");
        slow_done.store(true);
    });

    // Wait until the slow handler is actually executing, so the fast call
    // below genuinely overlaps it.
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!slow_running_.load() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE(slow_running_.load()) << "slow handler never started";

    const auto started = std::chrono::steady_clock::now();
    std::string response;
    auto status = fast_client.call(addr_, kFastFunc, "", response);
    const auto elapsed = std::chrono::steady_clock::now() - started;

    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(response, "fast");
    EXPECT_TRUE(slow_done.load() == false || slow_running_.load() == false);
    EXPECT_LT(elapsed, kFastCallBudget)
        << "fast RPC took "
        << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
               .count()
        << "ms: it waited for the slow handler instead of being served "
           "concurrently";

    slow_thread.join();
    EXPECT_TRUE(slow_done.load());
}

// Handlers left at the default offload=false keep running inline on the
// io_context thread, which is what every non-blocking handler wants: no thread
// hop, no extra latency.
TEST_F(RpcHandlerIsolationTest, InlineHandlerStillWorks) {
    CoroRpcAgent client;
    std::string response;
    ASSERT_TRUE(client.call(addr_, kFastFunc, "", response).ok());
    EXPECT_EQ(response, "fast");
}

TEST_F(RpcHandlerIsolationTest, OffloadedHandlerReturnsItsResponse) {
    CoroRpcAgent client;
    std::string response;
    ASSERT_TRUE(client.call(addr_, kSlowFunc, "", response).ok());
    EXPECT_EQ(response, "slow");
}

// Handlers do throw -- onDelegate starts with json::parse. The executor
// reports that through a Try rather than by unwinding, so it has to be
// rethrown or the caller would see an empty success.
TEST_F(RpcHandlerIsolationTest, OffloadedHandlerExceptionReachesTheCaller) {
    CoroRpcAgent client;
    std::string response;
    EXPECT_FALSE(client.call(addr_, kThrowingFunc, "", response).ok());
}

TEST(ControlRpcCompatibility, LegacyWireIdsPreserved) {
    static_assert(GetSegmentDesc == 1);
    static_assert(BootstrapRdma == 2);
    static_assert(SendData == 3);
    static_assert(RecvData == 4);
    static_assert(Notify == 5);
    static_assert(Delegate == 6);
    static_assert(Pin == 7);
    static_assert(Unpin == 8);
    static_assert(Probe == 9);
    static_assert(SubscribeSegmentUpdate == 10);
    static_assert(NotifySegmentUpdated == 11);

    constexpr int kLegacyDelegate = 6;
    constexpr int kLegacyPin = 7;
    constexpr int kLegacyUnpin = 8;
    constexpr int kProbe = 9;

    CoroRpcAgent server;
    std::atomic<int> probe_calls{0};
    std::atomic<int> delegate_calls{0};
    std::atomic<int> pin_calls{0};
    std::atomic<int> unpin_calls{0};

    ASSERT_TRUE(
        server
            .registerFunction(Probe, [&](const std::string_view&,
                                         std::string&) { ++probe_calls; })
            .ok());
    ASSERT_TRUE(server
                    .registerFunction(
                        Delegate,
                        [&](const std::string_view&, std::string&) {
                            ++delegate_calls;
                        },
                        /*offload=*/true)
                    .ok());
    ASSERT_TRUE(server
                    .registerFunction(Pin, [&](const std::string_view&,
                                               std::string&) { ++pin_calls; })
                    .ok());
    ASSERT_TRUE(
        server
            .registerFunction(Unpin, [&](const std::string_view&,
                                         std::string&) { ++unpin_calls; })
            .ok());

    uint16_t port = 0;
    ASSERT_TRUE(server.start(port).ok());
    const std::string addr =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));

    CoroRpcAgent client;
    std::string response;
    ASSERT_TRUE(client.call(addr, kLegacyDelegate, "{}", response).ok());
    EXPECT_EQ(delegate_calls.load(), 1);
    EXPECT_EQ(probe_calls.load(), 0);

    ASSERT_TRUE(client.call(addr, kLegacyPin, "\"remote\"", response).ok());
    EXPECT_EQ(pin_calls.load(), 1);
    EXPECT_EQ(delegate_calls.load(), 1);

    ASSERT_TRUE(client.call(addr, kLegacyUnpin, "1234", response).ok());
    EXPECT_EQ(unpin_calls.load(), 1);
    EXPECT_EQ(pin_calls.load(), 1);

    ASSERT_TRUE(client.call(addr, kProbe, "", response).ok());
    EXPECT_EQ(probe_calls.load(), 1);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
