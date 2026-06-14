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

// Hardware-free unit tests for ConnectPauseTracker (the active-connect
// circuit-breaker state). The tracker takes an injected clock, so the
// TTL/expiry/prune logic is exercised deterministically with no RDMA device
// and no real sleeps. The header is inline-only, so the test links just gtest.

#include "transport/rdma_transport/connect_pause_tracker.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using mooncake::ConnectPauseTracker;

namespace {

// Manually-advanced clock so expiry transitions are deterministic.
struct FakeClock {
    std::atomic<uint64_t> now{0};
    uint64_t operator()() const { return now.load(std::memory_order_relaxed); }
};

ConnectPauseTracker makeTracker(std::shared_ptr<FakeClock> clk) {
    return ConnectPauseTracker([clk] { return (*clk)(); });
}

TEST(ConnectPauseTracker, UnknownPeerNotPaused) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    EXPECT_FALSE(t.isPaused("10.0.0.1:1234"));
    EXPECT_EQ(t.size(), 0u);
}

TEST(ConnectPauseTracker, PausedUntilExpiry) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    const std::string peer = "10.0.0.1:1234";
    t.pause(peer, 1000);            // paused until ts == 1000
    EXPECT_TRUE(t.isPaused(peer));  // now == 0
    clk->now = 999;
    EXPECT_TRUE(t.isPaused(peer));   // just before expiry
    clk->now = 1000;                 // at expiry (>= until)
    EXPECT_FALSE(t.isPaused(peer));  // expired
    EXPECT_EQ(t.size(), 0u);         // and lazily deleted on the failing check
}

TEST(ConnectPauseTracker, RefreshExtendsWindow) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    const std::string peer = "p";
    t.pause(peer, 100);
    clk->now = 50;
    t.pause(peer, 200);  // re-arm while still paused -> extend
    clk->now = 150;
    EXPECT_TRUE(t.isPaused(peer));  // within the extended window
    clk->now = 200;
    EXPECT_FALSE(t.isPaused(peer));
}

TEST(ConnectPauseTracker, PruneDropsOnlyExpired) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    t.pause("a", 100);
    t.pause("b", 300);
    EXPECT_EQ(t.size(), 2u);
    clk->now = 200;  // "a" expired, "b" still paused
    t.prune();
    EXPECT_EQ(t.size(), 1u);
    EXPECT_FALSE(t.isPaused("a"));
    EXPECT_TRUE(t.isPaused("b"));
}

TEST(ConnectPauseTracker, PerPeerIndependent) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    t.pause("a", 100);
    t.pause("b", 1000);
    clk->now = 150;
    EXPECT_FALSE(t.isPaused("a"));  // a's window lapsed
    EXPECT_TRUE(t.isPaused("b"));   // b's has not
}

// Hammer all entry points concurrently; primarily a ThreadSanitizer target
// (run under -fsanitize=thread). The clock is pinned far in the future so
// entries don't expire mid-run, isolating the data-race check.
TEST(ConnectPauseTracker, ConcurrentAccessIsRaceFree) {
    auto clk = std::make_shared<FakeClock>();
    auto t = makeTracker(clk);
    constexpr int kIters = 5000;
    constexpr uint64_t kFarFuture = 1ull << 40;
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i)
        threads.emplace_back([&t, i] {
            std::string s = "peer" + std::to_string(i % 3);
            for (int k = 0; k < kIters; ++k) t.pause(s, kFarFuture);
        });
    for (int i = 0; i < 4; ++i)
        threads.emplace_back([&t] {
            for (int k = 0; k < kIters; ++k) (void)t.isPaused("peer0");
        });
    threads.emplace_back([&t] {
        for (int k = 0; k < kIters; ++k) t.prune();
    });
    for (auto& th : threads) th.join();
    SUCCEED();  // TSan asserts the absence of data races
}

}  // namespace
