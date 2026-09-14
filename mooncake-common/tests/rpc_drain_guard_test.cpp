// Tests for RpcDrainGuard: the teardown guard that keeps a shared RPC pool
// from being released under a suspended request coroutine (#3909).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "rpc_client_io_context.h"

namespace mooncake {
namespace {

TEST(RpcDrainGuardTest, DrainWaitsForInFlightAndStopsNewCalls) {
    RpcDrainGuard guard;

    auto slow_call = [&] {
        RpcDrainGuard::ScopedCall call(guard);
        if (!call.ok()) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        return true;
    };

    bool call_result = false;
    std::thread worker([&] { call_result = slow_call(); });
    // let the worker enter the guard before the drain starts
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    const auto started = std::chrono::steady_clock::now();
    EXPECT_TRUE(guard.drain_for(std::chrono::seconds(5)));
    const auto waited = std::chrono::steady_clock::now() - started;
    // the drain actually waited for the in-flight call, not just returned
    EXPECT_GE(waited, std::chrono::milliseconds(100));
    worker.join();
    EXPECT_TRUE(call_result);

    // once drained, new calls are refused
    EXPECT_FALSE(slow_call());
}

TEST(RpcDrainGuardTest, DrainWithNothingInFlightReturnsImmediately) {
    RpcDrainGuard guard;
    const auto started = std::chrono::steady_clock::now();
    EXPECT_TRUE(guard.drain_for(std::chrono::seconds(5)));
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::milliseconds(500));
}

TEST(RpcDrainGuardTest, DrainTimeoutReportsFalse) {
    RpcDrainGuard guard;
    std::atomic<bool> release{false};
    std::thread worker([&] {
        RpcDrainGuard::ScopedCall call(guard);
        while (!release.load()) std::this_thread::yield();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    EXPECT_FALSE(guard.drain_for(std::chrono::milliseconds(50)));

    release.store(true);
    worker.join();
}


// Timed-out drain must not reopen admission; callers that would free shared
// state on timeout (#3909 review) would still race with the in-flight call.
TEST(RpcDrainGuardTest, TimedOutDrainKeepsBlockingNewCalls) {
    RpcDrainGuard guard;

    std::atomic<bool> entered{false};
    std::thread worker([&] {
        RpcDrainGuard::ScopedCall call(guard);
        EXPECT_TRUE(call.ok());
        entered.store(true);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    });
    while (!entered.load()) {
        std::this_thread::yield();
    }

    EXPECT_FALSE(guard.drain_for(std::chrono::milliseconds(50)));
    {
        RpcDrainGuard::ScopedCall late(guard);
        EXPECT_FALSE(late.ok());
    }
    worker.join();
    EXPECT_TRUE(guard.drain_for(std::chrono::milliseconds(200)));
}

}  // namespace
}  // namespace mooncake
