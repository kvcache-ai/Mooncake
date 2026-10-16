// Tests for RpcDrainGuard: the teardown guard that keeps a shared RPC pool
// from being released under a suspended request coroutine (#3909).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
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

// A timed-out drain must leave the counters consistent: once the straggler
// exits, a fresh drain completes.
TEST(RpcDrainGuardTest, TimedOutDrainThenLateLeaveDrainsCleanly) {
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
    EXPECT_TRUE(guard.drain_for(std::chrono::seconds(1)));
}

// Teardown after a timed-out drain destroys the guard while a call is still
// in flight; the shared state keeps the late leave() safe. Run this under
// ASAN to give the test its teeth (#3943 review).
TEST(RpcDrainGuardTest, ScopedCallSurvivesGuard) {
    auto guard = std::make_unique<RpcDrainGuard>();
    auto call = std::make_unique<RpcDrainGuard::ScopedCall>(*guard);
    ASSERT_TRUE(call->ok());
    guard.reset();
    call.reset();
}

}  // namespace
}  // namespace mooncake
