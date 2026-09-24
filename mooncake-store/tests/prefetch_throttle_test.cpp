// Unit tests for PrefetchThrottle: per-key dedup TTL, failed-key retry
// backoff, memory-pressure cooldown, and concurrent reserve uniqueness.

#include "prefetch_throttle.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace mooncake::test {
namespace {

std::vector<std::string> MakeKeys(const std::string& prefix, int n) {
    std::vector<std::string> keys;
    keys.reserve(n);
    for (int i = 0; i < n; ++i) {
        keys.push_back(prefix + "_" + std::to_string(i));
    }
    return keys;
}

}  // namespace

TEST(PrefetchThrottleTest, ReserveDedupsWithinTtl) {
    PrefetchThrottle throttle;
    throttle.configure(/*cooldown_sec=*/1, /*dedup_ttl_sec=*/60);

    auto first = throttle.reserve(MakeKeys("k", 4));
    EXPECT_EQ(first.size(), 4u);
    // Same keys inside the TTL window are suppressed.
    auto second = throttle.reserve(MakeKeys("k", 4));
    EXPECT_TRUE(second.empty());
}

TEST(PrefetchThrottleTest, ReserveAllowsNewKeysOnly) {
    PrefetchThrottle throttle;
    throttle.configure(1, 60);

    auto first = throttle.reserve({"a", "b"});
    EXPECT_EQ(first.size(), 2u);
    auto second = throttle.reserve({"a", "b", "c"});
    ASSERT_EQ(second.size(), 1u);
    EXPECT_EQ(second[0], "c");
}

TEST(PrefetchThrottleTest, ZeroTtlDisablesDedup) {
    PrefetchThrottle throttle;
    throttle.configure(1, 0);

    EXPECT_EQ(throttle.reserve({"a"}).size(), 1u);
    EXPECT_EQ(throttle.reserve({"a"}).size(), 1u);
}

TEST(PrefetchThrottleTest, FailedKeyRetriesAfterBackoffNotFullTtl) {
    PrefetchThrottle throttle;
    // cooldown (== failed retry backoff) 0s -> failed entries expire
    // immediately; dedup TTL stays large.
    throttle.configure(/*cooldown_sec=*/0, /*dedup_ttl_sec=*/3600);

    ASSERT_EQ(throttle.reserve({"a"}).size(), 1u);
    EXPECT_TRUE(throttle.reserve({"a"}).empty());
    throttle.markFailed("a");
    // With a 0 backoff the failed entry no longer blocks re-triggering.
    EXPECT_EQ(throttle.reserve({"a"}).size(), 1u);
}

TEST(PrefetchThrottleTest, CooldownBlocksUntilWindowExpires) {
    PrefetchThrottle throttle;
    throttle.configure(1, 60);

    EXPECT_FALSE(throttle.inCooldown());
    throttle.enterCooldown();
    EXPECT_TRUE(throttle.inCooldown());
}

TEST(PrefetchThrottleTest, ZeroCooldownDisablesBackoff) {
    PrefetchThrottle throttle;
    throttle.configure(0, 60);

    throttle.enterCooldown();
    EXPECT_FALSE(throttle.inCooldown());
}

TEST(PrefetchThrottleTest, CompletionLifecycle) {
    PrefetchThrottle throttle;
    throttle.configure(1, 60);

    ASSERT_EQ(throttle.reserve({"a"}).size(), 1u);
    EXPECT_EQ(throttle.stateOf("a"), PrefetchThrottle::State::kTriggered);
    EXPECT_GE(throttle.triggeredAt("a"), 0);
    EXPECT_EQ(throttle.completedAt("a"), -1);

    throttle.markInFlight("a");
    EXPECT_EQ(throttle.stateOf("a"), PrefetchThrottle::State::kInFlight);
    EXPECT_TRUE(throttle.promoteAttempted("a"));

    throttle.markCompleted("a");
    EXPECT_EQ(throttle.stateOf("a"), PrefetchThrottle::State::kCompleted);
    EXPECT_GE(throttle.completedAt("a"), 0);

    // A completed key still blocks re-triggering within the TTL.
    EXPECT_TRUE(throttle.reserve({"a"}).empty());
}

TEST(PrefetchThrottleTest, AlreadyResidentClearsPromoteAttempted) {
    PrefetchThrottle throttle;
    throttle.configure(1, 60);

    ASSERT_EQ(throttle.reserve({"a"}).size(), 1u);
    throttle.markAlreadyResident("a");
    EXPECT_EQ(throttle.stateOf("a"), PrefetchThrottle::State::kAlreadyResident);
    EXPECT_FALSE(throttle.promoteAttempted("a"));
}

TEST(PrefetchThrottleTest, WaitForCompletionReturnsOnComplete) {
    PrefetchThrottle throttle;
    throttle.configure(1, 60);
    ASSERT_EQ(throttle.reserve({"a"}).size(), 1u);

    std::thread finisher([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        throttle.markCompleted("a");
    });
    EXPECT_TRUE(throttle.waitForCompletion("a", /*max_wait_ms=*/2000));
    finisher.join();
}

TEST(PrefetchThrottleTest, WaitForCompletionTimesOutOrFailsFast) {
    PrefetchThrottle throttle;
    throttle.configure(1, 60);

    // Never triggered: treated as failed, returns immediately.
    EXPECT_FALSE(throttle.waitForCompletion("never", 100));

    ASSERT_EQ(throttle.reserve({"b"}).size(), 1u);
    throttle.markFailed("b");
    EXPECT_FALSE(throttle.waitForCompletion("b", 1000));

    ASSERT_EQ(throttle.reserve({"c"}).size(), 1u);
    EXPECT_FALSE(throttle.waitForCompletion("c", /*max_wait_ms=*/20));
}

TEST(PrefetchThrottleTest, DelegatedKeysBlockRetriggerAndNeverWait) {
    PrefetchThrottle throttle;
    throttle.configure(/*cooldown_sec=*/1, /*dedup_ttl_sec=*/60);

    ASSERT_EQ(throttle.reserve({"a"}).size(), 1u);
    throttle.markDelegated("a");
    EXPECT_EQ(throttle.stateOf("a"), PrefetchThrottle::State::kDelegated);
    // Delegated keys block re-triggering for the full TTL (the remote holder
    // is executing; re-delegating would spam the holder RPC)...
    EXPECT_TRUE(throttle.reserve({"a"}).empty());
    // ...and are never waited on locally: immediate false, no budget burn.
    EXPECT_FALSE(throttle.waitForCompletion("a", /*max_wait_ms=*/1000));
}

TEST(PrefetchThrottleTest, ConcurrentReserveGrantsEachKeyOnce) {
    PrefetchThrottle throttle;
    throttle.configure(1, 60);

    const auto keys = MakeKeys("shared", 64);
    std::atomic<size_t> total_reserved{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&] {
            auto granted = throttle.reserve(keys);
            total_reserved.fetch_add(granted.size(), std::memory_order_relaxed);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(total_reserved.load(), keys.size());
}

}  // namespace mooncake::test
