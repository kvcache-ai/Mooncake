#include "eviction/coordinator.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace mooncake {
namespace eviction {
namespace {

using std::chrono::milliseconds;
using std::chrono::steady_clock;

int64_t ElapsedMs(steady_clock::time_point since) {
    return std::chrono::duration_cast<milliseconds>(steady_clock::now() - since)
        .count();
}

template <typename Fn>
int64_t MeasureWakeLatencyMs(EvictionCoordinator& coordinator, Fn&& notify) {
    std::atomic<bool> waiting{false};
    steady_clock::time_point wake_time;

    std::thread worker([&] {
        waiting.store(true, std::memory_order_release);
        coordinator.WaitForNextCycle(/*keep_draining_memory=*/false);
        wake_time = steady_clock::now();
    });
    while (!waiting.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(milliseconds(10));
    const auto notify_time = steady_clock::now();
    notify();
    worker.join();
    return std::chrono::duration_cast<milliseconds>(wake_time - notify_time)
        .count();
}

TEST(EvictionCoordinatorTest, EventDrivenMemoryWakeupIsImmediate) {
    EvictionCoordinator coordinator({.idle_sleep = milliseconds(200),
                                     .no_progress_backoff = milliseconds(50)});
    const int64_t wake_ms = MeasureWakeLatencyMs(
        coordinator, [&] { coordinator.NotifyMemoryHighWatermark(); });
    EXPECT_GE(wake_ms, 0);
    EXPECT_LT(wake_ms, 50);
}

TEST(EvictionCoordinatorTest, ContinuousDrainSkipsIdleSleep) {
    EvictionCoordinator coordinator({.idle_sleep = milliseconds(200),
                                     .no_progress_backoff = milliseconds(50)});
    const auto start = steady_clock::now();
    for (int i = 0; i < 10; ++i) {
        coordinator.WaitForNextCycle(/*keep_draining_memory=*/true);
    }
    EXPECT_LT(ElapsedMs(start), 50);
}

TEST(EvictionCoordinatorTest, MemoryBackoffSuppressesWakeupUntilExpiry) {
    EvictionCoordinator coordinator({.idle_sleep = milliseconds(200),
                                     .no_progress_backoff = milliseconds(25)});
    const auto start = steady_clock::now();
    coordinator.RecordMemoryRound(/*freed_memory=*/false);
    ASSERT_TRUE(coordinator.IsInMemoryNoProgressBackoff());

    // Pressure notifications during backoff must not wake early, and
    // WaitForNextCycle clamps sleep to backoff duration (25ms < 200ms).
    coordinator.NotifyMemoryHighWatermark();
    coordinator.NotifyMemoryAllocFailure();
    coordinator.WaitForNextCycle(/*keep_draining_memory=*/false);
    const int64_t slept_ms = ElapsedMs(start);
    EXPECT_GE(slept_ms, 20);
    EXPECT_LT(slept_ms, 100);
    EXPECT_FALSE(coordinator.IsInMemoryNoProgressBackoff());
}

TEST(EvictionCoordinatorTest,
     AllocCooldownCoalescesAllocFailuresWithoutBlockingHighWatermark) {
    EvictionCoordinator coordinator({.idle_sleep = milliseconds(80),
                                     .no_progress_backoff = milliseconds(80)});
    // First below-watermark allocation failure wakes immediately when idle.
    const int64_t first_wake_ms = MeasureWakeLatencyMs(
        coordinator, [&] { coordinator.NotifyMemoryAllocFailure(); });
    EXPECT_GE(first_wake_ms, 0);
    EXPECT_LT(first_wake_ms, 35);

    // After a productive below-watermark round, subsequent allocation failures
    // are coalesced to the idle_sleep cadence.
    const auto start = steady_clock::now();
    coordinator.RecordMemoryRound(/*freed_memory=*/true,
                                  /*pace_alloc_failures=*/true);
    ASSERT_TRUE(coordinator.IsInMemoryAllocCooldown());
    ASSERT_FALSE(coordinator.IsInMemoryNoProgressBackoff());
    coordinator.NotifyMemoryAllocFailure();
    coordinator.WaitForNextCycle(/*keep_draining_memory=*/false);
    EXPECT_GE(ElapsedMs(start), 60);
    EXPECT_FALSE(coordinator.IsInMemoryAllocCooldown());

    // A high-watermark notification still wakes immediately during cooldown.
    coordinator.RecordMemoryRound(/*freed_memory=*/true,
                                  /*pace_alloc_failures=*/true);
    ASSERT_TRUE(coordinator.IsInMemoryAllocCooldown());
    const int64_t hwm_wake_ms = MeasureWakeLatencyMs(
        coordinator, [&] { coordinator.NotifyMemoryHighWatermark(); });
    EXPECT_GE(hwm_wake_ms, 0);
    EXPECT_LT(hwm_wake_ms, 35);
    EXPECT_FALSE(coordinator.IsInMemoryAllocCooldown());
}

TEST(EvictionCoordinatorTest,
     RecordMemoryRoundPreservesPendingWakeupOnProductiveRound) {
    EvictionCoordinator coordinator({.idle_sleep = milliseconds(200),
                                     .no_progress_backoff = milliseconds(50)});
    // A concurrent notification arriving before RecordMemoryRound finishes
    // must not be overwritten with kIdle.
    coordinator.NotifyMemoryAllocFailure();
    coordinator.RecordMemoryRound(/*freed_memory=*/true,
                                  /*pace_alloc_failures=*/false);
    const auto start = steady_clock::now();
    coordinator.WaitForNextCycle(/*keep_draining_memory=*/false);
    EXPECT_LT(ElapsedMs(start), 50);
}

TEST(EvictionCoordinatorTest, StopUnblocksWaitForNextCycleImmediately) {
    EvictionCoordinator coordinator({.idle_sleep = milliseconds(500),
                                     .no_progress_backoff = milliseconds(50)});
    const int64_t wake_ms =
        MeasureWakeLatencyMs(coordinator, [&] { coordinator.Stop(); });
    EXPECT_GE(wake_ms, 0);
    EXPECT_LT(wake_ms, 50);
}

TEST(EvictionCoordinatorTest,
     WaitForNextCycleReturnsImmediatelyWhenBackoffAlreadyExpired) {
    EvictionCoordinator coordinator({.idle_sleep = milliseconds(300),
                                     .no_progress_backoff = milliseconds(15)});
    coordinator.RecordMemoryRound(/*freed_memory=*/false);
    ASSERT_TRUE(coordinator.IsInMemoryNoProgressBackoff());

    // Simulate maintenance work outlasting the 15ms backoff window.
    std::this_thread::sleep_for(milliseconds(25));
    const auto start = steady_clock::now();
    coordinator.WaitForNextCycle(/*keep_draining_memory=*/false);
    EXPECT_LT(ElapsedMs(start), 50);
    EXPECT_FALSE(coordinator.IsInMemoryNoProgressBackoff());
}

}  // namespace
}  // namespace eviction
}  // namespace mooncake
