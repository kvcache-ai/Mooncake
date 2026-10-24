/**
 * @file inflight_tracker_test.cpp
 * @brief Unit tests for InflightTracker: admission, enter/leave hooks,
 *        Close() rejecting new operations, Wait() until drain, and
 *        cross-thread Guard destruction (safe after counter-based drain).
 */
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <thread>

#include "inflight_tracker.h"

namespace mooncake {

TEST(InflightTrackerTest, AdmitsAndFiresHook) {
    std::atomic<int> inflight{0};
    InflightTracker tracker("test", [&] { ++inflight; }, [&] { --inflight; });
    {
        auto g = tracker.Enter();
        EXPECT_TRUE(g.is_valid());
        EXPECT_EQ(inflight.load(), 1);
        EXPECT_EQ(tracker.inflight(), 1);
    }
    EXPECT_EQ(inflight.load(), 0);
    EXPECT_EQ(tracker.inflight(), 0);
}

TEST(InflightTrackerTest, CloseRejectsNewGuards) {
    int entered = 0;
    int left = 0;
    InflightTracker tracker("test", [&] { ++entered; }, [&] { ++left; });

    EXPECT_TRUE(tracker.Close());   // initiates the stop, returns at once
    EXPECT_FALSE(tracker.Close());  // already closed (idempotent)
    tracker.Wait();                 // idle: returns immediately

    auto g = tracker.Enter();
    EXPECT_FALSE(g.is_valid());  // new operations rejected
    EXPECT_EQ(entered, 0);       // a rejected guard does not fire the hook
    EXPECT_EQ(left, 0);
}

TEST(InflightTrackerTest, WaitBlocksUntilInflightReleased) {
    std::atomic<int> inflight{0};
    InflightTracker tracker("test", [&] { ++inflight; }, [&] { --inflight; });
    std::atomic<bool> guard_held{false};
    std::atomic<bool> release{false};
    std::atomic<bool> wait_returned{false};

    std::thread worker([&] {
        auto g = tracker.Enter();
        EXPECT_TRUE(g.is_valid());
        guard_held.store(true, std::memory_order_release);
        while (!release.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        // g released when the lambda returns
    });

    while (!guard_held.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(inflight.load(), 1);

    std::thread drainer([&] {
        tracker.Close();
        tracker.Wait();  // blocks until the worker releases its guard
        wait_returned.store(true, std::memory_order_release);
    });

    // Wait stays blocked while the worker holds its guard.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(wait_returned.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    drainer.join();
    worker.join();
    EXPECT_TRUE(wait_returned.load(std::memory_order_acquire));
    EXPECT_EQ(inflight.load(), 0);
}

TEST(InflightTrackerTest, GuardCanBeDestroyedOnAnotherThread) {
    std::atomic<int> hook_inflight{0};
    InflightTracker tracker(
        "cross-thread", [&] { ++hook_inflight; }, [&] { --hook_inflight; });

    std::optional<InflightTracker::Guard> guard;
    guard.emplace(tracker.Enter());
    ASSERT_TRUE(guard->is_valid());
    EXPECT_EQ(tracker.inflight(), 1);

    std::thread other([&] {
        guard.reset();  // Retire on a different OS thread than Enter
    });
    other.join();
    EXPECT_EQ(tracker.inflight(), 0);
    EXPECT_EQ(hook_inflight.load(), 0);

    tracker.Close();
    tracker.Wait();
}

}  // namespace mooncake
