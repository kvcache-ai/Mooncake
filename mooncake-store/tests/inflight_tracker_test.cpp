/**
 * @file inflight_tracker_test.cpp
 * @brief Unit tests for InflightTracker: admission, enter/leave hooks,
 *        Close() rejecting new operations, Wait() until drain, and
 *        cross-thread Guard destruction (safe after counter-based drain).
 */
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <future>
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

    {
        auto g = tracker.Enter();
        EXPECT_FALSE(g.is_valid());  // new operations rejected
    }
    // A rejected guard must not fire either hook, including on destruction.
    EXPECT_EQ(entered, 0);
    EXPECT_EQ(left, 0);
    EXPECT_EQ(tracker.inflight(), 0);
}

TEST(InflightTrackerTest, WaitBlocksUntilInflightReleased) {
    std::atomic<int> inflight{0};
    InflightTracker tracker("test", [&] { ++inflight; }, [&] { --inflight; });
    std::optional<InflightTracker::Guard> guard{tracker.Enter()};
    ASSERT_TRUE(guard->is_valid());
    EXPECT_EQ(inflight.load(), 1);
    EXPECT_EQ(tracker.inflight(), 1);
    EXPECT_TRUE(tracker.Close());
    {
        auto rejected = tracker.Enter();
        EXPECT_FALSE(rejected.is_valid());
    }
    EXPECT_EQ(inflight.load(), 1);
    EXPECT_EQ(tracker.inflight(), 1);

    std::promise<void> wait_started;
    std::promise<void> wait_finished;
    auto started = wait_started.get_future();
    auto finished = wait_finished.get_future();

    std::thread drainer([&] {
        wait_started.set_value();
        tracker.Wait();
        wait_finished.set_value();
    });

    const auto start_status = started.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(start_status, std::future_status::ready)
        << "Drain thread did not start within the test deadline";
    if (start_status == std::future_status::ready) {
        EXPECT_EQ(finished.wait_for(std::chrono::milliseconds(100)),
                  std::future_status::timeout)
            << "Wait returned while a guard was still held";
    }

    // Release the guard even when an earlier expectation failed.
    guard.reset();
    EXPECT_EQ(finished.wait_for(std::chrono::seconds(5)),
              std::future_status::ready)
        << "Wait did not finish after the guard was released";
    // Keep the tracker alive; a stuck Wait requires an external timeout.
    drainer.join();
    EXPECT_EQ(tracker.inflight(), 0);
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

TEST(InflightTrackerTest, WaitsForLastGuardWithoutHooks) {
    InflightTracker tracker("multiple guards");
    tracker.Wait();
    EXPECT_TRUE(tracker.is_running());

    std::optional<InflightTracker::Guard> first{tracker.Enter()};
    std::optional<InflightTracker::Guard> second{tracker.Enter()};
    ASSERT_TRUE(first->is_valid());
    ASSERT_TRUE(second->is_valid());
    EXPECT_EQ(tracker.inflight(), 2);
    EXPECT_TRUE(tracker.Close());

    std::array<std::promise<void>, 2> wait_started;
    std::array<std::promise<void>, 2> wait_finished;
    std::array<std::future<void>, 2> started;
    std::array<std::future<void>, 2> finished;
    std::array<std::thread, 2> waiters;
    for (size_t i = 0; i < waiters.size(); ++i) {
        started[i] = wait_started[i].get_future();
        finished[i] = wait_finished[i].get_future();
        waiters[i] = std::thread([&, i] {
            wait_started[i].set_value();
            tracker.Wait();
            wait_finished[i].set_value();
        });
    }
    for (size_t i = 0; i < waiters.size(); ++i) {
        EXPECT_EQ(started[i].wait_for(std::chrono::seconds(5)),
                  std::future_status::ready)
            << "Waiter " << i << " did not start within the test deadline";
    }

    first.reset();
    EXPECT_EQ(tracker.inflight(), 1);
    for (size_t i = 0; i < waiters.size(); ++i) {
        EXPECT_EQ(finished[i].wait_for(std::chrono::milliseconds(100)),
                  std::future_status::timeout)
            << "Waiter " << i << " returned before the last guard was released";
    }

    second.reset();
    for (size_t i = 0; i < waiters.size(); ++i) {
        EXPECT_EQ(finished[i].wait_for(std::chrono::seconds(5)),
                  std::future_status::ready)
            << "Waiter " << i << " did not finish after the last guard";
    }
    for (auto& waiter : waiters) {
        waiter.join();
    }
    EXPECT_EQ(tracker.inflight(), 0);
    tracker.Wait();  // Waiting again after drain must also complete.
    EXPECT_FALSE(tracker.is_running());
}

TEST(InflightTrackerTest, CloseDrainsEnteringAndLeavingHooks) {
    std::promise<void> entering;
    std::promise<void> release_enter;
    std::promise<void> leaving;
    std::promise<void> release_leave;
    auto entering_reached = entering.get_future();
    auto enter_released = release_enter.get_future();
    auto leaving_reached = leaving.get_future();
    auto leave_released = release_leave.get_future();
    std::atomic<int> entered{0};
    std::atomic<int> left{0};
    std::atomic<bool> admitted{false};
    InflightTracker tracker(
        "paused hooks",
        [&] {
            ++entered;
            entering.set_value();
            enter_released.wait();
        },
        [&] {
            leaving.set_value();
            leave_released.wait();
            ++left;
        });

    std::thread worker([&] {
        auto guard = tracker.Enter();
        admitted.store(guard.is_valid());
    });
    EXPECT_EQ(entering_reached.wait_for(std::chrono::seconds(5)),
              std::future_status::ready)
        << "Enter did not reach its hook within the test deadline";
    EXPECT_EQ(tracker.inflight(), 1);
    EXPECT_TRUE(tracker.Close());

    std::promise<void> wait_started;
    std::promise<void> wait_finished;
    auto started = wait_started.get_future();
    auto finished = wait_finished.get_future();
    std::thread drainer([&] {
        wait_started.set_value();
        tracker.Wait();
        wait_finished.set_value();
    });
    EXPECT_EQ(started.wait_for(std::chrono::seconds(5)),
              std::future_status::ready)
        << "Drain thread did not start within the test deadline";
    EXPECT_EQ(finished.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout)
        << "Wait returned before the entering hook completed";

    release_enter.set_value();
    EXPECT_EQ(leaving_reached.wait_for(std::chrono::seconds(5)),
              std::future_status::ready)
        << "The admitted guard did not reach its leaving hook";
    EXPECT_TRUE(admitted.load());
    EXPECT_EQ(tracker.inflight(), 1);
    EXPECT_EQ(left.load(), 0);
    EXPECT_EQ(finished.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout)
        << "Wait returned before the leaving hook completed";

    // Unblock both hooks before joining, including on expectation failures.
    release_leave.set_value();
    EXPECT_EQ(finished.wait_for(std::chrono::seconds(5)),
              std::future_status::ready)
        << "Wait did not finish after both hooks completed";
    worker.join();
    drainer.join();
    EXPECT_TRUE(admitted.load());
    EXPECT_EQ(entered.load(), 1);
    EXPECT_EQ(left.load(), 1);
    EXPECT_EQ(tracker.inflight(), 0);
}

TEST(InflightTrackerTest, ConcurrentEnterAndClosePreservesAccounting) {
    constexpr int kRounds = 64;
    constexpr int kEntrants = 4;
    for (int round = 0; round < kRounds; ++round) {
        SCOPED_TRACE(round);
        std::atomic<int> entered{0};
        std::atomic<int> left{0};
        std::atomic<int> admitted{0};
        InflightTracker tracker("concurrent close", [&] { ++entered; },
                                [&] { ++left; });
        std::barrier start(kEntrants + 1);
        std::array<std::thread, kEntrants> entrants;
        for (auto& entrant : entrants) {
            entrant = std::thread([&] {
                start.arrive_and_wait();
                auto guard = tracker.Enter();
                if (guard.is_valid()) {
                    ++admitted;
                }
            });
        }

        // Race public operations; this does not force the internal rollback path.
        start.arrive_and_wait();
        EXPECT_TRUE(tracker.Close());
        for (auto& entrant : entrants) {
            entrant.join();
        }
        {
            auto rejected = tracker.Enter();
            EXPECT_FALSE(rejected.is_valid());
        }
        EXPECT_FALSE(tracker.is_running());
        EXPECT_EQ(entered.load(), admitted.load());
        EXPECT_EQ(left.load(), admitted.load());
        EXPECT_EQ(tracker.inflight(), 0);
        tracker.Wait();
    }
}

}  // namespace mooncake
