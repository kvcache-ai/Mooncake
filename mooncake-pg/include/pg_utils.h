#ifndef MOONCAKE_PG_UTILS_H
#define MOONCAKE_PG_UTILS_H

#pragma once

#include <chrono>
#include <thread>
#include <algorithm>
#include <cstdint>

// For PAUSE macro
#include <transfer_engine.h>

namespace mooncake {

/**
 * @brief Configuration parameters for the BackoffWaiter.
 *
 * Defines the thresholds and durations for the multi-stage backoff strategy:
 * Spin -> Thread Yield -> Exponential Sleep.
 */
struct BackoffWaiterConfig {
    /**
     * @brief The maximum number of iterations to perform CPU
     * busy-waiting (spinning). During this phase, the thread uses PAUSE
     * to minimize latency.
     */
    uint32_t spin_limit = 200;

    /**
     * @brief The maximum number of times to yield.
     * This phase occurs after spinning is exhausted, reducing CPU consumption
     * while still maintaining relatively high responsiveness.
     */
    uint32_t yield_limit = 50;

    /**
     * @brief The initial sleep duration once the waiter enters the sleep phase.
     */
    std::chrono::microseconds init_sleep{10};

    /**
     * @brief The maximum allowed sleep duration. The sleep time will double
     * exponentially up to this cap to prevent excessive overhead during waits.
     */
    std::chrono::microseconds max_sleep{100000};  // 100ms

    /**
     * @brief Creates a configuration that skips spinning and yielding, using
     * only sleep-based backoff.
     *
     * @param init_sleep The initial sleep duration.
     * @param max_sleep The maximum sleep duration cap.
     * @return BackoffWaiterConfig instance using sleep-only strategy.
     */
    static BackoffWaiterConfig sleepOnly(
        std::chrono::microseconds init_sleep,
        std::chrono::microseconds max_sleep) noexcept {
        return {0, 0, init_sleep, max_sleep};
    }

    /**
     * @brief Creates a configuration that uses a fixed sleep duration.
     *
     * This disables spinning, yielding, and exponential backoff.
     * The thread will sleep for a constant duration on each wait iteration.
     *
     * @param sleep_time The constant sleep duration to use.
     * @return BackoffWaiterConfig instance with constant sleep behavior.
     */
    static BackoffWaiterConfig constantSleep(
        std::chrono::microseconds sleep_time) noexcept {
        return {0, 0, sleep_time, sleep_time};
    }
};

/**
 * @brief A waiting utility with adaptive backoff strategy.
 *
 * This class provides a three-stage backoff mechanism (Spin -> Yield -> Sleep)
 * designed for efficiently polling asynchronous operations. It minimizes
 * latency for fast operations by using CPU spinning initially, then
 * progressively reduces CPU usage through thread yielding and exponential sleep
 * backoff for long-running waits.
 */
class BackoffWaiter {
   public:
    explicit BackoffWaiter(
        const BackoffWaiterConfig& cfg = BackoffWaiterConfig{})
        : config_(cfg), current_sleep_(cfg.init_sleep) {}

    void reset() noexcept {
        spin_count_ = 0;
        yield_count_ = 0;
        current_sleep_ = config_.init_sleep;
    }

    /**
     * @brief Advances the waiter to the next backoff state.
     *
     * @par Example Usage:
     * Manually calling `step()` is useful for custom waiting
     * where `wait()` or `wait_for()` is not applicable.
     *
     * @code
     * std::atomic<bool> ready_flag{false};
     * mooncake::BackoffWaiter waiter;
     *
     * // Wait indefinitely until the flag is set by another thread
     * while (!ready_flag.load(std::memory_order_acquire)) {
     *     // Perform some custom logic here if needed...
     *     waiter.step();
     * }
     *
     * // Reset the state if you plan to reuse this waiter instance later
     * waiter.reset();
     * @endcode
     */
    void step() {
        if (spin_count_ < config_.spin_limit) {
            PAUSE();
            ++spin_count_;
        } else if (yield_count_ < config_.yield_limit) {
            std::this_thread::yield();
            ++yield_count_;
        } else {
            std::this_thread::sleep_for(current_sleep_);
            current_sleep_ = std::min(current_sleep_ * 2, config_.max_sleep);
        }
    }

    /**
     * @brief Blocks the current thread until the predicate is satisfied
     * or the timeout expires.
     *
     * Repeatedly evaluates the given predicate. If the predicate returns false,
     * it progresses the backoff state using step().
     *
     * @tparam Predicate A callable that returns a boolean condition.
     * @tparam Rep An arithmetic type representing the number of ticks.
     * @tparam Period A std::ratio representing the tick period.
     * @param timeout The maximum duration to wait before giving up.
     * @param pred The condition to wait for.
     * @return true if the predicate evaluated to true within the timeout.
     * @return false if the timeout expired before the predicate was satisfied.
     */
    template <typename Predicate, typename Rep, typename Period>
    [[nodiscard]] bool wait_for(std::chrono::duration<Rep, Period> timeout,
                                Predicate pred) {
        reset();

        auto start = std::chrono::steady_clock::now();
        while (!pred()) {
            if (std::chrono::steady_clock::now() - start > timeout) {
                return false;
            }
            step();
        }
        return true;
    }

    /**
     * @brief Blocks the current thread indefinitely until the predicate is
     * satisfied.
     *
     * @tparam Predicate A callable that returns a boolean condition.
     * @param pred The condition to wait for.
     */
    template <typename Predicate>
    void wait(Predicate pred) {
        reset();

        while (!pred()) {
            step();
        }
    }

   private:
    BackoffWaiterConfig config_;
    uint32_t spin_count_{0};
    uint32_t yield_count_{0};
    std::chrono::microseconds current_sleep_;
};
}  // namespace mooncake

#endif
