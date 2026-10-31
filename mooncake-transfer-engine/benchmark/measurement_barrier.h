// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef TEBENCH_MEASUREMENT_BARRIER_H
#define TEBENCH_MEASUREMENT_BARRIER_H

#include <atomic>
#include <thread>

namespace mooncake {
namespace tent {

// One-shot warmup barrier. The last worker takes the baseline snapshot before
// publishing STARTED; no worker may issue measured transfers before that point.
class MeasurementBarrier {
   public:
    explicit MeasurementBarrier(int workers) : workers_(workers) {}

    template <typename Snapshot>
    bool arriveAndWait(Snapshot snapshot) {
        if (failed()) return false;
        if (ready_.fetch_add(1, std::memory_order_acq_rel) + 1 == workers_) {
            if (failed()) return false;
            if (!snapshot()) {
                fail();
                return false;
            }
            auto expected = State::WAITING;
            // A failed worker must never be overwritten by a successful start.
            state_.compare_exchange_strong(expected, State::STARTED,
                                           std::memory_order_acq_rel);
        }
        State state;
        while ((state = state_.load(std::memory_order_acquire)) ==
               State::WAITING) {
            std::this_thread::yield();
        }
        return state == State::STARTED;
    }

    void fail() { state_.store(State::FAILED, std::memory_order_release); }

    bool failed() const {
        return state_.load(std::memory_order_acquire) == State::FAILED;
    }

   private:
    enum class State { WAITING, STARTED, FAILED };
    const int workers_;
    std::atomic<int> ready_{0};
    std::atomic<State> state_{State::WAITING};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_MEASUREMENT_BARRIER_H
