#pragma once

#include <atomic>
#include <cstdint>
#include <shared_mutex>

namespace mooncake::route {

#if defined(__cpp_lib_atomic_wait)
// Each route slot has its own reader/writer lock. The low bits count shared
// owners. An exclusive waiter closes admission to readers until the current
// owners drain. Contended callers park on the state word.
class SlotMutex {
   public:
    bool try_lock() noexcept {
        uint32_t empty = 0;
        return state_.compare_exchange_strong(empty, kWriter,
                                              std::memory_order_acquire);
    }

    void lock() noexcept {
        auto state = state_.load(std::memory_order_relaxed);
        for (;;) {
            if ((state & ~kWaiting) == 0) {
                // Carry the wait bit through ownership so unlock also wakes
                // other threads still parked behind the previous readers.
                if (state_.compare_exchange_weak(state, state | kWriter,
                                                 std::memory_order_acquire)) {
                    return;
                }
            } else {
                state = Park(state);
            }
        }
    }

    void unlock() noexcept {
        if (state_.exchange(0, std::memory_order_release) & kWaiting) {
            state_.notify_all();
        }
    }

    bool try_lock_shared() noexcept {
        auto state = state_.load(std::memory_order_relaxed);
        while (state < kMaxReaders) {
            if (state_.compare_exchange_weak(state, state + 1,
                                             std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

    void lock_shared() noexcept {
        auto state = state_.load(std::memory_order_relaxed);
        for (;;) {
            if (state < kMaxReaders) {
                if (state_.compare_exchange_weak(state, state + 1,
                                                 std::memory_order_acquire)) {
                    return;
                }
            } else if (state == kMaxReaders) {
                // The next reader release frees capacity. Only an exclusive
                // waiter may close admission while readers own the lock.
                state_.wait(state, std::memory_order_relaxed);
                state = state_.load(std::memory_order_relaxed);
            } else {
                state = Park(state);
            }
        }
    }

    void unlock_shared() noexcept {
        const auto before = state_.fetch_sub(1, std::memory_order_release);
        if (before == (kWaiting | 1) || before == kMaxReaders) {
            state_.notify_all();
        }
    }

   private:
    static constexpr uint32_t kWriter = uint32_t{1} << 31;
    static constexpr uint32_t kWaiting = uint32_t{1} << 30;
    static constexpr uint32_t kMaxReaders = kWaiting - 1;

    uint32_t Park(uint32_t state) noexcept {
        if (!(state & kWaiting)) {
            if (!state_.compare_exchange_strong(state, state | kWaiting,
                                                std::memory_order_relaxed)) {
                return state;
            }
            state |= kWaiting;
        }
        state_.wait(state, std::memory_order_relaxed);
        return state_.load(std::memory_order_relaxed);
    }

    std::atomic<uint32_t> state_{0};
};
#else
using SlotMutex = std::shared_mutex;
#endif

}  // namespace mooncake::route
