#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>

namespace mooncake {
namespace eviction {

// Coordinates event-driven memory eviction wakeups, continuous draining,
// below-watermark failure coalescing, and no-progress backoff for the
// background eviction thread.
class EvictionCoordinator {
   public:
    struct Options {
        std::chrono::milliseconds idle_sleep{10};
        std::chrono::milliseconds no_progress_backoff{10};
    };

    explicit EvictionCoordinator(Options options);
    EvictionCoordinator() : EvictionCoordinator(Options{}) {}

    // Returns true while memory eviction is in its no-progress backoff window.
    [[nodiscard]] bool IsInMemoryNoProgressBackoff() const noexcept {
        return mem_state_.load(std::memory_order_acquire) == MemState::kBackoff;
    }

    // Returns true while below-watermark allocation failures are being
    // coalesced to the idle_sleep cadence.
    [[nodiscard]] bool IsInMemoryAllocCooldown() const noexcept {
        return mem_state_.load(std::memory_order_acquire) ==
               MemState::kAllocCooldown;
    }

    // Wakes the eviction thread immediately when global memory exceeds the
    // high watermark (bypasses below-watermark alloc-failure cooldown).
    void NotifyMemoryHighWatermark() {
        auto state = mem_state_.load(std::memory_order_relaxed);
        while (state == MemState::kIdle || state == MemState::kAllocCooldown) {
            if (mem_state_.compare_exchange_weak(state, MemState::kWakePending,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
                NotifyMemoryPressureSlow();
                return;
            }
        }
    }

    // Wakes the eviction thread on a memory allocation failure when idle.
    // Subsequent below-watermark failures are coalesced during cooldown.
    void NotifyMemoryAllocFailure() {
        auto expected = MemState::kIdle;
        if (mem_state_.load(std::memory_order_relaxed) != MemState::kIdle ||
            !mem_state_.compare_exchange_strong(
                expected, MemState::kWakePending, std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return;
        }
        NotifyMemoryPressureSlow();
    }

    // Records whether a memory eviction round freed space.
    // When pace_alloc_failures is true for a productive round, subsequent
    // below-watermark allocation failures are coalesced to idle_sleep.
    void RecordMemoryRound(bool freed_memory, bool pace_alloc_failures = false);

    // Waits until the next eviction cycle, or returns immediately when
    // draining.
    void WaitForNextCycle(bool keep_draining_memory);

    // Signals shutdown and wakes the eviction thread immediately.
    void Stop();

   private:
    enum class MemState : uint8_t {
        kIdle,
        kAllocCooldown,
        kWakePending,
        kBackoff,
    };

    void NotifyMemoryPressureSlow();

    const Options options_;

    alignas(64) std::atomic<MemState> mem_state_{MemState::kIdle};

    alignas(64) mutable std::mutex mutex_;
    std::condition_variable wake_cv_;
    std::optional<std::chrono::steady_clock::time_point> mem_backoff_until_;
    std::optional<std::chrono::steady_clock::time_point>
        mem_alloc_cooldown_until_;
    bool stopped_{false};
};

}  // namespace eviction
}  // namespace mooncake
