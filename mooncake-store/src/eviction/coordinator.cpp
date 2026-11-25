#include "eviction/coordinator.h"

namespace mooncake {
namespace eviction {

EvictionCoordinator::EvictionCoordinator(Options options) : options_(options) {}

void EvictionCoordinator::NotifyMemoryPressureSlow() {
    {
        std::lock_guard lock(mutex_);
        if (stopped_) {
            return;
        }
    }
    wake_cv_.notify_one();
}

void EvictionCoordinator::RecordMemoryRound(bool freed_memory,
                                            bool pace_alloc_failures) {
    std::lock_guard lock(mutex_);
    if (!freed_memory && options_.no_progress_backoff.count() > 0) {
        mem_alloc_cooldown_until_.reset();
        mem_backoff_until_ =
            std::chrono::steady_clock::now() + options_.no_progress_backoff;
        mem_state_.store(MemState::kBackoff, std::memory_order_release);
    } else if (freed_memory && pace_alloc_failures &&
               options_.idle_sleep.count() > 0) {
        mem_backoff_until_.reset();
        mem_alloc_cooldown_until_ =
            std::chrono::steady_clock::now() + options_.idle_sleep;
        mem_state_.store(MemState::kAllocCooldown, std::memory_order_release);
    } else {
        mem_backoff_until_.reset();
        mem_alloc_cooldown_until_.reset();
        auto state = mem_state_.load(std::memory_order_relaxed);
        while (state == MemState::kBackoff ||
               state == MemState::kAllocCooldown) {
            if (mem_state_.compare_exchange_weak(state, MemState::kIdle,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
                break;
            }
        }
    }
}

void EvictionCoordinator::WaitForNextCycle(bool keep_draining_memory) {
    if (keep_draining_memory) {
        auto expected = MemState::kWakePending;
        mem_state_.compare_exchange_strong(expected, MemState::kIdle,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed);
        return;
    }

    std::unique_lock lock(mutex_);
    const auto now = std::chrono::steady_clock::now();
    auto deadline = now + options_.idle_sleep;
    bool timer_expired = false;
    if (mem_backoff_until_.has_value()) {
        if (now >= *mem_backoff_until_) {
            mem_backoff_until_.reset();
            timer_expired = true;
        } else if (*mem_backoff_until_ < deadline) {
            deadline = *mem_backoff_until_;
        }
    }
    if (mem_alloc_cooldown_until_.has_value()) {
        if (now >= *mem_alloc_cooldown_until_) {
            mem_alloc_cooldown_until_.reset();
            timer_expired = true;
        } else if (mem_state_.load(std::memory_order_acquire) ==
                   MemState::kWakePending) {
            mem_alloc_cooldown_until_.reset();
        } else if (*mem_alloc_cooldown_until_ < deadline) {
            deadline = *mem_alloc_cooldown_until_;
        }
    }
    if (timer_expired) {
        mem_state_.exchange(MemState::kIdle, std::memory_order_acq_rel);
        return;
    }
    wake_cv_.wait_until(lock, deadline, [this] {
        return stopped_ || mem_state_.load(std::memory_order_acquire) ==
                               MemState::kWakePending;
    });
    const auto wake_now = std::chrono::steady_clock::now();
    if (mem_backoff_until_.has_value() && wake_now >= *mem_backoff_until_) {
        mem_backoff_until_.reset();
    }
    if (mem_alloc_cooldown_until_.has_value() &&
        (wake_now >= *mem_alloc_cooldown_until_ ||
         mem_state_.load(std::memory_order_acquire) ==
             MemState::kWakePending)) {
        mem_alloc_cooldown_until_.reset();
    }
    if (!mem_backoff_until_.has_value() &&
        !mem_alloc_cooldown_until_.has_value()) {
        mem_state_.exchange(MemState::kIdle, std::memory_order_acq_rel);
    }
}

void EvictionCoordinator::Stop() {
    {
        std::lock_guard lock(mutex_);
        stopped_ = true;
    }
    wake_cv_.notify_all();
}

}  // namespace eviction
}  // namespace mooncake
