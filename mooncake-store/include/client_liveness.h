#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <utility>

namespace mooncake {

enum class ClientLivenessState {
    ACTIVE,
    SUSPECTED,
    OFFLINE,
};

// Readable spelling for log lines and diagnostics; operator-facing messages
// should not print the numeric enum value.
[[nodiscard]] constexpr std::string_view toString(
    ClientLivenessState state) noexcept {
    switch (state) {
        case ClientLivenessState::ACTIVE:
            return "ACTIVE";
        case ClientLivenessState::SUSPECTED:
            return "SUSPECTED";
        case ClientLivenessState::OFFLINE:
            return "OFFLINE";
    }
    return "UNKNOWN";
}

enum class ClientLivenessTransition {
    NONE,
    BECAME_SUSPECTED,
    BECAME_OFFLINE,
};

enum class ClientLivenessObservation {
    REFRESHED_ACTIVE,
    RECOVERED_ACTIVE,
    REJECTED_OFFLINE,
};

class ClientLivenessRecord {
   public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    explicit ClientLivenessRecord(TimePoint initial_observation)
        : last_liveness_at_(initial_observation) {}

    [[nodiscard]] ClientLivenessState state() const {
        return state_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool IsServing() const {
        return state() == ClientLivenessState::ACTIVE;
    }

    [[nodiscard]] bool ShouldRetainResources() const {
        return state() != ClientLivenessState::OFFLINE;
    }

    // Reports only the liveness effect: refresh, recovery, or rejection of
    // an OFFLINE record. Session readiness and RPC status belong to the
    // registry.
    [[nodiscard]] ClientLivenessObservation Observe(TimePoint now) {
        std::lock_guard<std::mutex> lock(transition_mutex_);
        const auto current_state = state_.load(std::memory_order_relaxed);
        if (current_state == ClientLivenessState::OFFLINE) {
            return ClientLivenessObservation::REJECTED_OFFLINE;
        }
        last_liveness_at_ = std::max(last_liveness_at_, now);
        if (current_state == ClientLivenessState::SUSPECTED) {
            SetState(ClientLivenessState::ACTIVE);
            return ClientLivenessObservation::RECOVERED_ACTIVE;
        }
        return ClientLivenessObservation::REFRESHED_ACTIVE;
    }

    [[nodiscard]] ClientLivenessTransition Evaluate(
        TimePoint now, Clock::duration active_ttl,
        Clock::duration suspicion_ttl) {
        std::lock_guard<std::mutex> lock(transition_mutex_);
        switch (state_.load(std::memory_order_relaxed)) {
            case ClientLivenessState::ACTIVE:
                if (now - last_liveness_at_ >= active_ttl) {
                    suspected_since_ = now;
                    SetState(ClientLivenessState::SUSPECTED);
                    return ClientLivenessTransition::BECAME_SUSPECTED;
                }
                break;
            case ClientLivenessState::SUSPECTED:
                if (now - suspected_since_ >= suspicion_ttl) {
                    SetState(ClientLivenessState::OFFLINE);
                    return ClientLivenessTransition::BECAME_OFFLINE;
                }
                break;
            case ClientLivenessState::OFFLINE:
                break;
        }
        return ClientLivenessTransition::NONE;
    }

    using TransitionObserver =
        std::function<void(ClientLivenessState, ClientLivenessState)>;

    // The observer runs under the transition lock, in state-change order. It
    // must not throw or reenter the record; use it to enqueue work, not to run
    // resource cleanup. Install before publishing a newly registered record.
    void SetTransitionObserver(TransitionObserver observer) {
        std::lock_guard<std::mutex> lock(transition_mutex_);
        on_transition_ = std::move(observer);
        observer_enabled_.store(true, std::memory_order_release);
    }

    // Wait for any in-flight observer, disable future notifications, and
    // return the state at that point. Does not change liveness. Later
    // transitions do not invoke the observer unless it is reinstalled; already
    // queued notifications are not retracted.
    [[nodiscard]] ClientLivenessState StopObserving() {
        std::lock_guard lock(transition_mutex_);
        observer_enabled_.store(false, std::memory_order_release);
        return state();
    }

   private:
    TransitionObserver on_transition_;
    std::atomic<bool> observer_enabled_{false};

    void SetState(ClientLivenessState next) {
        const auto previous = state_.load(std::memory_order_relaxed);
        state_.store(next, std::memory_order_release);
        if (observer_enabled_.load(std::memory_order_acquire) &&
            on_transition_) {
            on_transition_(previous, next);
        }
    }

    std::atomic<ClientLivenessState> state_{ClientLivenessState::ACTIVE};
    std::mutex transition_mutex_;
    TimePoint last_liveness_at_;
    TimePoint suspected_since_{};
};

// Read-only, shared identity of one client registration. Resources retain this
// handle; mutable liveness records and operation admission belong to the
// registry.
using ClientSessionSharedPtr = std::shared_ptr<const ClientLivenessRecord>;

}  // namespace mooncake
