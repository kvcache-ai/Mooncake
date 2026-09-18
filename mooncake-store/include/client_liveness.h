#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
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
    // The wrapped operation failed its success predicate, so state and the
    // observation timestamp were left unchanged.
    OBSERVATION_WITHHELD,
};

class ClientLivenessRecord {
   public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    explicit ClientLivenessRecord(TimePoint initial_observation)
        : last_liveness_at_(initial_observation) {}

    class ServingGuard {
       public:
        ServingGuard(const ServingGuard&) = delete;
        ServingGuard& operator=(const ServingGuard&) = delete;
        ServingGuard(ServingGuard&&) noexcept = default;
        ServingGuard& operator=(ServingGuard&&) noexcept = default;

       private:
        friend class ClientLivenessRecord;
        explicit ServingGuard(std::unique_lock<std::mutex>&& lock)
            : lock_(std::move(lock)) {}

        std::unique_lock<std::mutex> lock_;
    };

    class RetainingGuard {
       public:
        RetainingGuard(const RetainingGuard&) = delete;
        RetainingGuard& operator=(const RetainingGuard&) = delete;
        RetainingGuard(RetainingGuard&&) noexcept = default;
        RetainingGuard& operator=(RetainingGuard&&) = delete;

        // Commit successful work without reacquiring the transition mutex.
        [[nodiscard]] ClientLivenessObservation Observe(TimePoint now) {
            assert(lock_.owns_lock());
            return record_.CommitObservation(
                now, record_.state_.load(std::memory_order_relaxed));
        }

       private:
        friend class ClientLivenessRecord;
        RetainingGuard(ClientLivenessRecord& record,
                       std::unique_lock<std::mutex>&& lock)
            : record_(record), lock_(std::move(lock)) {}

        // Non-null and fixed for this guard's lifetime; move construction
        // transfers the lock, but move assignment must not rebind the record.
        ClientLivenessRecord& record_;
        std::unique_lock<std::mutex> lock_;
    };

    [[nodiscard]] ClientLivenessState state() const {
        return state_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool IsServing() const {
        return state() == ClientLivenessState::ACTIVE;
    }

    [[nodiscard]] bool ShouldRetainResources() const {
        return state() != ClientLivenessState::OFFLINE;
    }

    [[nodiscard]] std::optional<ServingGuard> TryAcquireServingGuard() {
        std::unique_lock<std::mutex> lock(transition_mutex_);
        if (state_.load(std::memory_order_relaxed) !=
            ClientLivenessState::ACTIVE) {
            return std::nullopt;
        }
        return ServingGuard(std::move(lock));
    }

    [[nodiscard]] std::optional<RetainingGuard> TryAcquireRetainingGuard() {
        std::unique_lock<std::mutex> lock(transition_mutex_);
        if (state_.load(std::memory_order_relaxed) ==
            ClientLivenessState::OFFLINE) {
            return std::nullopt;
        }
        return RetainingGuard(*this, std::move(lock));
    }

    // Reports only the liveness effect: refresh, recovery, or rejection of
    // an OFFLINE record. Session readiness and RPC status belong to the
    // manager.
    [[nodiscard]] ClientLivenessObservation Observe(TimePoint now) {
        return ObserveAndRun(now, [] { return true; });
    }

    template <typename Operation>
    [[nodiscard]] ClientLivenessObservation ObserveAndRun(
        TimePoint now, Operation&& operation) {
        std::lock_guard<std::mutex> lock(transition_mutex_);
        const auto current_state = state_.load(std::memory_order_relaxed);
        if (current_state == ClientLivenessState::OFFLINE) {
            return ClientLivenessObservation::REJECTED_OFFLINE;
        }

        const bool should_commit = std::forward<Operation>(operation)();
        if (!should_commit) {
            return ClientLivenessObservation::OBSERVATION_WITHHELD;
        }

        return CommitObservation(now, current_state);
    }

    [[nodiscard]] ClientLivenessTransition Evaluate(
        TimePoint now, Clock::duration active_ttl,
        Clock::duration suspicion_ttl) {
        return EvaluateAndRetire(now, active_ttl, suspicion_ttl, [] {});
    }

    template <typename RetireOperation>
    [[nodiscard]] ClientLivenessTransition EvaluateAndRetire(
        TimePoint now, Clock::duration active_ttl,
        Clock::duration suspicion_ttl, RetireOperation&& retire_operation) {
        return EvaluateAndRetire(
            now, active_ttl, suspicion_ttl, [] {},
            std::forward<RetireOperation>(retire_operation));
    }

    template <typename ReserveRetirement, typename RetireOperation>
    [[nodiscard]] ClientLivenessTransition EvaluateAndRetire(
        TimePoint now, Clock::duration active_ttl,
        Clock::duration suspicion_ttl, ReserveRetirement&& reserve_retirement,
        RetireOperation&& retire_operation) {
        ClientLivenessTransition transition = ClientLivenessTransition::NONE;
        {
            std::lock_guard<std::mutex> lock(transition_mutex_);
            switch (state_.load(std::memory_order_relaxed)) {
                case ClientLivenessState::ACTIVE:
                    if (now - last_liveness_at_ >= active_ttl) {
                        suspected_since_ = now;
                        SetState(ClientLivenessState::SUSPECTED);
                        transition = ClientLivenessTransition::BECAME_SUSPECTED;
                    }
                    break;
                case ClientLivenessState::SUSPECTED:
                    if (now - suspected_since_ >= suspicion_ttl) {
                        // Publish the external barrier before OFFLINE so a
                        // concurrent snapshot cannot miss terminal work.
                        std::forward<ReserveRetirement>(reserve_retirement)();
                        SetState(ClientLivenessState::OFFLINE);
                        transition = ClientLivenessTransition::BECAME_OFFLINE;
                    }
                    break;
                case ClientLivenessState::OFFLINE:
                    break;
            }
        }
        // OFFLINE is already visible and no new guard can enter. Run the
        // one-shot retirement work without nesting this mutex under Segment,
        // metadata, or snapshot locks acquired by the callback.
        if (transition == ClientLivenessTransition::BECAME_OFFLINE) {
            std::forward<RetireOperation>(retire_operation)();
        }
        return transition;
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
    // transitions do not invoke the observer unless it is reinstalled.
    [[nodiscard]] ClientLivenessState StopObserving() {
        std::lock_guard lock(transition_mutex_);
        DisableTransitionObserver();
        return state();
    }

    // Low-level disable, also usable while holding a RetainingGuard. Prefer
    // StopObserving when a final state snapshot is needed. The owner must
    // serialize removal against state-changing operations; already queued
    // notifications are not retracted.
    void DisableTransitionObserver() {
        observer_enabled_.store(false, std::memory_order_release);
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

    [[nodiscard]] ClientLivenessObservation CommitObservation(
        TimePoint now, ClientLivenessState current_state) {
        last_liveness_at_ = std::max(last_liveness_at_, now);
        if (current_state == ClientLivenessState::SUSPECTED) {
            SetState(ClientLivenessState::ACTIVE);
            return ClientLivenessObservation::RECOVERED_ACTIVE;
        }
        return ClientLivenessObservation::REFRESHED_ACTIVE;
    }

    std::atomic<ClientLivenessState> state_{ClientLivenessState::ACTIVE};
    std::mutex transition_mutex_;
    TimePoint last_liveness_at_;
    TimePoint suspected_since_{};
};

// Read-only, shared identity of one client registration. Resources retain this
// handle; mutable liveness records and operation admission belong to the
// manager.
using ClientSessionSharedPtr = std::shared_ptr<const ClientLivenessRecord>;

}  // namespace mooncake
