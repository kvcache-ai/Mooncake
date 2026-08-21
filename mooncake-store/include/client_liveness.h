#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <utility>

namespace mooncake {

enum class ClientLivenessState {
    ACTIVE,
    SUSPECTED,
    OFFLINE,
};

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
        RetainingGuard& operator=(RetainingGuard&&) noexcept = default;

        [[nodiscard]] ClientLivenessObservation Observe(TimePoint now) {
            return record_->CommitObservationLocked(now);
        }

       private:
        friend class ClientLivenessRecord;
        RetainingGuard(ClientLivenessRecord* record,
                       std::unique_lock<std::mutex>&& lock)
            : record_(record), lock_(std::move(lock)) {}

        ClientLivenessRecord* record_;
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
        return RetainingGuard(this, std::move(lock));
    }

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

        return CommitObservationLocked(now, current_state);
    }

    template <typename Operation>
    [[nodiscard]] bool RunIfServing(Operation&& operation) {
        std::lock_guard<std::mutex> lock(transition_mutex_);
        if (state_.load(std::memory_order_relaxed) !=
            ClientLivenessState::ACTIVE) {
            return false;
        }
        std::forward<Operation>(operation)();
        return true;
    }

    template <typename Operation>
    [[nodiscard]] bool RunUnlessOffline(Operation&& operation) {
        std::lock_guard<std::mutex> lock(transition_mutex_);
        if (state_.load(std::memory_order_relaxed) ==
            ClientLivenessState::OFFLINE) {
            return false;
        }
        std::forward<Operation>(operation)();
        return true;
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
        std::lock_guard<std::mutex> lock(transition_mutex_);
        switch (state_.load(std::memory_order_relaxed)) {
            case ClientLivenessState::ACTIVE:
                if (now - last_liveness_at_ >= active_ttl) {
                    suspected_since_ = now;
                    state_.store(ClientLivenessState::SUSPECTED,
                                 std::memory_order_release);
                    return ClientLivenessTransition::BECAME_SUSPECTED;
                }
                break;
            case ClientLivenessState::SUSPECTED:
                if (now - suspected_since_ >= suspicion_ttl) {
                    state_.store(ClientLivenessState::OFFLINE,
                                 std::memory_order_release);
                    std::forward<RetireOperation>(retire_operation)();
                    return ClientLivenessTransition::BECAME_OFFLINE;
                }
                break;
            case ClientLivenessState::OFFLINE:
                break;
        }
        return ClientLivenessTransition::NONE;
    }

   private:
    [[nodiscard]] ClientLivenessObservation CommitObservationLocked(
        TimePoint now) {
        return CommitObservationLocked(
            now, state_.load(std::memory_order_relaxed));
    }

    [[nodiscard]] ClientLivenessObservation CommitObservationLocked(
        TimePoint now, ClientLivenessState current_state) {
        last_liveness_at_ = now;
        if (current_state == ClientLivenessState::SUSPECTED) {
            state_.store(ClientLivenessState::ACTIVE,
                         std::memory_order_release);
            return ClientLivenessObservation::RECOVERED_ACTIVE;
        }
        return ClientLivenessObservation::REFRESHED_ACTIVE;
    }

    std::atomic<ClientLivenessState> state_{ClientLivenessState::ACTIVE};
    std::mutex transition_mutex_;
    TimePoint last_liveness_at_;
    TimePoint suspected_since_{};
};

}  // namespace mooncake
