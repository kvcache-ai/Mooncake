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

       private:
        friend class ClientLivenessRecord;
        explicit RetainingGuard(std::unique_lock<std::mutex>&& lock)
            : lock_(std::move(lock)) {}

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
        return RetainingGuard(std::move(lock));
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

    [[nodiscard]] ClientLivenessTransition Evaluate(
        TimePoint now, Clock::duration active_ttl,
        Clock::duration suspicion_ttl) {
        return EvaluateAndRetire(now, active_ttl, suspicion_ttl, [] {});
    }

    template <typename RetireOperation>
    [[nodiscard]] ClientLivenessTransition EvaluateAndRetire(
        TimePoint now, Clock::duration active_ttl,
        Clock::duration suspicion_ttl, RetireOperation&& retire_operation) {
        return EvaluateAndRetire(now, active_ttl, suspicion_ttl, [] {},
                                 std::forward<RetireOperation>(
                                     retire_operation));
    }

    template <typename ReserveRetirement, typename RetireOperation>
    [[nodiscard]] ClientLivenessTransition EvaluateAndRetire(
        TimePoint now, Clock::duration active_ttl,
        Clock::duration suspicion_ttl,
        ReserveRetirement&& reserve_retirement,
        RetireOperation&& retire_operation) {
        ClientLivenessTransition transition = ClientLivenessTransition::NONE;
        {
            std::lock_guard<std::mutex> lock(transition_mutex_);
            switch (state_.load(std::memory_order_relaxed)) {
                case ClientLivenessState::ACTIVE:
                    if (now - last_liveness_at_ >= active_ttl) {
                        suspected_since_ = now;
                        state_.store(ClientLivenessState::SUSPECTED,
                                     std::memory_order_release);
                        transition = ClientLivenessTransition::BECAME_SUSPECTED;
                    }
                    break;
                case ClientLivenessState::SUSPECTED:
                    if (now - suspected_since_ >= suspicion_ttl) {
                        // Publish the external barrier before OFFLINE so a
                        // concurrent snapshot cannot miss terminal work.
                        std::forward<ReserveRetirement>(reserve_retirement)();
                        state_.store(ClientLivenessState::OFFLINE,
                                     std::memory_order_release);
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

   private:
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
