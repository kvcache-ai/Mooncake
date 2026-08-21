#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
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

    [[nodiscard]] ClientLivenessObservation Observe(TimePoint now) {
        return ObserveAndRun(now, [] {});
    }

    template <typename Operation>
    [[nodiscard]] ClientLivenessObservation ObserveAndRun(
        TimePoint now, Operation&& operation) {
        std::lock_guard<std::mutex> lock(transition_mutex_);
        const auto current_state = state_.load(std::memory_order_relaxed);
        if (current_state == ClientLivenessState::OFFLINE) {
            return ClientLivenessObservation::REJECTED_OFFLINE;
        }

        last_liveness_at_ = now;
        if (current_state == ClientLivenessState::SUSPECTED) {
            state_.store(ClientLivenessState::ACTIVE,
                         std::memory_order_release);
            std::forward<Operation>(operation)();
            return ClientLivenessObservation::RECOVERED_ACTIVE;
        }
        std::forward<Operation>(operation)();
        return ClientLivenessObservation::REFRESHED_ACTIVE;
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
    std::atomic<ClientLivenessState> state_{ClientLivenessState::ACTIVE};
    std::mutex transition_mutex_;
    TimePoint last_liveness_at_;
    TimePoint suspected_since_{};
};

}  // namespace mooncake
