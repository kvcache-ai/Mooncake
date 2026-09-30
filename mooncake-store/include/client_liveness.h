#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>
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
                        PublishStateLocked(ClientLivenessState::SUSPECTED);
                        transition = ClientLivenessTransition::BECAME_SUSPECTED;
                    }
                    break;
                case ClientLivenessState::SUSPECTED:
                    if (now - suspected_since_ >= suspicion_ttl) {
                        // Publish the external barrier before OFFLINE so a
                        // concurrent snapshot cannot miss terminal work.
                        std::forward<ReserveRetirement>(reserve_retirement)();
                        PublishStateLocked(ClientLivenessState::OFFLINE);
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
    friend class SegmentAllocatorRegistration;

    // These callbacks only update serving-name counters. They must not acquire
    // transition_mutex_ or any client, segment, or metadata lock. A separate
    // mutex allows mounting to subscribe inside ObserveAndRun's operation.
    void AddServingObserver(const void* owner,
                            std::function<void(bool)> observer) {
        std::lock_guard lock(serving_observers_mutex_);
        const auto [it, inserted] =
            serving_observers_.emplace(owner, std::move(observer));
        if (inserted && IsServing()) {
            it->second(true);
        }
    }

    void RemoveServingObserver(const void* owner) {
        std::lock_guard lock(serving_observers_mutex_);
        const auto it = serving_observers_.find(owner);
        if (it != serving_observers_.end()) {
            if (IsServing()) {
                it->second(false);
            }
            serving_observers_.erase(it);
        }
    }

    // Caller holds transition_mutex_. Ordinary ACTIVE heartbeats never enter
    // this path; observers run only when the serving state changes.
    void PublishStateLocked(ClientLivenessState next) {
        std::lock_guard lock(serving_observers_mutex_);
        const bool was_serving = IsServing();
        state_.store(next, std::memory_order_release);
        const bool serving = next == ClientLivenessState::ACTIVE;
        if (was_serving != serving) {
            for (const auto& [owner, observer] : serving_observers_) {
                observer(serving);
            }
        }
    }

    [[nodiscard]] ClientLivenessObservation CommitObservationLocked(
        TimePoint now, ClientLivenessState current_state) {
        last_liveness_at_ = now;
        if (current_state == ClientLivenessState::SUSPECTED) {
            PublishStateLocked(ClientLivenessState::ACTIVE);
            return ClientLivenessObservation::RECOVERED_ACTIVE;
        }
        return ClientLivenessObservation::REFRESHED_ACTIVE;
    }

    std::atomic<ClientLivenessState> state_{ClientLivenessState::ACTIVE};
    std::mutex transition_mutex_;
    std::mutex serving_observers_mutex_;
    std::unordered_map<const void*, std::function<void(bool)>>
        serving_observers_;
    TimePoint last_liveness_at_;
    TimePoint suspected_since_{};
};

}  // namespace mooncake
