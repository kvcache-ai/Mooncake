#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/functional/hash.hpp>

#include "types.h"

namespace mooncake {

// One counter per distinct segment name. Multiple registrations of the same
// name contribute only once to the live AllocatorManager's aggregate.
class ServingNameCounter {
   public:
    explicit ServingNameCounter(std::shared_ptr<std::atomic<size_t>> total)
        : total_(std::move(total)) {}
    void Update(bool serving);

   private:
    std::mutex mutex_;
    size_t serving_registrations_ = 0;
    std::shared_ptr<std::atomic<size_t>> total_;
};

struct RetainingClientIndex {
    using ClientIds = std::unordered_set<UUID, boost::hash<UUID>>;
    void Update(const UUID& client_id, bool retaining);
    std::shared_ptr<const ClientIds> Snapshot();

    // Leaf lock: updates must not acquire client, segment, or metadata locks.
    std::mutex mutex;
    ClientIds clients;
    // Invalidate on membership changes; the next reader publishes one copy.
    // In-flight RPCs retain their immutable view after releasing client_mutex_.
    std::shared_ptr<const ClientIds> snapshot =
        std::make_shared<const ClientIds>();
};

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
    friend class MasterService;

    // Mounts can attach counters inside ObserveAndRun while transition_mutex_
    // is held. This separate lock serializes bindings with state publication.
    // Index/counter updates take only leaf locks, never registry locks.
    void AddServingCounter(std::shared_ptr<ServingNameCounter> counter) {
        std::lock_guard lock(resource_mutex_);
        serving_counters_.push_back(counter);
        if (IsServing()) {
            counter->Update(true);
        }
    }

    void RemoveServingCounter(
        const std::shared_ptr<ServingNameCounter>& counter) {
        std::lock_guard lock(resource_mutex_);
        const auto it = std::find(serving_counters_.begin(),
                                  serving_counters_.end(), counter);
        if (it != serving_counters_.end()) {
            if (IsServing()) {
                counter->Update(false);
            }
            serving_counters_.erase(it);
        }
    }

    void BindRetainingClient(const UUID& client_id,
                             std::shared_ptr<RetainingClientIndex> index) {
        std::lock_guard lock(resource_mutex_);
        assert(!retaining_client_index_);
        retaining_client_id_ = client_id;
        retaining_client_index_ = std::move(index);
        if (ShouldRetainResources()) {
            retaining_client_index_->Update(client_id, true);
        }
    }

    void UnbindRetainingClient() {
        std::lock_guard lock(resource_mutex_);
        if (retaining_client_index_) {
            if (ShouldRetainResources()) {
                retaining_client_index_->Update(retaining_client_id_, false);
            }
            retaining_client_index_.reset();
        }
    }

    // Caller holds transition_mutex_. Ordinary ACTIVE heartbeats never enter
    // this path. Update only the resource condition that actually changed.
    void PublishStateLocked(ClientLivenessState next) {
        std::lock_guard lock(resource_mutex_);
        const auto previous = state();
        state_.store(next, std::memory_order_release);
        const bool serving = next == ClientLivenessState::ACTIVE;
        if ((previous == ClientLivenessState::ACTIVE) != serving) {
            for (const auto& counter : serving_counters_) {
                counter->Update(serving);
            }
        }
        const bool retaining = next != ClientLivenessState::OFFLINE;
        if (retaining_client_index_ &&
            (previous != ClientLivenessState::OFFLINE) != retaining) {
            retaining_client_index_->Update(retaining_client_id_, retaining);
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
    std::mutex resource_mutex_;
    // One entry per allocatable registration; same-name entries may repeat.
    std::vector<std::shared_ptr<ServingNameCounter>> serving_counters_;
    std::shared_ptr<RetainingClientIndex> retaining_client_index_;
    UUID retaining_client_id_{};
    TimePoint last_liveness_at_;
    TimePoint suspected_since_{};
};

}  // namespace mooncake
