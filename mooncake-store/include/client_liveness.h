#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string_view>

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

// The liveness state machine of one client incarnation. It reports every
// transition to the caller that caused it and has no side effects of its own:
// whoever mutates a record owns what a transition means. Only the session
// registry does; everyone else holds the read-only ClientSessionSharedPtr.
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
            state_.store(ClientLivenessState::ACTIVE,
                         std::memory_order_release);
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
                    state_.store(ClientLivenessState::SUSPECTED,
                                 std::memory_order_release);
                    return ClientLivenessTransition::BECAME_SUSPECTED;
                }
                break;
            case ClientLivenessState::SUSPECTED:
                if (now - suspected_since_ >= suspicion_ttl) {
                    state_.store(ClientLivenessState::OFFLINE,
                                 std::memory_order_release);
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

// Read-only, shared identity of one client registration. Resources retain this
// handle; mutable liveness records and operation admission belong to the
// registry.
using ClientSessionSharedPtr = std::shared_ptr<const ClientLivenessRecord>;

}  // namespace mooncake
