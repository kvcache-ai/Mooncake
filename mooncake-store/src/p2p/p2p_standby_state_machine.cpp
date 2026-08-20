#include "p2p/p2p_standby_state_machine.h"

#include <glog/logging.h>

#include "p2p/p2p_ha_metric_manager.h"

namespace mooncake {

P2PStandbyStateMachine::P2PStandbyStateMachine()
    : state_enter_time_(std::chrono::steady_clock::now()) {}

StateTransitionResult P2PStandbyStateMachine::ValidateTransition(
    P2PStandbyState from, P2PStandbyEvent event) const {
    StateTransitionResult result;
    result.allowed = false;
    result.old_state = from;
    result.new_state = from;

    // State transition table
    switch (from) {
        case P2PStandbyState::STOPPED:
            if (event == P2PStandbyEvent::START) {
                result.allowed = true;
                result.new_state = P2PStandbyState::CONNECTING;
            }
            break;

        case P2PStandbyState::CONNECTING:
            switch (event) {
                case P2PStandbyEvent::CONNECTED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::SYNCING;
                    break;
                case P2PStandbyEvent::CONNECTION_FAILED:
                case P2PStandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::FAILED;
                    break;
                case P2PStandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::STOPPED;
                    break;
                default:
                    break;
            }
            break;

        case P2PStandbyState::SYNCING:
            switch (event) {
                case P2PStandbyEvent::SYNC_COMPLETE:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::WATCHING;
                    break;
                case P2PStandbyEvent::SYNC_FAILED:
                case P2PStandbyEvent::DISCONNECTED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::RECONNECTING;
                    break;
                case P2PStandbyEvent::RESYNC_REQUIRED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::SYNCING;
                    break;
                case P2PStandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::STOPPED;
                    break;
                case P2PStandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::FAILED;
                    break;
                default:
                    break;
            }
            break;

        case P2PStandbyState::WATCHING:
            switch (event) {
                case P2PStandbyEvent::WATCH_BROKEN:
                case P2PStandbyEvent::DISCONNECTED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::RECONNECTING;
                    break;
                case P2PStandbyEvent::RESYNC_REQUIRED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::SYNCING;
                    break;
                case P2PStandbyEvent::MAX_ERRORS_REACHED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::RECOVERING;
                    break;
                case P2PStandbyEvent::PROMOTE:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::PROMOTING;
                    break;
                case P2PStandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::STOPPED;
                    break;
                case P2PStandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::FAILED;
                    break;
                // WATCH_HEALTHY in WATCHING state is a no-op (stay in WATCHING)
                case P2PStandbyEvent::WATCH_HEALTHY:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::WATCHING;
                    break;
                default:
                    break;
            }
            break;

        case P2PStandbyState::RECOVERING:
            switch (event) {
                case P2PStandbyEvent::RECOVERY_SUCCESS:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::WATCHING;
                    break;
                case P2PStandbyEvent::RECOVERY_FAILED:
                case P2PStandbyEvent::DISCONNECTED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::RECONNECTING;
                    break;
                case P2PStandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::STOPPED;
                    break;
                case P2PStandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::FAILED;
                    break;
                default:
                    break;
            }
            break;

        case P2PStandbyState::RECONNECTING:
            switch (event) {
                case P2PStandbyEvent::RESYNC_REQUIRED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::SYNCING;
                    break;
                case P2PStandbyEvent::CONNECTED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::SYNCING;
                    break;
                case P2PStandbyEvent::WATCH_HEALTHY:
                    // Watch successfully re-established after reconnect
                    result.allowed = true;
                    result.new_state = P2PStandbyState::WATCHING;
                    break;
                case P2PStandbyEvent::RECOVERY_SUCCESS:
                    // Missed entries synced — ready to watch again
                    result.allowed = true;
                    result.new_state = P2PStandbyState::WATCHING;
                    break;
                case P2PStandbyEvent::RECOVERY_FAILED:
                    // Sync failed — stay in RECONNECTING and retry
                    result.allowed = true;
                    result.new_state = P2PStandbyState::RECONNECTING;
                    break;
                case P2PStandbyEvent::MAX_ERRORS_REACHED:
                case P2PStandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::FAILED;
                    break;
                case P2PStandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::STOPPED;
                    break;
                default:
                    break;
            }
            break;

        case P2PStandbyState::PROMOTING:
            switch (event) {
                case P2PStandbyEvent::PROMOTION_SUCCESS:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::PROMOTED;
                    break;
                case P2PStandbyEvent::PROMOTION_FAILED:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::FAILED;
                    break;
                case P2PStandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = P2PStandbyState::STOPPED;
                    break;
                default:
                    break;
            }
            break;

        case P2PStandbyState::PROMOTED:
            if (event == P2PStandbyEvent::STOP) {
                result.allowed = true;
                result.new_state = P2PStandbyState::STOPPED;
            }
            break;

        case P2PStandbyState::FAILED:
            if (event == P2PStandbyEvent::STOP) {
                result.allowed = true;
                result.new_state = P2PStandbyState::STOPPED;
            } else if (event == P2PStandbyEvent::START) {
                // Allow restart from FAILED state
                result.allowed = true;
                result.new_state = P2PStandbyState::CONNECTING;
            } else if (event == P2PStandbyEvent::FORCE_PROMOTE) {
                result.allowed = true;
                result.new_state = P2PStandbyState::PROMOTING;
            }
            break;
    }

    if (!result.allowed) {
        result.reason = std::string("Invalid transition from ") +
                        P2PStandbyStateToString(from) + " on event " +
                        P2PStandbyEventToString(event);
    }

    return result;
}

StateTransitionResult P2PStandbyStateMachine::ProcessEvent(P2PStandbyEvent event) {
    P2PStandbyState old_state = current_state_.load(std::memory_order_acquire);
    StateTransitionResult result = ValidateTransition(old_state, event);

    std::vector<StateChangeCallback> callbacks_copy;
    if (result.allowed && result.new_state != old_state) {
        std::lock_guard<std::mutex> lock(mutex_);

        // Double-check state hasn't changed (compare-and-swap pattern)
        P2PStandbyState current = current_state_.load(std::memory_order_acquire);
        if (current != old_state) {
            // State changed by another thread, re-validate
            result = ValidateTransition(current, event);
            old_state = current;
            result.old_state = current;
            if (!result.allowed || result.new_state == old_state) {
                return result;
            }
        }

        // Record transition
        TransitionRecord record;
        record.timestamp = std::chrono::steady_clock::now();
        record.from_state = old_state;
        record.to_state = result.new_state;
        record.event = event;

        transition_history_.push_back(record);
        if (transition_history_.size() > kMaxHistorySize) {
            transition_history_.erase(transition_history_.begin());
        }

        // Update state
        current_state_.store(result.new_state, std::memory_order_release);
        state_enter_time_ = record.timestamp;
        P2PHAMetricManager::instance().set_standby_state(
            static_cast<int64_t>(result.new_state));
        P2PHAMetricManager::instance().inc_state_transitions();

        LOG(INFO) << "Standby state transition: "
                  << P2PStandbyStateToString(old_state) << " -> "
                  << P2PStandbyStateToString(result.new_state)
                  << " (event: " << P2PStandbyEventToString(event) << ")";

        // Copy callbacks while holding the lock; invoke them after releasing
        // the lock to avoid deadlock (callbacks may re-enter ProcessEvent).
        callbacks_copy = callbacks_;
    } else if (!result.allowed) {
        VLOG(1) << "Standby state transition rejected: " << result.reason;
    }

    // Notify callbacks outside the lock to avoid deadlock when callbacks
    // re-enter ProcessEvent (e.g. IncrementErrors -> MAX_ERRORS_REACHED).
    for (const auto& callback : callbacks_copy) {
        if (callback) {
            callback(old_state, result.new_state, event);
        }
    }

    return result;
}

void P2PStandbyStateMachine::RegisterCallback(StateChangeCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callbacks_.push_back(std::move(callback));
}

std::vector<P2PStandbyStateMachine::TransitionRecord>
P2PStandbyStateMachine::GetTransitionHistory(size_t max_records) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (transition_history_.size() <= max_records) {
        return transition_history_;
    }

    return std::vector<TransitionRecord>(
        transition_history_.end() - max_records, transition_history_.end());
}

std::chrono::milliseconds P2PStandbyStateMachine::GetTimeInCurrentState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now - state_enter_time_);
}

int P2PStandbyStateMachine::IncrementErrors() {
    int new_count = consecutive_errors_.fetch_add(1) + 1;
    if (new_count >= kMaxConsecutiveErrors) {
        // Trigger MAX_ERRORS_REACHED event
        ProcessEvent(P2PStandbyEvent::MAX_ERRORS_REACHED);
    }
    return new_count;
}

}  // namespace mooncake
