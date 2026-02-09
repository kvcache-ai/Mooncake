#include "standby_state_machine.h"

#include <glog/logging.h>

namespace mooncake {

StandbyStateMachine::StandbyStateMachine()
    : state_enter_time_(std::chrono::steady_clock::now()) {}

StateTransitionResult StandbyStateMachine::ValidateTransition(
    StandbyState from, StandbyEvent event) const {
    StateTransitionResult result;
    result.allowed = false;
    result.old_state = from;
    result.new_state = from;

    // State transition table
    switch (from) {
        case StandbyState::STOPPED:
            if (event == StandbyEvent::START) {
                result.allowed = true;
                result.new_state = StandbyState::CONNECTING;
            }
            break;

        case StandbyState::CONNECTING:
            switch (event) {
                case StandbyEvent::CONNECTED:
                    result.allowed = true;
                    result.new_state = StandbyState::SYNCING;
                    break;
                case StandbyEvent::CONNECTION_FAILED:
                case StandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = StandbyState::FAILED;
                    break;
                case StandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = StandbyState::STOPPED;
                    break;
                default:
                    break;
            }
            break;

        case StandbyState::SYNCING:
            switch (event) {
                case StandbyEvent::SYNC_COMPLETE:
                    result.allowed = true;
                    result.new_state = StandbyState::WATCHING;
                    break;
                case StandbyEvent::SYNC_FAILED:
                case StandbyEvent::DISCONNECTED:
                    result.allowed = true;
                    result.new_state = StandbyState::RECONNECTING;
                    break;
                case StandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = StandbyState::STOPPED;
                    break;
                case StandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = StandbyState::FAILED;
                    break;
                default:
                    break;
            }
            break;

        case StandbyState::WATCHING:
            switch (event) {
                case StandbyEvent::WATCH_BROKEN:
                case StandbyEvent::DISCONNECTED:
                    result.allowed = true;
                    result.new_state = StandbyState::RECONNECTING;
                    break;
                case StandbyEvent::MAX_ERRORS_REACHED:
                    result.allowed = true;
                    result.new_state = StandbyState::RECOVERING;
                    break;
                case StandbyEvent::PROMOTE:
                    result.allowed = true;
                    result.new_state = StandbyState::PROMOTING;
                    break;
                case StandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = StandbyState::STOPPED;
                    break;
                case StandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = StandbyState::FAILED;
                    break;
                // WATCH_HEALTHY in WATCHING state is a no-op (stay in WATCHING)
                case StandbyEvent::WATCH_HEALTHY:
                    result.allowed = true;
                    result.new_state = StandbyState::WATCHING;
                    break;
                default:
                    break;
            }
            break;

        case StandbyState::RECOVERING:
            switch (event) {
                case StandbyEvent::RECOVERY_SUCCESS:
                    result.allowed = true;
                    result.new_state = StandbyState::WATCHING;
                    break;
                case StandbyEvent::RECOVERY_FAILED:
                case StandbyEvent::DISCONNECTED:
                    result.allowed = true;
                    result.new_state = StandbyState::RECONNECTING;
                    break;
                case StandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = StandbyState::STOPPED;
                    break;
                case StandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = StandbyState::FAILED;
                    break;
                default:
                    break;
            }
            break;

        case StandbyState::RECONNECTING:
            switch (event) {
                case StandbyEvent::CONNECTED:
                    result.allowed = true;
                    result.new_state = StandbyState::SYNCING;
                    break;
                case StandbyEvent::MAX_ERRORS_REACHED:
                case StandbyEvent::FATAL_ERROR:
                    result.allowed = true;
                    result.new_state = StandbyState::FAILED;
                    break;
                case StandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = StandbyState::STOPPED;
                    break;
                default:
                    break;
            }
            break;

        case StandbyState::PROMOTING:
            switch (event) {
                case StandbyEvent::PROMOTION_SUCCESS:
                    result.allowed = true;
                    result.new_state = StandbyState::PROMOTED;
                    break;
                case StandbyEvent::PROMOTION_FAILED:
                    result.allowed = true;
                    result.new_state = StandbyState::FAILED;
                    break;
                case StandbyEvent::STOP:
                    result.allowed = true;
                    result.new_state = StandbyState::STOPPED;
                    break;
                default:
                    break;
            }
            break;

        case StandbyState::PROMOTED:
            if (event == StandbyEvent::STOP) {
                result.allowed = true;
                result.new_state = StandbyState::STOPPED;
            }
            break;

        case StandbyState::FAILED:
            if (event == StandbyEvent::STOP) {
                result.allowed = true;
                result.new_state = StandbyState::STOPPED;
            } else if (event == StandbyEvent::START) {
                // Allow restart from FAILED state
                result.allowed = true;
                result.new_state = StandbyState::CONNECTING;
            }
            break;
    }

    if (!result.allowed) {
        result.reason = std::string("Invalid transition from ") +
                        StandbyStateToString(from) + " on event " +
                        StandbyEventToString(event);
    }

    return result;
}

StateTransitionResult StandbyStateMachine::ProcessEvent(StandbyEvent event) {
    StandbyState old_state = current_state_.load(std::memory_order_acquire);
    StateTransitionResult result = ValidateTransition(old_state, event);

    std::vector<StateChangeCallback> callbacks_copy;
    if (result.allowed && result.new_state != old_state) {
        std::lock_guard<std::mutex> lock(mutex_);

        // Double-check state hasn't changed (compare-and-swap pattern)
        StandbyState current = current_state_.load(std::memory_order_acquire);
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

        LOG(INFO) << "Standby state transition: "
                  << StandbyStateToString(old_state) << " -> "
                  << StandbyStateToString(result.new_state)
                  << " (event: " << StandbyEventToString(event) << ")";

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

void StandbyStateMachine::RegisterCallback(StateChangeCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callbacks_.push_back(std::move(callback));
}

std::vector<StandbyStateMachine::TransitionRecord>
StandbyStateMachine::GetTransitionHistory(size_t max_records) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (transition_history_.size() <= max_records) {
        return transition_history_;
    }

    return std::vector<TransitionRecord>(
        transition_history_.end() - max_records, transition_history_.end());
}

std::chrono::milliseconds StandbyStateMachine::GetTimeInCurrentState() const {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now - state_enter_time_);
}

int StandbyStateMachine::IncrementErrors() {
    int new_count = consecutive_errors_.fetch_add(1) + 1;
    if (new_count >= kMaxConsecutiveErrors) {
        // Trigger MAX_ERRORS_REACHED event
        ProcessEvent(StandbyEvent::MAX_ERRORS_REACHED);
    }
    return new_count;
}

}  // namespace mooncake
