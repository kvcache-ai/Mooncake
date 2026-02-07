#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {

/**
 * @brief Standby service states
 *
 * State transition diagram:
 *
 *     ┌─────────┐
 *     │ STOPPED │◄──────────────────────────────────────┐
 *     └────┬────┘                                       │
 *          │ Start()                                    │ Stop()/Error
 *          ▼                                            │
 *     ┌─────────────┐                                   │
 *     │ CONNECTING  │◄──────────────────────┐           │
 *     └──────┬──────┘                       │           │
 *            │ Connected                    │ Reconnect │
 *            ▼                              │           │
 *     ┌─────────────┐    Error/Gap     ┌────┴─────┐     │
 *     │   SYNCING   │─────────────────►│RECOVERING│─────┤
 *     └──────┬──────┘                  └──────────┘     │
 *            │ Sync complete                            │
 *            ▼                                          │
 *     ┌─────────────┐    Watch broken  ┌────────────┐   │
 *     │  WATCHING   │─────────────────►│RECONNECTING│───┤
 *     └──────┬──────┘                  └────────────┘   │
 *            │ Promote()                                │
 *            ▼                                          │
 *     ┌─────────────┐                                   │
 *     │  PROMOTING  │───────────────────────────────────┤
 *     └──────┬──────┘                                   │
 *            │ Success                                  │
 *            ▼                                          │
 *     ┌─────────────┐                                   │
 *     │  PROMOTED   │───────────────────────────────────┘
 *     └─────────────┘
 */
enum class StandbyState : uint8_t {
    // Initial state, service not started
    STOPPED = 0,

    // Connecting to etcd cluster
    CONNECTING = 1,

    // Initial sync: reading historical OpLog entries
    SYNCING = 2,

    // Normal operation: watching for new OpLog entries
    WATCHING = 3,

    // Recovering from error: re-syncing missed entries
    RECOVERING = 4,

    // Reconnecting after watch failure
    RECONNECTING = 5,

    // Promotion in progress: final catch-up before becoming Primary
    PROMOTING = 6,

    // Successfully promoted to Primary
    PROMOTED = 7,

    // Fatal error, cannot recover
    FAILED = 8,
};

/**
 * @brief Get human-readable state name
 */
inline const char* StandbyStateToString(StandbyState state) {
    switch (state) {
        case StandbyState::STOPPED:
            return "STOPPED";
        case StandbyState::CONNECTING:
            return "CONNECTING";
        case StandbyState::SYNCING:
            return "SYNCING";
        case StandbyState::WATCHING:
            return "WATCHING";
        case StandbyState::RECOVERING:
            return "RECOVERING";
        case StandbyState::RECONNECTING:
            return "RECONNECTING";
        case StandbyState::PROMOTING:
            return "PROMOTING";
        case StandbyState::PROMOTED:
            return "PROMOTED";
        case StandbyState::FAILED:
            return "FAILED";
        default:
            return "UNKNOWN";
    }
}

/**
 * @brief Events that trigger state transitions
 */
enum class StandbyEvent : uint8_t {
    // User/system actions
    START,    // Start() called
    STOP,     // Stop() called
    PROMOTE,  // Promote() called

    // Connection events
    CONNECTED,          // Successfully connected to etcd
    CONNECTION_FAILED,  // Failed to connect to etcd
    DISCONNECTED,       // Connection lost

    // Sync events
    SYNC_COMPLETE,  // Initial sync completed
    SYNC_FAILED,    // Sync failed

    // Watch events
    WATCH_HEALTHY,  // Watch is healthy and receiving events
    WATCH_BROKEN,   // Watch connection broken

    // Recovery events
    RECOVERY_SUCCESS,  // Successfully recovered from error
    RECOVERY_FAILED,   // Recovery failed

    // Promotion events
    PROMOTION_SUCCESS,  // Successfully promoted
    PROMOTION_FAILED,   // Promotion failed

    // Error events
    MAX_ERRORS_REACHED,  // Too many consecutive errors
    FATAL_ERROR,         // Unrecoverable error
};

inline const char* StandbyEventToString(StandbyEvent event) {
    switch (event) {
        case StandbyEvent::START:
            return "START";
        case StandbyEvent::STOP:
            return "STOP";
        case StandbyEvent::PROMOTE:
            return "PROMOTE";
        case StandbyEvent::CONNECTED:
            return "CONNECTED";
        case StandbyEvent::CONNECTION_FAILED:
            return "CONNECTION_FAILED";
        case StandbyEvent::DISCONNECTED:
            return "DISCONNECTED";
        case StandbyEvent::SYNC_COMPLETE:
            return "SYNC_COMPLETE";
        case StandbyEvent::SYNC_FAILED:
            return "SYNC_FAILED";
        case StandbyEvent::WATCH_HEALTHY:
            return "WATCH_HEALTHY";
        case StandbyEvent::WATCH_BROKEN:
            return "WATCH_BROKEN";
        case StandbyEvent::RECOVERY_SUCCESS:
            return "RECOVERY_SUCCESS";
        case StandbyEvent::RECOVERY_FAILED:
            return "RECOVERY_FAILED";
        case StandbyEvent::PROMOTION_SUCCESS:
            return "PROMOTION_SUCCESS";
        case StandbyEvent::PROMOTION_FAILED:
            return "PROMOTION_FAILED";
        case StandbyEvent::MAX_ERRORS_REACHED:
            return "MAX_ERRORS_REACHED";
        case StandbyEvent::FATAL_ERROR:
            return "FATAL_ERROR";
        default:
            return "UNKNOWN";
    }
}

/**
 * @brief State transition result
 */
struct StateTransitionResult {
    bool allowed{false};
    StandbyState old_state{StandbyState::STOPPED};
    StandbyState new_state{StandbyState::STOPPED};
    std::string reason;
};

/**
 * @brief Callback for state transition notifications
 */
using StateChangeCallback = std::function<void(
    StandbyState old_state, StandbyState new_state, StandbyEvent event)>;

/**
 * @brief Standby State Machine
 *
 * Thread-safe state machine for managing Standby service lifecycle.
 * All state transitions are explicit and logged.
 */
class StandbyStateMachine {
   public:
    StandbyStateMachine();

    /**
     * @brief Get current state (thread-safe)
     */
    StandbyState GetState() const {
        return current_state_.load(std::memory_order_acquire);
    }

    /**
     * @brief Check if in a specific state
     */
    bool IsInState(StandbyState state) const { return GetState() == state; }

    /**
     * @brief Check if service is running (SYNCING, WATCHING, RECOVERING,
     * RECONNECTING, PROMOTING)
     */
    bool IsRunning() const {
        StandbyState s = GetState();
        return s == StandbyState::SYNCING || s == StandbyState::WATCHING ||
               s == StandbyState::RECOVERING ||
               s == StandbyState::RECONNECTING || s == StandbyState::PROMOTING;
    }

    /**
     * @brief Check if connected to etcd
     */
    bool IsConnected() const {
        StandbyState s = GetState();
        return s == StandbyState::SYNCING || s == StandbyState::WATCHING ||
               s == StandbyState::RECOVERING || s == StandbyState::PROMOTING;
    }

    /**
     * @brief Check if watch is healthy
     */
    bool IsWatchHealthy() const { return GetState() == StandbyState::WATCHING; }

    /**
     * @brief Check if ready for promotion
     */
    bool IsReadyForPromotion() const {
        return GetState() == StandbyState::WATCHING;
    }

    /**
     * @brief Process an event and perform state transition
     * @param event The event to process
     * @return Result indicating if transition was allowed and new state
     */
    StateTransitionResult ProcessEvent(StandbyEvent event);

    /**
     * @brief Register a callback for state change notifications
     */
    void RegisterCallback(StateChangeCallback callback);

    /**
     * @brief State transition record for debugging
     */
    struct TransitionRecord {
        std::chrono::steady_clock::time_point timestamp;
        StandbyState from_state;
        StandbyState to_state;
        StandbyEvent event;
    };

    /**
     * @brief Get state transition history (for debugging)
     */
    std::vector<TransitionRecord> GetTransitionHistory(
        size_t max_records = 100) const;

    /**
     * @brief Get time spent in current state
     */
    std::chrono::milliseconds GetTimeInCurrentState() const;

    /**
     * @brief Get consecutive error count
     */
    int GetConsecutiveErrors() const { return consecutive_errors_.load(); }

    /**
     * @brief Increment consecutive error count
     * @return New error count
     */
    int IncrementErrors();

    /**
     * @brief Reset consecutive error count
     */
    void ResetErrors() { consecutive_errors_.store(0); }

    /**
     * @brief Get reconnect attempt count
     */
    int GetReconnectCount() const { return reconnect_count_.load(); }

    /**
     * @brief Increment reconnect count
     */
    void IncrementReconnectCount() { reconnect_count_.fetch_add(1); }

    /**
     * @brief Reset reconnect count
     */
    void ResetReconnectCount() { reconnect_count_.store(0); }

    // Constants
    static constexpr int kMaxConsecutiveErrors = 10;
    static constexpr int kMaxReconnectAttempts = 100;

   private:
    /**
     * @brief Check if a transition is valid and get new state
     */
    StateTransitionResult ValidateTransition(StandbyState from,
                                             StandbyEvent event) const;

    /**
     * @brief Notify all registered callbacks
     */
    void NotifyCallbacks(StandbyState old_state, StandbyState new_state,
                         StandbyEvent event);

    std::atomic<StandbyState> current_state_{StandbyState::STOPPED};
    std::atomic<int> consecutive_errors_{0};
    std::atomic<int> reconnect_count_{0};
    std::chrono::steady_clock::time_point state_enter_time_;

    mutable std::mutex mutex_;
    std::vector<StateChangeCallback> callbacks_;
    std::vector<TransitionRecord> transition_history_;

    static constexpr size_t kMaxHistorySize = 1000;
};

}  // namespace mooncake
