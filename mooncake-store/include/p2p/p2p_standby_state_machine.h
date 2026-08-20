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
 *     │ STOPPED │◄───────────────────────────────────────┐
 *     └────┬────┘                                        │
 *          │ Start()                                     │ Stop()/Error
 *          ▼                                             │
 *     ┌─────────────┐                                    │
 *     │ CONNECTING  │                                    │
 *     └──────┬──────┘                                    │
 *            │ Connected                                 │
 *            ▼                              Connected    │
 *     ┌─────────────┐ Error/Disconnect ┌────────────┐    │
 *     │   SYNCING   │────────────────► │RECONNECTING│────┤
 *     └──────┬──────┘◄──────────────── └──────┬─────┘    │
 *            │ Sync complete               ▲  │          │
 *            ▼                             │  │Watch     │
 *     ┌─────────────┐  Watch broken/   ────┘  │healthy   │
 *     │  WATCHING   │── Disconnect            │          │
 *     └──────┬──────┘◄────────────────────────┘          │
 *            │         Max errors      ┌────────────┐    │
 *            │────────────────────────►│ RECOVERING │────┤
 *            │                         └────────────┘    │
 *            │ Promote()                                 │
 *            ▼                                           │
 *     ┌─────────────┐                                    │
 *     │  PROMOTING  │────────────────────────────────────┤
 *     └──────┬──────┘                                    │
 *            │ Success                                   │
 *            ▼                                           │
 *     ┌─────────────┐                                    │
 *     │  PROMOTED   │────────────────────────────────────┘
 *     └─────────────┘
 */
enum class P2PStandbyState : uint8_t {
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
inline const char* P2PStandbyStateToString(P2PStandbyState state) {
    switch (state) {
        case P2PStandbyState::STOPPED:
            return "STOPPED";
        case P2PStandbyState::CONNECTING:
            return "CONNECTING";
        case P2PStandbyState::SYNCING:
            return "SYNCING";
        case P2PStandbyState::WATCHING:
            return "WATCHING";
        case P2PStandbyState::RECOVERING:
            return "RECOVERING";
        case P2PStandbyState::RECONNECTING:
            return "RECONNECTING";
        case P2PStandbyState::PROMOTING:
            return "PROMOTING";
        case P2PStandbyState::PROMOTED:
            return "PROMOTED";
        case P2PStandbyState::FAILED:
            return "FAILED";
        default:
            return "UNKNOWN";
    }
}

/**
 * @brief Events that trigger state transitions
 */
enum class P2PStandbyEvent : uint8_t {
    // User/system actions
    START,          // Start() called
    STOP,           // Stop() called
    PROMOTE,        // Promote() called
    FORCE_PROMOTE,  // Operator-approved promotion from a degraded standby

    // Connection events
    CONNECTED,          // Successfully connected to etcd
    CONNECTION_FAILED,  // Failed to connect to etcd
    DISCONNECTED,       // Connection lost

    // Sync events
    SYNC_COMPLETE,  // Initial sync completed
    SYNC_FAILED,    // Sync failed

    // Watch events
    WATCH_HEALTHY,    // Watch is healthy and receiving events
    WATCH_BROKEN,     // Watch connection broken
    RESYNC_REQUIRED,  // OpLog history was trimmed; full bootstrap is required

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

inline const char* P2PStandbyEventToString(P2PStandbyEvent event) {
    switch (event) {
        case P2PStandbyEvent::START:
            return "START";
        case P2PStandbyEvent::STOP:
            return "STOP";
        case P2PStandbyEvent::PROMOTE:
            return "PROMOTE";
        case P2PStandbyEvent::FORCE_PROMOTE:
            return "FORCE_PROMOTE";
        case P2PStandbyEvent::CONNECTED:
            return "CONNECTED";
        case P2PStandbyEvent::CONNECTION_FAILED:
            return "CONNECTION_FAILED";
        case P2PStandbyEvent::DISCONNECTED:
            return "DISCONNECTED";
        case P2PStandbyEvent::SYNC_COMPLETE:
            return "SYNC_COMPLETE";
        case P2PStandbyEvent::SYNC_FAILED:
            return "SYNC_FAILED";
        case P2PStandbyEvent::WATCH_HEALTHY:
            return "WATCH_HEALTHY";
        case P2PStandbyEvent::WATCH_BROKEN:
            return "WATCH_BROKEN";
        case P2PStandbyEvent::RESYNC_REQUIRED:
            return "RESYNC_REQUIRED";
        case P2PStandbyEvent::RECOVERY_SUCCESS:
            return "RECOVERY_SUCCESS";
        case P2PStandbyEvent::RECOVERY_FAILED:
            return "RECOVERY_FAILED";
        case P2PStandbyEvent::PROMOTION_SUCCESS:
            return "PROMOTION_SUCCESS";
        case P2PStandbyEvent::PROMOTION_FAILED:
            return "PROMOTION_FAILED";
        case P2PStandbyEvent::MAX_ERRORS_REACHED:
            return "MAX_ERRORS_REACHED";
        case P2PStandbyEvent::FATAL_ERROR:
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
    P2PStandbyState old_state{P2PStandbyState::STOPPED};
    P2PStandbyState new_state{P2PStandbyState::STOPPED};
    std::string reason;
};

/**
 * @brief Callback for state transition notifications
 */
using StateChangeCallback = std::function<void(
    P2PStandbyState old_state, P2PStandbyState new_state, P2PStandbyEvent event)>;

/**
 * @brief Standby State Machine
 *
 * Thread-safe state machine for managing Standby service lifecycle.
 * All state transitions are explicit and logged.
 */
class P2PStandbyStateMachine {
   public:
    P2PStandbyStateMachine();

    /**
     * @brief Get current state (thread-safe)
     */
    P2PStandbyState GetState() const {
        return current_state_.load(std::memory_order_acquire);
    }

    /**
     * @brief Check if in a specific state
     */
    bool IsInState(P2PStandbyState state) const { return GetState() == state; }

    /**
     * @brief Check if service is running (SYNCING, WATCHING, RECOVERING,
     * RECONNECTING, PROMOTING)
     */
    bool IsRunning() const {
        P2PStandbyState s = GetState();
        return s == P2PStandbyState::SYNCING || s == P2PStandbyState::WATCHING ||
               s == P2PStandbyState::RECOVERING ||
               s == P2PStandbyState::RECONNECTING || s == P2PStandbyState::PROMOTING;
    }

    /**
     * @brief Check if connected to etcd
     */
    bool IsConnected() const {
        P2PStandbyState s = GetState();
        return s == P2PStandbyState::SYNCING || s == P2PStandbyState::WATCHING ||
               s == P2PStandbyState::RECOVERING || s == P2PStandbyState::PROMOTING;
    }

    /**
     * @brief Check if watch is healthy
     */
    bool IsWatchHealthy() const { return GetState() == P2PStandbyState::WATCHING; }

    /**
     * @brief Check if ready for promotion
     */
    bool IsReadyForPromotion() const {
        return GetState() == P2PStandbyState::WATCHING;
    }

    /**
     * @brief Process an event and perform state transition
     * @param event The event to process
     * @return Result indicating if transition was allowed and new state
     */
    StateTransitionResult ProcessEvent(P2PStandbyEvent event);

    /**
     * @brief Register a callback for state change notifications
     */
    void RegisterCallback(StateChangeCallback callback);

    /**
     * @brief State transition record for debugging
     */
    struct TransitionRecord {
        std::chrono::steady_clock::time_point timestamp;
        P2PStandbyState from_state;
        P2PStandbyState to_state;
        P2PStandbyEvent event;
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
    StateTransitionResult ValidateTransition(P2PStandbyState from,
                                             P2PStandbyEvent event) const;

    /**
     * @brief Notify all registered callbacks
     */
    void NotifyCallbacks(P2PStandbyState old_state, P2PStandbyState new_state,
                         P2PStandbyEvent event);

    std::atomic<P2PStandbyState> current_state_{P2PStandbyState::STOPPED};
    std::atomic<int> consecutive_errors_{0};
    std::atomic<int> reconnect_count_{0};
    std::chrono::steady_clock::time_point state_enter_time_;

    mutable std::mutex mutex_;
    std::vector<StateChangeCallback> callbacks_;
    std::vector<TransitionRecord> transition_history_;

    static constexpr size_t kMaxHistorySize = 1000;
};

}  // namespace mooncake
