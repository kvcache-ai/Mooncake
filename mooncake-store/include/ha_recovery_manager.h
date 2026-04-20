#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <boost/functional/hash.hpp>

#include "async_metadata_notifier.h"
#include "data_manager.h"
#include "p2p_master_client.h"
#include "types.h"

namespace mooncake {

/**
 * @class HARecoveryManager
 * @brief Manages client-side HA state machine and multi-phase recovery pipeline
 *        for Master crash recovery.
 *
 * State machine (2 events: MASTER_UNREACHABLE, MASTER_REACHABLE):
 *   FULL ──MASTER_UNREACHABLE─────> DEGRADED
 *   FULL ──MASTER_REACHABLE───────> SYNCING  (Master restarted)
 *   DEGRADED ──MASTER_REACHABLE───> SYNCING  (always full re-sync)
 *   SYNCING ──recovery complete───> FULL
 *   SYNCING ──MASTER_UNREACHABLE──> DEGRADED
 *   SYNCING ──MASTER_REACHABLE────> SYNCING  (restart pipeline)
 *
 * Thread safety:
 *   state_ is atomic for lock-free reads on data-path hot path.
 *   mutex_ protects transitions and recovery thread lifecycle.
 *   need_abort_ (shared atomic bool) signals the recovery thread to stop
 *   without requiring mutex_, enabling safe join from HandleEvent.
 */
class HARecoveryManager {
   public:
    HARecoveryManager(const UUID& client_id, P2PMasterClient& master_client,
                      std::optional<DataManager>& data_manager,
                      std::unique_ptr<AsyncMetadataNotifier>& notifier,
                      std::atomic<ViewVersionId>& view_version);
    ~HARecoveryManager();

    HARecoveryManager(const HARecoveryManager&) = delete;
    HARecoveryManager& operator=(const HARecoveryManager&) = delete;

    void Stop();

    bool IsDegraded() const {
        return state_.load(std::memory_order_acquire) ==
               HAClientState::DEGRADED;
    }

    HAClientState GetState() const {
        return state_.load(std::memory_order_acquire);
    }

    /**
     * @brief Set HA state. Used for degraded startup when master is
     * unavailable.
     */
    void SetState(HAClientState state) {
        state_.store(state, std::memory_order_release);
        LOG(INFO) << "HA state set to: " << state;
    }

    /**
     * @brief Mark that P2PClientService::Init has completed. Called at the end
     * of Init(). Recovery thread will wait for this before accessing
     * data_manager_.
     */
    void SetReadyForRecovery() {
        ready_for_recovery_.store(true, std::memory_order_release);
    }

    void HandleEvent(HAEvent event);

    tl::expected<void, ErrorCode> SetSyncCompleted();

   private:
    using AbortToken = std::shared_ptr<std::atomic<bool>>;

    void TransitionState(HAClientState to, const std::string& reason);
    void StartRecoveryThread();
    void RecoveryPipelineMain(AbortToken need_abort);

    /**
     * @brief Retry enqueue until success or abort is signalled.
     *        Hot keys use the normal (high-priority) queue; recovery keys use
     *        the recovery queue. Sleeps 10 ms between attempts so normal
     *        writes keep priority.
     * @return true on success, false if aborted.
     */
    bool EnqueueWithRetry(const std::string& key, const UUID& tier_id,
                          size_t size, bool is_hot,
                          const AbortToken& need_abort);

    const UUID& client_id_;
    P2PMasterClient& master_client_;
    std::optional<DataManager>& data_manager_;
    std::unique_ptr<AsyncMetadataNotifier>& notifier_;
    std::atomic<ViewVersionId>& view_version_;

    std::atomic<HAClientState> state_{HAClientState::FULL};
    std::atomic<bool> ready_for_recovery_{
        false};         // Set true when P2PClientService::Init completes
    std::mutex mutex_;  // protects transitions + recovery thread lifecycle
    std::thread recovery_thread_;
    AbortToken need_abort_;  // signals recovery thread to exit

    // Separate mutex/CV for interruptible sleeps inside the recovery thread.
    // Must NOT be mutex_ (HandleEvent holds mutex_ while joining the thread).
    std::mutex abort_mutex_;
    std::condition_variable abort_cv_;
};

}  // namespace mooncake
