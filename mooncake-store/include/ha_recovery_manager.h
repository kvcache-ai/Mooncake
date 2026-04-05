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
                      ViewVersionId& view_version);
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

    void HandleEvent(HAEvent event);

    tl::expected<void, ErrorCode> SetSyncCompleted();

   private:
    using AbortToken = std::shared_ptr<std::atomic<bool>>;

    void TransitionState(HAClientState to, const std::string& reason);
    void StartRecoveryThread();
    void RecoveryPipelineMain(AbortToken need_abort);

    /** @brief TryEnqueue with back-off, yielding priority to normal writes. */
    bool RecoveryEnqueueAdd(const std::string& key, const UUID& tier_id,
                            size_t size, const AbortToken& need_abort);

    const UUID& client_id_;
    P2PMasterClient& master_client_;
    std::optional<DataManager>& data_manager_;
    std::unique_ptr<AsyncMetadataNotifier>& notifier_;
    ViewVersionId& view_version_;

    std::atomic<HAClientState> state_{HAClientState::FULL};
    std::mutex mutex_;  // protects transitions + recovery thread lifecycle
    std::thread recovery_thread_;
    AbortToken need_abort_;  // signals recovery thread to exit
};

}  // namespace mooncake
