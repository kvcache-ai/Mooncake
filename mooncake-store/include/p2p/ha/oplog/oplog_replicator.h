#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "p2p/ha/oplog/oplog_change_notifier.h"
#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/p2p_standby_state_machine.h"
#include "types.h"

namespace mooncake {

// Forward declarations
class P2POpLogApplierBase;

// Callback type for state events
using ReplicatorStateCallback = std::function<void(P2PStandbyEvent)>;

/**
 * @brief Replicate OpLog entries from a remote source and apply them locally.
 *
 * Delegates watch/notification to an OpLogChangeNotifier and applies
 * received entries via P2POpLogApplierBase. This class is a thin orchestration
 * layer; the actual watch implementation lives in OpLogChangeNotifier.
 */
class OpLogReplicator {
   public:
    /**
     * @brief Constructor
     * @param notifier Change notifier that delivers OpLog entries
     * @param applier OpLog applier to process entries
     */
    OpLogReplicator(OpLogChangeNotifier* notifier, P2POpLogApplierBase* applier);

    ~OpLogReplicator();

    /**
     * @brief Start replication from the beginning.
     */
    void Start();

    /**
     * @brief Start from a known last-applied sequence_id.
     */
    bool StartFromSequenceId(uint64_t start_seq_id);

    /**
     * @brief Stop replication.
     */
    void Stop();

    /**
     * @brief Get the last processed sequence ID.
     */
    uint64_t GetLastProcessedSequenceId() const;

    /**
     * @brief Set callback for state events.
     */
    void SetStateCallback(ReplicatorStateCallback callback) {
        state_callback_ = std::move(callback);
    }

    /**
     * @brief Check if replication is healthy.
     */
    bool IsHealthy() const;

   private:
    void AdvanceLastProcessedSequenceId(uint64_t sequence_id);
    void ReportApplyFailureIfNeeded();

    void NotifyStateEvent(P2PStandbyEvent event) {
        if (state_callback_) {
            state_callback_(event);
        }
    }

    OpLogChangeNotifier* notifier_;
    P2POpLogApplierBase* applier_;
    std::atomic<uint64_t> last_processed_sequence_id_{0};
    std::atomic<bool> running_{false};
    std::atomic<bool> apply_failure_reported_{false};

    ReplicatorStateCallback state_callback_;
};

}  // namespace mooncake
