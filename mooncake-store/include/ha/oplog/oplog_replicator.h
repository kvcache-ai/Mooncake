#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "ha/oplog/oplog_change_notifier.h"
#include "ha/oplog/oplog_manager.h"
#include "standby_state_machine.h"
#include "types.h"

namespace mooncake {

// Forward declarations
class OpLogApplier;

// Callback type for state events
using ReplicatorStateCallback = std::function<void(StandbyEvent)>;

/**
 * @brief Replicate OpLog entries from a remote source and apply them locally.
 *
 * Delegates watch/notification to an OpLogChangeNotifier and applies
 * received entries via OpLogApplier. This class is a thin orchestration
 * layer; the actual watch implementation lives in OpLogChangeNotifier.
 */
class OpLogReplicator {
   public:
    /**
     * @brief Constructor
     * @param notifier Change notifier that delivers OpLog entries
     * @param applier OpLog applier to process entries
     */
    OpLogReplicator(OpLogChangeNotifier* notifier, OpLogApplier* applier);

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
    void NotifyStateEvent(StandbyEvent event) {
        if (state_callback_) {
            state_callback_(event);
        }
    }

    OpLogChangeNotifier* notifier_;
    OpLogApplier* applier_;
    std::atomic<uint64_t> last_processed_sequence_id_{0};
    std::atomic<bool> running_{false};

    ReplicatorStateCallback state_callback_;
};

}  // namespace mooncake
