#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "oplog_manager.h"
#include "standby_state_machine.h"
#include "types.h"

namespace mooncake {

// Forward declaration
class OpLogApplier;

// Callback type for state events
using WatcherStateCallback = std::function<void(StandbyEvent)>;

/**
 * @brief Watch etcd for OpLog changes and apply them to Standby
 *
 * This class watches etcd for new OpLog entries and forwards them
 * to OpLogApplier for processing.
 */
class OpLogWatcher {
   public:
    /**
     * @brief Constructor
     * @param etcd_endpoints Comma-separated etcd endpoints
     * @param cluster_id Cluster identifier
     * @param applier OpLog applier to process entries
     */
    OpLogWatcher(const std::string& etcd_endpoints,
                 const std::string& cluster_id, OpLogApplier* applier);

    ~OpLogWatcher();

    /**
     * @brief Start watching etcd for OpLog changes
     */
    void Start();

    /**
     * @brief Start from a known last-applied sequence_id.
     *
     * It will read historical OpLogs at a consistent etcd revision, then start
     * watch from revision+1 to close the gap between "read" and "watch".
     */
    bool StartFromSequenceId(uint64_t start_seq_id);

    /**
     * @brief Stop watching
     */
    void Stop();

    /**
     * @brief Get the last processed sequence ID
     * @return Last processed sequence ID
     */
    uint64_t GetLastProcessedSequenceId() const;

    /**
     * @brief Set callback for state events
     * @param callback Callback function to invoke on state events
     */
    void SetStateCallback(WatcherStateCallback callback) {
        state_callback_ = std::move(callback);
    }

    /**
     * @brief Check if watch is healthy
     */
    bool IsWatchHealthy() const { return watch_healthy_.load(); }

   private:
    /**
     * @brief Notify state callback
     */
    void NotifyStateEvent(StandbyEvent event) {
        if (state_callback_) {
            state_callback_(event);
        }
    }

    bool ReadOpLogSince(uint64_t start_seq_id, std::vector<OpLogEntry>& entries,
                        EtcdRevisionId& revision_id);
    // Callback includes etcd KV mod_revision for precise resume.
    static void WatchCallback(void* context, const char* key, size_t key_size,
                              const char* value, size_t value_size,
                              int event_type, int64_t mod_revision);

    /**
     * @brief Watch etcd OpLog changes (runs in background thread)
     */
    void WatchOpLog();

    /**
     * @brief Process a Watch event
     * @param key etcd key
     * @param value etcd value (JSON string for PUT events, empty for DELETE
     * events)
     * @param event_type Event type (0 = PUT, 1 = DELETE)
     */
    void HandleWatchEvent(const std::string& key, const std::string& value,
                          int event_type);
    void HandleWatchEvent(const std::string& key, const std::string& value,
                          int event_type, int64_t mod_revision);

    /**
     * @brief Deserialize OpLogEntry from JSON string
     * @param json_str JSON string
     * @param entry Output OpLog entry
     * @return true on success, false on failure
     */
    bool DeserializeOpLogEntry(const std::string& json_str, OpLogEntry& entry);

    /**
     * @brief Attempt to reconnect after watch failure
     */
    void TryReconnect();

    /**
     * @brief Sync missed OpLog entries after reconnection
     * @return true if sync was successful
     */
    bool SyncMissedEntries();

    // Next watch revision (0 means from now). Updated by consistent reads.
    std::atomic<int64_t> next_watch_revision_{0};

    std::string etcd_endpoints_;
    std::string cluster_id_;
    OpLogApplier* applier_;
    std::atomic<bool> running_{false};
    std::thread watch_thread_;
    std::atomic<uint64_t> last_processed_sequence_id_{0};

    // Error handling and recovery
    std::atomic<int> consecutive_errors_{0};
    std::atomic<int> reconnect_count_{0};
    std::atomic<bool> watch_healthy_{false};

    // State callback for notifying HotStandbyService
    WatcherStateCallback state_callback_;

    // Constants for error handling
    static constexpr int kMaxConsecutiveErrors = 10;
    static constexpr int kReconnectDelayMs = 1000;
    static constexpr int kMaxReconnectDelayMs = 30000;
    static constexpr int kSyncBatchSize = 1000;
};

}  // namespace mooncake
