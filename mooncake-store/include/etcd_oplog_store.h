#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <optional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "oplog_manager.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Store for OpLog entries in etcd.
 *
 * This class is responsible for writing OpLog entries to etcd and reading them
 * back. OpLog entries are stored with keys in the format:
 *   /oplog/{cluster_id}/{sequence_id}
 *
 * The latest sequence_id is also stored at:
 *   /oplog/{cluster_id}/latest
 */
class EtcdOpLogStore {
   public:
    /**
     * @brief Constructor.
     * @param cluster_id: The cluster ID for this OpLog store.
     * @param enable_latest_seq_batch_update: Whether to start background thread
     *        to batch-update `/latest`. Readers (Standby) should set this to
     * false to avoid unnecessary thread creation.
     */
    explicit EtcdOpLogStore(const std::string& cluster_id,
                            bool enable_latest_seq_batch_update = false);

    /**
     * @brief Write an OpLog entry to etcd.
     * @param entry: The OpLog entry to write.
     * @param sync: If true, wait until the entry is persisted to etcd.
     *              If false, buffer it and return immediately (Group Commit).
     * @return: Error code.
     */
    ErrorCode WriteOpLog(const OpLogEntry& entry, bool sync = true);

    /**
     * @brief Read an OpLog entry from etcd by sequence_id.
     * @param sequence_id: The sequence ID of the entry to read.
     * @param entry: Output param, the OpLog entry.
     * @return: Error code.
     */
    ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry);

    /**
     * @brief Read OpLog entries starting from a given sequence_id.
     * @param start_sequence_id: The starting sequence ID (exclusive).
     * @param limit: Maximum number of entries to read (default: 1000).
     * @param entries: Output param, vector of OpLog entries.
     * @return: Error code.
     */
    ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                             std::vector<OpLogEntry>& entries);

    // Like ReadOpLogSince, but also returns the etcd revision for consistent
    // "read then watch(from revision+1)" startup.
    ErrorCode ReadOpLogSinceWithRevision(uint64_t start_sequence_id,
                                         size_t limit,
                                         std::vector<OpLogEntry>& entries,
                                         EtcdRevisionId& revision_id);

    /**
     * @brief Get the latest sequence_id from etcd.
     * @param sequence_id: Output param, the latest sequence_id.
     * @return: Error code. ETCD_KEY_NOT_EXIST if no OpLog exists yet.
     */
    ErrorCode GetLatestSequenceId(uint64_t& sequence_id);

    // Stronger (than `/latest`) best-effort query: return the maximum existing
    // sequence_id by scanning etcd keys under /oplog/{cluster_id}/ with
    // descending key order.
    // Return ETCD_KEY_NOT_EXIST if no OpLog exists yet.
    ErrorCode GetMaxSequenceId(uint64_t& sequence_id);

    /**
     * @brief Update the latest sequence_id in etcd.
     * @param sequence_id: The latest sequence_id to update.
     * @return: Error code.
     */
    ErrorCode UpdateLatestSequenceId(uint64_t sequence_id);

    /**
     * @brief Record the sequence_id corresponding to a snapshot.
     * @param snapshot_id: The snapshot ID.
     * @param sequence_id: The sequence_id at which the snapshot was taken.
     * @return: Error code.
     */
    ErrorCode RecordSnapshotSequenceId(const std::string& snapshot_id,
                                       uint64_t sequence_id);

    /**
     * @brief Get the sequence_id for a given snapshot.
     * @param snapshot_id: The snapshot ID.
     * @param sequence_id: Output param, the sequence_id.
     * @return: Error code. ETCD_KEY_NOT_EXIST if snapshot not found.
     */
    ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                    uint64_t& sequence_id);

    /**
     * @brief Clean up OpLog entries before a given sequence_id.
     * @param before_sequence_id: All entries with sequence_id <
     * before_sequence_id will be deleted.
     * @return: Error code.
     */
    ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id);

    /**
     * @brief Destructor - stops batch update thread.
     */
    ~EtcdOpLogStore();

   private:
    /**
     * @brief Build the etcd key for an OpLog entry.
     * @param sequence_id: The sequence ID.
     * @return: The etcd key.
     */
    std::string BuildOpLogKey(uint64_t sequence_id) const;

    /**
     * @brief Build the etcd key for the latest sequence_id.
     * @return: The etcd key.
     */
    std::string BuildLatestKey() const;

    /**
     * @brief Build the etcd key for a snapshot sequence_id.
     * @param snapshot_id: The snapshot ID.
     * @return: The etcd key.
     */
    std::string BuildSnapshotKey(const std::string& snapshot_id) const;

    // Best-effort: find the minimum existing OpLog sequence_id in etcd.
    // Used for robust cleanup (Scheme 3) so we don't rely on a persisted
    // "cleaned_upto" marker.
    std::optional<uint64_t> GetMinSequenceId() const;

    // Best-effort: find the maximum existing OpLog sequence_id in etcd.
    std::optional<uint64_t> GetMaxSequenceIdInternal() const;

    /**
     * @brief Serialize an OpLogEntry to JSON string.
     * @param entry: The OpLog entry to serialize.
     * @return: The JSON string.
     */
    std::string SerializeOpLogEntry(const OpLogEntry& entry) const;

    /**
     * @brief Deserialize a JSON string to OpLogEntry.
     * @param json_str: The JSON string.
     * @param entry: Output param, the OpLog entry.
     * @return: true if successful, false otherwise.
     */
    bool DeserializeOpLogEntry(const std::string& json_str,
                               OpLogEntry& entry) const;

    /**
     * @brief Batch update thread function.
     * Periodically updates latest_sequence_id in etcd.
     */
    void BatchUpdateThread();

    /**
     * @brief Trigger immediate batch update if threshold is reached.
     */
    void TriggerBatchUpdateIfNeeded();

    /**
     * @brief Perform the actual batch update to etcd.
     */
    void DoBatchUpdate();

    std::string cluster_id_;
    static constexpr const char* kOpLogPrefix = "/oplog/";
    static constexpr const char* kLatestSuffix = "/latest";
    static constexpr const char* kSnapshotPrefix = "/oplog/";
    static constexpr const char* kSnapshotSuffix = "/snapshot/";

    // Batch update mechanism for latest_sequence_id
    const bool enable_latest_seq_batch_update_{false};
    std::atomic<uint64_t> pending_latest_seq_id_{0};
    std::atomic<size_t> pending_count_{0};
    std::atomic<bool> batch_update_running_{false};
    std::mutex batch_update_mutex_;
    std::thread batch_update_thread_;
    std::chrono::steady_clock::time_point last_update_time_;

    // Batch update configuration
    static constexpr size_t kBatchSize = 100;      // Update every 100 entries
    static constexpr int kBatchIntervalMs = 1000;  // Or every 1 second

    // Group Commit / Batch Write support
    struct BatchEntry {
        std::string key;
        std::string value;
        uint64_t sequence_id;
        bool is_sync;  // Track if entry requires sync
    };

    void BatchWriteThread();
    void FlushBatch();

    mutable std::mutex batch_mutex_;
    std::deque<BatchEntry> pending_batch_;
    std::condition_variable cv_batch_updated_;   // Notify background thread
    std::condition_variable cv_sync_completed_;  // Notify sync waiters
    std::atomic<bool> batch_write_running_{false};
    std::thread batch_write_thread_;
    std::atomic<uint64_t> last_persisted_seq_id_{0};

    // Configs for OpLog batching
    static constexpr size_t kOpLogBatchSizeLimit =
        1 * 1024 * 1024;  // 1MB payload limit (soft)
    static constexpr size_t kOpLogBatchCountLimit = 100;  // 100 entries
    static constexpr int kOpLogBatchTimeoutMs =
        10;  // 10ms max latency for Async
    static constexpr int kSyncWaitTimeoutMs =
        3000;                                   // 3s timeout for Sync writes
    static constexpr int kFlushRetryCount = 3;  // Retries for failed flush
    static constexpr int kFlushRetryIntervalMs = 50;  // Retry interval
};

}  // namespace mooncake
