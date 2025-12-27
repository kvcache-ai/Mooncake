#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "oplog_manager.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Store for OpLog entries in etcd.
 *
 * This class is responsible for writing OpLog entries to etcd and reading them back.
 * OpLog entries are stored with keys in the format:
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
     */
    explicit EtcdOpLogStore(const std::string& cluster_id);

    /**
     * @brief Write an OpLog entry to etcd.
     * @param entry: The OpLog entry to write.
     * @return: Error code.
     */
    ErrorCode WriteOpLog(const OpLogEntry& entry);

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

    /**
     * @brief Get the latest sequence_id from etcd.
     * @param sequence_id: Output param, the latest sequence_id.
     * @return: Error code. ETCD_KEY_NOT_EXIST if no OpLog exists yet.
     */
    ErrorCode GetLatestSequenceId(uint64_t& sequence_id);

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
     * @param before_sequence_id: All entries with sequence_id < before_sequence_id
     *                            will be deleted.
     * @return: Error code.
     */
    ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id);

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

    std::string cluster_id_;
    static constexpr const char* kOpLogPrefix = "/oplog/";
    static constexpr const char* kLatestSuffix = "/latest";
    static constexpr const char* kSnapshotPrefix = "/oplog/";
    static constexpr const char* kSnapshotSuffix = "/snapshot/";
};

}  // namespace mooncake
