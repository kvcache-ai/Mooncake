#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "oplog_manager.h"

namespace mooncake {

/**
 * @brief Store OpLog entries to etcd for reliable replication
 *
 * This class handles writing OpLog entries to etcd and provides methods
 * for reading and managing OpLog entries in etcd.
 */
class EtcdOpLogStore {
   public:
    /**
     * @brief Constructor
     * @param etcd_endpoints Comma-separated etcd endpoints
     * @param cluster_id Cluster identifier
     */
    EtcdOpLogStore(const std::string& etcd_endpoints,
                   const std::string& cluster_id);

    ~EtcdOpLogStore();

    /**
     * @brief Write a single OpLog entry to etcd
     * @param entry OpLog entry to write
     * @return true on success, false on failure
     */
    bool WriteOpLog(const OpLogEntry& entry);

    /**
     * @brief Write multiple OpLog entries to etcd (batch operation)
     * @param entries OpLog entries to write
     * @return true on success, false on failure
     */
    bool WriteOpLogBatch(const std::vector<OpLogEntry>& entries);

    /**
     * @brief Update the latest sequence ID in etcd
     * @param sequence_id Latest sequence ID
     * @return true on success, false on failure
     */
    bool UpdateLatestSequenceId(uint64_t sequence_id);

    /**
     * @brief Get the latest sequence ID from etcd
     * @return Latest sequence ID, or 0 if not found
     */
    uint64_t GetLatestSequenceId() const;

    /**
     * @brief Record snapshot sequence ID
     * @param snapshot_id Snapshot identifier
     * @param sequence_id Sequence ID at snapshot time
     * @return true on success, false on failure
     */
    bool RecordSnapshotSequenceId(const std::string& snapshot_id,
                                   uint64_t sequence_id);

    /**
     * @brief Get snapshot sequence ID
     * @param snapshot_id Snapshot identifier
     * @return Sequence ID, or 0 if not found
     */
    uint64_t GetSnapshotSequenceId(const std::string& snapshot_id) const;

    /**
     * @brief Read OpLog entries from etcd since a given sequence ID
     * @param start_seq_id Starting sequence ID (exclusive)
     * @param limit Maximum number of entries to read
     * @param entries Output vector of OpLog entries
     * @return true on success, false on failure
     */
    bool ReadOpLogSince(uint64_t start_seq_id, size_t limit,
                        std::vector<OpLogEntry>& entries) const;

    /**
     * @brief Read a single OpLog entry by sequence ID
     * @param sequence_id Sequence ID
     * @param entry Output OpLog entry
     * @return true on success, false on failure
     */
    bool ReadOpLogEntry(uint64_t sequence_id, OpLogEntry& entry) const;

    /**
     * @brief Cleanup OpLog entries before a given sequence ID
     * @param sequence_id Sequence ID (entries with seq_id < sequence_id will be deleted)
     * @return true on success, false on failure
     */
    bool CleanupOpLogBefore(uint64_t sequence_id);

   private:
    /**
     * @brief Build etcd key for OpLog entry
     * @param sequence_id Sequence ID
     * @return etcd key string
     */
    std::string BuildOpLogKey(uint64_t sequence_id) const;

    /**
     * @brief Build etcd key for latest sequence ID
     * @return etcd key string
     */
    std::string BuildLatestSequenceIdKey() const;

    /**
     * @brief Build etcd key for snapshot sequence ID
     * @param snapshot_id Snapshot identifier
     * @return etcd key string
     */
    std::string BuildSnapshotSequenceIdKey(const std::string& snapshot_id) const;

    /**
     * @brief Serialize OpLog entry to JSON string
     * @param entry OpLog entry
     * @return JSON string
     */
    std::string SerializeOpLogEntry(const OpLogEntry& entry) const;

    /**
     * @brief Deserialize OpLog entry from JSON string
     * @param data JSON string
     * @param entry Output OpLog entry
     * @return true on success, false on failure
     */
    bool DeserializeOpLogEntry(const std::string& data,
                               OpLogEntry& entry) const;

    std::string etcd_endpoints_;
    std::string cluster_id_;
    std::string etcd_prefix_;  // e.g., "mooncake-store/oplog"
    
    // etcd client will be added when implementing
    // For now, we use EtcdHelper
};

}  // namespace mooncake

