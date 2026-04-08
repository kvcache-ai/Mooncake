// mooncake-store/include/oplog_store.h
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "ha/oplog/oplog_change_notifier.h"
#include "ha/oplog/oplog_manager.h"
#include "types.h"

namespace mooncake {

// Normalize and validate cluster_id for OpLog key prefix construction.
// Strips trailing slashes, then validates the remaining string.
// Returns true if valid (or empty after normalization), false otherwise.
inline bool NormalizeAndValidateClusterId(std::string& cluster_id) {
    while (!cluster_id.empty() && cluster_id.back() == '/') {
        cluster_id.pop_back();
    }
    return cluster_id.empty() || IsValidClusterIdComponent(cluster_id);
}

// Abstract interface for OpLog persistent storage.
// Implementations: EtcdOpLogStore, (future) HdfsOpLogStore, etc.
class OpLogStore {
   public:
    virtual ~OpLogStore() = default;
    virtual ErrorCode Init() = 0;

    // Write
    virtual ErrorCode WriteOpLog(const OpLogEntry& entry, bool sync = true) = 0;

    // Read
    virtual ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) = 0;
    virtual ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                                     std::vector<OpLogEntry>& entries) = 0;

    // Sequence ID management
    virtual ErrorCode GetLatestSequenceId(uint64_t& sequence_id) = 0;
    virtual ErrorCode GetMaxSequenceId(uint64_t& sequence_id) = 0;
    virtual ErrorCode UpdateLatestSequenceId(uint64_t sequence_id) = 0;

    // Snapshot
    virtual ErrorCode RecordSnapshotSequenceId(const std::string& snapshot_id,
                                               uint64_t sequence_id) = 0;
    virtual ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                            uint64_t& sequence_id) = 0;

    // Cleanup
    virtual ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id) = 0;

    // Create a change notifier for this store.
    // Each backend provides its own notifier (e.g., etcd watch, polling).
    // Returns nullptr if the backend does not support change notification.
    virtual std::unique_ptr<OpLogChangeNotifier> CreateChangeNotifier(
        const std::string& cluster_id) {
        (void)cluster_id;
        return nullptr;
    }
};

}  // namespace mooncake
