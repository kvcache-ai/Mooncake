// mooncake-store/include/oplog_store.h
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "p2p/ha/oplog/oplog_change_notifier.h"
#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/common/p2p_types.h"

namespace mooncake {

struct OpLogReadProgress {
    uint64_t last_scanned_sequence_id{0};
};

inline std::vector<uint64_t> FindMissingSequenceIds(
    uint64_t start_sequence_id, const std::vector<OpLogEntry>& entries,
    uint64_t last_scanned_sequence_id) {
    std::vector<uint64_t> missing_sequence_ids;
    uint64_t expected_sequence_id = start_sequence_id + 1;
    for (const auto& entry : entries) {
        while (expected_sequence_id < entry.sequence_id) {
            missing_sequence_ids.push_back(expected_sequence_id++);
        }
        expected_sequence_id = entry.sequence_id + 1;
    }
    while (expected_sequence_id <= last_scanned_sequence_id) {
        missing_sequence_ids.push_back(expected_sequence_id++);
    }
    return missing_sequence_ids;
}

// Validate cluster_id for OpLog key prefix construction.
// Returns true if valid or empty; empty is later replaced by
// DEFAULT_CLUSTER_ID.
inline bool NormalizeAndValidateClusterId(std::string& cluster_id) {
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

    // Close write admission and wake waiters without waiting for workers.
    // Backends without an async stop keep their existing shutdown behavior.
    virtual void RequestStopWriting() {}

    // Request cancellation, clean up pending writes, and wait for workers.
    // Must complete before exporting a demotion handoff or destroying the
    // store.
    virtual void StopWriting() {}

    // Read
    virtual ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) = 0;
    virtual ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                                     std::vector<OpLogEntry>& entries) = 0;
    virtual ErrorCode ReadOpLogSinceWithProgress(
        uint64_t start_sequence_id, size_t limit,
        std::vector<OpLogEntry>& entries, OpLogReadProgress& progress) {
        progress.last_scanned_sequence_id = start_sequence_id;
        ErrorCode err = ReadOpLogSince(start_sequence_id, limit, entries);
        if (err == ErrorCode::OK && !entries.empty()) {
            progress.last_scanned_sequence_id = entries.back().sequence_id;
        }
        return err;
    }

    // Sequence ID management
    virtual ErrorCode GetLatestSequenceId(uint64_t& sequence_id) = 0;
    // Return the latest sequence this store instance has locally confirmed as
    // committed. For single-writer/local backends this is equivalent to latest;
    // shared backends may override it to avoid observing another writer's
    // later progress.
    virtual ErrorCode GetLocalCommittedSequenceId(uint64_t& sequence_id) {
        return GetLatestSequenceId(sequence_id);
    }
    virtual ErrorCode GetMaxSequenceId(uint64_t& sequence_id) = 0;
    virtual ErrorCode GetTrimmedSequenceId(uint64_t& sequence_id) {
        sequence_id = 0;
        return ErrorCode::OK;
    }
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
        const std::string& cluster_id, size_t poll_batch_size = 1000) {
        (void)cluster_id;
        (void)poll_batch_size;
        return nullptr;
    }
};

}  // namespace mooncake
