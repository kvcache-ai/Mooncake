#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Forward declaration
class EtcdOpLogStore;

// Operation types for hot-standby replication.
// This is a minimal subset that can be extended later.
enum class OpType : uint8_t {
    PUT_END = 1,
    PUT_REVOKE = 2,
    REMOVE = 3,
    // Deprecated: LEASE_RENEW is intentionally not recorded in OpLog in the
    // current etcd-based hot-standby design (Standby relies on Primary DELETE
    // operations).
    LEASE_RENEW = 4,
};

// A single operation log entry.
// Note: Payload contains JSON serialized MetadataPayload (defined in
// metadata_store.h) for PUT_END operations, allowing Standby to restore
// complete metadata.
struct OpLogEntry {
    uint64_t sequence_id{0};   // Monotonically increasing global sequence
    uint64_t timestamp_ms{0};  // Logical timestamp in milliseconds
    OpType op_type{OpType::PUT_END};
    std::string object_key;  // Target object key
    std::string payload;     // Serialized extra data (optional)
    uint32_t checksum{0};    // Checksum of payload (implementation-defined)
    uint32_t prefix_hash{
        0};  // Hash of the entire key (for verification and optimization)
};

/**
 * @brief In-memory operation log manager.
 *
 * This class is intentionally simple: it keeps a bounded deque of OpLogEntry
 * and provides append / get-since primitives. It can later be extended to
 * or to spill to disk if needed. In the new etcd-based design, OpLog will be
 * written to etcd.
 */
class OpLogManager {
   public:
    OpLogManager();

    // Set the EtcdOpLogStore for writing OpLog to etcd (optional).
    // If not set, OpLog will only be stored in memory buffer.
    void SetEtcdOpLogStore(std::shared_ptr<EtcdOpLogStore> etcd_oplog_store);

    // Append a new entry and return the assigned sequence_id.
    uint64_t Append(OpType type, const std::string& key,
                    const std::string& payload = std::string());

    // Allocate a new OpLogEntry with a reserved sequence_id, append it to the
    // in-memory buffer, and return the full entry.
    //
    // IMPORTANT: This will advance last_seq_id_ even if the caller later fails
    // to persist it to etcd. This supports "seq pre-allocation" semantics where
    // retries use the same (smaller) sequence_id.
    OpLogEntry AllocateEntry(OpType type, const std::string& key,
                             const std::string& payload = std::string());

    // Persist an already-allocated entry to etcd using its sequence_id.
    // Does NOT modify sequence counters.
    ErrorCode PersistEntryToEtcd(const OpLogEntry& entry) const;

    // Append a new entry and durably persist it to etcd (if EtcdOpLogStore is
    // set).
    //
    // This is intended for operations that may free/reuse memory (e.g. REMOVE),
    // where best-effort replication is unsafe: Standby must observe the DELETE
    // before promotion, otherwise it may return stale descriptors that point to
    // reused memory and cause silent data corruption.
    //
    // Design (updated for seq pre-allocation):
    // - sequence_id is allocated first and never reused.
    // - If etcd write fails, caller may retry PersistEntryToEtcd with the same
    //   entry (sequence_id fixed and "smaller" than later entries).
    tl::expected<uint64_t, ErrorCode> AppendAndPersist(
        OpType type, const std::string& key,
        const std::string& payload = std::string());

    // Get the latest assigned sequence id. Returns 0 if no entry exists.
    uint64_t GetLastSequenceId() const;

    // Set the initial sequence ID (used when promoting Standby to Primary).
    // This ensures the new Primary's OpLogManager continues from the correct
    // sequence_id.
    void SetInitialSequenceId(uint64_t sequence_id);

    // Current number of entries in the buffer.
    size_t GetEntryCount() const;

    // Verify checksum of an OpLogEntry payload.
    // Returns true if checksum matches, false otherwise.
    // This is public so OpLogWatcher and OpLogApplier can validate entries.
    static bool VerifyChecksum(const OpLogEntry& entry);

    // Basic DoS protection for externally sourced OpLog entries (etcd watch /
    // reads). Enforce conservative bounds on key/payload sizes before
    // parsing/applying.
    static constexpr size_t kMaxObjectKeySize = 4096;            // 4 KiB
    static constexpr size_t kMaxPayloadSize = 10 * 1024 * 1024;  // 10 MiB

    // Validate OpLogEntry key/payload sizes. If invalid, returns false and
    // optionally sets a human-readable reason.
    static bool ValidateEntrySize(const OpLogEntry& entry,
                                  std::string* reason = nullptr);

   private:
    static uint64_t NowMs();
    static uint32_t ComputeChecksum(const std::string& data);
    static uint32_t ComputePrefixHash(const std::string& key);

    mutable std::shared_mutex mutex_;
    std::deque<OpLogEntry> buffer_;
    uint64_t first_seq_id_{1};  // sequence_id of buffer_.front()
    uint64_t last_seq_id_{0};   // last assigned sequence_id

    // Note: We removed key_sequence_map_ and key_remove_time_map_.
    // Global sequence_id is sufficient for ordering guarantee.
    // All operations are applied in sequence_id order, which ensures
    // consistency.

    // Optional etcd OpLog store for persistent storage
    std::shared_ptr<EtcdOpLogStore> etcd_oplog_store_;

    // Simple bounds to avoid unbounded memory growth.
    static constexpr size_t kMaxBufferEntries_ = 100000;
};

}  // namespace mooncake
