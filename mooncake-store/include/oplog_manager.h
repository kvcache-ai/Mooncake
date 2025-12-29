#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

namespace mooncake {

// Forward declaration
class EtcdOpLogStore;

// Operation types for hot-standby replication.
// This is a minimal subset that can be extended later.
enum class OpType : uint8_t {
    PUT_END = 1,
    PUT_REVOKE = 2,
    REMOVE = 3,
    LEASE_RENEW = 4,
};

// A single operation log entry.
// Note: Payload contains JSON serialized MetadataPayload (defined in metadata_store.h)
// for PUT_END operations, allowing Standby to restore complete metadata.
struct OpLogEntry {
    uint64_t sequence_id{0};     // Monotonically increasing global sequence
    uint64_t timestamp_ms{0};    // Logical timestamp in milliseconds
    OpType op_type{OpType::PUT_END};
    std::string object_key;      // Target object key
    std::string payload;         // Serialized extra data (optional)
    uint32_t checksum{0};        // Checksum of payload (implementation-defined)
    uint32_t prefix_hash{0};     // Hash of the entire key (for verification and optimization)
    uint64_t key_sequence_id{0}; // Per-key sequence ID (for ordering guarantee)
};

/**
 * @brief In-memory operation log manager.
 *
 * This class is intentionally simple: it keeps a bounded deque of OpLogEntry
 * and provides append / get-since primitives. It can later be extended to
 * or to spill to disk if needed. In the new etcd-based design, OpLog will be written to etcd.
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

    // Get the latest assigned sequence id. Returns 0 if no entry exists.
    uint64_t GetLastSequenceId() const;

    // Set the initial sequence ID (used when promoting Standby to Primary).
    // This ensures the new Primary's OpLogManager continues from the correct sequence_id.
    void SetInitialSequenceId(uint64_t sequence_id);

    // Current number of entries in the buffer.
    size_t GetEntryCount() const;


   private:
    static uint64_t NowMs();
    static uint32_t ComputeChecksum(const std::string& data);
    static uint32_t ComputePrefixHash(const std::string& key);

    mutable std::shared_mutex mutex_;
    std::deque<OpLogEntry> buffer_;
    uint64_t first_seq_id_{1};   // sequence_id of buffer_.front()
    uint64_t last_seq_id_{0};    // last assigned sequence_id
    
    // Note: We removed key_sequence_map_ and key_remove_time_map_.
    // Global sequence_id is sufficient for ordering guarantee.
    // All operations are applied in sequence_id order, which ensures consistency.

    // Optional etcd OpLog store for persistent storage
    std::shared_ptr<EtcdOpLogStore> etcd_oplog_store_;

    // Simple bounds to avoid unbounded memory growth.
    static constexpr size_t kMaxBufferEntries_ = 100000;
};

}  // namespace mooncake


