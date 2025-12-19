#pragma once

#include <cstdint>
#include <deque>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

namespace mooncake {

// Operation types for hot-standby replication.
// This is a minimal subset that can be extended later.
enum class OpType : uint8_t {
    PUT_END = 1,
    PUT_REVOKE = 2,
    REMOVE = 3,
    LEASE_RENEW = 4,
};

// A single operation log entry.
struct OpLogEntry {
    uint64_t sequence_id{0};     // Monotonically increasing sequence
    uint64_t timestamp_ms{0};    // Logical timestamp in milliseconds
    OpType op_type{OpType::PUT_END};
    std::string object_key;      // Target object key
    std::string payload;         // Serialized extra data (optional)
    uint32_t checksum{0};        // Checksum of payload (implementation-defined)
    uint32_t prefix_hash{0};     // Hash of key prefix (for future verification)
};

/**
 * @brief In-memory operation log manager.
 *
 * This class is intentionally simple: it keeps a bounded deque of OpLogEntry
 * and provides append / get-since primitives. It can later be extended to
 * notify ReplicationService or to spill to disk if needed.
 */
class OpLogManager {
   public:
    OpLogManager();

    // Append a new entry and return the assigned sequence_id.
    uint64_t Append(OpType type, const std::string& key,
                    const std::string& payload = std::string());

    // Get entries with sequence_id > since_seq_id, up to at most limit entries.
    std::vector<OpLogEntry> GetEntriesSince(uint64_t since_seq_id,
                                            size_t limit = 1000) const;

    // Get the latest assigned sequence id. Returns 0 if no entry exists.
    uint64_t GetLastSequenceId() const;

    // Truncate all entries with sequence_id < min_seq_to_keep.
    void TruncateBefore(uint64_t min_seq_to_keep);

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

    // Simple bounds to avoid unbounded memory growth.
    static constexpr size_t kMaxBufferEntries_ = 100000;
};

}  // namespace mooncake


