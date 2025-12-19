#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "oplog_manager.h"
#include "types.h"

namespace mooncake {

// Forward declarations
class MasterService;
class OpLogManager;

/**
 * @brief Replication stream interface (placeholder for future gRPC implementation)
 *
 * This is a minimal interface that will be replaced with actual gRPC streaming
 * in the future. For now, it serves as a placeholder to establish the
 * architecture.
 */
class ReplicationStream {
   public:
    virtual ~ReplicationStream() = default;
    virtual bool Send(const std::vector<OpLogEntry>& entries) = 0;
    virtual bool IsConnected() const = 0;
};

/**
 * @brief Verification request/response structures
 */
struct VerificationRequest {
    std::string standby_id;
    std::vector<std::pair<std::string, uint32_t>> samples;  // (key, checksum)
    uint32_t prefix_hash;
};

struct VerificationResponse {
    std::vector<std::string> mismatched_keys;  // Keys with checksum mismatch
    bool is_consistent;
};

/**
 * @brief ReplicationService manages OpLog replication from Primary to Standbys
 *
 * This service runs on the Primary Master and is responsible for:
 * - Broadcasting OpLog entries to all connected Standby nodes
 * - Tracking replication lag for each Standby
 * - Handling verification requests from Standbys
 *
 * For now, this is a skeleton implementation without actual network
 * communication. The gRPC integration will be added later.
 */
class ReplicationService {
   public:
    explicit ReplicationService(OpLogManager& oplog_manager,
                                MasterService& master_service);

    ~ReplicationService();

    /**
     * @brief Register a new Standby connection
     * @param standby_id Unique identifier for the Standby node
     * @param stream Replication stream for sending OpLog entries
     */
    void RegisterStandby(const std::string& standby_id,
                         std::shared_ptr<ReplicationStream> stream);

    /**
     * @brief Unregister a Standby (when it disconnects)
     * @param standby_id Unique identifier for the Standby node
     */
    void UnregisterStandby(const std::string& standby_id);

    /**
     * @brief Called by OpLogManager when a new entry is appended
     * @param entry The newly appended OpLog entry
     *
     * This method should be called by OpLogManager (via callback) or
     * directly from MasterService after appending to OpLog.
     */
    void OnNewOpLog(const OpLogEntry& entry);

    /**
     * @brief Handle verification request from a Standby
     * @param request Verification request containing checksums
     * @return Verification response with mismatched keys
     */
    VerificationResponse HandleVerification(const VerificationRequest& request);

    /**
     * @brief Get replication lag for each Standby
     * @return Map of standby_id -> lag in sequence IDs
     */
    std::map<std::string, uint64_t> GetReplicationLag() const;

    /**
     * @brief Get the number of connected Standbys
     */
    size_t GetStandbyCount() const;

   private:
    /**
     * @brief Broadcast an OpLog entry to all connected Standbys
     * @param entry The OpLog entry to broadcast
     */
    void BroadcastEntry(const OpLogEntry& entry);

    /**
     * @brief Send a batch of OpLog entries to a specific Standby
     * @param standby_id Target Standby identifier
     * @param entries Batch of OpLog entries to send
     */
    void SendBatch(const std::string& standby_id,
                   const std::vector<OpLogEntry>& entries);

    /**
     * @brief State for each connected Standby
     */
    struct StandbyState {
        std::shared_ptr<ReplicationStream> stream;
        uint64_t acked_seq_id{0};  // Last acknowledged sequence ID
        std::chrono::steady_clock::time_point last_ack_time;
        std::vector<OpLogEntry> pending_batch;  // Batched entries
    };

    OpLogManager& oplog_manager_;
    MasterService& master_service_;

    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, StandbyState> standbys_;

    // Batch configuration
    static constexpr size_t kBatchSize = 100;
    static constexpr uint32_t kBatchTimeoutMs = 10;
};

}  // namespace mooncake

