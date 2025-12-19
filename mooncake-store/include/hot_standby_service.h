#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "oplog_manager.h"
#include "types.h"

namespace mooncake {

// Forward declarations
class MasterService;
class ReplicationStream;

/**
 * @brief Configuration for HotStandbyService
 */
struct HotStandbyConfig {
    std::string standby_id;
    std::string primary_address;
    uint32_t replication_port{0};
    uint32_t verification_interval_sec{30};
    uint32_t max_replication_lag_entries{1000};
    bool enable_verification{true};
};

/**
 * @brief Sync status information for HotStandbyService
 */
struct StandbySyncStatus {
    uint64_t applied_seq_id{0};
    uint64_t primary_seq_id{0};
    uint64_t lag_entries{0};
    std::chrono::milliseconds lag_time{0};
    bool is_syncing{false};
    bool is_connected{false};
};

/**
 * @brief HotStandbyService manages standby replication and promotion
 *
 * This service runs on Standby Master nodes and is responsible for:
 * - Connecting to Primary and receiving OpLog entries
 * - Applying OpLog entries to local metadata store
 * - Periodically verifying data consistency with Primary
 * - Promoting to Primary when elected as new Leader
 *
 * For now, this is a skeleton implementation without actual network
 * communication. The gRPC integration will be added later.
 */
class HotStandbyService {
   public:
    explicit HotStandbyService(const HotStandbyConfig& config);
    ~HotStandbyService();

    /**
     * @brief Start connecting to Primary and begin replication
     * @param primary_address Address of the Primary Master
     * @return ErrorCode::OK on success
     */
    ErrorCode Start(const std::string& primary_address);

    /**
     * @brief Stop replication and disconnect from Primary
     */
    void Stop();

    /**
     * @brief Get current synchronization status
     * @return StandbySyncStatus with current sync state
     */
    StandbySyncStatus GetSyncStatus() const;

    /**
     * @brief Check if standby is ready for promotion
     * @return true if replication lag is within threshold
     */
    bool IsReadyForPromotion() const;

    /**
     * @brief Promote this standby to Primary
     *
     * This method should be called after successful leader election.
     * It returns a MasterService instance initialized with the replicated
     * metadata, ready to serve as the new Primary.
     *
     * @return Unique pointer to MasterService, or nullptr on failure
     */
    std::unique_ptr<MasterService> Promote();

    /**
     * @brief Get the number of metadata entries in the local store
     */
    size_t GetMetadataCount() const;

   private:
    /**
     * @brief Main replication loop (runs in background thread)
     */
    void ReplicationLoop();

    /**
     * @brief Verification loop (runs in background thread)
     */
    void VerificationLoop();

    /**
     * @brief Apply a single OpLog entry to local metadata store
     * @param entry The OpLog entry to apply
     */
    void ApplyOpLogEntry(const OpLogEntry& entry);

    /**
     * @brief Connect to Primary and establish replication stream
     * @return true on success, false on failure
     */
    bool ConnectToPrimary();

    /**
     * @brief Disconnect from Primary
     */
    void DisconnectFromPrimary();

    /**
     * @brief Process a batch of OpLog entries received from Primary
     * @param entries Batch of OpLog entries
     */
    void ProcessOpLogBatch(const std::vector<OpLogEntry>& entries);

    HotStandbyConfig config_;

    // Metadata store (simplified - in full implementation this would be
    // a complete replica of MasterService's metadata)
    // For now, we use a placeholder structure
    struct MetadataStore {
        // Placeholder: In full implementation, this would mirror
        // MasterService's metadata_shards_ structure
        size_t entry_count{0};
    };
    std::unique_ptr<MetadataStore> metadata_store_;

    // Replication state
    std::shared_ptr<ReplicationStream> replication_stream_;
    std::atomic<uint64_t> applied_seq_id_{0};
    std::atomic<uint64_t> primary_seq_id_{0};
    std::atomic<bool> running_{false};
    std::atomic<bool> is_connected_{false};

    // Background threads
    std::thread replication_thread_;
    std::thread verification_thread_;

    // Synchronization
    mutable std::mutex mutex_;
};

}  // namespace mooncake

