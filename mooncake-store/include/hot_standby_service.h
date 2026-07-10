#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "metadata_store.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_replicator.h"
#include "ha/oplog/oplog_store_factory.h"
#include "ha/snapshot/snapshot_provider.h"
#include "standby_state_machine.h"
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

    // Snapshot bootstrap phase (optional): Standby will try to load the latest
    // snapshot baseline before switching to steady-state replication.
    bool enable_snapshot_bootstrap{false};

    // OpLog following phase (optional): when disabled, Start() stops after the
    // snapshot bootstrap phase and keeps the standby in a snapshot-only steady
    // state.
    bool enable_oplog_following{true};

    // OpLog store configuration
    OpLogStoreType oplog_store_type{kDefaultOpLogStoreType};
    std::string oplog_store_root_dir{kDefaultOpLogRootDir};
    int oplog_poll_interval_ms{kDefaultOpLogPollIntervalMs};
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
    StandbyState state{StandbyState::STOPPED};
    std::chrono::milliseconds time_in_state{0};
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
    using SyncStatusCallback = std::function<void(const StandbySyncStatus&)>;

    explicit HotStandbyService(const HotStandbyConfig& config);
    ~HotStandbyService();

    /**
     * @brief Start standby runtime: snapshot bootstrap plus optional OpLog
     * following
     * @param primary_address Address of the Primary Master (not used with
     * OpLog backend-based sync)
     * @param oplog_endpoints Comma-separated OpLog backend endpoints
     * @param cluster_id Cluster identifier for OpLog path
     * @return ErrorCode::OK on success
     */
    ErrorCode Start(const std::string& primary_address,
                    const std::string& oplog_endpoints,
                    const std::string& cluster_id);

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
     * It ensures all Op Logs are applied and transitions the state machine.
     * The caller is responsible for creating the MasterService separately.
     *
     * @return ErrorCode::OK on success, other codes on failure
     */
    ErrorCode Promote();

    /**
     * @brief Get the number of metadata entries in the local store
     */
    size_t GetMetadataCount() const;

    /**
     * @brief Get the latest applied sequence ID after promotion
     *
     * This should be called after Promote() to get the sequence_id
     * that the new Primary's OpLogManager should start from.
     *
     * @return Latest applied sequence ID, or 0 if not available
     */
    uint64_t GetLatestAppliedSequenceId() const;

    // Export a point-in-time snapshot of all replicated metadata.
    // This is used by MasterServiceSupervisor to initialize the new Primary
    // after leader election (fast recovery).
    bool ExportMetadataSnapshot(
        std::vector<std::pair<std::string, StandbyObjectMetadata>>& out) const;

    // Inject a snapshot provider (from external snapshot implementation).
    void SetSnapshotProvider(std::unique_ptr<SnapshotProvider> provider);

    // Notify callers when standby sync status changes. The callback is invoked
    // from existing standby worker threads; no extra monitor thread is created.
    void SetSyncStatusCallback(SyncStatusCallback callback);

    /**
     * @brief Get current state from state machine
     */
    StandbyState GetState() const { return state_machine_.GetState(); }

    /**
     * @brief Get state machine for monitoring/debugging
     */
    const StandbyStateMachine& GetStateMachine() const {
        return state_machine_;
    }

    /**
     * @brief Callback for OpLogReplicator state changes
     * @param event The event to process
     */
    void OnWatcherEvent(StandbyEvent event);

   private:
    ErrorCode PrepareBootstrapBaselineLocked(uint64_t& baseline_seq_id);
    ErrorCode LoadSnapshotBaselineLocked(uint64_t& baseline_seq_id);
    ErrorCode StartOplogFollowingLocked(uint64_t baseline_seq_id);
    void ActivateSnapshotOnlyStandbyLocked(uint64_t baseline_seq_id);
    uint64_t GetLocalLastAppliedSequenceIdLocked() const;
    void ResolvePromotionGapsLocked();
    ErrorCode FinalCatchUpForPromotionLocked(uint64_t current_applied_seq_id);
    void NotifySyncStatus();

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
     * @deprecated Use OpLogApplier instead
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

    // Simple in-memory metadata store implementation
    class StandbyMetadataStore : public MetadataStore {
       public:
        bool PutMetadata(const std::string& key,
                         const StandbyObjectMetadata& metadata) override;
        bool Put(const std::string& key,
                 const std::string& payload = std::string()) override;
        std::optional<StandbyObjectMetadata> GetMetadata(
            const std::string& key) const override;
        bool Remove(const std::string& key) override;
        bool Exists(const std::string& key) const override;
        size_t GetKeyCount() const override;
        void Clear();

        // Snapshot for promotion/restore.
        void Snapshot(
            std::vector<std::pair<std::string, StandbyObjectMetadata>>& out)
            const;

       private:
        mutable std::mutex mutex_;
        std::unordered_map<std::string, StandbyObjectMetadata> store_;
    };
    std::unique_ptr<StandbyMetadataStore> metadata_store_;
    std::unique_ptr<SnapshotProvider> snapshot_provider_{
        std::make_unique<NoopSnapshotProvider>()};

    // OpLog replication components
    std::unique_ptr<OpLogApplier> oplog_applier_;
    std::shared_ptr<OpLogStore> watcher_oplog_store_;
    std::unique_ptr<OpLogChangeNotifier> oplog_change_notifier_;
    std::unique_ptr<OpLogReplicator> oplog_replicator_;

    // Configuration for OpLog sync
    std::string oplog_endpoints_;
    std::string cluster_id_;

    // Replication state
    std::shared_ptr<ReplicationStream> replication_stream_;
    std::atomic<uint64_t> applied_seq_id_{0};
    std::atomic<uint64_t> primary_seq_id_{0};

    // State machine for managing service lifecycle
    StandbyStateMachine state_machine_;

    // Helper methods for state machine
    bool IsRunning() const { return state_machine_.IsRunning(); }
    bool IsConnected() const { return state_machine_.IsConnected(); }

    // Background threads
    std::thread replication_thread_;
    std::thread verification_thread_;

    // Synchronization
    mutable std::mutex mutex_;
    mutable std::mutex sync_status_callback_mutex_;
    SyncStatusCallback sync_status_callback_;
};

}  // namespace mooncake
