#include "hot_standby_service.h"

#include <glog/logging.h>

#include <chrono>
#include <thread>

#include "etcd_helper.h"
#include "etcd_oplog_store.h"
#include "master_service.h"
#include "oplog_applier.h"
#include "oplog_manager.h"
#include "oplog_watcher.h"

namespace mooncake {

HotStandbyService::HotStandbyService(const HotStandbyConfig& config)
    : config_(config) {
    metadata_store_ = std::make_unique<StandbyMetadataStore>();
    // OpLogApplier will be created in Start() with cluster_id
    // For now, create without cluster_id (will be updated in Start)
    oplog_applier_ = std::make_unique<OpLogApplier>(metadata_store_.get());
}

// StandbyMetadataStore implementation
bool HotStandbyService::StandbyMetadataStore::Put(const std::string& key,
                                                   const std::string& payload) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_[key] = payload;  // payload may be empty for now
    return true;
}

bool HotStandbyService::StandbyMetadataStore::Remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = store_.find(key);
    if (it != store_.end()) {
        store_.erase(it);
        return true;
    }
    return false;
}

bool HotStandbyService::StandbyMetadataStore::Exists(
    const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return store_.find(key) != store_.end();
}

size_t HotStandbyService::StandbyMetadataStore::GetKeyCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return store_.size();
}

HotStandbyService::~HotStandbyService() {
    Stop();
}

ErrorCode HotStandbyService::Start(const std::string& primary_address,
                                    const std::string& etcd_endpoints,
                                    const std::string& cluster_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (running_.load()) {
        LOG(WARNING) << "HotStandbyService is already running";
        return ErrorCode::OK;
    }

    config_.primary_address = primary_address;
    etcd_endpoints_ = etcd_endpoints;
    cluster_id_ = cluster_id;

#ifdef STORE_USE_ETCD
    // Connect to etcd
    ErrorCode err = EtcdHelper::ConnectToEtcdStoreClient(etcd_endpoints.c_str());
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to etcd: " << etcd_endpoints;
        return err;
    }

    // Recreate OpLogApplier with cluster_id (for requesting missing OpLog)
    oplog_applier_ = std::make_unique<OpLogApplier>(metadata_store_.get(), cluster_id);

    // Create OpLogWatcher
    oplog_watcher_ = std::make_unique<OpLogWatcher>(
        etcd_endpoints, cluster_id, oplog_applier_.get());

    running_.store(true);
    is_connected_.store(true);

    // Read historical OpLog entries first
    // Get the last applied sequence ID from OpLogApplier
    uint64_t last_applied_seq_id = oplog_applier_->GetExpectedSequenceId() - 1;
    if (last_applied_seq_id == 0) {
        // First time - start from sequence_id 0 (will read from sequence_id 1)
        last_applied_seq_id = 0;
    }

    std::vector<OpLogEntry> historical_entries;
    if (oplog_watcher_->ReadOpLogSince(last_applied_seq_id, historical_entries)) {
        LOG(INFO) << "Read " << historical_entries.size()
                  << " historical OpLog entries, applying...";
        // Apply historical entries
        size_t applied_count = oplog_applier_->ApplyOpLogEntries(historical_entries);
        LOG(INFO) << "Applied " << applied_count
                  << " historical OpLog entries";
    } else {
        LOG(WARNING) << "Failed to read historical OpLog entries, continuing anyway";
    }

    // Start OpLogWatcher (this will start watching etcd in background)
    oplog_watcher_->Start();

    // Start background threads
    replication_thread_ = std::thread(&HotStandbyService::ReplicationLoop, this);
    if (config_.enable_verification) {
        verification_thread_ =
            std::thread(&HotStandbyService::VerificationLoop, this);
    }

    LOG(INFO) << "HotStandbyService started, watching etcd OpLog for cluster: "
              << cluster_id;
    return ErrorCode::OK;
#else
    LOG(ERROR) << "STORE_USE_ETCD is not enabled, cannot start HotStandbyService";
    return ErrorCode::INTERNAL_ERROR;
#endif
}

void HotStandbyService::Stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);
    is_connected_.store(false);

    // Stop OpLogWatcher
    if (oplog_watcher_) {
        oplog_watcher_->Stop();
        oplog_watcher_.reset();
    }

    // Wait for threads to finish
    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }
    if (verification_thread_.joinable()) {
        verification_thread_.join();
    }

    LOG(INFO) << "HotStandbyService stopped";
}

StandbySyncStatus HotStandbyService::GetSyncStatus() const {
    StandbySyncStatus status;
    
    // Get applied sequence ID from OpLogApplier
    if (oplog_applier_) {
        status.applied_seq_id = oplog_applier_->GetExpectedSequenceId() - 1;
        if (status.applied_seq_id == 0) {
            status.applied_seq_id = applied_seq_id_.load();  // Fallback
        }
    } else {
        status.applied_seq_id = applied_seq_id_.load();
    }

    // Get primary sequence ID from etcd (if OpLogWatcher is available)
    // For now, we use a placeholder - in full implementation we would
    // query etcd for the latest sequence_id
    status.primary_seq_id = primary_seq_id_.load();
    status.is_connected = is_connected_.load();

    if (status.primary_seq_id > status.applied_seq_id) {
        status.lag_entries = status.primary_seq_id - status.applied_seq_id;
    } else {
        status.lag_entries = 0;
    }

    // Calculate lag time (placeholder - in full implementation this would
    // track actual time differences)
    status.lag_time = std::chrono::milliseconds(0);
    status.is_syncing = running_.load() && is_connected_.load();

    return status;
}

bool HotStandbyService::IsReadyForPromotion() const {
    StandbySyncStatus status = GetSyncStatus();
    if (!status.is_connected) {
        return false;
    }

    // Check if lag is within threshold
    return status.lag_entries <= config_.max_replication_lag_entries;
}

std::unique_ptr<MasterService> HotStandbyService::Promote() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!IsReadyForPromotion()) {
        LOG(ERROR) << "Standby is not ready for promotion. Lag: "
                   << GetSyncStatus().lag_entries << " entries";
        return nullptr;
    }

    LOG(INFO) << "Promoting Standby to Primary. Applied seq_id: "
              << applied_seq_id_.load();

    // Stop replication
    Stop();

    // In full implementation, we would:
    // 1. Create a new MasterService instance
    // 2. Initialize it with the replicated metadata from metadata_store_
    // 3. Return the MasterService instance

    // For now, this is a placeholder
    // TODO: Implement full promotion logic
    auto master_service = std::make_unique<MasterService>();

    LOG(INFO) << "Standby promoted to Primary successfully";
    return master_service;
}

size_t HotStandbyService::GetMetadataCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return metadata_store_ ? metadata_store_->GetKeyCount() : 0;
}

void HotStandbyService::ReplicationLoop() {
    LOG(INFO) << "Replication loop started (etcd-based OpLog sync)";

    // With etcd-based OpLog sync, OpLogWatcher handles the actual watching
    // in its own thread. This loop now just monitors the status and updates
    // metrics.

    while (running_.load()) {
        if (!is_connected_.load()) {
            // Not connected - wait a bit before checking again
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        // Update applied_seq_id from OpLogApplier
        if (oplog_applier_) {
            uint64_t current_applied = oplog_applier_->GetExpectedSequenceId() - 1;
            if (current_applied > 0) {
                applied_seq_id_.store(current_applied);
            }
        }

        // TODO: Update primary_seq_id by querying etcd for latest sequence_id
        // For now, we assume it's being updated elsewhere

        // Sleep and check again
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    LOG(INFO) << "Replication loop stopped";
}

void HotStandbyService::VerificationLoop() {
    LOG(INFO) << "Verification loop started";

    while (running_.load()) {
        std::this_thread::sleep_for(
            std::chrono::seconds(config_.verification_interval_sec));

        if (!is_connected_.load()) {
            continue;
        }

        // In full implementation, this would:
        // 1. Sample keys from local metadata store
        // 2. Calculate checksums
        // 3. Send verification request to Primary
        // 4. Handle mismatches if any

        // Placeholder: Log that verification would happen
        VLOG(1) << "Verification check (placeholder)";
    }

    LOG(INFO) << "Verification loop stopped";
}

void HotStandbyService::ApplyOpLogEntry(const OpLogEntry& entry) {
    // NOTE: This method is deprecated. OpLog entries are now applied via
    // OpLogApplier, which is called by OpLogWatcher. This method is kept
    // for backward compatibility but should not be used in the new etcd-based
    // implementation.

    // Update applied_seq_id for status tracking
    applied_seq_id_.store(entry.sequence_id);

    // The actual application is handled by OpLogApplier via OpLogWatcher
    VLOG(2) << "ApplyOpLogEntry called (deprecated), sequence_id="
            << entry.sequence_id << ", op_type=" << static_cast<int>(entry.op_type)
            << ", key=" << entry.object_key;
}

void HotStandbyService::ProcessOpLogBatch(
    const std::vector<OpLogEntry>& entries) {
    for (const auto& entry : entries) {
        ApplyOpLogEntry(entry);
    }
}

bool HotStandbyService::ConnectToPrimary() {
    // With etcd-based OpLog sync, connection is handled by OpLogWatcher
    // This method is kept for compatibility but is no longer used
    LOG(INFO) << "ConnectToPrimary called (no-op with etcd-based sync)";
    return true;
}

void HotStandbyService::DisconnectFromPrimary() {
    // With etcd-based OpLog sync, disconnection is handled by OpLogWatcher
    // This method is kept for compatibility
    if (is_connected_.load()) {
        is_connected_.store(false);
        replication_stream_.reset();
        LOG(INFO) << "Disconnected from Primary (etcd-based sync)";
    }
}

}  // namespace mooncake

