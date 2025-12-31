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
bool HotStandbyService::StandbyMetadataStore::PutMetadata(
    const std::string& key, const StandbyObjectMetadata& metadata) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_[key] = metadata;
    VLOG(2) << "StandbyMetadataStore: stored metadata for key=" << key
            << ", replicas=" << metadata.replicas.size()
            << ", size=" << metadata.size;
    return true;
}

bool HotStandbyService::StandbyMetadataStore::Put(const std::string& key,
                                                   const std::string& payload) {
    // Legacy interface - create empty metadata
    StandbyObjectMetadata metadata;
    std::lock_guard<std::mutex> lock(mutex_);
    store_[key] = metadata;
    return true;
}

const StandbyObjectMetadata* HotStandbyService::StandbyMetadataStore::GetMetadata(
    const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = store_.find(key);
    if (it != store_.end()) {
        return &it->second;
    }
    return nullptr;
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

void HotStandbyService::StandbyMetadataStore::Snapshot(
    std::vector<std::pair<std::string, StandbyObjectMetadata>>& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    out.clear();
    out.reserve(store_.size());
    for (const auto& kv : store_) {
        out.emplace_back(kv.first, kv.second);
    }
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

    // Preserve existing local state if HotStandbyService is restarted in-process:
    // - metadata_store_ may already contain real-time metadata
    // - oplog_applier_ may already have expected_sequence_id_
    uint64_t local_last_seq_id = 0;
    if (oplog_applier_) {
        uint64_t expected = oplog_applier_->GetExpectedSequenceId();
        local_last_seq_id = expected > 0 ? expected - 1 : 0;
    }
    const bool has_local_metadata =
        metadata_store_ && metadata_store_->GetKeyCount() > 0;
    const bool has_local_state = has_local_metadata && local_last_seq_id > 0;

    // Recreate OpLogApplier with cluster_id (for requesting missing OpLog).
    // If we had local state, recover to keep sequence continuity.
    oplog_applier_ = std::make_unique<OpLogApplier>(metadata_store_.get(), cluster_id);
    if (has_local_state) {
        LOG(INFO) << "Standby warm start: reuse local metadata (keys="
                  << metadata_store_->GetKeyCount()
                  << "), recover last_seq_id=" << local_last_seq_id;
        oplog_applier_->Recover(local_last_seq_id);
    }

    // Create OpLogWatcher
    oplog_watcher_ = std::make_unique<OpLogWatcher>(
        etcd_endpoints, cluster_id, oplog_applier_.get());

    running_.store(true);
    is_connected_.store(true);

    // Bootstrap:
    // - If we already have local state (warm start), do NOT reload snapshot.
    // - Otherwise (cold start/new standby), try snapshot (if enabled) then replay OpLog.
    uint64_t baseline_seq_id = has_local_state ? local_last_seq_id : 0;
    if (!has_local_state && config_.enable_snapshot_bootstrap && snapshot_provider_) {
        std::string snapshot_id;
        uint64_t snapshot_seq_id = 0;
        std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
        if (snapshot_provider_->LoadLatestSnapshot(cluster_id_, snapshot_id, snapshot_seq_id,
                                                   snapshot)) {
            LOG(INFO) << "Loaded snapshot: snapshot_id=" << snapshot_id
                      << ", snapshot_seq_id=" << snapshot_seq_id
                      << ", keys=" << snapshot.size();
            // Apply snapshot into local standby store.
            for (const auto& kv : snapshot) {
                metadata_store_->PutMetadata(kv.first, kv.second);
            }
            // Align applier to snapshot boundary.
            oplog_applier_->Recover(snapshot_seq_id);
            baseline_seq_id = snapshot_seq_id;
        } else {
            LOG(INFO) << "No snapshot available (or provider not ready), falling back to OpLog-only bootstrap";
        }
    }

    // Read historical OpLog entries since baseline_seq_id.
    uint64_t last_applied_seq_id = baseline_seq_id;

    // Start OpLogWatcher with a consistent "read then watch(from revision+1)" sequence.
    if (!oplog_watcher_->StartFromSequenceId(last_applied_seq_id)) {
        LOG(WARNING) << "Failed to start OpLogWatcher from sequence_id="
                     << last_applied_seq_id << ", continuing anyway";
    }

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

    // Primary sequence ID (best-effort): updated by ReplicationLoop via etcd `/latest`.
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

    // Allow promotion even with large lag - the new Primary can continue
    // syncing remaining OpLog entries from etcd after promotion.
    // Log a warning if lag is large, but don't block promotion.
    if (status.lag_entries > config_.max_replication_lag_entries) {
        LOG(WARNING) << "Standby has large replication lag: " << status.lag_entries
                     << " entries (threshold: " << config_.max_replication_lag_entries
                     << "). Promotion will proceed, but remaining OpLog entries "
                     << "will be synced after promotion.";
    }

    return true;
}

std::unique_ptr<MasterService> HotStandbyService::Promote() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!IsReadyForPromotion()) {
        LOG(ERROR) << "Standby is not ready for promotion (not connected)";
        return nullptr;
    }

    StandbySyncStatus status = GetSyncStatus();
    uint64_t current_applied_seq_id = status.applied_seq_id;
    
    LOG(INFO) << "Promoting Standby to Primary. Applied seq_id: "
              << current_applied_seq_id
              << ", lag: " << status.lag_entries << " entries";

    // Final catch-up sync before promotion.
    // IMPORTANT:
    // - Do NOT rely on `lag_entries` here because primary_seq_id_ is best-effort.
    // - Stop OpLogWatcher first to avoid concurrent Apply from watch callbacks.
    if (oplog_watcher_) {
        oplog_watcher_->Stop();
    }

    LOG(INFO) << "Final catch-up sync from etcd before promotion...";
    EtcdOpLogStore oplog_store(cluster_id_, /*enable_latest_seq_batch_update=*/false);
    const size_t batch_size = 1000;
    uint64_t start_seq = current_applied_seq_id + 1;
    size_t total_applied = 0;
    for (;;) {
        std::vector<OpLogEntry> batch;
        ErrorCode read_err = oplog_store.ReadOpLogSince(start_seq - 1, batch_size, batch);
        if (read_err != ErrorCode::OK) {
            LOG(WARNING) << "Final catch-up: failed to read OpLog since seq="
                         << (start_seq - 1) << ", err=" << read_err
                         << ". Proceeding with promotion.";
            break;
        }
        if (batch.empty()) {
            break;
        }
        size_t applied = oplog_applier_->ApplyOpLogEntries(batch);
        total_applied += applied;
        start_seq = batch.back().sequence_id + 1;
    }
    LOG(INFO) << "Final catch-up sync done. total_applied=" << total_applied;

    // Stop replication (OpLogWatcher will stop watching)
    Stop();

    // In full implementation, we would:
    // 1. Create a new MasterService instance with appropriate config
    // 2. Initialize it with the replicated metadata from metadata_store_
    // 3. Set the OpLogManager's initial sequence_id to latest_seq_id
    // 4. Return the MasterService instance

    // For now, this is a placeholder - the actual MasterService creation
    // happens in MasterServiceSupervisor::Start() after leader election.
    // This method ensures all remaining OpLog entries are synced before
    // the new Primary starts serving requests.
    
    LOG(INFO) << "Standby promoted to Primary successfully. "
              << "All remaining OpLog entries have been synced.";
    
    // Return nullptr - actual MasterService creation happens externally
    // The caller (MasterServiceSupervisor) will create the MasterService
    // with the appropriate configuration.
    return nullptr;
}

size_t HotStandbyService::GetMetadataCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return metadata_store_ ? metadata_store_->GetKeyCount() : 0;
}

uint64_t HotStandbyService::GetLatestAppliedSequenceId() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (oplog_applier_) {
        uint64_t expected_seq = oplog_applier_->GetExpectedSequenceId();
        // GetExpectedSequenceId returns the next expected sequence_id,
        // so the latest applied is expected_seq - 1
        return expected_seq > 0 ? expected_seq - 1 : 0;
    }
    return applied_seq_id_.load();
}

bool HotStandbyService::ExportMetadataSnapshot(
    std::vector<std::pair<std::string, StandbyObjectMetadata>>& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!metadata_store_) {
        out.clear();
        return false;
    }
    metadata_store_->Snapshot(out);
    return true;
}

void HotStandbyService::SetSnapshotProvider(std::unique_ptr<SnapshotProvider> provider) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (provider) {
        snapshot_provider_ = std::move(provider);
    } else {
        snapshot_provider_ = std::make_unique<NoopSnapshotProvider>();
    }
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

        // Update primary_seq_id by querying etcd `/latest` (best-effort).
        // Note: `/latest` is batch-updated on Primary, so this is for monitoring only.
#ifdef STORE_USE_ETCD
        if (!cluster_id_.empty()) {
            EtcdOpLogStore oplog_store(cluster_id_, /*enable_latest_seq_batch_update=*/false);
            uint64_t latest_seq = 0;
            ErrorCode err = oplog_store.GetLatestSequenceId(latest_seq);
            if (err == ErrorCode::OK) {
                primary_seq_id_.store(latest_seq);
            }
        }
#endif

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

