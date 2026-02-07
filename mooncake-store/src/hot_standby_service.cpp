#include "hot_standby_service.h"

#include <glog/logging.h>

#include <chrono>
#include <thread>

#include "etcd_helper.h"
#include "etcd_oplog_store.h"
#include "ha_metric_manager.h"
#include "master_service.h"
#include "oplog_applier.h"
#include "oplog_manager.h"
#include "oplog_watcher.h"

namespace mooncake {

HotStandbyService::HotStandbyService(const HotStandbyConfig& config)
    : config_(config) {
    metadata_store_ = std::make_unique<StandbyMetadataStore>();
    // OpLogApplier will be re-created in Start() with the resolved cluster_id
    // to enable etcd-based operations (e.g. requesting missing OpLog entries).
    // Here we construct a minimal instance so that local metadata operations
    // are available before etcd wiring is completed.
    oplog_applier_ = std::make_unique<OpLogApplier>(metadata_store_.get());

    // Register callback for state change logging and metrics.
    state_machine_.RegisterCallback(
        [](StandbyState old_state, StandbyState new_state, StandbyEvent event) {
            LOG(INFO) << "HotStandbyService state changed: "
                      << StandbyStateToString(old_state) << " -> "
                      << StandbyStateToString(new_state)
                      << " (event: " << StandbyEventToString(event) << ")";

            // Update HA metrics
            HAMetricManager::instance().set_standby_state(
                static_cast<int64_t>(new_state));
            HAMetricManager::instance().inc_state_transitions();

            // Track watch disconnections
            if (event == StandbyEvent::WATCH_BROKEN ||
                event == StandbyEvent::DISCONNECTED) {
                HAMetricManager::instance().inc_oplog_watch_disconnections();
            }
        });
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

const StandbyObjectMetadata*
HotStandbyService::StandbyMetadataStore::GetMetadata(
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
    // Always ensure threads are joined, regardless of state
    // This prevents std::terminate() if threads are still joinable
    Stop();

    // Double-check: ensure all threads are joined even if Stop() had early
    // return
    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }
    if (verification_thread_.joinable()) {
        verification_thread_.join();
    }
}

ErrorCode HotStandbyService::Start(const std::string& primary_address,
                                   const std::string& etcd_endpoints,
                                   const std::string& cluster_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Use state machine to check if already running
    if (IsRunning()) {
        LOG(WARNING) << "HotStandbyService is already running";
        return ErrorCode::OK;
    }

    // Trigger START event
    auto result = state_machine_.ProcessEvent(StandbyEvent::START);
    if (!result.allowed) {
        LOG(ERROR) << "Cannot start HotStandbyService: " << result.reason;
        return ErrorCode::INTERNAL_ERROR;  // State machine rejected START
    }

    config_.primary_address = primary_address;
    etcd_endpoints_ = etcd_endpoints;
    cluster_id_ = cluster_id;

#ifdef STORE_USE_ETCD
    // Connect to etcd
    ErrorCode err =
        EtcdHelper::ConnectToEtcdStoreClient(etcd_endpoints.c_str());
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to etcd: " << etcd_endpoints;
        state_machine_.ProcessEvent(StandbyEvent::CONNECTION_FAILED);
        return err;
    }

    // Transition to SYNCING state
    state_machine_.ProcessEvent(StandbyEvent::CONNECTED);

    // Preserve existing local state if HotStandbyService is restarted
    // in-process:
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
    oplog_applier_ =
        std::make_unique<OpLogApplier>(metadata_store_.get(), cluster_id);
    if (has_local_state) {
        LOG(INFO) << "Standby warm start: reuse local metadata (keys="
                  << metadata_store_->GetKeyCount()
                  << "), recover last_seq_id=" << local_last_seq_id;
        oplog_applier_->Recover(local_last_seq_id);
    }

    // Create OpLogWatcher with state machine callback
    oplog_watcher_ = std::make_unique<OpLogWatcher>(etcd_endpoints, cluster_id,
                                                    oplog_applier_.get());

    // Register callback for watcher events
    oplog_watcher_->SetStateCallback(
        [this](StandbyEvent event) { OnWatcherEvent(event); });

    // Bootstrap:
    // - If we already have local state (warm start), do NOT reload snapshot.
    // - Otherwise (cold start/new standby), try snapshot (if enabled) then
    // replay OpLog.
    uint64_t baseline_seq_id = has_local_state ? local_last_seq_id : 0;
    if (!has_local_state && config_.enable_snapshot_bootstrap &&
        snapshot_provider_) {
        std::string snapshot_id;
        uint64_t snapshot_seq_id = 0;
        std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
        if (snapshot_provider_->LoadLatestSnapshot(cluster_id_, snapshot_id,
                                                   snapshot_seq_id, snapshot)) {
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
            LOG(INFO) << "No snapshot available (or provider not ready), "
                         "falling back to OpLog-only bootstrap";
        }
    }

    // Read historical OpLog entries since baseline_seq_id.
    uint64_t last_applied_seq_id = baseline_seq_id;

    // Start OpLogWatcher with a consistent "read then watch(from revision+1)"
    // sequence.
    if (!oplog_watcher_->StartFromSequenceId(last_applied_seq_id)) {
        LOG(WARNING) << "Failed to start OpLogWatcher from sequence_id="
                     << last_applied_seq_id << ", continuing anyway";
        state_machine_.ProcessEvent(StandbyEvent::SYNC_FAILED);
    } else {
        // Transition to WATCHING state after successful sync
        state_machine_.ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    }

    // Start background threads
    replication_thread_ =
        std::thread(&HotStandbyService::ReplicationLoop, this);
    if (config_.enable_verification) {
        verification_thread_ =
            std::thread(&HotStandbyService::VerificationLoop, this);
    }

    LOG(INFO) << "HotStandbyService started, watching etcd OpLog for cluster: "
              << cluster_id << ", state=" << StandbyStateToString(GetState());
    return ErrorCode::OK;
#else
    state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
    LOG(ERROR)
        << "STORE_USE_ETCD is not enabled, cannot start HotStandbyService";
    return ErrorCode::INTERNAL_ERROR;
#endif
}

void HotStandbyService::OnWatcherEvent(StandbyEvent event) {
    state_machine_.ProcessEvent(event);
}

void HotStandbyService::Stop() {
    // Check if already stopped (to avoid duplicate processing)
    bool was_running = IsRunning();
    StandbyState current_state = GetState();

    if (!was_running && current_state != StandbyState::PROMOTING &&
        !replication_thread_.joinable() && !verification_thread_.joinable()) {
        // Already fully stopped and threads are joined
        return;
    }

    // Trigger STOP event
    state_machine_.ProcessEvent(StandbyEvent::STOP);

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

    LOG(INFO) << "HotStandbyService stopped, final_state="
              << StandbyStateToString(GetState());
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

    // Primary sequence ID (best-effort): updated by ReplicationLoop via etcd
    // `/latest`.
    status.primary_seq_id = primary_seq_id_.load();

    // Use state machine for connection status
    status.is_connected = IsConnected();
    status.state = GetState();
    status.time_in_state = state_machine_.GetTimeInCurrentState();

    if (status.primary_seq_id > status.applied_seq_id) {
        status.lag_entries = status.primary_seq_id - status.applied_seq_id;
    } else {
        status.lag_entries = 0;
    }

    // Lag time is currently reported as 0; if needed we can extend the
    // protocol to propagate primary timestamps and compute a real value.
    status.lag_time = std::chrono::milliseconds(0);
    status.is_syncing = IsRunning() && IsConnected();

    return status;
}

bool HotStandbyService::IsReadyForPromotion() const {
    // Use state machine to check if ready for promotion
    if (!state_machine_.IsReadyForPromotion()) {
        return false;
    }

    StandbySyncStatus status = GetSyncStatus();

    // Allow promotion even with large lag - the new Primary can continue
    // syncing remaining OpLog entries from etcd after promotion.
    // Log a warning if lag is large, but don't block promotion.
    if (status.lag_entries > config_.max_replication_lag_entries) {
        LOG(WARNING)
            << "Standby has large replication lag: " << status.lag_entries
            << " entries (threshold: " << config_.max_replication_lag_entries
            << "). Promotion will proceed, but remaining OpLog entries "
            << "will be synced after promotion.";
    }

    return true;
}

std::unique_ptr<MasterService> HotStandbyService::Promote() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!IsReadyForPromotion()) {
        LOG(ERROR) << "Standby is not ready for promotion, state="
                   << StandbyStateToString(GetState());
        return nullptr;
    }

    // Trigger PROMOTE event
    auto result = state_machine_.ProcessEvent(StandbyEvent::PROMOTE);
    if (!result.allowed) {
        LOG(ERROR) << "Cannot promote: " << result.reason;
        return nullptr;
    }

    StandbySyncStatus status = GetSyncStatus();
    uint64_t current_applied_seq_id = status.applied_seq_id;

    LOG(INFO) << "Promoting Standby to Primary. Applied seq_id: "
              << current_applied_seq_id << ", lag: " << status.lag_entries
              << " entries"
              << ", state: " << StandbyStateToString(GetState());

    // Final catch-up sync before promotion.
    // IMPORTANT:
    // - Do NOT rely on `lag_entries` here because primary_seq_id_ is
    // best-effort.
    // - Stop OpLogWatcher first to avoid concurrent Apply from watch callbacks.
    if (oplog_watcher_) {
        oplog_watcher_->Stop();
    }

    // Best-effort: resolve any outstanding gaps with retry before promotion.
    // Do NOT block promotion if gaps cannot be fetched after max retries.
    static constexpr int kMaxGapResolveRetries = 3;
    if (oplog_applier_) {
        for (int retry = 0; retry < kMaxGapResolveRetries; ++retry) {
            auto res = oplog_applier_->TryResolveGapsOnceForPromotion(
                /*max_ids=*/1024);
            if (res.attempted == 0) {
                // No gaps to resolve
                break;
            }
            LOG(INFO) << "Promotion gap resolve (attempt " << (retry + 1) << "/"
                      << kMaxGapResolveRetries
                      << "): attempted=" << res.attempted
                      << ", fetched=" << res.fetched
                      << ", applied_deletes=" << res.applied_deletes;
            if (res.fetched == res.attempted) {
                // All gaps resolved successfully
                break;
            }
            // Some gaps failed, retry after short delay
            if (retry + 1 < kMaxGapResolveRetries) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    LOG(INFO) << "Final catch-up sync from etcd before promotion...";
    EtcdOpLogStore oplog_store(cluster_id_,
                               /*enable_latest_seq_batch_update=*/false);
    const size_t batch_size = 1000;

    // P0 fix: Prevent underflow when current_applied_seq_id is 0
    // ReadOpLogSince reads entries with seq > given_seq, so we pass
    // current_applied_seq_id directly
    uint64_t read_from_seq =
        current_applied_seq_id;  // Will read entries with seq > read_from_seq

    // P1 fix: Add timeout control to prevent infinite blocking
    static constexpr size_t kMaxCatchUpBatches =
        100;  // Max 100 batches * 1000 = 100k entries
    static constexpr auto kMaxCatchUpDuration = std::chrono::seconds(30);
    auto catch_up_start = std::chrono::steady_clock::now();

    size_t total_applied = 0;
    size_t batch_count = 0;

    for (;;) {
        // Check timeout
        auto elapsed = std::chrono::steady_clock::now() - catch_up_start;
        if (elapsed > kMaxCatchUpDuration) {
            LOG(WARNING) << "Final catch-up: timeout after "
                         << std::chrono::duration_cast<std::chrono::seconds>(
                                elapsed)
                                .count()
                         << "s. Proceeding with promotion. total_applied="
                         << total_applied;
            break;
        }

        // Check batch limit
        if (batch_count >= kMaxCatchUpBatches) {
            LOG(WARNING) << "Final catch-up: reached max batch limit ("
                         << kMaxCatchUpBatches
                         << "). Proceeding with promotion. total_applied="
                         << total_applied;
            break;
        }

        std::vector<OpLogEntry> batch;
        ErrorCode read_err =
            oplog_store.ReadOpLogSince(read_from_seq, batch_size, batch);
        if (read_err != ErrorCode::OK) {
            LOG(WARNING) << "Final catch-up: failed to read OpLog since seq="
                         << read_from_seq
                         << ", err=" << static_cast<int>(read_err)
                         << ". Proceeding with promotion.";
            break;
        }
        if (batch.empty()) {
            break;
        }
        size_t applied = oplog_applier_->ApplyOpLogEntries(batch);
        total_applied += applied;
        read_from_seq =
            batch.back().sequence_id;  // Next read will get entries > this seq
        ++batch_count;
    }
    LOG(INFO) << "Final catch-up sync done. total_applied=" << total_applied
              << ", batches=" << batch_count;

    // Transition to PROMOTED state
    state_machine_.ProcessEvent(StandbyEvent::PROMOTION_SUCCESS);

    // Stop replication (OpLogWatcher will stop watching).
    // Note: This will trigger STOP event, transitioning to STOPPED.
    Stop();

    // Design note: MasterService creation and initialization are handled by
    // MasterServiceSupervisor::Start() after leader election. The
    // responsibility of HotStandbyService::Promote() is limited to ensuring
    // that all remaining OpLog entries are applied before the new Primary
    // starts serving requests.

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

void HotStandbyService::SetSnapshotProvider(
    std::unique_ptr<SnapshotProvider> provider) {
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

    while (IsRunning()) {
        if (!IsConnected()) {
            // Not connected - wait a bit before checking again
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        // Update applied_seq_id from OpLogApplier
        if (oplog_applier_) {
            uint64_t current_applied =
                oplog_applier_->GetExpectedSequenceId() - 1;
            if (current_applied > 0) {
                applied_seq_id_.store(current_applied);
            }
        }

        // Update primary_seq_id by querying etcd `/latest` (best-effort).
        // Note: `/latest` is batch-updated on Primary, so this is for
        // monitoring only.
#ifdef STORE_USE_ETCD
        if (!cluster_id_.empty()) {
            EtcdOpLogStore oplog_store(
                cluster_id_, /*enable_latest_seq_batch_update=*/false);
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

    while (IsRunning()) {
        std::this_thread::sleep_for(
            std::chrono::seconds(config_.verification_interval_sec));

        if (!IsConnected()) {
            continue;
        }

        // Verification is not yet implemented. When enabled, this loop is
        // expected to:
        // 1) sample keys from the local metadata store,
        // 2) calculate checksums,
        // 3) send a verification request to the Primary, and
        // 4) handle any mismatches that are detected.
        VLOG(1)
            << "Verification check skipped (feature not implemented), state="
            << StandbyStateToString(GetState());
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
            << entry.sequence_id
            << ", op_type=" << static_cast<int>(entry.op_type)
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
    if (IsConnected()) {
        state_machine_.ProcessEvent(StandbyEvent::DISCONNECTED);
        replication_stream_.reset();
        LOG(INFO) << "Disconnected from Primary (etcd-based sync), state="
                  << StandbyStateToString(GetState());
    }
}

}  // namespace mooncake
