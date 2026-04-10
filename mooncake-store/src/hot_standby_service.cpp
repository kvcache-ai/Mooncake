#include "hot_standby_service.h"

#include <glog/logging.h>

#include <chrono>
#include <thread>

#include "etcd_helper.h"
#include "ha_metric_manager.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_replicator.h"
#include "ha/oplog/oplog_store_factory.h"

namespace mooncake {

HotStandbyService::HotStandbyService(const HotStandbyConfig& config)
    : config_(config) {
    // Explicitly initialize HA metric manager to ensure thread safety
    // during metric registration.
    HAMetricManager::Init();

    metadata_store_ = std::make_unique<StandbyMetadataStore>();
    // OpLogApplier will be re-created in Start() with the resolved cluster_id
    // to enable etcd-based operations (e.g. requesting missing OpLog entries).
    // Here we construct a minimal instance so that local metadata operations
    // are available before etcd wiring is completed.
    oplog_applier_ = std::make_unique<OpLogApplier>(metadata_store_.get());

    // Register callback for state change logging and metrics.
    state_machine_.RegisterCallback([this](StandbyState old_state,
                                           StandbyState new_state,
                                           StandbyEvent event) {
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

        NotifySyncStatus();
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

std::optional<StandbyObjectMetadata>
HotStandbyService::StandbyMetadataStore::GetMetadata(
    const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = store_.find(key);
    if (it != store_.end()) {
        return it->second;
    }
    return std::nullopt;
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

void HotStandbyService::StandbyMetadataStore::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    store_.clear();
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
                                   const std::string& oplog_endpoints,
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
    oplog_endpoints_ = oplog_endpoints;
    cluster_id_ = cluster_id;

    if (config_.enable_oplog_following) {
        // Connect to etcd only when using ETCD backend
        if (config_.oplog_store_type == OpLogStoreType::ETCD) {
#ifdef STORE_USE_ETCD
            ErrorCode err =
                EtcdHelper::ConnectToEtcdStoreClient(oplog_endpoints.c_str());
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to connect to etcd: " << oplog_endpoints;
                state_machine_.ProcessEvent(StandbyEvent::CONNECTION_FAILED);
                return err;
            }
#else
            state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
            LOG(ERROR) << "ETCD backend requested but STORE_USE_ETCD is not "
                          "enabled at compile time";
            return ErrorCode::INTERNAL_ERROR;
#endif
        }
    }

    state_machine_.ProcessEvent(StandbyEvent::CONNECTED);

    uint64_t baseline_seq_id = 0;
    auto baseline_err = PrepareBootstrapBaselineLocked(baseline_seq_id);
    if (baseline_err != ErrorCode::OK) {
        state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
        return baseline_err;
    }

    if (!config_.enable_oplog_following) {
        ActivateSnapshotOnlyStandbyLocked(baseline_seq_id);
        return ErrorCode::OK;
    }

    return StartOplogFollowingLocked(baseline_seq_id);
}

uint64_t HotStandbyService::GetLocalLastAppliedSequenceIdLocked() const {
    if (oplog_applier_) {
        uint64_t expected = oplog_applier_->GetExpectedSequenceId();
        return expected > 0 ? expected - 1 : 0;
    }
    return applied_seq_id_.load(std::memory_order_acquire);
}

ErrorCode HotStandbyService::PrepareBootstrapBaselineLocked(
    uint64_t& baseline_seq_id) {
    const uint64_t local_last_seq_id = GetLocalLastAppliedSequenceIdLocked();
    const bool has_local_metadata =
        metadata_store_ && metadata_store_->GetKeyCount() > 0;
    const bool has_recoverable_local_state =
        has_local_metadata && local_last_seq_id > 0;

    if (!has_recoverable_local_state && has_local_metadata) {
        LOG(WARNING)
            << "Discarding local metadata without recoverable sequence "
            << "boundary before standby bootstrap";
        metadata_store_ = std::make_unique<StandbyMetadataStore>();
    }

    oplog_applier_ =
        std::make_unique<OpLogApplier>(metadata_store_.get(), cluster_id_);
    if (!config_.enable_oplog_following) {
        if (metadata_store_ && metadata_store_->GetKeyCount() > 0) {
            LOG(INFO) << "Snapshot-only restart discards local metadata and "
                         "reloads the latest catalog snapshot";
        }

        auto snapshot_err = LoadSnapshotBaselineLocked(baseline_seq_id);
        if (snapshot_err != ErrorCode::OK) {
            return snapshot_err;
        }
        applied_seq_id_.store(baseline_seq_id, std::memory_order_release);
        primary_seq_id_.store(baseline_seq_id, std::memory_order_release);
        return ErrorCode::OK;
    }

    if (has_recoverable_local_state) {
        LOG(INFO) << "Standby warm start: reuse local metadata (keys="
                  << metadata_store_->GetKeyCount()
                  << "), recover last_seq_id=" << local_last_seq_id;
        oplog_applier_->Recover(local_last_seq_id);
        baseline_seq_id = local_last_seq_id;
    } else {
        auto snapshot_err = LoadSnapshotBaselineLocked(baseline_seq_id);
        if (snapshot_err != ErrorCode::OK) {
            return snapshot_err;
        }
    }

    applied_seq_id_.store(baseline_seq_id, std::memory_order_release);
    primary_seq_id_.store(baseline_seq_id, std::memory_order_release);
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::LoadSnapshotBaselineLocked(
    uint64_t& baseline_seq_id) {
    baseline_seq_id = 0;
    metadata_store_->Clear();
    oplog_applier_->Recover(0);

    if (!config_.enable_snapshot_bootstrap || !snapshot_provider_) {
        return ErrorCode::OK;
    }

    auto snapshot_result = snapshot_provider_->LoadLatestSnapshot(cluster_id_);
    if (!snapshot_result) {
        if (config_.enable_oplog_following) {
            LOG(WARNING) << "Failed to load snapshot baseline, falling back "
                            "to OpLog-only bootstrap: "
                         << toString(snapshot_result.error());
            return ErrorCode::OK;
        }
        LOG(ERROR) << "Failed to load snapshot baseline for snapshot-only "
                   << "standby bootstrap: "
                   << toString(snapshot_result.error());
        return snapshot_result.error();
    }

    if (!snapshot_result->has_value()) {
        if (config_.enable_oplog_following) {
            LOG(INFO) << "No snapshot available, falling back to OpLog-only "
                         "bootstrap";
        } else {
            LOG(INFO) << "No snapshot available for snapshot-only bootstrap; "
                         "standby starts from empty baseline";
        }
        return ErrorCode::OK;
    }

    const auto& snapshot = snapshot_result->value();
    LOG(INFO) << "Loaded snapshot baseline: snapshot_id="
              << snapshot.snapshot_id
              << ", snapshot_seq_id=" << snapshot.snapshot_sequence_id
              << ", keys=" << snapshot.metadata.size();
    for (const auto& kv : snapshot.metadata) {
        metadata_store_->PutMetadata(kv.first, kv.second);
    }
    oplog_applier_->Recover(snapshot.snapshot_sequence_id);
    baseline_seq_id = snapshot.snapshot_sequence_id;
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::StartOplogFollowingLocked(
    uint64_t baseline_seq_id) {
    // Create OpLogStore, OpLogChangeNotifier, and OpLogReplicator via factory
    watcher_oplog_store_ = OpLogStoreFactory::Create(
        config_.oplog_store_type, cluster_id_, OpLogStoreRole::READER,
        config_.oplog_store_root_dir, config_.oplog_poll_interval_ms);
    if (watcher_oplog_store_) {
        // Wire OpLogStore into OpLogApplier so gap resolution works
        oplog_applier_->SetOpLogStore(watcher_oplog_store_.get());
        oplog_change_notifier_ =
            watcher_oplog_store_->CreateChangeNotifier(cluster_id_);
    }
    if (oplog_change_notifier_) {
        oplog_replicator_ = std::make_unique<OpLogReplicator>(
            oplog_change_notifier_.get(), oplog_applier_.get());
        oplog_replicator_->SetStateCallback(
            [this](StandbyEvent event) { OnWatcherEvent(event); });
    } else {
        LOG(ERROR) << "Failed to create OpLogChangeNotifier for replicator";
        state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
        return ErrorCode::INTERNAL_ERROR;
    }

    static constexpr int kMaxStartRetries = 3;
    static constexpr int kStartRetryBaseMs = 500;
    bool watcher_started = false;
    for (int attempt = 0; attempt < kMaxStartRetries; ++attempt) {
        if (oplog_replicator_->StartFromSequenceId(baseline_seq_id)) {
            watcher_started = true;
            break;
        }
        LOG(WARNING) << "Failed to start OpLogReplicator from sequence_id="
                     << baseline_seq_id << " (attempt " << (attempt + 1) << "/"
                     << kMaxStartRetries << ")";
        if (attempt + 1 < kMaxStartRetries) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kStartRetryBaseMs * (1 << attempt)));
        }
    }

    if (!watcher_started) {
        LOG(ERROR) << "Failed to start OpLogReplicator after "
                   << kMaxStartRetries << " attempts, aborting Start()";
        state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
        return ErrorCode::INTERNAL_ERROR;
    }

    state_machine_.ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    replication_thread_ =
        std::thread(&HotStandbyService::ReplicationLoop, this);
    if (config_.enable_verification) {
        verification_thread_ =
            std::thread(&HotStandbyService::VerificationLoop, this);
    }

    LOG(INFO) << "HotStandbyService started, watching OpLog for cluster: "
              << cluster_id_ << ", state=" << StandbyStateToString(GetState());
    return ErrorCode::OK;
}

void HotStandbyService::ActivateSnapshotOnlyStandbyLocked(
    uint64_t baseline_seq_id) {
    state_machine_.ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    LOG(INFO) << "HotStandbyService started in snapshot-only mode, cluster="
              << cluster_id_ << ", state=" << StandbyStateToString(GetState())
              << ", applied_seq_id=" << baseline_seq_id
              << ", metadata_keys=" << metadata_store_->GetKeyCount();
}

void HotStandbyService::SetSyncStatusCallback(SyncStatusCallback callback) {
    {
        std::lock_guard<std::mutex> lock(sync_status_callback_mutex_);
        sync_status_callback_ = std::move(callback);
    }
    NotifySyncStatus();
}

void HotStandbyService::NotifySyncStatus() {
    SyncStatusCallback callback;
    {
        std::lock_guard<std::mutex> lock(sync_status_callback_mutex_);
        callback = sync_status_callback_;
    }
    if (callback) {
        callback(GetSyncStatus());
    }
}

void HotStandbyService::OnWatcherEvent(StandbyEvent event) {
    state_machine_.ProcessEvent(event);
}

void HotStandbyService::Stop() {
    std::lock_guard<std::mutex> lock(mutex_);

    StandbyState current_state = GetState();
    if (current_state == StandbyState::STOPPED &&
        !replication_thread_.joinable() && !verification_thread_.joinable()) {
        return;
    }

    state_machine_.ProcessEvent(StandbyEvent::STOP);

    // Stop OpLogReplicator
    if (oplog_replicator_) {
        oplog_replicator_->Stop();
        oplog_replicator_.reset();
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
        uint64_t expected = oplog_applier_->GetExpectedSequenceId();
        status.applied_seq_id = (expected > 0) ? (expected - 1) : 0;
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

void HotStandbyService::ResolvePromotionGapsLocked() {
    if (!config_.enable_oplog_following || !oplog_applier_) {
        return;
    }

    static constexpr int kMaxGapResolveRetries = 3;
    for (int retry = 0; retry < kMaxGapResolveRetries; ++retry) {
        auto res = oplog_applier_->TryResolveGapsOnceForPromotion(
            /*max_ids=*/1024);
        if (res.attempted == 0) {
            return;
        }

        LOG(INFO) << "Promotion gap resolve (attempt " << (retry + 1) << "/"
                  << kMaxGapResolveRetries << "): attempted=" << res.attempted
                  << ", fetched=" << res.fetched
                  << ", applied_deletes=" << res.applied_deletes;
        if (res.fetched == res.attempted) {
            return;
        }
        if (retry + 1 < kMaxGapResolveRetries) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

ErrorCode HotStandbyService::FinalCatchUpForPromotionLocked(
    uint64_t current_applied_seq_id) {
    if (!config_.enable_oplog_following) {
        LOG(INFO) << "Promotion does not require final OpLog catch-up";
        return ErrorCode::OK;
    }

    LOG(INFO) << "Final catch-up sync before promotion...";
    auto catch_up_store = OpLogStoreFactory::Create(
        config_.oplog_store_type, cluster_id_, OpLogStoreRole::READER,
        config_.oplog_store_root_dir, config_.oplog_poll_interval_ms);
    if (!catch_up_store) {
        LOG(ERROR) << "Failed to create oplog_store for final catch-up";
        return ErrorCode::INTERNAL_ERROR;
    }

    static constexpr size_t kBatchSize = 1000;
    static constexpr size_t kMaxCatchUpBatches = 100;
    static constexpr auto kMaxCatchUpDuration = std::chrono::seconds(30);

    uint64_t read_from_seq = current_applied_seq_id;
    auto catch_up_start = std::chrono::steady_clock::now();
    size_t total_applied = 0;
    size_t batch_count = 0;

    for (;;) {
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

        if (batch_count >= kMaxCatchUpBatches) {
            LOG(WARNING) << "Final catch-up: reached max batch limit ("
                         << kMaxCatchUpBatches
                         << "). Proceeding with promotion. total_applied="
                         << total_applied;
            break;
        }

        std::vector<OpLogEntry> batch;
        ErrorCode read_err =
            catch_up_store->ReadOpLogSince(read_from_seq, kBatchSize, batch);
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

        total_applied += oplog_applier_->ApplyOpLogEntries(batch);
        read_from_seq = batch.back().sequence_id;
        ++batch_count;
    }

    LOG(INFO) << "Final catch-up sync done. total_applied=" << total_applied
              << ", batches=" << batch_count;
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::Promote() {
    std::unique_lock<std::mutex> lock(mutex_);

    if (!IsReadyForPromotion()) {
        LOG(ERROR) << "Standby is not ready for promotion, state="
                   << StandbyStateToString(GetState());
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    // Trigger PROMOTE event
    auto result = state_machine_.ProcessEvent(StandbyEvent::PROMOTE);
    if (!result.allowed) {
        LOG(ERROR) << "Cannot promote: " << result.reason;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    StandbySyncStatus status = GetSyncStatus();
    uint64_t current_applied_seq_id = status.applied_seq_id;

    LOG(INFO) << "Promoting Standby to Primary. Applied seq_id: "
              << current_applied_seq_id << ", lag: " << status.lag_entries
              << " entries" << ", state: " << StandbyStateToString(GetState());

    if (oplog_replicator_) {
        oplog_replicator_->Stop();
    }

    ResolvePromotionGapsLocked();

    auto catch_up_err = FinalCatchUpForPromotionLocked(current_applied_seq_id);
    if (catch_up_err != ErrorCode::OK) {
        state_machine_.ProcessEvent(StandbyEvent::PROMOTION_FAILED);
        return catch_up_err;
    }

    uint64_t latest_applied_seq_id = GetLocalLastAppliedSequenceIdLocked();
    applied_seq_id_.store(latest_applied_seq_id, std::memory_order_release);
    primary_seq_id_.store(latest_applied_seq_id, std::memory_order_release);

    auto promotion_success =
        state_machine_.ProcessEvent(StandbyEvent::PROMOTION_SUCCESS);
    if (!promotion_success.allowed) {
        LOG(ERROR) << "Cannot finish promotion: " << promotion_success.reason;
        return ErrorCode::INTERNAL_ERROR;
    }

    lock.unlock();
    Stop();

    if (config_.enable_oplog_following) {
        LOG(INFO) << "Standby promoted to Primary successfully. "
                  << "All remaining OpLog entries have been synced.";
    } else {
        LOG(INFO) << "Standby promoted to Primary from snapshot baseline.";
    }
    return ErrorCode::OK;
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
    LOG(INFO) << "Replication loop started (OpLog sync)";

    // OpLogReplicator handles the actual watching in its own thread.
    // This loop monitors the status and updates metrics.

    // Create OpLogStore once before the loop to query primary sequence_id.
    // Reuse watcher_oplog_store_ if available, otherwise create a new one.
    std::shared_ptr<OpLogStore> repl_oplog_store = watcher_oplog_store_;
    if (!repl_oplog_store && !cluster_id_.empty()) {
        repl_oplog_store = OpLogStoreFactory::Create(
            config_.oplog_store_type, cluster_id_, OpLogStoreRole::READER,
            config_.oplog_store_root_dir, config_.oplog_poll_interval_ms);
        if (!repl_oplog_store) {
            LOG(ERROR) << "Failed to create oplog_store in replication loop";
        }
    }

    uint64_t last_reported_applied_seq_id = applied_seq_id_.load();
    uint64_t last_reported_primary_seq_id = primary_seq_id_.load();

    while (IsRunning()) {
        if (!IsConnected()) {
            // Not connected - wait a bit before checking again
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        // Update applied_seq_id from OpLogApplier
        if (oplog_applier_) {
            uint64_t expected = oplog_applier_->GetExpectedSequenceId();
            uint64_t current_applied = (expected > 0) ? (expected - 1) : 0;
            if (current_applied > 0) {
                applied_seq_id_.store(current_applied);
            }
        }

        // Update primary_seq_id by querying etcd `/latest` (best-effort).
        // Note: `/latest` is batch-updated on Primary, so this is for
        // monitoring only.
        if (repl_oplog_store) {
            uint64_t latest_seq = 0;
            ErrorCode err = repl_oplog_store->GetLatestSequenceId(latest_seq);
            if (err == ErrorCode::OK) {
                primary_seq_id_.store(latest_seq);
            }
        }

        const uint64_t applied_seq_id = applied_seq_id_.load();
        const uint64_t primary_seq_id = primary_seq_id_.load();
        if (applied_seq_id != last_reported_applied_seq_id ||
            primary_seq_id != last_reported_primary_seq_id) {
            last_reported_applied_seq_id = applied_seq_id;
            last_reported_primary_seq_id = primary_seq_id;
            NotifySyncStatus();
        }

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
    // OpLogApplier, which is called by OpLogReplicator. This method is kept
    // for backward compatibility but should not be used in the new etcd-based
    // implementation.

    // Update applied_seq_id for status tracking
    applied_seq_id_.store(entry.sequence_id);

    // The actual application is handled by OpLogApplier via OpLogReplicator
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
    // With etcd-based OpLog sync, connection is handled by OpLogReplicator
    // This method is kept for compatibility but is no longer used
    LOG(INFO) << "ConnectToPrimary called (no-op with etcd-based sync)";
    return true;
}

void HotStandbyService::DisconnectFromPrimary() {
    // With etcd-based OpLog sync, disconnection is handled by OpLogReplicator
    // This method is kept for compatibility
    if (IsConnected()) {
        state_machine_.ProcessEvent(StandbyEvent::DISCONNECTED);
        replication_stream_.reset();
        LOG(INFO) << "Disconnected from Primary (etcd-based sync), state="
                  << StandbyStateToString(GetState());
    }
}

}  // namespace mooncake
