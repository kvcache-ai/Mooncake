#include "hot_standby_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <thread>

#include "etcd_helper.h"
#ifdef STORE_USE_ETCD
#include "ha/kv/etcd_ha_kv_backend.h"
#endif
#include "ha_metric_manager.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_standby_reader.h"
#include "ha/oplog/oplog_test_failpoint.h"
#include "ha/oplog/oplog_types.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_provider.h"

namespace mooncake {

HotStandbyService::HotStandbyService(const HotStandbyConfig& config)
    : config_(config) {
    metadata_store_ = std::make_unique<StandbyMetadataStore>();
    // OpLogApplier is re-created in Start() with the resolved cluster_id.
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

    // A failed asynchronous run leaves joinable worker threads behind. Reap
    // them before constructing the next run's readers and workers.
    replication_loop_running_.store(false, std::memory_order_release);
    replication_loop_cv_.notify_all();
    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }
    if (verification_thread_.joinable()) {
        verification_thread_.join();
    }
    batch_standby_reader_.reset();
    batch_standby_kv_backend_.reset();
    batch_snapshot_baseline_.reset();
    {
        std::lock_guard<std::mutex> cursor_lock(batch_snapshot_cursor_mutex_);
        last_applied_batch_snapshot_prefix_.reset();
    }

    last_error_.store(ErrorCode::OK, std::memory_order_release);

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
        // Injected test backends exercise the production Start/replication
        // path without a live etcd cluster (and without STORE_USE_ETCD).
        if (!catch_up_batch_kv_backend_for_testing_) {
#ifdef STORE_USE_ETCD
            ErrorCode err =
                EtcdHelper::ConnectToEtcdStoreClient(oplog_endpoints.c_str());
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to connect to etcd: " << oplog_endpoints;
                state_machine_.ProcessEvent(StandbyEvent::CONNECTION_FAILED);
                return err;
            }
#else
            // Without STORE_USE_ETCD, the following loop needs an injected
            // test backend.
            state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
            LOG(ERROR) << "Batch-record OpLog requires STORE_USE_ETCD";
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
    if (batch_oplog_snapshot_provider_ && config_.enable_snapshot_bootstrap) {
        return LoadBatchOpLogSnapshotBaselineLocked(baseline_seq_id);
    }
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
    for (const auto& entry : snapshot.metadata) {
        if (!metadata_store_->RestoreMetadata(entry.tenant_id, entry.key,
                                              entry.metadata)) {
            LOG(ERROR) << "Snapshot baseline contains duplicate object: tenant="
                       << entry.tenant_id << ", key=" << entry.key;
            metadata_store_->Clear();
            return ErrorCode::DESERIALIZE_FAIL;
        }
    }
    // Load segment registry from snapshot
    if (oplog_applier_) {
        oplog_applier_->LoadSegmentRegistry(snapshot.segments);
    }
    oplog_applier_->Recover(snapshot.snapshot_sequence_id);
    baseline_seq_id = snapshot.snapshot_sequence_id;
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::LoadBatchOpLogSnapshotBaselineLocked(
    uint64_t& baseline_seq_id) {
    baseline_seq_id = 0;
    auto temporary_metadata = std::make_unique<StandbyMetadataStore>();
    auto temporary_applier =
        std::make_unique<OpLogApplier>(temporary_metadata.get(), cluster_id_);
    StandbySegmentRegistry temporary_registry;
    auto restored = batch_oplog_snapshot_provider_->RestoreBaseline(
        *temporary_metadata, temporary_registry, temporary_applier.get());
    if (!restored) {
        return restored.error();
    }

    // The provider has already replayed the suffix in temporary state. The
    // running reader/applier starts from that proven final sequence.
    metadata_store_ = std::move(temporary_metadata);
    oplog_applier_ = std::move(temporary_applier);
    baseline_seq_id = oplog_applier_->GetExpectedSequenceId() - 1;
    batch_snapshot_baseline_ =
        DurablePrefix{.batch_id = restored->last_applied_batch_id,
                      .last_seq = restored->last_applied_seq};
    applied_seq_id_.store(baseline_seq_id, std::memory_order_release);
    primary_seq_id_.store(baseline_seq_id, std::memory_order_release);
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::StartOplogFollowingLocked(
    uint64_t baseline_seq_id) {
    (void)baseline_seq_id;
    if (catch_up_batch_kv_backend_for_testing_) {
        batch_standby_kv_backend_ = catch_up_batch_kv_backend_for_testing_;
    } else {
#ifdef STORE_USE_ETCD
        batch_standby_kv_backend_ = std::make_shared<EtcdHaKvBackend>();
#else
        state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
        LOG(ERROR) << "Batch-record OpLog requires STORE_USE_ETCD";
        return ErrorCode::INTERNAL_ERROR;
#endif
    }
    batch_standby_reader_ = std::make_unique<OpLogBatchStandbyReader>(
        cluster_id_, *batch_standby_kv_backend_, *oplog_applier_);
    if (batch_snapshot_baseline_) {
        const ErrorCode cursor_error =
            batch_standby_reader_->SetBaselineCursor(*batch_snapshot_baseline_);
        if (cursor_error != ErrorCode::OK) {
            return cursor_error;
        }
        std::lock_guard<std::mutex> cursor_lock(batch_snapshot_cursor_mutex_);
        last_applied_batch_snapshot_prefix_ = *batch_snapshot_baseline_;
    }

    state_machine_.ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    replication_loop_running_.store(true, std::memory_order_release);
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

void HotStandbyService::SetCatchUpBatchKvBackendForTesting(
    std::shared_ptr<HaKvBackend> backend) {
    catch_up_batch_kv_backend_for_testing_ = std::move(backend);
}

void HotStandbyService::StopReplicationLoop() {
    CancelSnapshotCapture();
    replication_loop_running_.store(false, std::memory_order_release);
    replication_loop_cv_.notify_all();
    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }
}

void HotStandbyService::Stop() {
    std::lock_guard<std::mutex> lock(mutex_);

    StandbyState current_state = GetState();
    if (current_state == StandbyState::STOPPED &&
        !replication_thread_.joinable() && !verification_thread_.joinable()) {
        return;
    }

    if (current_state != StandbyState::PROMOTED) {
        NotifySnapshotStop();
    }
    state_machine_.ProcessEvent(StandbyEvent::STOP);
    StopReplicationLoop();

    // Wait for threads to finish
    if (verification_thread_.joinable()) {
        verification_thread_.join();
    }

    LOG(INFO) << "HotStandbyService stopped, final_state="
              << StandbyStateToString(GetState());
}

std::optional<DurablePrefix>
HotStandbyService::GetLastAppliedBatchOpLogSnapshotPrefix() const {
    std::lock_guard<std::mutex> lock(batch_snapshot_cursor_mutex_);
    return last_applied_batch_snapshot_prefix_;
}

void HotStandbyService::CancelBatchOpLogSnapshotCapture() {
    CancelSnapshotCapture();
}

void HotStandbyService::SetBatchOpLogSnapshotCaptureReleasedCallback(
    SnapshotLifecycleCallback callback) {
    std::lock_guard<std::mutex> lock(snapshot_lifecycle_callback_mutex_);
    snapshot_capture_released_callback_ = std::move(callback);
}

void HotStandbyService::SetBatchOpLogSnapshotPromotionCallback(
    SnapshotLifecycleCallback callback) {
    std::lock_guard<std::mutex> lock(snapshot_lifecycle_callback_mutex_);
    snapshot_promotion_callback_ = std::move(callback);
}

void HotStandbyService::SetBatchOpLogSnapshotStopCallback(
    SnapshotLifecycleCallback callback) {
    std::lock_guard<std::mutex> lock(snapshot_lifecycle_callback_mutex_);
    snapshot_stop_callback_ = std::move(callback);
}

void HotStandbyService::NotifySnapshotPromotion() {
    SnapshotLifecycleCallback callback;
    {
        std::lock_guard<std::mutex> lock(snapshot_lifecycle_callback_mutex_);
        callback = snapshot_promotion_callback_;
    }
    if (callback) {
        callback();
    }
}

void HotStandbyService::NotifySnapshotStop() {
    SnapshotLifecycleCallback callback;
    {
        std::lock_guard<std::mutex> lock(snapshot_lifecycle_callback_mutex_);
        callback = snapshot_stop_callback_;
    }
    if (callback) {
        callback();
    }
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
    status.last_error = last_error_.load(std::memory_order_acquire);
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

    // NOTE: unresolved-gap check is deferred to PromoteLockedInternal, which
    // runs gap resolution + final catch-up first and only rejects promotion
    // when gaps remain after both attempts. Checking here would return a
    // misleading UNAVAILABLE_IN_CURRENT_STATUS before gap resolution runs.
    return true;
}

ErrorCode HotStandbyService::FinalCatchUpForPromotionLocked(
    uint64_t current_applied_seq_id) {
    (void)current_applied_seq_id;
    if (!config_.enable_oplog_following) {
        LOG(INFO) << "Promotion does not require final OpLog catch-up";
        return ErrorCode::OK;
    }
    if (!oplog_applier_) {
        LOG(ERROR) << "Final catch-up requires OpLogApplier";
        return ErrorCode::INTERNAL_ERROR;
    }

    if (catch_up_batch_kv_backend_for_testing_) {
        return FinalCatchUpBatchRecordsLocked(
            *catch_up_batch_kv_backend_for_testing_);
    }

#ifdef STORE_USE_ETCD
    EtcdHaKvBackend batch_backend;
    return FinalCatchUpBatchRecordsLocked(batch_backend);
#else
    LOG(ERROR) << "Final OpLog catch-up requires STORE_USE_ETCD";
    return ErrorCode::INTERNAL_ERROR;
#endif
}

ErrorCode HotStandbyService::FinalCatchUpBatchRecordsLocked(
    HaKvBackend& backend) {
    std::unique_ptr<OpLogBatchStandbyReader> local_reader;
    OpLogBatchStandbyReader* reader = batch_standby_reader_.get();
    if (reader == nullptr) {
        local_reader = std::make_unique<OpLogBatchStandbyReader>(
            cluster_id_, backend, *oplog_applier_);
        reader = local_reader.get();
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    const auto initial_retry_delay =
        std::chrono::milliseconds(std::max(config_.oplog_poll_interval_ms, 1));
    const auto max_retry_delay =
        std::max(initial_retry_delay, std::chrono::milliseconds(1000));
    auto retry_delay = initial_retry_delay;
    auto wait_to_retry = [&] {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::min(
            retry_delay, std::chrono::duration_cast<std::chrono::milliseconds>(
                             deadline - now)));
        retry_delay = std::min(retry_delay * 2, max_retry_delay);
        return true;
    };
    for (;;) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
        }
        auto result = reader->PollOnce();
        if (result.error != ErrorCode::OK) {
            if (result.disposition !=
                    OpLogBatchStandbyPollDisposition::RETRYABLE ||
                !wait_to_retry()) {
                return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
            }
            continue;
        }
        retry_delay = initial_retry_delay;
        if (!result.durable_prefix_present) {
            return GetLocalLastAppliedSequenceIdLocked() == 0
                       ? ErrorCode::OK
                       : ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
        }
        if (GetLocalLastAppliedSequenceIdLocked() >=
            result.durable_prefix.last_seq) {
            return ErrorCode::OK;
        }
    }
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
              << " entries"
              << ", state: " << StandbyStateToString(GetState());

    auto internal_err = PromoteLockedInternal(current_applied_seq_id);
    if (internal_err != ErrorCode::OK) {
        return internal_err;
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

ErrorCode HotStandbyService::PromoteLockedInternal(
    uint64_t current_applied_seq_id) {
    NotifySnapshotPromotion();
    StopReplicationLoop();
    ErrorCode catch_up_err =
        FinalCatchUpForPromotionLocked(current_applied_seq_id);
    if (catch_up_err != ErrorCode::OK) {
        state_machine_.ProcessEvent(StandbyEvent::PROMOTION_FAILED);
        return catch_up_err;
    }
    TestFailPoint::Wait("promotion_final_catch_up_before_complete");
    uint64_t latest_applied_seq_id = GetLocalLastAppliedSequenceIdLocked();
    applied_seq_id_.store(latest_applied_seq_id, std::memory_order_release);
    primary_seq_id_.store(latest_applied_seq_id, std::memory_order_release);

    auto promotion_success =
        state_machine_.ProcessEvent(StandbyEvent::PROMOTION_SUCCESS);
    if (!promotion_success.allowed) {
        LOG(ERROR) << "Cannot finish promotion: " << promotion_success.reason;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode HotStandbyService::PromoteAndExportSnapshot(StandbySnapshot& out) {
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
              << " entries"
              << ", state: " << StandbyStateToString(GetState());

    auto internal_err = PromoteLockedInternal(current_applied_seq_id);
    if (internal_err != ErrorCode::OK) {
        return internal_err;
    }

    // Export snapshot BEFORE unlocking mutex (atomic promotion + export)
    uint64_t latest_applied_seq_id = GetLocalLastAppliedSequenceIdLocked();
    out.oplog_sequence_id = latest_applied_seq_id;
    if (metadata_store_) {
        metadata_store_->Snapshot(out.objects);
    } else {
        out.objects.clear();
    }
    if (oplog_applier_) {
        out.segments = oplog_applier_->GetSegmentRegistry().GetAllSegments();
    } else {
        out.segments.clear();
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
    std::vector<StandbyObjectEntry>& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!metadata_store_) {
        out.clear();
        return false;
    }
    metadata_store_->Snapshot(out);
    return true;
}

bool HotStandbyService::ExportStandbySnapshot(StandbySnapshot& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsRunning()) {
        return false;
    }

    // Get applied sequence ID (inline to avoid recursive mutex lock)
    if (oplog_applier_) {
        uint64_t expected_seq = oplog_applier_->GetExpectedSequenceId();
        out.oplog_sequence_id = expected_seq > 0 ? expected_seq - 1 : 0;
    } else {
        out.oplog_sequence_id = applied_seq_id_.load();
    }

    // Export object metadata
    if (metadata_store_) {
        metadata_store_->Snapshot(out.objects);
    } else {
        out.objects.clear();
    }

    // Export segments from OpLogApplier's registry (Patch B)
    if (oplog_applier_) {
        out.segments = oplog_applier_->GetSegmentRegistry().GetAllSegments();
    } else {
        out.segments.clear();
    }

    return true;
}

std::optional<BatchOpLogSnapshotCapture>
HotStandbyService::BeginBatchOpLogSnapshotCapture() {
    std::unique_lock<std::mutex> service_lock(mutex_);
    auto state = snapshot_capture_state_;
    std::unique_lock<std::mutex> capture_lock(state->mutex);
    if (!IsRunning() || !batch_standby_reader_ || state->requested ||
        state->active) {
        return std::nullopt;
    }

    ++state->generation;
    state->requested = true;
    service_lock.unlock();
    replication_loop_cv_.notify_all();
    state->cv.wait(capture_lock, [&state] { return !state->requested; });
    if (!ready_snapshot_capture_ || !state->active) {
        return std::nullopt;
    }

    auto capture = std::move(*ready_snapshot_capture_);
    ready_snapshot_capture_.reset();
    return capture;
}

bool HotStandbyService::CopyNextBatchOpLogSnapshotChunk(
    size_t count, BatchOpLogSnapshotCapture& capture,
    std::vector<StandbyObjectEntry>& out) {
    auto state = snapshot_capture_state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    if (capture.lease_state_ != state || !state->active ||
        capture.generation_ != state->generation || !metadata_store_) {
        out.clear();
        return false;
    }
    return metadata_store_->CopyNextSnapshotChunk(count, capture.cursor_, out);
}

void HotStandbyService::EndBatchOpLogSnapshotCapture(
    BatchOpLogSnapshotCapture& capture) {
    if (capture.lease_state_ == snapshot_capture_state_) {
        capture.Release();
        SnapshotLifecycleCallback callback;
        {
            std::lock_guard<std::mutex> lock(
                snapshot_lifecycle_callback_mutex_);
            callback = snapshot_capture_released_callback_;
        }
        if (callback) {
            callback();
        }
    }
}

void HotStandbyService::HandleSnapshotCaptureRequest(
    const OpLogBatchStandbyPollResult& result) {
    auto state = snapshot_capture_state_;
    std::unique_lock<std::mutex> lock(state->mutex);
    if (!state->requested) {
        return;
    }

    state->requested = false;
    auto applied_prefix = batch_standby_reader_->GetLastAppliedDurablePrefix();
    ViewVersionId producer_view_version = 0;
    const ErrorCode producer_view_error =
        batch_standby_reader_->ReadProducerView(producer_view_version);
    const uint64_t expected = oplog_applier_->GetExpectedSequenceId();
    const bool consistent =
        result.error == ErrorCode::OK && result.durable_prefix_present &&
        (producer_view_error == ErrorCode::OK ||
         producer_view_error == ErrorCode::ETCD_KEY_NOT_EXIST) &&
        applied_prefix && *applied_prefix == result.durable_prefix &&
        expected > 0 && expected - 1 == applied_prefix->last_seq;
    if (consistent && IsRunning() && metadata_store_) {
        BatchOpLogSnapshotCapture capture(
            applied_prefix->last_seq, applied_prefix->batch_id,
            producer_view_version,
            oplog_applier_->GetSegmentRegistry().GetAllSegments(),
            metadata_store_->BeginSnapshotTraversal(), state->generation,
            state);
        ready_snapshot_capture_ = std::move(capture);
        state->active = true;
    }
    state->cv.notify_all();

    state->cv.wait(lock, [this, &state] {
        return !state->active ||
               !replication_loop_running_.load(std::memory_order_acquire);
    });
}

void HotStandbyService::CancelSnapshotCapture() {
    auto state = snapshot_capture_state_;
    std::optional<BatchOpLogSnapshotCapture> ready_capture;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->requested = false;
        state->active = false;
        ready_capture = std::move(ready_snapshot_capture_);
        ready_snapshot_capture_.reset();
        state->cv.notify_all();
    }
    ready_capture.reset();
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

void HotStandbyService::SetBatchOpLogSnapshotProvider(
    std::unique_ptr<BatchOpLogSnapshotProvider> provider) {
    std::lock_guard<std::mutex> lock(mutex_);
    batch_oplog_snapshot_provider_ = std::move(provider);
}

void HotStandbyService::ReplicationLoop() {
    LOG(INFO) << "Replication loop started (OpLog sync)";

    uint64_t last_reported_applied_seq_id = applied_seq_id_.load();
    uint64_t last_reported_primary_seq_id = primary_seq_id_.load();
    const auto retry_base =
        std::chrono::milliseconds(std::max(config_.oplog_poll_interval_ms, 1));
    const auto retry_limit =
        std::chrono::seconds(config_.batch_oplog_retry_timeout_sec);
    const auto max_retry_delay =
        std::max(retry_base, std::chrono::milliseconds(5000));
    auto retry_delay = retry_base;
    std::optional<std::chrono::steady_clock::time_point> retry_started;

    auto wait_for_next_poll = [this](std::chrono::milliseconds delay) {
        std::unique_lock<std::mutex> lock(replication_loop_mutex_);
        replication_loop_cv_.wait_for(lock, delay, [this] {
            return !replication_loop_running_.load(std::memory_order_acquire);
        });
    };

    while (replication_loop_running_.load(std::memory_order_acquire) &&
           IsRunning()) {
        if (!IsConnected()) {
            wait_for_next_poll(std::chrono::seconds(1));
            continue;
        }

        if (batch_standby_reader_) {
            const uint64_t expected_before =
                oplog_applier_->GetExpectedSequenceId();
            auto result = batch_standby_reader_->PollOnce();
            {
                std::lock_guard<std::mutex> cursor_lock(
                    batch_snapshot_cursor_mutex_);
                last_applied_batch_snapshot_prefix_ =
                    batch_standby_reader_->GetLastAppliedDurablePrefix();
            }
            HandleSnapshotCaptureRequest(result);
            if (result.durable_prefix_present) {
                const uint64_t current_primary = primary_seq_id_.load();
                if (result.durable_prefix.last_seq > current_primary) {
                    primary_seq_id_.store(result.durable_prefix.last_seq);
                }
            }

            const uint64_t expected_after =
                oplog_applier_->GetExpectedSequenceId();
            if (expected_after > 0) {
                applied_seq_id_.store(expected_after - 1);
            }
            if (result.error != ErrorCode::OK) {
                last_error_.store(result.error, std::memory_order_release);
                const bool made_progress = expected_after > expected_before;
                if (result.disposition ==
                    OpLogBatchStandbyPollDisposition::RETRYABLE) {
                    const auto now = std::chrono::steady_clock::now();
                    if (!retry_started || made_progress) {
                        retry_started = now;
                        retry_delay = retry_base;
                    }
                    if (now - *retry_started < retry_limit) {
                        LOG(WARNING)
                            << "Transient batch-record standby poll failure, "
                            << "retrying in " << retry_delay.count()
                            << " ms, err=" << static_cast<int>(result.error);
                        NotifySyncStatus();
                        wait_for_next_poll(retry_delay);
                        retry_delay =
                            std::min(retry_delay * 2, max_retry_delay);
                        continue;
                    }
                    LOG(ERROR)
                        << "Batch-record standby retry timeout after "
                        << config_.batch_oplog_retry_timeout_sec
                        << " seconds, err=" << static_cast<int>(result.error);
                } else {
                    LOG(ERROR) << "Fatal batch-record standby poll failure, "
                               << "err=" << static_cast<int>(result.error);
                }
                state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
                replication_loop_cv_.notify_all();
                break;
            }
            retry_started.reset();
            retry_delay = retry_base;
            last_error_.store(ErrorCode::OK, std::memory_order_release);
        }

        // Update applied_seq_id from OpLogApplier
        if (oplog_applier_) {
            uint64_t expected = oplog_applier_->GetExpectedSequenceId();
            uint64_t current_applied = (expected > 0) ? (expected - 1) : 0;
            if (current_applied > 0) {
                applied_seq_id_.store(current_applied);
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

        wait_for_next_poll(
            std::chrono::milliseconds(config_.oplog_poll_interval_ms));
    }

    replication_loop_running_.store(false, std::memory_order_release);
    CancelSnapshotCapture();
    replication_loop_cv_.notify_all();
    LOG(INFO) << "Replication loop stopped";
}

void HotStandbyService::VerificationLoop() {
    LOG(INFO) << "Verification loop started";

    while (IsRunning()) {
        std::unique_lock<std::mutex> lock(replication_loop_mutex_);
        replication_loop_cv_.wait_for(
            lock, std::chrono::seconds(config_.verification_interval_sec),
            [this] {
                return !replication_loop_running_.load(
                           std::memory_order_acquire) ||
                       !IsRunning();
            });
        lock.unlock();

        if (!IsRunning() ||
            !replication_loop_running_.load(std::memory_order_acquire)) {
            break;
        }

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

}  // namespace mooncake
