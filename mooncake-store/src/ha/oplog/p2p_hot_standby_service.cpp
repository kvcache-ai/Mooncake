#include "ha/oplog/p2p_hot_standby_service.h"

#include <glog/logging.h>

#include <thread>
#include <utility>
#include <vector>

namespace mooncake {

P2PHotStandbyService::P2PHotStandbyService(P2PHotStandbyConfig config)
    : config_(std::move(config)) {
    metadata_store_ = std::make_unique<P2PStandbyMetadataStore>();
    oplog_applier_ = std::make_unique<P2POpLogApplier>(metadata_store_.get(),
                                                       config_.cluster_id);
}

P2PHotStandbyService::~P2PHotStandbyService() { Stop(); }

ErrorCode P2PHotStandbyService::Start(uint64_t baseline_sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_machine_.IsRunning()) {
        LOG(WARNING) << "P2PHotStandbyService is already running";
        return ErrorCode::OK;
    }

    auto start_result = state_machine_.ProcessEvent(StandbyEvent::START);
    if (!start_result.allowed) {
        LOG(ERROR) << "P2PHotStandbyService: cannot start: "
                   << start_result.reason;
        return ErrorCode::INTERNAL_ERROR;
    }

    state_machine_.ProcessEvent(StandbyEvent::CONNECTED);
    metadata_store_->RemoveAllMetadata();
    oplog_applier_ = std::make_unique<P2POpLogApplier>(metadata_store_.get(),
                                                       config_.cluster_id);
    oplog_applier_->Recover(baseline_sequence_id);

    auto err = StartOplogFollowingLocked(baseline_sequence_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "P2PHotStandbyService: failed to start oplog following"
                   << ", baseline_sequence_id=" << baseline_sequence_id
                   << ", err=" << err;
        state_machine_.ProcessEvent(StandbyEvent::FATAL_ERROR);
        ResetOplogFollowingLocked();
        return err;
    }

    state_machine_.ProcessEvent(StandbyEvent::SYNC_COMPLETE);
    LOG(INFO) << "P2PHotStandbyService started"
              << ", cluster_id=" << config_.cluster_id
              << ", baseline_sequence_id=" << baseline_sequence_id;
    return ErrorCode::OK;
}

ErrorCode P2PHotStandbyService::StartOplogFollowingLocked(
    uint64_t baseline_sequence_id) {
    ResetOplogFollowingLocked();

    watcher_oplog_store_ = OpLogStoreFactory::Create(
        config_.oplog_store_type, config_.cluster_id, OpLogStoreRole::READER,
        config_.oplog_store_type == OpLogStoreType::REDIS
            ? config_.redis_endpoint
            : config_.oplog_store_root_dir,
        config_.oplog_poll_interval_ms, config_.redis_password,
        config_.redis_username, config_.redis_db_index);
    if (!watcher_oplog_store_) {
        LOG(ERROR) << "P2PHotStandbyService: failed to create reader store"
                   << ", cluster_id=" << config_.cluster_id;
        return ErrorCode::INTERNAL_ERROR;
    }

    oplog_applier_->SetOpLogStore(watcher_oplog_store_.get());
    oplog_change_notifier_ =
        watcher_oplog_store_->CreateChangeNotifier(config_.cluster_id);
    if (!oplog_change_notifier_) {
        LOG(ERROR)
            << "P2PHotStandbyService: failed to create OpLogChangeNotifier"
            << ", cluster_id=" << config_.cluster_id;
        return ErrorCode::INTERNAL_ERROR;
    }

    oplog_replicator_ = std::make_unique<OpLogReplicator>(
        oplog_change_notifier_.get(), oplog_applier_.get());
    oplog_replicator_->SetStateCallback(
        [this](StandbyEvent event) { OnWatcherEvent(event); });

    static constexpr int kMaxStartRetries = 3;
    for (int attempt = 0; attempt < kMaxStartRetries; ++attempt) {
        if (oplog_replicator_->StartFromSequenceId(baseline_sequence_id)) {
            return ErrorCode::OK;
        }
        if (attempt + 1 < kMaxStartRetries) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(100 * (1 << attempt)));
        }
    }

    LOG(ERROR) << "P2PHotStandbyService: failed to start OpLogReplicator";
    return ErrorCode::INTERNAL_ERROR;
}

void P2PHotStandbyService::ResetOplogFollowingLocked() {
    if (oplog_replicator_) {
        oplog_replicator_->Stop();
        oplog_replicator_.reset();
    }
    if (oplog_applier_) {
        oplog_applier_->SetOpLogStore(nullptr);
    }
    oplog_change_notifier_.reset();
    watcher_oplog_store_.reset();
}

void P2PHotStandbyService::Stop() {
    std::lock_guard<std::mutex> lock(mutex_);

    ResetOplogFollowingLocked();

    StandbyState state = state_machine_.GetState();
    if (state != StandbyState::STOPPED) {
        auto result = state_machine_.ProcessEvent(StandbyEvent::STOP);
        if (!result.allowed) {
            LOG(WARNING) << "P2PHotStandbyService: stop transition rejected: "
                         << result.reason;
        }
    }
}

ErrorCode P2PHotStandbyService::Promote(bool force) {
    std::lock_guard<std::mutex> lock(mutex_);
    const bool apply_failed =
        oplog_applier_ != nullptr && !oplog_applier_->IsHealthy();
    const bool force_apply_failure =
        force && apply_failed && GetState() == StandbyState::FAILED;
    if (!IsReadyForPromotion() && !force_apply_failure) {
        LOG(ERROR) << "P2PHotStandbyService: not ready for promotion"
                   << ", state=" << StandbyStateToString(GetState())
                   << ", apply_healthy=" << !apply_failed;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    auto promote_result = state_machine_.ProcessEvent(
        force_apply_failure ? StandbyEvent::FORCE_PROMOTE
                            : StandbyEvent::PROMOTE);
    if (!promote_result.allowed) {
        LOG(ERROR) << "P2PHotStandbyService: cannot promote: "
                   << promote_result.reason;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    const uint64_t applied_before_catch_up =
        GetLocalLastAppliedSequenceIdLocked();
    if (oplog_replicator_) {
        oplog_replicator_->Stop();
    }

    if (force_apply_failure) {
        LOG(ERROR) << "P2PHotStandbyService: forcing promotion with unapplied "
                      "OpLog entry"
                   << ", failed_sequence_id="
                   << oplog_applier_->GetFailedSequenceId()
                   << ", latest_applied_sequence_id="
                   << applied_before_catch_up;
    }

    auto gaps = force_apply_failure
                    ? OpLogApplier::GapResolveResult{}
                    : oplog_applier_->TryResolveGapsOnceForPromotion();
    if (gaps.attempted > 0) {
        LOG(INFO) << "P2PHotStandbyService: promotion gap resolve"
                  << ", attempted=" << gaps.attempted
                  << ", fetched=" << gaps.fetched
                  << ", applied_deletes=" << gaps.applied_deletes;
    }

    auto err = force_apply_failure
                   ? ErrorCode::OK
                   : FinalCatchUpForPromotionLocked(applied_before_catch_up);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "P2PHotStandbyService: final catch-up for promotion "
                      "failed"
                   << ", applied_before_catch_up=" << applied_before_catch_up
                   << ", err=" << err;
        state_machine_.ProcessEvent(StandbyEvent::PROMOTION_FAILED);
        ResetOplogFollowingLocked();
        return err;
    }

    auto success = state_machine_.ProcessEvent(StandbyEvent::PROMOTION_SUCCESS);
    if (!success.allowed) {
        LOG(ERROR) << "P2PHotStandbyService: cannot finish promotion: "
                   << success.reason;
        return ErrorCode::INTERNAL_ERROR;
    }

    ResetOplogFollowingLocked();
    LOG(INFO) << "P2PHotStandbyService promoted"
              << ", latest_applied_sequence_id="
              << GetLocalLastAppliedSequenceIdLocked();
    return ErrorCode::OK;
}

ErrorCode P2PHotStandbyService::FinalCatchUpForPromotionLocked(
    uint64_t current_applied_seq_id) {
    auto catch_up_store = OpLogStoreFactory::Create(
        config_.oplog_store_type, config_.cluster_id, OpLogStoreRole::READER,
        config_.oplog_store_type == OpLogStoreType::REDIS
            ? config_.redis_endpoint
            : config_.oplog_store_root_dir,
        config_.oplog_poll_interval_ms, config_.redis_password,
        config_.redis_username, config_.redis_db_index);
    if (!catch_up_store) {
        LOG(ERROR) << "P2PHotStandbyService: failed to create catch-up store";
        return ErrorCode::INTERNAL_ERROR;
    }

    static constexpr size_t kBatchSize = 1000;
    static constexpr size_t kMaxCatchUpBatches = 100;
    // TODO: Add promotion readiness gating and a clear fail/continue policy for
    // incomplete catch-up: require initial sync completion, enforce max standby
    // lag, handle final catch-up read failures, and monitor apply rate.
    uint64_t read_from_seq = current_applied_seq_id;
    size_t total_applied = 0;
    size_t batch_count = 0;

    for (; batch_count < kMaxCatchUpBatches; ++batch_count) {
        std::vector<OpLogEntry> batch;
        OpLogReadProgress progress;
        ErrorCode read_err = catch_up_store->ReadOpLogSinceWithProgress(
            read_from_seq, kBatchSize, batch, progress);
        if (read_err != ErrorCode::OK) {
            LOG(WARNING) << "P2PHotStandbyService: final catch-up read failed"
                         << ", from_seq=" << read_from_seq
                         << ", error=" << toString(read_err)
                         << ". Proceeding with promotion.";
            break;
        }

        // Keep promotion catch-up best-effort, matching the centralized
        // HotStandbyService availability-first behavior. Confirmed sparse
        // ranges are skipped while existing entries are applied in order.
        total_applied += oplog_applier_->ApplyOpLogEntries(batch);
        if (!oplog_applier_->IsHealthy()) {
            LOG(ERROR) << "P2PHotStandbyService: final catch-up apply failed"
                       << ", failed_sequence_id="
                       << oplog_applier_->GetFailedSequenceId()
                       << ", failed_op_type="
                       << oplog_applier_->GetFailedOpType();
            return ErrorCode::INTERNAL_ERROR;
        }
        oplog_applier_->ConfirmMissingSequenceIds(FindMissingSequenceIds(
            read_from_seq, batch, progress.last_scanned_sequence_id));
        oplog_applier_->ProcessPendingEntries();

        if (progress.last_scanned_sequence_id == read_from_seq) {
            break;
        }
        read_from_seq = progress.last_scanned_sequence_id;
    }

    if (batch_count >= kMaxCatchUpBatches) {
        // Do not block promotion solely on the bounded catch-up loop. The
        // promoted primary continues from the best applied state in this phase.
        LOG(WARNING) << "P2PHotStandbyService: final catch-up reached batch "
                        "limit"
                     << ", max_batches=" << kMaxCatchUpBatches;
    }
    LOG(INFO) << "P2PHotStandbyService: final catch-up done"
              << ", total_applied=" << total_applied
              << ", batches=" << batch_count;
    return ErrorCode::OK;
}

P2PStandbySyncStatus P2PHotStandbyService::GetSyncStatus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    P2PStandbySyncStatus status;
    status.state = state_machine_.GetState();
    status.time_in_state = state_machine_.GetTimeInCurrentState();
    status.is_connected = state_machine_.IsConnected();
    status.applied_seq_id = GetLocalLastAppliedSequenceIdLocked();
    if (oplog_applier_) {
        status.apply_healthy = oplog_applier_->IsHealthy();
        status.failed_sequence_id = oplog_applier_->GetFailedSequenceId();
        status.failed_op_type = oplog_applier_->GetFailedOpType();
        status.failure_reason = oplog_applier_->GetFailureReason();
    }

    if (watcher_oplog_store_) {
        uint64_t latest_seq = 0;
        if (watcher_oplog_store_->GetLatestSequenceId(latest_seq) ==
            ErrorCode::OK) {
            status.primary_seq_id = latest_seq;
        }
    }
    if (status.primary_seq_id > status.applied_seq_id) {
        status.lag_entries = status.primary_seq_id - status.applied_seq_id;
    }
    return status;
}

bool P2PHotStandbyService::IsReadyForPromotion() const {
    return state_machine_.IsReadyForPromotion();
}

uint64_t P2PHotStandbyService::GetLatestAppliedSequenceId() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return GetLocalLastAppliedSequenceIdLocked();
}

uint64_t P2PHotStandbyService::GetLocalLastAppliedSequenceIdLocked() const {
    if (!oplog_applier_) {
        return 0;
    }
    uint64_t expected = oplog_applier_->GetExpectedSequenceId();
    return expected > 0 ? expected - 1 : 0;
}

P2PStandbyMetadataStore::ExportedMetadata P2PHotStandbyService::ExportMetadata()
    const {
    std::lock_guard<std::mutex> lock(mutex_);
    return metadata_store_->ExportMetadata();
}

bool P2PHotStandbyService::WaitForAppliedSequence(
    uint64_t sequence_id, std::chrono::milliseconds timeout) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (GetLatestAppliedSequenceId() >= sequence_id) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return GetLatestAppliedSequenceId() >= sequence_id;
}

void P2PHotStandbyService::OnWatcherEvent(StandbyEvent event) {
    auto result = state_machine_.ProcessEvent(event);
    if (!result.allowed) {
        VLOG(1) << "P2PHotStandbyService: watcher event rejected"
                << ", event=" << StandbyEventToString(event)
                << ", reason=" << result.reason;
    }
    // TODO: Add service-level recovery orchestration for RECONNECTING and
    // RECOVERING states in the follow-up error handling PR.
}

}  // namespace mooncake
