#include "ha/oplog/oplog_replicator.h"

#include <glog/logging.h>

#include "ha/oplog/oplog_applier.h"

namespace mooncake {

OpLogReplicator::OpLogReplicator(OpLogChangeNotifier* notifier,
                                 OpLogApplier* applier)
    : notifier_(notifier), applier_(applier) {
    if (notifier_ == nullptr) {
        LOG(FATAL) << "OpLogChangeNotifier cannot be null";
    }
    if (applier_ == nullptr) {
        LOG(FATAL) << "OpLogApplier cannot be null";
    }
}

OpLogReplicator::~OpLogReplicator() { Stop(); }

void OpLogReplicator::Start() {
    // Backward-compatible: start from the last processed sequence id.
    (void)StartFromSequenceId(last_processed_sequence_id_.load());
}

bool OpLogReplicator::StartFromSequenceId(uint64_t start_seq_id) {
    if (running_.load()) {
        LOG(WARNING) << "OpLogReplicator is already running";
        return true;
    }

    apply_failure_reported_.store(false);

    auto on_entry = [this](const OpLogEntry& entry) {
        if (applier_->ApplyOpLogEntry(entry)) {
            AdvanceLastProcessedSequenceId(entry.sequence_id);
        }
        ReportApplyFailureIfNeeded();
    };

    auto on_error = [this](ErrorCode err) {
        LOG(ERROR) << "OpLogReplicator: notifier error="
                   << static_cast<int>(err);
        NotifyStateEvent(StandbyEvent::WATCH_BROKEN);
    };

    auto on_maintenance =
        [this](const std::vector<uint64_t>& missing_sequences) {
            applier_->ConfirmMissingSequenceIds(missing_sequences);
            applier_->ProcessPendingEntries();
            ReportApplyFailureIfNeeded();
            const uint64_t expected = applier_->GetExpectedSequenceId();
            if (expected > 0) {
                AdvanceLastProcessedSequenceId(expected - 1);
            }
        };

    ErrorCode err =
        notifier_->Start(start_seq_id, on_entry, on_error, on_maintenance);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to start OpLogChangeNotifier, error="
                   << static_cast<int>(err);
        return false;
    }

    running_.store(true);
    NotifyStateEvent(StandbyEvent::WATCH_HEALTHY);
    LOG(INFO) << "OpLogReplicator started from sequence_id=" << start_seq_id;
    return true;
}

void OpLogReplicator::Stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);
    notifier_->Stop();
    LOG(INFO) << "OpLogReplicator stopped";
}

void OpLogReplicator::AdvanceLastProcessedSequenceId(uint64_t sequence_id) {
    uint64_t current = last_processed_sequence_id_.load();
    while (IsSequenceNewer(sequence_id, current) &&
           !last_processed_sequence_id_.compare_exchange_weak(current,
                                                              sequence_id)) {
    }
}

void OpLogReplicator::ReportApplyFailureIfNeeded() {
    if (applier_->IsHealthy() || apply_failure_reported_.exchange(true)) {
        return;
    }
    LOG(ERROR) << "OpLogReplicator: critical apply failure"
               << ", sequence_id=" << applier_->GetFailedSequenceId();
    NotifyStateEvent(StandbyEvent::FATAL_ERROR);
}

uint64_t OpLogReplicator::GetLastProcessedSequenceId() const {
    return last_processed_sequence_id_.load();
}

bool OpLogReplicator::IsHealthy() const {
    return running_.load() && notifier_->IsHealthy() && applier_->IsHealthy();
}

}  // namespace mooncake
