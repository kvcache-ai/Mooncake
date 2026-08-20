#include "p2p/ha/oplog/oplog_replicator.h"

#include <glog/logging.h>

#include <algorithm>

#include "p2p/ha/oplog/p2p_oplog_applier_base.h"
#include "p2p/p2p_ha_metric_manager.h"

namespace mooncake {

OpLogReplicator::OpLogReplicator(OpLogChangeNotifier* notifier,
                                 P2POpLogApplierBase* applier)
    : notifier_(notifier), applier_(applier) {
    if (notifier_ == nullptr) {
        LOG(FATAL) << "OpLogChangeNotifier cannot be null";
    }
    if (applier_ == nullptr) {
        LOG(FATAL) << "P2POpLogApplierBase cannot be null";
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
        NotifyStateEvent(err == ErrorCode::OPLOG_TRIMMED
                             ? P2PStandbyEvent::RESYNC_REQUIRED
                             : P2PStandbyEvent::WATCH_BROKEN);
    };

    auto on_maintenance =
        [this](const std::vector<uint64_t>& missing_sequences) {
            applier_->ConfirmMissingSequenceIds(missing_sequences);
            applier_->ProcessPendingEntries();
            ReportApplyFailureIfNeeded();
            const uint64_t expected = applier_->GetExpectedSequenceId();
            if (expected > 0) {
                AdvanceLastProcessedSequenceId(expected - 1);
                const int64_t latest =
                    P2PHAMetricManager::instance().get_oplog_last_sequence_id();
                const int64_t applied = static_cast<int64_t>(expected - 1);
                P2PHAMetricManager::instance().set_oplog_standby_lag(
                    std::max<int64_t>(0, latest - applied));
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
    NotifyStateEvent(P2PStandbyEvent::WATCH_HEALTHY);
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
    NotifyStateEvent(P2PStandbyEvent::FATAL_ERROR);
}

uint64_t OpLogReplicator::GetLastProcessedSequenceId() const {
    return last_processed_sequence_id_.load();
}

bool OpLogReplicator::IsHealthy() const {
    return running_.load() && notifier_->IsHealthy() && applier_->IsHealthy();
}

}  // namespace mooncake
