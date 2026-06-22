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

    auto on_entry = [this](const OpLogEntry& entry) {
        if (applier_->ApplyOpLogEntry(entry)) {
            uint64_t cur = last_processed_sequence_id_.load();
            while (IsSequenceNewer(entry.sequence_id, cur) &&
                   !last_processed_sequence_id_.compare_exchange_weak(
                       cur, entry.sequence_id)) {
            }
        }
    };

    auto on_error = [this](ErrorCode err) {
        LOG(ERROR) << "OpLogReplicator: notifier error="
                   << static_cast<int>(err);
        NotifyStateEvent(StandbyEvent::WATCH_BROKEN);
    };

    ErrorCode err = notifier_->Start(start_seq_id, on_entry, on_error);
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

uint64_t OpLogReplicator::GetLastProcessedSequenceId() const {
    return last_processed_sequence_id_.load();
}

bool OpLogReplicator::IsHealthy() const {
    return running_.load() && notifier_->IsHealthy();
}

}  // namespace mooncake
