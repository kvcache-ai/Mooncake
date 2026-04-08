#include "ha/oplog/oplog_gc_controller.h"

#include <algorithm>
#include <chrono>

#include <glog/logging.h>

#include "ha/oplog/oplog_store.h"
#include "ha/progress/standby_progress_store.h"

namespace mooncake {
namespace ha {

namespace {

int64_t CurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool BlocksOpLogGc(const StandbyProgressRecord& record) {
    return record.mode == StandbyProgressMode::kOplogFollowing &&
           record.applied_seq_id > 0 && record.required_seq_id > 0;
}

}  // namespace

OpLogGcController::OpLogGcController(
    std::shared_ptr<OpLogStore> oplog_store,
    std::shared_ptr<StandbyProgressStore> progress_store)
    : oplog_store_(std::move(oplog_store)),
      progress_store_(std::move(progress_store)) {}

ErrorCode OpLogGcController::CleanupAfterSnapshot(
    const SnapshotDescriptor& descriptor) const {
    if (!oplog_store_ || !progress_store_ ||
        descriptor.last_included_seq == 0) {
        return ErrorCode::OK;
    }

    auto progress_records = progress_store_->List();
    if (!progress_records) {
        LOG(WARNING) << "Skip OpLog cleanup after snapshot because standby "
                        "progress listing failed, snapshot_id="
                     << descriptor.snapshot_id
                     << ", error=" << toString(progress_records.error());
        return progress_records.error();
    }

    const int64_t now_ms = CurrentTimeMs();
    OpLogSequenceId safe_gc_seq = descriptor.last_included_seq;
    size_t live_standby_count = 0;
    size_t blocking_standby_count = 0;

    for (const auto& record : progress_records.value()) {
        if (!standby_progress_store_detail::IsStandbyProgressFresh(record,
                                                                   now_ms)) {
            continue;
        }
        ++live_standby_count;

        if (!BlocksOpLogGc(record)) {
            continue;
        }

        ++blocking_standby_count;
        safe_gc_seq =
            std::min<OpLogSequenceId>(safe_gc_seq, record.required_seq_id - 1);
    }

    if (safe_gc_seq == 0) {
        return ErrorCode::OK;
    }

    const ErrorCode err = oplog_store_->CleanupBefore(safe_gc_seq + 1);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to cleanup persistent OpLog after snapshot, "
                     << "snapshot_id=" << descriptor.snapshot_id
                     << ", last_included_seq=" << descriptor.last_included_seq
                     << ", safe_gc_seq=" << safe_gc_seq
                     << ", live_standbys=" << live_standby_count
                     << ", blocking_standbys=" << blocking_standby_count
                     << ", error=" << toString(err);
        return err;
    }

    LOG(INFO) << "Cleaned persistent OpLog after snapshot, snapshot_id="
              << descriptor.snapshot_id
              << ", last_included_seq=" << descriptor.last_included_seq
              << ", safe_gc_seq=" << safe_gc_seq
              << ", live_standbys=" << live_standby_count
              << ", blocking_standbys=" << blocking_standby_count;
    return ErrorCode::OK;
}

}  // namespace ha
}  // namespace mooncake
