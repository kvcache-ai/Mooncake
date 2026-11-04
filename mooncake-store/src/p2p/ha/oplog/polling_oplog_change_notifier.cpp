#include "p2p/ha/oplog/polling_oplog_change_notifier.h"

#include <glog/logging.h>

#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/ha_status.h"

namespace mooncake {

PollingOpLogChangeNotifier::PollingOpLogChangeNotifier(OpLogStore* store,
                                                       int poll_interval_ms,
                                                       size_t poll_batch_size)
    : store_(store),
      poll_interval_ms_(poll_interval_ms),
      poll_batch_size_(poll_batch_size) {
    CHECK_GT(poll_batch_size_, 0);
}

PollingOpLogChangeNotifier::~PollingOpLogChangeNotifier() { Stop(); }

ErrorCode PollingOpLogChangeNotifier::Start(
    uint64_t start_sequence_id, EntryCallback on_entry, ErrorCallback on_error,
    MaintenanceCallback on_maintenance) {
    if (running_.load()) {
        return ErrorCode::INTERNAL_ERROR;
    }

    on_entry_ = std::move(on_entry);
    on_error_ = std::move(on_error);
    on_maintenance_ = std::move(on_maintenance);
    last_scanned_sequence_id_.store(start_sequence_id);
    running_.store(true);
    healthy_.store(true);
    poll_thread_ = std::thread(&PollingOpLogChangeNotifier::PollLoop, this);

    return ErrorCode::OK;
}

void PollingOpLogChangeNotifier::Stop() {
    running_.store(false);
    stop_cv_.notify_all();
    if (poll_thread_.joinable()) {
        poll_thread_.join();
    }
    healthy_.store(false);
}

bool PollingOpLogChangeNotifier::IsHealthy() const {
    return running_.load() && healthy_.load();
}

void PollingOpLogChangeNotifier::PollLoop() {
    while (running_.load()) {
        uint64_t last_seq = last_scanned_sequence_id_.load();
        std::vector<OpLogEntry> entries;
        OpLogReadProgress progress;
        auto err = store_->ReadOpLogSinceWithProgress(
            last_seq, poll_batch_size_, entries, progress);
        std::vector<uint64_t> missing_sequence_ids;
        if (err == ErrorCode::OK) {
            // Keep the issue active until an actual poll succeeds, even if it
            // returns no entries. Restarting the reader must not clear it.
            HAStatusManager::instance().SetDoctorIssue("OPLOG_READ_FAILED");
            missing_sequence_ids = FindMissingSequenceIds(
                last_seq, entries, progress.last_scanned_sequence_id);
            HAMetricManager::instance()
                .set_oplog_last_successful_poll_timestamp_ms(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count());
            if (!missing_sequence_ids.empty()) {
                HAMetricManager::instance().inc_oplog_confirmed_holes(
                    static_cast<int64_t>(missing_sequence_ids.size()));
            }
        }

        const bool made_progress = err == ErrorCode::OK &&
                                   progress.last_scanned_sequence_id > last_seq;

        if (err == ErrorCode::OK && !entries.empty()) {
            for (const auto& entry : entries) {
                if (!running_.load()) break;
                on_entry_(entry);
            }
        } else if (err != ErrorCode::OK) {
            // Store and replicator logs already describe the failure; persist
            // the doctor issue across notifier restarts.
            HAStatusManager::instance().SetDoctorIssue(
                "OPLOG_READ_FAILED",
                "Read after seq=" + std::to_string(last_seq) + ": " +
                    std::string(toString(err)));
            HAMetricManager::instance().inc_oplog_read_failures();
            if (on_error_) {
                on_error_(err);
            }
            healthy_.store(false);
        }

        if (err == ErrorCode::OK) {
            last_scanned_sequence_id_.store(progress.last_scanned_sequence_id);
            healthy_.store(true);
        }
        if (on_maintenance_) {
            on_maintenance_(missing_sequence_ids);
        }
        if (made_progress) {
            // Keep draining backlog even when the scanned range only contained
            // confirmed missing entries.
            continue;
        }

        // Interruptible sleep: wakes immediately on Stop()
        {
            std::unique_lock<std::mutex> lock(stop_mutex_);
            stop_cv_.wait_for(lock,
                              std::chrono::milliseconds(poll_interval_ms_),
                              [&] { return !running_.load(); });
        }
    }
}

}  // namespace mooncake
