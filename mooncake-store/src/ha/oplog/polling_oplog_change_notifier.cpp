#include "ha/oplog/polling_oplog_change_notifier.h"

#include <glog/logging.h>

namespace mooncake {

PollingOpLogChangeNotifier::PollingOpLogChangeNotifier(OpLogStore* store,
                                                       int poll_interval_ms)
    : store_(store), poll_interval_ms_(poll_interval_ms) {}

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
        auto err = store_->ReadOpLogSinceWithProgress(last_seq, kPollBatchSize,
                                                      entries, progress);
        std::vector<uint64_t> missing_sequence_ids;
        if (err == ErrorCode::OK) {
            missing_sequence_ids = FindMissingSequenceIds(
                last_seq, entries, progress.last_scanned_sequence_id);
        }

        if (err == ErrorCode::OK && !entries.empty()) {
            for (const auto& entry : entries) {
                if (!running_.load()) break;
                on_entry_(entry);
            }
            last_scanned_sequence_id_.store(progress.last_scanned_sequence_id);
            healthy_.store(true);
            if (on_maintenance_) {
                on_maintenance_(missing_sequence_ids);
            }
            // Data available — poll again immediately to drain backlog
            continue;
        } else if (err != ErrorCode::OK) {
            if (on_error_) {
                on_error_(err);
            }
            healthy_.store(false);
        }

        if (err == ErrorCode::OK) {
            last_scanned_sequence_id_.store(progress.last_scanned_sequence_id);
        }
        if (on_maintenance_) {
            on_maintenance_(missing_sequence_ids);
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
