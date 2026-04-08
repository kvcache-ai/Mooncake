#include "ha/oplog/oplog_watcher.h"

#include <chrono>
#include <thread>

#include <glog/logging.h>

#include "ha/oplog/oplog_codec.h"
#include "ha/oplog/oplog_applier.h"

namespace mooncake {

OpLogWatcher::OpLogWatcher(std::shared_ptr<ha::OpLogStore> oplog_store,
                           OpLogApplier* applier)
    : oplog_store_(std::move(oplog_store)), applier_(applier) {
    if (!oplog_store_) {
        LOG(FATAL) << "OpLogWatcher requires a valid OpLogStore";
    }
    if (applier_ == nullptr) {
        LOG(FATAL) << "OpLogWatcher requires a valid OpLogApplier";
    }
}

OpLogWatcher::~OpLogWatcher() { Stop(); }

void OpLogWatcher::Start() { (void)StartFromSequenceId(0); }

bool OpLogWatcher::StartFromSequenceId(uint64_t start_seq_id) {
    if (running_.exchange(true)) {
        return true;
    }

    last_processed_sequence_id_.store(start_seq_id, std::memory_order_release);
    consecutive_errors_.store(0, std::memory_order_release);

    uint64_t cursor = start_seq_id;
    for (;;) {
        auto poll_result = oplog_store_->PollFrom(cursor, kSyncBatchSize,
                                                  std::chrono::milliseconds(0));
        if (!poll_result) {
            LOG(ERROR) << "Failed to bootstrap OpLogWatcher from sequence_id="
                       << start_seq_id
                       << ", error=" << toString(poll_result.error());
            running_.store(false, std::memory_order_release);
            watch_healthy_.store(false, std::memory_order_release);
            return false;
        }

        if (!ApplyPollResult(*poll_result) || poll_result->records.empty()) {
            break;
        }
        cursor = last_processed_sequence_id_.load(std::memory_order_acquire);
    }

    watch_healthy_.store(true, std::memory_order_release);
    watch_thread_ = std::thread(&OpLogWatcher::WatchLoop, this);
    return true;
}

void OpLogWatcher::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (watch_thread_.joinable()) {
        watch_thread_.join();
    }
    watch_healthy_.store(false, std::memory_order_release);
}

uint64_t OpLogWatcher::GetLastProcessedSequenceId() const {
    return last_processed_sequence_id_.load(std::memory_order_acquire);
}

void OpLogWatcher::NotifyStateEvent(StandbyEvent event) const {
    if (state_callback_) {
        state_callback_(event);
    }
}

bool OpLogWatcher::ApplyPollResult(const ha::OpLogPollResult& result) {
    bool applied_any = false;
    for (const auto& record : result.records) {
        auto decoded = ha::oplog::DeserializeEntryPayload(record.payload);
        if (!decoded) {
            LOG(ERROR) << "Failed to decode OpLog record, seq=" << record.seq
                       << ", error=" << toString(decoded.error());
            continue;
        }

        applier_->ApplyOpLogEntry(decoded.value());
        last_processed_sequence_id_.store(decoded->sequence_id,
                                          std::memory_order_release);
        applied_any = true;
    }
    return applied_any;
}

void OpLogWatcher::WatchLoop() {
    while (running_.load(std::memory_order_acquire)) {
        const uint64_t cursor =
            last_processed_sequence_id_.load(std::memory_order_acquire);
        auto poll_result =
            oplog_store_->PollFrom(cursor, kSyncBatchSize, kPollTimeout);
        if (!poll_result) {
            const int error_count =
                consecutive_errors_.fetch_add(1, std::memory_order_acq_rel) + 1;
            if (watch_healthy_.exchange(false, std::memory_order_acq_rel)) {
                NotifyStateEvent(StandbyEvent::WATCH_BROKEN);
            }

            LOG(WARNING) << "OpLogWatcher poll failed, seq=" << cursor
                         << ", error=" << toString(poll_result.error())
                         << ", consecutive_errors=" << error_count;
            if (error_count >= kMaxConsecutiveErrors) {
                NotifyStateEvent(StandbyEvent::MAX_ERRORS_REACHED);
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }

        consecutive_errors_.store(0, std::memory_order_release);
        if (!watch_healthy_.exchange(true, std::memory_order_acq_rel)) {
            NotifyStateEvent(StandbyEvent::WATCH_HEALTHY);
        }

        (void)ApplyPollResult(*poll_result);
    }
}

}  // namespace mooncake
