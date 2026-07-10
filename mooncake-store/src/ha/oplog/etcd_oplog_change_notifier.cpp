#include "ha/oplog/etcd_oplog_change_notifier.h"

#include <algorithm>
#include <chrono>
#include <glog/logging.h>
#include <sstream>
#include <thread>

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "ha_metric_manager.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_serializer.h"

namespace mooncake {

EtcdOpLogChangeNotifier::EtcdOpLogChangeNotifier(const std::string& cluster_id,
                                                 EtcdOpLogStore* oplog_store)
    : cluster_id_(cluster_id), oplog_store_(oplog_store) {
    if (!NormalizeAndValidateClusterId(cluster_id_)) {
        LOG(FATAL) << "Invalid cluster_id for EtcdOpLogChangeNotifier: '"
                   << cluster_id_
                   << "'. Allowed chars: [A-Za-z0-9_.-], max_len=128.";
    }
    watch_prefix_ = "/oplog/" + cluster_id_ + "/";

    callback_ctx_ = new ChangeNotifierCallbackContext();
    callback_ctx_->notifier = this;
}

EtcdOpLogChangeNotifier::~EtcdOpLogChangeNotifier() {
    Stop();
    // If Stop() returned early (Change Notifier was never started),
    // callback_ctx_ was never freed.  It is safe to delete here because no
    // goroutine / watch thread was ever launched, so no callbacks can be
    // in-flight. In all other paths, Stop() already sets callback_ctx_ to
    // nullptr, so `delete nullptr` is a harmless no-op.
    delete callback_ctx_;
    callback_ctx_ = nullptr;
}

ErrorCode EtcdOpLogChangeNotifier::Start(uint64_t start_sequence_id,
                                         EntryCallback on_entry,
                                         ErrorCallback on_error) {
    if (running_.load()) {
        LOG(WARNING) << "EtcdOpLogChangeNotifier is already running";
        return ErrorCode::OK;
    }

    on_entry_ = std::move(on_entry);
    on_error_ = std::move(on_error);
    last_processed_sequence_id_.store(start_sequence_id);

    // Initial sync: read and deliver historical entries
    if (oplog_store_) {
        int64_t delivered = DeliverHistoricalEntries(start_sequence_id);
        if (delivered < 0) {
            next_watch_revision_.store(0);
        }
        LOG(INFO) << "EtcdOpLogChangeNotifier initial sync done"
                  << ", last_seq=" << last_processed_sequence_id_.load()
                  << ", next_watch_revision=" << next_watch_revision_.load();
    }

    running_.store(true);
    watch_thread_ = std::thread(&EtcdOpLogChangeNotifier::WatchLoop, this);
    LOG(INFO) << "EtcdOpLogChangeNotifier started for cluster_id="
              << cluster_id_;
    return ErrorCode::OK;
}

void EtcdOpLogChangeNotifier::Stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);

    // Invalidate the callback context under the mutex.
    if (callback_ctx_) {
        std::lock_guard<std::mutex> lock(callback_ctx_->mutex);
        callback_ctx_->notifier = nullptr;
    }

    if (watch_thread_.joinable()) {
        watch_thread_.join();
    }

    // Cancel the Go goroutine and wait for it to fully exit.
    ErrorCode err = EtcdHelper::CancelWatchWithPrefix(watch_prefix_.c_str(),
                                                      watch_prefix_.size());
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to cancel watch for prefix " << watch_prefix_
                     << ", error=" << static_cast<int>(err);
    }

    ErrorCode wait_err = EtcdHelper::WaitWatchWithPrefixStopped(
        watch_prefix_.c_str(), watch_prefix_.size(), /*timeout_ms=*/5000);

    if (wait_err == ErrorCode::OK) {
        delete callback_ctx_;
    } else {
        LOG(WARNING) << "Watch goroutine did not stop in time for prefix "
                     << watch_prefix_
                     << "; leaking ChangeNotifierCallbackContext to avoid UAF";
    }
    callback_ctx_ = nullptr;

    LOG(INFO) << "EtcdOpLogChangeNotifier stopped";
}

bool EtcdOpLogChangeNotifier::IsHealthy() const {
    return watch_healthy_.load();
}

bool EtcdOpLogChangeNotifier::ReadOpLogSince(uint64_t start_seq_id,
                                             std::vector<OpLogEntry>& entries,
                                             EtcdRevisionId& revision_id) {
    if (!oplog_store_) {
        return false;
    }
    ErrorCode err = oplog_store_->ReadOpLogSinceWithRevision(
        start_seq_id, kSyncBatchSize, entries, revision_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to read OpLog since sequence_id=" << start_seq_id
                   << ", error=" << static_cast<int>(err);
        return false;
    }
    return true;
}

void EtcdOpLogChangeNotifier::WatchCallback(void* context, const char* key,
                                            size_t key_size, const char* value,
                                            size_t value_size, int event_type,
                                            int64_t mod_revision) {
    auto* ctx = static_cast<ChangeNotifierCallbackContext*>(context);
    if (ctx == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(ctx->mutex);
    EtcdOpLogChangeNotifier* notifier = ctx->notifier;
    if (notifier == nullptr) {
        return;
    }

    if (!notifier->running_.load(std::memory_order_acquire)) {
        return;
    }

    std::string key_str;
    if (key != nullptr && key_size > 0) {
        key_str.assign(key, key_size);
    }
    std::string value_str;
    if (value != nullptr && value_size > 0) {
        value_str = std::string(value, value_size);
    }
    notifier->HandleWatchEvent(key_str, value_str, event_type, mod_revision);
}

void EtcdOpLogChangeNotifier::WatchLoop() {
    LOG(INFO) << "OpLog watch thread started for cluster_id=" << cluster_id_;

    while (running_.load()) {
        // Cancel any existing watch before starting a new one
        (void)EtcdHelper::CancelWatchWithPrefix(watch_prefix_.c_str(),
                                                watch_prefix_.size());
        (void)EtcdHelper::WaitWatchWithPrefixStopped(watch_prefix_.c_str(),
                                                     watch_prefix_.size(),
                                                     /*timeout_ms=*/5000);

        EtcdRevisionId start_rev =
            static_cast<EtcdRevisionId>(next_watch_revision_.load());
        ErrorCode err = EtcdHelper::WatchWithPrefixFromRevision(
            watch_prefix_.c_str(), watch_prefix_.size(), start_rev,
            callback_ctx_, WatchCallback);

        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start watch for prefix " << watch_prefix_
                       << ", error=" << static_cast<int>(err);
            watch_healthy_.store(false);
            if (on_error_) {
                on_error_(err);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            TryReconnect();
            continue;
        }

        LOG(INFO) << "Watch started for prefix " << watch_prefix_;
        watch_healthy_.store(true);
        consecutive_errors_.store(0);

        while (running_.load() && watch_healthy_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            if (consecutive_errors_.load() >= kMaxConsecutiveErrors) {
                LOG(WARNING)
                    << "Too many consecutive errors ("
                    << consecutive_errors_.load() << "), reconnecting watch...";
                watch_healthy_.store(false);
                break;
            }
        }

        if (running_.load() && !watch_healthy_.load()) {
            (void)EtcdHelper::CancelWatchWithPrefix(watch_prefix_.c_str(),
                                                    watch_prefix_.size());
            (void)EtcdHelper::WaitWatchWithPrefixStopped(watch_prefix_.c_str(),
                                                         watch_prefix_.size(),
                                                         /*timeout_ms=*/5000);
            TryReconnect();
        }
    }

    LOG(INFO) << "OpLog watch thread stopped";
}

void EtcdOpLogChangeNotifier::HandleWatchEvent(const std::string& key,
                                               const std::string& value,
                                               int event_type,
                                               int64_t mod_revision) {
    // event_type: 0 = PUT, 1 = DELETE, 2 = WATCH_BROKEN
    if (event_type == 2) {
        LOG(WARNING) << "OpLog watch broken, will reconnect. cluster_id="
                     << cluster_id_
                     << ", next_watch_revision=" << next_watch_revision_.load()
                     << ", last_seq=" << last_processed_sequence_id_.load();
        watch_healthy_.store(false);
        consecutive_errors_.fetch_add(1);
        return;
    }

    if (mod_revision > 0) {
        int64_t candidate = mod_revision + 1;
        int64_t cur = next_watch_revision_.load();
        while (candidate > cur &&
               !next_watch_revision_.compare_exchange_weak(cur, candidate)) {
        }
    }

    if (event_type == 1) {
        VLOG(1) << "OpLog entry deleted: " << key;
        consecutive_errors_.store(0);
        return;
    }

    if (event_type != 0) {
        LOG(WARNING) << "Unknown event type: " << event_type
                     << " for key: " << key;
        consecutive_errors_.fetch_add(1);
        return;
    }

    // Skip the "latest" key and snapshot keys
    if (key.find("/latest") != std::string::npos ||
        key.find("/snapshot/") != std::string::npos) {
        return;
    }

    // Parse the OpLog entry (DeserializeOpLogEntry validates entry size)
    OpLogEntry entry;
    if (!DeserializeOpLogEntry(value, entry)) {
        LOG(ERROR) << "Failed to deserialize OpLog entry from key: " << key;
        consecutive_errors_.fetch_add(1);
        return;
    }

    // Verify checksum at the trust boundary (data from etcd)
    if (!OpLogManager::VerifyChecksum(entry)) {
        LOG(ERROR) << "OpLog entry checksum mismatch: sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ". Possible data corruption. Discarding entry.";
        consecutive_errors_.fetch_add(1);
        HAMetricManager::instance().inc_oplog_checksum_failures();
        return;
    }

    // Deliver to callback
    if (on_entry_) {
        on_entry_(entry);
    }

    // Update last processed (monotonic)
    uint64_t cur = last_processed_sequence_id_.load();
    while (IsSequenceNewer(entry.sequence_id, cur) &&
           !last_processed_sequence_id_.compare_exchange_weak(
               cur, entry.sequence_id)) {
    }

    consecutive_errors_.store(0);
    reconnect_count_.store(0);
    VLOG(2) << "Delivered OpLog entry: sequence_id=" << entry.sequence_id
            << ", op_type=" << static_cast<int>(entry.op_type)
            << ", key=" << entry.object_key;
}

void EtcdOpLogChangeNotifier::TryReconnect() {
    if (!running_.load()) {
        return;
    }

    int reconnect_attempt = reconnect_count_.fetch_add(1) + 1;
    int delay_ms =
        std::min(kReconnectDelayMs * reconnect_attempt, kMaxReconnectDelayMs);

    LOG(INFO) << "Attempting to reconnect watch (attempt #" << reconnect_attempt
              << "), waiting " << delay_ms << "ms...";

    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

    if (SyncMissedEntries()) {
        LOG(INFO) << "Successfully synced missed OpLog entries";
    } else {
        LOG(WARNING)
            << "Failed to sync missed OpLog entries, continuing anyway";
    }
}

bool EtcdOpLogChangeNotifier::SyncMissedEntries() {
    uint64_t last_seq = last_processed_sequence_id_.load();
    if (last_seq == 0) {
        return true;
    }
    LOG(INFO) << "Syncing missed OpLog entries since sequence_id=" << last_seq;
    int64_t delivered = DeliverHistoricalEntries(last_seq);
    if (delivered < 0) {
        return false;
    }
    LOG(INFO) << "Synced " << delivered << " missed OpLog entries";
    return true;
}

int64_t EtcdOpLogChangeNotifier::DeliverHistoricalEntries(
    uint64_t start_seq_id) {
    uint64_t read_seq_id = start_seq_id;
    EtcdRevisionId last_read_rev = 0;
    int64_t total_delivered = 0;

    for (;;) {
        std::vector<OpLogEntry> batch;
        EtcdRevisionId rev = 0;
        if (!ReadOpLogSince(read_seq_id, batch, rev)) {
            return -1;
        }
        if (rev > 0) {
            last_read_rev = rev;
        }
        for (const auto& entry : batch) {
            if (on_entry_) {
                on_entry_(entry);
            }
            last_processed_sequence_id_.store(entry.sequence_id);
            read_seq_id = entry.sequence_id;
            total_delivered++;
        }
        if (batch.size() < kSyncBatchSize) {
            break;
        }
    }

    if (last_read_rev > 0) {
        next_watch_revision_.store(static_cast<int64_t>(last_read_rev + 1));
    }

    return total_delivered;
}

}  // namespace mooncake

#else  // STORE_USE_ETCD not defined

namespace mooncake {

EtcdOpLogChangeNotifier::EtcdOpLogChangeNotifier(const std::string& cluster_id,
                                                 EtcdOpLogStore* oplog_store)
    : cluster_id_(cluster_id), oplog_store_(oplog_store) {
    callback_ctx_ = new ChangeNotifierCallbackContext();
    callback_ctx_->notifier = this;
}

EtcdOpLogChangeNotifier::~EtcdOpLogChangeNotifier() {
    Stop();
    delete callback_ctx_;
    callback_ctx_ = nullptr;
}

ErrorCode EtcdOpLogChangeNotifier::Start(uint64_t /*start_sequence_id*/,
                                         EntryCallback /*on_entry*/,
                                         ErrorCallback /*on_error*/) {
    LOG(ERROR) << "EtcdOpLogChangeNotifier requires STORE_USE_ETCD";
    return ErrorCode::ETCD_OPERATION_ERROR;
}

void EtcdOpLogChangeNotifier::Stop() {}

bool EtcdOpLogChangeNotifier::IsHealthy() const { return false; }

bool EtcdOpLogChangeNotifier::ReadOpLogSince(
    uint64_t /*start_seq_id*/, std::vector<OpLogEntry>& /*entries*/,
    EtcdRevisionId& /*revision_id*/) {
    return false;
}

void EtcdOpLogChangeNotifier::WatchCallback(
    void* /*context*/, const char* /*key*/, size_t /*key_size*/,
    const char* /*value*/, size_t /*value_size*/, int /*event_type*/,
    int64_t /*mod_revision*/) {}

void EtcdOpLogChangeNotifier::WatchLoop() {}

void EtcdOpLogChangeNotifier::HandleWatchEvent(const std::string& /*key*/,
                                               const std::string& /*value*/,
                                               int /*event_type*/,
                                               int64_t /*mod_revision*/) {}

void EtcdOpLogChangeNotifier::TryReconnect() {}

bool EtcdOpLogChangeNotifier::SyncMissedEntries() { return false; }

int64_t EtcdOpLogChangeNotifier::DeliverHistoricalEntries(
    uint64_t /*start_seq_id*/) {
    return -1;
}

}  // namespace mooncake

#endif  // STORE_USE_ETCD
