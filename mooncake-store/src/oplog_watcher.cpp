#include "oplog_watcher.h"

#include <algorithm>
#include <chrono>
#include <glog/logging.h>
#include <sstream>
#include <thread>

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "etcd_oplog_store.h"
#include "oplog_applier.h"

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif

namespace mooncake {

OpLogWatcher::OpLogWatcher(const std::string& etcd_endpoints,
                           const std::string& cluster_id, OpLogApplier* applier)
    : etcd_endpoints_(etcd_endpoints), cluster_id_(cluster_id), applier_(applier) {
    if (applier_ == nullptr) {
        LOG(FATAL) << "OpLogApplier cannot be null";
    }
}

OpLogWatcher::~OpLogWatcher() {
    Stop();
}

void OpLogWatcher::Start() {
    // Backward-compatible: start from the last processed sequence id.
    (void)StartFromSequenceId(last_processed_sequence_id_.load());
}

bool OpLogWatcher::StartFromSequenceId(uint64_t start_seq_id) {
    if (running_.load()) {
        LOG(WARNING) << "OpLogWatcher is already running";
        return true;
    }

#ifdef STORE_USE_ETCD
    uint64_t cursor_seq = start_seq_id;
    EtcdRevisionId last_read_rev = 0;
    size_t total_applied = 0;

    for (;;) {
        std::vector<OpLogEntry> batch;
        EtcdRevisionId rev = 0;
        if (!ReadOpLogSinceWithRevision(cursor_seq, batch, rev)) {
            last_read_rev = 0;
            break;
        }
        last_read_rev = rev;
        if (!batch.empty()) {
            for (const auto& e : batch) {
                if (applier_->ApplyOpLogEntry(e)) {
                    last_processed_sequence_id_.store(e.sequence_id);
                    cursor_seq = e.sequence_id;
                    total_applied++;
                }
            }
        }
        if (batch.size() < kSyncBatchSize) {
            break;
        }
    }

    if (last_read_rev > 0) {
        next_watch_revision_.store(static_cast<int64_t>(last_read_rev + 1));
    } else {
        next_watch_revision_.store(0);
    }

    LOG(INFO) << "OpLogWatcher initial sync done: applied=" << total_applied
              << ", last_seq=" << last_processed_sequence_id_.load()
              << ", next_watch_revision=" << next_watch_revision_.load();
#endif

    running_.store(true);
    watch_thread_ = std::thread(&OpLogWatcher::WatchOpLog, this);
    LOG(INFO) << "OpLogWatcher started for cluster_id=" << cluster_id_;
    return true;
}

void OpLogWatcher::Stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);

#ifdef STORE_USE_ETCD
    // Cancel the watch
    std::string watch_prefix = "/oplog/" + cluster_id_ + "/";
    ErrorCode err = EtcdHelper::CancelWatchWithPrefix(watch_prefix.c_str(), watch_prefix.size());
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to cancel watch for prefix " << watch_prefix
                     << ", error=" << static_cast<int>(err);
    }
#endif

    // Wait for watch thread to finish
    if (watch_thread_.joinable()) {
        watch_thread_.join();
    }

    LOG(INFO) << "OpLogWatcher stopped";
}

bool OpLogWatcher::ReadOpLogSince(uint64_t start_seq_id,
                                  std::vector<OpLogEntry>& entries) {
#ifdef STORE_USE_ETCD
    EtcdOpLogStore oplog_store(cluster_id_, /*enable_latest_seq_batch_update=*/false);
    ErrorCode err = oplog_store.ReadOpLogSince(start_seq_id, 1000, entries);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to read OpLog since sequence_id=" << start_seq_id
                   << ", error=" << static_cast<int>(err);
        return false;
    }
    LOG(INFO) << "Read " << entries.size() << " OpLog entries since sequence_id="
              << start_seq_id;
    return true;
#else
    LOG(ERROR) << "STORE_USE_ETCD is not enabled, cannot read OpLog from etcd";
    return false;
#endif
}

bool OpLogWatcher::ReadOpLogSinceWithRevision(uint64_t start_seq_id,
                                             std::vector<OpLogEntry>& entries,
                                             EtcdRevisionId& revision_id) {
#ifdef STORE_USE_ETCD
    EtcdOpLogStore oplog_store(cluster_id_, /*enable_latest_seq_batch_update=*/false);
    ErrorCode err = oplog_store.ReadOpLogSinceWithRevision(
        start_seq_id, kSyncBatchSize, entries, revision_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to read OpLog since sequence_id=" << start_seq_id
                   << ", error=" << static_cast<int>(err);
        return false;
    }
    return true;
#else
    (void)start_seq_id;
    (void)entries;
    (void)revision_id;
    return false;
#endif
}

uint64_t OpLogWatcher::GetLastProcessedSequenceId() const {
    return last_processed_sequence_id_.load();
}

// Static callback function for etcd Watch (defined before WatchOpLog uses it)
void OpLogWatcher::WatchCallback(void* context, const char* key, size_t key_size,
                                 const char* value, size_t value_size, int event_type) {
    OpLogWatcher* watcher = static_cast<OpLogWatcher*>(context);
    if (watcher == nullptr) {
        LOG(ERROR) << "OpLogWatcher context is null";
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

    watcher->HandleWatchEvent(key_str, value_str, event_type, /*mod_revision=*/0);
}

void OpLogWatcher::WatchCallbackV2(void* context, const char* key, size_t key_size,
                                   const char* value, size_t value_size,
                                   int event_type, int64_t mod_revision) {
    OpLogWatcher* watcher = static_cast<OpLogWatcher*>(context);
    if (watcher == nullptr) {
        LOG(ERROR) << "OpLogWatcher context is null";
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
    watcher->HandleWatchEvent(key_str, value_str, event_type, mod_revision);
}

void OpLogWatcher::WatchOpLog() {
#ifdef STORE_USE_ETCD
    LOG(INFO) << "OpLog watch thread started for cluster_id=" << cluster_id_;

    std::string watch_prefix = "/oplog/" + cluster_id_ + "/";

    while (running_.load()) {
        // Start watching - pass static callback function and this pointer as context
        EtcdRevisionId start_rev =
            static_cast<EtcdRevisionId>(next_watch_revision_.load());
        // Always use V2 watcher so we can update next_watch_revision_ precisely.
        ErrorCode err = EtcdHelper::WatchWithPrefixFromRevisionV2(
            watch_prefix.c_str(), watch_prefix.size(), start_rev, this, WatchCallbackV2);
        
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start watch for prefix " << watch_prefix
                       << ", error=" << static_cast<int>(err);
            watch_healthy_.store(false);
            
            // Try to reconnect
            TryReconnect();
            continue;
        }

        LOG(INFO) << "Watch started for prefix " << watch_prefix;
        watch_healthy_.store(true);
        consecutive_errors_.store(0);

        // The watch is now running in the background (via Go goroutine)
        // We just need to keep the thread alive until Stop() is called or watch fails
        while (running_.load() && watch_healthy_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            // Periodically check watch health
            if (consecutive_errors_.load() >= kMaxConsecutiveErrors) {
                LOG(WARNING) << "Too many consecutive errors (" << consecutive_errors_.load()
                            << "), reconnecting watch...";
                watch_healthy_.store(false);
                break;
            }
        }
        
        if (running_.load() && !watch_healthy_.load()) {
            // Cancel current watch before reconnecting
            EtcdHelper::CancelWatchWithPrefix(watch_prefix.c_str(), watch_prefix.size());
            TryReconnect();
        }
    }

    LOG(INFO) << "OpLog watch thread stopped";
#else
    LOG(ERROR) << "STORE_USE_ETCD is not enabled, cannot watch OpLog from etcd";
    running_.store(false);
#endif
}

void OpLogWatcher::TryReconnect() {
    if (!running_.load()) {
        return;
    }
    
    int reconnect_attempt = reconnect_count_.fetch_add(1) + 1;
    
    // Calculate delay with exponential backoff
    int delay_ms = std::min(kReconnectDelayMs * reconnect_attempt, kMaxReconnectDelayMs);
    
    LOG(INFO) << "Attempting to reconnect watch (attempt #" << reconnect_attempt
              << "), waiting " << delay_ms << "ms...";
    
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    
    // Sync any missed entries before resuming watch
    if (SyncMissedEntries()) {
        LOG(INFO) << "Successfully synced missed OpLog entries";
    } else {
        LOG(WARNING) << "Failed to sync missed OpLog entries, continuing anyway";
    }
}

bool OpLogWatcher::SyncMissedEntries() {
#ifdef STORE_USE_ETCD
    uint64_t last_seq = last_processed_sequence_id_.load();
    if (last_seq == 0) {
        // No entries processed yet, nothing to sync
        return true;
    }
    
    LOG(INFO) << "Syncing missed OpLog entries since sequence_id=" << last_seq;
    
    std::vector<OpLogEntry> entries;
    EtcdRevisionId rev = 0;
    if (!ReadOpLogSinceWithRevision(last_seq, entries, rev)) {
        LOG(ERROR) << "Failed to read missed OpLog entries";
        return false;
    }
    if (rev > 0) {
        next_watch_revision_.store(static_cast<int64_t>(rev + 1));
    }
    
    if (entries.empty()) {
        LOG(INFO) << "No missed OpLog entries to sync";
        return true;
    }
    
    LOG(INFO) << "Syncing " << entries.size() << " missed OpLog entries";
    
    for (const auto& entry : entries) {
        if (applier_->ApplyOpLogEntry(entry)) {
            last_processed_sequence_id_.store(entry.sequence_id);
        } else {
            LOG(WARNING) << "Failed to apply missed OpLog entry, sequence_id="
                        << entry.sequence_id;
        }
    }
    
    return true;
#else
    return false;
#endif
}

void OpLogWatcher::HandleWatchEvent(const std::string& key, const std::string& value,
                                    int event_type) {
    HandleWatchEvent(key, value, event_type, /*mod_revision=*/0);
}

void OpLogWatcher::HandleWatchEvent(const std::string& key, const std::string& value,
                                    int event_type, int64_t mod_revision) {
    // event_type:
    // 0 = PUT, 1 = DELETE, 2 = WATCH_BROKEN (Go watcher terminated; should reconnect)
    if (event_type == 2) {
        LOG(WARNING) << "OpLog watch broken, will reconnect. cluster_id=" << cluster_id_
                     << ", next_watch_revision=" << next_watch_revision_.load()
                     << ", last_seq=" << last_processed_sequence_id_.load();
        watch_healthy_.store(false);
        consecutive_errors_.fetch_add(1);
        return;
    }

    if (mod_revision > 0) {
        // Keep next_watch_revision_ monotonic: next = max(next, modRev+1)
        int64_t candidate = mod_revision + 1;
        int64_t cur = next_watch_revision_.load();
        while (candidate > cur &&
               !next_watch_revision_.compare_exchange_weak(cur, candidate)) {
            // retry
        }
    }
    // event_type: 0 = PUT, 1 = DELETE
    if (event_type == 1) {
        // DELETE event - OpLog entry was cleaned up
        VLOG(1) << "OpLog entry deleted: " << key;
        consecutive_errors_.store(0);  // Watch is working
        return;
    }

    if (event_type != 0) {
        LOG(WARNING) << "Unknown event type: " << event_type << " for key: " << key;
        consecutive_errors_.fetch_add(1);
        return;
    }

    // Skip the "latest" key and snapshot keys
    if (key.find("/latest") != std::string::npos ||
        key.find("/snapshot/") != std::string::npos) {
        return;
    }

    // Parse the OpLog entry from JSON
    OpLogEntry entry;
    if (!DeserializeOpLogEntry(value, entry)) {
        LOG(ERROR) << "Failed to deserialize OpLog entry from key: " << key;
        consecutive_errors_.fetch_add(1);
        return;
    }

    // Apply the OpLog entry
    if (applier_->ApplyOpLogEntry(entry)) {
        last_processed_sequence_id_.store(entry.sequence_id);
        consecutive_errors_.store(0);  // Reset error counter on success
        reconnect_count_.store(0);     // Reset reconnect counter on success
        VLOG(2) << "Applied OpLog entry: sequence_id=" << entry.sequence_id
                << ", op_type=" << static_cast<int>(entry.op_type)
                << ", key=" << entry.object_key;
    } else {
        // ApplyOpLogEntry returns false for out-of-order entries,
        // which is expected behavior, not an error
        VLOG(1) << "OpLog entry not applied (may be out of order): sequence_id="
                << entry.sequence_id;
    }
}

bool OpLogWatcher::DeserializeOpLogEntry(const std::string& json_str,
                                         OpLogEntry& entry) {
    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string errs;
    std::istringstream s(json_str);

    if (!Json::parseFromStream(reader, s, &root, &errs)) {
        LOG(ERROR) << "Failed to parse OpLogEntry JSON: " << errs;
        return false;
    }

    entry.sequence_id = root.get("sequence_id", 0).asUInt64();
    entry.timestamp_ms = root.get("timestamp_ms", 0).asUInt64();
    entry.op_type = static_cast<OpType>(root.get("op_type", 0).asInt());
    entry.object_key = root.get("object_key", "").asString();
    entry.payload = root.get("payload", "").asString();
    entry.checksum = root.get("checksum", 0).asUInt();
    entry.prefix_hash = root.get("prefix_hash", 0).asUInt();
    entry.key_sequence_id = root.get("key_sequence_id", 0).asUInt64();
    return true;
}

}  // namespace mooncake

#else  // STORE_USE_ETCD not defined

namespace mooncake {

OpLogWatcher::OpLogWatcher(const std::string& etcd_endpoints,
                           const std::string& cluster_id, OpLogApplier* applier)
    : etcd_endpoints_(etcd_endpoints), cluster_id_(cluster_id), applier_(applier) {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
}

OpLogWatcher::~OpLogWatcher() {
    Stop();
}

void OpLogWatcher::Start() {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
}

bool OpLogWatcher::StartFromSequenceId(uint64_t /*start_seq_id*/) {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
    return false;
}

void OpLogWatcher::Stop() {
    // No-op when STORE_USE_ETCD is not enabled
}

bool OpLogWatcher::ReadOpLogSince(uint64_t start_seq_id,
                                  std::vector<OpLogEntry>& entries) {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
    return false;
}

bool OpLogWatcher::ReadOpLogSinceWithRevision(uint64_t /*start_seq_id*/,
                                              std::vector<OpLogEntry>& /*entries*/,
                                              EtcdRevisionId& /*revision_id*/) {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
    return false;
}

uint64_t OpLogWatcher::GetLastProcessedSequenceId() const {
    return last_processed_sequence_id_.load();
}

void OpLogWatcher::WatchOpLog() {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
}

void OpLogWatcher::HandleWatchEvent(const std::string& key, const std::string& value,
                                    int event_type) {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
}

void OpLogWatcher::HandleWatchEvent(const std::string& key, const std::string& value,
                                    int event_type, int64_t mod_revision) {
    (void)key;
    (void)value;
    (void)event_type;
    (void)mod_revision;
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
}

void OpLogWatcher::TryReconnect() {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
}

bool OpLogWatcher::SyncMissedEntries() {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
    return false;
}

}  // namespace mooncake

#endif  // STORE_USE_ETCD

