#include "oplog_watcher.h"

#include <algorithm>
#include <chrono>
#include <glog/logging.h>
#include <sstream>
#include <thread>

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "etcd_oplog_store.h"
#include "ha_metric_manager.h"
#include "oplog_applier.h"
#include "oplog_manager.h"

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif

namespace mooncake {

namespace {

// Base64 decoding for binary payload (must match encoding in
// etcd_oplog_store.cpp)
std::string Base64Decode(const std::string& encoded) {
    static const unsigned char decode_table[256] = {
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63, 52, 53, 54, 55, 56, 57,
        58, 59, 60, 61, 64, 64, 64, 64, 64, 64, 64, 0,  1,  2,  3,  4,  5,  6,
        7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 64, 64, 64, 64, 64, 64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
        37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64};

    std::string result;
    result.reserve((encoded.size() * 3) / 4);

    size_t i = 0;
    while (i < encoded.size()) {
        // Skip whitespace and invalid chars
        while (i < encoded.size() &&
               (encoded[i] == ' ' || encoded[i] == '\n' || encoded[i] == '\r' ||
                encoded[i] == '\t')) {
            i++;
        }
        if (i >= encoded.size()) break;

        uint32_t sextet_a =
            decode_table[static_cast<unsigned char>(encoded[i++])];
        if (i >= encoded.size() || sextet_a == 64) break;

        uint32_t sextet_b =
            decode_table[static_cast<unsigned char>(encoded[i++])];
        if (sextet_b == 64) break;

        uint32_t sextet_c =
            (i < encoded.size())
                ? decode_table[static_cast<unsigned char>(encoded[i++])]
                : 64;
        uint32_t sextet_d =
            (i < encoded.size())
                ? decode_table[static_cast<unsigned char>(encoded[i++])]
                : 64;

        uint32_t triple = (sextet_a << 18) | (sextet_b << 12) |
                          ((sextet_c != 64) ? (sextet_c << 6) : 0) |
                          ((sextet_d != 64) ? sextet_d : 0);

        result.push_back(static_cast<char>((triple >> 16) & 0xFF));
        if (sextet_c != 64) {
            result.push_back(static_cast<char>((triple >> 8) & 0xFF));
        }
        if (sextet_d != 64) {
            result.push_back(static_cast<char>(triple & 0xFF));
        }
    }

    return result;
}

}  // namespace

OpLogWatcher::OpLogWatcher(const std::string& etcd_endpoints,
                           const std::string& cluster_id, OpLogApplier* applier)
    : etcd_endpoints_(etcd_endpoints),
      cluster_id_(cluster_id),
      applier_(applier) {
    if (applier_ == nullptr) {
        LOG(FATAL) << "OpLogApplier cannot be null";
    }
    // Normalize cluster_id to avoid double slashes in watch prefix.
    while (!cluster_id_.empty() && cluster_id_.back() == '/') {
        cluster_id_.pop_back();
    }
    if (!cluster_id_.empty() && !IsValidClusterIdComponent(cluster_id_)) {
        LOG(FATAL)
            << "Invalid cluster_id for OpLogWatcher: '" << cluster_id_
            << "'. Allowed chars: [A-Za-z0-9_.-], max_len=128, no slashes.";
    }
}

OpLogWatcher::~OpLogWatcher() { Stop(); }

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
    uint64_t read_seq_id = start_seq_id;
    EtcdRevisionId last_read_rev = 0;
    size_t total_applied = 0;

    for (;;) {
        std::vector<OpLogEntry> batch;
        EtcdRevisionId rev = 0;
        if (!ReadOpLogSince(read_seq_id, batch, rev)) {
            last_read_rev = 0;
            break;
        }
        last_read_rev = rev;
        if (!batch.empty()) {
            for (const auto& e : batch) {
                if (applier_->ApplyOpLogEntry(e)) {
                    last_processed_sequence_id_.store(e.sequence_id);
                    read_seq_id = e.sequence_id;
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
    // Wait for watch thread to finish first. This ensures that the watch thread
    // has exited before we cancel the watch, reducing the chance of race
    // conditions.
    if (watch_thread_.joinable()) {
        watch_thread_.join();
    }

    // Now cancel the watch. This will trigger the Go goroutine to exit.
    // The watch thread has already stopped, so we won't have race conditions
    // with it trying to access the watcher object.
    std::string watch_prefix = "/oplog/" + cluster_id_ + "/";
    ErrorCode err = EtcdHelper::CancelWatchWithPrefix(watch_prefix.c_str(),
                                                      watch_prefix.size());
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to cancel watch for prefix " << watch_prefix
                     << ", error=" << static_cast<int>(err);
    }

    // Wait for Go watch goroutine to fully exit (no more callbacks).
    // This avoids callback-after-free without relying on sleeps.
    (void)EtcdHelper::WaitWatchWithPrefixStopped(watch_prefix.c_str(),
                                                 watch_prefix.size(),
                                                 /*timeout_ms=*/5000);
#endif

    LOG(INFO) << "OpLogWatcher stopped";
}

bool OpLogWatcher::ReadOpLogSince(uint64_t start_seq_id,
                                  std::vector<OpLogEntry>& entries,
                                  EtcdRevisionId& revision_id) {
#ifdef STORE_USE_ETCD
    EtcdOpLogStore oplog_store(cluster_id_,
                               /*enable_latest_seq_batch_update=*/false);
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

void OpLogWatcher::WatchCallback(void* context, const char* key,
                                 size_t key_size, const char* value,
                                 size_t value_size, int event_type,
                                 int64_t mod_revision) {
    // Use try-catch to prevent crashes if object is destroyed
    try {
        OpLogWatcher* watcher = static_cast<OpLogWatcher*>(context);
        if (watcher == nullptr) {
            // Context is null, ignore callback
            return;
        }

        // Early exit check: First, try to read running_ flag with minimal
        // object access. If object is destroyed, this access might cause
        // SIGSEGV, which will be caught by signal handler or cause immediate
        // crash (better than accessing more members). We use
        // memory_order_acquire for consistency, but if object is destroyed,
        // even this access can fail.
        //
        // Note: There's no perfect way to check if a C++ object is still valid
        // without potentially accessing invalid memory. The best we can do is:
        // 1. Check quickly and exit early if stopped
        // 2. Use try-catch for C++ exceptions (won't catch SIGSEGV)
        // 3. Ensure Stop() waits long enough for all callbacks to complete
        bool is_running = false;
        try {
            is_running = watcher->running_.load(std::memory_order_acquire);
        } catch (...) {
            // Object may be destroyed, ignore callback
            return;
        }

        if (!is_running) {
            // Watcher is being stopped, ignore callback
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
    } catch (const std::exception& e) {
        // C++ object may have been destroyed, ignore the exception
        LOG(WARNING) << "Exception in WatchCallback (likely object destroyed): "
                     << e.what();
    } catch (...) {
        // Catch all other exceptions (including access violations)
        LOG(WARNING)
            << "Unknown exception in WatchCallback (likely object destroyed)";
    }
}

void OpLogWatcher::WatchOpLog() {
#ifdef STORE_USE_ETCD
    LOG(INFO) << "OpLog watch thread started for cluster_id=" << cluster_id_;

    std::string watch_prefix = "/oplog/" + cluster_id_ + "/";

    while (running_.load()) {
        // Cancel any existing watch before starting a new one
        // This prevents "prefix already being watched" errors
        (void)EtcdHelper::CancelWatchWithPrefix(watch_prefix.c_str(),
                                                watch_prefix.size());
        (void)EtcdHelper::WaitWatchWithPrefixStopped(watch_prefix.c_str(),
                                                     watch_prefix.size(),
                                                     /*timeout_ms=*/5000);

        // Start watching - pass static callback function and this pointer as
        // context
        EtcdRevisionId start_rev =
            static_cast<EtcdRevisionId>(next_watch_revision_.load());
        // Use watcher with mod_revision so we can update next_watch_revision_
        // precisely.
        ErrorCode err = EtcdHelper::WatchWithPrefixFromRevision(
            watch_prefix.c_str(), watch_prefix.size(), start_rev, this,
            WatchCallback);

        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to start watch for prefix " << watch_prefix
                       << ", error=" << static_cast<int>(err);
            watch_healthy_.store(false);
            NotifyStateEvent(StandbyEvent::WATCH_BROKEN);

            // Wait a bit longer before retrying, to ensure old goroutines have
            // time to exit
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            // Try to reconnect
            TryReconnect();
            continue;
        }

        LOG(INFO) << "Watch started for prefix " << watch_prefix;
        watch_healthy_.store(true);
        consecutive_errors_.store(0);
        NotifyStateEvent(StandbyEvent::WATCH_HEALTHY);

        // The watch is now running in the background (via Go goroutine)
        // We just need to keep the thread alive until Stop() is called or watch
        // fails
        while (running_.load() && watch_healthy_.load()) {
            // Drive pending/missing handling even when no new watch events
            // arrive. Without this, a single out-of-order arrival could park
            // entries in pending_entries_ forever if the missing entry isn't
            // delivered via watch (but exists in etcd and could be fetched).
            (void)applier_->ProcessPendingEntries();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // Periodically check watch health
            if (consecutive_errors_.load() >= kMaxConsecutiveErrors) {
                LOG(WARNING)
                    << "Too many consecutive errors ("
                    << consecutive_errors_.load() << "), reconnecting watch...";
                watch_healthy_.store(false);
                NotifyStateEvent(StandbyEvent::MAX_ERRORS_REACHED);
                break;
            }
        }

        if (running_.load() && !watch_healthy_.load()) {
            // Cancel current watch before reconnecting
            (void)EtcdHelper::CancelWatchWithPrefix(watch_prefix.c_str(),
                                                    watch_prefix.size());
            (void)EtcdHelper::WaitWatchWithPrefixStopped(watch_prefix.c_str(),
                                                         watch_prefix.size(),
                                                         /*timeout_ms=*/5000);
            NotifyStateEvent(StandbyEvent::WATCH_BROKEN);
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
    int delay_ms =
        std::min(kReconnectDelayMs * reconnect_attempt, kMaxReconnectDelayMs);

    LOG(INFO) << "Attempting to reconnect watch (attempt #" << reconnect_attempt
              << "), waiting " << delay_ms << "ms...";

    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

    // Sync any missed entries before resuming watch
    if (SyncMissedEntries()) {
        LOG(INFO) << "Successfully synced missed OpLog entries";
        NotifyStateEvent(StandbyEvent::RECOVERY_SUCCESS);
    } else {
        LOG(WARNING)
            << "Failed to sync missed OpLog entries, continuing anyway";
        NotifyStateEvent(StandbyEvent::RECOVERY_FAILED);
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
    if (!ReadOpLogSince(last_seq, entries, rev)) {
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

void OpLogWatcher::HandleWatchEvent(const std::string& key,
                                    const std::string& value, int event_type) {
    HandleWatchEvent(key, value, event_type, /*mod_revision=*/0);
}

void OpLogWatcher::HandleWatchEvent(const std::string& key,
                                    const std::string& value, int event_type,
                                    int64_t mod_revision) {
    // event_type:
    // 0 = PUT, 1 = DELETE, 2 = WATCH_BROKEN (Go watcher terminated; should
    // reconnect)
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

    // Parse the OpLog entry from JSON
    OpLogEntry entry;
    if (!DeserializeOpLogEntry(value, entry)) {
        LOG(ERROR) << "Failed to deserialize OpLog entry from key: " << key;
        consecutive_errors_.fetch_add(1);
        return;
    }

    // Basic DoS protection: validate key/payload sizes before further
    // processing.
    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "OpLog entry size rejected: sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        consecutive_errors_.fetch_add(1);
        return;
    }

    // Verify checksum to detect data corruption or tampering.
    if (!OpLogManager::VerifyChecksum(entry)) {
        LOG(ERROR)
            << "OpLog entry checksum mismatch: sequence_id="
            << entry.sequence_id << ", key=" << entry.object_key
            << ". Possible data corruption or tampering. Discarding entry.";
        consecutive_errors_.fetch_add(1);
        HAMetricManager::instance().inc_oplog_checksum_failures();
        return;
    }

    // Apply the OpLog entry
    if (applier_->ApplyOpLogEntry(entry)) {
        // last_processed_sequence_id_ must be monotonic. We may "consume"
        // duplicate / already-applied entries (entry.sequence_id < expected) as
        // no-ops, so never regress this counter.
        uint64_t cur = last_processed_sequence_id_.load();
        while (IsSequenceNewer(entry.sequence_id, cur) &&
               !last_processed_sequence_id_.compare_exchange_weak(
                   cur, entry.sequence_id)) {
            // retry
        }
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

    // CRITICAL: Base64 decode payload to restore binary data
    std::string encoded_payload = root.get("payload", "").asString();
    entry.payload = Base64Decode(encoded_payload);

    entry.checksum = root.get("checksum", 0).asUInt();
    entry.prefix_hash = root.get("prefix_hash", 0).asUInt();
    return true;
}

}  // namespace mooncake

#else  // STORE_USE_ETCD not defined

namespace mooncake {

OpLogWatcher::OpLogWatcher(const std::string& etcd_endpoints,
                           const std::string& cluster_id, OpLogApplier* applier)
    : etcd_endpoints_(etcd_endpoints),
      cluster_id_(cluster_id),
      applier_(applier) {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
}

OpLogWatcher::~OpLogWatcher() { Stop(); }

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

bool OpLogWatcher::ReadOpLogSince(uint64_t /*start_seq_id*/,
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

void OpLogWatcher::HandleWatchEvent(const std::string& key,
                                    const std::string& value, int event_type) {
    LOG(FATAL) << "OpLogWatcher requires STORE_USE_ETCD to be enabled";
}

void OpLogWatcher::HandleWatchEvent(const std::string& key,
                                    const std::string& value, int event_type,
                                    int64_t mod_revision) {
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
