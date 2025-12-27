#include "oplog_watcher.h"

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
    if (running_.load()) {
        LOG(WARNING) << "OpLogWatcher is already running";
        return;
    }

    running_.store(true);
    watch_thread_ = std::thread(&OpLogWatcher::WatchOpLog, this);
    LOG(INFO) << "OpLogWatcher started for cluster_id=" << cluster_id_;
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
    EtcdOpLogStore oplog_store(cluster_id_);
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

    std::string key_str(key, key_size);
    std::string value_str;
    if (value != nullptr && value_size > 0) {
        value_str = std::string(value, value_size);
    }

    watcher->HandleWatchEvent(key_str, value_str, event_type);
}

void OpLogWatcher::WatchOpLog() {
#ifdef STORE_USE_ETCD
    LOG(INFO) << "OpLog watch thread started for cluster_id=" << cluster_id_;

    std::string watch_prefix = "/oplog/" + cluster_id_ + "/";

    // Start watching - pass static callback function and this pointer as context
    ErrorCode err = EtcdHelper::WatchWithPrefix(
        watch_prefix.c_str(), watch_prefix.size(), this, WatchCallback);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to start watch for prefix " << watch_prefix
                   << ", error=" << static_cast<int>(err);
        running_.store(false);
        return;
    }

    LOG(INFO) << "Watch started for prefix " << watch_prefix;

    // The watch is now running in the background (via Go goroutine)
    // We just need to keep the thread alive until Stop() is called
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG(INFO) << "OpLog watch thread stopped";
#else
    LOG(ERROR) << "STORE_USE_ETCD is not enabled, cannot watch OpLog from etcd";
    running_.store(false);
#endif
}

void OpLogWatcher::HandleWatchEvent(const std::string& key, const std::string& value,
                                    int event_type) {
    // event_type: 0 = PUT, 1 = DELETE
    if (event_type == 1) {
        // DELETE event - OpLog entry was cleaned up
        VLOG(1) << "OpLog entry deleted: " << key;
        return;
    }

    if (event_type != 0) {
        LOG(WARNING) << "Unknown event type: " << event_type << " for key: " << key;
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
        return;
    }

    // Apply the OpLog entry
    if (applier_->ApplyOpLogEntry(entry)) {
        last_processed_sequence_id_.store(entry.sequence_id);
        VLOG(2) << "Applied OpLog entry: sequence_id=" << entry.sequence_id
                << ", op_type=" << static_cast<int>(entry.op_type)
                << ", key=" << entry.object_key;
    } else {
        LOG(WARNING) << "Failed to apply OpLog entry: sequence_id="
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

void OpLogWatcher::Stop() {
    // No-op when STORE_USE_ETCD is not enabled
}

bool OpLogWatcher::ReadOpLogSince(uint64_t start_seq_id,
                                  std::vector<OpLogEntry>& entries) {
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

}  // namespace mooncake

#endif  // STORE_USE_ETCD

