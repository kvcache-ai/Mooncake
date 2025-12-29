#include "etcd_oplog_store.h"

#include <glog/logging.h>
#include <sstream>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif

#include "etcd_helper.h"

namespace mooncake {

EtcdOpLogStore::EtcdOpLogStore(const std::string& cluster_id)
    : cluster_id_(cluster_id),
      last_update_time_(std::chrono::steady_clock::now()) {
    // Start batch update thread
    batch_update_running_.store(true);
    batch_update_thread_ = std::thread(&EtcdOpLogStore::BatchUpdateThread, this);
}

EtcdOpLogStore::~EtcdOpLogStore() {
    // Stop batch update thread
    batch_update_running_.store(false);
    if (batch_update_thread_.joinable()) {
        batch_update_thread_.join();
    }
    
    // Perform final update if there are pending updates
    if (pending_count_.load() > 0) {
        DoBatchUpdate();
    }
}

ErrorCode EtcdOpLogStore::WriteOpLog(const OpLogEntry& entry) {
    std::string key = BuildOpLogKey(entry.sequence_id);
    std::string value = SerializeOpLogEntry(entry);

    ErrorCode err = EtcdHelper::Put(key.c_str(), key.size(), value.c_str(),
                                    value.size());
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to write OpLog entry, sequence_id="
                   << entry.sequence_id;
        return err;
    }

    // Add to batch update queue instead of immediate update
    pending_latest_seq_id_.store(entry.sequence_id);
    size_t count = pending_count_.fetch_add(1) + 1;
    
    // Trigger immediate update if batch size threshold is reached
    if (count >= kBatchSize) {
        DoBatchUpdate();
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::ReadOpLog(uint64_t sequence_id,
                                     OpLogEntry& entry) {
    std::string key = BuildOpLogKey(sequence_id);
    std::string value;
    EtcdRevisionId revision_id;
    ErrorCode err = EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
    if (err != ErrorCode::OK) {
        return err;
    }

    if (!DeserializeOpLogEntry(value, entry)) {
        LOG(ERROR) << "Failed to deserialize OpLog entry, sequence_id="
                   << sequence_id;
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::ReadOpLogSince(uint64_t start_sequence_id,
                                          size_t limit,
                                          std::vector<OpLogEntry>& entries) {
    // TODO: Implement ReadOpLogSince using GetWithPrefix
    // For now, read entries one by one (inefficient but works)
    entries.clear();
    entries.reserve(limit);

    uint64_t current_seq = start_sequence_id + 1;
    for (size_t i = 0; i < limit; ++i) {
        OpLogEntry entry;
        ErrorCode err = ReadOpLog(current_seq, entry);
        if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
            // No more entries
            break;
        }
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to read OpLog entry, sequence_id="
                       << current_seq;
            return err;
        }
        entries.push_back(entry);
        current_seq++;
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::GetLatestSequenceId(uint64_t& sequence_id) {
    std::string key = BuildLatestKey();
    std::string value;
    EtcdRevisionId revision_id;
    ErrorCode err = EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
    if (err != ErrorCode::OK) {
        return err;
    }

    try {
        sequence_id = std::stoull(value);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse latest sequence_id: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::UpdateLatestSequenceId(uint64_t sequence_id) {
    std::string key = BuildLatestKey();
    std::string value = std::to_string(sequence_id);
    return EtcdHelper::Put(key.c_str(), key.size(), value.c_str(), value.size());
}

ErrorCode EtcdOpLogStore::RecordSnapshotSequenceId(
    const std::string& snapshot_id, uint64_t sequence_id) {
    std::string key = BuildSnapshotKey(snapshot_id);
    std::string value = std::to_string(sequence_id);
    return EtcdHelper::Put(key.c_str(), key.size(), value.c_str(), value.size());
}

ErrorCode EtcdOpLogStore::GetSnapshotSequenceId(
    const std::string& snapshot_id, uint64_t& sequence_id) {
    std::string key = BuildSnapshotKey(snapshot_id);
    std::string value;
    EtcdRevisionId revision_id;
    ErrorCode err = EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
    if (err != ErrorCode::OK) {
        return err;
    }

    try {
        sequence_id = std::stoull(value);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to parse snapshot sequence_id: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::CleanupOpLogBefore(uint64_t before_sequence_id) {
    // Build start and end keys for the range
    std::string start_key = BuildOpLogKey(1);  // Start from sequence_id 1
    std::string end_key = BuildOpLogKey(before_sequence_id);  // End before this

    return EtcdHelper::DeleteRange(start_key.c_str(), start_key.size(),
                                   end_key.c_str(), end_key.size());
}

std::string EtcdOpLogStore::BuildOpLogKey(uint64_t sequence_id) const {
    std::ostringstream oss;
    oss << kOpLogPrefix << cluster_id_ << "/" << sequence_id;
    return oss.str();
}

std::string EtcdOpLogStore::BuildLatestKey() const {
    std::ostringstream oss;
    oss << kOpLogPrefix << cluster_id_ << kLatestSuffix;
    return oss.str();
}

std::string EtcdOpLogStore::BuildSnapshotKey(
    const std::string& snapshot_id) const {
    std::ostringstream oss;
    oss << kOpLogPrefix << cluster_id_ << kSnapshotSuffix << snapshot_id
        << "/sequence_id";
    return oss.str();
}

std::string EtcdOpLogStore::SerializeOpLogEntry(
    const OpLogEntry& entry) const {
    Json::Value root;
    root["sequence_id"] = static_cast<Json::UInt64>(entry.sequence_id);
    root["timestamp_ms"] = static_cast<Json::UInt64>(entry.timestamp_ms);
    root["op_type"] = static_cast<int>(entry.op_type);
    root["object_key"] = entry.object_key;
    root["payload"] = entry.payload;
    root["checksum"] = static_cast<Json::UInt>(entry.checksum);
    root["prefix_hash"] = static_cast<Json::UInt>(entry.prefix_hash);
    root["key_sequence_id"] = static_cast<Json::UInt64>(entry.key_sequence_id);

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";  // Compact format
    std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
    std::ostringstream oss;
    writer->write(root, &oss);
    return oss.str();
}

bool EtcdOpLogStore::DeserializeOpLogEntry(const std::string& json_str,
                                             OpLogEntry& entry) const {
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errors;

    if (!reader->parse(json_str.data(), json_str.data() + json_str.size(),
                       &root, &errors)) {
        LOG(ERROR) << "Failed to parse JSON: " << errors;
        return false;
    }

    try {
        entry.sequence_id = root["sequence_id"].asUInt64();
        entry.timestamp_ms = root["timestamp_ms"].asUInt64();
        entry.op_type = static_cast<OpType>(root["op_type"].asInt());
        entry.object_key = root["object_key"].asString();
        entry.payload = root["payload"].asString();
        entry.checksum = root["checksum"].asUInt();
        entry.prefix_hash = root["prefix_hash"].asUInt();
        entry.key_sequence_id = root["key_sequence_id"].asUInt64();
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to deserialize OpLogEntry: " << e.what();
        return false;
    }

    return true;
}

void EtcdOpLogStore::BatchUpdateThread() {
    while (batch_update_running_.load()) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kBatchIntervalMs));
        
        // Check if we need to update based on time interval
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - last_update_time_).count();
        
        if (pending_count_.load() > 0 && elapsed >= kBatchIntervalMs) {
            DoBatchUpdate();
        }
    }
}

void EtcdOpLogStore::TriggerBatchUpdateIfNeeded() {
    // This method is kept for potential future use (e.g., manual trigger)
    // Currently, DoBatchUpdate() is called directly from WriteOpLog
    // when batch size threshold is reached
    if (pending_count_.load() >= kBatchSize) {
        DoBatchUpdate();
    }
}

void EtcdOpLogStore::DoBatchUpdate() {
    std::lock_guard<std::mutex> lock(batch_update_mutex_);
    
    // Get the pending sequence_id and reset counters
    uint64_t seq_id_to_update = pending_latest_seq_id_.load();
    size_t count = pending_count_.exchange(0);
    
    if (count == 0) {
        return;  // Nothing to update
    }
    
    // Update latest_sequence_id in etcd
    ErrorCode err = UpdateLatestSequenceId(seq_id_to_update);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to batch update latest_sequence_id="
                     << seq_id_to_update << ", error=" << err
                     << ". Will retry in next batch.";
        // Restore the count so it will be retried
        pending_count_.fetch_add(count);
    } else {
        last_update_time_ = std::chrono::steady_clock::now();
        VLOG(2) << "Batch updated latest_sequence_id=" << seq_id_to_update
                << " (count=" << count << " entries)";
    }
}

}  // namespace mooncake

