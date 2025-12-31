#include "etcd_oplog_store.h"

#include <glog/logging.h>
#include <sstream>
#include <iomanip>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>  // Ubuntu
#else
#include <json/json.h>  // CentOS
#endif

#include "etcd_helper.h"

namespace mooncake {

EtcdOpLogStore::EtcdOpLogStore(const std::string& cluster_id,
                               bool enable_latest_seq_batch_update)
    : cluster_id_(cluster_id),
      enable_latest_seq_batch_update_(enable_latest_seq_batch_update),
      last_update_time_(std::chrono::steady_clock::now()) {
    // Start batch update thread only for writers.
    if (enable_latest_seq_batch_update_) {
        batch_update_running_.store(true);
        batch_update_thread_ =
            std::thread(&EtcdOpLogStore::BatchUpdateThread, this);
    }
}

EtcdOpLogStore::~EtcdOpLogStore() {
    if (!enable_latest_seq_batch_update_) {
        return;
    }

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

    // Update `/latest`.
    // - Writers: batch update to reduce etcd write pressure.
    // - Readers / tests: update immediately for simplicity.
    if (!enable_latest_seq_batch_update_) {
        return UpdateLatestSequenceId(entry.sequence_id);
    }

    pending_latest_seq_id_.store(entry.sequence_id);
    size_t count = pending_count_.fetch_add(1) + 1;
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
    EtcdRevisionId rev = 0;
    return ReadOpLogSinceWithRevision(start_sequence_id, limit, entries, rev);
}

ErrorCode EtcdOpLogStore::ReadOpLogSinceWithRevision(uint64_t start_sequence_id,
                                                     size_t limit,
                                                     std::vector<OpLogEntry>& entries,
                                                     EtcdRevisionId& revision_id) {
    entries.clear();
    entries.reserve(limit);

    // Range is limited to OpLog entry keys only.
    const std::string prefix = std::string(kOpLogPrefix) + cluster_id_ + "/";
    std::string current_start_key = BuildOpLogKey(start_sequence_id + 1);

    // Compute prefix range end (etcd prefix end).
    auto prefix_end = [](std::string p) -> std::string {
        for (int i = static_cast<int>(p.size()) - 1; i >= 0; --i) {
            unsigned char c = static_cast<unsigned char>(p[i]);
            if (c < 0xFF) {
                p[i] = static_cast<char>(c + 1);
                p.resize(i + 1);
                return p;
            }
        }
        return std::string(1, '\0');
    };
    const std::string end_key = prefix_end(prefix);

    // Pagination:
    // - Use range-get with limit
    // - Start next page from lastKey + '\0' (lexicographically just after lastKey)
    // This avoids repeating the last key without adding new Go/C++ APIs.
    revision_id = 0;
    while (entries.size() < limit) {
        const size_t page_limit = limit - entries.size();
        std::string json;
        EtcdRevisionId page_rev = 0;
        ErrorCode err =
            EtcdHelper::GetRangeAsJson(current_start_key.c_str(),
                                       current_start_key.size(), end_key.c_str(),
                                       end_key.size(), page_limit, json, page_rev);
        if (err != ErrorCode::OK) {
            return err;
        }
        if (page_rev > revision_id) {
            revision_id = page_rev;
        }

        // Parse kv list: [{"key":"...","value":"..."}]
        Json::Value root;
        Json::CharReaderBuilder reader;
        std::string errs;
        std::istringstream s(json);
        if (!Json::parseFromStream(reader, s, &root, &errs)) {
            LOG(ERROR) << "Failed to parse range JSON: " << errs;
            return ErrorCode::INTERNAL_ERROR;
        }
        if (!root.isArray()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        if (root.empty()) {
            break;  // no more data
        }

        std::string last_key_in_page;
        for (const auto& kv : root) {
            const std::string key = kv.get("key", "").asString();
            last_key_in_page = key;
            if (key.empty() || key.find("/latest") != std::string::npos ||
                key.find("/snapshot/") != std::string::npos) {
                continue;
            }

            // Parse seq from key suffix and filter (handles legacy keys too).
            size_t pos = key.rfind('/');
            if (pos == std::string::npos || pos + 1 >= key.size()) {
                continue;
            }
            uint64_t seq = 0;
            try {
                seq = static_cast<uint64_t>(std::stoull(key.substr(pos + 1)));
            } catch (...) {
                continue;
            }
            if (seq <= start_sequence_id) {
                continue;
            }

            OpLogEntry entry;
            const std::string value = kv.get("value", "").asString();
            if (!DeserializeOpLogEntry(value, entry)) {
                LOG(ERROR) << "Failed to deserialize OpLog entry from key=" << key;
                return ErrorCode::INTERNAL_ERROR;
            }
            entries.push_back(std::move(entry));
            if (entries.size() >= limit) {
                break;
            }
        }

        // Advance start key for next page.
        if (last_key_in_page.empty()) {
            break;
        }
        current_start_key = last_key_in_page;
        current_start_key.push_back('\0');
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
    // Robust cleanup (Scheme 3):
    // - Determine current minimum sequence_id in etcd
    // - DeleteRange [min_key, before_key)
    //
    // IMPORTANT: This relies on lexicographical ordering of keys, so the
    // sequence_id portion MUST be fixed-width (zero-padded).
    auto min_seq_opt = GetMinSequenceId();
    if (!min_seq_opt.has_value()) {
        return ErrorCode::OK;  // nothing to cleanup
    }

    uint64_t min_seq = min_seq_opt.value();
    if (before_sequence_id <= min_seq) {
        return ErrorCode::OK;
    }

    std::string start_key = BuildOpLogKey(min_seq);
    std::string end_key = BuildOpLogKey(before_sequence_id);  // delete < before_sequence_id

    return EtcdHelper::DeleteRange(start_key.c_str(), start_key.size(),
                                   end_key.c_str(), end_key.size());
}

std::string EtcdOpLogStore::BuildOpLogKey(uint64_t sequence_id) const {
    std::ostringstream oss;
    // Fixed-width encoding for correct etcd lexicographical range operations.
    // 20 digits is enough for uint64_t max (18446744073709551615).
    oss << kOpLogPrefix << cluster_id_ << "/"
        << std::setw(20) << std::setfill('0') << sequence_id;
    return oss.str();
}

std::optional<uint64_t> EtcdOpLogStore::GetMinSequenceId() const {
    std::string prefix = std::string(kOpLogPrefix) + cluster_id_ + "/";
    std::string first_key;
    ErrorCode err =
        EtcdHelper::GetFirstKeyWithPrefix(prefix.c_str(), prefix.size(), first_key);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    // Skip non-entry keys if any (e.g. "/latest" or "/snapshot/...").
    // Entries are expected to be ".../<20-digit-seq>".
    // If the first key isn't an entry key, fall back to nullopt (safe no-op).
    if (first_key.find("/latest") != std::string::npos ||
        first_key.find("/snapshot/") != std::string::npos) {
        return std::nullopt;
    }

    size_t pos = first_key.rfind('/');
    if (pos == std::string::npos || pos + 1 >= first_key.size()) {
        return std::nullopt;
    }
    std::string seq_str = first_key.substr(pos + 1);
    try {
        return static_cast<uint64_t>(std::stoull(seq_str));
    } catch (...) {
        return std::nullopt;
    }
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
    if (!enable_latest_seq_batch_update_) {
        return;
    }
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
    if (!enable_latest_seq_batch_update_) {
        return;
    }
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

