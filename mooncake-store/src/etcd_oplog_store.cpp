#include "etcd_oplog_store.h"

#include <glog/logging.h>
#include <sstream>
#include <iomanip>

#include "ha_metric_manager.h"

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
    // Normalize cluster_id to avoid accidental double slashes in etcd keys when
    // caller passes a trailing '/' (master_view_key uses trailing '/', OpLog
    // keys don't).
    while (!cluster_id_.empty() && cluster_id_.back() == '/') {
        cluster_id_.pop_back();
    }

    if (!cluster_id_.empty() && !IsValidClusterIdComponent(cluster_id_)) {
        LOG(FATAL)
            << "Invalid cluster_id for EtcdOpLogStore: '" << cluster_id_
            << "'. Allowed chars: [A-Za-z0-9_.-], max_len=128, no slashes.";
    }

    // Initialize /latest key to 0 if it doesn't exist (first startup).
    // This avoids "key not found" errors when querying the latest sequence ID.
    // Important: Only initialize if the key doesn't exist to avoid overwriting
    // existing data.
    if (!cluster_id_.empty()) {
        std::string latest_key = BuildLatestKey();
        std::string existing_value;
        EtcdRevisionId revision_id;
        ErrorCode get_err = EtcdHelper::Get(
            latest_key.c_str(), latest_key.size(), existing_value, revision_id);
        if (get_err == ErrorCode::ETCD_KEY_NOT_EXIST) {
            // Key doesn't exist, safe to initialize to 0
            std::string initial_value = "0";
            ErrorCode create_err =
                EtcdHelper::Create(latest_key.c_str(), latest_key.size(),
                                   initial_value.c_str(), initial_value.size());
            if (create_err == ErrorCode::OK) {
                LOG(INFO) << "Initialized /latest key to 0 for cluster_id="
                          << cluster_id_;
            } else if (create_err == ErrorCode::ETCD_TRANSACTION_FAIL) {
                // Race condition: another instance created it between Get and
                // Create
                LOG(INFO) << "/latest key was created by another instance for "
                             "cluster_id="
                          << cluster_id_;
            } else {
                // Other errors (e.g., etcd not connected) are logged but don't
                // fail construction The key will be created when the first
                // OpLog entry is written
                LOG(WARNING)
                    << "Failed to initialize /latest key (error=" << create_err
                    << "), will be created on first OpLog write";
            }
        } else if (get_err == ErrorCode::OK) {
            // Key already exists, do nothing - preserve existing value
            LOG(INFO) << "/latest key already exists (value=" << existing_value
                      << ") for cluster_id=" << cluster_id_;
        } else {
            // Other errors (e.g., etcd not connected) are logged but don't fail
            // construction
            LOG(WARNING) << "Failed to check /latest key existence (error="
                         << get_err
                         << "), will be created on first OpLog write";
        }
    }

    // Start batch update thread only for writers.
    if (enable_latest_seq_batch_update_) {
        batch_update_running_.store(true);
        batch_update_thread_ =
            std::thread(&EtcdOpLogStore::BatchUpdateThread, this);
    }

    // Start OpLog batch write thread
    batch_write_running_.store(true);
    batch_write_thread_ = std::thread(&EtcdOpLogStore::BatchWriteThread, this);
}

EtcdOpLogStore::~EtcdOpLogStore() {
    // Stop OpLog batch write thread
    batch_write_running_.store(false);
    cv_batch_updated_.notify_all();
    if (batch_write_thread_.joinable()) {
        batch_write_thread_.join();
    }

    // Attempt final flush
    {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        if (!pending_batch_.empty()) {
            FlushBatch();
        }
    }

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

ErrorCode EtcdOpLogStore::WriteOpLog(const OpLogEntry& entry, bool sync) {
    std::string key = BuildOpLogKey(entry.sequence_id);
    std::string value = SerializeOpLogEntry(entry);

    {
        std::unique_lock<std::mutex> lock(batch_mutex_);
        pending_batch_.push_back(
            {std::move(key), std::move(value), entry.sequence_id, sync});

        bool should_notify = false;
        if (sync) {
            // Strategy 2+: Sync writes (DELETE) trigger immediate flush
            should_notify = true;
        } else {
            // Async writes (PUT_END): trigger if threshold reached
            if (pending_batch_.size() >= kOpLogBatchCountLimit) {
                should_notify = true;
            }
        }

        if (should_notify) {
            cv_batch_updated_.notify_one();
        }

        if (sync) {
            // Wait for persistence
            uint64_t target_seq = entry.sequence_id;
            bool success = cv_sync_completed_.wait_for(
                lock, std::chrono::milliseconds(kSyncWaitTimeoutMs),
                [&] { return last_persisted_seq_id_.load() >= target_seq; });
            if (!success) {
                LOG(ERROR) << "Timeout waiting for OpLog persistence, seq="
                           << target_seq;
                return ErrorCode::ETCD_OPERATION_ERROR;
            }
        }
    }

    // Update /latest pointer logic
    // We defer this to the batch flush or just queue it up here?
    // Original logic:
    if (!enable_latest_seq_batch_update_) {
        // Direct update (may be slow, but it's what config asked for)
        // Warning: This is now done AFTER op log write, which is correct order.
        return UpdateLatestSequenceId(entry.sequence_id);
    }

    // For batch update, we update the pending counter
    pending_latest_seq_id_.store(entry.sequence_id);
    size_t count = pending_count_.fetch_add(1) + 1;
    if (count >= kBatchSize) {
        DoBatchUpdate();
    }

    return ErrorCode::OK;
}

void EtcdOpLogStore::BatchWriteThread() {
    while (batch_write_running_.load()) {
        std::unique_lock<std::mutex> lock(batch_mutex_);
        if (pending_batch_.empty()) {
            // Wait for signal or timeout (Group Commit time window)
            cv_batch_updated_.wait_for(
                lock, std::chrono::milliseconds(kOpLogBatchTimeoutMs));
        }

        if (!batch_write_running_.load() && pending_batch_.empty()) {
            break;
        }

        if (!pending_batch_.empty()) {
            FlushBatch();
        }
    }
}

void EtcdOpLogStore::FlushBatch() {
    // Note: batch_mutex_ is LOCKED when entering this function
    std::deque<BatchEntry> batch_to_write;
    batch_to_write.swap(pending_batch_);

    // Unlock to allow new appends while we perform IO
    batch_mutex_.unlock();

    if (batch_to_write.empty()) {
        // Should not happen usually
        batch_mutex_.lock();
        return;
    }

    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(batch_to_write.size());
    values.reserve(batch_to_write.size());

    uint64_t max_seq = 0;
    bool has_sync_entry = false;
    for (const auto& entry : batch_to_write) {
        keys.push_back(entry.key);
        if (entry.is_sync) {
            has_sync_entry = true;
        }
        values.push_back(entry.value);
        if (entry.sequence_id > max_seq) {
            max_seq = entry.sequence_id;
        }
    }

    // Perform Batch IO
    ErrorCode err = ErrorCode::OK;
    for (int i = 0; i <= kFlushRetryCount; ++i) {
        err = EtcdHelper::BatchCreate(keys, values);
        if (err == ErrorCode::OK) {
            break;
        }
        if (i < kFlushRetryCount) {
            LOG(WARNING) << "Failed to flush OpLog batch (attempt " << i + 1
                         << "/" << kFlushRetryCount + 1 << "), retrying...";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kFlushRetryIntervalMs));
        }
    }

    // Re-lock to update state
    batch_mutex_.lock();

    if (err == ErrorCode::OK) {
        if (max_seq > last_persisted_seq_id_.load()) {
            last_persisted_seq_id_.store(max_seq);
        }

        // Update HA metrics
        HAMetricManager::instance().inc_oplog_batch_commits();
        if (has_sync_entry) {
            HAMetricManager::instance().inc_oplog_sync_batch_commits();
        }

        if (batch_to_write.size() > 1) {
            LOG(INFO) << "HA Strategy: Group Commit flush success. batch_size="
                      << batch_to_write.size() << ", max_seq=" << max_seq;
        } else {
            VLOG(3) << "HA Strategy: Group Commit flush success. batch_size=1, "
                       "max_seq="
                    << max_seq;
            if (!has_sync_entry) {
                // Log occasionally if we are flushing single async entries
                // (inefficiency indicator)
                LOG_EVERY_N(INFO, 1000) << "Note: Frequent single-entry async "
                                           "flushes detected (sample).";
            }
        }
    } else {
        LOG(ERROR) << "Failed to flush OpLog batch, count="
                   << batch_to_write.size();
    }

    // Wake up all waiting threads (Strategy 2+: DELETE waiters)
    cv_sync_completed_.notify_all();
}

ErrorCode EtcdOpLogStore::ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) {
    std::string key = BuildOpLogKey(sequence_id);
    std::string value;
    EtcdRevisionId revision_id;
    ErrorCode err =
        EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
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

ErrorCode EtcdOpLogStore::ReadOpLogSinceWithRevision(
    uint64_t start_sequence_id, size_t limit, std::vector<OpLogEntry>& entries,
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
    // - Start next page from lastKey + '\0' (lexicographically just after
    // lastKey) This avoids repeating the last key without adding new Go/C++
    // APIs.
    revision_id = 0;
    while (entries.size() < limit) {
        const size_t page_limit = limit - entries.size();
        std::string json;
        EtcdRevisionId page_rev = 0;
        ErrorCode err = EtcdHelper::GetRangeAsJson(
            current_start_key.c_str(), current_start_key.size(),
            end_key.c_str(), end_key.size(), page_limit, json, page_rev);
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
            if (IsSequenceOlderOrEqual(seq, start_sequence_id)) {
                continue;
            }

            OpLogEntry entry;
            const std::string value = kv.get("value", "").asString();
            if (!DeserializeOpLogEntry(value, entry)) {
                LOG(ERROR) << "Failed to deserialize OpLog entry from key="
                           << key;
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
    ErrorCode err =
        EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
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

ErrorCode EtcdOpLogStore::GetMaxSequenceId(uint64_t& sequence_id) {
    auto max_seq_opt = GetMaxSequenceIdInternal();
    if (!max_seq_opt.has_value()) {
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    sequence_id = max_seq_opt.value();
    return ErrorCode::OK;
}

ErrorCode EtcdOpLogStore::UpdateLatestSequenceId(uint64_t sequence_id) {
    std::string key = BuildLatestKey();
    std::string value = std::to_string(sequence_id);
    return EtcdHelper::Put(key.c_str(), key.size(), value.c_str(),
                           value.size());
}

ErrorCode EtcdOpLogStore::RecordSnapshotSequenceId(
    const std::string& snapshot_id, uint64_t sequence_id) {
    std::string key = BuildSnapshotKey(snapshot_id);
    std::string value = std::to_string(sequence_id);
    return EtcdHelper::Put(key.c_str(), key.size(), value.c_str(),
                           value.size());
}

ErrorCode EtcdOpLogStore::GetSnapshotSequenceId(const std::string& snapshot_id,
                                                uint64_t& sequence_id) {
    std::string key = BuildSnapshotKey(snapshot_id);
    std::string value;
    EtcdRevisionId revision_id;
    ErrorCode err =
        EtcdHelper::Get(key.c_str(), key.size(), value, revision_id);
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
    std::string end_key =
        BuildOpLogKey(before_sequence_id);  // delete < before_sequence_id

    return EtcdHelper::DeleteRange(start_key.c_str(), start_key.size(),
                                   end_key.c_str(), end_key.size());
}

std::string EtcdOpLogStore::BuildOpLogKey(uint64_t sequence_id) const {
    std::ostringstream oss;
    // Fixed-width encoding for correct etcd lexicographical range operations.
    // 20 digits is enough for uint64_t max (18446744073709551615).
    oss << kOpLogPrefix << cluster_id_ << "/" << std::setw(20)
        << std::setfill('0') << sequence_id;
    return oss.str();
}

std::optional<uint64_t> EtcdOpLogStore::GetMinSequenceId() const {
    std::string prefix = std::string(kOpLogPrefix) + cluster_id_ + "/";
    std::string first_key;
    ErrorCode err = EtcdHelper::GetFirstKeyWithPrefix(prefix.c_str(),
                                                      prefix.size(), first_key);
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

std::optional<uint64_t> EtcdOpLogStore::GetMaxSequenceIdInternal() const {
    // Entry keys are fixed-width 20-digit numbers, which (in practice) start
    // with '0'. Use "/0" to avoid picking up "/latest" which is
    // lexicographically after digits.
    std::string prefix = std::string(kOpLogPrefix) + cluster_id_ + "/0";
    std::string last_key;
    ErrorCode err = EtcdHelper::GetLastKeyWithPrefix(prefix.c_str(),
                                                     prefix.size(), last_key);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    size_t pos = last_key.rfind('/');
    if (pos == std::string::npos || pos + 1 >= last_key.size()) {
        return std::nullopt;
    }
    std::string seq_str = last_key.substr(pos + 1);
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

namespace {

// Base64 encoding for binary payload
// JsonCpp treats strings as UTF-8, so we must encode binary data
std::string Base64Encode(const std::string& data) {
    static const char base64_chars[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string result;
    result.reserve(((data.size() + 2) / 3) * 4);

    size_t i = 0;
    size_t data_len = data.size();

    // Process 3 bytes at a time
    while (i + 2 < data_len) {
        uint32_t octet_a = static_cast<unsigned char>(data[i++]);
        uint32_t octet_b = static_cast<unsigned char>(data[i++]);
        uint32_t octet_c = static_cast<unsigned char>(data[i++]);

        uint32_t triple = (octet_a << 16) | (octet_b << 8) | octet_c;

        result.push_back(base64_chars[(triple >> 18) & 0x3F]);
        result.push_back(base64_chars[(triple >> 12) & 0x3F]);
        result.push_back(base64_chars[(triple >> 6) & 0x3F]);
        result.push_back(base64_chars[triple & 0x3F]);
    }

    // Handle remaining bytes
    size_t remaining = data_len - i;
    if (remaining > 0) {
        uint32_t octet_a = static_cast<unsigned char>(data[i++]);
        uint32_t octet_b =
            (remaining > 1) ? static_cast<unsigned char>(data[i++]) : 0;
        uint32_t octet_c = 0;

        uint32_t triple = (octet_a << 16) | (octet_b << 8) | octet_c;

        result.push_back(base64_chars[(triple >> 18) & 0x3F]);
        result.push_back(base64_chars[(triple >> 12) & 0x3F]);
        result.push_back((remaining > 1) ? base64_chars[(triple >> 6) & 0x3F]
                                         : '=');
        result.push_back('=');
    }

    return result;
}

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

std::string EtcdOpLogStore::SerializeOpLogEntry(const OpLogEntry& entry) const {
    Json::Value root;
    root["sequence_id"] = static_cast<Json::UInt64>(entry.sequence_id);
    root["timestamp_ms"] = static_cast<Json::UInt64>(entry.timestamp_ms);
    root["op_type"] = static_cast<int>(entry.op_type);
    root["object_key"] = entry.object_key;
    // CRITICAL: Base64 encode binary payload to prevent UTF-8 corruption in
    // JSON
    root["payload"] = Base64Encode(entry.payload);
    root["checksum"] = static_cast<Json::UInt>(entry.checksum);
    root["prefix_hash"] = static_cast<Json::UInt>(entry.prefix_hash);

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
        // CRITICAL: Base64 decode payload to restore binary data
        entry.payload = Base64Decode(root["payload"].asString());
        entry.checksum = root["checksum"].asUInt();
        entry.prefix_hash = root["prefix_hash"].asUInt();
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to deserialize OpLogEntry: " << e.what();
        return false;
    }

    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "EtcdOpLogStore: entry size rejected, sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
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
                           now - last_update_time_)
                           .count();

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
