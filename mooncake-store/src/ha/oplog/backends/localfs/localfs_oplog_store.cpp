#include "ha/oplog/backends/localfs/localfs_oplog_store.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <utility>

#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include "ha/oplog/oplog_codec.h"
#include "types.h"

namespace fs = std::filesystem;

namespace mooncake {
namespace ha {
namespace backends {
namespace localfs {

LocalFsOpLogStore::LocalFsOpLogStore(std::string root_dir,
                                     ClusterNamespace cluster_namespace,
                                     bool enable_batch_write)
    : root_dir_(std::move(root_dir)),
      cluster_namespace_(std::move(cluster_namespace)),
      enable_batch_write_(enable_batch_write) {
    while (!cluster_namespace_.empty() && cluster_namespace_.back() == '/') {
        cluster_namespace_.pop_back();
    }

    if (root_dir_.empty() || cluster_namespace_.empty() ||
        !IsValidClusterIdComponent(cluster_namespace_)) {
        LOG(FATAL) << "Invalid LocalFs OpLog backend config, root_dir="
                   << root_dir_ << ", cluster_namespace=" << cluster_namespace_;
    }
}

LocalFsOpLogStore::~LocalFsOpLogStore() {
    if (enable_batch_write_) {
        batch_write_running_.store(false, std::memory_order_release);
        cv_batch_updated_.notify_all();
        if (batch_write_thread_.joinable()) {
            batch_write_thread_.join();
        }
        (void)FlushPendingBatch();
    }
    cv_sync_completed_.notify_all();
}

ErrorCode LocalFsOpLogStore::Init() {
    try {
        fs::create_directories(SegmentsDir());
    } catch (const fs::filesystem_error& error) {
        LOG(ERROR) << "Failed to create LocalFs OpLog directory, path="
                   << SegmentsDir() << ", error=" << error.what();
        return ErrorCode::INTERNAL_ERROR;
    }

    CleanupTempFiles();

    if (!enable_batch_write_) {
        return ErrorCode::OK;
    }

    if (!fs::exists(LatestFilePath())) {
        auto err = AtomicWriteFile(LatestFilePath(), std::string("0"));
        if (err != ErrorCode::OK) {
            return err;
        }
    }

    auto recover_err = RecoverPersistedState();
    if (recover_err != ErrorCode::OK) {
        return recover_err;
    }

    batch_write_running_.store(true, std::memory_order_release);
    batch_write_thread_ = std::thread(&LocalFsOpLogStore::BatchWriteLoop, this);
    return ErrorCode::OK;
}

std::string LocalFsOpLogStore::ClusterDir() const {
    return root_dir_ + "/" + cluster_namespace_;
}

std::string LocalFsOpLogStore::SegmentsDir() const {
    return ClusterDir() + "/segments";
}

std::string LocalFsOpLogStore::LatestFilePath() const {
    return ClusterDir() + "/latest";
}

std::string LocalFsOpLogStore::BuildSegmentFilename(
    OpLogSequenceId min_seq, OpLogSequenceId max_seq) const {
    char buffer[128];
    std::snprintf(buffer, sizeof(buffer), "seg_%020llu_%020llu",
                  static_cast<unsigned long long>(min_seq),
                  static_cast<unsigned long long>(max_seq));
    return SegmentsDir() + "/" + buffer;
}

bool LocalFsOpLogStore::ParseSegmentFilename(const std::string& filename,
                                             OpLogSequenceId& min_seq,
                                             OpLogSequenceId& max_seq) {
    if (filename.size() != 45 || filename.rfind("seg_", 0) != 0 ||
        filename[24] != '_') {
        return false;
    }

    try {
        min_seq =
            static_cast<OpLogSequenceId>(std::stoull(filename.substr(4, 20)));
        max_seq =
            static_cast<OpLogSequenceId>(std::stoull(filename.substr(25, 20)));
    } catch (...) {
        return false;
    }
    return true;
}

void LocalFsOpLogStore::CleanupTempFiles() const {
    try {
        for (const auto& dir : {ClusterDir(), SegmentsDir()}) {
            if (!fs::exists(dir)) {
                continue;
            }
            for (const auto& entry : fs::directory_iterator(dir)) {
                if (entry.path().extension() == ".tmp") {
                    fs::remove(entry.path());
                }
            }
        }
    } catch (const fs::filesystem_error& error) {
        LOG(WARNING) << "Failed to cleanup LocalFs OpLog temp files, error="
                     << error.what();
    }
}

ErrorCode LocalFsOpLogStore::AtomicWriteFile(const std::string& target_path,
                                             const std::string& content) const {
    return AtomicWriteFile(target_path, content.data(), content.size());
}

ErrorCode LocalFsOpLogStore::AtomicWriteFile(const std::string& target_path,
                                             const void* data,
                                             size_t size) const {
    const auto tmp_path = target_path + ".tmp";
    const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG(ERROR) << "Failed to open temp file for LocalFs OpLog, path="
                   << tmp_path << ", errno=" << errno;
        return ErrorCode::INTERNAL_ERROR;
    }

    const char* cursor = static_cast<const char*>(data);
    size_t remaining = size;
    while (remaining > 0) {
        const ssize_t written = ::write(fd, cursor, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            LOG(ERROR) << "Failed to write LocalFs OpLog file, path="
                       << tmp_path << ", errno=" << errno;
            ::close(fd);
            ::unlink(tmp_path.c_str());
            return ErrorCode::INTERNAL_ERROR;
        }
        cursor += written;
        remaining -= static_cast<size_t>(written);
    }

    if (::fsync(fd) != 0) {
        LOG(ERROR) << "Failed to fsync LocalFs OpLog file, path=" << tmp_path
                   << ", errno=" << errno;
        ::close(fd);
        ::unlink(tmp_path.c_str());
        return ErrorCode::INTERNAL_ERROR;
    }
    ::close(fd);

    if (::rename(tmp_path.c_str(), target_path.c_str()) != 0) {
        LOG(ERROR) << "Failed to rename LocalFs OpLog file, from=" << tmp_path
                   << ", to=" << target_path << ", errno=" << errno;
        ::unlink(tmp_path.c_str());
        return ErrorCode::INTERNAL_ERROR;
    }

    const auto parent_dir = fs::path(target_path).parent_path().string();
    const int dir_fd = ::open(parent_dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd >= 0) {
        (void)::fsync(dir_fd);
        ::close(dir_fd);
    }
    return ErrorCode::OK;
}

ErrorCode LocalFsOpLogStore::ReadUint64FromFile(const std::string& filepath,
                                                OpLogSequenceId& value) const {
    std::ifstream file(filepath);
    if (!file) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    std::string content;
    file >> content;
    try {
        value = static_cast<OpLogSequenceId>(std::stoull(content));
    } catch (...) {
        LOG(ERROR) << "Invalid LocalFs OpLog numeric file, path=" << filepath
                   << ", content=" << content;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

std::vector<LocalFsOpLogStore::SegmentInfo> LocalFsOpLogStore::ListSegments()
    const {
    std::vector<SegmentInfo> segments;
    if (!fs::exists(SegmentsDir())) {
        return segments;
    }

    try {
        for (const auto& entry : fs::directory_iterator(SegmentsDir())) {
            if (!entry.is_regular_file() ||
                entry.path().extension() == ".tmp") {
                continue;
            }
            SegmentInfo info;
            info.filename = entry.path().filename().string();
            if (ParseSegmentFilename(info.filename, info.min_seq,
                                     info.max_seq)) {
                segments.push_back(std::move(info));
            }
        }
    } catch (const fs::filesystem_error& error) {
        LOG(WARNING) << "Failed to list LocalFs OpLog segments, error="
                     << error.what();
    }

    std::sort(segments.begin(), segments.end(),
              [](const SegmentInfo& lhs, const SegmentInfo& rhs) {
                  return lhs.min_seq < rhs.min_seq;
              });
    return segments;
}

ErrorCode LocalFsOpLogStore::NormalizePendingEntries(
    std::vector<PendingEntry>& entries) const {
    std::sort(entries.begin(), entries.end(),
              [](const PendingEntry& lhs, const PendingEntry& rhs) {
                  return lhs.sequence_id < rhs.sequence_id;
              });

    std::vector<PendingEntry> normalized;
    normalized.reserve(entries.size());
    for (auto& entry : entries) {
        if (normalized.empty() ||
            normalized.back().sequence_id != entry.sequence_id) {
            normalized.push_back(std::move(entry));
            continue;
        }

        if (normalized.back().payload != entry.payload) {
            LOG(ERROR) << "Detected LocalFs OpLog fencing violation in "
                          "pending batch, sequence_id="
                       << entry.sequence_id;
            return ErrorCode::PERSISTENT_FAIL;
        }
    }

    entries = std::move(normalized);
    return ErrorCode::OK;
}

ErrorCode LocalFsOpLogStore::WriteSegmentFile(
    const std::vector<PendingEntry>& entries) const {
    if (entries.empty()) {
        return ErrorCode::OK;
    }

    const auto min_seq = entries.front().sequence_id;
    const auto max_seq = entries.back().sequence_id;

    SegmentHeader header{};
    std::memcpy(header.magic, kSegmentMagic, sizeof(kSegmentMagic));
    header.version = kSegmentVersion;
    header.min_seq = min_seq;
    header.max_seq = max_seq;
    header.entry_count = static_cast<uint32_t>(entries.size());

    std::string content;
    size_t estimated_size = kSegmentHeaderSize;
    for (const auto& entry : entries) {
        estimated_size += sizeof(uint32_t) + entry.payload.size();
    }
    content.reserve(estimated_size);
    content.append(reinterpret_cast<const char*>(&header), sizeof(header));

    for (const auto& entry : entries) {
        const auto payload_size = static_cast<uint32_t>(entry.payload.size());
        content.append(reinterpret_cast<const char*>(&payload_size),
                       sizeof(payload_size));
        content.append(entry.payload);
    }

    return AtomicWriteFile(BuildSegmentFilename(min_seq, max_seq), content);
}

ErrorCode LocalFsOpLogStore::ReadSegmentRecords(
    const std::string& filepath, std::vector<OpLogRecord>& records) const {
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        LOG(ERROR) << "Failed to open LocalFs OpLog segment, path=" << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    SegmentHeader header{};
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!file || static_cast<size_t>(file.gcount()) != sizeof(header)) {
        LOG(ERROR) << "Failed to read LocalFs OpLog segment header, path="
                   << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    if (std::memcmp(header.magic, kSegmentMagic, sizeof(kSegmentMagic)) != 0 ||
        header.version != kSegmentVersion) {
        LOG(ERROR) << "Invalid LocalFs OpLog segment header, path=" << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    records.reserve(records.size() + header.entry_count);
    for (uint32_t index = 0; index < header.entry_count; ++index) {
        uint32_t payload_size = 0;
        file.read(reinterpret_cast<char*>(&payload_size), sizeof(payload_size));
        if (!file) {
            LOG(ERROR) << "Failed to read LocalFs OpLog entry length, path="
                       << filepath << ", index=" << index;
            return ErrorCode::INTERNAL_ERROR;
        }

        std::string payload(payload_size, '\0');
        file.read(payload.data(), payload_size);
        if (!file) {
            LOG(ERROR) << "Failed to read LocalFs OpLog entry payload, path="
                       << filepath << ", index=" << index;
            return ErrorCode::INTERNAL_ERROR;
        }

        auto decoded = oplog::DeserializeEntryPayload(payload);
        if (!decoded) {
            LOG(ERROR) << "Failed to decode LocalFs OpLog entry, path="
                       << filepath << ", index=" << index
                       << ", error=" << toString(decoded.error());
            return ErrorCode::INTERNAL_ERROR;
        }

        records.push_back(OpLogRecord{
            .seq = decoded->sequence_id,
            .producer_view_version = 0,
            .payload = std::move(payload),
        });
    }

    return ErrorCode::OK;
}

ErrorCode LocalFsOpLogStore::ReadPayloadForSequence(
    OpLogSequenceId sequence_id, std::string& payload) const {
    const auto segments = ListSegments();
    const auto segment_it =
        std::lower_bound(segments.begin(), segments.end(), sequence_id,
                         [](const SegmentInfo& segment, OpLogSequenceId seq) {
                             return segment.max_seq < seq;
                         });
    if (segment_it == segments.end() || sequence_id < segment_it->min_seq ||
        sequence_id > segment_it->max_seq) {
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    std::vector<OpLogRecord> records;
    auto err =
        ReadSegmentRecords(SegmentsDir() + "/" + segment_it->filename, records);
    if (err != ErrorCode::OK) {
        return err;
    }

    for (auto& record : records) {
        if (record.seq == sequence_id) {
            payload = std::move(record.payload);
            return ErrorCode::OK;
        }
    }
    return ErrorCode::OBJECT_NOT_FOUND;
}

ErrorCode LocalFsOpLogStore::VerifyPersistedPayloadMatches(
    OpLogSequenceId sequence_id, const std::string& payload) const {
    std::string persisted_payload;
    const auto err = ReadPayloadForSequence(sequence_id, persisted_payload);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (persisted_payload != payload) {
        LOG(ERROR) << "Detected LocalFs OpLog fencing violation, sequence_id="
                   << sequence_id;
        return ErrorCode::PERSISTENT_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode LocalFsOpLogStore::RecoverPersistedState() {
    const auto segments = ListSegments();
    if (segments.empty()) {
        last_persisted_seq_id_.store(0, std::memory_order_release);
        return AtomicWriteFile(LatestFilePath(), std::string("0"));
    }

    const auto max_seq = segments.back().max_seq;
    last_persisted_seq_id_.store(max_seq, std::memory_order_release);
    return AtomicWriteFile(LatestFilePath(), std::to_string(max_seq));
}

void LocalFsOpLogStore::BatchWriteLoop() {
    while (batch_write_running_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lock(batch_mutex_);
            if (pending_batch_.empty()) {
                cv_batch_updated_.wait_for(lock, kBatchTimeout);
            }
            if (!batch_write_running_.load(std::memory_order_acquire) &&
                pending_batch_.empty()) {
                break;
            }
        }
        (void)FlushPendingBatch();
    }
}

ErrorCode LocalFsOpLogStore::FlushPendingBatch() {
    std::deque<PendingEntry> pending_entries;
    {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        if (pending_batch_.empty()) {
            return ErrorCode::OK;
        }
        pending_entries.swap(pending_batch_);
    }

    std::vector<PendingEntry> entries(
        std::make_move_iterator(pending_entries.begin()),
        std::make_move_iterator(pending_entries.end()));
    auto normalize_err = NormalizePendingEntries(entries);
    if (normalize_err != ErrorCode::OK) {
        cv_sync_completed_.notify_all();
        return normalize_err;
    }

    ErrorCode write_err = ErrorCode::INTERNAL_ERROR;
    for (int retry = 0; retry < kFlushRetryCount; ++retry) {
        write_err = WriteSegmentFile(entries);
        if (write_err == ErrorCode::OK) {
            break;
        }
        if (retry + 1 < kFlushRetryCount) {
            std::this_thread::sleep_for(kFlushRetryInterval);
        }
    }

    if (write_err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to flush LocalFs OpLog batch, first_seq="
                   << entries.front().sequence_id
                   << ", last_seq=" << entries.back().sequence_id
                   << ", error=" << toString(write_err);
        cv_sync_completed_.notify_all();
        return write_err;
    }

    const auto max_seq = entries.back().sequence_id;
    last_persisted_seq_id_.store(max_seq, std::memory_order_release);
    auto latest_err =
        AtomicWriteFile(LatestFilePath(), std::to_string(max_seq));
    if (latest_err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to update LocalFs OpLog latest sequence, seq="
                   << max_seq;
    }

    cv_sync_completed_.notify_all();
    return latest_err;
}

tl::expected<OpLogSequenceId, ErrorCode> LocalFsOpLogStore::Append(
    const OpLogAppendRequest& request) {
    if (!enable_batch_write_) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!batch_write_running_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (request.expected_next_seq == 0 || request.payload.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto decoded = oplog::DeserializeEntryPayload(request.payload);
    if (!decoded) {
        return tl::make_unexpected(decoded.error());
    }
    if (decoded->sequence_id != request.expected_next_seq) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (last_persisted_seq_id_.load(std::memory_order_acquire) >=
        request.expected_next_seq) {
        const auto verify_err = VerifyPersistedPayloadMatches(
            request.expected_next_seq, request.payload);
        if (verify_err == ErrorCode::OK) {
            return request.expected_next_seq;
        }
        if (verify_err != ErrorCode::OBJECT_NOT_FOUND) {
            return tl::make_unexpected(verify_err);
        }
    }

    {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        pending_batch_.push_back(PendingEntry{
            .payload = request.payload,
            .sequence_id = request.expected_next_seq,
        });
    }
    cv_batch_updated_.notify_one();

    std::unique_lock<std::mutex> lock(batch_mutex_);
    const bool flushed =
        cv_sync_completed_.wait_for(lock, kSyncWaitTimeout, [&]() {
            if (last_persisted_seq_id_.load(std::memory_order_acquire) <
                request.expected_next_seq) {
                return false;
            }
            return VerifyPersistedPayloadMatches(request.expected_next_seq,
                                                 request.payload) ==
                   ErrorCode::OK;
        });
    if (!flushed) {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    return request.expected_next_seq;
}

tl::expected<OpLogPollResult, ErrorCode> LocalFsOpLogStore::PollFrom(
    OpLogSequenceId start_seq, size_t max_records,
    std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    OpLogPollResult result;
    result.next_seq = start_seq + 1;

    for (;;) {
        result.records.clear();
        const auto segments = ListSegments();
        auto segment_it = std::lower_bound(
            segments.begin(), segments.end(), start_seq,
            [](const SegmentInfo& segment, OpLogSequenceId seq) {
                return segment.max_seq <= seq;
            });

        for (; segment_it != segments.end() &&
               result.records.size() < max_records;
             ++segment_it) {
            std::vector<OpLogRecord> records;
            const auto err = ReadSegmentRecords(
                SegmentsDir() + "/" + segment_it->filename, records);
            if (err != ErrorCode::OK) {
                return tl::make_unexpected(err);
            }

            for (auto& record : records) {
                if (record.seq <= start_seq) {
                    continue;
                }
                result.records.push_back(std::move(record));
                if (result.records.size() >= max_records) {
                    break;
                }
            }
        }

        if (!result.records.empty()) {
            result.next_seq = result.records.back().seq + 1;
            result.timed_out = false;
            return result;
        }

        if (timeout.count() == 0 ||
            std::chrono::steady_clock::now() >= deadline) {
            result.timed_out = true;
            return result;
        }
        std::this_thread::sleep_for(kPollRetrySleep);
    }
}

tl::expected<OpLogSequenceId, ErrorCode>
LocalFsOpLogStore::GetLatestSequence() {
    const auto segments = ListSegments();
    OpLogSequenceId segment_latest = 0;
    if (!segments.empty()) {
        segment_latest = segments.back().max_seq;
    }

    OpLogSequenceId latest_seq = 0;
    const auto latest_err = ReadUint64FromFile(LatestFilePath(), latest_seq);
    if (latest_err == ErrorCode::OK) {
        return std::max(latest_seq, segment_latest);
    }
    if (latest_err == ErrorCode::OBJECT_NOT_FOUND) {
        return segment_latest;
    }
    return tl::make_unexpected(latest_err);
}

ErrorCode LocalFsOpLogStore::CleanupBefore(OpLogSequenceId before_sequence_id) {
    if (before_sequence_id == 0) {
        return ErrorCode::OK;
    }

    const auto segments = ListSegments();
    for (const auto& segment : segments) {
        if (segment.max_seq >= before_sequence_id) {
            break;
        }
        try {
            fs::remove(SegmentsDir() + "/" + segment.filename);
        } catch (const fs::filesystem_error& error) {
            LOG(WARNING) << "Failed to cleanup LocalFs OpLog segment, file="
                         << segment.filename << ", error=" << error.what();
        }
    }
    return ErrorCode::OK;
}

}  // namespace localfs
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
