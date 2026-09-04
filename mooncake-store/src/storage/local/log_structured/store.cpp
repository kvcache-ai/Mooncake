#include "storage/local/log_structured/store.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <map>
#include <string_view>

namespace mooncake::logstructured {
namespace {

constexpr std::string_view kSegmentPrefix = "segment-";
constexpr std::string_view kSegmentSuffix = ".log";
constexpr std::string_view kWalFile = "WAL-000001";

std::optional<uint64_t> ParseSegmentId(std::string_view name) {
    if (!name.starts_with(kSegmentPrefix) || !name.ends_with(kSegmentSuffix)) {
        return std::nullopt;
    }
    name.remove_prefix(kSegmentPrefix.size());
    name.remove_suffix(kSegmentSuffix.size());
    if (name.empty()) return std::nullopt;
    uint64_t id = 0;
    for (const char ch : name) {
        if (ch < '0' || ch > '9' ||
            id > (std::numeric_limits<uint64_t>::max() - (ch - '0')) / 10) {
            return std::nullopt;
        }
        id = id * 10 + static_cast<uint64_t>(ch - '0');
    }
    return id;
}

std::string FormatSegmentName(uint64_t segment_id) {
    std::string digits = std::to_string(segment_id);
    if (digits.size() < 20) digits.insert(0, 20 - digits.size(), '0');
    return std::string(kSegmentPrefix) + digits + std::string(kSegmentSuffix);
}

StoreError MapIndexError(IndexError error) {
    if (error == IndexError::kNotFound) return StoreError::kNotFound;
    return StoreError::kInvalidTransition;
}

}  // namespace

LogStructuredStore::LogStructuredStore(LogStructuredStoreConfig config)
    : config_(std::move(config)),
      segments_path_(config_.root_path + "/segments"),
      wal_path_(config_.root_path + "/" + std::string(kWalFile)) {}

tl::expected<std::unique_ptr<LogStructuredStore>, StoreError>
LogStructuredStore::Open(LogStructuredStoreConfig config) {
    if (config.root_path.empty() || config.max_segment_bytes == 0) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }
    auto store = std::unique_ptr<LogStructuredStore>(
        new LogStructuredStore(std::move(config)));
    auto recovered = store->Recover();
    if (!recovered) return tl::unexpected(recovered.error());
    return store;
}

std::string LogStructuredStore::SegmentPath(uint64_t segment_id) const {
    return segments_path_ + "/" + FormatSegmentName(segment_id);
}

tl::expected<void, StoreError> LogStructuredStore::Recover() {
    namespace fs = std::filesystem;
    std::error_code error;
    fs::create_directories(segments_path_, error);
    if (error) return tl::unexpected(StoreError::kIoError);

    std::map<uint64_t, std::string> ordered_segments;
    for (const auto& entry : fs::directory_iterator(segments_path_, error)) {
        if (error) return tl::unexpected(StoreError::kIoError);
        if (!entry.is_regular_file(error) || error) {
            error.clear();
            continue;
        }
        auto id = ParseSegmentId(entry.path().filename().string());
        if (id) ordered_segments.emplace(*id, entry.path().string());
    }

    std::unordered_map<uint64_t, std::vector<ScannedRecord>> scanned_segments;
    uint64_t max_sequence = 0;
    for (auto it = ordered_segments.begin(); it != ordered_segments.end();
         ++it) {
        auto scan = ScanSegment(it->second, it->first);
        if (!scan) return tl::unexpected(StoreError::kIoError);
        const bool is_last = std::next(it) == ordered_segments.end();
        if (scan->termination == ScanTermination::kCorruptRecord ||
            (scan->termination == ScanTermination::kIncompleteTail &&
             !is_last)) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        if (scan->termination == ScanTermination::kIncompleteTail) {
            auto truncated = TruncateSegment(it->second, scan->valid_bytes);
            if (!truncated) return tl::unexpected(StoreError::kIoError);
        }
        for (const auto& record : scan->records) {
            max_sequence = std::max(max_sequence, record.sequence);
        }
        scanned_segments.emplace(it->first, std::move(scan->records));
        segment_paths_.emplace(it->first, it->second);
        next_segment_id_ = std::max(next_segment_id_, it->first + 1);
    }

    if (fs::exists(wal_path_, error)) {
        if (error) return tl::unexpected(StoreError::kIoError);
        auto scan = ScanWal(wal_path_);
        if (!scan) return tl::unexpected(StoreError::kIoError);
        if (scan->termination == WalScanTermination::kCorruptRecord) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        auto replayed = ReplayWal(scan->records, index_);
        if (!replayed) return tl::unexpected(StoreError::kCorruptData);
        for (const auto& record : scan->records) {
            max_sequence = std::max(max_sequence, record.sequence);
        }
        auto opened = WalWriter::OpenForAppend(wal_path_, scan->valid_bytes);
        if (!opened) return tl::unexpected(StoreError::kIoError);
        wal_ = std::move(opened.value());
    } else {
        if (error) return tl::unexpected(StoreError::kIoError);
        auto created = WalWriter::Create(wal_path_);
        if (!created) return tl::unexpected(StoreError::kIoError);
        wal_ = std::move(created.value());
    }

    for (const auto& snapshot : index_.Snapshot()) {
        if (snapshot.version.physical.total_length == 0) continue;
        auto segment =
            scanned_segments.find(snapshot.version.physical.segment_id);
        if (segment == scanned_segments.end()) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        const auto record = std::find_if(
            segment->second.begin(), segment->second.end(),
            [&](const ScannedRecord& candidate) {
                return candidate.identity == snapshot.identity &&
                       candidate.sequence == snapshot.version.sequence &&
                       candidate.physical == snapshot.version.physical;
            });
        if (record == segment->second.end()) {
            return tl::unexpected(StoreError::kCorruptData);
        }
    }

    next_sequence_ = max_sequence + 1;
    if (ordered_segments.empty()) {
        const uint64_t segment_id = next_segment_id_++;
        const std::string path = SegmentPath(segment_id);
        auto created = SegmentWriter::Create(path, segment_id);
        if (!created) return tl::unexpected(StoreError::kIoError);
        segment_paths_.emplace(segment_id, path);
        active_segment_ = std::move(created.value());
    } else {
        const auto& [segment_id, path] = *ordered_segments.rbegin();
        auto scan = ScanSegment(path, segment_id);
        if (!scan) return tl::unexpected(StoreError::kIoError);
        auto opened =
            SegmentWriter::OpenForAppend(path, segment_id, scan->valid_bytes);
        if (!opened) return tl::unexpected(StoreError::kIoError);
        active_segment_ = std::move(opened.value());
    }
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::RotateSegmentIfNeeded(
    uint64_t next_record_bytes) {
    if (active_segment_->tail() == 0 ||
        active_segment_->tail() + next_record_bytes <=
            config_.max_segment_bytes) {
        return {};
    }
    auto synced = active_segment_->Sync();
    if (!synced) return tl::unexpected(StoreError::kIoError);
    const uint64_t segment_id = next_segment_id_++;
    const std::string path = SegmentPath(segment_id);
    auto created = SegmentWriter::Create(path, segment_id);
    if (!created) return tl::unexpected(StoreError::kIoError);
    segment_paths_.emplace(segment_id, path);
    active_segment_ = std::move(created.value());
    return {};
}

tl::expected<PreparedWrite, StoreError> LogStructuredStore::PreparePut(
    const RecordIdentity& identity, std::string_view value) {
    std::lock_guard lock(mutex_);
    const uint64_t record_bytes = AlignedRecordSize(
        identity.tenant_id.size(), identity.object_key.size(), value.size());
    if (record_bytes == 0) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }
    auto rotated = RotateSegmentIfNeeded(record_bytes);
    if (!rotated) return tl::unexpected(rotated.error());

    const uint64_t sequence = next_sequence_++;
    auto appended = active_segment_->Append(identity, value, RecordKind::kValue,
                                            sequence, config_.sync_data);
    if (!appended) return tl::unexpected(StoreError::kIoError);
    WalRecord transition{.type = WalRecordType::kPrepareValue,
                         .sequence = sequence,
                         .identity = identity,
                         .physical = appended.value()};
    auto wal_result = wal_->Append(transition, config_.sync_wal);
    if (!wal_result) return tl::unexpected(StoreError::kIoError);
    auto prepared = index_.Prepare(identity, appended.value(), sequence);
    if (!prepared) return tl::unexpected(MapIndexError(prepared.error()));
    return PreparedWrite{.identity = identity,
                         .sequence = sequence,
                         .physical = appended.value()};
}

tl::expected<void, StoreError> LogStructuredStore::CommitPut(
    const RecordIdentity& identity, uint64_t sequence) {
    std::lock_guard lock(mutex_);
    auto current = index_.Lookup(identity);
    if (!current) return tl::unexpected(StoreError::kNotFound);
    if (current->state == VersionState::kCommitted &&
        current->sequence == sequence) {
        return {};
    }
    if (current->state != VersionState::kPrepared ||
        current->sequence != sequence) {
        return tl::unexpected(StoreError::kInvalidTransition);
    }
    auto persisted = wal_->Append(WalRecord{.type = WalRecordType::kCommitValue,
                                            .sequence = sequence,
                                            .identity = identity,
                                            .physical = {}},
                                  config_.sync_wal);
    if (!persisted) return tl::unexpected(StoreError::kIoError);
    auto committed = index_.Commit(identity, sequence);
    if (!committed) return tl::unexpected(MapIndexError(committed.error()));
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::AbortPut(
    const RecordIdentity& identity, uint64_t sequence) {
    std::lock_guard lock(mutex_);
    auto current = index_.Lookup(identity);
    if (!current) return tl::unexpected(StoreError::kNotFound);
    if (current->state == VersionState::kAborted &&
        current->sequence == sequence) {
        return {};
    }
    if (current->state != VersionState::kPrepared ||
        current->sequence != sequence) {
        return tl::unexpected(StoreError::kInvalidTransition);
    }
    auto persisted = wal_->Append(WalRecord{.type = WalRecordType::kAbortValue,
                                            .sequence = sequence,
                                            .identity = identity,
                                            .physical = {}},
                                  config_.sync_wal);
    if (!persisted) return tl::unexpected(StoreError::kIoError);
    auto aborted = index_.Abort(identity, sequence);
    if (!aborted) return tl::unexpected(MapIndexError(aborted.error()));
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::Delete(
    const RecordIdentity& identity) {
    std::lock_guard lock(mutex_);
    const uint64_t record_bytes = AlignedRecordSize(
        identity.tenant_id.size(), identity.object_key.size(), 0);
    auto rotated = RotateSegmentIfNeeded(record_bytes);
    if (!rotated) return tl::unexpected(rotated.error());
    const uint64_t sequence = next_sequence_++;
    auto appended = active_segment_->Append(
        identity, "", RecordKind::kTombstone, sequence, config_.sync_data);
    if (!appended) return tl::unexpected(StoreError::kIoError);
    auto persisted =
        wal_->Append(WalRecord{.type = WalRecordType::kApplyTombstone,
                               .sequence = sequence,
                               .identity = identity,
                               .physical = {}},
                     config_.sync_wal);
    if (!persisted) return tl::unexpected(StoreError::kIoError);
    auto tombstoned = index_.ApplyTombstone(identity, sequence);
    if (!tombstoned) {
        return tl::unexpected(MapIndexError(tombstoned.error()));
    }
    return {};
}

tl::expected<std::string, StoreError> LogStructuredStore::Get(
    const RecordIdentity& identity) const {
    std::lock_guard lock(mutex_);
    auto entry = index_.LookupCommitted(identity);
    if (!entry) return tl::unexpected(StoreError::kNotFound);
    auto path = segment_paths_.find(entry->physical.segment_id);
    if (path == segment_paths_.end()) {
        return tl::unexpected(StoreError::kCorruptData);
    }
    auto record = ReadRecord(path->second, entry->physical);
    if (!record || record->identity != identity ||
        record->kind == RecordKind::kTombstone) {
        return tl::unexpected(StoreError::kCorruptData);
    }
    return std::move(record->value);
}

std::vector<IndexSnapshotEntry> LogStructuredStore::SnapshotIndex() const {
    std::lock_guard lock(mutex_);
    return index_.Snapshot();
}

uint64_t LogStructuredStore::active_segment_id() const {
    std::lock_guard lock(mutex_);
    return active_segment_->segment_id();
}

uint64_t LogStructuredStore::next_sequence() const {
    std::lock_guard lock(mutex_);
    return next_sequence_;
}

}  // namespace mooncake::logstructured
