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
constexpr std::string_view kInitialWalFile = "WAL-00000000000000000001";

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

std::string FormatNumberedName(std::string_view prefix, uint64_t number,
                               std::string_view suffix = {}) {
    std::string digits = std::to_string(number);
    if (digits.size() < 20) digits.insert(0, 20 - digits.size(), '0');
    return std::string(prefix) + digits + std::string(suffix);
}

std::string FormatSegmentName(uint64_t segment_id) {
    return FormatNumberedName(kSegmentPrefix, segment_id, kSegmentSuffix);
}

std::optional<uint64_t> ParseNumberedName(std::string_view name,
                                          std::string_view prefix) {
    if (!name.starts_with(prefix)) return std::nullopt;
    name.remove_prefix(prefix.size());
    if (name.empty()) return std::nullopt;
    uint64_t value = 0;
    for (const char ch : name) {
        if (ch < '0' || ch > '9' ||
            value > (std::numeric_limits<uint64_t>::max() - (ch - '0')) / 10) {
            return std::nullopt;
        }
        value = value * 10 + static_cast<uint64_t>(ch - '0');
    }
    return value;
}

StoreError MapIndexError(IndexError error) {
    if (error == IndexError::kNotFound) return StoreError::kNotFound;
    return StoreError::kInvalidTransition;
}

}  // namespace

LogStructuredStore::LogStructuredStore(LogStructuredStoreConfig config)
    : config_(std::move(config)),
      segments_path_(config_.root_path + "/segments"),
      wal_path_(config_.root_path + "/" + std::string(kInitialWalFile)) {}

tl::expected<std::unique_ptr<LogStructuredStore>, StoreError>
LogStructuredStore::Open(LogStructuredStoreConfig config) {
    if (config.root_path.empty() || config.max_segment_bytes == 0) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }
    auto store = std::unique_ptr<LogStructuredStore>(
        new LogStructuredStore(std::move(config)));
    auto directory = StorageDirectory::Open(store->config_.root_path);
    if (!directory) {
        if (directory.error() == StorageDirectoryError::kAlreadyMounted) {
            return tl::unexpected(StoreError::kAlreadyMounted);
        }
        if (directory.error() == StorageDirectoryError::kUnrecognizedFormat) {
            return tl::unexpected(StoreError::kUnrecognizedFormat);
        }
        if (directory.error() == StorageDirectoryError::kInvalidArgument) {
            return tl::unexpected(StoreError::kInvalidArgument);
        }
        return tl::unexpected(StoreError::kCorruptData);
    }
    store->directory_ = std::move(directory.value());
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

    std::vector<SegmentMetadata> expected_segments;
    uint64_t expected_active_segment = 0;
    if (fs::exists(config_.root_path + "/CURRENT", error)) {
        if (error) return tl::unexpected(StoreError::kIoError);
        auto manifest = LoadCurrentManifest(config_.root_path);
        if (!manifest) return tl::unexpected(StoreError::kCorruptData);
        auto checkpoint =
            LoadCheckpoint(config_.root_path, manifest->checkpoint_file);
        if (!checkpoint ||
            checkpoint->checkpoint_sequence != manifest->checkpoint_sequence ||
            checkpoint->next_sequence != manifest->next_sequence ||
            checkpoint->next_segment_id != manifest->next_segment_id ||
            checkpoint->segments != manifest->segments) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        auto restored = index_.Restore(checkpoint->index);
        if (!restored) return tl::unexpected(StoreError::kCorruptData);
        manifest_generation_ = manifest->generation;
        checkpoint_sequence_ = manifest->checkpoint_sequence;
        next_sequence_ = manifest->next_sequence;
        next_segment_id_ = manifest->next_segment_id;
        applied_delete_watermark_ = checkpoint->applied_delete_watermark;
        expected_segments = manifest->segments;
        expected_active_segment = manifest->active_segment_id;
        auto wal_generation = ParseNumberedName(manifest->wal_file, "WAL-");
        if (!wal_generation) return tl::unexpected(StoreError::kCorruptData);
        wal_generation_ = *wal_generation;
        wal_path_ = config_.root_path + "/" + manifest->wal_file;
    } else if (error) {
        return tl::unexpected(StoreError::kIoError);
    }

    uint64_t max_sequence = checkpoint_sequence_;
    if (fs::exists(wal_path_, error)) {
        if (error) return tl::unexpected(StoreError::kIoError);
        auto scan = ScanWal(wal_path_);
        if (!scan) return tl::unexpected(StoreError::kIoError);
        if (scan->termination == WalScanTermination::kCorruptRecord) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        for (const auto& record : scan->records) {
            if (record.sequence <= checkpoint_sequence_) {
                return tl::unexpected(StoreError::kCorruptData);
            }
            max_sequence = std::max(max_sequence, record.sequence);
        }
        auto replayed = ReplayWal(scan->records, index_);
        if (!replayed) return tl::unexpected(StoreError::kCorruptData);
        auto opened = WalWriter::OpenForAppend(wal_path_, scan->valid_bytes);
        if (!opened) return tl::unexpected(StoreError::kIoError);
        wal_ = std::move(opened.value());
    } else {
        if (error || manifest_generation_ != 0) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        auto created = WalWriter::Create(wal_path_);
        if (!created) return tl::unexpected(StoreError::kIoError);
        wal_ = std::move(created.value());
    }

    auto recovered_segments =
        RecoverSegments(expected_segments, expected_active_segment);
    if (!recovered_segments) return recovered_segments;
    next_sequence_ = std::max(next_sequence_, max_sequence + 1);
    RefreshSegmentLiveBytes();
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::RecoverSegments(
    const std::vector<SegmentMetadata>& expected_segments,
    uint64_t checkpoint_active_segment_id) {
    namespace fs = std::filesystem;
    std::error_code error;
    const uint64_t post_checkpoint_segment_floor = next_segment_id_;
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

    for (const auto& segment : expected_segments) {
        if (segment.segment_id == 0 ||
            !segments_.emplace(segment.segment_id, segment).second ||
            !ordered_segments.contains(segment.segment_id)) {
            return tl::unexpected(StoreError::kCorruptData);
        }
    }
    for (const auto& [segment_id, path] : ordered_segments) {
        static_cast<void>(path);
        if (!expected_segments.empty() && !segments_.contains(segment_id) &&
            segment_id < post_checkpoint_segment_floor) {
            return tl::unexpected(StoreError::kCorruptData);
        }
    }

    uint64_t active_segment_id = checkpoint_active_segment_id;
    if (!ordered_segments.empty() &&
        ordered_segments.rbegin()->first > active_segment_id) {
        active_segment_id = ordered_segments.rbegin()->first;
    }
    if (expected_segments.empty() && !ordered_segments.empty()) {
        active_segment_id = ordered_segments.rbegin()->first;
    }

    std::unordered_map<uint64_t, std::vector<ScannedRecord>> scanned_segments;
    for (const auto& [segment_id, path] : ordered_segments) {
        auto scan = ScanSegment(path, segment_id);
        if (!scan) return tl::unexpected(StoreError::kCorruptData);
        if (scan->termination == ScanTermination::kCorruptRecord ||
            (scan->termination == ScanTermination::kIncompleteTail &&
             segment_id != active_segment_id)) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        if (scan->termination == ScanTermination::kIncompleteTail) {
            auto truncated = TruncateSegment(path, scan->valid_bytes);
            if (!truncated) return tl::unexpected(StoreError::kIoError);
        }

        auto metadata = segments_.find(segment_id);
        if (metadata == segments_.end()) {
            segments_.emplace(
                segment_id,
                SegmentMetadata{.segment_id = segment_id,
                                .level = 0,
                                .state = segment_id == active_segment_id
                                             ? SegmentLifecycle::kActive
                                             : SegmentLifecycle::kSealed,
                                .valid_bytes = scan->valid_bytes,
                                .live_bytes = 0,
                                .record_count = scan->records.size(),
                                .mutation_epoch = 1});
        } else {
            const bool was_checkpoint_active =
                segment_id == checkpoint_active_segment_id;
            if ((!was_checkpoint_active &&
                 (metadata->second.valid_bytes != scan->valid_bytes ||
                  metadata->second.record_count != scan->records.size())) ||
                (was_checkpoint_active &&
                 (metadata->second.valid_bytes > scan->valid_bytes ||
                  metadata->second.record_count > scan->records.size()))) {
                return tl::unexpected(StoreError::kCorruptData);
            }
            metadata->second.valid_bytes = scan->valid_bytes;
            metadata->second.record_count = scan->records.size();
            if (segment_id != active_segment_id &&
                metadata->second.state == SegmentLifecycle::kActive) {
                metadata->second.state = SegmentLifecycle::kSealed;
                ++metadata->second.mutation_epoch;
            }
        }
        scanned_segments.emplace(segment_id, std::move(scan->records));
        segment_paths_.emplace(segment_id, path);
        next_segment_id_ = std::max(next_segment_id_, segment_id + 1);
    }

    auto validated = ValidateIndexRecords(scanned_segments);
    if (!validated) return validated;

    if (active_segment_id == 0) {
        const uint64_t segment_id = next_segment_id_++;
        const std::string path = SegmentPath(segment_id);
        auto created = SegmentWriter::Create(path, segment_id);
        if (!created) return tl::unexpected(StoreError::kIoError);
        segment_paths_.emplace(segment_id, path);
        segments_.emplace(segment_id,
                          SegmentMetadata{.segment_id = segment_id,
                                          .level = 0,
                                          .state = SegmentLifecycle::kActive,
                                          .valid_bytes = 0,
                                          .live_bytes = 0,
                                          .record_count = 0,
                                          .mutation_epoch = 1});
        active_segment_ = std::move(created.value());
        return {};
    }

    auto active_path = ordered_segments.find(active_segment_id);
    auto active_metadata = segments_.find(active_segment_id);
    if (active_path == ordered_segments.end() ||
        active_metadata == segments_.end()) {
        return tl::unexpected(StoreError::kCorruptData);
    }
    active_metadata->second.state = SegmentLifecycle::kActive;
    auto opened =
        SegmentWriter::OpenForAppend(active_path->second, active_segment_id,
                                     active_metadata->second.valid_bytes);
    if (!opened) return tl::unexpected(StoreError::kIoError);
    active_segment_ = std::move(opened.value());
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::ValidateIndexRecords(
    const std::unordered_map<uint64_t, std::vector<ScannedRecord>>&
        scanned_segments) const {
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
    return {};
}

void LogStructuredStore::RefreshSegmentLiveBytes() {
    for (auto& [segment_id, segment] : segments_) {
        static_cast<void>(segment_id);
        segment.live_bytes = 0;
    }
    for (const auto& item : index_.Snapshot()) {
        if (item.version.state != VersionState::kCommitted) continue;
        auto segment = segments_.find(item.version.physical.segment_id);
        if (segment != segments_.end()) {
            segment->second.live_bytes += item.version.physical.total_length;
        }
    }
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
    auto old_metadata = segments_.find(active_segment_->segment_id());
    if (old_metadata == segments_.end()) {
        return tl::unexpected(StoreError::kCorruptData);
    }
    old_metadata->second.state = SegmentLifecycle::kSealed;
    ++old_metadata->second.mutation_epoch;
    const uint64_t segment_id = next_segment_id_++;
    const std::string path = SegmentPath(segment_id);
    auto created = SegmentWriter::Create(path, segment_id);
    if (!created) return tl::unexpected(StoreError::kIoError);
    segment_paths_.emplace(segment_id, path);
    segments_.emplace(segment_id,
                      SegmentMetadata{.segment_id = segment_id,
                                      .level = 0,
                                      .state = SegmentLifecycle::kActive,
                                      .valid_bytes = 0,
                                      .live_bytes = 0,
                                      .record_count = 0,
                                      .mutation_epoch = 1});
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
    auto& segment = segments_.at(active_segment_->segment_id());
    segment.valid_bytes = active_segment_->tail();
    ++segment.record_count;
    ++segment.mutation_epoch;
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
    RefreshSegmentLiveBytes();
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
    if (identity.tenant_id.size() > kMaxTenantLength ||
        identity.object_key.size() > kMaxKeyLength) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }
    const uint64_t record_bytes =
        AlignedRecordSize(static_cast<uint32_t>(identity.tenant_id.size()),
                          static_cast<uint32_t>(identity.object_key.size()), 0);
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
    auto& segment = segments_.at(active_segment_->segment_id());
    segment.valid_bytes = active_segment_->tail();
    ++segment.record_count;
    ++segment.mutation_epoch;
    applied_delete_watermark_ = sequence;
    RefreshSegmentLiveBytes();
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::Checkpoint() {
    std::lock_guard lock(mutex_);
    if (!active_segment_->Sync() || !wal_->Sync()) {
        return tl::unexpected(StoreError::kIoError);
    }
    RefreshSegmentLiveBytes();
    const uint64_t generation = manifest_generation_ + 1;
    const uint64_t checkpoint_sequence = next_sequence_ - 1;
    std::vector<SegmentMetadata> segment_snapshot;
    segment_snapshot.reserve(segments_.size());
    for (const auto& [segment_id, segment] : segments_) {
        static_cast<void>(segment_id);
        segment_snapshot.push_back(segment);
    }
    CheckpointState checkpoint{
        .format_version = 1,
        .checkpoint_sequence = checkpoint_sequence,
        .next_sequence = next_sequence_,
        .next_segment_id = next_segment_id_,
        .applied_delete_watermark = applied_delete_watermark_,
        .index = index_.Snapshot(),
        .segments = segment_snapshot};
    auto checkpoint_file =
        WriteCheckpoint(config_.root_path, generation, checkpoint);
    if (!checkpoint_file) return tl::unexpected(StoreError::kIoError);

    const uint64_t next_wal_generation = wal_generation_ + 1;
    const std::string next_wal_file =
        FormatNumberedName("WAL-", next_wal_generation);
    const std::string next_wal_path = config_.root_path + "/" + next_wal_file;
    auto next_wal = WalWriter::Create(next_wal_path);
    if (!next_wal || !(*next_wal)->Sync()) {
        return tl::unexpected(StoreError::kIoError);
    }

    ManifestState manifest{.format_version = 1,
                           .generation = generation,
                           .checkpoint_sequence = checkpoint_sequence,
                           .next_sequence = next_sequence_,
                           .next_segment_id = next_segment_id_,
                           .active_segment_id = active_segment_->segment_id(),
                           .checkpoint_file = *checkpoint_file,
                           .wal_file = next_wal_file,
                           .segments = segment_snapshot};
    auto published = PublishManifest(config_.root_path, manifest);
    if (!published) {
        RemoveFileDurably(config_.root_path, next_wal_file);
        return tl::unexpected(StoreError::kIoError);
    }

    const std::string old_wal_file =
        std::filesystem::path(wal_path_).filename().string();
    wal_ = std::move(next_wal.value());
    wal_path_ = next_wal_path;
    wal_generation_ = next_wal_generation;
    manifest_generation_ = generation;
    checkpoint_sequence_ = checkpoint_sequence;
    if (old_wal_file != next_wal_file) {
        auto removed = RemoveFileDurably(config_.root_path, old_wal_file);
        if (!removed) return tl::unexpected(StoreError::kIoError);
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
