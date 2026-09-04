#include "storage/local/log_structured/store.h"

#include <algorithm>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <limits>
#include <map>
#include <string_view>
#include <thread>
#include <unordered_set>

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

bool SyncDirectory(const std::string& path) {
    const int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return false;
    const bool synced = fsync(fd) == 0;
    const bool closed = close(fd) == 0;
    return synced && closed;
}

bool IsCompactionOnly(const SegmentScanResult& scan) {
    return !scan.records.empty() &&
           std::all_of(scan.records.begin(), scan.records.end(),
                       [](const ScannedRecord& record) {
                           return record.kind == RecordKind::kCompactionCopy;
                       });
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
    fs::create_directories(config_.root_path + "/tmp", error);
    if (error) return tl::unexpected(StoreError::kIoError);
    for (const auto& entry :
         fs::directory_iterator(config_.root_path + "/tmp", error)) {
        if (error) return tl::unexpected(StoreError::kIoError);
        fs::remove_all(entry.path(), error);
        if (error) return tl::unexpected(StoreError::kIoError);
    }

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
            !segments_.emplace(segment.segment_id, segment).second) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        if (!ordered_segments.contains(segment.segment_id) &&
            segment.state != SegmentLifecycle::kRetired) {
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

    for (auto it = ordered_segments.begin(); it != ordered_segments.end();) {
        if (segments_.contains(it->first) ||
            it->first < post_checkpoint_segment_floor) {
            ++it;
            continue;
        }
        auto scan = ScanSegment(it->second, it->first);
        if (!scan) return tl::unexpected(StoreError::kCorruptData);
        if (scan->termination != ScanTermination::kCleanEof ||
            !IsCompactionOnly(*scan)) {
            ++it;
            continue;
        }
        const auto filename = fs::path(it->second).filename().string();
        auto removed = RemoveFileDurably(segments_path_, filename);
        if (!removed) return tl::unexpected(StoreError::kIoError);
        it = ordered_segments.erase(it);
    }

    uint64_t active_segment_id = checkpoint_active_segment_id;
    if (expected_segments.empty() && !ordered_segments.empty()) {
        active_segment_id = ordered_segments.rbegin()->first;
    } else {
        for (const auto& [segment_id, path] : ordered_segments) {
            static_cast<void>(path);
            if (!segments_.contains(segment_id)) {
                active_segment_id = std::max(active_segment_id, segment_id);
            }
        }
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

    for (auto it = segments_.begin(); it != segments_.end();) {
        if (it->second.state != SegmentLifecycle::kRetired) {
            ++it;
            continue;
        }
        auto path = segment_paths_.find(it->first);
        if (path != segment_paths_.end()) {
            const auto filename = fs::path(path->second).filename().string();
            auto removed = RemoveFileDurably(segments_path_, filename);
            if (!removed) return tl::unexpected(StoreError::kIoError);
            segment_paths_.erase(path);
        }
        it = segments_.erase(it);
    }

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
    return RotateActiveSegmentLocked();
}

tl::expected<void, StoreError> LogStructuredStore::RotateActiveSegmentLocked() {
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
    return PreparePutLocked(identity, value);
}

tl::expected<PreparedWrite, StoreError> LogStructuredStore::PreparePut(
    std::string tenant_id, std::string object_key, std::string_view value) {
    std::lock_guard lock(mutex_);
    RecordIdentity identity{.tenant_id = std::move(tenant_id),
                            .object_key = std::move(object_key),
                            .incarnation = {.high = directory_->identity().high,
                                            .low = next_sequence_}};
    return PreparePutLocked(identity, value);
}

tl::expected<PreparedWrite, StoreError> LogStructuredStore::PreparePutLocked(
    const RecordIdentity& identity, std::string_view value) {
    if (identity.tenant_id.size() > kMaxTenantLength ||
        identity.object_key.size() > kMaxKeyLength) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }
    const uint64_t record_bytes = AlignedRecordSize(
        static_cast<uint32_t>(identity.tenant_id.size()),
        static_cast<uint32_t>(identity.object_key.size()), value.size());
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

tl::expected<void, StoreError> LogStructuredStore::Sync() {
    std::lock_guard lock(mutex_);
    if (!active_segment_->Sync() || !wal_->Sync()) {
        return tl::unexpected(StoreError::kIoError);
    }
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::SealActiveSegment() {
    std::lock_guard lock(mutex_);
    if (active_segment_->tail() == 0) return {};
    return RotateActiveSegmentLocked();
}

tl::expected<void, StoreError> LogStructuredStore::Checkpoint() {
    std::lock_guard lock(mutex_);
    return CheckpointLocked();
}

tl::expected<void, StoreError> LogStructuredStore::CheckpointLocked() {
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
        static_cast<void>(RemoveFileDurably(config_.root_path, old_wal_file));
    }
    return {};
}

void LogStructuredStore::CleanupRetiredSegmentsLocked() {
    namespace fs = std::filesystem;
    for (auto it = segments_.begin(); it != segments_.end();) {
        if (it->second.state != SegmentLifecycle::kRetired) {
            ++it;
            continue;
        }
        auto path = segment_paths_.find(it->first);
        if (path != segment_paths_.end()) {
            const auto filename = fs::path(path->second).filename().string();
            if (!RemoveFileDurably(segments_path_, filename)) {
                ++it;
                continue;
            }
            segment_paths_.erase(path);
        }
        it = segments_.erase(it);
    }
}

tl::expected<CompactionResult, StoreError> LogStructuredStore::CompactOnce(
    const CompactionOptions& options) {
    if (options.max_source_segments == 0 || options.max_input_bytes == 0 ||
        options.min_reclaim_ratio < 0.0 || options.min_reclaim_ratio > 1.0) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }
    if (options.stop_token.stop_requested()) {
        return tl::unexpected(StoreError::kCancelled);
    }

    std::lock_guard compaction_lock(compaction_mutex_);
    std::vector<SegmentMetadata> sources;
    std::unordered_map<uint64_t, std::string> source_paths;
    std::vector<IndexSnapshotEntry> live_entries;
    std::vector<uint64_t> reserved_target_ids;
    uint32_t target_level = 1;
    uint64_t input_bytes = 0;

    {
        std::lock_guard lock(mutex_);
        CleanupRetiredSegmentsLocked();
        RefreshSegmentLiveBytes();
        std::vector<SegmentMetadata> sealed;
        for (const auto& [segment_id, segment] : segments_) {
            static_cast<void>(segment_id);
            if (segment.state != SegmentLifecycle::kSealed ||
                segment.valid_bytes == 0 ||
                segment.live_bytes > segment.valid_bytes) {
                continue;
            }
            sealed.push_back(segment);
            const uint64_t reclaimable =
                segment.valid_bytes - segment.live_bytes;
            const double reclaim_ratio =
                static_cast<double>(reclaimable) / segment.valid_bytes;
            if (reclaimable != 0 &&
                (segment.live_bytes == 0 ||
                 reclaim_ratio >= options.min_reclaim_ratio)) {
                sources.push_back(segment);
            }
        }
        std::sort(
            sources.begin(), sources.end(),
            [](const SegmentMetadata& left, const SegmentMetadata& right) {
                const uint64_t left_reclaim =
                    left.valid_bytes - left.live_bytes;
                const uint64_t right_reclaim =
                    right.valid_bytes - right.live_bytes;
                if (left_reclaim != right_reclaim) {
                    return left_reclaim > right_reclaim;
                }
                return left.segment_id < right.segment_id;
            });
        if (sources.empty() && options.enable_tiering) {
            for (uint32_t level = 0; level + 1 < options.max_levels; ++level) {
                std::vector<SegmentMetadata> level_segments;
                for (const auto& segment : sealed) {
                    if (segment.level == level)
                        level_segments.push_back(segment);
                }
                if (level_segments.size() < options.fanout) continue;
                std::sort(level_segments.begin(), level_segments.end(),
                          [](const SegmentMetadata& left,
                             const SegmentMetadata& right) {
                              return left.segment_id < right.segment_id;
                          });
                level_segments.resize(options.fanout);
                sources = std::move(level_segments);
                break;
            }
        }
        if (sources.size() > options.max_source_segments) {
            sources.resize(options.max_source_segments);
        }
        uint64_t selected_bytes = 0;
        size_t selected_count = 0;
        for (; selected_count < sources.size(); ++selected_count) {
            const auto bytes = sources[selected_count].valid_bytes;
            if (selected_count != 0 &&
                bytes > options.max_input_bytes - selected_bytes) {
                break;
            }
            selected_bytes += bytes;
            if (selected_bytes >= options.max_input_bytes) {
                ++selected_count;
                break;
            }
        }
        sources.resize(selected_count);
        if (sources.empty()) return CompactionResult{};

        std::unordered_set<uint64_t> source_ids;
        for (auto& source : sources) {
            source_ids.insert(source.segment_id);
            input_bytes += source.valid_bytes;
            const uint32_t next_level =
                source.level + static_cast<uint32_t>(source.level + 1 != 0);
            target_level = std::max(target_level, next_level);
            auto path = segment_paths_.find(source.segment_id);
            if (path == segment_paths_.end()) {
                return tl::unexpected(StoreError::kCorruptData);
            }
            source_paths.emplace(source.segment_id, path->second);
            auto& current = segments_.at(source.segment_id);
            current.state = SegmentLifecycle::kCompacting;
            ++current.mutation_epoch;
            source = current;
        }
        for (const auto& entry : index_.CurrentSnapshot()) {
            if (source_ids.contains(entry.version.physical.segment_id)) {
                live_entries.push_back(entry);
            }
        }
        reserved_target_ids.reserve(std::max<size_t>(1, live_entries.size()));
        for (size_t i = 0; i < std::max<size_t>(1, live_entries.size()); ++i) {
            reserved_target_ids.push_back(next_segment_id_++);
        }
    }

    target_level = std::min(target_level, options.max_levels - 1);
    uint64_t target_bytes = config_.max_segment_bytes;
    for (uint32_t level = 0; level < target_level; ++level) {
        if (target_bytes > options.max_target_bytes / options.fanout) {
            target_bytes = options.max_target_bytes;
            break;
        }
        target_bytes *= options.fanout;
    }
    target_bytes = std::min(target_bytes, options.max_target_bytes);

    struct TargetOutput {
        uint64_t segment_id{0};
        std::string temporary_path;
        std::string final_path;
        uint64_t valid_bytes{0};
        uint64_t record_count{0};
    };
    std::vector<TargetOutput> targets;
    std::vector<CompactionIndexUpdate> updates;
    std::unique_ptr<SegmentWriter> writer;

    const auto cleanup_targets = [&]() {
        writer.reset();
        std::error_code error;
        for (const auto& target : targets) {
            std::filesystem::remove(target.temporary_path, error);
            error.clear();
            std::filesystem::remove(target.final_path, error);
            error.clear();
        }
    };
    const auto reset_sources = [&]() {
        std::lock_guard lock(mutex_);
        for (const auto& source : sources) {
            auto current = segments_.find(source.segment_id);
            if (current != segments_.end() &&
                current->second.state == SegmentLifecycle::kCompacting) {
                current->second.state = SegmentLifecycle::kSealed;
                ++current->second.mutation_epoch;
            }
        }
    };

    const auto copy_started = std::chrono::steady_clock::now();
    uint64_t copied_bytes = 0;
    for (const auto& entry : live_entries) {
        if (options.stop_token.stop_requested()) {
            cleanup_targets();
            reset_sources();
            return tl::unexpected(StoreError::kCancelled);
        }
        const auto path = source_paths.find(entry.version.physical.segment_id);
        if (path == source_paths.end()) {
            cleanup_targets();
            reset_sources();
            return tl::unexpected(StoreError::kCorruptData);
        }
        auto record = ReadRecord(path->second, entry.version.physical);
        if (!record || record->identity != entry.identity ||
            record->kind == RecordKind::kTombstone) {
            cleanup_targets();
            reset_sources();
            return tl::unexpected(StoreError::kCorruptData);
        }
        const uint64_t record_bytes = entry.version.physical.total_length;
        if (!writer || (writer->tail() != 0 &&
                        writer->tail() + record_bytes > target_bytes)) {
            if (writer && !writer->Sync()) {
                cleanup_targets();
                reset_sources();
                return tl::unexpected(StoreError::kIoError);
            }
            writer.reset();
            const uint64_t segment_id = reserved_target_ids[targets.size()];
            const std::string temporary_path =
                config_.root_path + "/tmp/" + FormatSegmentName(segment_id);
            const std::string final_path = SegmentPath(segment_id);
            auto created = SegmentWriter::Create(temporary_path, segment_id);
            if (!created) {
                cleanup_targets();
                reset_sources();
                return tl::unexpected(StoreError::kIoError);
            }
            writer = std::move(created.value());
            targets.push_back(TargetOutput{.segment_id = segment_id,
                                           .temporary_path = temporary_path,
                                           .final_path = final_path});
        }
        auto appended = writer->Append(entry.identity, record->value,
                                       RecordKind::kCompactionCopy,
                                       entry.version.sequence, false);
        if (!appended) {
            cleanup_targets();
            reset_sources();
            return tl::unexpected(StoreError::kIoError);
        }
        targets.back().valid_bytes = writer->tail();
        ++targets.back().record_count;
        updates.push_back(CompactionIndexUpdate{
            .identity = entry.identity,
            .expected_source = entry.version.physical,
            .expected_epoch = entry.version.mutation_epoch,
            .target = *appended});
        copied_bytes += record_bytes;
        if (options.max_bytes_per_second != 0) {
            const auto target_elapsed = std::chrono::duration<double>(
                static_cast<double>(copied_bytes) /
                options.max_bytes_per_second);
            while (!options.stop_token.stop_requested() &&
                   std::chrono::steady_clock::now() - copy_started <
                       target_elapsed) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            if (options.stop_token.stop_requested()) {
                cleanup_targets();
                reset_sources();
                return tl::unexpected(StoreError::kCancelled);
            }
        }
    }
    if (options.stop_token.stop_requested()) {
        cleanup_targets();
        reset_sources();
        return tl::unexpected(StoreError::kCancelled);
    }
    if (writer && !writer->Sync()) {
        cleanup_targets();
        reset_sources();
        return tl::unexpected(StoreError::kIoError);
    }
    if (options.stop_token.stop_requested()) {
        cleanup_targets();
        reset_sources();
        return tl::unexpected(StoreError::kCancelled);
    }
    writer.reset();

    for (const auto& target : targets) {
        if (rename(target.temporary_path.c_str(), target.final_path.c_str()) !=
            0) {
            cleanup_targets();
            reset_sources();
            return tl::unexpected(StoreError::kIoError);
        }
    }
    if (!targets.empty() && !SyncDirectory(segments_path_)) {
        cleanup_targets();
        reset_sources();
        return tl::unexpected(StoreError::kIoError);
    }

    {
        std::lock_guard lock(mutex_);
        const auto index_before = index_.Snapshot();
        auto installed = index_.InstallCompactionCopies(updates);
        if (!installed) {
            cleanup_targets();
            for (const auto& source : sources) {
                auto current = segments_.find(source.segment_id);
                if (current != segments_.end()) {
                    current->second.state = SegmentLifecycle::kSealed;
                    ++current->second.mutation_epoch;
                }
            }
            return tl::unexpected(MapIndexError(installed.error()));
        }
        std::unordered_set<uint64_t> source_ids;
        for (const auto& source : sources) source_ids.insert(source.segment_id);
        index_.ReclaimNonCurrentVersionsInSegments(source_ids);
        for (const auto& target : targets) {
            segment_paths_.emplace(target.segment_id, target.final_path);
            segments_.emplace(
                target.segment_id,
                SegmentMetadata{.segment_id = target.segment_id,
                                .level = target_level,
                                .state = SegmentLifecycle::kSealed,
                                .valid_bytes = target.valid_bytes,
                                .live_bytes = target.valid_bytes,
                                .record_count = target.record_count,
                                .mutation_epoch = 1});
        }
        for (const auto& source : sources) {
            auto& current = segments_.at(source.segment_id);
            current.state = SegmentLifecycle::kRetired;
            ++current.mutation_epoch;
        }
        RefreshSegmentLiveBytes();
        auto checkpointed = CheckpointLocked();
        if (!checkpointed) {
            static_cast<void>(index_.Restore(index_before));
            for (const auto& target : targets) {
                segment_paths_.erase(target.segment_id);
                segments_.erase(target.segment_id);
            }
            for (const auto& source : sources) {
                auto current = segments_.find(source.segment_id);
                if (current != segments_.end()) {
                    current->second.state = SegmentLifecycle::kSealed;
                    ++current->second.mutation_epoch;
                }
            }
            cleanup_targets();
            return tl::unexpected(checkpointed.error());
        }
        CleanupRetiredSegmentsLocked();
        RefreshSegmentLiveBytes();
    }

    uint64_t output_bytes = 0;
    for (const auto& target : targets) output_bytes += target.valid_bytes;
    return CompactionResult{.source_segments = sources.size(),
                            .target_segments = targets.size(),
                            .input_bytes = input_bytes,
                            .output_bytes = output_bytes,
                            .reclaimed_bytes = input_bytes - output_bytes};
}

tl::expected<std::string, StoreError> LogStructuredStore::ReadEntryLocked(
    const IndexSnapshotEntry& entry) const {
    auto path = segment_paths_.find(entry.version.physical.segment_id);
    if (path == segment_paths_.end()) {
        return tl::unexpected(StoreError::kCorruptData);
    }
    auto record = ReadRecord(path->second, entry.version.physical);
    if (!record || record->identity != entry.identity ||
        record->kind == RecordKind::kTombstone) {
        return tl::unexpected(StoreError::kCorruptData);
    }
    return std::move(record->value);
}

tl::expected<std::string, StoreError> LogStructuredStore::Get(
    const RecordIdentity& identity) const {
    std::lock_guard lock(mutex_);
    auto version = index_.LookupCommitted(identity);
    if (!version) return tl::unexpected(StoreError::kNotFound);
    return ReadEntryLocked(
        IndexSnapshotEntry{.identity = identity, .version = *version});
}

tl::expected<std::string, StoreError> LogStructuredStore::GetLatest(
    std::string_view tenant_id, std::string_view object_key) const {
    std::lock_guard lock(mutex_);
    auto entry = index_.LookupCurrent(tenant_id, object_key);
    if (!entry) return tl::unexpected(StoreError::kNotFound);
    return ReadEntryLocked(*entry);
}

bool LogStructuredStore::ContainsLatest(std::string_view tenant_id,
                                        std::string_view object_key) const {
    std::lock_guard lock(mutex_);
    return index_.LookupCurrent(tenant_id, object_key).has_value();
}

std::vector<IndexSnapshotEntry> LogStructuredStore::SnapshotIndex() const {
    std::lock_guard lock(mutex_);
    return index_.Snapshot();
}

std::vector<IndexSnapshotEntry> LogStructuredStore::SnapshotCurrentIndex()
    const {
    std::lock_guard lock(mutex_);
    return index_.CurrentSnapshot();
}

StoreStats LogStructuredStore::SnapshotStats() const {
    std::lock_guard lock(mutex_);
    StoreStats stats;
    for (const auto& [segment_id, segment] : segments_) {
        static_cast<void>(segment_id);
        stats.physical_bytes += segment.valid_bytes;
        stats.live_record_bytes += segment.live_bytes;
        if (segment.state == SegmentLifecycle::kActive) {
            ++stats.active_segments;
        } else if (segment.state == SegmentLifecycle::kRetired) {
            ++stats.retired_segments;
        } else {
            ++stats.sealed_segments;
        }
    }
    for (const auto& entry : index_.CurrentSnapshot()) {
        stats.logical_value_bytes += entry.version.physical.value_length;
    }
    stats.reclaimable_bytes = stats.physical_bytes - stats.live_record_bytes;
    return stats;
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
