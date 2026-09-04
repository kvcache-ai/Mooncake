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

std::mutex compaction_crash_mutex;
std::function<bool(CompactionCrashPoint)> compaction_crash_predicate;

bool HitCompactionCrashPoint(CompactionCrashPoint point) {
    std::function<bool(CompactionCrashPoint)> predicate;
    {
        std::lock_guard lock(compaction_crash_mutex);
        predicate = compaction_crash_predicate;
    }
    return predicate && predicate(point);
}

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

class ScopeExit {
   public:
    explicit ScopeExit(std::function<void()> callback)
        : callback_(std::move(callback)) {}
    ~ScopeExit() { callback_(); }

    ScopeExit(const ScopeExit&) = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;

   private:
    std::function<void()> callback_;
};

bool IsCompactionOnly(const SegmentScanResult& scan) {
    return !scan.records.empty() &&
           std::all_of(scan.records.begin(), scan.records.end(),
                       [](const ScannedRecord& record) {
                           return record.kind == RecordKind::kCompactionCopy;
                       });
}

bool IsKnownSegmentLifecycle(SegmentLifecycle state) {
    switch (state) {
        case SegmentLifecycle::kCreating:
        case SegmentLifecycle::kActive:
        case SegmentLifecycle::kSealing:
        case SegmentLifecycle::kSealed:
        case SegmentLifecycle::kCompacting:
        case SegmentLifecycle::kRetired:
            return true;
    }
    return false;
}

bool ValidatePersistentMetadata(const CheckpointState& checkpoint,
                                const ManifestState& manifest) {
    if (checkpoint.next_sequence == 0 || checkpoint.next_segment_id == 0 ||
        checkpoint.checkpoint_sequence != checkpoint.next_sequence - 1 ||
        checkpoint.applied_delete_watermark > checkpoint.checkpoint_sequence ||
        manifest.active_segment_id == 0) {
        return false;
    }

    std::unordered_set<uint64_t> segment_ids;
    size_t active_segments = 0;
    bool declared_active_found = false;
    for (const auto& segment : checkpoint.segments) {
        if (!IsKnownSegmentLifecycle(segment.state) ||
            segment.segment_id == 0 ||
            segment.segment_id >= checkpoint.next_segment_id ||
            segment.state == SegmentLifecycle::kCreating ||
            segment.live_bytes > segment.valid_bytes ||
            segment.mutation_epoch == 0 ||
            !segment_ids.insert(segment.segment_id).second) {
            return false;
        }
        if (segment.state == SegmentLifecycle::kActive) {
            ++active_segments;
            if (segment.segment_id != manifest.active_segment_id) return false;
        }
        if (segment.segment_id == manifest.active_segment_id) {
            declared_active_found = true;
            if (segment.state != SegmentLifecycle::kActive &&
                segment.state != SegmentLifecycle::kSealing) {
                return false;
            }
        }
    }
    if (!declared_active_found || active_segments > 1) return false;

    for (const auto& item : checkpoint.index) {
        if (item.version.sequence == 0 ||
            item.version.sequence > checkpoint.checkpoint_sequence) {
            return false;
        }
    }
    return true;
}

}  // namespace

LogStructuredStore::LogStructuredStore(LogStructuredStoreConfig config)
    : config_(std::move(config)),
      segments_path_(config_.root_path + "/segments"),
      wal_path_(config_.root_path + "/" + std::string(kInitialWalFile)) {}

void LogStructuredStore::SetCompactionCrashPredicateForTest(
    std::function<bool(CompactionCrashPoint)> predicate) {
    std::lock_guard lock(compaction_crash_mutex);
    compaction_crash_predicate = std::move(predicate);
}

tl::expected<std::unique_ptr<LogStructuredStore>, StoreError>
LogStructuredStore::Open(LogStructuredStoreConfig config) {
    if (config.root_path.empty() || config.max_segment_bytes == 0 ||
        config.max_physical_bytes > config.max_total_physical_bytes) {
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
            checkpoint->segments != manifest->segments ||
            !ValidatePersistentMetadata(*checkpoint, *manifest)) {
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
        if (!wal_generation || *wal_generation == 0) {
            return tl::unexpected(StoreError::kCorruptData);
        }
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
        index_.ReclaimNonCommittedVersionsAfterRecovery();
        auto opened = WalWriter::OpenForAppend(wal_path_, scan->valid_bytes);
        if (!opened) return tl::unexpected(StoreError::kIoError);
        wal_ = std::move(opened.value());
    } else {
        if (error || manifest_generation_ != 0) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        auto created = WalWriter::Create(wal_path_);
        if (!created || !(*created)->Sync() ||
            !SyncDirectory(config_.root_path)) {
            return tl::unexpected(StoreError::kIoError);
        }
        wal_ = std::move(created.value());
    }

    auto recovered_segments =
        RecoverSegments(expected_segments, expected_active_segment);
    if (!recovered_segments) return recovered_segments;
    next_sequence_ = std::max(next_sequence_, max_sequence + 1);
    RefreshSegmentLiveBytes();
    const std::string wal_file =
        std::filesystem::path(wal_path_).filename().string();
    auto cleaned = CleanupMetadataArtifacts(config_.root_path,
                                            manifest_generation_, wal_file);
    if (!cleaned) return tl::unexpected(StoreError::kIoError);
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
            segment.state == SegmentLifecycle::kCreating) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        auto recovered_segment = segment;
        if (recovered_segment.state == SegmentLifecycle::kSealing ||
            recovered_segment.state == SegmentLifecycle::kCompacting) {
            recovered_segment.state = SegmentLifecycle::kSealed;
            ++recovered_segment.mutation_epoch;
        }
        if (!segments_
                 .emplace(recovered_segment.segment_id,
                          std::move(recovered_segment))
                 .second) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        if (!ordered_segments.contains(segment.segment_id) &&
            segment.state != SegmentLifecycle::kRetired) {
            return tl::unexpected(StoreError::kCorruptData);
        }
    }
    for (auto it = ordered_segments.begin(); it != ordered_segments.end();) {
        if (segments_.contains(it->first)) {
            ++it;
            continue;
        }
        auto scan = ScanSegment(it->second, it->first);
        if (!scan) return tl::unexpected(StoreError::kCorruptData);
        if (scan->termination == ScanTermination::kCleanEof &&
            IsCompactionOnly(*scan)) {
            const auto filename = fs::path(it->second).filename().string();
            auto removed = RemoveFileDurably(segments_path_, filename);
            if (!removed) return tl::unexpected(StoreError::kIoError);
            it = ordered_segments.erase(it);
            continue;
        }
        if (!expected_segments.empty() &&
            it->first < post_checkpoint_segment_floor) {
            return tl::unexpected(StoreError::kCorruptData);
        }
        ++it;
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
        auto metadata = segments_.find(segment_id);
        if (metadata != segments_.end() &&
            metadata->second.state == SegmentLifecycle::kRetired) {
            segment_paths_.emplace(segment_id, path);
            continue;
        }
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

        metadata = segments_.find(segment_id);
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
            ForgetSegmentReaderLocked(it->first);
        }
        it = segments_.erase(it);
    }

    if (active_segment_id == 0) {
        const uint64_t segment_id = next_segment_id_;
        const std::string path = SegmentPath(segment_id);
        auto created = SegmentWriter::Create(path, segment_id);
        if (!created || !SyncDirectory(segments_path_)) {
            return tl::unexpected(StoreError::kIoError);
        }
        ++next_segment_id_;
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
        if (snapshot.version.state != VersionState::kCommitted) continue;
        if (snapshot.version.physical.total_length == 0) {
            return tl::unexpected(StoreError::kCorruptData);
        }
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

uint64_t LogStructuredStore::PhysicalBytesLocked() const {
    uint64_t physical_bytes = 0;
    for (const auto& [segment_id, segment] : segments_) {
        static_cast<void>(segment_id);
        if (segment.valid_bytes >
            std::numeric_limits<uint64_t>::max() - physical_bytes) {
            return std::numeric_limits<uint64_t>::max();
        }
        physical_bytes += segment.valid_bytes;
    }
    return physical_bytes;
}

void LogStructuredStore::RefreshSegmentLiveBytes() {
    for (auto& [segment_id, segment] : segments_) {
        static_cast<void>(segment_id);
        segment.live_bytes = 0;
    }
    for (const auto& item : index_.Snapshot()) {
        if (item.version.state != VersionState::kCommitted &&
            item.version.state != VersionState::kPrepared) {
            continue;
        }
        auto segment = segments_.find(item.version.physical.segment_id);
        if (segment != segments_.end()) {
            segment->second.live_bytes += item.version.physical.total_length;
        }
    }
}

tl::expected<void, StoreError> LogStructuredStore::AddLiveRecordLocked(
    const PhysicalRecord& physical) {
    auto segment = segments_.find(physical.segment_id);
    if (physical.total_length == 0 || segment == segments_.end() ||
        segment->second.live_bytes >
            std::numeric_limits<uint64_t>::max() - physical.total_length) {
        recovery_required_ = true;
        return tl::unexpected(StoreError::kCorruptData);
    }
    segment->second.live_bytes += physical.total_length;
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::RemoveLiveRecordLocked(
    const PhysicalRecord& physical) {
    auto segment = segments_.find(physical.segment_id);
    if (physical.total_length == 0 || segment == segments_.end() ||
        segment->second.live_bytes < physical.total_length) {
        recovery_required_ = true;
        return tl::unexpected(StoreError::kCorruptData);
    }
    segment->second.live_bytes -= physical.total_length;
    return {};
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
    const uint64_t segment_id = next_segment_id_;
    const std::string path = SegmentPath(segment_id);
    auto created = SegmentWriter::Create(path, segment_id);
    if (!created || !SyncDirectory(segments_path_)) {
        return tl::unexpected(StoreError::kIoError);
    }
    old_metadata->second.state = SegmentLifecycle::kSealed;
    ++old_metadata->second.mutation_epoch;
    ++next_segment_id_;
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
    std::vector<PutRequest> requests;
    requests.push_back({.tenant_id = std::move(tenant_id),
                        .object_key = std::move(object_key),
                        .value = value});
    auto prepared = PreparePutBatch(requests);
    if (!prepared) return tl::unexpected(prepared.error());
    return std::move(prepared->front());
}

tl::expected<std::vector<PreparedWrite>, StoreError>
LogStructuredStore::PreparePutBatch(const std::vector<PutRequest>& requests) {
    std::lock_guard lock(mutex_);
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
    if (requests.empty()) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }

    uint64_t total_record_bytes = 0;
    std::vector<uint64_t> record_sizes;
    record_sizes.reserve(requests.size());
    for (const auto& request : requests) {
        if (request.tenant_id.size() > kMaxTenantLength ||
            request.object_key.size() > kMaxKeyLength) {
            return tl::unexpected(StoreError::kInvalidArgument);
        }
        const uint64_t record_bytes =
            AlignedRecordSize(static_cast<uint32_t>(request.tenant_id.size()),
                              static_cast<uint32_t>(request.object_key.size()),
                              request.value.size());
        if (record_bytes == 0 ||
            total_record_bytes >
                std::numeric_limits<uint64_t>::max() - record_bytes) {
            return tl::unexpected(StoreError::kInvalidArgument);
        }
        total_record_bytes += record_bytes;
        record_sizes.push_back(record_bytes);
    }

    const uint64_t physical_bytes = PhysicalBytesLocked();
    if (physical_bytes > config_.max_physical_bytes ||
        total_record_bytes > config_.max_physical_bytes - physical_bytes ||
        physical_bytes > config_.max_total_physical_bytes ||
        compaction_reserved_bytes_ >
            config_.max_total_physical_bytes - physical_bytes ||
        total_record_bytes > config_.max_total_physical_bytes - physical_bytes -
                                 compaction_reserved_bytes_) {
        return tl::unexpected(StoreError::kNoSpace);
    }

    std::vector<PreparedWrite> prepared;
    prepared.reserve(requests.size());
    size_t begin = 0;
    while (begin < requests.size()) {
        auto rotated = RotateSegmentIfNeeded(record_sizes[begin]);
        if (!rotated) return tl::unexpected(rotated.error());

        const uint64_t segment_tail = active_segment_->tail();
        uint64_t chunk_bytes = 0;
        size_t end = begin;
        while (end < requests.size()) {
            const uint64_t next_bytes = record_sizes[end];
            if (end != begin &&
                (chunk_bytes > config_.max_segment_bytes - segment_tail ||
                 next_bytes >
                     config_.max_segment_bytes - segment_tail - chunk_bytes)) {
                break;
            }
            chunk_bytes += next_bytes;
            ++end;
            if (segment_tail + chunk_bytes >= config_.max_segment_bytes) break;
        }

        std::vector<SegmentAppendRequest> appends;
        std::vector<WalRecord> wal_records;
        appends.reserve(end - begin);
        wal_records.reserve(end - begin);
        for (size_t index = begin; index < end; ++index) {
            const uint64_t sequence = next_sequence_++;
            RecordIdentity identity{
                .tenant_id = requests[index].tenant_id,
                .object_key = requests[index].object_key,
                .incarnation = {.high = directory_->identity().high,
                                .low = sequence}};
            appends.push_back({.identity = std::move(identity),
                               .value = requests[index].value,
                               .kind = RecordKind::kValue,
                               .sequence = sequence});
        }

        auto physical_records = active_segment_->AppendBatch(
            appends, config_.sync_data, config_.payload_write_parallelism);
        if (!physical_records) {
            return tl::unexpected(StoreError::kIoError);
        }
        auto& segment = segments_.at(active_segment_->segment_id());
        segment.valid_bytes = active_segment_->tail();
        segment.record_count += appends.size();
        ++segment.mutation_epoch;

        for (size_t index = 0; index < appends.size(); ++index) {
            wal_records.push_back({.type = WalRecordType::kPrepareValue,
                                   .sequence = appends[index].sequence,
                                   .identity = appends[index].identity,
                                   .physical = (*physical_records)[index]});
        }
        if (!wal_->AppendBatch(wal_records, config_.sync_wal)) {
            return tl::unexpected(StoreError::kIoError);
        }

        for (size_t index = 0; index < appends.size(); ++index) {
            auto indexed = index_.Prepare(appends[index].identity,
                                          (*physical_records)[index],
                                          appends[index].sequence);
            if (!indexed) {
                recovery_required_ = true;
                return tl::unexpected(StoreError::kRecoveryRequired);
            }
            auto live = AddLiveRecordLocked((*physical_records)[index]);
            if (!live) return tl::unexpected(live.error());
            prepared.push_back({.identity = appends[index].identity,
                                .sequence = appends[index].sequence,
                                .physical = (*physical_records)[index]});
        }
        begin = end;
    }
    return prepared;
}

tl::expected<PreparedWrite, StoreError> LogStructuredStore::PreparePutLocked(
    const RecordIdentity& identity, std::string_view value) {
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
    if (identity.tenant_id.size() > kMaxTenantLength ||
        identity.object_key.size() > kMaxKeyLength) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }
    if (index_.Lookup(identity)) {
        return tl::unexpected(StoreError::kInvalidTransition);
    }
    const uint64_t record_bytes = AlignedRecordSize(
        static_cast<uint32_t>(identity.tenant_id.size()),
        static_cast<uint32_t>(identity.object_key.size()), value.size());
    if (record_bytes == 0) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }
    const uint64_t physical_bytes = PhysicalBytesLocked();
    if (physical_bytes > config_.max_physical_bytes ||
        record_bytes > config_.max_physical_bytes - physical_bytes ||
        physical_bytes > config_.max_total_physical_bytes ||
        compaction_reserved_bytes_ >
            config_.max_total_physical_bytes - physical_bytes ||
        record_bytes > config_.max_total_physical_bytes - physical_bytes -
                           compaction_reserved_bytes_) {
        return tl::unexpected(StoreError::kNoSpace);
    }
    auto rotated = RotateSegmentIfNeeded(record_bytes);
    if (!rotated) return tl::unexpected(rotated.error());

    const uint64_t sequence = next_sequence_++;
    auto appended = active_segment_->Append(identity, value, RecordKind::kValue,
                                            sequence, config_.sync_data);
    if (!appended) return tl::unexpected(StoreError::kIoError);
    auto& segment = segments_.at(active_segment_->segment_id());
    segment.valid_bytes = active_segment_->tail();
    ++segment.record_count;
    ++segment.mutation_epoch;
    WalRecord transition{.type = WalRecordType::kPrepareValue,
                         .sequence = sequence,
                         .identity = identity,
                         .physical = appended.value()};
    auto wal_result = wal_->Append(transition, config_.sync_wal);
    if (!wal_result) return tl::unexpected(StoreError::kIoError);
    auto prepared = index_.Prepare(identity, appended.value(), sequence);
    if (!prepared) return tl::unexpected(MapIndexError(prepared.error()));
    auto live = AddLiveRecordLocked(appended.value());
    if (!live) return tl::unexpected(live.error());
    return PreparedWrite{.identity = identity,
                         .sequence = sequence,
                         .physical = appended.value()};
}

tl::expected<void, StoreError> LogStructuredStore::CommitPut(
    const RecordIdentity& identity, uint64_t sequence) {
    std::lock_guard lock(mutex_);
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
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
    auto previous_current =
        index_.LookupCurrent(identity.tenant_id, identity.object_key);
    auto persisted = wal_->Append(WalRecord{.type = WalRecordType::kCommitValue,
                                            .sequence = sequence,
                                            .identity = identity,
                                            .physical = {}},
                                  config_.sync_wal);
    if (!persisted) return tl::unexpected(StoreError::kIoError);
    auto committed = index_.Commit(identity, sequence);
    if (!committed) return tl::unexpected(MapIndexError(committed.error()));
    if (previous_current && previous_current->identity != identity) {
        auto removed =
            RemoveLiveRecordLocked(previous_current->version.physical);
        if (!removed) return tl::unexpected(removed.error());
    }
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::CommitPuts(
    const std::vector<PreparedWrite>& writes) {
    std::lock_guard lock(mutex_);
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
    if (writes.empty()) return {};

    std::vector<WalRecord> records;
    records.reserve(writes.size());
    for (const auto& write : writes) {
        auto current = index_.Lookup(write.identity);
        if (!current || current->state != VersionState::kPrepared ||
            current->sequence != write.sequence) {
            return tl::unexpected(current ? StoreError::kInvalidTransition
                                          : StoreError::kNotFound);
        }
        records.push_back({.type = WalRecordType::kCommitValue,
                           .sequence = write.sequence,
                           .identity = write.identity,
                           .physical = {}});
    }
    if (!wal_->AppendBatch(records, config_.sync_wal)) {
        return tl::unexpected(StoreError::kIoError);
    }
    for (const auto& write : writes) {
        auto previous_current = index_.LookupCurrent(write.identity.tenant_id,
                                                     write.identity.object_key);
        auto committed = index_.Commit(write.identity, write.sequence);
        if (!committed) {
            recovery_required_ = true;
            return tl::unexpected(StoreError::kRecoveryRequired);
        }
        if (previous_current && previous_current->identity != write.identity) {
            auto removed =
                RemoveLiveRecordLocked(previous_current->version.physical);
            if (!removed) return tl::unexpected(removed.error());
        }
    }
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::AbortPut(
    const RecordIdentity& identity, uint64_t sequence) {
    std::lock_guard lock(mutex_);
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
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
    auto removed = RemoveLiveRecordLocked(current->physical);
    if (!removed) return tl::unexpected(removed.error());
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::AbortPuts(
    const std::vector<PreparedWrite>& writes) {
    std::lock_guard lock(mutex_);
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
    if (writes.empty()) return {};

    std::vector<WalRecord> records;
    records.reserve(writes.size());
    for (const auto& write : writes) {
        auto current = index_.Lookup(write.identity);
        if (!current || current->state != VersionState::kPrepared ||
            current->sequence != write.sequence) {
            return tl::unexpected(current ? StoreError::kInvalidTransition
                                          : StoreError::kNotFound);
        }
        records.push_back({.type = WalRecordType::kAbortValue,
                           .sequence = write.sequence,
                           .identity = write.identity,
                           .physical = {}});
    }
    if (!wal_->AppendBatch(records, config_.sync_wal)) {
        return tl::unexpected(StoreError::kIoError);
    }
    for (const auto& write : writes) {
        auto current = index_.Lookup(write.identity);
        auto aborted = index_.Abort(write.identity, write.sequence);
        if (!aborted) {
            recovery_required_ = true;
            return tl::unexpected(StoreError::kRecoveryRequired);
        }
        auto removed = RemoveLiveRecordLocked(current->physical);
        if (!removed) return tl::unexpected(removed.error());
    }
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::Delete(
    const RecordIdentity& identity) {
    std::lock_guard lock(mutex_);
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
    if (identity.tenant_id.size() > kMaxTenantLength ||
        identity.object_key.size() > kMaxKeyLength) {
        return tl::unexpected(StoreError::kInvalidArgument);
    }
    const auto existing = index_.Lookup(identity);
    const uint64_t sequence = next_sequence_++;
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
    if (existing && (existing->state == VersionState::kPrepared ||
                     existing->state == VersionState::kCommitted)) {
        auto removed = RemoveLiveRecordLocked(existing->physical);
        if (!removed) return tl::unexpected(removed.error());
    }
    applied_delete_watermark_ = sequence;
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::Sync() {
    std::lock_guard lock(mutex_);
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
    if (!active_segment_->Sync() || !wal_->Sync()) {
        return tl::unexpected(StoreError::kIoError);
    }
    return {};
}

tl::expected<void, StoreError> LogStructuredStore::SealActiveSegment() {
    std::lock_guard lock(mutex_);
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
    if (active_segment_->tail() == 0) return {};
    return RotateActiveSegmentLocked();
}

tl::expected<void, StoreError> LogStructuredStore::Checkpoint() {
    std::lock_guard lock(mutex_);
    return CheckpointLocked();
}

tl::expected<void, StoreError> LogStructuredStore::CheckpointLocked() {
    if (recovery_required_) {
        return tl::unexpected(StoreError::kRecoveryRequired);
    }
    if (!active_segment_->Sync() || !wal_->Sync()) {
        return tl::unexpected(StoreError::kIoError);
    }
    const uint64_t generation = manifest_generation_ + 1;
    const uint64_t checkpoint_sequence = next_sequence_ - 1;
    std::vector<SegmentMetadata> segment_snapshot;
    segment_snapshot.reserve(segments_.size());
    for (const auto& [segment_id, segment] : segments_) {
        static_cast<void>(segment_id);
        if (segment.state == SegmentLifecycle::kCreating) {
            return tl::unexpected(StoreError::kInvalidTransition);
        }
        auto stable_segment = segment;
        if (stable_segment.state == SegmentLifecycle::kSealing ||
            stable_segment.state == SegmentLifecycle::kCompacting) {
            stable_segment.state = SegmentLifecycle::kSealed;
        }
        segment_snapshot.push_back(std::move(stable_segment));
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
    if (HitCompactionCrashPoint(CompactionCrashPoint::kBeforeManifestWrite)) {
        return tl::unexpected(StoreError::kIoError);
    }
    auto published = PublishManifest(
        config_.root_path, manifest,
        [] {
            return HitCompactionCrashPoint(
                CompactionCrashPoint::kAfterManifestWrite);
        },
        [] {
            return HitCompactionCrashPoint(
                CompactionCrashPoint::kAfterCurrentRenameBeforeDirectorySync);
        });
    if (!published) {
        if (published.error() == MetadataError::kPublicationUncertain) {
            recovery_required_ = true;
            return tl::unexpected(StoreError::kRecoveryRequired);
        }
        static_cast<void>(RemoveFileDurably(config_.root_path, next_wal_file));
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
    static_cast<void>(
        CleanupMetadataArtifacts(config_.root_path, generation, next_wal_file));
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
            ForgetSegmentReaderLocked(it->first);
        }
        it = segments_.erase(it);
    }
}

tl::expected<CompactionResult, StoreError> LogStructuredStore::CompactOnce(
    const CompactionOptions& options) {
    if (options.max_source_segments == 0 || options.max_input_bytes == 0 ||
        options.max_target_bytes == 0 || options.fanout == 0 ||
        options.max_levels == 0 || options.min_reclaim_ratio < 0.0 ||
        options.min_reclaim_ratio > 1.0) {
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
        if (recovery_required_) {
            return tl::unexpected(StoreError::kRecoveryRequired);
        }
        CleanupRetiredSegmentsLocked();
        std::unordered_set<uint64_t> prepared_segments;
        for (const auto& entry : index_.Snapshot()) {
            if (entry.version.state == VersionState::kPrepared &&
                entry.version.physical.total_length != 0) {
                prepared_segments.insert(entry.version.physical.segment_id);
            }
        }
        std::vector<SegmentMetadata> sealed;
        for (const auto& [segment_id, segment] : segments_) {
            static_cast<void>(segment_id);
            if (segment.state != SegmentLifecycle::kSealed ||
                segment.valid_bytes == 0 ||
                segment.live_bytes > segment.valid_bytes ||
                prepared_segments.contains(segment.segment_id)) {
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
        const uint64_t physical_bytes = PhysicalBytesLocked();
        const uint64_t available_total_bytes =
            physical_bytes < config_.max_total_physical_bytes
                ? config_.max_total_physical_bytes - physical_bytes
                : 0;
        const uint64_t temporary_budget =
            std::min(options.max_temporary_bytes, available_total_bytes);
        std::vector<SegmentMetadata> bounded_sources;
        bounded_sources.reserve(
            std::min(sources.size(), options.max_source_segments));
        uint64_t selected_bytes = 0;
        uint64_t selected_live_bytes = 0;
        for (const auto& source : sources) {
            if (bounded_sources.size() == options.max_source_segments) break;
            if (!bounded_sources.empty() &&
                (selected_bytes >= options.max_input_bytes ||
                 source.valid_bytes >
                     options.max_input_bytes - selected_bytes)) {
                break;
            }
            if (selected_live_bytes > temporary_budget ||
                source.live_bytes > temporary_budget - selected_live_bytes) {
                continue;
            }
            bounded_sources.push_back(source);
            selected_bytes += source.valid_bytes;
            selected_live_bytes += source.live_bytes;
            if (selected_bytes >= options.max_input_bytes) break;
        }
        sources = std::move(bounded_sources);
        if (sources.empty()) return CompactionResult{};

        std::unordered_set<uint64_t> source_ids;
        for (const auto& source : sources) {
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
        }
        compaction_reserved_bytes_ = selected_live_bytes;
        for (auto& source : sources) {
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

    const ScopeExit release_reservation([this] {
        std::lock_guard lock(mutex_);
        compaction_reserved_bytes_ = 0;
    });

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
    if (HitCompactionCrashPoint(CompactionCrashPoint::kBeforeTargetSync)) {
        return tl::unexpected(StoreError::kIoError);
    }
    if (writer && !writer->Sync()) {
        cleanup_targets();
        reset_sources();
        return tl::unexpected(StoreError::kIoError);
    }
    if (HitCompactionCrashPoint(CompactionCrashPoint::kAfterTargetSync)) {
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
    if (HitCompactionCrashPoint(CompactionCrashPoint::kAfterTargetRename)) {
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
                    current->second.live_bytes = source.live_bytes;
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
            current.live_bytes = 0;
            ++current.mutation_epoch;
        }
        auto checkpointed = CheckpointLocked();
        if (!checkpointed) {
            if (checkpointed.error() == StoreError::kRecoveryRequired) {
                return tl::unexpected(StoreError::kRecoveryRequired);
            }
            static_cast<void>(index_.Restore(index_before));
            for (const auto& target : targets) {
                segment_paths_.erase(target.segment_id);
                ForgetSegmentReaderLocked(target.segment_id);
                segments_.erase(target.segment_id);
            }
            for (const auto& source : sources) {
                auto current = segments_.find(source.segment_id);
                if (current != segments_.end()) {
                    current->second.state = SegmentLifecycle::kSealed;
                    current->second.live_bytes = source.live_bytes;
                    ++current->second.mutation_epoch;
                }
            }
            cleanup_targets();
            return tl::unexpected(checkpointed.error());
        }
        if (HitCompactionCrashPoint(
                CompactionCrashPoint::kAfterManifestPublication)) {
            return tl::unexpected(StoreError::kIoError);
        }
        CleanupRetiredSegmentsLocked();
        if (HitCompactionCrashPoint(CompactionCrashPoint::kAfterSourceUnlink)) {
            return tl::unexpected(StoreError::kIoError);
        }
    }

    uint64_t output_bytes = 0;
    for (const auto& target : targets) output_bytes += target.valid_bytes;
    return CompactionResult{.source_segments = sources.size(),
                            .target_segments = targets.size(),
                            .input_bytes = input_bytes,
                            .output_bytes = output_bytes,
                            .reclaimed_bytes = input_bytes - output_bytes};
}

tl::expected<std::shared_ptr<SegmentReader>, StoreError>
LogStructuredStore::GetSegmentReaderLocked(uint64_t segment_id) const {
    auto cached = segment_readers_.find(segment_id);
    if (cached != segment_readers_.end()) {
        auto reader = cached->second.lock();
        if (reader) return reader;
    }
    auto path = segment_paths_.find(segment_id);
    if (path == segment_paths_.end()) {
        return tl::unexpected(StoreError::kCorruptData);
    }
    auto opened = SegmentReader::Open(path->second, segment_id);
    if (!opened) return tl::unexpected(StoreError::kIoError);
    constexpr size_t kReaderCacheCapacity = 128;
    if (reader_cache_.size() >= kReaderCacheCapacity) {
        reader_cache_.pop_front();
    }
    segment_readers_[segment_id] = *opened;
    reader_cache_.emplace_back(segment_id, *opened);
    return *opened;
}

void LogStructuredStore::ForgetSegmentReaderLocked(uint64_t segment_id) {
    segment_readers_.erase(segment_id);
    std::erase_if(reader_cache_, [segment_id](const auto& cached) {
        return cached.first == segment_id;
    });
}

tl::expected<LogStructuredStore::PinnedEntry, StoreError>
LogStructuredStore::PinEntryLocked(IndexSnapshotEntry entry) const {
    auto reader = GetSegmentReaderLocked(entry.version.physical.segment_id);
    if (!reader) return tl::unexpected(reader.error());
    return PinnedEntry{.entry = std::move(entry), .reader = std::move(*reader)};
}

tl::expected<void, StoreError> LogStructuredStore::ReadPinnedEntryInto(
    const PinnedEntry& pinned, char* value, size_t value_size) {
    auto read = pinned.reader->ReadValue(pinned.entry.version.physical, value,
                                         value_size);
    if (!read) {
        return tl::unexpected(read.error() == SegmentError::kIoError
                                  ? StoreError::kIoError
                                  : StoreError::kCorruptData);
    }
    return {};
}

tl::expected<std::string, StoreError> LogStructuredStore::ReadPinnedEntry(
    const PinnedEntry& pinned) {
    const uint64_t value_length = pinned.entry.version.physical.value_length;
    if (value_length >
        static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        return tl::unexpected(StoreError::kCorruptData);
    }
    std::string value(static_cast<size_t>(value_length), '\0');
    auto read = ReadPinnedEntryInto(pinned, value.data(), value.size());
    if (!read) return tl::unexpected(read.error());
    return value;
}

tl::expected<std::string, StoreError> LogStructuredStore::Get(
    const RecordIdentity& identity) const {
    auto pinned = [&]() -> tl::expected<PinnedEntry, StoreError> {
        std::lock_guard lock(mutex_);
        if (recovery_required_) {
            return tl::unexpected(StoreError::kRecoveryRequired);
        }
        auto version = index_.LookupCommitted(identity);
        if (!version) return tl::unexpected(StoreError::kNotFound);
        return PinEntryLocked(
            IndexSnapshotEntry{.identity = identity, .version = *version});
    }();
    if (!pinned) return tl::unexpected(pinned.error());
    return ReadPinnedEntry(*pinned);
}

tl::expected<std::string, StoreError> LogStructuredStore::GetLatest(
    std::string_view tenant_id, std::string_view object_key) const {
    auto pinned = [&]() -> tl::expected<PinnedEntry, StoreError> {
        std::lock_guard lock(mutex_);
        if (recovery_required_) {
            return tl::unexpected(StoreError::kRecoveryRequired);
        }
        auto entry = index_.LookupCurrent(tenant_id, object_key);
        if (!entry) return tl::unexpected(StoreError::kNotFound);
        return PinEntryLocked(std::move(*entry));
    }();
    if (!pinned) return tl::unexpected(pinned.error());
    return ReadPinnedEntry(*pinned);
}

tl::expected<void, StoreError> LogStructuredStore::GetLatestInto(
    std::string_view tenant_id, std::string_view object_key, char* value,
    size_t value_size) const {
    auto pinned = [&]() -> tl::expected<PinnedEntry, StoreError> {
        std::lock_guard lock(mutex_);
        if (recovery_required_) {
            return tl::unexpected(StoreError::kRecoveryRequired);
        }
        auto entry = index_.LookupCurrent(tenant_id, object_key);
        if (!entry) return tl::unexpected(StoreError::kNotFound);
        return PinEntryLocked(std::move(*entry));
    }();
    if (!pinned) return tl::unexpected(pinned.error());
    return ReadPinnedEntryInto(*pinned, value, value_size);
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
    stats.wal_sequence = next_sequence_ - 1;
    stats.checkpoint_sequence = checkpoint_sequence_;
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
