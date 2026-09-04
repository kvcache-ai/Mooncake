#pragma once

#include <cstdint>
#include <memory>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/local/log_structured/index.h"
#include "storage/local/log_structured/metadata.h"
#include "storage/local/log_structured/segment.h"
#include "storage/local/log_structured/storage_directory.h"
#include "storage/local/log_structured/wal.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake::logstructured {

enum class StoreError {
    kInvalidArgument,
    kIoError,
    kCorruptData,
    kAlreadyMounted,
    kUnrecognizedFormat,
    kNotFound,
    kInvalidTransition,
};

struct LogStructuredStoreConfig {
    std::string root_path;
    uint64_t max_segment_bytes{256ULL * 1024 * 1024};
    bool sync_data{true};
    bool sync_wal{true};
};

struct PreparedWrite {
    RecordIdentity identity;
    uint64_t sequence{0};
    PhysicalRecord physical;
};

struct CompactionOptions {
    size_t max_source_segments{8};
    uint64_t max_input_bytes{1024ULL * 1024 * 1024};
    uint64_t max_target_bytes{4ULL * 1024 * 1024 * 1024};
    size_t fanout{4};
    uint32_t max_levels{4};
    double min_reclaim_ratio{0.20};
    bool enable_tiering{false};
};

struct CompactionResult {
    size_t source_segments{0};
    size_t target_segments{0};
    uint64_t input_bytes{0};
    uint64_t output_bytes{0};
    uint64_t reclaimed_bytes{0};
};

class LogStructuredStore {
   public:
    static tl::expected<std::unique_ptr<LogStructuredStore>, StoreError> Open(
        LogStructuredStoreConfig config);

    tl::expected<PreparedWrite, StoreError> PreparePut(
        const RecordIdentity& identity, std::string_view value);
    tl::expected<PreparedWrite, StoreError> PreparePut(std::string tenant_id,
                                                       std::string object_key,
                                                       std::string_view value);
    tl::expected<void, StoreError> CommitPut(const RecordIdentity& identity,
                                             uint64_t sequence);
    tl::expected<void, StoreError> AbortPut(const RecordIdentity& identity,
                                            uint64_t sequence);
    tl::expected<void, StoreError> Delete(const RecordIdentity& identity);
    tl::expected<void, StoreError> Sync();
    tl::expected<void, StoreError> SealActiveSegment();
    tl::expected<void, StoreError> Checkpoint();
    tl::expected<CompactionResult, StoreError> CompactOnce(
        const CompactionOptions& options = {});
    tl::expected<std::string, StoreError> Get(
        const RecordIdentity& identity) const;
    tl::expected<std::string, StoreError> GetLatest(
        std::string_view tenant_id, std::string_view object_key) const;
    bool ContainsLatest(std::string_view tenant_id,
                        std::string_view object_key) const;

    std::vector<IndexSnapshotEntry> SnapshotIndex() const;
    std::vector<IndexSnapshotEntry> SnapshotCurrentIndex() const;
    uint64_t active_segment_id() const;
    uint64_t next_sequence() const;

   private:
    explicit LogStructuredStore(LogStructuredStoreConfig config);

    tl::expected<void, StoreError> Recover();
    tl::expected<void, StoreError> RecoverSegments(
        const std::vector<SegmentMetadata>& expected_segments,
        uint64_t active_segment_id);
    tl::expected<void, StoreError> ValidateIndexRecords(
        const std::unordered_map<uint64_t, std::vector<ScannedRecord>>&
            scanned_segments) const;
    void RefreshSegmentLiveBytes();
    tl::expected<void, StoreError> RotateSegmentIfNeeded(
        uint64_t next_record_bytes);
    tl::expected<void, StoreError> RotateActiveSegmentLocked();
    tl::expected<PreparedWrite, StoreError> PreparePutLocked(
        const RecordIdentity& identity, std::string_view value);
    tl::expected<std::string, StoreError> ReadEntryLocked(
        const IndexSnapshotEntry& entry) const;
    tl::expected<void, StoreError> CheckpointLocked();
    void CleanupRetiredSegmentsLocked();
    std::string SegmentPath(uint64_t segment_id) const;

    LogStructuredStoreConfig config_;
    std::string segments_path_;
    std::string wal_path_;
    mutable std::mutex mutex_;
    std::mutex compaction_mutex_;
    std::unique_ptr<StorageDirectory> directory_;
    VersionIndex index_;
    std::unordered_map<uint64_t, std::string> segment_paths_;
    std::map<uint64_t, SegmentMetadata> segments_;
    std::unique_ptr<SegmentWriter> active_segment_;
    std::unique_ptr<WalWriter> wal_;
    uint64_t next_sequence_{1};
    uint64_t next_segment_id_{1};
    uint64_t manifest_generation_{0};
    uint64_t wal_generation_{1};
    uint64_t checkpoint_sequence_{0};
    uint64_t applied_delete_watermark_{0};
};

}  // namespace mooncake::logstructured
