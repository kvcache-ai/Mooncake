#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "storage/local/log_structured/index.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake::logstructured {

enum class SegmentLifecycle : uint8_t {
    kCreating,
    kActive,
    kSealing,
    kSealed,
    kCompacting,
    kRetired,
};

struct SegmentMetadata {
    uint64_t segment_id{0};
    uint32_t level{0};
    SegmentLifecycle state{SegmentLifecycle::kCreating};
    uint64_t valid_bytes{0};
    uint64_t live_bytes{0};
    uint64_t record_count{0};
    uint64_t mutation_epoch{0};

    bool operator==(const SegmentMetadata&) const = default;
};

struct CheckpointState {
    uint32_t format_version{1};
    uint64_t checkpoint_sequence{0};
    uint64_t next_sequence{1};
    uint64_t next_segment_id{1};
    uint64_t applied_delete_watermark{0};
    std::vector<IndexSnapshotEntry> index;
    std::vector<SegmentMetadata> segments;
};

struct ManifestState {
    uint32_t format_version{1};
    uint64_t generation{0};
    uint64_t checkpoint_sequence{0};
    uint64_t next_sequence{1};
    uint64_t next_segment_id{1};
    uint64_t active_segment_id{0};
    std::string checkpoint_file;
    std::string wal_file;
    std::vector<SegmentMetadata> segments;
};

enum class MetadataError {
    kInvalidArgument,
    kIoError,
    kCorruptData,
    kNotFound,
};

tl::expected<std::string, MetadataError> WriteCheckpoint(
    const std::string& root_path, uint64_t generation,
    const CheckpointState& checkpoint);
tl::expected<CheckpointState, MetadataError> LoadCheckpoint(
    const std::string& root_path, const std::string& checkpoint_file);
tl::expected<void, MetadataError> PublishManifest(
    const std::string& root_path, const ManifestState& manifest);
tl::expected<ManifestState, MetadataError> LoadCurrentManifest(
    const std::string& root_path);
tl::expected<void, MetadataError> RemoveFileDurably(
    const std::string& root_path, const std::string& filename);

}  // namespace mooncake::logstructured
