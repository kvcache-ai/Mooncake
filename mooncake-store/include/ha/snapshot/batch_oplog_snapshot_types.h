#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "types.h"

namespace mooncake::ha {

inline constexpr uint32_t kBatchOpLogSnapshotSchemaVersion = 1;
inline constexpr char kBatchOpLogSnapshotFormat[] =
    "standby-oplog-materialized/v1";

struct BatchOpLogSnapshotDescriptor {
    uint32_t schema_version{kBatchOpLogSnapshotSchemaVersion};
    std::string snapshot_format{kBatchOpLogSnapshotFormat};
    std::string snapshot_id;
    uint64_t last_included_seq{0};
    uint64_t last_included_batch_id{0};
    ViewVersionId producer_view_version{0};
    std::string manifest_key;
    uint64_t manifest_size{0};
    uint32_t manifest_crc32c{0};
    int64_t created_at_ms{0};
};

struct BatchOpLogSnapshotObjectDescriptor {
    std::string key;
    uint64_t stored_size{0};
    uint32_t crc32c{0};
};

struct BatchOpLogSnapshotChunkDescriptor {
    uint64_t chunk_index{0};
    std::string key;
    uint64_t object_count{0};
    uint64_t stored_size{0};
    uint32_t crc32c{0};
};

struct BatchOpLogSnapshotManifest {
    uint32_t schema_version{kBatchOpLogSnapshotSchemaVersion};
    std::string snapshot_format{kBatchOpLogSnapshotFormat};
    std::string snapshot_id;
    uint64_t last_included_seq{0};
    uint64_t last_included_batch_id{0};
    ViewVersionId producer_view_version{0};
    BatchOpLogSnapshotObjectDescriptor segments;
    std::vector<BatchOpLogSnapshotChunkDescriptor> object_chunks;
};

std::string EncodeBatchOpLogSnapshotDescriptor(
    const BatchOpLogSnapshotDescriptor& descriptor);
bool DecodeBatchOpLogSnapshotDescriptor(
    std::string_view value, BatchOpLogSnapshotDescriptor* descriptor,
    std::string* reason = nullptr);

std::string EncodeBatchOpLogSnapshotManifest(
    const BatchOpLogSnapshotManifest& manifest);
bool DecodeBatchOpLogSnapshotManifest(std::string_view value,
                                      BatchOpLogSnapshotManifest* manifest,
                                      std::string* reason = nullptr);

std::string BuildBatchOpLogSnapshotId(uint64_t last_included_batch_id,
                                      int64_t maintenance_lease_id);

std::string BuildBatchOpLogSnapshotMaintenanceKey(
    const std::string& cluster_id);
std::string BuildBatchOpLogSnapshotLatestKey(const std::string& cluster_id);
std::string BuildBatchOpLogSnapshotFallbackKey(const std::string& cluster_id);
std::string BuildBatchOpLogSnapshotCompactionFloorKey(
    const std::string& cluster_id);

std::string BuildBatchOpLogSnapshotDescriptorKey(
    const std::string& snapshot_root, std::string_view snapshot_id);
std::string BuildBatchOpLogSnapshotManifestKey(const std::string& snapshot_root,
                                               std::string_view snapshot_id);
std::string BuildBatchOpLogSnapshotSegmentsKey(const std::string& snapshot_root,
                                               std::string_view snapshot_id);
std::string BuildBatchOpLogSnapshotObjectChunkKey(
    const std::string& snapshot_root, std::string_view snapshot_id,
    uint64_t chunk_index);

}  // namespace mooncake::ha
