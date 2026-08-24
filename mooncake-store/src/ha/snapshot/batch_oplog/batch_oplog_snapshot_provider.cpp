#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_provider.h"

#include <algorithm>
#include <memory>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_standby_reader.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/snapshot/batch_oplog/codec.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"

namespace mooncake {
namespace {

enum class AttemptDisposition { kSuccess, kInvalid, kInfrastructure };

struct AttemptResult {
    AttemptDisposition disposition{AttemptDisposition::kInvalid};
    ErrorCode error{ErrorCode::DESERIALIZE_FAIL};
    BatchOpLogSnapshotRestoreResult value;
};

AttemptResult Invalid(ErrorCode error = ErrorCode::DESERIALIZE_FAIL) {
    return {AttemptDisposition::kInvalid, error, {}};
}

AttemptResult Infrastructure(
    ErrorCode error = ErrorCode::ETCD_OPERATION_ERROR) {
    return {AttemptDisposition::kInfrastructure, error, {}};
}

bool IsRetryableBackendError(ErrorCode error) {
    return error == ErrorCode::ETCD_OPERATION_ERROR ||
           error == ErrorCode::ETCD_CTX_CANCELLED;
}

bool SameIdentity(const ha::BatchOpLogSnapshotDescriptor& descriptor,
                  const ha::BatchOpLogSnapshotManifest& manifest) {
    return descriptor.schema_version == manifest.schema_version &&
           descriptor.snapshot_format == manifest.snapshot_format &&
           descriptor.snapshot_id == manifest.snapshot_id &&
           descriptor.last_included_seq == manifest.last_included_seq &&
           descriptor.last_included_batch_id ==
               manifest.last_included_batch_id &&
           descriptor.producer_view_version == manifest.producer_view_version;
}

AttemptResult ReadVerifiedObject(SnapshotObjectStore& object_store,
                                 const std::string& key, uint64_t expected_size,
                                 uint32_t expected_crc,
                                 std::vector<uint8_t>& bytes) {
    auto inspection = object_store.InspectObject(key);
    if (!inspection) {
        return object_store.IsNotFoundError(inspection.error())
                   ? Invalid()
                   : Infrastructure();
    }
    if (inspection->stored_size != expected_size || expected_size == 0) {
        return Invalid();
    }
    if (inspection->crc32c && *inspection->crc32c != expected_crc) {
        return Invalid();
    }

    auto download = object_store.DownloadBuffer(key, bytes);
    if (!download) {
        return object_store.IsNotFoundError(download.error())
                   ? Invalid()
                   : Infrastructure();
    }
    if (bytes.size() != expected_size ||
        Crc32cValue(bytes.data(), bytes.size()) != expected_crc) {
        return Invalid();
    }
    return {AttemptDisposition::kSuccess, ErrorCode::OK, {}};
}

AttemptResult ReadDescriptorObject(SnapshotObjectStore& object_store,
                                   const std::string& key,
                                   std::string_view pointer_bytes) {
    std::vector<uint8_t> bytes;
    auto inspection = object_store.InspectObject(key);
    if (!inspection) {
        return object_store.IsNotFoundError(inspection.error())
                   ? Invalid()
                   : Infrastructure();
    }
    if (inspection->stored_size != pointer_bytes.size()) {
        return Invalid();
    }
    auto download = object_store.DownloadBuffer(key, bytes);
    if (!download) {
        return object_store.IsNotFoundError(download.error())
                   ? Invalid()
                   : Infrastructure();
    }
    if (bytes.size() != pointer_bytes.size() ||
        !std::equal(bytes.begin(), bytes.end(), pointer_bytes.begin(),
                    pointer_bytes.end())) {
        return Invalid();
    }
    return {AttemptDisposition::kSuccess, ErrorCode::OK, {}};
}

bool ValidateSegments(const std::vector<StandbySegmentInfo>& segments) {
    std::unordered_set<std::string> endpoints;
    for (const auto& segment : segments) {
        if (segment.transport_endpoint.empty() ||
            !endpoints.insert(segment.transport_endpoint).second) {
            return false;
        }
    }
    return true;
}

void CopyRegistry(const StandbySegmentRegistry& source,
                  StandbySegmentRegistry& destination) {
    destination.Clear();
    for (const auto& segment : source.GetAllSegments()) {
        destination.OnSegmentMount(segment);
    }
}

tl::expected<ReplicaID, ErrorCode> ScanLiveReplicaIds(
    const StandbyMetadataStore& metadata) {
    auto cursor = metadata.BeginSnapshotTraversal();
    ReplicaID max_replica_id = 0;
    while (!cursor.done()) {
        std::vector<StandbyObjectEntry> objects;
        if (!metadata.CopyNextSnapshotChunk(1, cursor, objects)) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        for (const auto& object : objects) {
            std::unordered_set<ReplicaID> object_ids;
            for (const auto& replica : object.metadata.replicas) {
                if (replica.id == 0 || !object_ids.insert(replica.id).second) {
                    return tl::make_unexpected(ErrorCode::DESERIALIZE_FAIL);
                }
                max_replica_id = std::max(max_replica_id, replica.id);
            }
        }
    }
    return max_replica_id;
}

AttemptResult ReplaySuffix(const std::string& cluster_id, HaKvBackend& backend,
                           OpLogApplier& applier,
                           const DurablePrefix* baseline) {
    OpLogBatchStandbyReader reader(cluster_id, backend, applier);
    const DurablePrefix start =
        baseline == nullptr ? DurablePrefix{} : *baseline;
    if (reader.SetBaselineCursor(start) != ErrorCode::OK) {
        return Invalid(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
    }

    for (;;) {
        auto poll = reader.PollOnce();
        if (poll.error != ErrorCode::OK) {
            return IsRetryableBackendError(poll.error)
                       ? Infrastructure(poll.error)
                       : Invalid(poll.error);
        }
        if (!poll.durable_prefix_present) {
            // A missing prefix cannot prove complete history, even for an
            // empty cluster.
            return Invalid(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
        }
        const uint64_t applied = applier.GetExpectedSequenceId() - 1;
        if (applied == poll.durable_prefix.last_seq) {
            return {AttemptDisposition::kSuccess,
                    ErrorCode::OK,
                    {.last_included_seq = poll.durable_prefix.last_seq,
                     .last_included_batch_id = poll.durable_prefix.batch_id}};
        }
        if (poll.applied_entries == 0) {
            return Invalid(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
        }
    }
}

AttemptResult RestorePointer(
    const std::string& cluster_id, HaKvBackend& backend,
    SnapshotObjectStore& object_store, const std::string& snapshot_root,
    std::string_view pointer_bytes, StandbyMetadataStore& metadata,
    StandbySegmentRegistry& registry, OpLogApplier* supplied_applier) {
    auto decoded_descriptor =
        ha::DecodeBatchOpLogSnapshotDescriptor(pointer_bytes);
    if (!decoded_descriptor) {
        return Invalid();
    }
    const auto& descriptor = *decoded_descriptor;
    const std::string expected_manifest_key =
        ha::BuildBatchOpLogSnapshotManifestKey(snapshot_root,
                                               descriptor.snapshot_id);
    if (descriptor.manifest_key != expected_manifest_key) {
        return Invalid();
    }
    const std::string descriptor_key = ha::BuildBatchOpLogSnapshotDescriptorKey(
        snapshot_root, descriptor.snapshot_id);
    if (descriptor_key.empty()) {
        return Invalid();
    }

    auto read_descriptor =
        ReadDescriptorObject(object_store, descriptor_key, pointer_bytes);
    if (read_descriptor.disposition != AttemptDisposition::kSuccess) {
        return read_descriptor;
    }

    std::vector<uint8_t> manifest_bytes;
    auto read_manifest = ReadVerifiedObject(
        object_store, descriptor.manifest_key, descriptor.manifest_size,
        descriptor.manifest_crc32c, manifest_bytes);
    if (read_manifest.disposition != AttemptDisposition::kSuccess) {
        return read_manifest;
    }
    auto decoded_manifest = ha::DecodeBatchOpLogSnapshotManifest(
        std::string_view(reinterpret_cast<const char*>(manifest_bytes.data()),
                         manifest_bytes.size()));
    std::vector<uint8_t>().swap(manifest_bytes);
    if (!decoded_manifest || !SameIdentity(descriptor, *decoded_manifest)) {
        return Invalid();
    }
    const auto& manifest = *decoded_manifest;
    const std::string expected_segments_key =
        ha::BuildBatchOpLogSnapshotSegmentsKey(snapshot_root,
                                               descriptor.snapshot_id);
    if (descriptor.manifest_key != expected_manifest_key ||
        manifest.segments.key != expected_segments_key) {
        return Invalid();
    }

    std::vector<uint8_t> segments_bytes;
    auto read_segments = ReadVerifiedObject(
        object_store, manifest.segments.key, manifest.segments.stored_size,
        manifest.segments.crc32c, segments_bytes);
    if (read_segments.disposition != AttemptDisposition::kSuccess) {
        return read_segments;
    }
    auto decoded_segments = DecodeBatchOpLogSnapshotSegments(segments_bytes);
    std::vector<uint8_t>().swap(segments_bytes);
    if (!decoded_segments || !ValidateSegments(*decoded_segments)) {
        return Invalid();
    }

    std::unique_ptr<OpLogApplier> owned_applier;
    OpLogApplier* applier = supplied_applier;
    if (applier == nullptr) {
        owned_applier = std::make_unique<OpLogApplier>(&metadata, cluster_id);
        applier = owned_applier.get();
    }
    applier->LoadSegmentRegistry(*decoded_segments);
    const DurablePrefix baseline{.batch_id = descriptor.last_included_batch_id,
                                 .last_seq = descriptor.last_included_seq};
    applier->Recover(baseline.last_seq);

    for (size_t i = 0; i < manifest.object_chunks.size(); ++i) {
        const auto& chunk = manifest.object_chunks[i];
        const std::string expected_key =
            ha::BuildBatchOpLogSnapshotObjectChunkKey(
                snapshot_root, descriptor.snapshot_id, i);
        if (chunk.chunk_index != i || chunk.key != expected_key) {
            return Invalid();
        }
        std::vector<uint8_t> chunk_bytes;
        auto read_chunk =
            ReadVerifiedObject(object_store, chunk.key, chunk.stored_size,
                               chunk.crc32c, chunk_bytes);
        if (read_chunk.disposition != AttemptDisposition::kSuccess) {
            return read_chunk;
        }
        auto decoded_chunk = DecodeBatchOpLogSnapshotObjectChunk(
            chunk_bytes, i, chunk.object_count);
        if (!decoded_chunk) {
            return Invalid();
        }
        for (const auto& entry : decoded_chunk->objects) {
            if (entry.key.empty() || !TenantId(entry.tenant_id).IsValid()) {
                return Invalid();
            }
            std::unordered_set<ReplicaID> object_ids;
            for (const auto& replica : entry.metadata.replicas) {
                if (replica.id == 0 || !object_ids.insert(replica.id).second) {
                    return Invalid();
                }
            }
            if (!metadata.RestoreMetadata(entry.tenant_id, entry.key,
                                          entry.metadata)) {
                return Invalid();
            }
        }
    }

    auto suffix = ReplaySuffix(cluster_id, backend, *applier, &baseline);
    if (suffix.disposition != AttemptDisposition::kSuccess) {
        return suffix;
    }
    auto live_max_replica_id = ScanLiveReplicaIds(metadata);
    if (!live_max_replica_id) {
        return Invalid(live_max_replica_id.error());
    }
    CopyRegistry(applier->GetSegmentRegistry(), registry);
    return {AttemptDisposition::kSuccess,
            ErrorCode::OK,
            {.last_included_seq = descriptor.last_included_seq,
             .last_included_batch_id = descriptor.last_included_batch_id,
             .producer_view_version = descriptor.producer_view_version,
             .max_replica_id = *live_max_replica_id}};
}

AttemptResult RestoreCompleteOpLog(const std::string& cluster_id,
                                   HaKvBackend& backend,
                                   StandbyMetadataStore& metadata,
                                   StandbySegmentRegistry& registry,
                                   OpLogApplier* supplied_applier) {
    std::unique_ptr<OpLogApplier> owned_applier;
    OpLogApplier* applier = supplied_applier;
    if (applier == nullptr) {
        owned_applier = std::make_unique<OpLogApplier>(&metadata, cluster_id);
        applier = owned_applier.get();
    }
    applier->Recover(0);
    applier->LoadSegmentRegistry({});
    OpLogBatchStorage storage(cluster_id, backend);
    DurablePrefix durable_prefix;
    const ErrorCode init_error = storage.InitDurablePrefix(durable_prefix);
    if (init_error != ErrorCode::OK) {
        return IsRetryableBackendError(init_error) ? Infrastructure(init_error)
                                                   : Invalid(init_error);
    }
    auto replay = ReplaySuffix(cluster_id, backend, *applier, nullptr);
    if (replay.disposition != AttemptDisposition::kSuccess) {
        return replay;
    }
    auto max_replica_id = ScanLiveReplicaIds(metadata);
    if (!max_replica_id) {
        return Invalid(max_replica_id.error());
    }
    CopyRegistry(applier->GetSegmentRegistry(), registry);
    replay.value.max_replica_id = *max_replica_id;
    return replay;
}

}  // namespace

BatchOpLogSnapshotProvider::BatchOpLogSnapshotProvider(
    std::string cluster_id, HaKvBackend& backend,
    SnapshotObjectStore& object_store, std::string snapshot_root)
    : cluster_id_(std::move(cluster_id)),
      backend_(backend),
      object_store_(object_store),
      snapshot_root_(std::move(snapshot_root)) {}

tl::expected<BatchOpLogSnapshotRestoreResult, ErrorCode>
BatchOpLogSnapshotProvider::RestoreBaseline(StandbyMetadataStore& metadata,
                                            StandbySegmentRegistry& registry,
                                            OpLogApplier* applier) {
    metadata.Clear();
    registry.Clear();
    if (!NormalizeAndValidateClusterId(cluster_id_) || cluster_id_.empty() ||
        snapshot_root_.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (const std::string& pointer_key :
         {ha::BuildBatchOpLogSnapshotLatestKey(cluster_id_),
          ha::BuildBatchOpLogSnapshotFallbackKey(cluster_id_)}) {
        std::string pointer_bytes;
        const ErrorCode pointer_error =
            backend_.Get(pointer_key, pointer_bytes);
        if (pointer_error == ErrorCode::ETCD_KEY_NOT_EXIST) {
            continue;
        }
        if (pointer_error != ErrorCode::OK) {
            return tl::make_unexpected(pointer_error);
        }

        auto attempt =
            RestorePointer(cluster_id_, backend_, object_store_, snapshot_root_,
                           pointer_bytes, metadata, registry, applier);
        if (attempt.disposition == AttemptDisposition::kSuccess) {
            return attempt.value;
        }
        metadata.Clear();
        registry.Clear();
        if (applier != nullptr) {
            applier->Recover(0);
            applier->LoadSegmentRegistry({});
        }
        if (attempt.disposition == AttemptDisposition::kInfrastructure) {
            return tl::make_unexpected(attempt.error);
        }
    }

    auto full_replay = RestoreCompleteOpLog(cluster_id_, backend_, metadata,
                                            registry, applier);
    if (full_replay.disposition != AttemptDisposition::kSuccess) {
        metadata.Clear();
        registry.Clear();
        return tl::make_unexpected(full_replay.error);
    }
    return full_replay.value;
}

}  // namespace mooncake
