#include "ha/snapshot/batch_oplog/batch_oplog_pruning_coordinator.h"

#include <charconv>
#include <utility>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"

namespace mooncake {
namespace {

bool InspectArtifact(SnapshotObjectStore& store, const std::string& key,
                     uint64_t size, uint32_t crc) {
    const auto inspection = store.InspectObject(key);
    // A successful inspection with no checksum means the backend does not
    // supply one. N03's full pre-publication readback remains the content
    // evidence; do not download all chunks again here. Unknown metadata is an
    // inspection error and fails closed.
    return size != 0 && inspection && inspection->stored_size == size &&
           (!inspection->crc32c || *inspection->crc32c == crc);
}

bool ValidateSnapshot(SnapshotObjectStore& store, const std::string& root,
                      const std::string& pointer,
                      const ha::BatchOpLogSnapshotDescriptor& descriptor,
                      const std::function<bool()>& cancelled) {
    if (descriptor.manifest_key !=
        ha::BuildBatchOpLogSnapshotManifestKey(root, descriptor.snapshot_id))
        return false;
    std::string bytes;
    const auto descriptor_key =
        ha::BuildBatchOpLogSnapshotDescriptorKey(root, descriptor.snapshot_id);
    if (descriptor_key.empty() ||
        !store.DownloadString(descriptor_key, bytes) || bytes != pointer)
        return false;
    if (!store.DownloadString(descriptor.manifest_key, bytes) ||
        bytes.size() != descriptor.manifest_size ||
        Crc32cValue(bytes.data(), bytes.size()) != descriptor.manifest_crc32c)
        return false;
    const auto manifest = ha::DecodeBatchOpLogSnapshotManifest(bytes);
    if (!manifest || manifest->schema_version != descriptor.schema_version ||
        manifest->snapshot_format != descriptor.snapshot_format ||
        manifest->snapshot_id != descriptor.snapshot_id ||
        manifest->last_included_batch_id != descriptor.last_included_batch_id ||
        manifest->last_included_seq != descriptor.last_included_seq ||
        manifest->producer_view_version != descriptor.producer_view_version ||
        manifest->segments.key != ha::BuildBatchOpLogSnapshotSegmentsKey(
                                      root, descriptor.snapshot_id) ||
        !InspectArtifact(store, manifest->segments.key,
                         manifest->segments.stored_size,
                         manifest->segments.crc32c))
        return false;
    for (size_t i = 0; i < manifest->object_chunks.size(); ++i) {
        if (cancelled()) return false;
        const auto& chunk = manifest->object_chunks[i];
        if (chunk.chunk_index != i ||
            chunk.key != ha::BuildBatchOpLogSnapshotObjectChunkKey(
                             root, descriptor.snapshot_id, i) ||
            !InspectArtifact(store, chunk.key, chunk.stored_size, chunk.crc32c))
            return false;
    }
    return true;
}

}  // namespace

BatchOpLogPruningCoordinator::BatchOpLogPruningCoordinator(
    HaKvBackend& backend, SnapshotObjectStore& object_store,
    std::string cluster_id, std::string snapshot_root)
    : backend_(backend),
      object_store_(object_store),
      cluster_id_(std::move(cluster_id)),
      snapshot_root_(std::move(snapshot_root)) {}

ErrorCode BatchOpLogPruningCoordinator::Run(
    const SnapshotMaintenanceLease& lease,
    const std::function<bool()>& cancelled) {
    const auto latest_key = ha::BuildBatchOpLogSnapshotLatestKey(cluster_id_);
    const auto fallback_key =
        ha::BuildBatchOpLogSnapshotFallbackKey(cluster_id_);
    const auto floor_key =
        ha::BuildBatchOpLogSnapshotCompactionFloorKey(cluster_id_);
    const auto lock_key =
        ha::BuildBatchOpLogSnapshotMaintenanceKey(cluster_id_);
    if (latest_key.empty() || snapshot_root_.empty() ||
        lease.lock_key() != lock_key || !backend_.SupportsTxn())
        return ErrorCode::INVALID_PARAMS;
    const auto stopped = [&] {
        return !lease.IsHeld() || (cancelled && cancelled());
    };
    if (stopped()) return ErrorCode::ETCD_TRANSACTION_FAIL;

    std::string latest, fallback, old_floor;
    auto error = backend_.Get(latest_key, latest);
    if (error != ErrorCode::OK) return error;
    error = backend_.Get(fallback_key, fallback);
    // The first publication has no redundant recovery baseline yet.
    if (error == ErrorCode::ETCD_KEY_NOT_EXIST) return ErrorCode::OK;
    if (error != ErrorCode::OK) return error;
    const auto latest_descriptor =
        ha::DecodeBatchOpLogSnapshotDescriptor(latest);
    const auto fallback_descriptor =
        ha::DecodeBatchOpLogSnapshotDescriptor(fallback);
    if (!latest_descriptor || !fallback_descriptor)
        return ErrorCode::INTERNAL_ERROR;
    const uint64_t candidate = fallback_descriptor->last_included_batch_id;
    if (latest_descriptor->snapshot_id == fallback_descriptor->snapshot_id ||
        latest_descriptor->last_included_batch_id <= candidate)
        return ErrorCode::OK;

    error = backend_.Get(floor_key, old_floor);
    const bool floor_exists = error == ErrorCode::OK;
    if (!floor_exists && error != ErrorCode::ETCD_KEY_NOT_EXIST) return error;
    if (floor_exists) {
        uint64_t floor = 0;
        const auto parsed = std::from_chars(
            old_floor.data(), old_floor.data() + old_floor.size(), floor);
        if (parsed.ec != std::errc() ||
            parsed.ptr != old_floor.data() + old_floor.size())
            return ErrorCode::INTERNAL_ERROR;
        if (candidate < floor) return ErrorCode::OK;
    }

    if (!ValidateSnapshot(object_store_, snapshot_root_, latest,
                          *latest_descriptor, stopped) ||
        !ValidateSnapshot(object_store_, snapshot_root_, fallback,
                          *fallback_descriptor, stopped))
        return ErrorCode::INTERNAL_ERROR;
    if (stopped()) return ErrorCode::ETCD_TRANSACTION_FAIL;

    KvTxn txn;
    txn.compares = {{.key = lock_key,
                     .kind = KvCompareKind::kValueEquals,
                     .expected_value = lease.owner_token()},
                    {.key = lock_key,
                     .kind = KvCompareKind::kCreateRevisionEquals,
                     .expected_value = "",
                     .expected_revision = lease.lock_create_revision()},
                    {.key = latest_key,
                     .kind = KvCompareKind::kValueEquals,
                     .expected_value = latest},
                    {.key = fallback_key,
                     .kind = KvCompareKind::kValueEquals,
                     .expected_value = fallback},
                    {.key = floor_key,
                     .kind = floor_exists ? KvCompareKind::kValueEquals
                                          : KvCompareKind::kKeyNotExists,
                     .expected_value = old_floor}};
    txn.puts.push_back({.key = floor_key, .value = std::to_string(candidate)});
    error = backend_.Txn(txn);
    if (error != ErrorCode::OK) return error;
    // Even if the lease is lost now, this range has already been advertised to
    // readers. A crash or failed delete leaves the floor in place; a later
    // successful publication can safely repeat this bounded deletion.
    if (stopped()) return ErrorCode::OK;
    return OpLogBatchStorage(cluster_id_, backend_)
        .DeleteBatchesThrough(candidate);
}

}  // namespace mooncake
