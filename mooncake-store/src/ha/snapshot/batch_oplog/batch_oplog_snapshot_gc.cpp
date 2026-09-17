#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_gc.h"

#include <unordered_set>
#include <utility>
#include <vector>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"

#include "ha_metric_manager.h"

namespace mooncake {
namespace {

bool VerifyObject(SnapshotObjectStore& store, const std::string& key,
                  uint64_t size, uint32_t crc) {
    auto inspection = store.InspectObject(key);
    if (!inspection || inspection->stored_size != size) return false;
    if (inspection->crc32c) return *inspection->crc32c == crc;
    std::vector<uint8_t> bytes;
    return store.DownloadBuffer(key, bytes) && bytes.size() == size &&
           Crc32cValue(bytes.data(), bytes.size()) == crc;
}

}  // namespace

BatchOpLogSnapshotGc::BatchOpLogSnapshotGc(HaKvBackend& backend,
                                           SnapshotObjectStore& store,
                                           std::string cluster_id,
                                           std::string snapshot_root)
    : backend_(backend),
      store_(store),
      cluster_(std::move(cluster_id)),
      root_(std::move(snapshot_root)) {}

ErrorCode BatchOpLogSnapshotGc::Run(
    const SnapshotMaintenanceLease& lease, std::string_view published,
    const std::optional<std::string>& expected_fallback) {
    return Run(lease, published, expected_fallback, {});
}

ErrorCode BatchOpLogSnapshotGc::Run(
    const SnapshotMaintenanceLease& lease, std::string_view published,
    const std::optional<std::string>& expected_fallback,
    const std::function<bool()>& cancelled) {
    HAMetricManager::SnapshotOperationTimer metric_timer(
        HAMetricManager::SnapshotOperation::Gc);
    if (!lease.IsHeld() || (cancelled && cancelled()))
        return ErrorCode::ETCD_TRANSACTION_FAIL;

    std::unordered_set<std::string> protected_prefixes;
    const std::string latest_key =
        ha::BuildBatchOpLogSnapshotLatestKey(cluster_);
    const std::string fallback_key =
        ha::BuildBatchOpLogSnapshotFallbackKey(cluster_);

    // Phase 1: validate pointer state and every referenced artifact.
    for (const auto& key : {latest_key, fallback_key}) {
        std::string pointer;
        const ErrorCode error = backend_.Get(key, pointer);
        const bool latest = key == latest_key;
        if (error == ErrorCode::ETCD_KEY_NOT_EXIST) {
            if (latest || expected_fallback.has_value())
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            continue;
        }
        if (error != ErrorCode::OK) return error;
        if ((latest && pointer != published) ||
            (!latest &&
             (!expected_fallback.has_value() || pointer != *expected_fallback)))
            return ErrorCode::ETCD_TRANSACTION_FAIL;

        auto descriptor = ha::DecodeBatchOpLogSnapshotDescriptor(pointer);
        if (!descriptor) return ErrorCode::INTERNAL_ERROR;
        const std::string prefix = ha::BuildBatchOpLogSnapshotArtifactPrefix(
            root_, descriptor->snapshot_id);
        if (prefix.empty() ||
            descriptor->manifest_key != ha::BuildBatchOpLogSnapshotManifestKey(
                                            root_, descriptor->snapshot_id))
            return ErrorCode::INTERNAL_ERROR;

        std::string manifest_json;
        if (!store_.DownloadString(descriptor->manifest_key, manifest_json) ||
            manifest_json.size() != descriptor->manifest_size ||
            Crc32cValue(manifest_json.data(), manifest_json.size()) !=
                descriptor->manifest_crc32c)
            return ErrorCode::INTERNAL_ERROR;
        auto manifest = ha::DecodeBatchOpLogSnapshotManifest(manifest_json);
        if (!manifest || manifest->snapshot_id != descriptor->snapshot_id ||
            manifest->segments.stored_size == 0 ||
            manifest->segments.key != ha::BuildBatchOpLogSnapshotSegmentsKey(
                                          root_, descriptor->snapshot_id) ||
            !VerifyObject(store_, manifest->segments.key,
                          manifest->segments.stored_size,
                          manifest->segments.crc32c))
            return ErrorCode::INTERNAL_ERROR;
        for (size_t i = 0; i < manifest->object_chunks.size(); ++i) {
            const auto& chunk = manifest->object_chunks[i];
            if (chunk.chunk_index != i || chunk.stored_size == 0 ||
                chunk.key != ha::BuildBatchOpLogSnapshotObjectChunkKey(
                                 root_, descriptor->snapshot_id, i) ||
                !VerifyObject(store_, chunk.key, chunk.stored_size,
                              chunk.crc32c))
                return ErrorCode::INTERNAL_ERROR;
        }
        protected_prefixes.insert(prefix);
    }

    // Phase 2: derive candidate attempt prefixes from the batch-oplog root.
    if (!lease.IsHeld() || (cancelled && cancelled()))
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    const std::string namespace_prefix = root_ + "/batch-oplog/";
    std::vector<std::string> objects;
    if (!store_.ListObjectsWithPrefix(namespace_prefix, objects))
        return ErrorCode::INTERNAL_ERROR;
    std::unordered_set<std::string> candidates;
    for (const auto& object : objects) {
        if (!object.starts_with(namespace_prefix)) continue;
        const size_t end = object.find('/', namespace_prefix.size());
        if (end != std::string::npos)
            candidates.insert(object.substr(0, end + 1));
    }

    uint64_t orphan_prefixes = 0;
    for (const auto& prefix : candidates)
        orphan_prefixes += !protected_prefixes.contains(prefix);
    HAMetricManager::instance().update_snapshot_runtime([&](auto& metrics) {
        metrics.gc_orphan_prefixes = orphan_prefixes;
        metrics.gc_deleted_prefixes = 0;
    });
    // Phase 3: fence every destructive operation.
    for (const auto& prefix : candidates) {
        if (protected_prefixes.contains(prefix)) continue;
        if (!lease.IsHeld() || (cancelled && cancelled()))
            return ErrorCode::ETCD_TRANSACTION_FAIL;
        if (!store_.DeleteObjectsWithPrefix(prefix))
            return ErrorCode::INTERNAL_ERROR;
        HAMetricManager::instance().update_snapshot_runtime(
            [](auto& metrics) { ++metrics.gc_deleted_prefixes; });
    }
    return metric_timer.Success(ErrorCode::OK);
}

}  // namespace mooncake
