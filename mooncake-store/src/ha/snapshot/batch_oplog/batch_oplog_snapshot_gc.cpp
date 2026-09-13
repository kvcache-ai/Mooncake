#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_gc.h"
#include <unordered_set>
#include <vector>
#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"
namespace mooncake {
BatchOpLogSnapshotGc::BatchOpLogSnapshotGc(HaKvBackend& b,
                                           SnapshotObjectStore& s,
                                           std::string c, std::string r)
    : backend_(b), store_(s), cluster_(std::move(c)), root_(std::move(r)) {}
ErrorCode BatchOpLogSnapshotGc::Run(
    const SnapshotMaintenanceLease& lease, std::string_view published,
    const std::optional<std::string>& expected_fallback) {
    return Run(lease, published, expected_fallback, {});
}
ErrorCode BatchOpLogSnapshotGc::Run(
    const SnapshotMaintenanceLease& lease, std::string_view published,
    const std::optional<std::string>& expected_fallback,
    const std::function<bool()>& cancelled) {
    if (!lease.IsHeld() || (cancelled && cancelled()))
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    std::unordered_set<std::string> keep;
    bool first = true;
    for (const auto& key : {ha::BuildBatchOpLogSnapshotLatestKey(cluster_),
                            ha::BuildBatchOpLogSnapshotFallbackKey(cluster_)}) {
        std::string raw;
        auto e = backend_.Get(key, raw);
        if (e == ErrorCode::ETCD_KEY_NOT_EXIST) {
            if (key.find("/latest") != std::string::npos)
                return ErrorCode::INTERNAL_ERROR;
            if (expected_fallback.has_value())
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            continue;
        }
        if (e != ErrorCode::OK) return e;
        if (first && raw != published) return ErrorCode::ETCD_TRANSACTION_FAIL;
        if (!first && expected_fallback != std::optional<std::string>(raw))
            return ErrorCode::ETCD_TRANSACTION_FAIL;
        first = false;
        auto d = ha::DecodeBatchOpLogSnapshotDescriptor(raw);
        if (!d) return ErrorCode::INTERNAL_ERROR;
        auto m =
            ha::BuildBatchOpLogSnapshotArtifactPrefix(root_, d->snapshot_id);
        if (m.empty() ||
            d->manifest_key !=
                ha::BuildBatchOpLogSnapshotManifestKey(root_, d->snapshot_id))
            return ErrorCode::INTERNAL_ERROR;
        std::string manifest;
        auto x = store_.DownloadString(d->manifest_key, manifest);
        if (!x) return ErrorCode::INTERNAL_ERROR;
        if (manifest.size() != d->manifest_size ||
            Crc32cValue(manifest.data(), manifest.size()) != d->manifest_crc32c)
            return ErrorCode::INTERNAL_ERROR;
        auto md = ha::DecodeBatchOpLogSnapshotManifest(manifest);
        if (!md || md->snapshot_id != d->snapshot_id ||
            md->segments.stored_size == 0 ||
            md->segments.key !=
                ha::BuildBatchOpLogSnapshotSegmentsKey(root_, d->snapshot_id))
            return ErrorCode::INTERNAL_ERROR;
        for (size_t i = 0; i < md->object_chunks.size(); ++i)
            if (md->object_chunks[i].chunk_index != i ||
                md->object_chunks[i].stored_size == 0 ||
                md->object_chunks[i].key !=
                    ha::BuildBatchOpLogSnapshotObjectChunkKey(
                        root_, d->snapshot_id, i))
                return ErrorCode::INTERNAL_ERROR;
        auto verify = [&](const std::string& key, uint64_t size, uint32_t crc) {
            auto info = store_.InspectObject(key);
            if (!info || info->stored_size != size) return false;
            if (info->crc32c) return *info->crc32c == crc;
            std::vector<uint8_t> bytes;
            if (!store_.DownloadBuffer(key, bytes) || bytes.size() != size)
                return false;
            return Crc32cValue(bytes.data(), bytes.size()) == crc;
        };
        if (!verify(md->segments.key, md->segments.stored_size,
                    md->segments.crc32c))
            return ErrorCode::INTERNAL_ERROR;
        for (const auto& chunk : md->object_chunks)
            if (!verify(chunk.key, chunk.stored_size, chunk.crc32c))
                return ErrorCode::INTERNAL_ERROR;
        keep.insert(m);
    }
    if (!lease.IsHeld() || (cancelled && cancelled()))
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    std::vector<std::string> keys;
    auto listed = store_.ListObjectsWithPrefix(root_ + "/batch-oplog/", keys);
    if (!listed) return ErrorCode::INTERNAL_ERROR;
    std::unordered_set<std::string> prefixes;
    const std::string namespace_prefix = root_ + "/batch-oplog/";
    for (const auto& k : keys) {
        auto p = k.find('/', namespace_prefix.size());
        if (p != std::string::npos) prefixes.insert(k.substr(0, p + 1));
    }
    for (const auto& p : prefixes)
        if (!keep.count(p)) {
            if (!lease.IsHeld() || (cancelled && cancelled()))
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            auto e = store_.DeleteObjectsWithPrefix(p);
            if (!e) return ErrorCode::INTERNAL_ERROR;
        }
    return ErrorCode::OK;
}
}  // namespace mooncake
