#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_gc.h"
#include <unordered_set>
#include <vector>
#include "ha/kv/ha_kv_backend.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"
namespace mooncake {
BatchOpLogSnapshotGc::BatchOpLogSnapshotGc(HaKvBackend& b,
                                           SnapshotObjectStore& s,
                                           std::string c, std::string r)
    : backend_(b), store_(s), cluster_(std::move(c)), root_(std::move(r)) {}
ErrorCode BatchOpLogSnapshotGc::Run(const SnapshotMaintenanceLease& lease) {
    if (!lease.IsHeld()) return ErrorCode::ETCD_TRANSACTION_FAIL;
    std::unordered_set<std::string> keep;
    for (const auto& key : {ha::BuildBatchOpLogSnapshotLatestKey(cluster_),
                            ha::BuildBatchOpLogSnapshotFallbackKey(cluster_)}) {
        std::string raw;
        auto e = backend_.Get(key, raw);
        if (e == ErrorCode::ETCD_KEY_NOT_EXIST) {
            if (key.find("/latest") != std::string::npos)
                return ErrorCode::INTERNAL_ERROR;
            continue;
        }
        if (e != ErrorCode::OK) return e;
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
        auto md = ha::DecodeBatchOpLogSnapshotManifest(manifest);
        if (!md || md->snapshot_id != d->snapshot_id ||
            md->segments.key !=
                ha::BuildBatchOpLogSnapshotSegmentsKey(root_, d->snapshot_id))
            return ErrorCode::INTERNAL_ERROR;
        for (size_t i = 0; i < md->object_chunks.size(); ++i)
            if (md->object_chunks[i].chunk_index != i ||
                md->object_chunks[i].key !=
                    ha::BuildBatchOpLogSnapshotObjectChunkKey(
                        root_, d->snapshot_id, i))
                return ErrorCode::INTERNAL_ERROR;
        keep.insert(m);
    }
    if (!lease.IsHeld()) return ErrorCode::ETCD_TRANSACTION_FAIL;
    std::vector<std::string> keys;
    auto listed = store_.ListObjectsWithPrefix(root_ + "/batch-oplog/", keys);
    if (!listed) return ErrorCode::INTERNAL_ERROR;
    std::unordered_set<std::string> prefixes;
    for (const auto& k : keys) {
        auto p = k.find('/', root_.size() + 12);
        if (p != std::string::npos) prefixes.insert(k.substr(0, p + 1));
    }
    for (const auto& p : prefixes)
        if (!keep.count(p)) {
            if (!lease.IsHeld()) return ErrorCode::ETCD_TRANSACTION_FAIL;
            auto e = store_.DeleteObjectsWithPrefix(p);
            if (!e) return ErrorCode::INTERNAL_ERROR;
        }
    return ErrorCode::OK;
}
}  // namespace mooncake
