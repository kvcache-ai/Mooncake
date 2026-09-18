#include "ha_metric_manager.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_gc.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"

namespace mooncake::test {

namespace {

class FakeBackend final : public HaKvBackend {
   public:
    ErrorCode DeleteRange(std::string_view, std::string_view) override {
        return ErrorCode::INVALID_PARAMS;
    }

    ErrorCode Get(std::string_view key, std::string& value) override {
        auto it = values.find(std::string(key));
        if (it == values.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view, std::string_view) override {
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view, std::string_view, size_t,
                    std::vector<KvPair>&) override {
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::OK; }

    std::map<std::string, std::string> values;
};

class RecordingObjectStore final : public SnapshotObjectStore {
   public:
    tl::expected<void, std::string> UploadBuffer(
        const std::string&, const std::vector<uint8_t>&) override {
        return {};
    }

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& bytes) override {
        auto it = objects.find(key);
        if (it == objects.end()) return tl::make_unexpected("missing");
        bytes = it->second;
        return {};
    }

    tl::expected<void, std::string> UploadString(const std::string&,
                                                 const std::string&) override {
        return {};
    }

    tl::expected<void, std::string> DownloadString(
        const std::string& key, std::string& value) override {
        std::vector<uint8_t> bytes;
        auto result = DownloadBuffer(key, bytes);
        if (!result) return result;
        value.assign(bytes.begin(), bytes.end());
        return {};
    }

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override {
        deleted.push_back(prefix);
        return {};
    }

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix, std::vector<std::string>& keys) override {
        for (const auto& [key, _] : objects)
            if (key.starts_with(prefix)) keys.push_back(key);
        return {};
    }

    tl::expected<SnapshotObjectInspection, std::string> InspectObject(
        const std::string& key) override {
        auto it = objects.find(key);
        if (it == objects.end()) return tl::make_unexpected("missing");
        return SnapshotObjectInspection{
            .stored_size = it->second.size(),
            .crc32c = Crc32cValue(it->second.data(), it->second.size())};
    }

    std::string GetConnectionInfo() const override { return "test"; }

    std::vector<std::string> deleted;
    std::map<std::string, std::vector<uint8_t>> objects;
};

ha::BatchOpLogSnapshotDescriptor MakeDescriptor(const std::string& root,
                                                uint64_t batch_id) {
    ha::BatchOpLogSnapshotDescriptor descriptor;
    descriptor.snapshot_id = ha::BuildBatchOpLogSnapshotId(batch_id, 1);
    descriptor.last_included_seq = batch_id * 10;
    descriptor.last_included_batch_id = batch_id;
    descriptor.producer_view_version = 1;
    descriptor.manifest_key =
        ha::BuildBatchOpLogSnapshotManifestKey(root, descriptor.snapshot_id);
    descriptor.manifest_size = 1;
    descriptor.manifest_crc32c = 0;
    descriptor.created_at_ms = 1;
    return descriptor;
}

}  // namespace

TEST(BatchOpLogSnapshotGcTest, PointerMismatchSkipsDeletion) {
    FakeBackend backend;
    RecordingObjectStore object_store;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("c", "1");
    backend.values[::mooncake::ha::BuildBatchOpLogSnapshotLatestKey("c")] =
        "changed";

    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              BatchOpLogSnapshotGc(backend, object_store, "c", "r")
                  .Run(*lease, "published", std::nullopt));
    EXPECT_TRUE(object_store.deleted.empty());
}

TEST(BatchOpLogSnapshotGcTest, DeletesOnlyUnprotectedAttempt) {
    HAMetricManager::instance().reset_snapshot_runtime(true);
    constexpr std::string_view kRoot = "snapshots";
    FakeBackend backend;
    RecordingObjectStore object_store;
    auto lease = SnapshotMaintenanceLease::MakeForTesting("c", "1");
    auto descriptor = MakeDescriptor(std::string(kRoot), 2);
    ha::BatchOpLogSnapshotManifest manifest;
    manifest.snapshot_id = descriptor.snapshot_id;
    manifest.last_included_batch_id = descriptor.last_included_batch_id;
    manifest.last_included_seq = descriptor.last_included_seq;
    const std::vector<uint8_t> segment_bytes = {0};
    manifest.segments = {ha::BuildBatchOpLogSnapshotSegmentsKey(
                             std::string(kRoot), descriptor.snapshot_id),
                         1, Crc32cValue(segment_bytes.data(), 1)};
    const std::string manifest_json =
        ha::EncodeBatchOpLogSnapshotManifest(manifest);
    descriptor.manifest_size = manifest_json.size();
    descriptor.manifest_crc32c =
        Crc32cValue(manifest_json.data(), manifest_json.size());
    const std::string descriptor_json =
        ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
    backend.values[::mooncake::ha::BuildBatchOpLogSnapshotLatestKey("c")] =
        descriptor_json;
    object_store.objects[descriptor.manifest_key] =
        std::vector<uint8_t>(manifest_json.begin(), manifest_json.end());
    object_store.objects[manifest.segments.key] = segment_bytes;

    const auto protected_prefix =
        ha::BuildBatchOpLogSnapshotArtifactPrefix(std::string(kRoot), "2-1");
    const auto stale_prefix =
        ha::BuildBatchOpLogSnapshotArtifactPrefix(std::string(kRoot), "1-1");
    object_store.objects[protected_prefix + "marker"] = {1};
    object_store.objects[stale_prefix + "marker"] = {2};

    EXPECT_EQ(ErrorCode::OK, BatchOpLogSnapshotGc(backend, object_store, "c",
                                                  std::string(kRoot))
                                 .Run(*lease, descriptor_json, std::nullopt));
    ASSERT_EQ(1U, object_store.deleted.size());
    EXPECT_EQ(
        1u,
        HAMetricManager::instance().get_snapshot_runtime().gc_orphan_prefixes);
    EXPECT_EQ(
        1u,
        HAMetricManager::instance().get_snapshot_runtime().gc_deleted_prefixes);
    EXPECT_EQ(stale_prefix, object_store.deleted.front());
}

}  // namespace mooncake::test
