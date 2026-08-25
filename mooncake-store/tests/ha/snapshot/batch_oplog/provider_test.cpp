#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_provider.h"

#include <gtest/gtest.h>

#include <map>
#include <string_view>
#include <utility>
#include <vector>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_types.h"
#include "ha/snapshot/batch_oplog/codec.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"

namespace mooncake::test {
namespace {

class EmptyBackend final : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        auto it = values_.find(std::string(key));
        if (it == values_.end()) {
            return ErrorCode::ETCD_KEY_NOT_EXIST;
        }
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        values_[std::string(key)] = std::string(value);
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view begin, std::string_view end, size_t limit,
                    std::vector<KvPair>& output) override {
        output.clear();
        for (const auto& [key, value] : values_) {
            if (key >= begin && key < end &&
                (limit == 0 || output.size() < limit)) {
                output.push_back({key, value});
            }
        }
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }

    ErrorCode Txn(const KvTxn& txn) override {
        for (const auto& put : txn.puts) {
            values_[put.key] = put.value;
        }
        return ErrorCode::OK;
    }

   private:
    std::map<std::string, std::string> values_;
};

}  // namespace

TEST(BatchOpLogSnapshotProviderTest, EmptyCompleteHistoryIsAValidBaseline) {
    EmptyBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 0, .last_seq = 0})));
    LocalFileSnapshotObjectStore object_store(
        "/tmp/mooncake-n05-provider-test");
    BatchOpLogSnapshotProvider provider("clusterA", backend, object_store,
                                        "snapshots");
    StandbyMetadataStore metadata;
    StandbySegmentRegistry registry;

    auto result = provider.RestoreBaseline(metadata, registry);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(0u, result->last_included_seq);
    EXPECT_EQ(0u, result->last_included_batch_id);
    EXPECT_EQ(0u, metadata.GetKeyCount());
    EXPECT_TRUE(registry.GetAllSegments().empty());
}

TEST(BatchOpLogSnapshotProviderTest, UsesFallbackWhenLatestIsCorrupt) {
    const std::string root = "/tmp/mooncake-n05-provider-fallback";
    LocalFileSnapshotObjectStore object_store(root);
    EmptyBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 0, .last_seq = 0})));

    const std::string snapshot_id = "0-7";
    const std::string segments_key =
        ha::BuildBatchOpLogSnapshotSegmentsKey("snapshots", snapshot_id);
    const auto segments = EncodeBatchOpLogSnapshotSegments({});
    ASSERT_TRUE(object_store.UploadBuffer(segments_key, segments));
    ha::BatchOpLogSnapshotManifest manifest;
    manifest.snapshot_id = snapshot_id;
    manifest.segments = {
        .key = segments_key,
        .stored_size = segments.size(),
        .crc32c = Crc32cValue(segments.data(), segments.size())};
    const std::string manifest_key =
        ha::BuildBatchOpLogSnapshotManifestKey("snapshots", snapshot_id);
    const std::string manifest_bytes =
        ha::EncodeBatchOpLogSnapshotManifest(manifest);
    ASSERT_TRUE(object_store.UploadString(manifest_key, manifest_bytes));

    ha::BatchOpLogSnapshotDescriptor descriptor;
    descriptor.snapshot_id = snapshot_id;
    descriptor.manifest_key = manifest_key;
    descriptor.manifest_size = manifest_bytes.size();
    descriptor.manifest_crc32c =
        Crc32cValue(manifest_bytes.data(), manifest_bytes.size());
    const std::string descriptor_bytes =
        ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
    const std::string descriptor_key =
        ha::BuildBatchOpLogSnapshotDescriptorKey("snapshots", snapshot_id);
    ASSERT_TRUE(object_store.UploadString(descriptor_key, descriptor_bytes));

    ASSERT_EQ(
        ErrorCode::OK,
        backend.Put(ha::BuildBatchOpLogSnapshotLatestKey("clusterA"), "{}"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotFallbackKey("clusterA"),
                          descriptor_bytes));

    BatchOpLogSnapshotProvider provider("clusterA", backend, object_store,
                                        "snapshots");
    StandbyMetadataStore metadata;
    StandbySegmentRegistry registry;
    auto result = provider.RestoreBaseline(metadata, registry);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(0u, result->last_included_seq);
    EXPECT_EQ(0u, metadata.GetKeyCount());
}

TEST(BatchOpLogSnapshotProviderTest, ReturnsFinalCursorAfterSuffixReplay) {
    const std::string root = "/tmp/mooncake-n05-provider-suffix";
    LocalFileSnapshotObjectStore object_store(root);
    EmptyBackend backend;

    OpLogBatchRecord suffix_batch;
    suffix_batch.batch_id = 2;
    suffix_batch.first_seq = 2;
    suffix_batch.last_seq = 2;
    OpLogEntry suffix_entry;
    suffix_entry.sequence_id = 2;
    suffix_entry.op_type = OpType::REMOVE;
    suffix_entry.tenant_id = "tenant";
    suffix_entry.object_key = "suffix-key";
    suffix_batch.entries.push_back(std::move(suffix_entry));
    ASSERT_EQ(ErrorCode::OK, backend.Put(BuildBatchRecordKey("clusterA", 2),
                                         EncodeOpLogBatchRecord(suffix_batch)));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildDurablePrefixKey("clusterA"),
                          EncodeDurablePrefix({.batch_id = 2, .last_seq = 2})));

    const std::string snapshot_id = "1-7";
    const auto segments = EncodeBatchOpLogSnapshotSegments({});
    const std::string segments_key =
        ha::BuildBatchOpLogSnapshotSegmentsKey("snapshots", snapshot_id);
    ASSERT_TRUE(object_store.UploadBuffer(segments_key, segments));

    ha::BatchOpLogSnapshotManifest manifest;
    manifest.snapshot_id = snapshot_id;
    manifest.last_included_seq = 1;
    manifest.last_included_batch_id = 1;
    manifest.segments = {
        .key = segments_key,
        .stored_size = segments.size(),
        .crc32c = Crc32cValue(segments.data(), segments.size())};
    const std::string manifest_bytes =
        ha::EncodeBatchOpLogSnapshotManifest(manifest);
    const std::string manifest_key =
        ha::BuildBatchOpLogSnapshotManifestKey("snapshots", snapshot_id);
    ASSERT_TRUE(object_store.UploadString(manifest_key, manifest_bytes));

    ha::BatchOpLogSnapshotDescriptor descriptor;
    descriptor.snapshot_id = snapshot_id;
    descriptor.last_included_seq = 1;
    descriptor.last_included_batch_id = 1;
    descriptor.manifest_key = manifest_key;
    descriptor.manifest_size = manifest_bytes.size();
    descriptor.manifest_crc32c =
        Crc32cValue(manifest_bytes.data(), manifest_bytes.size());
    const std::string descriptor_bytes =
        ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
    const std::string descriptor_key =
        ha::BuildBatchOpLogSnapshotDescriptorKey("snapshots", snapshot_id);
    ASSERT_TRUE(object_store.UploadString(descriptor_key, descriptor_bytes));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(ha::BuildBatchOpLogSnapshotLatestKey("clusterA"),
                          descriptor_bytes));

    BatchOpLogSnapshotProvider provider("clusterA", backend, object_store,
                                        "snapshots");
    StandbyMetadataStore metadata;
    StandbySegmentRegistry registry;
    auto result = provider.RestoreBaseline(metadata, registry);

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(1u, result->last_included_seq);
    EXPECT_EQ(1u, result->last_included_batch_id);
    EXPECT_EQ(2u, result->last_applied_seq);
    EXPECT_EQ(2u, result->last_applied_batch_id);
}

}  // namespace mooncake::test
