#include "ha_metric_manager.h"
#include "ha/snapshot/batch_oplog/writer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/struct_pack.hpp>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/snapshot/batch_oplog/codec.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_provider.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "hot_standby_service.h"

namespace mooncake::test {

namespace {

class FakeHaKvBackend final : public HaKvBackend {
   public:
    ErrorCode DeleteRange(std::string_view, std::string_view) override {
        return ErrorCode::INVALID_PARAMS;
    }

    ErrorCode Get(std::string_view key, std::string& value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = values_.find(std::string(key));
        if (it == values_.end()) {
            return ErrorCode::ETCD_KEY_NOT_EXIST;
        }
        value = it->second;
        return ErrorCode::OK;
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        values_[std::string(key)] = std::string(value);
        return ErrorCode::OK;
    }

    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        std::lock_guard<std::mutex> lock(mutex_);
        kvs.clear();
        for (const auto& [key, value] : values_) {
            if (key >= begin_key && key < end_key) {
                kvs.push_back({key, value});
                if (limit != 0 && kvs.size() == limit) {
                    break;
                }
            }
        }
        return ErrorCode::OK;
    }

    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::OK; }

   private:
    std::mutex mutex_;
    std::map<std::string, std::string> values_;
};

class FakeObjectStore final : public SnapshotObjectStore {
   public:
    tl::expected<void, std::string> UploadBuffer(
        const std::string& key, const std::vector<uint8_t>& buffer) override {
        ++upload_attempts;
        if (fail_upload_at && upload_attempts == *fail_upload_at) {
            return tl::make_unexpected("injected upload failure");
        }
        objects_[key] = buffer;
        return {};
    }

    tl::expected<void, std::string> DownloadBuffer(
        const std::string& key, std::vector<uint8_t>& buffer) override {
        ++download_attempts;
        if (fail_download) {
            return tl::make_unexpected("injected download failure");
        }
        auto it = objects_.find(key);
        if (it == objects_.end()) {
            return tl::make_unexpected("not found");
        }
        buffer = it->second;
        return {};
    }

    tl::expected<void, std::string> UploadString(
        const std::string& key, const std::string& data) override {
        ++string_upload_attempts;
        return UploadBuffer(key, {data.begin(), data.end()});
    }

    tl::expected<void, std::string> DownloadString(const std::string& key,
                                                   std::string& data) override {
        std::vector<uint8_t> buffer;
        auto result = DownloadBuffer(key, buffer);
        if (!result) {
            return result;
        }
        data.assign(buffer.begin(), buffer.end());
        return {};
    }

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& prefix) override {
        ++cleanup_attempts;
        for (auto it = objects_.begin(); it != objects_.end();) {
            if (it->first.starts_with(prefix)) {
                it = objects_.erase(it);
            } else {
                ++it;
            }
        }
        return {};
    }

    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string& prefix,
        std::vector<std::string>& object_keys) override {
        object_keys.clear();
        for (const auto& [key, value] : objects_) {
            (void)value;
            if (key.starts_with(prefix)) {
                object_keys.push_back(key);
            }
        }
        return {};
    }

    tl::expected<SnapshotObjectInspection, std::string> InspectObject(
        const std::string& key) override {
        auto it = objects_.find(key);
        if (it == objects_.end()) {
            return tl::make_unexpected("not found");
        }
        SnapshotObjectInspection inspection{.stored_size = it->second.size(),
                                            .crc32c = std::nullopt};
        if (provide_checksum) {
            inspection.crc32c =
                Crc32cValue(it->second.data(), it->second.size());
            if (bad_checksum) {
                *inspection.crc32c ^= 1;
            }
        }
        return inspection;
    }

    std::string GetConnectionInfo() const override { return "fake"; }

    void PutExisting(const std::string& key) { objects_[key] = {1}; }
    bool Contains(const std::string& key) const {
        return objects_.contains(key);
    }
    size_t size() const { return objects_.size(); }

    bool provide_checksum{false};
    bool bad_checksum{false};
    bool fail_download{false};
    std::optional<size_t> fail_upload_at;
    size_t upload_attempts{0};
    size_t download_attempts{0};
    size_t cleanup_attempts{0};
    size_t string_upload_attempts{0};

   private:
    std::map<std::string, std::vector<uint8_t>> objects_;
};

OpLogBatchRecord MakeObjectBatch(size_t object_count) {
    OpLogBatchRecord batch;
    batch.batch_id = 1;
    batch.first_seq = 1;
    batch.last_seq = object_count;
    for (size_t i = 0; i < object_count; ++i) {
        MetadataPayload metadata;
        metadata.client_id = {0, i + 1};
        metadata.size = 1024 + i;
        if (i == 1) {
            metadata.hard_pinned = true;
        }
        auto encoded = struct_pack::serialize(metadata);

        OpLogEntry entry;
        entry.sequence_id = i + 1;
        entry.op_type = OpType::PUT_END;
        entry.tenant_id = "tenant";
        entry.object_key = "key-" + std::to_string(i);
        entry.payload.assign(encoded.begin(), encoded.end());
        entry.checksum = ComputeOpLogChecksum(entry.payload);
        batch.entries.push_back(std::move(entry));
    }
    return batch;
}

}  // namespace

class BatchOpLogSnapshotWriterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("BatchOpLogSnapshotWriterTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override {
        if (standby_) {
            standby_->Stop();
        }
        google::ShutdownGoogleLogging();
    }

    std::optional<BatchOpLogSnapshotCapture> StartCapture(
        size_t object_count,
        const WeightMetadataSnapshot* weight_metadata = nullptr) {
        backend_ = std::make_shared<FakeHaKvBackend>();
        auto batch = MakeObjectBatch(object_count);
        const auto append = [&](OpType type, const std::string& key,
                                const auto& value) {
            const auto encoded = struct_pack::serialize(value);
            OpLogEntry entry;
            entry.sequence_id = ++batch.last_seq;
            entry.op_type = type;
            entry.tenant_id = "default";
            entry.object_key = key;
            entry.payload.assign(encoded.begin(), encoded.end());
            entry.checksum = ComputeOpLogChecksum(entry.payload);
            batch.entries.push_back(std::move(entry));
        };
        if (weight_metadata != nullptr) {
            for (const auto& metadata : weight_metadata->metadata) {
                auto importing = metadata;
                importing.metadata_generation = 1;
                importing.availability = WeightAvailabilityState::IMPORTING;
                importing.residency = WeightResidencyState::UNKNOWN;
                importing.manifest.manifest_key.clear();
                importing.manifest.manifest_sha256.clear();
                importing.manifest.payload_keys_sha256.clear();
                importing.updated_at_ms = importing.created_at_ms;
                append(OpType::WEIGHT_METADATA_UPSERT,
                       MakeWeightRevisionMetadataKey(importing.identity),
                       WeightMetadataUpsertOp{.metadata = importing});
                append(OpType::WEIGHT_METADATA_UPSERT,
                       MakeWeightRevisionMetadataKey(metadata.identity),
                       WeightMetadataUpsertOp{.metadata = metadata});
            }
            for (const auto& lease : weight_metadata->leases) {
                append(OpType::WEIGHT_LEASE_UPSERT,
                       "weight-lease:" + std::to_string(lease.lease_id), lease);
            }
        }
        if (!batch.entries.empty()) {
            EXPECT_EQ(ErrorCode::OK,
                      backend_->Put(BuildBatchRecordKey(kClusterId, 1),
                                    EncodeOpLogBatchRecord(batch)));
        }
        EXPECT_EQ(
            ErrorCode::OK,
            backend_->Put(BuildDurablePrefixKey(kClusterId),
                          EncodeDurablePrefix(
                              {.batch_id = batch.entries.empty() ? 0u : 1u,
                               .last_seq = batch.last_seq})));
        EXPECT_EQ(ErrorCode::OK,
                  backend_->Put(BuildProducerViewKey(kClusterId), "7"));

        HotStandbyConfig config;
        config.enable_oplog_following = true;
        config.enable_verification = false;
        config.oplog_poll_interval_ms = 1;
        standby_ = std::make_unique<HotStandbyService>(config);
        standby_->SetCatchUpBatchKvBackendForTesting(backend_);
        EXPECT_EQ(ErrorCode::OK, standby_->Start("", "", kClusterId));
        HAMetricManager::instance().reset_snapshot_runtime(true);
        return standby_->BeginBatchOpLogSnapshotCapture();
    }

    static constexpr char kClusterId[] = "n03-test";
    std::shared_ptr<FakeHaKvBackend> backend_;
    std::unique_ptr<HotStandbyService> standby_;
};

TEST_F(BatchOpLogSnapshotWriterTest, WritesAndVerifiesMultipleChunks) {
    auto capture = StartCapture(3);
    ASSERT_TRUE(capture);
    FakeObjectStore object_store;
    BatchOpLogSnapshotWriter writer(object_store);

    auto descriptor_json =
        writer.Write(*standby_, *capture, "snapshots", "1-42", 2, 1234);

    ASSERT_TRUE(descriptor_json) << descriptor_json.error();
    auto descriptor = ha::DecodeBatchOpLogSnapshotDescriptor(*descriptor_json);
    ASSERT_TRUE(descriptor) << descriptor.error();
    EXPECT_EQ("1-42", descriptor->snapshot_id);
    EXPECT_EQ(3u, descriptor->last_included_seq);
    EXPECT_EQ(1u, descriptor->last_included_batch_id);
    EXPECT_EQ(7, descriptor->producer_view_version);
    EXPECT_EQ(1234, descriptor->created_at_ms);

    std::string manifest_json;
    ASSERT_TRUE(
        object_store.DownloadString(descriptor->manifest_key, manifest_json));
    auto manifest = ha::DecodeBatchOpLogSnapshotManifest(manifest_json);
    ASSERT_TRUE(manifest) << manifest.error();
    ASSERT_EQ(2u, manifest->object_chunks.size());
    EXPECT_EQ(2u, manifest->object_chunks[0].object_count);
    EXPECT_EQ(1u, manifest->object_chunks[1].object_count);
    for (const auto& chunk : manifest->object_chunks) {
        std::vector<uint8_t> encoded_chunk;
        ASSERT_TRUE(object_store.DownloadBuffer(chunk.key, encoded_chunk));
        auto decoded_chunk = DecodeBatchOpLogSnapshotObjectChunk(
            encoded_chunk, chunk.chunk_index, chunk.object_count);
        ASSERT_TRUE(decoded_chunk) << decoded_chunk.error();
        for (const auto& object : decoded_chunk->objects) {
            EXPECT_EQ(object.key == "key-1",
                      object.metadata.hard_pinned.value_or(false));
        }
    }
    const auto metrics = HAMetricManager::instance().get_snapshot_runtime();
    EXPECT_EQ(2u, metrics.chunk_count);
    EXPECT_EQ(manifest->object_chunks[0].stored_size +
                  manifest->object_chunks[1].stored_size,
              metrics.chunk_bytes);
    EXPECT_EQ(metrics.chunk_bytes + manifest->segments.stored_size +
                  manifest_json.size() + descriptor_json->size(),
              metrics.snapshot_bytes);
    EXPECT_EQ(5u, object_store.size());
    EXPECT_EQ(ha::kBatchOpLogSnapshotSchemaVersion, descriptor->schema_version);
    EXPECT_FALSE(manifest->weight_metadata);
    EXPECT_EQ(2u, object_store.string_upload_attempts);
    EXPECT_GT(object_store.download_attempts, 0u);
    standby_
        ->Stop();  // Join the apply loop before inspecting its pause sample.
    EXPECT_GT(
        HAMetricManager::instance().get_snapshot_runtime().capture_pause_us,
        0u);
}

TEST_F(BatchOpLogSnapshotWriterTest, WritesEmptyClusterWithoutObjectChunks) {
    auto capture = StartCapture(0);
    ASSERT_TRUE(capture);
    FakeObjectStore object_store;
    object_store.provide_checksum = true;
    BatchOpLogSnapshotWriter writer(object_store);

    auto descriptor_json =
        writer.Write(*standby_, *capture, "snapshots", "0-42", 2, 1234);

    ASSERT_TRUE(descriptor_json) << descriptor_json.error();
    auto descriptor = ha::DecodeBatchOpLogSnapshotDescriptor(*descriptor_json);
    ASSERT_TRUE(descriptor);
    EXPECT_EQ(ha::kBatchOpLogSnapshotSchemaVersion, descriptor->schema_version);
    std::string manifest_json;
    ASSERT_TRUE(
        object_store.DownloadString(descriptor->manifest_key, manifest_json));
    auto manifest = ha::DecodeBatchOpLogSnapshotManifest(manifest_json);
    ASSERT_TRUE(manifest);
    EXPECT_TRUE(manifest->object_chunks.empty());
}

TEST_F(BatchOpLogSnapshotWriterTest,
       RestoresWeightMetadataAndLeasesAfterCompactedBaseline) {
    const WeightRevisionIdentity identity{
        .tenant_id = "default",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
    const WeightRevisionMetadata metadata{
        .identity = identity,
        .manifest =
            WeightManifestReference{
                .manifest_key =
                    "weights/production/llama-70b/step-100/7/manifest",
                .manifest_sha256 = std::string(64, 'a'),
                .payload_group_id = MakeWeightPayloadGroupId(identity),
                .payload_keys_sha256 = std::string(64, 'b'),
                .payload_count = 2,
                .logical_bytes = 2048,
            },
        .availability = WeightAvailabilityState::READY,
        .residency = WeightResidencyState::HOT,
        .operation = WeightOperationState::NONE,
        .metadata_generation = 2,
        .created_at_ms = 100,
        .updated_at_ms = 102,
    };
    const WeightRevisionLease lease{
        .lease_id = 42,
        .identity = identity,
        .holder = "worker-0",
        .expires_at_ms = 1000,
        .fenced_metadata_generation = 2,
    };
    const WeightMetadataSnapshot expected{
        .metadata = {metadata},
        .leases = {lease},
        .next_lease_id = 43,
    };
    auto capture = StartCapture(1, &expected);
    ASSERT_TRUE(capture);
    ASSERT_EQ(4u, capture->last_included_seq);
    FakeObjectStore object_store;
    BatchOpLogSnapshotWriter writer(object_store);
    auto descriptor =
        writer.Write(*standby_, *capture, "snapshots", "1-42", 2, 1234);
    ASSERT_TRUE(descriptor) << descriptor.error();
    const auto decoded_descriptor =
        ha::DecodeBatchOpLogSnapshotDescriptor(*descriptor);
    ASSERT_TRUE(decoded_descriptor);
    EXPECT_EQ(ha::kBatchOpLogWeightSnapshotSchemaVersion,
              decoded_descriptor->schema_version);
    standby_->Stop();

    // Only the materialized baseline remains; replay cannot recover omitted
    // weight records from the original batch.
    FakeHaKvBackend compacted_backend;
    ASSERT_EQ(
        ErrorCode::OK,
        compacted_backend.Put(ha::BuildBatchOpLogSnapshotLatestKey(kClusterId),
                              *descriptor));
    ASSERT_EQ(
        ErrorCode::OK,
        compacted_backend.Put(
            ha::BuildBatchOpLogSnapshotCompactionFloorKey(kClusterId), "1"));
    ASSERT_EQ(ErrorCode::OK,
              compacted_backend.Put(
                  BuildDurablePrefixKey(kClusterId),
                  EncodeDurablePrefix({.batch_id = 1, .last_seq = 4})));
    BatchOpLogSnapshotProvider provider(kClusterId, compacted_backend,
                                        object_store, "snapshots");
    StandbyMetadataStore restored;
    StandbySegmentRegistry registry;
    auto result = provider.RestoreBaseline(restored, registry);
    ASSERT_TRUE(result) << toString(result.error());
    EXPECT_EQ(expected, restored.SnapshotWeightMetadata());

    auto renewed = lease;
    renewed.expires_at_ms = 2000;
    const auto encoded_lease = struct_pack::serialize(renewed);
    OpLogEntry renewal;
    renewal.sequence_id = 5;
    renewal.op_type = OpType::WEIGHT_LEASE_UPSERT;
    renewal.tenant_id = "default";
    renewal.object_key = "weight-lease:42";
    renewal.payload.assign(encoded_lease.begin(), encoded_lease.end());
    renewal.checksum = ComputeOpLogChecksum(renewal.payload);
    OpLogBatchRecord suffix;
    suffix.batch_id = 2;
    suffix.first_seq = 5;
    suffix.last_seq = 5;
    suffix.entries.push_back(renewal);
    ASSERT_EQ(ErrorCode::OK,
              compacted_backend.Put(BuildBatchRecordKey(kClusterId, 2),
                                    EncodeOpLogBatchRecord(suffix)));
    ASSERT_EQ(ErrorCode::OK,
              compacted_backend.Put(
                  BuildDurablePrefixKey(kClusterId),
                  EncodeDurablePrefix({.batch_id = 2, .last_seq = 5})));
    ASSERT_TRUE(provider.RestoreBaseline(restored, registry));
    auto after_suffix = expected;
    after_suffix.leases.front() = renewed;
    EXPECT_EQ(after_suffix, restored.SnapshotWeightMetadata());

    const auto weight_key =
        ha::BuildBatchOpLogSnapshotWeightMetadataKey("snapshots", "1-42");
    std::vector<uint8_t> weight_bytes;
    ASSERT_TRUE(object_store.DownloadBuffer(weight_key, weight_bytes));
    ASSERT_FALSE(weight_bytes.empty());
    weight_bytes.back() ^= 1;
    ASSERT_TRUE(object_store.UploadBuffer(weight_key, weight_bytes));
    EXPECT_FALSE(provider.RestoreBaseline(restored, registry));
    EXPECT_TRUE(restored.SnapshotWeightMetadata().metadata.empty());
}

TEST_F(BatchOpLogSnapshotWriterTest,
       RejectsExistingCandidateWithoutDeletingIt) {
    auto capture = StartCapture(1);
    ASSERT_TRUE(capture);
    FakeObjectStore object_store;
    const std::string existing_key =
        ha::BuildBatchOpLogSnapshotSegmentsKey("snapshots", "1-42");
    object_store.PutExisting(existing_key);
    BatchOpLogSnapshotWriter writer(object_store);

    auto result =
        writer.Write(*standby_, *capture, "snapshots", "1-42", 2, 1234);

    EXPECT_FALSE(result);
    EXPECT_TRUE(object_store.Contains(existing_key));
    EXPECT_EQ(0u, object_store.cleanup_attempts);
}

TEST_F(BatchOpLogSnapshotWriterTest, CleansCandidateOnUploadFailure) {
    auto capture = StartCapture(3);
    ASSERT_TRUE(capture);
    FakeObjectStore object_store;
    object_store.fail_upload_at = 2;
    BatchOpLogSnapshotWriter writer(object_store);

    auto result =
        writer.Write(*standby_, *capture, "snapshots", "1-42", 2, 1234);

    EXPECT_FALSE(result);
    EXPECT_EQ(0u, object_store.size());
    EXPECT_EQ(1u, object_store.cleanup_attempts);
}

TEST_F(BatchOpLogSnapshotWriterTest, CleansCandidateOnChecksumMismatch) {
    auto capture = StartCapture(1);
    ASSERT_TRUE(capture);
    FakeObjectStore object_store;
    object_store.provide_checksum = true;
    object_store.bad_checksum = true;
    BatchOpLogSnapshotWriter writer(object_store);

    auto result =
        writer.Write(*standby_, *capture, "snapshots", "1-42", 2, 1234);

    EXPECT_FALSE(result);
    EXPECT_EQ(0u, object_store.size());
    EXPECT_EQ(1u, object_store.cleanup_attempts);
}

TEST_F(BatchOpLogSnapshotWriterTest, CleansCandidateOnReadbackFailure) {
    auto capture = StartCapture(1);
    ASSERT_TRUE(capture);
    FakeObjectStore object_store;
    object_store.fail_download = true;
    BatchOpLogSnapshotWriter writer(object_store);

    auto result =
        writer.Write(*standby_, *capture, "snapshots", "1-42", 2, 1234);

    EXPECT_FALSE(result);
    EXPECT_EQ(0u, object_store.size());
    EXPECT_EQ(1u, object_store.cleanup_attempts);
}

}  // namespace mooncake::test
