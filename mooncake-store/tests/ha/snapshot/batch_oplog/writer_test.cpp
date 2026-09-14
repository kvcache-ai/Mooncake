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
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "hot_standby_service.h"

namespace mooncake::test {

namespace {

class FakeHaKvBackend final : public HaKvBackend {
   public:
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

    std::optional<BatchOpLogSnapshotCapture> StartCapture(size_t object_count) {
        backend_ = std::make_shared<FakeHaKvBackend>();
        if (object_count != 0) {
            EXPECT_EQ(ErrorCode::OK,
                      backend_->Put(BuildBatchRecordKey(kClusterId, 1),
                                    EncodeOpLogBatchRecord(
                                        MakeObjectBatch(object_count))));
        }
        EXPECT_EQ(ErrorCode::OK,
                  backend_->Put(BuildDurablePrefixKey(kClusterId),
                                EncodeDurablePrefix(
                                    {.batch_id = object_count == 0 ? 0u : 1u,
                                     .last_seq = object_count})));
        EXPECT_EQ(ErrorCode::OK,
                  backend_->Put(BuildProducerViewKey(kClusterId), "7"));

        HotStandbyConfig config;
        config.enable_oplog_following = true;
        config.enable_verification = false;
        config.oplog_poll_interval_ms = 1;
        standby_ = std::make_unique<HotStandbyService>(config);
        standby_->SetCatchUpBatchKvBackendForTesting(backend_);
        EXPECT_EQ(ErrorCode::OK, standby_->Start("", "", kClusterId));
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
    EXPECT_EQ(5u, object_store.size());
    EXPECT_EQ(2u, object_store.string_upload_attempts);
    EXPECT_GT(object_store.download_attempts, 0u);
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
    std::string manifest_json;
    ASSERT_TRUE(
        object_store.DownloadString(descriptor->manifest_key, manifest_json));
    auto manifest = ha::DecodeBatchOpLogSnapshotManifest(manifest_json);
    ASSERT_TRUE(manifest);
    EXPECT_TRUE(manifest->object_chunks.empty());
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
