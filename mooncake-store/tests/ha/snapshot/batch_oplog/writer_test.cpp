#include "ha_metric_manager.h"
#include "ha/snapshot/batch_oplog/writer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map>
#include <chrono>
#include <condition_variable>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
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
#include "master_service.h"
#include "master_service/master_service_test_peer.h"

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
    ErrorCode Txn(const KvTxn& txn) override {
        std::unique_lock lock(mutex_);
        if (fail_txn_) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (block_txn_) {
            txn_entered_ = true;
            cv_.notify_all();
            cv_.wait(lock, [&] { return !block_txn_; });
        }
        for (const auto& compare : txn.compares) {
            const auto it = values_.find(compare.key);
            if (compare.kind == KvCompareKind::kKeyNotExists) {
                if (it != values_.end()) {
                    return ErrorCode::ETCD_TRANSACTION_FAIL;
                }
            } else if (it == values_.end() ||
                       it->second != compare.expected_value) {
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
        for (const auto& put : txn.puts) {
            values_[put.key] = put.value;
        }
        return ErrorCode::OK;
    }

    void BlockTxn() {
        std::lock_guard lock(mutex_);
        block_txn_ = true;
        txn_entered_ = false;
    }

    void FailTransactions() {
        std::lock_guard lock(mutex_);
        fail_txn_ = true;
    }

    bool WaitForBlockedTxn() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(5),
                            [&] { return txn_entered_; });
    }

    void AllowTxn() {
        std::lock_guard lock(mutex_);
        block_txn_ = false;
        cv_.notify_all();
    }

   private:
    std::mutex mutex_;
    std::map<std::string, std::string> values_;
    std::condition_variable cv_;
    bool block_txn_{false};
    bool fail_txn_{false};
    bool txn_entered_{false};
};

class PutEndAppendGate final : public OrderedOpLogWriter {
   public:
    using OrderedOpLogWriter::OrderedOpLogWriter;
    ~PutEndAppendGate() override { Stop(); }

    tl::expected<PendingHandle, ErrorCode> Commit(
        Reservation&& reservation, OpLogEntry entry,
        DurableCallback callback) override {
        if (entry.op_type == OpType::PUT_END &&
            entry.object_key == "inflight-import-payload") {
            std::unique_lock lock(mutex_);
            entered_ = true;
            cv_.notify_all();
            cv_.wait(lock, [&] { return released_; });
        }
        return OrderedOpLogWriter::Commit(
            std::move(reservation), std::move(entry), std::move(callback));
    }

    bool WaitUntilEntered() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(5),
                            [&] { return entered_; });
    }

    void Release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

    void Stop() override {
        Release();
        OrderedOpLogWriter::Stop();
    }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_{false};
    bool released_{false};
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

WeightMetadataSnapshot MakeWeightSnapshot(bool active) {
    WeightRevisionMetadata metadata;
    metadata.identity = {"default", "production", "llama-70b", "step-100", 7};
    metadata.manifest = {
        .manifest_key = MakeWeightManifestKey(metadata.identity),
        .manifest_sha256 = std::string(64, 'a'),
        .payload_group_id = MakeWeightPayloadGroupId(metadata.identity),
        .payload_keys_sha256 = std::string(64, 'b'),
        .payload_count = 2,
        .logical_bytes = 2048,
    };
    metadata.availability = WeightAvailabilityState::READY;
    metadata.residency = WeightResidencyState::HOT;
    metadata.metadata_generation = active ? 3 : 2;
    metadata.created_at_ms = 100;
    metadata.updated_at_ms = 200;
    WeightMetadataSnapshot snapshot;
    snapshot.leases.push_back({
        .lease_id = 42,
        .identity = metadata.identity,
        .holder = "worker-0",
        .expires_at_ms = std::numeric_limits<uint64_t>::max(),
        .fenced_metadata_generation = 2,
    });
    snapshot.next_lease_id = 43;
    if (active) {
        metadata.operation = WeightOperationState::REHYDRATING;
        metadata.operation_id = 9;
        snapshot.operations.push_back({
            .operation_id = 9,
            .identity = metadata.identity,
            .operation = WeightOperationState::REHYDRATING,
            .target_residency = WeightResidencyState::HOT,
            .fenced_metadata_generation = 3,
            .started_at_ms = 200,
            .updated_at_ms = 200,
            .processed_members = 0,
            .total_members = 3,
        });
        snapshot.next_operation_id = 10;
    }
    snapshot.metadata.push_back(metadata);
    return snapshot;
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
                importing.operation = WeightOperationState::NONE;
                importing.operation_id = 0;
                importing.manifest.manifest_key.clear();
                importing.manifest.manifest_sha256.clear();
                importing.manifest.payload_keys_sha256.clear();
                importing.updated_at_ms = importing.created_at_ms;
                append(OpType::WEIGHT_METADATA_UPSERT,
                       MakeWeightRevisionMetadataKey(importing.identity),
                       WeightMetadataUpsertOp{.metadata = importing});
                auto ready = metadata;
                ready.metadata_generation = 2;
                ready.availability = WeightAvailabilityState::READY;
                ready.residency = WeightResidencyState::HOT;
                ready.operation = WeightOperationState::NONE;
                ready.operation_id = 0;
                append(OpType::WEIGHT_METADATA_UPSERT,
                       MakeWeightRevisionMetadataKey(metadata.identity),
                       WeightMetadataUpsertOp{.metadata = ready});
                for (const auto& lease : weight_metadata->leases) {
                    if (lease.identity == metadata.identity) {
                        append(OpType::WEIGHT_LEASE_UPSERT,
                               MakeWeightLeaseMetadataKey(lease.lease_id),
                               lease);
                    }
                }
                if (metadata.operation != WeightOperationState::NONE) {
                    for (const auto& operation : weight_metadata->operations) {
                        if (operation.operation_id == metadata.operation_id) {
                            append(OpType::WEIGHT_METADATA_UPSERT,
                                   MakeWeightRevisionMetadataKey(
                                       metadata.identity),
                                   WeightMetadataUpsertOp{metadata, operation});
                        }
                    }
                }
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

    std::unique_ptr<HotStandbyService> Bootstrap(
        const std::shared_ptr<FakeHaKvBackend>& backend,
        FakeObjectStore& object_store) {
        HotStandbyConfig config;
        config.enable_snapshot_bootstrap = true;
        config.enable_oplog_following = true;
        config.enable_verification = false;
        config.oplog_poll_interval_ms = 1;
        config.batch_oplog_retry_timeout_sec = 0;
        auto standby = std::make_unique<HotStandbyService>(config);
        standby->SetCatchUpBatchKvBackendForTesting(backend);
        standby->SetBatchOpLogSnapshotProvider(
            std::make_unique<BatchOpLogSnapshotProvider>(
                kClusterId, *backend, object_store, "snapshots"));
        EXPECT_EQ(ErrorCode::OK, standby->Start("", "", kClusterId));
        return standby;
    }

    void VerifyPromotion(HotStandbyService& standby,
                         const WeightMetadataSnapshot& expected,
                         DurablePrefix cursor) {
        auto handoff = standby.PromoteAndDetachBatchOpLogStore();
        ASSERT_TRUE(handoff) << toString(handoff.error());
        EXPECT_EQ(cursor, handoff->applied_cursor);
        ASSERT_TRUE(handoff->metadata_store);
        EXPECT_EQ(expected, handoff->metadata_store->SnapshotWeightMetadata());
        MasterService restored;
        ASSERT_TRUE(
            restored.RestoreFromBatchOpLogPromotion(std::move(*handoff)));
        for (const auto& metadata : expected.metadata) {
            const auto view = restored.GetWeightRevision({metadata.identity});
            ASSERT_TRUE(view);
            EXPECT_EQ(metadata, view->metadata);
        }
        for (const auto& operation : expected.operations) {
            const auto queried = restored.QueryWeightOperation(
                {.operation_id = operation.operation_id});
            ASSERT_TRUE(queried);
            EXPECT_EQ(operation, *queried);
        }
        EXPECT_EQ(
            expected,
            MasterServiceTestPeer::WeightMetadata(restored).ExportSnapshot());
    }

    void CheckCompactedWeightPromotion(bool append_completion) {
        auto expected = MakeWeightSnapshot(true);
        auto capture = StartCapture(0, &expected);
        ASSERT_TRUE(capture);
        ASSERT_EQ(expected, capture->weight_metadata);
        ASSERT_EQ(4u, capture->last_included_seq);
        FakeObjectStore object_store;
        BatchOpLogSnapshotWriter writer(object_store);
        const auto descriptor =
            writer.Write(*standby_, *capture, "snapshots", "1-43", 2, 1234);
        ASSERT_TRUE(descriptor) << descriptor.error();
        standby_->Stop();

        auto compacted = std::make_shared<FakeHaKvBackend>();
        ASSERT_EQ(
            ErrorCode::OK,
            compacted->Put(ha::BuildBatchOpLogSnapshotLatestKey(kClusterId),
                           *descriptor));
        ASSERT_EQ(ErrorCode::OK,
                  compacted->Put(
                      ha::BuildBatchOpLogSnapshotCompactionFloorKey(kClusterId),
                      "1"));
        DurablePrefix cursor{.batch_id = 1, .last_seq = 4};
        if (append_completion) {
            auto& metadata = expected.metadata.front();
            metadata.metadata_generation = 4;
            metadata.updated_at_ms = 300;
            metadata.operation = WeightOperationState::NONE;
            metadata.operation_id = 0;
            auto& operation = expected.operations.front();
            operation.updated_at_ms = 300;
            operation.processed_members = operation.total_members;
            operation.message = kWeightOperationCompletedMessage;
            OpLogEntry entry;
            entry.sequence_id = 5;
            entry.op_type = OpType::WEIGHT_METADATA_UPSERT;
            entry.tenant_id = "default";
            entry.object_key = MakeWeightRevisionMetadataKey(metadata.identity);
            const auto encoded = struct_pack::serialize(
                WeightMetadataUpsertOp{metadata, operation});
            entry.payload.assign(encoded.begin(), encoded.end());
            entry.checksum = ComputeOpLogChecksum(entry.payload);
            OpLogBatchRecord suffix;
            suffix.batch_id = 2;
            suffix.first_seq = suffix.last_seq = 5;
            suffix.entries.push_back(entry);
            ASSERT_EQ(ErrorCode::OK,
                      compacted->Put(BuildBatchRecordKey(kClusterId, 2),
                                     EncodeOpLogBatchRecord(suffix)));
            cursor = {.batch_id = 2, .last_seq = 5};
        }
        ASSERT_EQ(ErrorCode::OK,
                  compacted->Put(BuildDurablePrefixKey(kClusterId),
                                 EncodeDurablePrefix(cursor)));
        std::string missing;
        ASSERT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
                  compacted->Get(BuildBatchRecordKey(kClusterId, 1), missing));
        const auto downloads_before = object_store.download_attempts;
        auto restored = Bootstrap(compacted, object_store);
        EXPECT_GT(object_store.download_attempts, downloads_before);
        VerifyPromotion(*restored, expected, cursor);
    }

    void CheckPublicDeleteCancellation(bool fail_append) {
        auto before = MakeWeightSnapshot(true);
        before.leases.clear();
        before.next_lease_id = 1;
        // Missing members keep this rehydration pending even if the worker
        // runs.
        before.metadata.front().availability =
            WeightAvailabilityState::DEGRADED;
        before.metadata.front().residency = WeightResidencyState::ABSENT;
        const auto identity = before.metadata.front().identity;
        auto capture = StartCapture(0, &before);
        ASSERT_TRUE(capture);
        ASSERT_EQ(before, capture->weight_metadata);
        const DurablePrefix baseline_cursor{
            .batch_id = capture->last_included_batch_id,
            .last_seq = capture->last_included_seq,
        };
        FakeObjectStore object_store;
        BatchOpLogSnapshotWriter writer(object_store);
        const auto descriptor =
            writer.Write(*standby_, *capture, "snapshots", "1-45", 2, 1234);
        ASSERT_TRUE(descriptor) << descriptor.error();
        standby_->Stop();
        ASSERT_EQ(
            ErrorCode::OK,
            backend_->Put(ha::BuildBatchOpLogSnapshotLatestKey(kClusterId),
                          *descriptor));
        auto source = Bootstrap(backend_, object_store);
        auto handoff = source->PromoteAndDetachBatchOpLogStore();
        ASSERT_TRUE(handoff);
        ASSERT_EQ(before, handoff->metadata_store->SnapshotWeightMetadata());
        MasterService primary(
            MasterServiceConfig::builder()
                .set_enable_ha(true)
                .set_enable_oplog(true)
                .set_weight_management_oplog_capability_confirmed(true)
                .set_cluster_id(kClusterId)
                .set_oplog_batch_max_entries(1)
                .build());
        ASSERT_TRUE(
            primary.RestoreFromBatchOpLogPromotion(std::move(*handoff)));
        ASSERT_EQ(ErrorCode::OK,
                  MasterServiceTestPeer(primary).SetBatchOpLogBackendForTesting(
                      backend_));
        auto& metadata_store = MasterServiceTestPeer::WeightMetadata(primary);
        const auto late_progress =
            metadata_store.PrepareUpdateOperationProgress(
                9, 1, 3, "late-member", WeightAvailabilityState::DEGRADED,
                WeightResidencyState::ABSENT, 201);
        ASSERT_TRUE(late_progress);
        if (fail_append) {
            backend_->FailTransactions();
        }
        // Cancellation must be emitted by the public Delete producer.
        const auto deleted = primary.DeleteWeightRevision({
            .identity = identity,
            .expected_metadata_generation =
                before.metadata.front().metadata_generation,
        });
        MasterServiceTestPeer::OrderedOplogWriter(primary)->Stop();
        OpLogBatchStorage storage(kClusterId, *backend_);
        DurablePrefix final_cursor;
        ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(final_cursor));
        if (fail_append) {
            ASSERT_FALSE(deleted);
            EXPECT_EQ(WeightManagementError::DURABILITY_FAILED,
                      deleted.error());
            EXPECT_EQ(baseline_cursor, final_cursor);
            EXPECT_EQ(before, metadata_store.ExportSnapshot());
            const auto view = primary.GetWeightRevision({identity});
            ASSERT_TRUE(view);
            EXPECT_EQ(before.metadata.front(), view->metadata);
            const auto operation =
                primary.QueryWeightOperation({.operation_id = 9});
            ASSERT_TRUE(operation);
            EXPECT_EQ(before.operations.front(), *operation);
            auto restored = Bootstrap(backend_, object_store);
            VerifyPromotion(*restored, before, baseline_cursor);
            return;
        }
        ASSERT_TRUE(deleted);
        EXPECT_EQ(WeightAvailabilityState::DELETED, deleted->availability);
        const auto expected = metadata_store.ExportSnapshot();
        ASSERT_EQ(1u, expected.operations.size());
        EXPECT_EQ(kWeightOperationCancelledMessage,
                  expected.operations.front().message);
        EXPECT_EQ(before.operations.front().target_residency,
                  expected.operations.front().target_residency);
        EXPECT_EQ(before.operations.front().processed_members,
                  expected.operations.front().processed_members);
        EXPECT_FALSE(metadata_store.Publish(*late_progress));
        EXPECT_EQ(expected, metadata_store.ExportSnapshot());

        StandbyMetadataStore replayed;
        OpLogApplier applier(&replayed, kClusterId);
        size_t cancellation_envelopes = 0;
        for (uint64_t id = 1; id <= final_cursor.batch_id; ++id) {
            OpLogBatchRecord batch;
            ASSERT_EQ(ErrorCode::OK, storage.ReadBatch(id, batch));
            for (const auto& entry : batch.entries) {
                ASSERT_TRUE(applier.ApplyOpLogEntry(entry));
                if (entry.op_type != OpType::WEIGHT_METADATA_UPSERT) {
                    continue;
                }
                WeightMetadataUpsertOp upsert;
                ASSERT_EQ(struct_pack::errc::ok,
                          struct_pack::deserialize_to(upsert, entry.payload));
                if (!upsert.operation || upsert.operation->message !=
                                             kWeightOperationCancelledMessage) {
                    continue;
                }
                ++cancellation_envelopes;
                EXPECT_EQ(WeightAvailabilityState::DELETING,
                          upsert.metadata.availability);
                EXPECT_EQ(WeightOperationState::NONE,
                          upsert.metadata.operation);
                EXPECT_EQ(0u, upsert.metadata.operation_id);
                const auto snapshot = replayed.SnapshotWeightMetadata();
                WeightMetadataStore restored;
                ASSERT_TRUE(restored.RestoreSnapshot(snapshot));
                const auto queried = restored.QueryOperation(9);
                ASSERT_TRUE(queried);
                EXPECT_EQ(*upsert.operation, *queried);
                EXPECT_FALSE(restored.Publish(*late_progress));
                EXPECT_EQ(snapshot, restored.ExportSnapshot());
            }
        }
        EXPECT_EQ(1u, cancellation_envelopes);
        EXPECT_EQ(final_cursor.last_seq + 1, applier.GetExpectedSequenceId());
        EXPECT_EQ(expected, replayed.SnapshotWeightMetadata());
        auto restored = Bootstrap(backend_, object_store);
        VerifyPromotion(*restored, expected, final_cursor);
    }

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
       PromotesActiveWeightStateFromCompactedSnapshot) {
    CheckCompactedWeightPromotion(false);
}

TEST_F(BatchOpLogSnapshotWriterTest,
       PromotesNewerWeightMetadataSuffixAfterCompactedSnapshot) {
    CheckCompactedWeightPromotion(true);
}

TEST_F(BatchOpLogSnapshotWriterTest,
       PublicDeleteCancellationReplaysAndSurvivesPromotion) {
    CheckPublicDeleteCancellation(false);
}

TEST_F(BatchOpLogSnapshotWriterTest,
       FailedPublicDeleteAppendPreservesPendingOperation) {
    CheckPublicDeleteCancellation(true);
}

TEST_F(BatchOpLogSnapshotWriterTest,
       InflightPutEndExcludesImportAbandonmentAndRefreshesActivity) {
    auto backend = std::make_shared<FakeHaKvBackend>();
    MasterService primary(
        MasterServiceConfig::builder()
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_weight_management_oplog_capability_confirmed(true)
            .set_cluster_id(kClusterId)
            .set_oplog_batch_max_entries(1)
            .build());
    MasterServiceTestPeer(primary).SetBatchOpLogWriterFactoryForTesting(
        [](OrderedOpLogWriterConfig config,
           OrderedOpLogWriter::WriteBatchFn write_batch) {
            return std::make_unique<PutEndAppendGate>(std::move(config),
                                                      std::move(write_batch));
        });
    ASSERT_EQ(
        ErrorCode::OK,
        MasterServiceTestPeer(primary).SetBatchOpLogBackendForTesting(backend));
    auto* gate = static_cast<PutEndAppendGate*>(
        MasterServiceTestPeer::OrderedOplogWriter(primary).get());
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "inflight-import-segment";
    segment.te_endpoint = segment.name;
    segment.base = 0x100000000ULL;
    segment.size = 16 * 1024 * 1024;
    const auto client = generate_uuid();
    ASSERT_TRUE(primary.MountSegment(segment, client));
    const auto identity = MakeWeightSnapshot(false).metadata.front().identity;
    const auto importing = primary.BeginWeightImport({
        .identity = identity,
        .expected_payload_count = 1,
        .expected_logical_bytes = 1024,
    });
    ASSERT_TRUE(importing);
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    ASSERT_TRUE(primary.PutStart(client, "inflight-import-payload",
                                 TenantId::Default(), 1024, config));
    const auto now_ms = [] {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
    };
    const auto before_end_ms = now_ms();
    const auto sweep_time = before_end_ms + 300'001;
    std::future<tl::expected<void, ErrorCode>> ending;
    std::future<size_t> sweeping;
    struct ReleaseGate {
        PutEndAppendGate& gate;
        ~ReleaseGate() { gate.Release(); }
    } release{*gate};
    ending = std::async(std::launch::async, [&] {
        return primary.PutEnd(client, "inflight-import-payload",
                              TenantId::Default(), ReplicaType::MEMORY);
    });
    ASSERT_TRUE(gate->WaitUntilEntered());
    sweeping = std::async(std::launch::async, [&] {
        return MasterServiceTestPeer(primary).RunWeightReconciliationForTesting(
            sweep_time, 32);
    });
    const auto sweep_status = sweeping.wait_for(std::chrono::seconds(5));
    const auto while_inflight = primary.GetWeightRevision({identity});
    // Make the successful End's activity timestamp strictly newer than the
    // cutoff used by both sweeps; the gate remains held throughout this wait.
    while (now_ms() <= before_end_ms + 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    gate->Release();
    EXPECT_EQ(std::future_status::ready, sweep_status);
    EXPECT_EQ(0u, sweeping.get());
    ASSERT_TRUE(ending.get());
    ASSERT_TRUE(while_inflight);
    EXPECT_EQ(WeightAvailabilityState::IMPORTING,
              while_inflight->metadata.availability);
    EXPECT_EQ(0u,
              MasterServiceTestPeer(primary).RunWeightReconciliationForTesting(
                  sweep_time, 32));
    const auto after = primary.GetWeightRevision({identity});
    ASSERT_TRUE(after);
    EXPECT_EQ(WeightAvailabilityState::IMPORTING, after->metadata.availability);
}

TEST_F(BatchOpLogSnapshotWriterTest,
       CapturesAppliedBoundaryBeforeGatedWeightOperationAndReplaysSuffix) {
    backend_ = std::make_shared<FakeHaKvBackend>();
    MasterService primary(
        MasterServiceConfig::builder()
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_weight_management_oplog_capability_confirmed(true)
            .set_cluster_id(kClusterId)
            .set_oplog_batch_max_entries(1)
            .build());
    ASSERT_EQ(ErrorCode::OK,
              MasterServiceTestPeer(primary).SetBatchOpLogBackendForTesting(
                  backend_));
    const auto wait_durable = [&](uint64_t sequence) {
        auto promise = std::make_shared<std::promise<ErrorCode>>();
        auto future = promise->get_future();
        MasterServiceTestPeer::OrderedOplogWriter(primary)
            ->AwaitDurable(sequence)
            .thenValue(
                [promise](ErrorCode error) { promise->set_value(error); });
        return future.wait_for(std::chrono::seconds(5)) ==
                   std::future_status::ready &&
               future.get() == ErrorCode::OK;
    };
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "snapshot-weight-segment";
    segment.te_endpoint = segment.name;
    segment.base = 0x100000000ULL;
    segment.size = 16 * 1024 * 1024;
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(primary.MountSegment(segment, client_id));
    ASSERT_TRUE(wait_durable(1));
    const auto identity = MakeWeightSnapshot(false).metadata.front().identity;
    const auto importing = primary.BeginWeightImport({
        .identity = identity,
        .expected_payload_count = 1,
        .expected_logical_bytes = 1024,
    });
    ASSERT_TRUE(importing);
    const std::string payload_key = "snapshot-weight-payload";
    const auto manifest_key = MakeWeightManifestKey(identity);
    ReplicateConfig replicate;
    replicate.replica_num = 1;
    replicate.with_hard_pin = true;
    replicate.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    uint64_t put_sequence = 3;
    for (const auto& key : {payload_key, manifest_key}) {
        replicate.data_type = key == payload_key ? ObjectDataType::WEIGHT
                                                 : ObjectDataType::METADATA;
        ASSERT_TRUE(primary.PutStart(client_id, key, TenantId::Default(),
                                     key == payload_key ? 1024 : 128,
                                     replicate));
        ASSERT_TRUE(primary.PutEnd(client_id, key, TenantId::Default(),
                                   ReplicaType::MEMORY));
        ASSERT_TRUE(wait_durable(put_sequence++));
    }
    const auto ready = primary.CommitWeightImport({
        .identity = identity,
        .expected_metadata_generation = importing->metadata_generation,
        .manifest =
            {
                .manifest_key = manifest_key,
                .manifest_sha256 = std::string(64, 'a'),
                .payload_group_id = importing->manifest.payload_group_id,
                .payload_keys_sha256 =
                    ComputeWeightPayloadKeysSha256({payload_key}),
                .payload_count = 1,
                .logical_bytes = 1024,
            },
    });
    ASSERT_TRUE(ready);
    const auto before =
        MasterServiceTestPeer::WeightMetadata(primary).ExportSnapshot();
    HotStandbyConfig config;
    config.enable_oplog_following = true;
    config.enable_verification = false;
    config.oplog_poll_interval_ms = 1;
    standby_ = std::make_unique<HotStandbyService>(config);
    standby_->SetCatchUpBatchKvBackendForTesting(backend_);
    ASSERT_EQ(ErrorCode::OK, standby_->Start("", "", kClusterId));

    std::future<WeightMetadataStore::Result<WeightResidencyOperation>>
        operation;
    // Release the transaction gate before the future and service destructors,
    // including when an assertion exits the test early.
    struct ReleaseGate {
        FakeHaKvBackend& backend;
        ~ReleaseGate() { backend.AllowTxn(); }
    } release{*backend_};
    backend_->BlockTxn();
    operation = std::async(std::launch::async, [&] {
        return primary.StartWeightResidencyOperation({
            .identity = identity,
            .expected_metadata_generation = ready->metadata_generation,
            .target_residency = WeightResidencyState::HOT,
        });
    });
    ASSERT_TRUE(backend_->WaitForBlockedTxn());
    OpLogBatchStorage storage(kClusterId, *backend_);
    DurablePrefix durable;
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(durable));
    ASSERT_EQ(5u, durable.last_seq);
    auto capture = standby_->BeginBatchOpLogSnapshotCapture();
    ASSERT_TRUE(capture);
    EXPECT_EQ(durable.last_seq, capture->last_included_seq);
    EXPECT_EQ(durable.batch_id, capture->last_included_batch_id);
    EXPECT_EQ(before, capture->weight_metadata);
    EXPECT_TRUE(capture->weight_metadata.operations.empty());
    EXPECT_EQ(std::future_status::timeout,
              operation.wait_for(std::chrono::milliseconds(0)));
    FakeObjectStore object_store;
    BatchOpLogSnapshotWriter writer(object_store);
    const auto descriptor =
        writer.Write(*standby_, *capture, "snapshots", "5-44", 2, 1234);
    ASSERT_TRUE(descriptor) << descriptor.error();
    const auto decoded = ha::DecodeBatchOpLogSnapshotDescriptor(*descriptor);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(5u, decoded->last_included_seq);
    std::vector<uint8_t> captured_weights;
    ASSERT_TRUE(object_store.DownloadBuffer(
        ha::BuildBatchOpLogSnapshotWeightMetadataKey("snapshots", "5-44"),
        captured_weights));
    const auto captured =
        DecodeBatchOpLogSnapshotWeightMetadata(captured_weights);
    ASSERT_TRUE(captured);
    EXPECT_EQ(before, *captured);
    standby_->Stop();

    backend_->AllowTxn();
    ASSERT_EQ(std::future_status::ready,
              operation.wait_for(std::chrono::seconds(5)));
    const auto committed = operation.get();
    ASSERT_TRUE(committed);
    MasterServiceTestPeer::OrderedOplogWriter(primary)->Stop();
    const auto expected =
        MasterServiceTestPeer::WeightMetadata(primary).ExportSnapshot();
    ASSERT_EQ(1u, expected.operations.size());
    EXPECT_EQ(committed->operation_id,
              expected.operations.front().operation_id);
    EXPECT_EQ(committed->operation_id + 1, expected.next_operation_id);
    DurablePrefix final_cursor;
    ASSERT_EQ(ErrorCode::OK, storage.ReadDurablePrefix(final_cursor));
    EXPECT_GT(final_cursor.last_seq, durable.last_seq);
    ASSERT_EQ(ErrorCode::OK,
              backend_->Put(ha::BuildBatchOpLogSnapshotLatestKey(kClusterId),
                            *descriptor));
    auto restored = Bootstrap(backend_, object_store);
    VerifyPromotion(*restored, expected, final_cursor);
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
