#include "ha/snapshot/batch_oplog/promotion.h"

#include <gtest/gtest.h>

#include <map>
#include <mutex>
#include <type_traits>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_provider.h"
#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"
#include "ha/standby_controller.h"
#include "hot_standby_service.h"
#include "master_service.h"

namespace mooncake::test {
namespace {

class MemoryBackend final : public HaKvBackend {
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

    ErrorCode Range(std::string_view begin, std::string_view end, size_t limit,
                    std::vector<KvPair>& output) override {
        std::lock_guard<std::mutex> lock(mutex_);
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
            auto error = Put(put.key, put.value);
            if (error != ErrorCode::OK) {
                return error;
            }
        }
        return ErrorCode::OK;
    }

   private:
    std::mutex mutex_;
    std::map<std::string, std::string> values_;
};

std::unique_ptr<HotStandbyService> MakeBatchSnapshotStandby(
    const std::shared_ptr<MemoryBackend>& backend,
    LocalFileSnapshotObjectStore& object_store,
    bool enable_oplog_following = true) {
    HotStandbyConfig config;
    config.enable_snapshot_bootstrap = false;
    config.enable_oplog_following = enable_oplog_following;
    config.enable_verification = false;
    config.oplog_poll_interval_ms = 1;
    config.batch_oplog_retry_timeout_sec = 0;
    auto standby = std::make_unique<HotStandbyService>(config);
    standby->SetCatchUpBatchKvBackendForTesting(backend);
    standby->SetBatchOpLogSnapshotProvider(
        std::make_unique<BatchOpLogSnapshotProvider>(
            "promotion-test", *backend, object_store, "snapshots"));
    return standby;
}

StandbyObjectMetadata MakeMetadata(uint64_t size) {
    StandbyObjectMetadata metadata;
    metadata.size = size;
    return metadata;
}

}  // namespace

static_assert(std::is_move_constructible_v<BatchOpLogPromotionHandoff>);
static_assert(!std::is_copy_constructible_v<BatchOpLogPromotionHandoff>);
static_assert(std::is_move_constructible_v<ha::PromotionContext>);
static_assert(!std::is_copy_constructible_v<ha::PromotionContext>);

TEST(BatchOpLogPromotionTest, EmptyStoreHandoffIsMoveOnly) {
    auto store = std::make_unique<StandbyMetadataStore>();
    BatchOpLogPromotionHandoff handoff;
    handoff.metadata_store = std::move(store);
    handoff.applied_cursor = {.batch_id = 0, .last_seq = 0};

    auto moved = std::move(handoff);
    ASSERT_TRUE(moved.metadata_store);
    EXPECT_EQ(0u, moved.metadata_store->GetKeyCount());

    ha::PromotionContext context;
    context.metadata_store = std::move(moved.metadata_store);
    EXPECT_TRUE(context.metadata_store);
}

TEST(BatchOpLogPromotionTest, StoreDrainsBoundedChunksAcrossTenants) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.PutMetadata("tenant-a", "a1", MakeMetadata(1)));
    ASSERT_TRUE(store.PutMetadata("tenant-a", "a2", MakeMetadata(2)));
    ASSERT_TRUE(store.PutMetadata("tenant-b", "b1", MakeMetadata(3)));

    std::vector<StandbyObjectEntry> chunk;
    ASSERT_TRUE(store.DrainChunk(2, chunk));
    EXPECT_EQ(2u, chunk.size());
    EXPECT_EQ(1u, store.GetKeyCount());
    ASSERT_TRUE(store.DrainChunk(2, chunk));
    EXPECT_EQ(1u, chunk.size());
    EXPECT_EQ(0u, store.GetKeyCount());
    ASSERT_TRUE(store.DrainChunk(2, chunk));
    EXPECT_TRUE(chunk.empty());
    EXPECT_FALSE(store.DrainChunk(0, chunk));
}

TEST(BatchOpLogPromotionTest, DetachesStoreWithFinalCursorAndProducerView) {
    auto backend = std::make_shared<MemoryBackend>();
    ASSERT_EQ(
        ErrorCode::OK,
        backend->Put(BuildDurablePrefixKey("promotion-test"),
                     EncodeDurablePrefix({.batch_id = 0, .last_seq = 0})));
    ASSERT_EQ(ErrorCode::OK,
              backend->Put(BuildProducerViewKey("promotion-test"), "7"));
    LocalFileSnapshotObjectStore object_store(
        "/tmp/mooncake-n07-promotion-success");
    auto standby = MakeBatchSnapshotStandby(backend, object_store);
    ASSERT_EQ(ErrorCode::OK, standby->Start("", "", "promotion-test"));

    auto handoff = standby->PromoteAndDetachBatchOpLogStore();

    ASSERT_TRUE(handoff.has_value());
    ASSERT_TRUE(handoff->metadata_store);
    EXPECT_EQ(DurablePrefix(), handoff->applied_cursor);
    EXPECT_EQ(7, handoff->producer_view_version);
    std::vector<StandbyObjectEntry> ignored;
    EXPECT_FALSE(standby->ExportMetadataSnapshot(ignored));
}

TEST(BatchOpLogPromotionTest, EmptyStoreWithoutDurablePrefixUsesZeroCursor) {
    auto backend = std::make_shared<MemoryBackend>();
    LocalFileSnapshotObjectStore object_store(
        "/tmp/mooncake-n07-promotion-missing-cursor");
    auto standby = MakeBatchSnapshotStandby(backend, object_store);
    ASSERT_EQ(ErrorCode::OK, standby->Start("", "", "promotion-test"));

    auto handoff = standby->PromoteAndDetachBatchOpLogStore();

    ASSERT_TRUE(handoff.has_value());
    EXPECT_EQ(DurablePrefix(), handoff->applied_cursor);
    ASSERT_TRUE(handoff->metadata_store);
    EXPECT_EQ(StandbyState::STOPPED, standby->GetState());
    std::vector<StandbyObjectEntry> retained;
    EXPECT_FALSE(standby->ExportMetadataSnapshot(retained));
}

TEST(BatchOpLogPromotionTest, SnapshotOnlyProviderFallsBackToLegacyExport) {
    auto backend = std::make_shared<MemoryBackend>();
    LocalFileSnapshotObjectStore object_store(
        "/tmp/mooncake-n07-promotion-snapshot-only");
    auto standby = MakeBatchSnapshotStandby(backend, object_store,
                                            /*enable_oplog_following=*/false);
    ASSERT_EQ(ErrorCode::OK, standby->Start("", "", "promotion-test"));

    EXPECT_FALSE(standby->IsBatchOpLogSnapshotMode());
    auto detach = standby->PromoteAndDetachBatchOpLogStore();
    ASSERT_FALSE(detach.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, detach.error());
    StandbySnapshot snapshot;
    ASSERT_EQ(ErrorCode::OK, standby->PromoteAndExportSnapshot(snapshot));
    EXPECT_EQ(0u, snapshot.oplog_sequence_id);
    EXPECT_TRUE(snapshot.objects.empty());
}

TEST(BatchOpLogPromotionTest, EmptyStoreRestoresWithoutChunks) {
    MasterService service(
        MasterServiceConfig::builder().set_enable_ha(false).build());
    BatchOpLogPromotionHandoff handoff;
    handoff.metadata_store = std::make_unique<StandbyMetadataStore>();
    handoff.applied_cursor = {.batch_id = 0, .last_seq = 0};

    EXPECT_TRUE(service.RestoreFromBatchOpLogPromotion(std::move(handoff), 1)
                    .has_value());
}

}  // namespace mooncake::test
