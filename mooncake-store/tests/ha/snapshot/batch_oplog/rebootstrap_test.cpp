#include "hot_standby_service.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <mutex>
#include <thread>

#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/snapshot/batch_oplog/codec.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"

namespace mooncake::test {
namespace {

class MemoryBackend final : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = values_.find(std::string(key));
        if (it == values_.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
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
        for (auto it = values_.lower_bound(std::string(begin));
             it != values_.end() && it->first < end &&
             (limit == 0 || output.size() < limit);
             ++it) {
            output.push_back({it->first, it->second});
        }
        return ErrorCode::OK;
    }
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn& txn) override {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& put : txn.puts) values_[put.key] = put.value;
        return ErrorCode::OK;
    }

   private:
    std::mutex mutex_;
    std::map<std::string, std::string> values_;
};

template <typename Predicate>
bool WaitUntil(Predicate predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

class RebootstrapTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char pattern[] = "/tmp/mooncake-n09-XXXXXX";
        const auto* directory = mkdtemp(pattern);
        ASSERT_NE(nullptr, directory);
        root_ = directory;
        objects_ = std::make_unique<LocalFileSnapshotObjectStore>(root_);
        HotStandbyConfig config;
        config.enable_snapshot_bootstrap = true;
        config.enable_verification = false;
        config.oplog_poll_interval_ms = 1;
        service_ = std::make_unique<HotStandbyService>(config);
        service_->SetCatchUpBatchKvBackendForTesting(backend_);
        service_->SetBatchOpLogSnapshotProvider(
            std::make_unique<BatchOpLogSnapshotProvider>(
                "n09", *backend_, *objects_, "snapshots"));
        Publish(1, "old", false);
        OpLogEntry initial;
        initial.sequence_id = 1;
        initial.op_type = OpType::REMOVE;
        initial.object_key = "absent";
        OpLogBatchRecord batch;
        batch.batch_id = batch.first_seq = batch.last_seq = 1;
        batch.entries = {initial};
        backend_->Put(BuildBatchRecordKey("n09", 1),
                      EncodeOpLogBatchRecord(batch));
        Prefix(1);
        ASSERT_EQ(ErrorCode::OK, service_->Start("", "", "n09"));
    }
    void TearDown() override {
        ReleaseSwap();
        service_->Stop();
        if (armed_) unsetenv("MOONCAKE_TEST_FAILPOINT_DIR");
        std::filesystem::remove_all(root_);
    }
    void Prefix(uint64_t id) {
        backend_->Put(BuildDurablePrefixKey("n09"),
                      EncodeDurablePrefix({.batch_id = id, .last_seq = id}));
    }
    void Publish(uint64_t id, const std::string& key, bool fallback) {
        const auto snapshot_id = std::to_string(id) + "-1";
        const auto segments = EncodeBatchOpLogSnapshotSegments(
            {{.segment_name = key,
              .transport_endpoint = key + ":1234",
              .file_path = ""}});
        const auto segment_key =
            ha::BuildBatchOpLogSnapshotSegmentsKey("snapshots", snapshot_id);
        ASSERT_TRUE(objects_->UploadBuffer(segment_key, segments));
        const auto chunk = EncodeBatchOpLogSnapshotObjectChunk(
            0, {{.key = key, .metadata = {}},
                {.key = "remove-me", .metadata = {}}});
        const auto chunk_key = ha::BuildBatchOpLogSnapshotObjectChunkKey(
            "snapshots", snapshot_id, 0);
        ASSERT_TRUE(objects_->UploadBuffer(chunk_key, chunk));
        ha::BatchOpLogSnapshotManifest manifest;
        manifest.snapshot_id = snapshot_id;
        manifest.last_included_batch_id = id;
        manifest.last_included_seq = id;
        manifest.segments = {
            .key = segment_key,
            .stored_size = segments.size(),
            .crc32c = Crc32cValue(segments.data(), segments.size())};
        manifest.object_chunks = {
            {.chunk_index = 0,
             .key = chunk_key,
             .object_count = 2,
             .stored_size = chunk.size(),
             .crc32c = Crc32cValue(chunk.data(), chunk.size())}};
        const auto bytes = ha::EncodeBatchOpLogSnapshotManifest(manifest);
        ha::BatchOpLogSnapshotDescriptor descriptor;
        descriptor.snapshot_id = snapshot_id;
        descriptor.last_included_batch_id = id;
        descriptor.last_included_seq = id;
        descriptor.manifest_key =
            ha::BuildBatchOpLogSnapshotManifestKey("snapshots", snapshot_id);
        descriptor.manifest_size = bytes.size();
        descriptor.manifest_crc32c = Crc32cValue(bytes.data(), bytes.size());
        ASSERT_TRUE(objects_->UploadString(descriptor.manifest_key, bytes));
        const auto encoded = ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
        ASSERT_TRUE(objects_->UploadString(
            ha::BuildBatchOpLogSnapshotDescriptorKey("snapshots", snapshot_id),
            encoded));
        backend_->Put(fallback ? ha::BuildBatchOpLogSnapshotFallbackKey("n09")
                               : ha::BuildBatchOpLogSnapshotLatestKey("n09"),
                      encoded);
    }
    void Compact() {
        // Freeze at a complete batch so the reader observes one new history.
        auto capture = service_->BeginBatchOpLogSnapshotCapture();
        ASSERT_TRUE(capture);
        OpLogEntry entry;
        entry.sequence_id = 4;
        entry.op_type = OpType::REMOVE;
        entry.tenant_id = "default";
        entry.object_key = "remove-me";
        OpLogBatchRecord batch;
        batch.batch_id = batch.first_seq = batch.last_seq = 4;
        batch.entries = {entry};
        backend_->Put(BuildBatchRecordKey("n09", 4),
                      EncodeOpLogBatchRecord(batch));
        Prefix(4);
        backend_->Put(ha::BuildBatchOpLogSnapshotCompactionFloorKey("n09"),
                      "3");
        service_->EndBatchOpLogSnapshotCapture(*capture);
    }
    void ExpectOldState() {
        StandbySnapshot snapshot;
        ASSERT_TRUE(service_->ExportStandbySnapshot(snapshot));
        EXPECT_EQ(1u, snapshot.oplog_sequence_id);
        ASSERT_EQ(2u, snapshot.objects.size());
        ASSERT_EQ(1u, snapshot.segments.size());
        EXPECT_EQ("old", snapshot.segments[0].segment_name);
        EXPECT_FALSE(service_->IsReadyForPromotion());
        EXPECT_FALSE(service_->BeginBatchOpLogSnapshotCapture());
        EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
                  service_->Promote());
    }
    void ExpectRecovered() {
        ASSERT_TRUE(WaitUntil([&] {
            return service_->GetLatestAppliedSequenceId() == 4 &&
                   !service_->GetSyncStatus().is_recovering;
        }));
        EXPECT_TRUE(service_->IsReadyForPromotion());
        auto handoff = service_->PromoteAndDetachBatchOpLogStore();
        ASSERT_TRUE(handoff);
        EXPECT_EQ(4u, handoff->applied_cursor.batch_id);
        EXPECT_EQ(4u, handoff->applied_cursor.last_seq);
        EXPECT_EQ(1u, handoff->metadata_store->GetKeyCount());
        EXPECT_TRUE(handoff->metadata_store->Exists("new"));
        ASSERT_EQ(1u, handoff->segments.size());
        EXPECT_EQ("new", handoff->segments[0].segment_name);
    }
    void ArmSwap() {
        setenv("MOONCAKE_TEST_FAILPOINT_DIR", root_.c_str(), 1);
        armed_ = true;
        std::ofstream(root_ + "/standby_rebootstrap_before_swap.arm");
    }
    void ReleaseSwap() {
        if (armed_)
            std::ofstream(root_ + "/standby_rebootstrap_before_swap.release");
    }
    std::string root_;
    bool armed_{false};
    std::shared_ptr<MemoryBackend> backend_{std::make_shared<MemoryBackend>()};
    std::unique_ptr<LocalFileSnapshotObjectStore> objects_;
    std::unique_ptr<HotStandbyService> service_;
};

TEST_F(RebootstrapTest, CorruptLatestUsesFallbackAndReplaysSuffix) {
    Publish(3, "new", true);
    backend_->Put(ha::BuildBatchOpLogSnapshotLatestKey("n09"), "{}");
    Compact();
    ExpectRecovered();
}

TEST_F(RebootstrapTest, NoEligibleSnapshotPreservesStateAndRetriesOnline) {
    Compact();
    ASSERT_TRUE(
        WaitUntil([&] { return service_->GetSyncStatus().is_recovering; }));
    ExpectOldState();
    Publish(3, "new", false);
    ExpectRecovered();
}

TEST_F(RebootstrapTest, CorruptCandidatePreservesOldStateUntilRepaired) {
    Publish(3, "new", false);
    ASSERT_TRUE(objects_->UploadString(
        ha::BuildBatchOpLogSnapshotObjectChunkKey("snapshots", "3-1", 0),
        "corrupt"));
    Compact();
    ASSERT_TRUE(
        WaitUntil([&] { return service_->GetSyncStatus().is_recovering; }));
    ExpectOldState();
    Publish(3, "new", false);
    ExpectRecovered();
}

#ifdef MOONCAKE_ENABLE_TEST_FAILPOINTS
TEST_F(RebootstrapTest, PromotionDuringSwapFailsClosedThenSucceeds) {
    ArmSwap();
    Publish(3, "new", false);
    Compact();
    ASSERT_TRUE(WaitUntil([&] {
        return std::filesystem::exists(root_ +
                                       "/standby_rebootstrap_before_swap.hit");
    }));
    ExpectOldState();
    ReleaseSwap();
    ExpectRecovered();
}

TEST_F(RebootstrapTest, StopDuringSwapDoesNotDeadlockOrInstallCandidate) {
    ArmSwap();
    Publish(3, "new", false);
    Compact();
    ASSERT_TRUE(WaitUntil([&] {
        return std::filesystem::exists(root_ +
                                       "/standby_rebootstrap_before_swap.hit");
    }));
    auto stopped = std::async(std::launch::async, [&] { service_->Stop(); });
    EXPECT_TRUE(WaitUntil(
        [&] { return service_->GetState() == StandbyState::STOPPED; }));
    ReleaseSwap();
    ASSERT_EQ(std::future_status::ready,
              stopped.wait_for(std::chrono::seconds(5)));
    EXPECT_EQ(1u, service_->GetLatestAppliedSequenceId());
    EXPECT_EQ(2u, service_->GetMetadataCount());
}

TEST_F(RebootstrapTest, FloorAdvancingBeforeSwapRejectsCandidate) {
    ArmSwap();
    Publish(3, "new", false);
    Compact();
    ASSERT_TRUE(WaitUntil([&] {
        return std::filesystem::exists(root_ +
                                       "/standby_rebootstrap_before_swap.hit");
    }));
    backend_->Put(ha::BuildBatchOpLogSnapshotCompactionFloorKey("n09"), "4");
    ReleaseSwap();
    ASSERT_TRUE(WaitUntil([&] {
        return !std::filesystem::exists(root_ +
                                        "/standby_rebootstrap_before_swap.hit");
    }));
    ExpectOldState();
    Publish(4, "new", false);
    ASSERT_TRUE(WaitUntil([&] {
        return service_->GetLatestAppliedSequenceId() == 4 &&
               !service_->GetSyncStatus().is_recovering;
    }));
    EXPECT_TRUE(service_->IsReadyForPromotion());
}
#endif

}  // namespace
}  // namespace mooncake::test
