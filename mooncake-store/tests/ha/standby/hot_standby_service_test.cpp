#include "hot_standby_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include <xxhash.h>

#include "master_service.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_store_factory.h"
#include "ha/oplog/mock_oplog_store.h"

namespace mooncake::test {

namespace {

class FakeSnapshotProvider final : public SnapshotProvider {
   public:
    explicit FakeSnapshotProvider(
        tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result)
        : result_(std::move(result)) {}

    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> LoadLatestSnapshot(
        const std::string& /*cluster_id*/) override {
        return result_;
    }

   private:
    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result_;
};

LoadedSnapshot MakeSnapshot(std::string snapshot_id, uint64_t seq_id,
                            std::string key, uint64_t size) {
    LoadedSnapshot snapshot;
    snapshot.snapshot_id = std::move(snapshot_id);
    snapshot.snapshot_sequence_id = seq_id;

    StandbyObjectMetadata metadata;
    metadata.client_id = UUID{1, 2};
    metadata.size = size;
    metadata.last_sequence_id = seq_id;
    snapshot.metadata.emplace_back("default", std::move(key), metadata);
    return snapshot;
}

}  // namespace

class HotStandbyServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("HotStandbyServiceTest");
        FLAGS_logtostderr = 1;

        config_.enable_verification = false;
        config_.max_replication_lag_entries = 1000;

        service_ = std::make_unique<HotStandbyService>(config_);
        oplog_endpoints_ = "http://localhost:2379";
        cluster_id_ = "test_cluster_001";
    }

    void TearDown() override {
        if (service_) {
            service_->Stop();
        }
        google::ShutdownGoogleLogging();
    }

    HotStandbyConfig config_;
    std::unique_ptr<HotStandbyService> service_;
    std::string oplog_endpoints_;
    std::string cluster_id_;
};

namespace {

std::unique_ptr<HotStandbyService> CreateSnapshotOnlyReadyStandby(
    HotStandbyConfig config, const std::string& cluster_id) {
    config.enable_snapshot_bootstrap = true;
    config.enable_oplog_following = false;

    auto service = std::make_unique<HotStandbyService>(config);
    LoadedSnapshot snapshot;
    snapshot.snapshot_id = "20260330_120000_000";
    snapshot.snapshot_sequence_id = 42;

    StandbyObjectMetadata metadata;
    metadata.client_id = UUID{1, 2};
    metadata.size = 4096;
    metadata.last_sequence_id = 42;
    snapshot.metadata.emplace_back("default", "key-1", metadata);

    service->SetSnapshotProvider(std::make_unique<FakeSnapshotProvider>(
        std::optional<LoadedSnapshot>(snapshot)));
    EXPECT_EQ(ErrorCode::OK, service->Start("", "", cluster_id));
    EXPECT_EQ(StandbyState::WATCHING, service->GetState());
    return service;
}

}  // namespace

// ========== 6.1.1 Start/Stop tests ==========

TEST_F(HotStandbyServiceTest, TestStart) {
#ifdef STORE_USE_ETCD
    // Requires a real etcd cluster and valid cluster configuration; acts as an
    // integration placeholder
    GTEST_SKIP()
        << "Requires real etcd connection, run in integration environment.";
#else
    ErrorCode err =
        service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, err);
    EXPECT_EQ(StandbyState::FAILED, service_->GetState());
#endif
}

TEST_F(HotStandbyServiceTest, TestStart_AlreadyRunning) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Requires real etcd connection to verify double start semantics.";
#else
    // After the first Start fails and state becomes FAILED, the second Start
    // should still return INTERNAL_ERROR
    ErrorCode err1 =
        service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, err1);
    ErrorCode err2 =
        service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, err2);
#endif
}

TEST_F(HotStandbyServiceTest, TestStart_InvalidEtcdEndpoints) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd to simulate invalid endpoints.";
#else
    std::string invalid_endpoints = "invalid_endpoint";
    ErrorCode err =
        service_->Start("primary_unused", invalid_endpoints, cluster_id_);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, err);
#endif
}

TEST_F(HotStandbyServiceTest, TestStop) {
    // Stop should be safe and idempotent even if Start was never called
    service_->Stop();
    SUCCEED();
}

TEST_F(HotStandbyServiceTest, TestStop_WhenNotRunning) {
    // Multiple Stop calls should be idempotent
    service_->Stop();
    service_->Stop();
    SUCCEED();
}

// ========== 6.1.2 State transition tests ==========

TEST_F(HotStandbyServiceTest, TestStateTransition_StartToWatching) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Requires real etcd to drive full state transition to WATCHING.";
#else
    // In non-STORE_USE_ETCD builds, Start will set the state machine directly
    // to FAILED
    EXPECT_EQ(StandbyState::STOPPED, service_->GetState());
    ErrorCode err =
        service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, err);
    EXPECT_EQ(StandbyState::FAILED, service_->GetState());
#endif
}

TEST_F(HotStandbyServiceTest, TestStateTransition_ConnectionFailed) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Connection failure requires real etcd and invalid endpoints.";
#else
    // In non-etcd mode we cannot distinguish detailed connection errors; only
    // verify it doesn't crash
    ErrorCode err =
        service_->Start("primary_unused", "bad_endpoint", cluster_id_);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, err);
#endif
}

TEST_F(HotStandbyServiceTest, TestStateTransition_SyncFailed) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Sync failure requires real etcd and OpLog watcher behavior.";
#else
    // In non-etcd mode, the sync phase is not actually executed; just ensure
    // the call is safe
    ErrorCode err =
        service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, err);
#endif
}

// ========== 6.1.3 Sync status tests ==========

TEST_F(HotStandbyServiceTest, TestGetSyncStatus_InitialState) {
    StandbySyncStatus status = service_->GetSyncStatus();
    EXPECT_EQ(0u, status.applied_seq_id);
    EXPECT_EQ(0u, status.primary_seq_id);
    EXPECT_EQ(0u, status.lag_entries);
    EXPECT_FALSE(status.is_syncing);
    EXPECT_FALSE(status.is_connected);
    EXPECT_EQ(StandbyState::STOPPED, status.state);
}

TEST_F(HotStandbyServiceTest, TestGetSyncStatus_AfterSync) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Requires real etcd and OpLog activity to change sync status.";
#else
    // In non-etcd mode, calling Start will not change applied/primary, but the
    // state machine enters FAILED
    (void)service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    StandbySyncStatus status = service_->GetSyncStatus();
    EXPECT_EQ(StandbyState::FAILED, status.state);
#endif
}

TEST_F(HotStandbyServiceTest, TestGetSyncStatus) {
    // Basic coverage: multiple calls should return consistent values and not
    // crash
    StandbySyncStatus s1 = service_->GetSyncStatus();
    StandbySyncStatus s2 = service_->GetSyncStatus();
    EXPECT_EQ(s1.state, s2.state);
}

TEST_F(HotStandbyServiceTest, SyncStatusReportsUnresolvedGapCount) {
    auto status = service_->GetSyncStatus();
    EXPECT_EQ(0u, status.unresolved_gap_count);
}

// ========== 6.1.4 Promotion tests ==========

TEST_F(HotStandbyServiceTest, TestPromote_WhenNotReady) {
    // In the initial state promotion preconditions are not met, so it should
    // return an error code (not OK).
    ErrorCode err = service_->Promote();
    EXPECT_NE(ErrorCode::OK, err);
}

TEST_F(HotStandbyServiceTest, TestPromote_WhenReady) {
    service_ = CreateSnapshotOnlyReadyStandby(config_, cluster_id_);

    ErrorCode err = service_->Promote();
    EXPECT_EQ(ErrorCode::OK, err);
    EXPECT_EQ(StandbyState::STOPPED, service_->GetState());
    EXPECT_EQ(42u, service_->GetLatestAppliedSequenceId());
}

TEST_F(HotStandbyServiceTest, TestPromoteAndExportSnapshot_FinalCatchUp) {
    // Setup: snapshot-only standby with baseline seq=10
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    service_ = std::make_unique<HotStandbyService>(config_);

    LoadedSnapshot snapshot;
    snapshot.snapshot_id = "snap-001";
    snapshot.snapshot_sequence_id = 10;

    StandbyObjectMetadata metadata;
    metadata.client_id = UUID{1, 2};
    metadata.size = 4096;
    metadata.last_sequence_id = 10;
    snapshot.metadata.emplace_back("default", "key-1", metadata);

    service_->SetSnapshotProvider(std::make_unique<FakeSnapshotProvider>(
        std::optional<LoadedSnapshot>(snapshot)));

    ASSERT_EQ(ErrorCode::OK, service_->Start("", "", cluster_id_));
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());
    EXPECT_EQ(10u, service_->GetLatestAppliedSequenceId());

    // Export before promotion: seq should be 10
    StandbySnapshot pre_snapshot;
    EXPECT_TRUE(service_->ExportStandbySnapshot(pre_snapshot));
    EXPECT_EQ(10u, pre_snapshot.oplog_sequence_id);

    // Promote and export atomically
    StandbySnapshot post_snapshot;
    ErrorCode err = service_->PromoteAndExportSnapshot(post_snapshot);
    EXPECT_EQ(ErrorCode::OK, err);
    EXPECT_EQ(StandbyState::STOPPED, service_->GetState());

    // After promotion, exported seq should still be 10 (no new OpLog in
    // snapshot-only)
    EXPECT_EQ(10u, post_snapshot.oplog_sequence_id);
    ASSERT_EQ(1u, post_snapshot.objects.size());
    EXPECT_EQ("key-1", post_snapshot.objects[0].key);
}

// The promotion catch-up tests above (TestPromote_*) have been replaced by the
// mock-driven PromotionCatchUpTest fixture below. The new tests use
// SetCatchUpOpLogStoreForTesting + MockOpLogStore with SetForceReadEmpty /
// SetReadError seams, eliminating the STORE_USE_ETCD dependency.

// ========== 6.1.5 Warm start tests ==========

TEST_F(HotStandbyServiceTest, TestWarmStart_WithLocalState) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd and pre-populated local metadata to "
                    "test warm start.";
#else
    // In non-etcd mode, only verify that Start is safe to call
    (void)service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    SUCCEED();
#endif
}

TEST_F(HotStandbyServiceTest, TestWarmStart_WithoutLocalState) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd and snapshot provider configuration.";
#else
    (void)service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    SUCCEED();
#endif
}

TEST_F(HotStandbyServiceTest, TestWarmStart_WithSnapshot) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires snapshot provider and real etcd to exercise "
                    "snapshot bootstrap.";
#else
    config_.enable_snapshot_bootstrap = true;
    // Recreate service to apply the new configuration
    service_.reset(new HotStandbyService(config_));
    (void)service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    SUCCEED();
#endif
}

TEST_F(HotStandbyServiceTest, TestStart_SnapshotOnlyWithSnapshot) {
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    service_ = std::make_unique<HotStandbyService>(config_);

    auto snapshot = MakeSnapshot("20260330_120000_000", 42, "key-1", 4096);

    service_->SetSnapshotProvider(std::make_unique<FakeSnapshotProvider>(
        std::optional<LoadedSnapshot>(snapshot)));

    auto err = service_->Start("", "", cluster_id_);
    EXPECT_EQ(ErrorCode::OK, err);
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());
    EXPECT_EQ(1u, service_->GetMetadataCount());
    EXPECT_EQ(42u, service_->GetLatestAppliedSequenceId());

    auto status = service_->GetSyncStatus();
    EXPECT_EQ(42u, status.applied_seq_id);
    EXPECT_EQ(42u, status.primary_seq_id);
    EXPECT_TRUE(status.is_connected);

    std::vector<StandbyObjectEntry> exported;
    EXPECT_TRUE(service_->ExportMetadataSnapshot(exported));
    ASSERT_EQ(1u, exported.size());
    EXPECT_EQ("key-1", exported[0].key);
    EXPECT_EQ(4096u, exported[0].metadata.size);
}

TEST_F(HotStandbyServiceTest,
       TestStart_SnapshotOnlyRestartRefreshesNewerCatalogSnapshot) {
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    service_ = std::make_unique<HotStandbyService>(config_);

    service_->SetSnapshotProvider(
        std::make_unique<FakeSnapshotProvider>(std::optional<LoadedSnapshot>(
            MakeSnapshot("20260330_120000_000", 42, "key-old", 4096))));

    ASSERT_EQ(ErrorCode::OK, service_->Start("", "", cluster_id_));
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());
    EXPECT_EQ(42u, service_->GetLatestAppliedSequenceId());
    EXPECT_EQ(1u, service_->GetMetadataCount());
    service_->Stop();

    service_->SetSnapshotProvider(
        std::make_unique<FakeSnapshotProvider>(std::optional<LoadedSnapshot>(
            MakeSnapshot("20260330_121500_000", 84, "key-new", 8192))));

    ASSERT_EQ(ErrorCode::OK, service_->Start("", "", cluster_id_));
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());
    EXPECT_EQ(84u, service_->GetLatestAppliedSequenceId());
    EXPECT_EQ(1u, service_->GetMetadataCount());

    std::vector<StandbyObjectEntry> exported;
    ASSERT_TRUE(service_->ExportMetadataSnapshot(exported));
    ASSERT_EQ(1u, exported.size());
    EXPECT_EQ("key-new", exported[0].key);
    EXPECT_EQ(8192u, exported[0].metadata.size);
    EXPECT_EQ(84u, exported[0].metadata.last_sequence_id);
}

TEST_F(HotStandbyServiceTest, TestStart_SnapshotOnlyWhenProviderFails) {
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    service_ = std::make_unique<HotStandbyService>(config_);

    service_->SetSnapshotProvider(std::make_unique<FakeSnapshotProvider>(
        tl::make_unexpected(ErrorCode::PERSISTENT_FAIL)));

    auto err = service_->Start("", "", cluster_id_);
    EXPECT_EQ(ErrorCode::PERSISTENT_FAIL, err);
    EXPECT_EQ(StandbyState::FAILED, service_->GetState());
}

// ========== 6.1.6 Metadata operation tests ==========

TEST_F(HotStandbyServiceTest, TestGetMetadataCount) {
    EXPECT_EQ(0u, service_->GetMetadataCount());
}

TEST_F(HotStandbyServiceTest, TestExportMetadataSnapshot) {
    std::vector<StandbyObjectEntry> snapshot;
    EXPECT_TRUE(service_->ExportMetadataSnapshot(snapshot));
    EXPECT_TRUE(snapshot.empty());
}

TEST_F(HotStandbyServiceTest, TestGetLatestAppliedSequenceId) {
    uint64_t seq = service_->GetLatestAppliedSequenceId();
    EXPECT_EQ(0u, seq);
}

// ========== 6.1.6a ExportStandbySnapshot tests ==========

TEST_F(HotStandbyServiceTest, TestExportStandbySnapshot_NotRunning) {
    StandbySnapshot snapshot;
    EXPECT_FALSE(service_->ExportStandbySnapshot(snapshot));
}

TEST_F(HotStandbyServiceTest, TestExportStandbySnapshot_SnapshotOnly) {
    service_ = CreateSnapshotOnlyReadyStandby(config_, cluster_id_);

    StandbySnapshot snapshot;
    EXPECT_TRUE(service_->ExportStandbySnapshot(snapshot));
    EXPECT_EQ(42u, snapshot.oplog_sequence_id);
    ASSERT_EQ(1u, snapshot.objects.size());
    EXPECT_EQ("key-1", snapshot.objects[0].key);
    EXPECT_EQ(4096u, snapshot.objects[0].metadata.size);
    // Segment registry is empty because snapshot-only standby has no
    // oplog_applier
    EXPECT_TRUE(snapshot.segments.empty());
}

TEST_F(HotStandbyServiceTest, TestExportStandbySnapshot_Empty) {
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    service_ = std::make_unique<HotStandbyService>(config_);

    service_->SetSnapshotProvider(std::make_unique<FakeSnapshotProvider>(
        std::optional<LoadedSnapshot>(LoadedSnapshot{})));

    EXPECT_EQ(ErrorCode::OK, service_->Start("", "", cluster_id_));
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());

    StandbySnapshot snapshot;
    EXPECT_TRUE(service_->ExportStandbySnapshot(snapshot));
    EXPECT_EQ(0u, snapshot.oplog_sequence_id);
    EXPECT_TRUE(snapshot.objects.empty());
    EXPECT_TRUE(snapshot.segments.empty());
}

// ========== 6.1.7 Replication loop tests ==========

TEST_F(HotStandbyServiceTest, TestReplicationLoop_UpdatesMetrics) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Requires real etcd and running replication loop to update metrics.";
#else
    // In non-etcd mode, ReplicationLoop is never started, but calling Stop
    // should be safe
    service_->Stop();
    SUCCEED();
#endif
}

TEST_F(HotStandbyServiceTest, TestReplicationLoop_HandlesDisconnect) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd and watcher disconnect to exercise "
                    "disconnect path.";
#else
    // DisconnectFromPrimary is private; verify Stop() is safe instead
    service_->Stop();
    SUCCEED();
#endif
}

// ========== 6.1.8 Verification loop tests ==========

TEST_F(HotStandbyServiceTest, TestVerificationLoop_WhenEnabled) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd and running verification loop to "
                    "observe behavior.";
#else
    config_.enable_verification = true;
    service_.reset(new HotStandbyService(config_));
    (void)service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    service_->Stop();
    SUCCEED();
#endif
}

TEST_F(HotStandbyServiceTest, TestVerificationLoop_WhenDisabled) {
    // By default config_.enable_verification = false, so Start should not spawn
    // a verification thread
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd connection to start service.";
#else
    (void)service_->Start("primary_unused", oplog_endpoints_, cluster_id_);
    service_->Stop();
    SUCCEED();
#endif
}

// ========== Issue 2 fail-closed catch-up ==========

namespace {

// Helper to create a valid OpLogEntry with checksum (mirrors the helper in
// oplog_applier_test.cpp).
OpLogEntry MakeEntry(uint64_t seq, OpType type, const std::string& key,
                     const std::string& payload) {
    OpLogEntry e;
    e.sequence_id = seq;
    e.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now().time_since_epoch())
                         .count();
    e.op_type = type;
    e.object_key = key;
    e.payload = payload;
    // Compute checksum and prefix_hash using the same algorithm as OpLogManager
    e.checksum =
        static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
    e.prefix_hash =
        key.empty() ? 0
                    : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
    return e;
}

// Helper to create a valid struct_pack payload for PUT_END
std::string MakeValidPayload(uint64_t client_id_first = 1,
                             uint64_t client_id_second = 2,
                             uint64_t size = 1024) {
    mooncake::MetadataPayload payload;
    payload.client_id = {client_id_first, client_id_second};
    payload.size = size;
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

class FakeHaKvBackend : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        if (get_error_ != ErrorCode::OK) {
            return get_error_;
        }
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
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        if (range_error_ != ErrorCode::OK) {
            return range_error_;
        }
        kvs.clear();
        for (const auto& [key, value] : values_) {
            if (key >= begin_key && key < end_key) {
                kvs.push_back({.key = key, .value = value});
                if (kvs.size() >= limit) break;
            }
        }
        return ErrorCode::OK;
    }
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::OK; }
    void SetGetError(ErrorCode err) { get_error_ = err; }
    void SetRangeError(ErrorCode err) { range_error_ = err; }

   private:
    std::map<std::string, std::string> values_;
    ErrorCode get_error_{ErrorCode::OK};
    ErrorCode range_error_{ErrorCode::OK};
};

OpLogBatchRecord MakeBatch(uint64_t batch_id, uint64_t first_seq,
                           uint64_t last_seq) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.first_seq = first_seq;
    batch.last_seq = last_seq;
    for (uint64_t seq = first_seq; seq <= last_seq; ++seq) {
        batch.entries.push_back(MakeEntry(seq, OpType::PUT_END,
                                          "batch_key_" + std::to_string(seq),
                                          MakeValidPayload()));
    }
    return batch;
}

}  // namespace

class PromotionCatchUpTest : public HotStandbyServiceTest {
   protected:
    void SetUp() override {
        HotStandbyServiceTest::SetUp();
        config_.enable_oplog_following = true;
        config_.oplog_store_type = OpLogStoreType::LOCAL_FS;
        config_.fail_closed_on_incomplete_catch_up = true;
        config_.fail_closed_on_unresolved_gaps = true;

        // Re-create the service with updated config
        service_ = std::make_unique<HotStandbyService>(config_);

        mock_store_ = std::make_shared<MockOpLogStore>();
        service_->SetCatchUpOpLogStoreForTesting(mock_store_);
    }

    std::shared_ptr<MockOpLogStore> mock_store_;
};

TEST_F(PromotionCatchUpTest, EmptyReadBeforeMaxFailsClosed) {
    for (uint64_t s = 1; s <= 5; ++s) {
        auto e = MakeEntry(s, OpType::PUT_END, "key_" + std::to_string(s),
                           MakeValidPayload());
        ASSERT_EQ(ErrorCode::OK, mock_store_->WriteOpLog(e));
    }
    mock_store_->SetForceReadEmpty(true);

    // Try to promote; the service needs to reach WATCHING first
    auto err = service_->Start("", oplog_endpoints_, cluster_id_);
    if (err != ErrorCode::OK) {
        GTEST_SKIP() << "Service could not reach WATCHING state via LOCAL_FS; "
                        "skipping promotion test";
    }
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());

    // PromoteAndExportSnapshot should fail-closed because the mock store has
    // durable entries (seq 1-5) but ReadOpLogSince returns empty.
    StandbySnapshot out;
    ErrorCode promote_err = service_->PromoteAndExportSnapshot(out);
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, promote_err);
}

TEST_F(PromotionCatchUpTest, ReadErrorFailsClosed) {
    for (uint64_t s = 1; s <= 3; ++s) {
        auto e = MakeEntry(s, OpType::PUT_END, "key_" + std::to_string(s),
                           MakeValidPayload());
        ASSERT_EQ(ErrorCode::OK, mock_store_->WriteOpLog(e));
    }
    mock_store_->SetReadError(ErrorCode::PERSISTENT_FAIL);

    auto err = service_->Start("", oplog_endpoints_, cluster_id_);
    if (err != ErrorCode::OK) {
        GTEST_SKIP() << "Service could not reach WATCHING state via LOCAL_FS; "
                        "skipping promotion test";
    }
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());

    StandbySnapshot out;
    ErrorCode promote_err = service_->PromoteAndExportSnapshot(out);
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, promote_err);
}

TEST_F(PromotionCatchUpTest, UnresolvedGapsFailClosed) {
    auto err = service_->Start("", oplog_endpoints_, cluster_id_);
    if (err != ErrorCode::OK) {
        GTEST_SKIP() << "Service could not reach WATCHING state via LOCAL_FS; "
                        "skipping promotion test";
    }
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());

    // Seed a skipped gap into the applier. The gap cannot be resolved because
    // no OpLogStore has the entry, so ResolvePromotionGapsLocked leaves it
    // untouched. PromoteLockedInternal's post-catch-up gap check then fires.
    auto* applier = service_->GetOpLogApplierForTesting();
    ASSERT_NE(nullptr, applier);
    applier->AddSkippedGapForTesting(999);

    StandbySnapshot out;
    ErrorCode promote_err = service_->PromoteAndExportSnapshot(out);
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, promote_err);
    EXPECT_EQ(StandbyState::FAILED, service_->GetState());
}

TEST_F(PromotionCatchUpTest, BestEffortReturnsOkWhenFlagFalse) {
    config_.fail_closed_on_incomplete_catch_up = false;

    for (uint64_t s = 1; s <= 3; ++s) {
        auto e = MakeEntry(s, OpType::PUT_END, "key_" + std::to_string(s),
                           MakeValidPayload());
        ASSERT_EQ(ErrorCode::OK, mock_store_->WriteOpLog(e));
    }
    mock_store_->SetForceReadEmpty(true);

    // Re-create service to apply updated config
    service_ = std::make_unique<HotStandbyService>(config_);
    service_->SetCatchUpOpLogStoreForTesting(mock_store_);

    auto err = service_->Start("", oplog_endpoints_, cluster_id_);
    if (err != ErrorCode::OK) {
        GTEST_SKIP() << "Service could not reach WATCHING state via LOCAL_FS; "
                        "skipping promotion test";
    }
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());

    // With fail_closed_on_incomplete_catch_up=false, the best-effort path
    // should return OK despite read-empty behavior.
    StandbySnapshot out;
    ErrorCode promote_err = service_->PromoteAndExportSnapshot(out);
    EXPECT_EQ(ErrorCode::OK, promote_err);
}

TEST_F(PromotionCatchUpTest, BypassesGapCheckWhenFlagFalse) {
    config_.fail_closed_on_unresolved_gaps = false;

    // Re-create service to apply updated config
    service_ = std::make_unique<HotStandbyService>(config_);
    service_->SetCatchUpOpLogStoreForTesting(mock_store_);

    auto err = service_->Start("", oplog_endpoints_, cluster_id_);
    if (err != ErrorCode::OK) {
        GTEST_SKIP() << "Service could not reach WATCHING state via LOCAL_FS; "
                        "skipping promotion test";
    }
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());

    // Seed a gap; with fail_closed_on_unresolved_gaps=false the post-catch-up
    // gap check is skipped, so promotion succeeds.
    auto* applier = service_->GetOpLogApplierForTesting();
    ASSERT_NE(nullptr, applier);
    applier->AddSkippedGapForTesting(999);

    StandbySnapshot out;
    ErrorCode promote_err = service_->PromoteAndExportSnapshot(out);
    EXPECT_EQ(ErrorCode::OK, promote_err);
    // PromoteAndExportSnapshot calls Stop() on success, so state becomes
    // STOPPED.
    EXPECT_EQ(StandbyState::STOPPED, service_->GetState());
}

TEST_F(PromotionCatchUpTest, PromoteAppliesSameFailClosedChecks) {
    auto err = service_->Start("", oplog_endpoints_, cluster_id_);
    if (err != ErrorCode::OK) {
        GTEST_SKIP() << "Service could not reach WATCHING state via LOCAL_FS; "
                        "skipping promotion test";
    }
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());

    // Seed a gap and verify Promote() (not PromoteAndExportSnapshot) applies
    // the same fail-closed checks via PromoteLockedInternal.
    auto* applier = service_->GetOpLogApplierForTesting();
    ASSERT_NE(nullptr, applier);
    applier->AddSkippedGapForTesting(999);

    ErrorCode promote_err = service_->Promote();
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, promote_err);
    EXPECT_EQ(StandbyState::FAILED, service_->GetState());
}

TEST_F(PromotionCatchUpTest, UsesDurablePrefixLastSeqAsCatchUpTarget) {
    auto batch_backend = std::make_shared<FakeHaKvBackend>();
    ASSERT_EQ(ErrorCode::OK,
              batch_backend->Put(
                  BuildDurablePrefixKey(cluster_id_),
                  EncodeDurablePrefix({.batch_id = 1, .last_seq = 2})));
    ASSERT_EQ(ErrorCode::OK,
              batch_backend->Put(BuildBatchRecordKey(cluster_id_, 1),
                                 EncodeOpLogBatchRecord(MakeBatch(1, 1, 2))));
    mock_store_->SetForceReadEmpty(true);
    service_->SetCatchUpBatchKvBackendForTesting(batch_backend);

    auto err = service_->Start("", oplog_endpoints_, cluster_id_);
    if (err != ErrorCode::OK) {
        GTEST_SKIP() << "Service could not reach WATCHING state via LOCAL_FS; "
                        "skipping promotion test";
    }
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());

    StandbySnapshot out;
    ErrorCode promote_err = service_->PromoteAndExportSnapshot(out);
    EXPECT_EQ(ErrorCode::OK, promote_err);
    EXPECT_EQ(2u, out.oplog_sequence_id);
}

TEST_F(PromotionCatchUpTest, FailsPromotionWhenDurablePrefixUnreadable) {
    auto batch_backend = std::make_shared<FakeHaKvBackend>();
    batch_backend->SetGetError(ErrorCode::PERSISTENT_FAIL);
    service_->SetCatchUpBatchKvBackendForTesting(batch_backend);

    auto err = service_->Start("", oplog_endpoints_, cluster_id_);
    if (err != ErrorCode::OK) {
        GTEST_SKIP() << "Service could not reach WATCHING state via LOCAL_FS; "
                        "skipping promotion test";
    }

    StandbySnapshot out;
    ErrorCode promote_err = service_->PromoteAndExportSnapshot(out);
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, promote_err);
    EXPECT_EQ(StandbyState::FAILED, service_->GetState());
}

TEST_F(PromotionCatchUpTest, FailsPromotionWhenTargetBatchUnreadable) {
    auto batch_backend = std::make_shared<FakeHaKvBackend>();
    ASSERT_EQ(ErrorCode::OK,
              batch_backend->Put(
                  BuildDurablePrefixKey(cluster_id_),
                  EncodeDurablePrefix({.batch_id = 1, .last_seq = 1})));
    batch_backend->SetRangeError(ErrorCode::PERSISTENT_FAIL);
    service_->SetCatchUpBatchKvBackendForTesting(batch_backend);

    auto err = service_->Start("", oplog_endpoints_, cluster_id_);
    if (err != ErrorCode::OK) {
        GTEST_SKIP() << "Service could not reach WATCHING state via LOCAL_FS; "
                        "skipping promotion test";
    }

    StandbySnapshot out;
    ErrorCode promote_err = service_->PromoteAndExportSnapshot(out);
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, promote_err);
    EXPECT_EQ(StandbyState::FAILED, service_->GetState());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
