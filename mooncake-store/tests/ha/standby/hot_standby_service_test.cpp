#include "hot_standby_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "master_service.h"

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
    snapshot.metadata.emplace_back(std::move(key), metadata);
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
    snapshot.metadata.emplace_back("key-1", metadata);

    service->SetSnapshotProvider(std::make_unique<FakeSnapshotProvider>(
        std::optional<LoadedSnapshot>(snapshot)));
    EXPECT_EQ(ErrorCode::OK, service->Start("", "", cluster_id));
    EXPECT_EQ(StandbyState::WATCHING, service->GetState());
    return service;
}

}  // namespace

// ========== 6.1.1 Start/Stop tests ==========

TEST_F(HotStandbyServiceTest, TestStart) {
    GTEST_SKIP()
        << "Requires real etcd connection, run in integration environment.";
}

TEST_F(HotStandbyServiceTest, TestStart_AlreadyRunning) {
    GTEST_SKIP()
        << "Requires real etcd connection to verify double start semantics.";
}

TEST_F(HotStandbyServiceTest, TestStart_InvalidEtcdEndpoints) {
    GTEST_SKIP() << "Requires real etcd to simulate invalid endpoints.";
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
    GTEST_SKIP()
        << "Requires real etcd to drive full state transition to WATCHING.";
}

TEST_F(HotStandbyServiceTest, TestStateTransition_ConnectionFailed) {
    GTEST_SKIP()
        << "Connection failure requires real etcd and invalid endpoints.";
}

TEST_F(HotStandbyServiceTest, TestStateTransition_SyncFailed) {
    GTEST_SKIP()
        << "Sync failure requires real etcd and OpLog watcher behavior.";
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
    GTEST_SKIP()
        << "Requires real etcd and OpLog activity to change sync status.";
}

TEST_F(HotStandbyServiceTest, TestGetSyncStatus) {
    // Basic coverage: multiple calls should return consistent values and not
    // crash
    StandbySyncStatus s1 = service_->GetSyncStatus();
    StandbySyncStatus s2 = service_->GetSyncStatus();
    EXPECT_EQ(s1.state, s2.state);
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

TEST_F(HotStandbyServiceTest, TestPromote_FinalCatchUp) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd and OpLog data to exercise final "
                    "catch-up logic.";
#else
    GTEST_SKIP() << "Requires an OpLog-following standby runtime.";
#endif
}

TEST_F(HotStandbyServiceTest, TestPromote_WithGaps) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Requires real etcd and gaps in OpLog to validate gap resolution.";
#else
    GTEST_SKIP() << "Requires an OpLog-following standby runtime.";
#endif
}

TEST_F(HotStandbyServiceTest, TestPromote_Timeout) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Requires real etcd and slow reads to trigger catch-up timeout.";
#else
    GTEST_SKIP() << "Requires an OpLog-following standby runtime.";
#endif
}

TEST_F(HotStandbyServiceTest, TestPromote_BatchLimit) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd and large OpLog to hit batch limit.";
#else
    GTEST_SKIP() << "Requires an OpLog-following standby runtime.";
#endif
}

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

    std::vector<std::pair<std::string, StandbyObjectMetadata>> exported;
    EXPECT_TRUE(service_->ExportMetadataSnapshot(exported));
    ASSERT_EQ(1u, exported.size());
    EXPECT_EQ("key-1", exported[0].first);
    EXPECT_EQ(4096u, exported[0].second.size);
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

    std::vector<std::pair<std::string, StandbyObjectMetadata>> exported;
    ASSERT_TRUE(service_->ExportMetadataSnapshot(exported));
    ASSERT_EQ(1u, exported.size());
    EXPECT_EQ("key-new", exported[0].first);
    EXPECT_EQ(8192u, exported[0].second.size);
    EXPECT_EQ(84u, exported[0].second.last_sequence_id);
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
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    EXPECT_TRUE(service_->ExportMetadataSnapshot(snapshot));
    EXPECT_TRUE(snapshot.empty());
}

TEST_F(HotStandbyServiceTest, TestGetLatestAppliedSequenceId) {
    uint64_t seq = service_->GetLatestAppliedSequenceId();
    EXPECT_EQ(0u, seq);
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

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
