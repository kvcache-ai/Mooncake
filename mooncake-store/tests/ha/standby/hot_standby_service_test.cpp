#include "hot_standby_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "master_service.h"

namespace mooncake::test {

namespace {

class FakeSnapshotProvider final : public SnapshotProvider {
   public:
    explicit FakeSnapshotProvider(
        tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result)
        : result_(std::move(result)) {}

    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> LoadLatestSnapshot(
        const std::string& /*cluster_id*/,
        SnapshotRestoreMode /*restore_mode*/) override {
        return result_;
    }

   private:
    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result_;
};

class MutableSnapshotProvider final : public SnapshotProvider {
   public:
    explicit MutableSnapshotProvider(
        tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result)
        : result_(std::move(result)) {}

    void Publish(
        tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result) {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = std::move(result);
    }

    tl::expected<std::optional<SnapshotVersionInfo>, ErrorCode>
    GetLatestSnapshotVersion(const std::string& /*cluster_id*/) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!result_) {
            return tl::make_unexpected(result_.error());
        }
        if (!result_->has_value()) {
            return std::optional<SnapshotVersionInfo>();
        }

        SnapshotVersionInfo version;
        version.snapshot_id = result_->value().snapshot_id;
        version.snapshot_sequence_id = result_->value().snapshot_sequence_id;
        return std::optional<SnapshotVersionInfo>(std::move(version));
    }

    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> LoadLatestSnapshot(
        const std::string& /*cluster_id*/,
        SnapshotRestoreMode /*restore_mode*/) override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_;
    }

   private:
    std::mutex mutex_;
    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result_;
};

class BlockingSnapshotProvider final : public SnapshotProvider {
   public:
    explicit BlockingSnapshotProvider(
        tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result)
        : result_(std::move(result)) {}

    void PublishAndBlockNextLoad(
        tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result) {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = std::move(result);
        block_next_load_ = true;
        load_started_ = false;
    }

    bool WaitForBlockedLoad(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this]() { return load_started_; });
    }

    void UnblockLoad() {
        std::lock_guard<std::mutex> lock(mutex_);
        block_next_load_ = false;
        cv_.notify_all();
    }

    tl::expected<std::optional<SnapshotVersionInfo>, ErrorCode>
    GetLatestSnapshotVersion(const std::string& /*cluster_id*/) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!result_) {
            return tl::make_unexpected(result_.error());
        }
        if (!result_->has_value()) {
            return std::optional<SnapshotVersionInfo>();
        }

        SnapshotVersionInfo version;
        version.snapshot_id = result_->value().snapshot_id;
        version.snapshot_sequence_id = result_->value().snapshot_sequence_id;
        return std::optional<SnapshotVersionInfo>(std::move(version));
    }

    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> LoadLatestSnapshot(
        const std::string& /*cluster_id*/,
        SnapshotRestoreMode /*restore_mode*/) override {
        std::unique_lock<std::mutex> lock(mutex_);
        if (block_next_load_) {
            load_started_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this]() { return !block_next_load_; });
        }
        return result_;
    }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> result_;
    bool block_next_load_{false};
    bool load_started_{false};
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

bool WaitUntil(const std::function<bool()>& predicate,
               std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
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
        etcd_endpoints_ = "http://localhost:2379";
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
    std::string etcd_endpoints_;
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
#ifdef STORE_USE_ETCD
    // Requires a real etcd cluster and valid cluster configuration; acts as an
    // integration placeholder
    GTEST_SKIP()
        << "Requires real etcd connection, run in integration environment.";
#else
    ErrorCode err =
        service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
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
        service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, err1);
    ErrorCode err2 =
        service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
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
        service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, err);
    EXPECT_EQ(StandbyState::FAILED, service_->GetState());
#endif
}

TEST_F(HotStandbyServiceTest, SnapshotOnlyStartPublishesWatermarkMetrics) {
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    service_ = std::make_unique<HotStandbyService>(config_);

    LoadedSnapshot snapshot;
    snapshot.snapshot_id = "20260401_120000_000";
    snapshot.snapshot_sequence_id = 42;

    StandbyObjectMetadata metadata;
    metadata.client_id = UUID{1, 2};
    metadata.size = 4096;
    metadata.last_sequence_id = 42;
    snapshot.metadata.emplace_back("key-1", metadata);

    service_->SetSnapshotProvider(std::make_unique<FakeSnapshotProvider>(
        std::optional<LoadedSnapshot>(snapshot)));

    ASSERT_EQ(ErrorCode::OK, service_->Start("", "", cluster_id_));
    EXPECT_EQ(StandbyState::WATCHING, service_->GetState());
    EXPECT_EQ(42, HAMetricManager::instance().get_oplog_last_sequence_id());
    EXPECT_EQ(42, HAMetricManager::instance().get_oplog_applied_sequence_id());
    EXPECT_EQ(0, HAMetricManager::instance().get_oplog_standby_lag());
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
        service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
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
    (void)service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
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
    (void)service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
    SUCCEED();
#endif
}

TEST_F(HotStandbyServiceTest, TestWarmStart_WithoutLocalState) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd and snapshot provider configuration.";
#else
    (void)service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
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
    (void)service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
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

TEST_F(HotStandbyServiceTest, TestSnapshotOnlyStartAndPromoteUpdateMetrics) {
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    service_ = std::make_unique<HotStandbyService>(config_);

    auto& metrics = HAMetricManager::instance();
    const auto bootstrap_before = metrics.get_runtime_phase_runs_total(
        HARuntimeMode::kSnapshotOnly, HARuntimePhase::kSnapshotBootstrap,
        HARuntimePhaseResult::kSuccess);
    const auto start_before = metrics.get_runtime_phase_runs_total(
        HARuntimeMode::kSnapshotOnly, HARuntimePhase::kStandbyStart,
        HARuntimePhaseResult::kSuccess);
    const auto promote_before = metrics.get_runtime_phase_runs_total(
        HARuntimeMode::kSnapshotOnly, HARuntimePhase::kStandbyPromote,
        HARuntimePhaseResult::kSuccess);

    service_->SetSnapshotProvider(
        std::make_unique<FakeSnapshotProvider>(std::optional<LoadedSnapshot>(
            MakeSnapshot("20260330_120000_000", 42, "key-1", 4096))));

    ASSERT_EQ(ErrorCode::OK, service_->Start("", "", cluster_id_));
    EXPECT_EQ(bootstrap_before + 1, metrics.get_runtime_phase_runs_total(
                                        HARuntimeMode::kSnapshotOnly,
                                        HARuntimePhase::kSnapshotBootstrap,
                                        HARuntimePhaseResult::kSuccess));
    EXPECT_EQ(start_before + 1,
              metrics.get_runtime_phase_runs_total(
                  HARuntimeMode::kSnapshotOnly, HARuntimePhase::kStandbyStart,
                  HARuntimePhaseResult::kSuccess));
    EXPECT_EQ(1, metrics.get_runtime_phase_last_key_count(
                     HARuntimeMode::kSnapshotOnly,
                     HARuntimePhase::kSnapshotBootstrap));
    EXPECT_EQ(4096, metrics.get_runtime_phase_last_logical_bytes(
                        HARuntimeMode::kSnapshotOnly,
                        HARuntimePhase::kSnapshotBootstrap));
    EXPECT_EQ(42, metrics.get_runtime_phase_last_applied_seq_id(
                      HARuntimeMode::kSnapshotOnly,
                      HARuntimePhase::kSnapshotBootstrap));

    ASSERT_EQ(ErrorCode::OK, service_->Promote());
    EXPECT_EQ(promote_before + 1,
              metrics.get_runtime_phase_runs_total(
                  HARuntimeMode::kSnapshotOnly, HARuntimePhase::kStandbyPromote,
                  HARuntimePhaseResult::kSuccess));
    EXPECT_EQ(
        1, metrics.get_runtime_phase_last_key_count(
               HARuntimeMode::kSnapshotOnly, HARuntimePhase::kStandbyPromote));
    EXPECT_EQ(4096, metrics.get_runtime_phase_last_logical_bytes(
                        HARuntimeMode::kSnapshotOnly,
                        HARuntimePhase::kStandbyPromote));
    EXPECT_EQ(
        42, metrics.get_runtime_phase_last_applied_seq_id(
                HARuntimeMode::kSnapshotOnly, HARuntimePhase::kStandbyPromote));
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

TEST_F(HotStandbyServiceTest,
       TestSnapshotOnlyStandbyRefreshesNewSnapshotInPlace) {
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    config_.snapshot_refresh_interval_ms = 20;
    service_ = std::make_unique<HotStandbyService>(config_);

    auto provider =
        std::make_unique<MutableSnapshotProvider>(std::optional<LoadedSnapshot>(
            MakeSnapshot("20260330_120000_000", 42, "key-old", 4096)));
    auto* provider_handle = provider.get();

    std::mutex callback_mutex;
    std::vector<StandbyState> state_history;
    service_->SetSyncStatusCallback([&](const StandbySyncStatus& status) {
        std::lock_guard<std::mutex> lock(callback_mutex);
        state_history.push_back(status.state);
    });
    service_->SetSnapshotProvider(std::move(provider));

    ASSERT_EQ(ErrorCode::OK, service_->Start("", "", cluster_id_));
    ASSERT_TRUE(WaitUntil(
        [&]() { return service_->GetLatestAppliedSequenceId() == 42; },
        std::chrono::milliseconds(500)));

    provider_handle->Publish(std::optional<LoadedSnapshot>(
        MakeSnapshot("20260330_121500_000", 84, "key-new", 8192)));

    ASSERT_TRUE(WaitUntil(
        [&]() { return service_->GetLatestAppliedSequenceId() == 84; },
        std::chrono::milliseconds(1000)));
    EXPECT_EQ(84u, service_->GetSyncStatus().primary_seq_id);

    std::vector<std::pair<std::string, StandbyObjectMetadata>> exported;
    ASSERT_TRUE(service_->ExportMetadataSnapshot(exported));
    ASSERT_EQ(1u, exported.size());
    EXPECT_EQ("key-new", exported[0].first);
    EXPECT_EQ(8192u, exported[0].second.size);
    EXPECT_EQ(84u, exported[0].second.last_sequence_id);

    std::lock_guard<std::mutex> lock(callback_mutex);
    EXPECT_NE(std::find(state_history.begin(), state_history.end(),
                        StandbyState::RECOVERING),
              state_history.end());

    service_->SetSyncStatusCallback(nullptr);
    service_->Stop();
}

TEST_F(HotStandbyServiceTest, TestSnapshotOnlyRefreshUpdatesMetrics) {
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    config_.snapshot_refresh_interval_ms = 20;
    service_ = std::make_unique<HotStandbyService>(config_);

    auto provider =
        std::make_unique<MutableSnapshotProvider>(std::optional<LoadedSnapshot>(
            MakeSnapshot("20260330_120000_000", 42, "key-old", 4096)));
    auto* provider_handle = provider.get();
    service_->SetSnapshotProvider(std::move(provider));

    auto& metrics = HAMetricManager::instance();
    const auto refresh_before = metrics.get_runtime_phase_runs_total(
        HARuntimeMode::kSnapshotOnly, HARuntimePhase::kSnapshotRefresh,
        HARuntimePhaseResult::kSuccess);

    ASSERT_EQ(ErrorCode::OK, service_->Start("", "", cluster_id_));
    ASSERT_TRUE(WaitUntil(
        [&]() { return service_->GetLatestAppliedSequenceId() == 42; },
        std::chrono::milliseconds(500)));

    provider_handle->Publish(std::optional<LoadedSnapshot>(
        MakeSnapshot("20260330_121500_000", 84, "key-new", 8192)));

    ASSERT_TRUE(WaitUntil(
        [&]() { return service_->GetLatestAppliedSequenceId() == 84; },
        std::chrono::milliseconds(1000)));

    EXPECT_EQ(refresh_before + 1, metrics.get_runtime_phase_runs_total(
                                      HARuntimeMode::kSnapshotOnly,
                                      HARuntimePhase::kSnapshotRefresh,
                                      HARuntimePhaseResult::kSuccess));
    EXPECT_EQ(
        1, metrics.get_runtime_phase_last_key_count(
               HARuntimeMode::kSnapshotOnly, HARuntimePhase::kSnapshotRefresh));
    EXPECT_EQ(8192, metrics.get_runtime_phase_last_logical_bytes(
                        HARuntimeMode::kSnapshotOnly,
                        HARuntimePhase::kSnapshotRefresh));
    EXPECT_EQ(84, metrics.get_runtime_phase_last_applied_seq_id(
                      HARuntimeMode::kSnapshotOnly,
                      HARuntimePhase::kSnapshotRefresh));
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

TEST_F(HotStandbyServiceTest,
       TestPromote_DuringSnapshotRefreshKeepsPreviousMetadataConsistent) {
    config_.enable_snapshot_bootstrap = true;
    config_.enable_oplog_following = false;
    config_.snapshot_refresh_interval_ms = 20;
    service_ = std::make_unique<HotStandbyService>(config_);

    auto provider = std::make_unique<BlockingSnapshotProvider>(
        std::optional<LoadedSnapshot>(
            MakeSnapshot("20260330_120000_000", 42, "key-old", 4096)));
    auto* provider_handle = provider.get();
    service_->SetSnapshotProvider(std::move(provider));

    ASSERT_EQ(ErrorCode::OK, service_->Start("", "", cluster_id_));
    ASSERT_TRUE(WaitUntil(
        [&]() { return service_->GetLatestAppliedSequenceId() == 42; },
        std::chrono::milliseconds(500)));

    provider_handle->PublishAndBlockNextLoad(std::optional<LoadedSnapshot>(
        MakeSnapshot("20260330_121500_000", 84, "key-new", 8192)));
    ASSERT_TRUE(
        provider_handle->WaitForBlockedLoad(std::chrono::milliseconds(1000)));
    ASSERT_TRUE(WaitUntil(
        [&]() { return service_->GetSyncStatus().primary_seq_id == 84; },
        std::chrono::milliseconds(500)));

    ErrorCode promote_err = ErrorCode::INTERNAL_ERROR;
    std::atomic<bool> promote_done{false};
    std::thread promote_thread([&]() {
        promote_err = service_->Promote();
        promote_done.store(true, std::memory_order_release);
    });

    ASSERT_TRUE(WaitUntil(
        [&]() {
            return service_->GetState() == StandbyState::STOPPED &&
                   !promote_done.load(std::memory_order_acquire);
        },
        std::chrono::milliseconds(1000)));

    provider_handle->UnblockLoad();
    promote_thread.join();

    EXPECT_EQ(ErrorCode::OK, promote_err);
    EXPECT_EQ(StandbyState::STOPPED, service_->GetState());
    EXPECT_EQ(42u, service_->GetLatestAppliedSequenceId());

    std::vector<std::pair<std::string, StandbyObjectMetadata>> exported;
    ASSERT_TRUE(service_->ExportMetadataSnapshot(exported));
    ASSERT_EQ(1u, exported.size());
    EXPECT_EQ("key-old", exported[0].first);
    EXPECT_EQ(4096u, exported[0].second.size);
    EXPECT_EQ(42u, exported[0].second.last_sequence_id);
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
    (void)service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
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
    (void)service_->Start("primary_unused", etcd_endpoints_, cluster_id_);
    service_->Stop();
    SUCCEED();
#endif
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
