#include "ha_metric_manager.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

namespace mooncake::test {

class HAMetricManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("HAMetricManagerTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    HAMetricManager& M() { return HAMetricManager::instance(); }
};

// ========== 7.1.1 Metric update tests ==========

TEST_F(HAMetricManagerTest, TestSetOpLogLastSequenceId) {
    M().set_oplog_last_sequence_id(123);
    EXPECT_EQ(123, M().get_oplog_last_sequence_id());
}

TEST_F(HAMetricManagerTest, TestSetOpLogAppliedSequenceId) {
    M().set_oplog_applied_sequence_id(456);
    EXPECT_EQ(456, M().get_oplog_applied_sequence_id());
}

TEST_F(HAMetricManagerTest, TestSetOpLogStandbyLag) {
    M().set_oplog_standby_lag(10);
    EXPECT_EQ(10, M().get_oplog_standby_lag());
}

TEST_F(HAMetricManagerTest, TestSetOpLogPendingEntries) {
    M().set_oplog_pending_entries(7);
    EXPECT_EQ(7, M().get_oplog_pending_entries());
}

TEST_F(HAMetricManagerTest, TestSetPendingMutationQueueSize) {
    M().set_pending_mutation_queue_size(5);
    EXPECT_EQ(5, M().get_pending_mutation_queue_size());
}

TEST_F(HAMetricManagerTest, TestIncOpLogSkippedEntries) {
    auto before = M().get_oplog_skipped_entries_total();
    M().inc_oplog_skipped_entries();
    EXPECT_EQ(before + 1, M().get_oplog_skipped_entries_total());
}

TEST_F(HAMetricManagerTest, TestIncOpLogChecksumFailures) {
    auto before = M().get_oplog_checksum_failures_total();
    M().inc_oplog_checksum_failures(2);
    EXPECT_EQ(before + 2, M().get_oplog_checksum_failures_total());
}

TEST_F(HAMetricManagerTest, TestIncGapResolveCounters) {
    auto before_attempts = M().get_oplog_gap_resolve_attempts_total();
    auto before_success = M().get_oplog_gap_resolve_success_total();

    M().inc_oplog_gap_resolve_attempts(3);
    M().inc_oplog_gap_resolve_success(1);

    EXPECT_EQ(before_attempts + 3, M().get_oplog_gap_resolve_attempts_total());
    EXPECT_EQ(before_success + 1, M().get_oplog_gap_resolve_success_total());
}

TEST_F(HAMetricManagerTest, TestIncOpLogEtcdWriteFailuresAndRetries) {
    auto before_failures = M().get_oplog_etcd_write_failures_total();
    auto before_retries = M().get_oplog_etcd_write_retries_total();

    M().inc_oplog_etcd_write_failures(4);
    M().inc_oplog_etcd_write_retries(5);

    EXPECT_EQ(before_failures + 4, M().get_oplog_etcd_write_failures_total());
    EXPECT_EQ(before_retries + 5, M().get_oplog_etcd_write_retries_total());
}

TEST_F(HAMetricManagerTest, TestIncWatchDisconnectionsAndAppliedEntries) {
    auto before_disc = M().get_oplog_watch_disconnections_total();
    auto before_applied = M().get_oplog_applied_entries_total();

    M().inc_oplog_watch_disconnections(2);
    M().inc_oplog_applied_entries(10);

    EXPECT_EQ(before_disc + 2, M().get_oplog_watch_disconnections_total());
    EXPECT_EQ(before_applied + 10, M().get_oplog_applied_entries_total());
}

TEST_F(HAMetricManagerTest, TestRecordOpLogEtcdWriteLatency) {
    // Call histogram observe functions, mainly to ensure they do not crash
    M().observe_oplog_etcd_write_latency_us(100);
    M().observe_oplog_etcd_write_latency_us(5000);
    SUCCEED();
}

TEST_F(HAMetricManagerTest, TestRecordOpLogApplyLatency) {
    M().observe_oplog_apply_latency_us(50);
    M().observe_oplog_apply_latency_us(1000);
    SUCCEED();
}

// ========== 7.1.2 Metric serialization tests ==========

TEST_F(HAMetricManagerTest, TestSerializeMetrics) {
    M().set_oplog_last_sequence_id(1);
    M().set_oplog_applied_sequence_id(1);
    M().set_oplog_standby_lag(0);

    std::string text = M().serialize_metrics();
    EXPECT_FALSE(text.empty());

    // Basic fields should appear in the Prometheus text output
    EXPECT_NE(std::string::npos, text.find("ha_oplog_last_sequence_id"));
    EXPECT_NE(std::string::npos, text.find("ha_oplog_applied_sequence_id"));
    EXPECT_NE(std::string::npos, text.find("ha_oplog_standby_lag"));
}

TEST_F(HAMetricManagerTest, TestGetSummaryString) {
    M().set_oplog_last_sequence_id(100);
    M().set_oplog_applied_sequence_id(95);
    M().set_oplog_standby_lag(5);

    std::string summary = M().get_summary_string();
    EXPECT_FALSE(summary.empty());
    // Summary string should contain key fields
    EXPECT_NE(std::string::npos, summary.find("last_seq"));
    EXPECT_NE(std::string::npos, summary.find("applied_seq"));
}

TEST_F(HAMetricManagerTest, TestObserveRuntimePhase) {
    const auto before = M().get_runtime_phase_runs_total(
        HARuntimeMode::kSnapshotOnly, HARuntimePhase::kSnapshotBootstrap,
        HARuntimePhaseResult::kSuccess);

    HARuntimePhaseStats stats;
    stats.key_count = 3;
    stats.logical_bytes = 12288;
    stats.oplog_entries = 0;
    stats.applied_seq_id = 42;

    M().ObserveRuntimePhase(
        HARuntimeMode::kSnapshotOnly, HARuntimePhase::kSnapshotBootstrap,
        HARuntimePhaseResult::kSuccess, std::chrono::microseconds(2000), stats);

    EXPECT_EQ(before + 1, M().get_runtime_phase_runs_total(
                              HARuntimeMode::kSnapshotOnly,
                              HARuntimePhase::kSnapshotBootstrap,
                              HARuntimePhaseResult::kSuccess));
    EXPECT_EQ(2000, M().get_runtime_phase_last_duration_us(
                        HARuntimeMode::kSnapshotOnly,
                        HARuntimePhase::kSnapshotBootstrap));
    EXPECT_EQ(3, M().get_runtime_phase_last_key_count(
                     HARuntimeMode::kSnapshotOnly,
                     HARuntimePhase::kSnapshotBootstrap));
    EXPECT_EQ(12288, M().get_runtime_phase_last_logical_bytes(
                         HARuntimeMode::kSnapshotOnly,
                         HARuntimePhase::kSnapshotBootstrap));
    EXPECT_EQ(42, M().get_runtime_phase_last_applied_seq_id(
                      HARuntimeMode::kSnapshotOnly,
                      HARuntimePhase::kSnapshotBootstrap));
    EXPECT_GT(
        M().get_runtime_phase_last_key_rate_per_sec(
            HARuntimeMode::kSnapshotOnly, HARuntimePhase::kSnapshotBootstrap),
        0);
}

TEST_F(HAMetricManagerTest, TestSerializeMetricsIncludesRuntimePhaseMetrics) {
    HARuntimePhaseStats stats;
    stats.oplog_entries = 7;
    M().ObserveRuntimePhase(
        HARuntimeMode::kSnapshotWithOplog, HARuntimePhase::kFinalCatchup,
        HARuntimePhaseResult::kSuccess, std::chrono::microseconds(5000), stats);

    std::string text = M().serialize_metrics();
    EXPECT_NE(std::string::npos, text.find("ha_runtime_phase_runs_total"));
    EXPECT_NE(std::string::npos, text.find("ha_runtime_phase_duration_us"));
    EXPECT_NE(std::string::npos,
              text.find("ha_runtime_phase_last_oplog_entries"));

    std::string summary = M().get_summary_string();
    EXPECT_NE(std::string::npos, summary.find("last_runtime_phase"));
}

// ========== 7.1.3 Singleton tests ==========

TEST_F(HAMetricManagerTest, TestSingletonInstance) {
    HAMetricManager& a = HAMetricManager::instance();
    HAMetricManager& b = HAMetricManager::instance();

    EXPECT_EQ(&a, &b);

    a.set_oplog_last_sequence_id(1234);
    EXPECT_EQ(1234, b.get_oplog_last_sequence_id());
}

TEST_F(HAMetricManagerTest, TestConcurrentAccess) {
    constexpr int kThreads = 8;
    constexpr int kIncrementsPerThread = 1000;

    auto& mgr = HAMetricManager::instance();
    auto before = mgr.get_oplog_applied_entries_total();

    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&mgr]() {
            for (int j = 0; j < kIncrementsPerThread; ++j) {
                mgr.inc_oplog_applied_entries();
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto after = mgr.get_oplog_applied_entries_total();
    EXPECT_EQ(before + kThreads * kIncrementsPerThread, after);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
