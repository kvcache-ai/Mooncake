#include <gtest/gtest.h>
#include <ylt/struct_json/json_reader.h>

#include <chrono>
#include <mutex>

#include "p2p/ha/ha_status.h"

namespace mooncake {

const HADoctorIssue* FindIssue(const HADoctorReport& report,
                               const std::string& code) {
    for (const auto& issue : report.issues) {
        if (issue.code == code) {
            return &issue;
        }
    }
    return nullptr;
}

struct HAStatusTestPeer {
    static void ExpireOldestChange(HAStatusManager& status) {
        std::lock_guard<std::mutex> lock(status.mutex_);
        status.election_.changes.front() -= std::chrono::seconds(301);
    }
    static void AgeLastPollSuccess(HAStatusManager& status,
                                   std::chrono::seconds age) {
        std::lock_guard<std::mutex> lock(status.mutex_);
        ASSERT_TRUE(status.oplog_poll_.last_success_at.has_value());
        *status.oplog_poll_.last_success_at -= age;
        status.oplog_poll_.last_success_ms -=
            std::chrono::duration_cast<std::chrono::milliseconds>(age).count();
    }
};

class HAStatusTest : public ::testing::Test {
   protected:
    HAStatusManager& status = HAStatusManager::instance();

    void SetUp() override {
        status.Reset();
        status.Configure(true, "test_cluster", "local");
        status.SetRoleState("standby", "WATCHING");
    }

    void TearDown() override { status.Reset(); }

    HADoctorReport doctor() {
        HADoctorReport report;
        struct_json::from_json(report, status.SerializeDoctorJson());
        return report;
    }
};

TEST_F(HAStatusTest, CountsObservedTermsAcrossSupervisorRounds) {
    status.SetMasterView("leader", 10);
    status.SetMasterView("leader", 10);
    status.SetLeader("leader", 11);
    status.SetMasterView("", 100);
    status.SetMasterView("leader", 12);
    EXPECT_EQ(doctor().status, "ok");
    status.SetMasterView("leader", 11);
    status.SetMasterView("leader", 12);
    status.ClearDoctorIssues();
    status.Configure(true, "test_cluster", "local");
    EXPECT_EQ(doctor().status, "ok");
    status.SetLeader("local", 20);
    status.SetMasterView("local", 20);
    status.SetRoleState("primary", "RUNNING");
    auto report = doctor();
    EXPECT_EQ(report.status, "degraded");
    ASSERT_EQ(report.issues.size(), 1u);
    EXPECT_EQ(report.issues[0].code, "ELECTION_UNSTABLE");
    EXPECT_NE(report.issues[0].message.find("window_sec=300, changes=3"),
              std::string::npos);
    EXPECT_NE(report.issues[0].message.find("leader=local, epoch=20"),
              std::string::npos);
    status.SetDoctorIssue("ELECTION_FAILED", "Redis unavailable");
    EXPECT_EQ(doctor().issues.size(), 2u);
    status.ClearDoctorIssues();
    EXPECT_EQ(doctor().status, "degraded");
}

TEST_F(HAStatusTest, ElectionWindowExpiresAndResets) {
    for (uint64_t epoch = 17; epoch <= 20; ++epoch) {
        status.SetLeader("other", epoch);
    }
    ASSERT_EQ(doctor().status, "degraded");
    HAStatusTestPeer::ExpireOldestChange(status);
    EXPECT_EQ(doctor().status, "ok");
    status.SetLeader("other", 21);
    EXPECT_EQ(doctor().status, "degraded");

    status.Configure(false, "test_cluster", "local");
    status.SetLeader("other", 22);
    EXPECT_EQ(doctor().status, "unknown");
    status.Configure(true, "test_cluster", "local");
    status.SetRoleState("standby", "WATCHING");
    status.SetLeader("other", 23);
    EXPECT_TRUE(doctor().issues.empty());
    status.SetLeader("other", 24);
    status.SetLeader("other", 25);

    status.Configure(true, "new_cluster", "local");
    status.SetLeader("new", 1);
    status.SetLeader("new", 2);
    EXPECT_TRUE(doctor().issues.empty());
    status.SetLeader("new", 3);
    status.SetLeader("new", 4);
    EXPECT_EQ(doctor().status, "degraded");
    status.Reset();
    status.Configure(true, "new_cluster", "local");
    status.SetRoleState("standby", "WATCHING");
    status.SetLeader("new", 5);
    EXPECT_TRUE(doctor().issues.empty());
}

TEST_F(HAStatusTest, DoctorReportsPrimaryMasterViewMismatch) {
    status.SetInstanceId("local-instance");
    status.SetMasterView("remote", 7, 1000, 10, "remote-instance");
    status.SetRoleState("primary", "RUNNING");
    auto report = doctor();
    EXPECT_EQ(report.status, "degraded");
    const auto* issue = FindIssue(report, "MASTER_VIEW_MISMATCH");
    ASSERT_NE(issue, nullptr);
    EXPECT_NE(issue->message.find("local=local"), std::string::npos);
    EXPECT_NE(issue->message.find("local_instance_id=local-instance"),
              std::string::npos);
    EXPECT_NE(issue->message.find("view=remote"), std::string::npos);
    EXPECT_NE(issue->message.find("view_instance_id=remote-instance"),
              std::string::npos);
    EXPECT_NE(issue->message.find("view_epoch=7"), std::string::npos);

    status.SetMasterView("local", 8, 1000, 10, "local-instance");
    report = doctor();
    EXPECT_EQ(report.status, "ok");
    EXPECT_EQ(FindIssue(report, "MASTER_VIEW_MISMATCH"), nullptr);
}

TEST_F(HAStatusTest, DoctorHandlesMissingViewAndEmptyInstanceConservatively) {
    status.SetRoleState("primary", "RUNNING");
    auto report = doctor();
    EXPECT_EQ(report.status, "degraded");
    const auto* issue = FindIssue(report, "MASTER_VIEW_MISMATCH");
    ASSERT_NE(issue, nullptr);
    EXPECT_NE(issue->message.find("view=<empty>"), std::string::npos);

    status.SetRoleState("standby", "WATCHING");
    report = doctor();
    EXPECT_EQ(report.status, "ok");
    EXPECT_EQ(FindIssue(report, "MASTER_VIEW_MISMATCH"), nullptr);

    status.SetRoleState("primary", "STARTING");
    report = doctor();
    EXPECT_EQ(report.status, "unknown");
    EXPECT_EQ(FindIssue(report, "MASTER_VIEW_MISMATCH"), nullptr);

    status.SetRoleState("primary", "RUNNING");
    status.SetMasterView("local", 1, 1000, 10, "remote-instance");
    report = doctor();
    EXPECT_EQ(report.status, "ok");
    EXPECT_EQ(FindIssue(report, "MASTER_VIEW_MISMATCH"), nullptr);
}

TEST_F(HAStatusTest, StatusTracksPendingWrites) {
    status.SetLeader("local", 1);
    status.SetRoleState("primary", "RUNNING");
    status.SetOpLogWriterRunning(true);
    status.SetOpLogWriteProgress(1, 3, 10);
    auto snapshot = status.Snapshot();
    EXPECT_EQ(snapshot.oplog_pending_writes, 3u);
    EXPECT_EQ(snapshot.oplog_writer_pending_writes, 3u);
    EXPECT_EQ(snapshot.oplog_writer_queue_capacity, 10u);
    EXPECT_EQ(snapshot.oplog_writer_running, true);
    EXPECT_EQ(snapshot.oplog_writer_last_committed_sequence_id, 1u);

    status.SetOpLogWriterRunning(false);
    snapshot = status.Snapshot();
    EXPECT_EQ(snapshot.oplog_pending_writes, 0u);
    EXPECT_EQ(snapshot.oplog_writer_pending_writes, 0u);
    EXPECT_EQ(snapshot.oplog_writer_queue_capacity, 0u);
    EXPECT_EQ(snapshot.oplog_writer_running, false);
    EXPECT_EQ(snapshot.oplog_writer_last_committed_sequence_id, 0u);

    status.SetOpLogWriteProgress(1, 5);
    EXPECT_EQ(status.Snapshot().oplog_writer_pending_writes, 0u);
}

TEST_F(HAStatusTest, DoctorReportsOpLogQueuePressure) {
    status.SetLeader("local", 1);
    status.SetRoleState("primary", "RUNNING");
    status.SetOpLogWriterRunning(true);
    status.SetOpLogWriteProgress(10, 79, 100);
    EXPECT_EQ(doctor().status, "ok");

    status.SetOpLogWriteProgress(10, 80, 100);
    auto report = doctor();
    EXPECT_EQ(report.status, "degraded");
    const auto* issue = FindIssue(report, "OPLOG_QUEUE_PRESSURE");
    ASSERT_NE(issue, nullptr);
    EXPECT_NE(issue->message.find("pending=80"), std::string::npos);
    EXPECT_NE(issue->message.find("capacity=100"), std::string::npos);
    EXPECT_NE(issue->message.find("usage_percent=80"), std::string::npos);
    EXPECT_NE(issue->message.find("threshold_percent=80"), std::string::npos);

    status.SetRoleState("standby", "WATCHING");
    EXPECT_EQ(FindIssue(doctor(), "OPLOG_QUEUE_PRESSURE"), nullptr);

    status.SetRoleState("primary", "RUNNING");
    status.SetOpLogWriteProgress(10, 80);
    EXPECT_EQ(FindIssue(doctor(), "OPLOG_QUEUE_PRESSURE"), nullptr);
}

TEST_F(HAStatusTest, DoctorReportsOpLogPollStale) {
    status.Configure(true, "test_cluster", "local", 10, 10);
    status.SetRoleState("standby", "WATCHING");
    status.SetOpLogPollSuccess();
    auto snapshot = status.Snapshot();
    EXPECT_GT(snapshot.oplog_last_successful_poll_timestamp_ms, 0);
    EXPECT_GE(snapshot.oplog_poll_success_age_ms, 0);
    EXPECT_EQ(doctor().status, "ok");

    HAStatusTestPeer::AgeLastPollSuccess(status, std::chrono::seconds(11));
    auto report = doctor();
    EXPECT_EQ(report.status, "degraded");
    const auto* issue = FindIssue(report, "OPLOG_POLL_STALE");
    ASSERT_NE(issue, nullptr);
    EXPECT_NE(issue->message.find("last_successful_poll_ms="),
              std::string::npos);
    EXPECT_NE(issue->message.find("stale_ms="), std::string::npos);
    EXPECT_NE(issue->message.find("timeout_sec=10"), std::string::npos);

    status.SetDoctorIssue("OPLOG_READ_FAILED", "Read after seq=10: TIMEOUT");
    report = doctor();
    EXPECT_NE(FindIssue(report, "OPLOG_READ_FAILED"), nullptr);
    EXPECT_EQ(FindIssue(report, "OPLOG_POLL_STALE"), nullptr);

    status.SetDoctorIssue("OPLOG_READ_FAILED");
    status.SetLeader("local", 1);
    status.SetMasterView("local", 1);
    status.SetRoleState("primary", "RUNNING");
    EXPECT_EQ(doctor().status, "ok");
}

TEST_F(HAStatusTest, StatusExposesClearOplogAndMasterViewAliases) {
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    status.SetMasterView("local", 9, now_ms - 1000, 10, "local-instance");
    status.SetOpLogProgress(12, 15, 2, 3);
    auto snapshot = status.Snapshot();
    EXPECT_EQ(snapshot.master_view_address, "local");
    EXPECT_EQ(snapshot.master_view_endpoint, "local");
    EXPECT_EQ(snapshot.master_view_timestamp_ms, now_ms - 1000);
    EXPECT_EQ(snapshot.master_view_updated_at_ms, now_ms - 1000);
    EXPECT_GE(snapshot.master_view_age_ms, 1000);
    EXPECT_GT(snapshot.master_view_expires_in_ms, 0);
    EXPECT_EQ(snapshot.applied_sequence_id, 12u);
    EXPECT_EQ(snapshot.oplog_applied_sequence_id, 12u);
    EXPECT_EQ(snapshot.latest_sequence_id, 15u);
    EXPECT_EQ(snapshot.oplog_latest_sequence_id, 15u);
    EXPECT_TRUE(snapshot.oplog_latest_read_ok);
    EXPECT_EQ(snapshot.trimmed_sequence_id, 2u);
    EXPECT_EQ(snapshot.oplog_trimmed_sequence_id, 2u);
    EXPECT_EQ(snapshot.gc_cursor_sequence_id, 3u);
    EXPECT_EQ(snapshot.oplog_gc_cursor_sequence_id, 3u);
    EXPECT_EQ(snapshot.oplog_lag, 3u);
    EXPECT_EQ(snapshot.oplog_apply_lag, 3u);

    status.SetOpLogProgress(13, std::nullopt, 4, 5);
    snapshot = status.Snapshot();
    EXPECT_EQ(snapshot.latest_sequence_id, 15u);
    EXPECT_EQ(snapshot.oplog_latest_sequence_id, 15u);
    EXPECT_FALSE(snapshot.oplog_latest_read_ok);
    EXPECT_EQ(snapshot.oplog_lag, 2u);
    EXPECT_EQ(snapshot.oplog_apply_lag, 2u);
}

TEST_F(HAStatusTest, DoctorReportsStandbyBehindTrim) {
    status.SetOpLogProgress(/*applied_sequence_id=*/99,
                            /*latest_sequence_id=*/150,
                            /*trimmed_sequence_id=*/100,
                            /*gc_cursor_sequence_id=*/100);
    auto report = doctor();
    EXPECT_EQ(report.status, "degraded");
    const auto* issue = FindIssue(report, "STANDBY_BEHIND_TRIM");
    ASSERT_NE(issue, nullptr);
    EXPECT_NE(issue->message.find("oplog_applied_sequence_id=99"),
              std::string::npos);
    EXPECT_NE(issue->message.find("oplog_trimmed_sequence_id=100"),
              std::string::npos);
    EXPECT_NE(issue->message.find("oplog_latest_sequence_id=150"),
              std::string::npos);
    EXPECT_NE(issue->message.find("oplog_apply_lag=51"), std::string::npos);
    EXPECT_NE(issue->message.find("action=snapshot_resync_required"),
              std::string::npos);
    EXPECT_EQ(FindIssue(report, "OPLOG_TRIM_RISK"), nullptr);

    status.SetRoleState("standby", "SYNCING");
    EXPECT_EQ(doctor().status, "unknown");
    status.SetRoleState("primary", "RUNNING");
    status.SetMasterView("local", 1);
    EXPECT_EQ(doctor().status, "ok");
}

TEST_F(HAStatusTest, DoctorReportsOpLogTrimRiskNearHorizon) {
    status.SetOpLogProgress(/*applied_sequence_id=*/105,
                            /*latest_sequence_id=*/150,
                            /*trimmed_sequence_id=*/100,
                            /*gc_cursor_sequence_id=*/100);
    auto report = doctor();
    EXPECT_EQ(report.status, "degraded");
    const auto* issue = FindIssue(report, "OPLOG_TRIM_RISK");
    ASSERT_NE(issue, nullptr);
    EXPECT_NE(issue->message.find("oplog_applied_sequence_id=105"),
              std::string::npos);
    EXPECT_NE(issue->message.find("oplog_trimmed_sequence_id=100"),
              std::string::npos);
    EXPECT_NE(issue->message.find("oplog_latest_sequence_id=150"),
              std::string::npos);
    EXPECT_NE(issue->message.find("oplog_apply_lag=45"), std::string::npos);
    EXPECT_NE(issue->message.find("oplog_trim_headroom=5"), std::string::npos);
    EXPECT_NE(issue->message.find("oplog_retained_window=50"),
              std::string::npos);
    EXPECT_NE(issue->message.find("threshold_percent=10"), std::string::npos);

    status.SetOpLogProgress(/*applied_sequence_id=*/106,
                            /*latest_sequence_id=*/150,
                            /*trimmed_sequence_id=*/100,
                            /*gc_cursor_sequence_id=*/100);
    EXPECT_EQ(doctor().status, "ok");
    status.SetOpLogProgress(/*applied_sequence_id=*/100,
                            /*latest_sequence_id=*/100,
                            /*trimmed_sequence_id=*/100,
                            /*gc_cursor_sequence_id=*/100);
    EXPECT_EQ(doctor().status, "ok");
}

}  // namespace mooncake
