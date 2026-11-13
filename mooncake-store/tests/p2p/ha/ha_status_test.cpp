#include <gtest/gtest.h>
#include <ylt/struct_json/json_reader.h>

#include <chrono>
#include <mutex>

#include "p2p/ha/ha_metric_manager.h"
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
    static void AgeLagGrowthStart(HAStatusManager& status,
                                  std::chrono::seconds age) {
        std::lock_guard<std::mutex> lock(status.mutex_);
        ASSERT_TRUE(status.oplog_lag_growth_.since.has_value());
        *status.oplog_lag_growth_.since -= age;
    }
    static void AgeMasterViewRefresh(HAStatusManager& status,
                                     std::chrono::milliseconds age) {
        std::lock_guard<std::mutex> lock(status.mutex_);
        status.master_view_refreshed_at_ms_ -= age.count();
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

    HAStatusSnapshot parsed_status() {
        HAStatusSnapshot parsed;
        struct_json::from_json(parsed, status.SerializeJson());
        return parsed;
    }
};

TEST_F(HAStatusTest, CountsObservedTermsAcrossSupervisorRounds) {
    status.SetMasterView("leader", 10);
    status.SetMasterView("leader", 10);
    status.SetLeader("leader", 11);
    status.SetMasterView("", 100);
    status.SetMasterView("leader", 12);
    EXPECT_EQ(doctor().health, "ok");
    status.SetMasterView("leader", 11);
    status.SetMasterView("leader", 12);
    status.ClearDoctorIssues();
    status.Configure(true, "test_cluster", "local");
    EXPECT_EQ(doctor().health, "ok");
    status.SetLeader("local", 20);
    status.SetMasterView("local", 20);
    status.SetRoleState("primary", "RUNNING");
    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
    ASSERT_EQ(report.issues.size(), 1u);
    EXPECT_EQ(report.issues[0].code, "ELECTION_UNSTABLE");
    EXPECT_NE(report.issues[0].message.find("window_sec=300, changes=3"),
              std::string::npos);
    EXPECT_NE(report.issues[0].message.find("leader=local, epoch=20"),
              std::string::npos);
    status.SetDoctorIssue("ELECTION_FAILED", "Redis unavailable");
    EXPECT_EQ(doctor().issues.size(), 2u);
    status.ClearDoctorIssues();
    EXPECT_EQ(doctor().health, "degraded");
}

TEST_F(HAStatusTest, ElectionWindowExpiresAndResets) {
    for (uint64_t epoch = 17; epoch <= 20; ++epoch) {
        status.SetLeader("other", epoch);
    }
    ASSERT_EQ(doctor().health, "degraded");
    HAStatusTestPeer::ExpireOldestChange(status);
    EXPECT_EQ(doctor().health, "ok");
    status.SetLeader("other", 21);
    EXPECT_EQ(doctor().health, "degraded");

    status.Configure(false, "test_cluster", "local");
    status.SetLeader("other", 22);
    EXPECT_EQ(doctor().health, "unknown");
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
    EXPECT_EQ(doctor().health, "degraded");
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
    EXPECT_EQ(report.health, "degraded");
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
    EXPECT_EQ(report.health, "ok");
    EXPECT_EQ(FindIssue(report, "MASTER_VIEW_MISMATCH"), nullptr);
}

TEST_F(HAStatusTest, DoctorHandlesMissingViewAndEmptyInstanceConservatively) {
    status.SetRoleState("primary", "RUNNING");
    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
    const auto* issue = FindIssue(report, "MASTER_VIEW_MISMATCH");
    ASSERT_NE(issue, nullptr);
    EXPECT_NE(issue->message.find("view=<empty>"), std::string::npos);

    status.SetRoleState("standby", "WATCHING");
    report = doctor();
    EXPECT_EQ(report.health, "ok");
    EXPECT_EQ(FindIssue(report, "MASTER_VIEW_MISMATCH"), nullptr);

    status.SetRoleState("primary", "STARTING");
    report = doctor();
    EXPECT_EQ(report.health, "unknown");
    EXPECT_EQ(FindIssue(report, "MASTER_VIEW_MISMATCH"), nullptr);

    status.SetRoleState("primary", "RUNNING");
    status.SetMasterView("local", 1, 1000, 10, "remote-instance");
    report = doctor();
    EXPECT_EQ(report.health, "ok");
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
    EXPECT_EQ(doctor().health, "ok");

    status.SetOpLogWriteProgress(10, 80, 100);
    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
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

TEST_F(HAStatusTest, DoctorMetricsTrackActiveIssuesAndRecovery) {
    const auto metrics = [&] {
        return HAMetricManager::instance().serialize_metrics();
    };
    EXPECT_NE(metrics().find("ha_doctor_health{status=\"ok\"} 1\n"),
              std::string::npos);
    EXPECT_NE(metrics().find("ha_doctor_issues 0\n"), std::string::npos);
    EXPECT_EQ(metrics().find("ha_doctor_issue_active{"), std::string::npos);

    status.SetDoctorIssue("OPLOG_READ_FAILED", "Redis read failed");
    auto output = metrics();
    EXPECT_NE(output.find("ha_doctor_health{status=\"degraded\"} 1\n"),
              std::string::npos);
    EXPECT_NE(output.find("ha_doctor_issues 1\n"), std::string::npos);
    EXPECT_NE(output.find("ha_doctor_issue_active{code=\"OPLOG_READ_FAILED\","
                          "category=\"oplog\",component=\"oplog_reader\","
                          "severity=\"error\"} 1\n"),
              std::string::npos);
    EXPECT_EQ(output.find("Redis read failed"), std::string::npos);

    status.SetDoctorIssue("OPLOG_READ_FAILED");
    output = metrics();
    EXPECT_NE(output.find("ha_doctor_health{status=\"ok\"} 1\n"),
              std::string::npos);
    EXPECT_NE(output.find("ha_doctor_issues 0\n"), std::string::npos);
    EXPECT_EQ(output.find("ha_doctor_issue_active{"), std::string::npos);

    status.Configure(false, "test_cluster", "local");
    EXPECT_NE(metrics().find("ha_doctor_health{status=\"unknown\"} 1\n"),
              std::string::npos);
}

TEST_F(HAStatusTest, DoctorMetricsRecomputeDerivedIssuesAtScrapeTime) {
    status.SetLeader("local", 1);
    status.SetRoleState("primary", "RUNNING");
    status.SetOpLogWriterRunning(true);
    status.SetOpLogWriteProgress(10, 80, 100);

    auto output = HAMetricManager::instance().serialize_metrics();
    EXPECT_NE(
        output.find("ha_doctor_issue_active{code=\"OPLOG_QUEUE_PRESSURE\","
                    "category=\"oplog\",component=\"oplog_writer\","
                    "severity=\"warning\"} 1\n"),
        std::string::npos);
    EXPECT_NE(output.find("ha_doctor_issues 1\n"), std::string::npos);

    status.SetOpLogWriteProgress(10, 0, 100);
    output = HAMetricManager::instance().serialize_metrics();
    EXPECT_EQ(output.find("ha_doctor_issue_active{"), std::string::npos);
    EXPECT_NE(output.find("ha_doctor_issues 0\n"), std::string::npos);

    status.SetMasterView("local", 1, /*timestamp_ms=*/1, /*ttl_sec=*/2);
    HAStatusTestPeer::AgeMasterViewRefresh(status,
                                           std::chrono::milliseconds(4000));
    output = HAMetricManager::instance().serialize_metrics();
    EXPECT_NE(output.find("code=\"MASTER_VIEW_LEASE_EXPIRED\""),
              std::string::npos);
    status.MarkMasterViewRefreshed();
    output = HAMetricManager::instance().serialize_metrics();
    EXPECT_EQ(output.find("code=\"MASTER_VIEW_LEASE_EXPIRED\""),
              std::string::npos);
}

TEST_F(HAStatusTest, DoctorMetricsEscapeIssueCodeLabels) {
    status.SetDoctorIssue("CUSTOM_\"\\\n", "unexpected code");
    const auto output = HAMetricManager::instance().serialize_metrics();
    EXPECT_NE(output.find("code=\"CUSTOM_\\\"\\\\\\n\""), std::string::npos);
}

TEST_F(HAStatusTest, DoctorReportsPendingRejectionUntilReinitialization) {
    status.RecordOpLogPendingRejection(1201, 100, 1000, 1000);
    status.RecordOpLogPendingRejection(1202, 100, 1000, 1000);
    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
    const auto* issue = FindIssue(report, "OPLOG_PENDING_REJECTED");
    ASSERT_NE(issue, nullptr);
    EXPECT_EQ(issue->component, "oplog_applier");
    EXPECT_EQ(issue->severity, "error");
    EXPECT_NE(issue->message.find("first_rejected_sequence_id=1201"),
              std::string::npos);
    EXPECT_NE(issue->message.find("capacity=1000"), std::string::npos);
    EXPECT_NE(issue->message.find("values=snapshot_at_first_rejection"),
              std::string::npos);
    status.SetRoleState("primary", "RUNNING");
    EXPECT_EQ(FindIssue(doctor(), "OPLOG_PENDING_REJECTED"), nullptr);
}

TEST_F(HAStatusTest, DoctorReportsGrowingLagDespiteApplyProgress) {
    status.SetOpLogProgress(100, 200);
    status.SetOpLogProgress(200, 1200);
    HAStatusTestPeer::AgeLagGrowthStart(status, std::chrono::seconds(31));
    status.SetOpLogProgress(300, 2300);
    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
    const auto* issue = FindIssue(report, "OPLOG_LAG_GROWING");
    ASSERT_NE(issue, nullptr);
    EXPECT_EQ(issue->severity, "warning");
    EXPECT_NE(issue->message.find("baseline_lag=100"), std::string::npos);
    EXPECT_NE(issue->message.find("current_lag=2000"), std::string::npos);
    EXPECT_EQ(FindIssue(report, "OPLOG_CATCHUP_STALLED"), nullptr);

    status.ResetOpLogLagGrowth();
    EXPECT_EQ(FindIssue(doctor(), "OPLOG_LAG_GROWING"), nullptr);

    status.SetOpLogProgress(2250, 2300);
    EXPECT_EQ(FindIssue(doctor(), "OPLOG_LAG_GROWING"), nullptr);

    status.SetOpLogProgress(2250, 3300);
    HAStatusTestPeer::AgeLagGrowthStart(status, std::chrono::seconds(31));
    status.SetOpLogProgress(2250, std::nullopt);
    EXPECT_EQ(FindIssue(doctor(), "OPLOG_LAG_GROWING"), nullptr);
}

TEST_F(HAStatusTest, DoctorReportsOpLogPollStale) {
    status.Configure(true, "test_cluster", "local", 10, 10);
    status.SetRoleState("standby", "WATCHING");
    status.SetOpLogPollSuccess();
    auto snapshot = status.Snapshot();
    EXPECT_GT(snapshot.oplog_last_successful_poll_timestamp_ms, 0);
    EXPECT_GE(snapshot.oplog_poll_success_age_ms, 0);
    EXPECT_EQ(doctor().health, "ok");

    HAStatusTestPeer::AgeLastPollSuccess(status, std::chrono::seconds(11));
    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
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
    EXPECT_EQ(doctor().health, "ok");
}

TEST_F(HAStatusTest, DoctorReportsElectionReadinessBlocked) {
    status.SetOpLogProgress(40, 140);
    EXPECT_EQ(doctor().health, "ok");

    // The readiness gate is event-driven: only set while the standby is
    // actually blocked inside the readiness wait loop or election callback.
    status.SetElectionReadinessBlocked(true, 100, 50);
    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
    const auto* issue = FindIssue(report, "ELECTION_READINESS_BLOCKED");
    ASSERT_NE(issue, nullptr);
    EXPECT_EQ(issue->severity, "warning");
    EXPECT_NE(issue->message.find("lag=100"), std::string::npos);
    EXPECT_NE(issue->message.find("max=50"), std::string::npos);
    EXPECT_NE(issue->message.find("applied_sequence_id=40"), std::string::npos);
    EXPECT_NE(issue->message.find("latest_sequence_id=140"), std::string::npos);

    // Once the standby catches up the caller clears the flag and the issue is
    // gone, so normal lag/catch-up never produces a false positive.
    status.SetElectionReadinessBlocked(false);
    EXPECT_EQ(FindIssue(doctor(), "ELECTION_READINESS_BLOCKED"), nullptr);
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

TEST_F(HAStatusTest, DoctorFlagsMasterViewLeaseExpiredForStuckPrimary) {
    status.SetInstanceId("local-instance");
    status.SetMasterView("local", 7, /*timestamp_ms=*/1, /*ttl_sec=*/2,
                         "local-instance");
    status.SetRoleState("primary", "RUNNING");

    // Freshly refreshed lease: no expiry issue.
    EXPECT_EQ(FindIssue(doctor(), "MASTER_VIEW_LEASE_EXPIRED"), nullptr);

    // Simulate a stuck renewal loop: no refresh for longer than the TTL (plus
    // the grace window). The lease has elapsed without LEADERSHIP_LOST firing.
    HAStatusTestPeer::AgeMasterViewRefresh(status,
                                           std::chrono::milliseconds(4000));
    // /ha/status reports a lease remaining time and therefore clamps an
    // expired estimate to zero; doctor retains the signed diagnostic detail.
    EXPECT_EQ(status.Snapshot().master_view_expires_in_ms, 0);
    auto report = doctor();
    const auto* issue = FindIssue(report, "MASTER_VIEW_LEASE_EXPIRED");
    ASSERT_NE(issue, nullptr);
    EXPECT_NE(issue->message.find("expires_in_ms=-"), std::string::npos);
    EXPECT_NE(issue->message.find("view_epoch=7"), std::string::npos);

    // The renew-failure path already raises LEADERSHIP_LOST; that should mask
    // this time-based issue to avoid a duplicate alarm.
    status.SetDoctorIssue("LEADERSHIP_LOST",
                          "Leadership renewal failed or lost");
    EXPECT_EQ(FindIssue(doctor(), "MASTER_VIEW_LEASE_EXPIRED"), nullptr);
}

TEST_F(HAStatusTest, MasterViewLeaseExpiredDoctorIsGraceBoundedAndSelfHealing) {
    status.SetInstanceId("local-instance");
    status.SetMasterView("local", 7, /*timestamp_ms=*/1, /*ttl_sec=*/2,
                         "local-instance");
    status.SetRoleState("primary", "RUNNING");

    // Past the lease but still inside the grace window: not yet flagged.
    HAStatusTestPeer::AgeMasterViewRefresh(status,
                                           std::chrono::milliseconds(2500));
    EXPECT_EQ(FindIssue(doctor(), "MASTER_VIEW_LEASE_EXPIRED"), nullptr);

    // Crossing the grace threshold once the lap is real flags it.
    HAStatusTestPeer::AgeMasterViewRefresh(status,
                                           std::chrono::milliseconds(2000));
    EXPECT_NE(FindIssue(doctor(), "MASTER_VIEW_LEASE_EXPIRED"), nullptr);

    // A successful renewal refreshes the base and the issue self-heals
    // (recomputed each poll, not a sticky SetDoctorIssue).
    status.MarkMasterViewRefreshed();
    EXPECT_EQ(FindIssue(doctor(), "MASTER_VIEW_LEASE_EXPIRED"), nullptr);
}

TEST_F(HAStatusTest, MasterViewLeaseExpiredDoctorIsPrimaryOnly) {
    status.SetInstanceId("local-instance");
    status.SetMasterView("local", 7, /*timestamp_ms=*/1, /*ttl_sec=*/2,
                         "local-instance");
    status.SetRoleState("standby", "WATCHING");
    HAStatusTestPeer::AgeMasterViewRefresh(status,
                                           std::chrono::milliseconds(5000));
    EXPECT_EQ(status.Snapshot().master_view_expires_in_ms, 0);
    EXPECT_EQ(FindIssue(doctor(), "MASTER_VIEW_LEASE_EXPIRED"), nullptr);
}

TEST_F(HAStatusTest, SerializedStatusClampsExpiredMasterViewExpiry) {
    status.SetMasterView("leader", 7, /*timestamp_ms=*/1,
                         /*ttl_sec=*/2, "leader-instance");
    HAStatusTestPeer::AgeMasterViewRefresh(status,
                                           std::chrono::milliseconds(5000));
    const auto parsed = parsed_status();
    EXPECT_EQ(parsed.master_view_expires_in_ms, 0);
    EXPECT_EQ(parsed.master_view_ttl_sec, 2);
    EXPECT_EQ(parsed.master_view_epoch, 7u);
}

TEST_F(HAStatusTest, MasterViewExpiryFollowsLeaseRenewalNotElectionTime) {
    // Simulate a long-running leader elected at a prehistoric timestamp with
    // a small TTL. The old fixed-timestamp formula reported a deeply negative
    // remaining lease; the refresh-based value must track renewals and stay
    // positive instead.
    status.SetMasterView("local", 7, /*timestamp_ms=*/1, /*ttl_sec=*/5,
                         "local-instance");
    EXPECT_EQ(status.Snapshot().master_view_timestamp_ms, 1);
    auto after_election = status.Snapshot();
    EXPECT_GT(after_election.master_view_expires_in_ms, 0);

    // Renewing the lease (as KeepLeader does) must keep the remaining lease
    // positive and anchored to the renewal, not the stale election timestamp.
    status.MarkMasterViewRefreshed();
    auto after_renew = status.Snapshot();
    EXPECT_GT(after_renew.master_view_expires_in_ms, 0);
    EXPECT_EQ(after_renew.master_view_timestamp_ms, 1);
}

TEST_F(HAStatusTest, DoctorContextMirrorsCurrentStatusSnapshot) {
    const auto before_heartbeat_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    status.SetInstanceId("local-instance");
    status.SetMasterView("local", 9, before_heartbeat_ms - 1000, 10,
                         "local-instance");
    status.SetRoleState("primary", "RUNNING");
    status.SetSnapshotReady(true);
    status.SetOpLogProgress(/*applied_sequence_id=*/12,
                            /*latest_sequence_id=*/15,
                            /*trimmed_sequence_id=*/2,
                            /*gc_cursor_sequence_id=*/3);
    status.RecordRegistryHeartbeat(ErrorCode::OK, "");

    auto report = doctor();
    EXPECT_EQ(report.health, "ok");
    EXPECT_EQ(report.status.role, "primary");
    EXPECT_EQ(report.status.state, "RUNNING");
    EXPECT_EQ(report.status.local_endpoint, "local");
    EXPECT_EQ(report.status.leader_endpoint, "local");
    EXPECT_EQ(report.status.epoch, 9u);
    EXPECT_EQ(report.status.instance_id, "local-instance");
    EXPECT_EQ(report.status.applied_sequence_id, 12u);
    EXPECT_EQ(report.status.latest_sequence_id, 15u);
    EXPECT_EQ(report.status.trimmed_sequence_id, 2u);
    EXPECT_EQ(report.status.gc_cursor_sequence_id, 3u);
    EXPECT_EQ(report.status.oplog_lag, 3u);
    EXPECT_TRUE(report.status.snapshot_ready);
    EXPECT_GE(report.status.heartbeat_age_ms, 0);
}

TEST_F(HAStatusTest, SetRoleStateClearsIssuesScopedToPreviousRole) {
    status.SetDoctorIssue("OPLOG_READ_FAILED", "old standby read failure");
    status.SetDoctorIssue("OPLOG_RECONNECT_FAILED",
                          "old standby reconnect failure");
    status.SetDoctorIssue("SNAPSHOT_RECOVERY_FAILED",
                          "old standby snapshot failure");
    status.SetDoctorIssue("REGISTRY_HEARTBEAT_FAILED", "shared registry issue");
    status.SetLeader("local", 1);
    status.SetRoleState("primary", "RUNNING");

    auto report = doctor();
    EXPECT_EQ(FindIssue(report, "OPLOG_READ_FAILED"), nullptr);
    EXPECT_EQ(FindIssue(report, "OPLOG_RECONNECT_FAILED"), nullptr);
    EXPECT_EQ(FindIssue(report, "SNAPSHOT_RECOVERY_FAILED"), nullptr);
    EXPECT_NE(FindIssue(report, "REGISTRY_HEARTBEAT_FAILED"), nullptr);

    status.ClearDoctorIssues();
    status.SetDoctorIssue("OPLOG_WRITE_FAILED", "old primary write failure");
    status.SetDoctorIssue("OPLOG_COMMIT_FAILED", "old primary commit failure");
    status.SetDoctorIssue("PROMOTION_CATCHUP_INCOMPLETE",
                          "old primary catch-up issue");
    status.SetDoctorIssue("OPLOG_READ_FAILED", "new standby read failure");
    status.SetRoleState("standby", "WATCHING");

    report = doctor();
    EXPECT_EQ(FindIssue(report, "OPLOG_WRITE_FAILED"), nullptr);
    EXPECT_EQ(FindIssue(report, "OPLOG_COMMIT_FAILED"), nullptr);
    EXPECT_EQ(FindIssue(report, "PROMOTION_CATCHUP_INCOMPLETE"), nullptr);
    EXPECT_NE(FindIssue(report, "OPLOG_READ_FAILED"), nullptr);
}

TEST_F(HAStatusTest, DoctorReportsStandbyBehindTrim) {
    status.SetOpLogProgress(/*applied_sequence_id=*/99,
                            /*latest_sequence_id=*/150,
                            /*trimmed_sequence_id=*/100,
                            /*gc_cursor_sequence_id=*/100);
    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
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
    EXPECT_EQ(doctor().health, "unknown");
    status.SetRoleState("primary", "RUNNING");
    status.SetMasterView("local", 1);
    EXPECT_EQ(doctor().health, "ok");
}

TEST_F(HAStatusTest, DoctorReportsOpLogTrimRiskNearHorizon) {
    status.SetOpLogProgress(/*applied_sequence_id=*/105,
                            /*latest_sequence_id=*/150,
                            /*trimmed_sequence_id=*/100,
                            /*gc_cursor_sequence_id=*/100);
    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
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
    EXPECT_EQ(doctor().health, "ok");
    status.SetOpLogProgress(/*applied_sequence_id=*/100,
                            /*latest_sequence_id=*/100,
                            /*trimmed_sequence_id=*/100,
                            /*gc_cursor_sequence_id=*/100);
    EXPECT_EQ(doctor().health, "ok");
}

TEST_F(HAStatusTest, DoctorReportsInsufficientStandbySnapshotSources) {
    status.SetRoleState("primary", "RUNNING");
    status.SetMasterView("local", 1);
    status.SetSnapshotReady(true);
    status.SetStandbySnapshotAvailability(/*available=*/0, /*required=*/1);

    auto report = doctor();
    EXPECT_EQ(report.health, "degraded");
    const auto* issue = FindIssue(report, "STANDBY_SNAPSHOT_INSUFFICIENT");
    ASSERT_NE(issue, nullptr);
    EXPECT_NE(issue->message.find("standby_snapshot_available=0"),
              std::string::npos);
    EXPECT_NE(issue->message.find("standby_snapshot_required=1"),
              std::string::npos);
    EXPECT_NE(issue->message.find("primary_snapshot_fallback_available=true"),
              std::string::npos);
    EXPECT_NE(issue->message.find("action=ensure_standby_snapshot_source"),
              std::string::npos);

    status.SetStandbySnapshotAvailability(/*available=*/1, /*required=*/1);
    EXPECT_EQ(doctor().health, "ok");

    status.SetStandbySnapshotAvailability(/*available=*/0, /*required=*/0);
    EXPECT_EQ(doctor().health, "ok");

    status.SetStandbySnapshotAvailability(/*available=*/0, /*required=*/1);
    status.SetRoleState("standby", "WATCHING");
    EXPECT_EQ(doctor().health, "ok");
}

}  // namespace mooncake
