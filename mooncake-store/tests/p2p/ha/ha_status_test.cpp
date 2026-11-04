#include <gtest/gtest.h>
#include <ylt/struct_json/json_reader.h>

#include <chrono>
#include <mutex>

#include "p2p/ha/ha_status.h"

namespace mooncake {

struct HAStatusTestPeer {
    static void ExpireOldestChange(HAStatusManager& status) {
        std::lock_guard<std::mutex> lock(status.mutex_);
        status.election_.changes.front() -= std::chrono::seconds(301);
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
    status.SetLeader("other", 20);
    status.SetMasterView("other", 20);
    status.SetRoleState("primary", "RUNNING");
    auto report = doctor();
    EXPECT_EQ(report.status, "degraded");
    ASSERT_EQ(report.issues.size(), 1u);
    EXPECT_EQ(report.issues[0].code, "ELECTION_UNSTABLE");
    EXPECT_NE(report.issues[0].message.find("window_sec=300, changes=3"),
              std::string::npos);
    EXPECT_NE(report.issues[0].message.find("leader=other, epoch=20"),
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

}  // namespace mooncake
