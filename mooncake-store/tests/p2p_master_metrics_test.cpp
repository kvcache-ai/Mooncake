// Tests for P2PMasterMetricManager: singleton routing, serialization
// partitioning and reset. Registers the P2P singleton (single architecture
// per process).

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "master_metric_manager.h"
#include "p2p_master_metric_manager.h"

namespace mooncake::test {

class P2PMasterMetricsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("P2PMasterMetricsTest");
        FLAGS_logtostderr = true;
        P2PMasterMetricManager::instance().reset_all_metrics();
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(P2PMasterMetricsTest, SingletonRoutingTest) {
    // The base singleton must route to the registered P2P instance.
    EXPECT_EQ(&MasterMetricManager::instance(),
              &P2PMasterMetricManager::instance());

    // Idempotent.
    EXPECT_EQ(&P2PMasterMetricManager::instance(),
              &P2PMasterMetricManager::instance());
}

TEST_F(P2PMasterMetricsTest, CountersAndResetTest) {
    auto& metrics = P2PMasterMetricManager::instance();

    metrics.inc_get_write_route_requests();
    metrics.inc_add_replica_requests(2);
    metrics.inc_batch_remove_replica_requests(5);
    metrics.inc_batch_get_write_route_requests(7);
    metrics.inc_batch_get_write_route_partial_success(2);
    EXPECT_EQ(metrics.get_get_write_route_requests(), 1);
    EXPECT_EQ(metrics.get_add_replica_requests(), 2);
    EXPECT_EQ(metrics.get_batch_remove_replica_requests(), 1);
    EXPECT_EQ(metrics.get_batch_remove_replica_items(), 5);
    EXPECT_EQ(metrics.get_batch_get_write_route_requests(), 1);
    EXPECT_EQ(metrics.get_batch_get_write_route_items(), 7);
    EXPECT_EQ(metrics.get_batch_get_write_route_partial_successes(), 1);
    EXPECT_EQ(metrics.get_batch_get_write_route_failed_items(), 2);

    metrics.reset_all_metrics();
    EXPECT_EQ(metrics.get_get_write_route_requests(), 0);
    EXPECT_EQ(metrics.get_add_replica_requests(), 0);
    EXPECT_EQ(metrics.get_batch_remove_replica_requests(), 0);
    EXPECT_EQ(metrics.get_batch_remove_replica_items(), 0);
    EXPECT_EQ(metrics.get_batch_get_write_route_items(), 0);
    EXPECT_EQ(metrics.get_batch_get_write_route_failed_items(), 0);
    // Shared metrics must still be reachable through the same instance.
    EXPECT_EQ(metrics.get_key_count(), 0);
}

TEST_F(P2PMasterMetricsTest, SerializeMetricsContentTest) {
    // After reset, zeros are force-marked changed so all owned metrics
    // appear.
    std::string text = MasterMetricManager::instance().serialize_metrics();

    EXPECT_NE(text.find("master_total_capacity_bytes"), std::string::npos);
    EXPECT_NE(text.find("master_key_count"), std::string::npos);
    EXPECT_NE(text.find("master_active_clients"), std::string::npos);
    EXPECT_NE(text.find("master_heartbeat_requests_total"), std::string::npos);
    EXPECT_NE(text.find("master_allocated_bytes"), std::string::npos);

    EXPECT_NE(text.find("master_get_write_route_requests_total"),
              std::string::npos);
    EXPECT_NE(text.find("master_add_replica_requests_total"),
              std::string::npos);
    EXPECT_NE(text.find("master_remove_replica_requests_total"),
              std::string::npos);
    EXPECT_NE(text.find("master_batch_remove_replica_requests_total"),
              std::string::npos);
    EXPECT_NE(text.find("master_batch_get_write_route_requests_total"),
              std::string::npos);
    EXPECT_NE(text.find("master_batch_get_write_route_items_total"),
              std::string::npos);
    EXPECT_NE(text.find("master_batch_get_write_route_failed_items_total"),
              std::string::npos);

    EXPECT_EQ(text.find("master_put_start_requests_total"), std::string::npos);
    EXPECT_EQ(text.find("master_attempted_evictions_total"), std::string::npos);
    EXPECT_EQ(text.find("master_copy_start_requests_total"), std::string::npos);
    EXPECT_EQ(text.find("master_batch_put_start_requests_total"),
              std::string::npos);
    EXPECT_EQ(text.find("master_batch_replica_clear_requests_total"),
              std::string::npos);
}

TEST_F(P2PMasterMetricsTest, SummaryArchTagTest) {
    std::string summary = MasterMetricManager::instance().get_summary_string();
    EXPECT_EQ(summary.find("[Arch: P2P] "), 0u);
    EXPECT_NE(summary.find("GetWriteRoute="), std::string::npos);
    EXPECT_NE(summary.find("AddReplica="), std::string::npos);
    EXPECT_NE(summary.find("RemoveReplica="), std::string::npos);
    EXPECT_NE(summary.find("GetWriteRoute:(Req="), std::string::npos);
    EXPECT_NE(summary.find("RemoveReplica:(Req="), std::string::npos);
}

}  // namespace mooncake::test
