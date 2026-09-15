#include "master_heartbeat_metric.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

namespace mooncake::test {
namespace {

std::string Serialize(MasterHeartbeatMetric& metric) {
    std::string result;
    metric.serialize(result);
    return result;
}

void Connect(MasterHeartbeatMetric& metric) {
    metric.EndConnection(metric.BeginConnection(), true);
}

TEST(MasterHeartbeatMetricTest, InitialObservationIsUnknown) {
    MasterHeartbeatMetric metric;
    EXPECT_FALSE(metric.BeginObservation());
    EXPECT_TRUE(Serialize(metric).empty());
    Connect(metric);
    EXPECT_TRUE(Serialize(metric).empty());
}

TEST(MasterHeartbeatMetricTest, StatusAndReceiveTimeFollowSuccessfulPing) {
    MasterHeartbeatMetric metric;
    Connect(metric);
    const auto generation = metric.BeginObservation();
    metric.Observe(generation, false, 100.25);
    const auto remount = Serialize(metric);
    EXPECT_NE(remount.find("mooncake_client_master_heartbeat_status_ok 0\n"),
              std::string::npos);
    EXPECT_NE(remount.find("timestamp_seconds 100.250000\n"),
              std::string::npos);
    EXPECT_NE(remount.find(
                  "# TYPE mooncake_client_master_heartbeat_status_ok gauge\n"),
              std::string::npos);
    metric.Observe(generation, true, 200.5);
    const auto recovered = Serialize(metric);
    EXPECT_NE(recovered.find("mooncake_client_master_heartbeat_status_ok 1\n"),
              std::string::npos);
    EXPECT_NE(recovered.find("timestamp_seconds 200.500000\n"),
              std::string::npos);
    EXPECT_EQ(recovered.find("100.250000"), std::string::npos);
}

TEST(MasterHeartbeatMetricTest, FailedOrUnsupportedObservationIsUnknown) {
    MasterHeartbeatMetric metric;
    Connect(metric);
    const auto generation = metric.BeginObservation();
    metric.Observe(generation, true, 100);
    metric.Observe(generation, std::nullopt, 0);
    EXPECT_TRUE(Serialize(metric).empty());
    metric.Observe(generation, std::nullopt, 0);
    EXPECT_TRUE(Serialize(metric).empty());
    metric.Observe(generation, false, 200);
    EXPECT_NE(Serialize(metric).find("heartbeat_status_ok 0\n"),
              std::string::npos);
}

TEST(MasterHeartbeatMetricTest, PreservesAndEscapesLabels) {
    MasterHeartbeatMetric metric(
        {{"cluster_id", "cluster1"}, {"instance_id", "node\"a\\b\nc"}});
    Connect(metric);
    metric.Observe(metric.BeginObservation(), true, 100);
    const auto text = Serialize(metric);
    const std::string labels =
        "{cluster_id=\"cluster1\",instance_id=\"node\\\"a\\\\b\\nc\"}";
    EXPECT_NE(text.find("heartbeat_status_ok" + labels + " 1\n"),
              std::string::npos);
    EXPECT_NE(text.find("timestamp_seconds" + labels + " 100.000000\n"),
              std::string::npos);
}

TEST(MasterHeartbeatMetricTest, ReconnectRejectsLateSuccessAndFailure) {
    MasterHeartbeatMetric metric;
    Connect(metric);
    const auto old = metric.BeginObservation();
    metric.Observe(old, true, 100);
    const auto connection = metric.BeginConnection();
    EXPECT_TRUE(Serialize(metric).empty());
    EXPECT_FALSE(metric.BeginObservation());
    metric.Observe(old, true, 200);
    EXPECT_TRUE(Serialize(metric).empty());
    metric.EndConnection(connection, true);
    metric.Observe(old, true, 300);
    EXPECT_TRUE(Serialize(metric).empty());
    metric.Observe(metric.BeginObservation(), false, 400);
    const auto current = Serialize(metric);
    metric.Observe(old, std::nullopt, 0);
    EXPECT_EQ(Serialize(metric), current);
    metric.Observe(old, true, 500);
    EXPECT_EQ(Serialize(metric), current);
}

TEST(MasterHeartbeatMetricTest, PingStartedDuringConnectCannotPublish) {
    MasterHeartbeatMetric metric;
    const auto connection = metric.BeginConnection();
    const auto observation = metric.BeginObservation();
    metric.EndConnection(connection, true);
    metric.Observe(observation, true, 100);
    EXPECT_TRUE(Serialize(metric).empty());
}

TEST(MasterHeartbeatMetricTest, FailedConnectRemainsUnknown) {
    MasterHeartbeatMetric metric;
    Connect(metric);
    metric.Observe(metric.BeginObservation(), true, 100);
    const auto connection = metric.BeginConnection();
    metric.EndConnection(connection, false);
    EXPECT_FALSE(metric.BeginObservation());
    EXPECT_TRUE(Serialize(metric).empty());
    Connect(metric);
    metric.Observe(metric.BeginObservation(), true, 200);
    EXPECT_NE(Serialize(metric).find("timestamp_seconds 200.000000\n"),
              std::string::npos);
}

TEST(MasterHeartbeatMetricTest, OldConnectionCompletionCannotEnableNewOne) {
    MasterHeartbeatMetric metric;
    const auto old = metric.BeginConnection();
    const auto current = metric.BeginConnection();
    metric.EndConnection(old, true);
    EXPECT_FALSE(metric.BeginObservation());
    metric.EndConnection(current, true);
    EXPECT_EQ(metric.BeginObservation(), current);
}

TEST(MasterHeartbeatMetricTest, ConcurrentScrapesKeepObservationTogether) {
    MasterHeartbeatMetric metric;
    Connect(metric);
    const auto generation = metric.BeginObservation();
    metric.Observe(generation, false, 100);
    const auto first = Serialize(metric);
    metric.Observe(generation, true, 200);
    const auto second = Serialize(metric);
    std::atomic<bool> start{false};
    std::thread writer([&] {
        while (!start.load()) std::this_thread::yield();
        for (int i = 0; i < 10000; ++i) {
            metric.Observe(generation, false, 100);
            metric.Observe(generation, std::nullopt, 0);
            metric.Observe(generation, true, 200);
        }
    });
    start.store(true);
    for (int i = 0; i < 10000; ++i) {
        const auto text = Serialize(metric);
        EXPECT_TRUE(text.empty() || text == first || text == second);
    }
    writer.join();
}

}  // namespace
}  // namespace mooncake::test
