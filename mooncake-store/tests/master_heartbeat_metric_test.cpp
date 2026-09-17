#include "master_heartbeat_metric.h"
#include "rpc_types.h"
#include <ylt/util/tl/expected.hpp>

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

tl::expected<PingResponse, ErrorCode> Ping(ClientStatus status) {
    return PingResponse{42, status};
}

TEST(MasterHeartbeatMetricTest,
     ObserveConnectPreservesResultAndInvalidatesSample) {
    MasterHeartbeatMetric metric;
    int calls = 0;
    EXPECT_EQ(metric.ObserveConnect([&] {
        ++calls;
        EXPECT_TRUE(Serialize(metric).empty());
        EXPECT_FALSE(metric.BeginObservation());
        return ErrorCode::OK;
    }),
              ErrorCode::OK);
    EXPECT_EQ(calls, 1);
    metric.ObservePing([] { return Ping(ClientStatus::OK); });
    EXPECT_FALSE(Serialize(metric).empty());
    EXPECT_EQ(metric.ObserveConnect([&] {
        ++calls;
        EXPECT_TRUE(Serialize(metric).empty());
        return ErrorCode::RPC_FAIL;
    }),
              ErrorCode::RPC_FAIL);
    EXPECT_EQ(calls, 2);
    EXPECT_FALSE(metric.BeginObservation());
    EXPECT_TRUE(Serialize(metric).empty());
}

TEST(MasterHeartbeatMetricTest, ObservePingMapsStatusAndPreservesResponse) {
    MasterHeartbeatMetric metric;
    metric.ObserveConnect([] { return ErrorCode::OK; });
    for (auto status : {ClientStatus::OK, ClientStatus::NEED_REMOUNT}) {
        const auto before = std::chrono::system_clock::now();
        int calls = 0;
        auto result = metric.ObservePing([&] {
            ++calls;
            // Reentering serialization would deadlock if the wrapper held
            // the metric lock while invoking the RPC callback.
            Serialize(metric);
            return Ping(status);
        });
        const auto after = std::chrono::system_clock::now();
        ASSERT_TRUE(result);
        EXPECT_EQ(calls, 1);
        EXPECT_EQ(result->view_version_id, 42);
        EXPECT_EQ(result->client_status, status);
        const auto text = Serialize(metric);
        EXPECT_NE(
            text.find(status == ClientStatus::OK ? "heartbeat_status_ok 1\n"
                                                 : "heartbeat_status_ok 0\n"),
            std::string::npos);
        const std::string name =
            "\nmooncake_client_master_heartbeat_observation_timestamp_seconds ";
        const auto offset = text.find(name);
        ASSERT_NE(offset, std::string::npos);
        const auto timestamp = std::stod(text.substr(offset + name.size()));
        // The gauge exposition rounds to six decimal places.
        EXPECT_GE(
            timestamp,
            std::chrono::duration<double>(before.time_since_epoch()).count() -
                1e-6);
        EXPECT_LE(
            timestamp,
            std::chrono::duration<double>(after.time_since_epoch()).count() +
                1e-6);
    }
}

TEST(MasterHeartbeatMetricTest,
     ObservePingClearsFailedAndUnsupportedResponses) {
    MasterHeartbeatMetric metric;
    metric.ObserveConnect([] { return ErrorCode::OK; });
    metric.ObservePing([] { return Ping(ClientStatus::OK); });
    auto unsupported =
        metric.ObservePing([] { return Ping(ClientStatus::UNDEFINED); });
    ASSERT_TRUE(unsupported);
    EXPECT_EQ(unsupported->client_status, ClientStatus::UNDEFINED);
    EXPECT_TRUE(Serialize(metric).empty());
    metric.ObservePing([] { return Ping(ClientStatus::OK); });
    auto failed =
        metric.ObservePing([]() -> tl::expected<PingResponse, ErrorCode> {
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        });
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::RPC_FAIL);
    EXPECT_TRUE(Serialize(metric).empty());
}

TEST(MasterHeartbeatMetricTest, ObservePingRejectsLateSuccessAndFailure) {
    MasterHeartbeatMetric metric;
    metric.ObserveConnect([] { return ErrorCode::OK; });
    for (bool success : {true, false}) {
        std::string current;
        auto result =
            metric.ObservePing([&]() -> tl::expected<PingResponse, ErrorCode> {
                metric.ObserveConnect([] { return ErrorCode::OK; });
                metric.ObservePing(
                    [] { return Ping(ClientStatus::NEED_REMOUNT); });
                current = Serialize(metric);
                if (success) return Ping(ClientStatus::OK);
                return tl::make_unexpected(ErrorCode::RPC_FAIL);
            });
        EXPECT_EQ(result.has_value(), success);
        EXPECT_FALSE(current.empty());
        EXPECT_EQ(Serialize(metric), current);
    }
}

TEST(MasterHeartbeatMetricTest, ObservePingDuringConnectCannotPublish) {
    MasterHeartbeatMetric metric;
    metric.ObserveConnect([&] {
        auto result = metric.ObservePing([] { return Ping(ClientStatus::OK); });
        EXPECT_TRUE(result);
        EXPECT_TRUE(Serialize(metric).empty());
        return ErrorCode::OK;
    });
    EXPECT_TRUE(metric.BeginObservation());
    EXPECT_TRUE(Serialize(metric).empty());
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
