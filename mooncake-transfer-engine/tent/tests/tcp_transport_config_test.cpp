// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>

#include "tent/common/config.h"
#include "tent/runtime/tcp_transport_config.h"

namespace mooncake::tent {
namespace {

TEST(TcpTransportConfigTest, DefaultsToEnabledStandard) {
    Config config;
    TcpTransportConfig parsed;
    ASSERT_TRUE(ParseTcpTransportConfig(config, &parsed).ok());
    EXPECT_TRUE(parsed.enabled);
    EXPECT_EQ(parsed.implementation, TcpImplementation::kStandard);
    EXPECT_FALSE(parsed.hpRequired());
}

TEST(TcpTransportConfigTest, RejectsWrongLeafTypes) {
    Config config;
    ASSERT_TRUE(
        config.load(R"({"transports":{"tcp":{"implementation":7}}})").ok());
    TcpTransportConfig parsed;
    EXPECT_TRUE(ParseTcpTransportConfig(config, &parsed).IsInvalidArgument());

    ASSERT_TRUE(
        config
            .load(
                R"({"transports":{"tcp":{"high_performance":{"worker_count":"16"}}}})")
            .ok());
    EXPECT_TRUE(ParseTcpTransportConfig(config, &parsed).IsInvalidArgument());
}

TEST(TcpTransportConfigTest, ParsesHighPerformanceAndValidatesLimits) {
    Config config;
    ASSERT_TRUE(
        config
            .load(
                R"({"transports":{"tcp":{"implementation":"high_performance","high_performance":{"port":0,"worker_count":4,"queue_capacity_per_worker":8,"connections_per_peer":2,"max_outstanding_tasks":16,"max_outstanding_bytes":4096,"max_transfer_bytes":1024,"chunk_size":512,"connect_timeout_ms":1,"progress_timeout_ms":2}}}})")
            .ok());
    TcpTransportConfig parsed;
    ASSERT_TRUE(ParseTcpTransportConfig(config, &parsed).ok());
    EXPECT_TRUE(parsed.hpRequired());
    EXPECT_EQ(parsed.high_performance.worker_count, 4U);
    EXPECT_EQ(parsed.high_performance.chunk_size, 512U);

    ASSERT_TRUE(
        config
            .load(
                R"({"transports":{"tcp":{"implementation":"high_performance","high_performance":{"chunk_size":2,"max_transfer_bytes":1}}}})")
            .ok());
    EXPECT_TRUE(ParseTcpTransportConfig(config, &parsed).IsInvalidArgument());
}

TEST(TcpTransportConfigTest, DisabledTcpDoesNotEnableHighPerformanceGate) {
    Config config;
    ASSERT_TRUE(config.load(R"({"transports":{"tcp":{"enable":false}}})").ok());
    TcpTransportConfig parsed;
    ASSERT_TRUE(ParseTcpTransportConfig(config, &parsed).ok());
    EXPECT_FALSE(parsed.enabled);
    EXPECT_FALSE(parsed.hpRequired());
}

TEST(TcpTransportConfigTest, DisabledTcpIgnoresHighPerformanceImplementation) {
    Config config;
    ASSERT_TRUE(
        config
            .load(
                R"({"transports":{"tcp":{"enable":false,"implementation":"high_performance","high_performance":{"worker_count":0}}}})")
            .ok());
    TcpTransportConfig parsed;
    ASSERT_TRUE(ParseTcpTransportConfig(config, &parsed).ok());
    EXPECT_FALSE(parsed.enabled);
    EXPECT_EQ(parsed.implementation, TcpImplementation::kHighPerformance);
    EXPECT_FALSE(parsed.hpRequired());
}

}  // namespace
}  // namespace mooncake::tent
