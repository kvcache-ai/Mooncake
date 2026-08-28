// Copyright 2026 KVCache.AI
#include <limits>

#include <gtest/gtest.h>

#include "tent/common/config.h"
#include "tent/runtime/hp_tcp_transport_config.h"

namespace mooncake::tent {
namespace {

TEST(HpTcpTransportConfigTest, DefaultsToDisabled) {
    Config config;
    HpTcpTransportConfig parsed;
    ASSERT_TRUE(ParseHpTcpTransportConfig(config, &parsed).ok());
    EXPECT_FALSE(parsed.enabled);
}

TEST(HpTcpTransportConfigTest, RejectsWrongLeafTypes) {
    Config config;
    ASSERT_TRUE(
        config.load(R"({"transports":{"hp_tcp":{"enable":"yes"}}})").ok());
    HpTcpTransportConfig parsed;
    EXPECT_TRUE(ParseHpTcpTransportConfig(config, &parsed).IsInvalidArgument());

    ASSERT_TRUE(
        config.load(R"({"transports":{"hp_tcp":{"worker_count":"16"}}})").ok());
    EXPECT_TRUE(ParseHpTcpTransportConfig(config, &parsed).IsInvalidArgument());
}

TEST(HpTcpTransportConfigTest, ParsesAndValidatesLimits) {
    Config config;
    ASSERT_TRUE(
        config
            .load(
                R"({"transports":{"tcp":{"enable":false},"hp_tcp":{"enable":true,"port":0,"worker_count":4,"connections_per_peer":2,"max_outstanding_tasks":16,"max_outstanding_bytes":4096,"max_transfer_bytes":1024,"chunk_size":512,"connect_timeout_ms":1,"progress_timeout_ms":2}}})")
            .ok());
    HpTcpTransportConfig parsed;
    ASSERT_TRUE(ParseHpTcpTransportConfig(config, &parsed).ok());
    EXPECT_TRUE(parsed.enabled);
    EXPECT_EQ(parsed.params.worker_count, 4U);
    EXPECT_EQ(parsed.params.chunk_size, 512U);

    ASSERT_TRUE(
        config
            .load(
                R"({"transports":{"tcp":{"enable":false},"hp_tcp":{"enable":true,"chunk_size":2,"max_transfer_bytes":1}}})")
            .ok());
    EXPECT_TRUE(ParseHpTcpTransportConfig(config, &parsed).IsInvalidArgument());
}

TEST(HpTcpTransportConfigTest, AcceptsFullWidthUnsignedLimit) {
    Config config;
    ASSERT_TRUE(
        config
            .load(
                R"({"transports":{"tcp":{"enable":false},"hp_tcp":{"enable":true,"max_outstanding_bytes":18446744073709551615}}})")
            .ok());
    HpTcpTransportConfig parsed;
    ASSERT_TRUE(ParseHpTcpTransportConfig(config, &parsed).ok());
    EXPECT_EQ(parsed.params.max_outstanding_bytes,
              std::numeric_limits<uint64_t>::max());
}

TEST(HpTcpTransportConfigTest, DisabledHpTcpIgnoresInactiveLimits) {
    Config config;
    ASSERT_TRUE(
        config
            .load(
                R"({"transports":{"hp_tcp":{"enable":false,"worker_count":0}}})")
            .ok());
    HpTcpTransportConfig parsed;
    ASSERT_TRUE(ParseHpTcpTransportConfig(config, &parsed).ok());
    EXPECT_FALSE(parsed.enabled);
}

TEST(HpTcpTransportConfigTest, AllowsTcpAndHpTcpTogether) {
    Config config;
    ASSERT_TRUE(
        config
            .load(
                R"({"transports":{"tcp":{"enable":true},"hp_tcp":{"enable":true}}})")
            .ok());
    HpTcpTransportConfig parsed;
    ASSERT_TRUE(ParseHpTcpTransportConfig(config, &parsed).ok());
    EXPECT_TRUE(parsed.enabled);
}

}  // namespace
}  // namespace mooncake::tent
