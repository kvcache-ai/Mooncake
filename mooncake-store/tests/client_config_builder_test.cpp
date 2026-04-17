#include <gtest/gtest.h>
#include <stdexcept>

#include "client_config_builder.h"

namespace mooncake {
namespace {

const char* kTieredConfigJson = R"({
  "tiers": [
    {
      "type": "DRAM",
      "capacity": 1048576,
      "priority": 10
    }
  ]
})";

TEST(ClientConfigBuilderTest, BuildP2PClientConfigUsesDefaults) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson);

    // TE mode: async copy params use struct defaults (not builder defaults)
    EXPECT_EQ(config.local_memcpy_async_worker_num, 32u);
    EXPECT_EQ(config.local_memcpy_async_queue_depth, 2048u);
    EXPECT_EQ(config.local_transfer_mode,
              LocalTransferMode::TE);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigAcceptsCustomAsyncCopyConfig) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "", 12345,
        8, 2048, 512 * 1024 * 1024, 120000, "memcpy", 3, 256);

    EXPECT_EQ(config.local_memcpy_async_worker_num, 3u);
    EXPECT_EQ(config.local_memcpy_async_queue_depth, 256u);
    EXPECT_EQ(config.local_transfer_mode,
              LocalTransferMode::MEMCPY);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigRejectsInvalidTransferMode) {
    EXPECT_THROW(
        ClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr,
            "", 12345, 8, 2048, 512 * 1024 * 1024, 120000,
            "invalid_mode"),
        std::runtime_error);
}

}  // namespace
}  // namespace mooncake
