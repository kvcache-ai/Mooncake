#include <gtest/gtest.h>
#include <stdexcept>
#include <fstream>
#include <filesystem>

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

    EXPECT_FALSE(config.tiered_backend_config.isNull());
    EXPECT_TRUE(config.tiered_backend_config.isMember("tiers"));
    EXPECT_EQ(config.tiered_backend_config["tiers"].size(), 1u);
    EXPECT_EQ(config.local_memcpy_async_worker_num, 32u);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::TE);
    EXPECT_EQ(config.p2p_key_lease_duration_ms,
              P2PClientConfig::kP2pDefaultKeyLeaseDurationMs);
    EXPECT_EQ(config.p2p_key_lease_scan_interval_ms,
              P2PClientConfig::kP2pDefaultKeyLeaseScanIntervalMs);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigKeyLeaseOverrides) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 8, 2048, 512 * 1024 * 1024, 120000, "te", 32, 9003, true, {},
        0, 2000, 0, 3333, 444);

    EXPECT_EQ(config.p2p_key_lease_duration_ms, 3333u);
    EXPECT_EQ(config.p2p_key_lease_scan_interval_ms, 444u);
}

TEST(ClientConfigBuilderTest,
     BuildP2PClientConfigAcceptsCustomAsyncCopyConfig) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 8, 2048, 512 * 1024 * 1024, 120000, "memcpy", 3);

    EXPECT_EQ(config.local_memcpy_async_worker_num, 3u);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::MEMCPY);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigRejectsInvalidTransferMode) {
    EXPECT_THROW(
        ClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
            12345, 8, 2048, 512 * 1024 * 1024, 120000, "invalid_mode"),
        std::runtime_error);
}

// ---- LoadTieredConfig: file path ----

TEST(ClientConfigBuilderTest, LoadFromFilePath) {
    const std::string tmp_path = "/tmp/mc_test_tiered_cfg.json";
    {
        std::ofstream f(tmp_path);
        f << kTieredConfigJson;
    }

    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", tmp_path);

    EXPECT_FALSE(config.tiered_backend_config.isNull());
    EXPECT_TRUE(config.tiered_backend_config.isMember("tiers"));
    EXPECT_EQ(config.tiered_backend_config["tiers"].size(), 1u);

    std::filesystem::remove(tmp_path);
}

// ---- LoadTieredConfig: invalid file path → throws ----

TEST(ClientConfigBuilderTest, InvalidFilePathThrows) {
    EXPECT_THROW(
        ClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", "/nonexistent/path/tiered.json"),
        std::runtime_error);
}

// ---- LoadTieredConfig: malformed JSON string → throws ----

TEST(ClientConfigBuilderTest, MalformedJsonStringThrows) {
    // Starts with '{' so LoadTieredConfig treats it as inline JSON, but it is
    // syntactically invalid and will fail to parse.
    std::string bad_json = "{ not valid json";
    EXPECT_THROW(ClientConfigBuilder::build_p2p_real_client(
                     "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
                     std::nullopt, "127.0.0.1:50051", bad_json),
                 std::runtime_error);
}

// ---- LoadTieredConfig: empty string → throws (tries to open file named "")
// ----

TEST(ClientConfigBuilderTest, EmptyStringThrows) {
    EXPECT_THROW(ClientConfigBuilder::build_p2p_real_client(
                     "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
                     std::nullopt, "127.0.0.1:50051", ""),
                 std::runtime_error);
}

}  // namespace
}  // namespace mooncake
