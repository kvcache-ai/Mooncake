#include <gtest/gtest.h>
#include <stdexcept>
#include <fstream>
#include <filesystem>
#include <unordered_map>

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
    EXPECT_EQ(config.te_async_poll_worker_num, 32u);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::TE);
    EXPECT_EQ(config.p2p_key_lease_duration_ms,
              P2PClientConfig::kP2pDefaultKeyLeaseDurationMs);
    EXPECT_EQ(config.p2p_key_lease_scan_interval_ms,
              P2PClientConfig::kP2pDefaultKeyLeaseScanIntervalMs);
    EXPECT_EQ(config.transfer_direction_mode, TransferDirectionMode::REVERSE);
    EXPECT_EQ(config.redis_master_view_ttl_sec, 4);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 1);
}

// The rollback switch. It lives in the same JSON document as `tiers` so an
// operator flips one file and restarts, with no client wiring to change; and
// it defaults to v1, so an existing deployment keeps the implementation it has
// been running until someone asks for the other one.
TEST(ClientConfigBuilderTest, DataManagerVersionDefaultsToV1) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson);
    EXPECT_EQ(config.data_manager_version, "v1");
}

TEST(ClientConfigBuilderTest, DataManagerVersionIsReadFromTheTierConfig) {
    static constexpr const char* kV2Config = R"({
  "data_manager_version": "v2",
  "tiers": [
    {
      "type": "DRAM",
      "capacity": 1048576,
      "priority": 10
    }
  ]
})";
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kV2Config);
    EXPECT_EQ(config.data_manager_version, "v2");
    // The tier list is unaffected by the selector sitting next to it.
    EXPECT_EQ(config.tiered_backend_config["tiers"].size(), 1u);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigUsesRedisDiscoveryDefaults) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "redis://127.0.0.1:6379"},
        {"tiered_backend_config", kTieredConfigJson},
    };

    auto config = ClientConfigBuilder::build_p2p_real_client(raw_config);

    EXPECT_EQ(config.redis_master_view_ttl_sec, 4);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 1);
}

TEST(ClientConfigBuilderTest,
     BuildCentralizedClientConfigUsesRedisDiscoveryDefaults) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "redis://127.0.0.1:6379"},
    };

    auto config =
        ClientConfigBuilder::build_centralized_real_client(raw_config);

    EXPECT_EQ(config.redis_master_view_ttl_sec, 4);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 1);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigKeyLeaseOverrides) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 8, 2048, 512 * 1024 * 1024, 120000, "te", 32, 9003, true, {}, 0,
        2000, 0, 3333, 444);

    EXPECT_EQ(config.p2p_key_lease_duration_ms, 3333u);
    EXPECT_EQ(config.p2p_key_lease_scan_interval_ms, 444u);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigReadsRedisDiscoveryConfig) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "redis://127.0.0.1:6379"},
        {"tiered_backend_config", kTieredConfigJson},
        {"redis_cluster_id", "test-cluster"},
        {"redis_password", "test-password"},
        {"redis_db_index", "3"},
        {"redis_master_view_ttl_sec", "9"},
        {"redis_heartbeat_interval_sec", "4"},
    };

    auto config = ClientConfigBuilder::build_p2p_real_client(raw_config);

    EXPECT_EQ(config.redis_cluster_id, "test-cluster");
    EXPECT_EQ(config.redis_password, "test-password");
    EXPECT_EQ(config.redis_db_index, 3);
    EXPECT_EQ(config.redis_master_view_ttl_sec, 9);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 4);
}

TEST(ClientConfigBuilderTest,
     BuildCentralizedClientConfigReadsRedisDiscoveryConfig) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "redis://127.0.0.1:6379"},
        {"redis_cluster_id", "test-cluster"},
        {"redis_password", "test-password"},
        {"redis_db_index", "3"},
        {"redis_master_view_ttl_sec", "9"},
        {"redis_heartbeat_interval_sec", "4"},
    };

    auto config =
        ClientConfigBuilder::build_centralized_real_client(raw_config);

    EXPECT_EQ(config.redis_cluster_id, "test-cluster");
    EXPECT_EQ(config.redis_password, "test-password");
    EXPECT_EQ(config.redis_db_index, 3);
    EXPECT_EQ(config.redis_master_view_ttl_sec, 9);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 4);
}

TEST(ClientConfigBuilderTest,
     BuildP2PClientConfigAcceptsCustomAsyncCopyConfig) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 8, 2048, 512 * 1024 * 1024, 120000, "memcpy", 3);

    EXPECT_EQ(config.local_memcpy_async_worker_num, 3u);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::MEMCPY);
    EXPECT_EQ(config.te_async_poll_worker_num, 32u);
}

TEST(ClientConfigBuilderTest, BuildP2PTeModePassesTeAsyncPollWorkerArg) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 2, 1024, 100 * 1024 * 1024, 60 * 1000, "te", 5, 9003, true, {},
        0, 2000, 0, 0, 0, "reverse", "", true, 60, DEFAULT_CLUSTER_ID, "", 0, 5,
        2, "", 0, 18);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::TE);
    EXPECT_EQ(config.te_async_poll_worker_num, 18u);
    EXPECT_EQ(config.local_memcpy_async_worker_num, 32u);
}

TEST(ClientConfigBuilderTest, BuildP2PMemcpyModePassesTeAsyncPollWorkerArg) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 2, 1024, 100 * 1024 * 1024, 60 * 1000, "memcpy", 5, 9003, true,
        {}, 0, 2000, 0, 0, 0, "reverse", "", true, 60, DEFAULT_CLUSTER_ID, "",
        0, 5, 2, "", 0, 99);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::MEMCPY);
    EXPECT_EQ(config.local_memcpy_async_worker_num, 5u);
    EXPECT_EQ(config.te_async_poll_worker_num, 99u);
}

TEST(ClientConfigBuilderTest, BuildP2PDictConfigPassesTeAsyncPollWorkerNum) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "127.0.0.1:50051"},
        {"tiered_backend_config", kTieredConfigJson},
        {"local_transfer_mode", "te"},
        {"te_async_poll_worker_num", "7"},
    };
    auto config = ClientConfigBuilder::build_p2p_real_client(raw_config);
    EXPECT_EQ(config.te_async_poll_worker_num, 7u);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigParsesTransferDirectionMode) {
    auto forward = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32, 9003, true,
        {}, 0, 2000, 0, 0, 0, "forward");
    EXPECT_EQ(forward.transfer_direction_mode, TransferDirectionMode::FORWARD);
}

TEST(ClientConfigBuilderTest,
     BuildP2PClientConfigRejectsInvalidTransferDirectionMode) {
    EXPECT_THROW(
        ClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
            12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32, 9003,
            true, {}, 0, 2000, 0, 0, 0, "invalid"),
        std::runtime_error);
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
