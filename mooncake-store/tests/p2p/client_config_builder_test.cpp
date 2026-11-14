#include <gtest/gtest.h>
#include <cstdlib>
#include <optional>
#include <fstream>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <utility>

#include "p2p/client/p2p_client_config_builder.h"

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

class RuntimeConfigBuilderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        const char* value = std::getenv("MC_RUNTIME_CONFIG");
        if (value != nullptr) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv("MC_RUNTIME_CONFIG"), 0);
    }

    void TearDown() override {
        if (original_.has_value()) {
            EXPECT_EQ(setenv("MC_RUNTIME_CONFIG", original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv("MC_RUNTIME_CONFIG"), 0);
        }
    }

    tl::expected<P2PClientConfig, ErrorCode> Build(
        const std::string& runtime_config = "") {
        return P2PClientConfigBuilder::build_p2p_real_client(
            {{"local_hostname", "127.0.0.1:12345"},
             {"metadata_server", "P2PHANDSHAKE"},
             {"tiered_backend_config", kTieredConfigJson},
             {"runtime_config", runtime_config}});
    }

   private:
    std::optional<std::string> original_;
};

TEST_F(RuntimeConfigBuilderTest, RuntimeEnvironmentBelongsToP2P) {
    ASSERT_EQ(
        setenv("MC_RUNTIME_CONFIG", R"({"write":{"remote_weight":0.8}})", 1),
        0);

    auto p2p = P2PClientConfigBuilder::build_p2p_real_client(
                   "127.0.0.1:12345", "P2PHANDSHAKE", "tcp", std::nullopt,
                   "127.0.0.1:50051", kTieredConfigJson)
                   .value();
    EXPECT_DOUBLE_EQ(
        p2p.runtime_config_json["write"]["remote_weight"].asDouble(), 0.8);

    ASSERT_EQ(setenv("MC_RUNTIME_CONFIG", "{invalid json", 1), 0);
    auto error_result = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "P2PHANDSHAKE", "tcp", std::nullopt,
        "127.0.0.1:50051", kTieredConfigJson);
    ASSERT_FALSE(error_result);
    EXPECT_EQ(error_result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(RuntimeConfigBuilderTest, UnsetAndEmptyEnvironmentKeepDefaults) {
    EXPECT_TRUE(Build().value().runtime_config_json.isNull());
    ASSERT_EQ(setenv("MC_RUNTIME_CONFIG", "", 1), 0);
    EXPECT_TRUE(Build().value().runtime_config_json.isNull());
}

TEST_F(RuntimeConfigBuilderTest, ExplicitRuntimeOverridesMalformedEnvironment) {
    ASSERT_EQ(setenv("MC_RUNTIME_CONFIG", "{invalid json", 1), 0);
    auto config = Build(R"({"write":{"remote_weight":0.2}})").value();
    EXPECT_DOUBLE_EQ(
        config.runtime_config_json["write"]["remote_weight"].asDouble(), 0.2);

    auto empty_object = Build("{}").value();
    EXPECT_TRUE(empty_object.runtime_config_json.isObject());
    EXPECT_TRUE(empty_object.runtime_config_json.empty());
}

TEST_F(RuntimeConfigBuilderTest, ReadsCurrentEnvironmentOnEachBuild) {
    ASSERT_EQ(
        setenv("MC_RUNTIME_CONFIG", R"({"write":{"remote_weight":0.4}})", 1),
        0);
    auto first = Build().value();
    ASSERT_EQ(
        setenv("MC_RUNTIME_CONFIG", R"({"write":{"remote_weight":0.8}})", 1),
        0);
    auto second = Build().value();
    EXPECT_DOUBLE_EQ(
        first.runtime_config_json["write"]["remote_weight"].asDouble(), 0.4);
    EXPECT_DOUBLE_EQ(
        second.runtime_config_json["write"]["remote_weight"].asDouble(), 0.8);
}

TEST_F(RuntimeConfigBuilderTest, InvalidExplicitRuntimeDoesNotFallBackToEnv) {
    ASSERT_EQ(setenv("MC_RUNTIME_CONFIG", "{}", 1), 0);
    auto error_result = Build("{invalid json");
    ASSERT_FALSE(error_result);
    EXPECT_EQ(error_result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(RuntimeConfigBuilderTest, DictConnectsHttpLeaseAndDirectionControls) {
    std::unordered_map<std::string, std::string> values = {
        {"local_hostname", "127.0.0.1"},
        {"metadata_server", "P2PHANDSHAKE"},
        {"tiered_backend_config", kTieredConfigJson}};
    auto defaults =
        P2PClientConfigBuilder::build_p2p_real_client(values).value();
    EXPECT_EQ(defaults.http_port, 9003);
    EXPECT_EQ(defaults.rpc_thread_num, 2u);
    EXPECT_EQ(defaults.p2p_key_lease_duration_ms, 5000u);
    EXPECT_EQ(defaults.p2p_key_lease_scan_interval_ms, 1000u);
    EXPECT_EQ(defaults.transfer_direction_mode, TransferDirectionMode::REVERSE);

    values.insert({{"http_port", "9123"},
                   {"p2p_key_lease_duration_ms", "7000"},
                   {"p2p_key_lease_scan_interval_ms", "2000"},
                   {"p2p_transfer_direction_mode", "forward"}});
    auto config = P2PClientConfigBuilder::build_p2p_real_client(values).value();
    EXPECT_EQ(config.http_port, 9123);
    EXPECT_EQ(config.p2p_key_lease_duration_ms, 7000u);
    EXPECT_EQ(config.p2p_key_lease_scan_interval_ms, 2000u);
    EXPECT_EQ(config.transfer_direction_mode, TransferDirectionMode::FORWARD);

    values["http_port"] = "0";
    values["p2p_key_lease_duration_ms"] = "0";
    values["p2p_key_lease_scan_interval_ms"] = "0";
    config = P2PClientConfigBuilder::build_p2p_real_client(values).value();
    EXPECT_EQ(config.http_port, 0);
    EXPECT_EQ(config.p2p_key_lease_duration_ms, 5000u);
    EXPECT_EQ(config.p2p_key_lease_scan_interval_ms, 1000u);
    values["p2p_transfer_direction_mode"] = "invalid";
    auto error_result = P2PClientConfigBuilder::build_p2p_real_client(values);
    ASSERT_FALSE(error_result);
    EXPECT_EQ(error_result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(RuntimeConfigBuilderTest, DictSeparatesByteSizesFromIntegers) {
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
        {{"local_hostname", "127.0.0.1"},
         {"metadata_server", "P2PHANDSHAKE"},
         {"tiered_backend_config", kTieredConfigJson},
         {"local_buffer_size", "16 MB"},
         {"route_cache_max_memory_bytes", "1.5 MB"},
         {"route_cache_ttl_ms", "9007199254740993"},
         {"rpc_thread_num", " +16 "},
         {"client_rpc_port", "65535"},
         {"async_route_queue_size", "0"}});
    ASSERT_TRUE(config);
    EXPECT_EQ(config->local_buffer_size, 16ULL * 1024 * 1024);
    EXPECT_EQ(config->route_cache_max_memory_bytes, 1536ULL * 1024);
    EXPECT_EQ(config->route_cache_ttl_ms, 9007199254740993ULL);
    EXPECT_EQ(config->rpc_thread_num, 16u);
    EXPECT_EQ(config->client_rpc_port, 65535u);
    EXPECT_EQ(config->async_route_queue_size, 0u);
}

TEST_F(RuntimeConfigBuilderTest, DictRejectsInvalidNumericInputs) {
    const std::unordered_map<std::string, std::string> base = {
        {"local_hostname", "127.0.0.1"},
        {"metadata_server", "P2PHANDSHAKE"},
        {"tiered_backend_config", kTieredConfigJson}};
    for (const auto* key : {"local_buffer_size",
                            "route_cache_max_memory_bytes",
                            "client_rpc_port",
                            "heartbeat_rpc_port",
                            "http_port",
                            "rpc_thread_num",
                            "lock_shard_count",
                            "local_io_worker_num",
                            "te_async_poll_worker_num",
                            "business_coro_worker_num",
                            "async_sender_thread_count",
                            "async_max_batch_size",
                            "async_route_queue_size",
                            "route_cache_ttl_ms",
                            "metric_report_interval_seconds",
                            "p2p_key_lease_duration_ms",
                            "p2p_key_lease_scan_interval_ms",
                            "redis_db_index",
                            "redis_master_view_ttl_sec",
                            "redis_heartbeat_interval_sec"}) {
        SCOPED_TRACE(key);
        auto values = base;
        values[key] = "invalid";
        auto result = P2PClientConfigBuilder::build_p2p_real_client(values);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
    // Representative checks for parser and destination-type selection.
    const std::pair<const char*, const char*> invalid_values[] = {
        {"client_rpc_port", "65536"},
        {"rpc_thread_num", "1 MB"},
        {"lock_shard_count", "1.5"},
        {"async_sender_thread_count", "-1"},
        {"redis_db_index", "2147483648"}};
    for (const auto& [key, value] : invalid_values) {
        SCOPED_TRACE(std::string(key) + "=" + value);
        auto values = base;
        values[key] = value;
        auto result = P2PClientConfigBuilder::build_p2p_real_client(values);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST_F(RuntimeConfigBuilderTest, DictBooleanControlsKeepDefaultsAndOverrides) {
    struct Control {
        const char* key;
        bool P2PClientConfig::* field;
        bool default_value;
    };
    const Control controls[] = {
        {"enable_http_server", &P2PClientConfig::enable_http_server, true},
        {"enable_metric_collection", &P2PClientConfig::enable_metric_collection,
         true},
        {"start_local_only", &P2PClientConfig::start_local_only, false}};
    for (const auto& control : controls) {
        SCOPED_TRACE(control.key);
        std::unordered_map<std::string, std::string> values = {
            {"local_hostname", "127.0.0.1"},
            {"metadata_server", "P2PHANDSHAKE"},
            {"tiered_backend_config", kTieredConfigJson}};
        auto config = P2PClientConfigBuilder::build_p2p_real_client(values);
        ASSERT_TRUE(config);
        EXPECT_EQ((*config).*control.field, control.default_value);
        const std::pair<const char*, bool> tokens[] = {
            {" True ", true},
            {"false", false},
            {"invalid", control.default_value}};
        for (const auto& [text, expected] : tokens) {
            SCOPED_TRACE(text);
            values[control.key] = text;
            config = P2PClientConfigBuilder::build_p2p_real_client(values);
            ASSERT_TRUE(config);
            EXPECT_EQ((*config).*control.field, expected);
        }
    }
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigUsesDefaults) {
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
                      "127.0.0.1:12345", "http://127.0.0.1:8080/metadata",
                      "tcp", std::nullopt, "127.0.0.1:50051", kTieredConfigJson)
                      .value();

    EXPECT_FALSE(config.start_local_only);
    EXPECT_EQ(config.data_manager_version, "v1");
    EXPECT_FALSE(config.tiered_backend_config.isNull());
    EXPECT_TRUE(config.tiered_backend_config.isMember("tiers"));
    EXPECT_EQ(config.tiered_backend_config["tiers"].size(), 1u);
    EXPECT_EQ(config.local_io_worker_num, 32u);
    EXPECT_EQ(config.te_async_poll_worker_num, 32u);
    EXPECT_EQ(config.business_coro_worker_num, 32u);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::TE);
    EXPECT_EQ(config.p2p_key_lease_duration_ms,
              P2PClientConfig::kP2pDefaultKeyLeaseDurationMs);
    EXPECT_EQ(config.p2p_key_lease_scan_interval_ms,
              P2PClientConfig::kP2pDefaultKeyLeaseScanIntervalMs);
    EXPECT_EQ(config.transfer_direction_mode, TransferDirectionMode::REVERSE);
    EXPECT_EQ(config.redis_master_view_ttl_sec, 4);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 1);
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
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
                      "127.0.0.1:12345", "http://127.0.0.1:8080/metadata",
                      "tcp", std::nullopt, "127.0.0.1:50051", kV2Config)
                      .value();
    EXPECT_EQ(config.data_manager_version, "v2");
    EXPECT_EQ(config.tiered_backend_config["tiers"].size(), 1u);
}

TEST(ClientConfigBuilderTest, PreservesAscendAddressForTransferEngine) {
    const std::pair<const char*, const char*> addresses[] = {
        {"127.0.0.1:12345:npu_2", "127.0.0.1"},
        {"localhost:12345:npu_2", "localhost"},
        {"[::1]:12345:npu_2", "::1"},
    };
    for (const auto& [input, host] : addresses) {
        SCOPED_TRACE(input);
        const auto config = P2PClientConfigBuilder::build_p2p_real_client(
                                {{"local_hostname", input},
                                 {"metadata_server", "P2PHANDSHAKE"},
                                 {"protocol", "ascend"},
                                 {"tiered_backend_config", kTieredConfigJson}})
                                .value();
        EXPECT_EQ(config.local_hostname, input);
        int device = -1;
        const auto [te_host, te_port] =
            parseHostNameWithPortAscend(config.local_hostname, &device);
        EXPECT_EQ(te_host, host);
        EXPECT_EQ(te_port, 12345);
        EXPECT_EQ(device, 2);
    }
}

TEST(ClientConfigBuilderTest, P2PDictPreservesIpcSocketPath) {
    std::unordered_map<std::string, std::string> values = {
        {"local_hostname", "127.0.0.1:0"},
        {"metadata_server", "P2PHANDSHAKE"},
        {"tiered_backend_config", kTieredConfigJson},
    };
    EXPECT_TRUE(P2PClientConfigBuilder::build_p2p_real_client(values)
                    .value()
                    .ipc_socket_path.empty());
    values["ipc_socket_path"] = "custom_mooncake_ipc";
    EXPECT_EQ(P2PClientConfigBuilder::build_p2p_real_client(values)
                  .value()
                  .ipc_socket_path,
              values["ipc_socket_path"]);
    values["ipc_socket_path"] = "";
    EXPECT_TRUE(P2PClientConfigBuilder::build_p2p_real_client(values)
                    .value()
                    .ipc_socket_path.empty());
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigUsesRedisDiscoveryDefaults) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "redis://127.0.0.1:6379"},
        {"tiered_backend_config", kTieredConfigJson},
    };

    auto config =
        P2PClientConfigBuilder::build_p2p_real_client(raw_config).value();

    EXPECT_EQ(config.redis_db_index, 0);
    EXPECT_EQ(config.redis_master_view_ttl_sec, 4);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 1);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigKeyLeaseOverrides) {
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
                      "127.0.0.1:12345", "http://127.0.0.1:8080/metadata",
                      "tcp", std::nullopt, "127.0.0.1:50051", kTieredConfigJson,
                      0, nullptr, "", 12345, 8, 2048, 512 * 1024 * 1024, 120000,
                      "te", 32, 9003, true, {}, 0, 2000, 0, 3333, 444)
                      .value();

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
        {"redis_db_index", " +3 "},
        {"redis_master_view_ttl_sec", "9"},
        {"redis_heartbeat_interval_sec", "0"},
    };

    auto config =
        P2PClientConfigBuilder::build_p2p_real_client(raw_config).value();

    EXPECT_EQ(config.redis_cluster_id, "test-cluster");
    EXPECT_EQ(config.redis_password, "test-password");
    EXPECT_EQ(config.redis_db_index, 3);
    EXPECT_EQ(config.redis_master_view_ttl_sec, 9);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 0);
}

TEST(ClientConfigBuilderTest,
     BuildP2PClientConfigAcceptsCustomAsyncCopyConfig) {
    auto config =
        P2PClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
            12345, 8, 2048, 512 * 1024 * 1024, 120000, "memcpy", 3)
            .value();

    EXPECT_EQ(config.local_io_worker_num, 3u);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::MEMCPY);
    EXPECT_EQ(config.te_async_poll_worker_num, 32u);
}

TEST(ClientConfigBuilderTest, BuildP2PTransferModesPreserveWorkerArguments) {
    struct ModeCase {
        const char* name;
        LocalTransferMode mode;
        size_t poll_workers;
        bool start_local_only;
    };
    const ModeCase cases[] = {{"te", LocalTransferMode::TE, 18, true},
                              {"memcpy", LocalTransferMode::MEMCPY, 99, false}};
    for (const auto& test : cases) {
        SCOPED_TRACE(test.name);
        auto config = P2PClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
            12345, 2, 1024, 100 * 1024 * 1024, 60 * 1000, test.name, 5, 9003,
            true, {}, 0, 2000, 0, 0, 0, "reverse", "", true, 60,
            DEFAULT_CLUSTER_ID, "", 0, 5, 2, "", 0, test.poll_workers,
            test.start_local_only);
        ASSERT_TRUE(config);
        EXPECT_EQ(config->local_transfer_mode, test.mode);
        EXPECT_EQ(config->local_io_worker_num, 5u);
        EXPECT_EQ(config->te_async_poll_worker_num, test.poll_workers);
        EXPECT_EQ(config->start_local_only, test.start_local_only);
    }
}

TEST(ClientConfigBuilderTest, BuildP2PDictConfigPassesTeAsyncPollWorkerNum) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "127.0.0.1:50051"},
        {"tiered_backend_config", kTieredConfigJson},
        {"local_transfer_mode", "te"},
        {"te_async_poll_worker_num", "7"},
        {"business_coro_worker_num", "3"},
        {"local_io_worker_num", "5"},
    };
    auto config =
        P2PClientConfigBuilder::build_p2p_real_client(raw_config).value();
    EXPECT_EQ(config.te_async_poll_worker_num, 7u);
    EXPECT_EQ(config.business_coro_worker_num, 3u);
    EXPECT_EQ(config.local_io_worker_num, 5u);

    raw_config["business_coro_worker_num"] = "0";
    raw_config["local_io_worker_num"] = "0";
    config = P2PClientConfigBuilder::build_p2p_real_client(raw_config).value();
    EXPECT_EQ(config.business_coro_worker_num, 0u);
    EXPECT_EQ(config.local_io_worker_num, 0u);
    EXPECT_EQ(config.te_async_poll_worker_num, 7u);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigParsesTransferDirectionMode) {
    auto forward =
        P2PClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
            12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32, 9003,
            true, {}, 0, 2000, 0, 0, 0, "forward")
            .value();
    EXPECT_EQ(forward.transfer_direction_mode, TransferDirectionMode::FORWARD);
}

TEST(ClientConfigBuilderTest,
     BuildP2PClientConfigRejectsInvalidTransferDirectionMode) {
    auto error_result = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32, 9003, true,
        {}, 0, 2000, 0, 0, 0, "invalid");
    ASSERT_FALSE(error_result);
    EXPECT_EQ(error_result.error(), ErrorCode::INVALID_PARAMS);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigRejectsInvalidTransferMode) {
    auto error_result = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 8, 2048, 512 * 1024 * 1024, 120000, "invalid_mode");
    ASSERT_FALSE(error_result);
    EXPECT_EQ(error_result.error(), ErrorCode::INVALID_PARAMS);
}

// ---- LoadTieredConfig: file path ----

TEST(ClientConfigBuilderTest, LoadFromFilePath) {
    const std::string tmp_path = "/tmp/mc_test_tiered_cfg.json";
    {
        std::ofstream f(tmp_path);
        f << kTieredConfigJson;
    }

    auto config = P2PClientConfigBuilder::build_p2p_real_client(
                      "127.0.0.1:12345", "http://127.0.0.1:8080/metadata",
                      "tcp", std::nullopt, "127.0.0.1:50051", tmp_path)
                      .value();

    EXPECT_FALSE(config.tiered_backend_config.isNull());
    EXPECT_TRUE(config.tiered_backend_config.isMember("tiers"));
    EXPECT_EQ(config.tiered_backend_config["tiers"].size(), 1u);

    // A non-null JSON array is not an object with a "tiers" member.
    {
        std::ofstream f(tmp_path);
        f << "[]";
    }
    auto non_object = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "P2PHANDSHAKE", "tcp", std::nullopt,
        "127.0.0.1:50051", tmp_path);
    EXPECT_FALSE(non_object);
    if (!non_object) {
        EXPECT_EQ(non_object.error(), ErrorCode::INVALID_PARAMS);
    }
    std::filesystem::remove(tmp_path);
}

TEST(ClientConfigBuilderTest, InvalidTieredConfigInputsReturnError) {
    // Preserve the missing-file, malformed-inline-JSON and empty-input cases.
    for (const auto* input :
         {"/nonexistent/path/tiered.json", "{ not valid json", ""}) {
        SCOPED_TRACE(std::string("tiered_backend_config=") + input);
        auto result = P2PClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", input);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
}

}  // namespace
}  // namespace mooncake
