#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>

#include "config/metrics_bootstrap_config_loader.h"
#include "config/nof_heartbeat_bootstrap_config_loader.h"
#include "default_config.h"
#include "ha/snapshot/batch_oplog/config.h"
#include "master_config.h"

namespace mooncake::test {

class MetricsBootstrapConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() /
                               "metrics_bootstrap_config_test_XXXXXX")
                                  .string();
        char* directory = mkdtemp(pattern.data());
        ASSERT_NE(directory, nullptr);
        temp_dir_ = directory;
    }

    void TearDown() override {
        if (!temp_dir_.empty()) {
            std::filesystem::remove_all(temp_dir_);
        }
    }

    std::unique_ptr<DefaultConfig> LoadConfig(const std::string& extension,
                                              const std::string& contents) {
        const auto path = temp_dir_ / ("config" + extension);
        {
            std::ofstream file(path);
            EXPECT_TRUE(file.is_open());
            file << contents;
        }

        auto config = std::make_unique<DefaultConfig>();
        config->SetPath(path.string());
        config->Load();
        return config;
    }

    std::filesystem::path temp_dir_;
};

TEST_F(MetricsBootstrapConfigTest, UsesOwnerDefaultsWithoutSources) {
    const auto resolved = ResolveMetricsBootstrapConfig(nullptr, {});

    EXPECT_TRUE(resolved.enabled);
    EXPECT_EQ(resolved.port, 9003u);
    EXPECT_EQ(resolved.host, "0.0.0.0");
}

TEST_F(MetricsBootstrapConfigTest, LoadsExistingFlatYamlAndJsonKeys) {
    const auto yaml =
        LoadConfig(".yaml",
                   "enable_metric_reporting: false\nmetrics_port: 9101\n"
                   "metrics_host: 127.0.0.1\n");
    const auto from_yaml = ResolveMetricsBootstrapConfig(yaml.get(), {});
    EXPECT_FALSE(from_yaml.enabled);
    EXPECT_EQ(from_yaml.port, 9101u);
    EXPECT_EQ(from_yaml.host, "127.0.0.1");

    const auto json = LoadConfig(
        ".json",
        R"({"enable_metric_reporting":false,"metrics_port":9102,"metrics_host":"127.0.0.2"})");
    const auto from_json = ResolveMetricsBootstrapConfig(json.get(), {});
    EXPECT_FALSE(from_json.enabled);
    EXPECT_EQ(from_json.port, 9102u);
    EXPECT_EQ(from_json.host, "127.0.0.2");
}

TEST_F(MetricsBootstrapConfigTest, AcceptsPortRangeBoundaries) {
    const auto yaml = LoadConfig(".yaml", "metrics_port: 0\n");
    EXPECT_EQ(ResolveMetricsBootstrapConfig(yaml.get(), {}).port, 0u);

    const auto json = LoadConfig(".json", R"({"metrics_port":65535})");
    EXPECT_EQ(ResolveMetricsBootstrapConfig(json.get(), {}).port, 65535u);

    MetricsBootstrapCommandLineOverrides command_line;
    command_line.port = 0;
    EXPECT_EQ(ResolveMetricsBootstrapConfig(nullptr, command_line).port, 0u);
    command_line.port = 65535;
    EXPECT_EQ(ResolveMetricsBootstrapConfig(nullptr, command_line).port,
              65535u);
}

TEST_F(MetricsBootstrapConfigTest, RejectsOutOfRangeYamlAndJsonPorts) {
    const auto yaml = LoadConfig(".yaml", "metrics_port: 65536\n");
    EXPECT_THROW(ResolveMetricsBootstrapConfig(yaml.get(), {}),
                 std::invalid_argument);

    const auto json = LoadConfig(".json", R"({"metrics_port":4294967295})");
    EXPECT_THROW(ResolveMetricsBootstrapConfig(json.get(), {}),
                 std::invalid_argument);
}

TEST_F(MetricsBootstrapConfigTest,
       ExplicitCommandLineValuesOverrideFileValues) {
    const auto file =
        LoadConfig(".yaml",
                   "enable_metric_reporting: true\nmetrics_port: 9401\n"
                   "metrics_host: configured-host\n");
    const MetricsBootstrapCommandLineOverrides command_line{
        .enabled = false,
        .port = 0,
        .host = "",
    };

    const auto resolved =
        ResolveMetricsBootstrapConfig(file.get(), command_line);

    EXPECT_FALSE(resolved.enabled);
    EXPECT_EQ(resolved.port, 0u);
    EXPECT_TRUE(resolved.host.empty());
}

TEST_F(MetricsBootstrapConfigTest, AbsentCommandLineValuesPreserveFileValues) {
    const auto file =
        LoadConfig(".yaml",
                   "enable_metric_reporting: false\nmetrics_port: 9501\n"
                   "metrics_host: configured-host\n");

    const auto resolved = ResolveMetricsBootstrapConfig(file.get(), {});

    EXPECT_FALSE(resolved.enabled);
    EXPECT_EQ(resolved.port, 9501u);
    EXPECT_EQ(resolved.host, "configured-host");
}

TEST_F(MetricsBootstrapConfigTest, RejectsOutOfRangeCommandLinePort) {
    MetricsBootstrapCommandLineOverrides command_line;
    command_line.port = 65536;
    EXPECT_THROW(ResolveMetricsBootstrapConfig(nullptr, command_line),
                 std::invalid_argument);

    // A negative int32 flag becomes UINT32_MAX at the existing CLI bridge.
    command_line.port = UINT32_MAX;
    EXPECT_THROW(ResolveMetricsBootstrapConfig(nullptr, command_line),
                 std::invalid_argument);
}

class NofHeartbeatBootstrapConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() /
                               "nof_heartbeat_bootstrap_test_XXXXXX")
                                  .string();
        char* directory = mkdtemp(pattern.data());
        ASSERT_NE(directory, nullptr);
        temp_dir_ = directory;
    }

    void TearDown() override {
        if (!temp_dir_.empty()) {
            std::filesystem::remove_all(temp_dir_);
        }
    }

    std::unique_ptr<DefaultConfig> LoadConfig(const std::string& extension,
                                              const std::string& contents) {
        const auto path = temp_dir_ / ("config" + extension);
        {
            std::ofstream file(path);
            EXPECT_TRUE(file.is_open());
            file << contents;
        }
        auto config = std::make_unique<DefaultConfig>();
        config->SetPath(path.string());
        config->Load();
        return config;
    }

    std::filesystem::path temp_dir_;
};

TEST_F(NofHeartbeatBootstrapConfigTest, UsesExistingDefaults) {
    const auto resolved = ResolveNofHeartbeatBootstrapConfig(nullptr, {});
    EXPECT_EQ(resolved.interval, std::chrono::seconds(10));
    EXPECT_EQ(resolved.probe_timeout, std::chrono::milliseconds(1000));
    EXPECT_EQ(resolved.failures_threshold, 3u);
}

TEST_F(NofHeartbeatBootstrapConfigTest, LoadsExistingFlatYamlAndJsonKeys) {
    const auto yaml = LoadConfig(".yaml",
                                 "nof_heartbeat_interval_sec: 7\n"
                                 "nof_heartbeat_probe_timeout_ms: 250\n"
                                 "nof_heartbeat_failures_threshold: 4\n");
    const auto from_yaml = ResolveNofHeartbeatBootstrapConfig(yaml.get(), {});
    EXPECT_EQ(from_yaml.interval, std::chrono::seconds(7));
    EXPECT_EQ(from_yaml.probe_timeout, std::chrono::milliseconds(250));
    EXPECT_EQ(from_yaml.failures_threshold, 4u);

    const auto json = LoadConfig(
        ".json",
        R"({"nof_heartbeat_interval_sec":2,"nof_heartbeat_probe_timeout_ms":300,"nof_heartbeat_failures_threshold":5})");
    const auto from_json = ResolveNofHeartbeatBootstrapConfig(json.get(), {});
    EXPECT_EQ(from_json.interval, std::chrono::seconds(2));
    EXPECT_EQ(from_json.probe_timeout, std::chrono::milliseconds(300));
    EXPECT_EQ(from_json.failures_threshold, 5u);
}

TEST_F(NofHeartbeatBootstrapConfigTest, ExplicitCliValuesOverrideFile) {
    const auto file = LoadConfig(".yaml",
                                 "nof_heartbeat_interval_sec: 7\n"
                                 "nof_heartbeat_probe_timeout_ms: 250\n"
                                 "nof_heartbeat_failures_threshold: 4\n");
    const NofHeartbeatCommandLineOverrides command_line{
        .interval_seconds = 0,
        .probe_timeout_ms = 0,
        .failures_threshold = 0,
    };
    const auto resolved =
        ResolveNofHeartbeatBootstrapConfig(file.get(), command_line);
    EXPECT_EQ(resolved.interval, std::chrono::seconds(0));
    EXPECT_EQ(resolved.probe_timeout, std::chrono::milliseconds(0));
    EXPECT_EQ(resolved.failures_threshold, 0u);

    const auto without_cli = ResolveNofHeartbeatBootstrapConfig(file.get(), {});
    EXPECT_EQ(without_cli.interval, std::chrono::seconds(7));
    EXPECT_EQ(without_cli.probe_timeout, std::chrono::milliseconds(250));
    EXPECT_EQ(without_cli.failures_threshold, 4u);
}

TEST_F(NofHeartbeatBootstrapConfigTest, PreservesSignedAndUnsignedRanges) {
    const auto yaml = LoadConfig(".yaml", "nof_heartbeat_interval_sec: -1\n");
    const auto json = LoadConfig(
        ".json",
        R"({"nof_heartbeat_probe_timeout_ms":4294967295,"nof_heartbeat_failures_threshold":4294967295})");
    EXPECT_EQ(ResolveNofHeartbeatBootstrapConfig(yaml.get(), {}).interval,
              std::chrono::seconds(-1));
    const auto from_json = ResolveNofHeartbeatBootstrapConfig(json.get(), {});
    EXPECT_EQ(from_json.probe_timeout,
              std::chrono::milliseconds(std::numeric_limits<uint32_t>::max()));
    EXPECT_EQ(from_json.failures_threshold,
              std::numeric_limits<uint32_t>::max());

    NofHeartbeatCommandLineOverrides command_line;
    command_line.interval_seconds = std::numeric_limits<int64_t>::min();
    EXPECT_EQ(
        ResolveNofHeartbeatBootstrapConfig(nullptr, command_line).interval,
        std::chrono::seconds(std::numeric_limits<int64_t>::min()));
}

TEST_F(NofHeartbeatBootstrapConfigTest, KeepsInvalidYamlFailure) {
    const auto file =
        LoadConfig(".yaml", "nof_heartbeat_interval_sec: invalid\n");
    EXPECT_THROW(ResolveNofHeartbeatBootstrapConfig(file.get(), {}),
                 std::exception);
}

TEST(NofHeartbeatBootstrapConfigPropagationTest,
     ReachesStandaloneAndHaServingConfigs) {
    MasterConfig master_config{};
    master_config.allocation_strategy = "random";
    master_config.nof_heartbeat.interval = std::chrono::seconds(-7);
    master_config.nof_heartbeat.probe_timeout = NofHeartbeatProbeTimeout(250);
    master_config.nof_heartbeat.failures_threshold = 9;
    // The probe timeout stays unsigned by type, so no signed value can be
    // narrowed into the serving field while it is forwarded.
    static_assert(std::is_unsigned_v<
                  decltype(master_config.nof_heartbeat.probe_timeout)::rep>);

    WrappedMasterServiceConfig standalone(master_config, 0);
    EXPECT_EQ(standalone.nof_heartbeat_interval_sec, -7);
    EXPECT_EQ(standalone.nof_heartbeat_probe_timeout_ms, 250u);
    EXPECT_EQ(standalone.nof_heartbeat_failures_threshold, 9u);

    MasterServiceSupervisorConfig supervisor(master_config);
    EXPECT_EQ(supervisor.nof_heartbeat_interval_sec.Get(), -7);
    EXPECT_EQ(supervisor.nof_heartbeat_probe_timeout_ms.Get(), 250u);
    EXPECT_EQ(supervisor.nof_heartbeat_failures_threshold.Get(), 9u);

    WrappedMasterServiceConfig ha_serving(supervisor, 0);
    EXPECT_EQ(ha_serving.nof_heartbeat_interval_sec, -7);
    EXPECT_EQ(ha_serving.nof_heartbeat_probe_timeout_ms, 250u);
    EXPECT_EQ(ha_serving.nof_heartbeat_failures_threshold, 9u);
}

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesDefaultsTo1024) {
    MasterConfig master_config;
    EXPECT_EQ(1024u, master_config.oplog_batch_max_entries);

    MasterServiceConfig service_config;
    EXPECT_EQ(1024u, service_config.oplog_batch_max_entries);
}

TEST(MasterServiceConfigTest, OplogIsDisabledByDefault) {
    MasterConfig master_config;
    EXPECT_FALSE(master_config.enable_oplog);

    MasterServiceConfig service_config;
    EXPECT_FALSE(service_config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogBuilderOverrideIsRespected) {
    auto config = MasterServiceConfig::builder().set_enable_oplog(true).build();

    EXPECT_TRUE(config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogSnapshotDefaultsAndOverridesPropagate) {
    MasterConfig master_config{};
    EXPECT_FALSE(master_config.enable_oplog_snapshot);
    EXPECT_EQ(1000000u, master_config.snapshot_chunk_object_count);

    master_config.enable_oplog_snapshot = true;
    master_config.snapshot_chunk_object_count = 17;
    MasterServiceSupervisorConfig supervisor_config(master_config);
    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_TRUE(service_config.enable_oplog_snapshot);
    EXPECT_EQ(17u, service_config.snapshot_chunk_object_count);
}

TEST(MasterServiceConfigTest, ValidatesOplogSnapshotConfiguration) {
    MasterConfig config{};
    EXPECT_FALSE(ValidateBatchOpLogSnapshotConfig(config).has_value());

    config.enable_oplog_snapshot = true;
    EXPECT_TRUE(ValidateBatchOpLogSnapshotConfig(config).has_value());
    config.enable_oplog = true;
    config.enable_ha = true;
    config.ha_backend_type = "etcd";
    config.cluster_id = "snapshot-config-test";
    config.snapshot_object_store_type = "local";
    EXPECT_FALSE(ValidateBatchOpLogSnapshotConfig(config).has_value());

    config.snapshot_chunk_object_count = 0;
    EXPECT_TRUE(ValidateBatchOpLogSnapshotConfig(config).has_value());
    config.snapshot_chunk_object_count = 1;
    config.snapshot_object_store_type.clear();
    EXPECT_TRUE(ValidateBatchOpLogSnapshotConfig(config).has_value());
}

TEST(MasterServiceConfigTest, OplogEnablementPropagatesToServingConfig) {
    MasterConfig master_config{};
    master_config.enable_oplog = true;
    MasterServiceSupervisorConfig supervisor_config(master_config);

    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_TRUE(supervisor_config.enable_oplog);
    EXPECT_TRUE(wrapped_config.enable_oplog);
    EXPECT_TRUE(service_config.enable_oplog);
}

TEST(MasterServiceConfigTest, WeightCapabilityConfirmationPropagates) {
    MasterConfig master_config{};
    EXPECT_FALSE(master_config.weight_management_oplog_capability_confirmed);
    EXPECT_FALSE(
        MasterServiceConfig{}.weight_management_oplog_capability_confirmed);
    master_config.weight_management_oplog_capability_confirmed = true;
    MasterServiceSupervisorConfig supervisor_config(master_config);
    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);
    EXPECT_TRUE(supervisor_config.weight_management_oplog_capability_confirmed);
    EXPECT_TRUE(wrapped_config.weight_management_oplog_capability_confirmed);
    EXPECT_TRUE(service_config.weight_management_oplog_capability_confirmed);
    const auto built =
        MasterServiceConfig::builder()
            .set_weight_management_oplog_capability_confirmed(true)
            .build();
    EXPECT_TRUE(built.weight_management_oplog_capability_confirmed);
}

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesBuilderOverrideRespected) {
    auto config =
        MasterServiceConfig::builder().set_oplog_batch_max_entries(17).build();

    EXPECT_EQ(17u, config.oplog_batch_max_entries);
}

TEST(MasterServiceConfigTest, MetricsBootstrapConfigPropagatesToSupervisor) {
    MasterConfig master_config{};
    master_config.metrics = {
        .enabled = false,
        .port = 65535,
        .host = "127.0.0.7",
    };

    const MasterServiceSupervisorConfig supervisor_config(master_config);
    const auto metrics = supervisor_config.metrics.Get();

    EXPECT_FALSE(metrics.enabled);
    EXPECT_EQ(metrics.port, 65535u);
    EXPECT_EQ(metrics.host, "127.0.0.7");
}

TEST(MasterServiceConfigTest,
     MetricsBootstrapConfigPreservesWrappedCompatibility) {
    MasterConfig master_config{};
    master_config.metrics = {
        .enabled = false,
        .port = 65535,
        .host = "127.0.0.8",
    };

    const WrappedMasterServiceConfig from_master(master_config, 1);
    const MasterServiceSupervisorConfig supervisor_config(master_config);
    const WrappedMasterServiceConfig from_supervisor(supervisor_config, 1);

    EXPECT_FALSE(from_master.enable_metric_reporting);
    EXPECT_EQ(from_master.http_port, UINT16_MAX);
    EXPECT_FALSE(from_supervisor.enable_metric_reporting);
    EXPECT_EQ(from_supervisor.http_port, UINT16_MAX);
}

}  // namespace mooncake::test
