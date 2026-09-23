#include <gtest/gtest.h>

#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "../src/config/metrics_bootstrap_config_loader.h"
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
