#include <gtest/gtest.h>

#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "../src/config/metrics_bootstrap_config_loader.h"
#include "default_config.h"

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

}  // namespace mooncake::test
