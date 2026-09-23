#include "master_config.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>

#include "../src/config/runtime_identity_config_loader.h"
#include "default_config.h"

namespace mooncake::test {
namespace {

class ScopedPodEnvironment {
   public:
    ScopedPodEnvironment(const char* name, const char* value) : name_(name) {
        if (const char* previous = std::getenv(name)) {
            previous_ = previous;
        }
        if (value != nullptr) {
            EXPECT_EQ(setenv(name, value, 1), 0);
        } else {
            EXPECT_EQ(unsetenv(name), 0);
        }
    }

    ~ScopedPodEnvironment() {
        if (previous_) {
            setenv(name_.c_str(), previous_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::optional<std::string> previous_;
};

class RuntimeIdentityConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() /
                               "runtime_identity_config_test_XXXXXX")
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

TEST_F(RuntimeIdentityConfigTest, UsesEmptyDefaultsWithoutSources) {
    ScopedPodEnvironment name("POD_NAME", nullptr);
    ScopedPodEnvironment pod_namespace("POD_NAMESPACE", nullptr);

    const auto config = ResolveRuntimeIdentityConfig(nullptr, {});
    EXPECT_TRUE(config.pod_name.empty());
    EXPECT_TRUE(config.pod_namespace.empty());
}

TEST_F(RuntimeIdentityConfigTest, LoadsExistingFlatYamlAndJsonKeys) {
    ScopedPodEnvironment name("POD_NAME", "environment-name");
    ScopedPodEnvironment pod_namespace("POD_NAMESPACE", "environment-ns");

    const auto yaml =
        LoadConfig(".yaml", "pod_name: yaml-pod\npod_namespace: yaml-ns\n");
    const auto from_yaml = ResolveRuntimeIdentityConfig(yaml.get(), {});
    EXPECT_EQ(from_yaml.pod_name, "yaml-pod");
    EXPECT_EQ(from_yaml.pod_namespace, "yaml-ns");

    const auto json = LoadConfig(
        ".json", R"({"pod_name":"json-pod","pod_namespace":"json-ns"})");
    const auto from_json = ResolveRuntimeIdentityConfig(json.get(), {});
    EXPECT_EQ(from_json.pod_name, "json-pod");
    EXPECT_EQ(from_json.pod_namespace, "json-ns");
}

TEST_F(RuntimeIdentityConfigTest, ExplicitCommandLineOverridesFile) {
    ScopedPodEnvironment name("POD_NAME", "environment-name");
    ScopedPodEnvironment pod_namespace("POD_NAMESPACE", "environment-ns");
    const auto file =
        LoadConfig(".yaml", "pod_name: file-pod\npod_namespace: file-ns\n");
    RuntimeIdentityCommandLineOverrides command_line;
    command_line.pod_name = "cli-pod";
    command_line.pod_namespace = "cli-ns";

    const auto config = ResolveRuntimeIdentityConfig(file.get(), command_line);
    EXPECT_EQ(config.pod_name, "cli-pod");
    EXPECT_EQ(config.pod_namespace, "cli-ns");
}

TEST_F(RuntimeIdentityConfigTest, AbsentCommandLinePreservesFileValues) {
    ScopedPodEnvironment name("POD_NAME", "environment-name");
    ScopedPodEnvironment pod_namespace("POD_NAMESPACE", "environment-ns");
    const auto file =
        LoadConfig(".yaml", "pod_name: file-pod\npod_namespace: file-ns\n");

    const auto config = ResolveRuntimeIdentityConfig(file.get(), {});
    EXPECT_EQ(config.pod_name, "file-pod");
    EXPECT_EQ(config.pod_namespace, "file-ns");
}

TEST_F(RuntimeIdentityConfigTest, MissingFieldUsesItsOwnEnvironmentFallback) {
    ScopedPodEnvironment name("POD_NAME", "environment-name");
    ScopedPodEnvironment pod_namespace("POD_NAMESPACE", "environment-ns");
    const auto file = LoadConfig(".json", R"({"pod_name":"file-pod"})");

    const auto config = ResolveRuntimeIdentityConfig(file.get(), {});
    EXPECT_EQ(config.pod_name, "file-pod");
    EXPECT_EQ(config.pod_namespace, "environment-ns");
}

TEST_F(RuntimeIdentityConfigTest,
       EmptyEffectiveValuesUseIndependentEnvFallbacks) {
    ScopedPodEnvironment name("POD_NAME", "environment-name");
    ScopedPodEnvironment pod_namespace("POD_NAMESPACE", "environment-ns");
    const auto file =
        LoadConfig(".yaml", "pod_name: file-pod\npod_namespace: ''\n");
    RuntimeIdentityCommandLineOverrides command_line;
    command_line.pod_name = "";

    const auto config = ResolveRuntimeIdentityConfig(file.get(), command_line);
    EXPECT_EQ(config.pod_name, "environment-name");
    EXPECT_EQ(config.pod_namespace, "environment-ns");
}

TEST_F(RuntimeIdentityConfigTest, EmptyEnvironmentValuesRemainEmpty) {
    ScopedPodEnvironment name("POD_NAME", "");
    ScopedPodEnvironment pod_namespace("POD_NAMESPACE", nullptr);

    const auto config = ResolveRuntimeIdentityConfig(nullptr, {});
    EXPECT_TRUE(config.pod_name.empty());
    EXPECT_TRUE(config.pod_namespace.empty());
}

TEST_F(RuntimeIdentityConfigTest, IdentityPropagatesToHAServingConfig) {
    MasterConfig master_config{};
    master_config.identity.pod_name = "my-pod";
    master_config.identity.pod_namespace = "my-namespace";

    MasterServiceSupervisorConfig supervisor_config(master_config);
    EXPECT_EQ(supervisor_config.pod_name, "my-pod");
    EXPECT_EQ(supervisor_config.pod_namespace, "my-namespace");
}

TEST(ClientLivenessConfigTest, UsesIndependentDefaultsWhenNothingIsSet) {
    const auto resolved = ResolveClientLivenessConfig({}, {});

    EXPECT_EQ(resolved.active_ttl_sec, DEFAULT_CLIENT_LIVE_TTL_SEC);
    EXPECT_EQ(resolved.suspicion_ttl_sec, DEFAULT_CLIENT_SUSPICION_TTL_SEC);
}

TEST(ClientLivenessConfigTest, ExplicitActiveAlsoDefaultsSuspicion) {
    const ClientLivenessConfigSource file{
        .active_ttl_sec = std::nullopt,
        .legacy_ttl_sec = 60,
        .suspicion_ttl_sec = std::nullopt,
    };

    const auto resolved = ResolveClientLivenessConfig(file, {});
    EXPECT_EQ(resolved.active_ttl_sec, 60);
    EXPECT_EQ(resolved.suspicion_ttl_sec, 60);
}

TEST(ClientLivenessConfigTest, CommandLineCanonicalValuesTakePrecedence) {
    const ClientLivenessConfigSource file{
        .active_ttl_sec = 30,
        .legacy_ttl_sec = 40,
        .suspicion_ttl_sec = 50,
    };
    const ClientLivenessConfigSource command_line{
        .active_ttl_sec = 70,
        .legacy_ttl_sec = 80,
        .suspicion_ttl_sec = 90,
    };

    const auto resolved = ResolveClientLivenessConfig(file, command_line);
    EXPECT_EQ(resolved.active_ttl_sec, 70);
    EXPECT_EQ(resolved.suspicion_ttl_sec, 90);
    EXPECT_TRUE(resolved.config_active_conflict);
    EXPECT_TRUE(resolved.command_line_active_conflict);
}

TEST(ClientLivenessConfigTest, RejectsNonPositiveExplicitValues) {
    const ClientLivenessConfigSource file{
        .active_ttl_sec = std::nullopt,
        .legacy_ttl_sec = std::nullopt,
        .suspicion_ttl_sec = 0,
    };

    EXPECT_THROW(ResolveClientLivenessConfig(file, {}), std::invalid_argument);
}

TEST(ClientLivenessConfigTest, LegacyBuilderSetterDefaultsBothWindows) {
    const auto config =
        MasterServiceConfig::builder().set_client_live_ttl_sec(45).build();

    EXPECT_EQ(config.client_active_ttl_sec, 45);
    EXPECT_EQ(config.client_suspicion_ttl_sec, 45);
}

}  // namespace
}  // namespace mooncake::test
