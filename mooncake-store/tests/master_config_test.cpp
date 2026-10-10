#include "master_config.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "config/tenant_quota_bootstrap_config_loader.h"
#include "default_config.h"

namespace mooncake::test {
namespace {

class TenantQuotaBootstrapConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() /
                               "tenant_quota_bootstrap_config_test_XXXXXX")
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

TEST_F(TenantQuotaBootstrapConfigTest, UsesLegacyDefaultsWithoutSources) {
    const auto resolved = ResolveTenantQuotaBootstrapConfig(nullptr, {});

    EXPECT_FALSE(resolved.enable_multi_tenants);
    EXPECT_EQ(resolved.tenant_quota_connector_type, "file");
    EXPECT_TRUE(resolved.tenant_quota_connector_uri.empty());
}

TEST_F(TenantQuotaBootstrapConfigTest, LoadsExistingFlatYamlAndJsonKeys) {
    const auto yaml = LoadConfig(
        ".yaml",
        "enable_multi_tenants: true\ntenant_quota_connector_type: etcd\n"
        "tenant_quota_connector_uri: yaml-endpoint\n");
    const auto from_yaml = ResolveTenantQuotaBootstrapConfig(yaml.get(), {});
    EXPECT_TRUE(from_yaml.enable_multi_tenants);
    EXPECT_EQ(from_yaml.tenant_quota_connector_type, "etcd");
    EXPECT_EQ(from_yaml.tenant_quota_connector_uri, "yaml-endpoint");

    const auto json = LoadConfig(
        ".json",
        R"({"enable_multi_tenants":true,"tenant_quota_connector_type":"file","tenant_quota_connector_uri":"json-policy.yaml"})");
    const auto from_json = ResolveTenantQuotaBootstrapConfig(json.get(), {});
    EXPECT_TRUE(from_json.enable_multi_tenants);
    EXPECT_EQ(from_json.tenant_quota_connector_type, "file");
    EXPECT_EQ(from_json.tenant_quota_connector_uri, "json-policy.yaml");
}

TEST_F(TenantQuotaBootstrapConfigTest, MissingFlatKeysKeepTheirOwnDefaults) {
    const auto file =
        LoadConfig(".yaml", "tenant_quota_connector_uri: policy.yaml\n");

    const auto resolved = ResolveTenantQuotaBootstrapConfig(file.get(), {});
    EXPECT_FALSE(resolved.enable_multi_tenants);
    EXPECT_EQ(resolved.tenant_quota_connector_type, "file");
    EXPECT_EQ(resolved.tenant_quota_connector_uri, "policy.yaml");
}

TEST_F(TenantQuotaBootstrapConfigTest,
       ExplicitCommandLineFalseAndEmptyStringsOverrideFile) {
    const auto file = LoadConfig(
        ".yaml",
        "enable_multi_tenants: true\ntenant_quota_connector_type: etcd\n"
        "tenant_quota_connector_uri: configured-endpoint\n");
    const TenantQuotaCommandLineOverrides command_line{
        .enable_multi_tenants = false,
        .tenant_quota_connector_type = "",
        .tenant_quota_connector_uri = "",
    };

    const auto resolved =
        ResolveTenantQuotaBootstrapConfig(file.get(), command_line);
    EXPECT_FALSE(resolved.enable_multi_tenants);
    EXPECT_TRUE(resolved.tenant_quota_connector_type.empty());
    EXPECT_TRUE(resolved.tenant_quota_connector_uri.empty());
}

TEST_F(TenantQuotaBootstrapConfigTest,
       AbsentCommandLineValuesPreserveFileValues) {
    const auto file = LoadConfig(
        ".json",
        R"({"enable_multi_tenants":true,"tenant_quota_connector_type":"etcd","tenant_quota_connector_uri":"configured-endpoint"})");

    const auto resolved = ResolveTenantQuotaBootstrapConfig(file.get(), {});
    EXPECT_TRUE(resolved.enable_multi_tenants);
    EXPECT_EQ(resolved.tenant_quota_connector_type, "etcd");
    EXPECT_EQ(resolved.tenant_quota_connector_uri, "configured-endpoint");
}

TEST_F(TenantQuotaBootstrapConfigTest,
       DisabledFeatureDoesNotValidateConnectorChoice) {
    const auto file = LoadConfig(".yaml",
                                 "enable_multi_tenants: false\n"
                                 "tenant_quota_connector_type: unsupported\n"
                                 "tenant_quota_connector_uri: ''\n");

    const auto resolved = ResolveTenantQuotaBootstrapConfig(file.get(), {});
    EXPECT_FALSE(resolved.enable_multi_tenants);
    EXPECT_EQ(resolved.tenant_quota_connector_type, "unsupported");
    EXPECT_TRUE(resolved.tenant_quota_connector_uri.empty());
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
