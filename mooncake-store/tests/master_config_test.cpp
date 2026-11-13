#include "master_config.h"

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

TEST(ObjectStorageConfigTest, RegionIsRequiredOnlyWhenEnabled) {
    ObjectStorageConfig config;
    EXPECT_TRUE(config.target.region.empty());
    EXPECT_TRUE(config.Validate());
    config.enabled = true;
    config.target.endpoint = "https://objects.example.com";
    config.target.bucket = "bucket";
    config.target.prefix = "cluster";
    for (const char* provider : {"s3", "oss"}) {
        SCOPED_TRACE(provider);
        config.target.provider = provider;
        config.target.region.clear();
        EXPECT_FALSE(config.Validate());
        config.target.region = "cn-hangzhou";
        EXPECT_TRUE(config.Validate());
    }
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

TEST(HALeaseTtlConfigTest, SupervisorConfigDefaultsToMasterViewTtl) {
    const MasterServiceSupervisorConfig config{};

    EXPECT_EQ(DEFAULT_MASTER_VIEW_LEASE_TTL_SEC,
              config.ha_leader_lease_ttl_sec);
}

TEST(HALeaseTtlConfigTest, SupervisorConfigCarriesMasterConfigTtl) {
    MasterConfig master_config{};
    master_config.ha_backend_type = "etcd";
    EXPECT_EQ(DEFAULT_MASTER_VIEW_LEASE_TTL_SEC,
              master_config.ha_leader_lease_ttl_sec);
    master_config.ha_leader_lease_ttl_sec = 17;

    const MasterServiceSupervisorConfig supervisor_config(master_config);

    EXPECT_EQ(17, supervisor_config.ha_leader_lease_ttl_sec);
}

}  // namespace
}  // namespace mooncake::test
