#include "master_config.h"

#include <gtest/gtest.h>

#include <limits>

namespace mooncake::test {
namespace {

TEST(RedisHaConfigTest, ReplicaCountSurvivesAllConfigAdapters) {
    MasterConfig master{};
    EXPECT_EQ(0u, master.ha_redis_expected_replicas_per_shard);
    EXPECT_EQ(0u, MasterServiceConfigBuilder()
                      .build()
                      .ha_redis_expected_replicas_per_shard);
    master.ha_redis_expected_replicas_per_shard = 2;
    master.ha_backend_type = "redis";
    master.ha_backend_connstring = "redis+cluster://localhost:6379";
    MasterServiceSupervisorConfig supervisor(master);
    EXPECT_EQ(2u, supervisor.ha_redis_expected_replicas_per_shard);
    WrappedMasterServiceConfig wrapped(master, 0);
    EXPECT_EQ(2u, wrapped.ha_redis_expected_replicas_per_shard);
    auto service = MasterServiceConfigBuilder()
                       .set_ha_redis_expected_replicas_per_shard(2)
                       .build();
    EXPECT_EQ(2u, service.ha_redis_expected_replicas_per_shard);
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

TEST(OffloadPromotionConfigTest, ConfigSurvivesAdaptersAndValidates) {
    MasterConfig master{};
    master.enable_offload = true;
    master.offload_on_evict = true;
    master.offload_force_evict = true;
    master.offloading_queue_limit = 4096;
    master.offload_cap_ratio = 0.25;
    master.promotion_on_hit = true;
    master.promotion_admission_threshold = 3;
    master.promotion_queue_limit = 2048;
    master.promotion_max_per_heartbeat = 4;

    WrappedMasterServiceConfig wrapped(master, 0);
    MasterServiceConfig service(wrapped);
    EXPECT_TRUE(service.offload_config.enable_offload);
    EXPECT_TRUE(service.offload_config.offload_on_evict);
    EXPECT_TRUE(service.offload_config.offload_force_evict);
    EXPECT_EQ(service.offload_config.offloading_queue_limit, 4096u);
    EXPECT_DOUBLE_EQ(service.offload_config.offload_cap_ratio, 0.25);
    EXPECT_TRUE(service.promotion_config.promotion_on_hit);
    EXPECT_EQ(service.promotion_config.promotion_admission_threshold, 3u);
    EXPECT_EQ(service.promotion_config.promotion_queue_limit, 2048u);
    EXPECT_EQ(service.promotion_config.promotion_max_per_heartbeat, 4u);
    EXPECT_NO_THROW(service.Validate());
    EXPECT_EQ(service.offload_config.OffloadCap(), 1024);
    EXPECT_TRUE(service.offload_config.ShouldOffloadOnEvict());
    EXPECT_FALSE(service.offload_config.ShouldOffloadOnPutEnd());

    {
        OffloadConfig put_end_cfg = service.offload_config;
        put_end_cfg.offload_on_evict = false;
        EXPECT_FALSE(put_end_cfg.ShouldOffloadOnEvict());
        EXPECT_TRUE(put_end_cfg.ShouldOffloadOnPutEnd());
    }
    {
        OffloadConfig disabled_cfg = service.offload_config;
        disabled_cfg.enable_offload = false;
        EXPECT_FALSE(disabled_cfg.ShouldOffloadOnEvict());
        EXPECT_FALSE(disabled_cfg.ShouldOffloadOnPutEnd());
    }

    // Invalid offload & promotion values fail config validation directly.
    {
        MasterServiceConfig bad = service;
        bad.offload_config.offload_cap_ratio = 1.5;
        EXPECT_THROW(bad.Validate(), std::invalid_argument);
    }
    {
        MasterServiceConfig bad = service;
        bad.offload_config.offload_cap_ratio =
            std::numeric_limits<double>::quiet_NaN();
        EXPECT_THROW(bad.Validate(), std::invalid_argument);
    }
    {
        MasterServiceConfig bad = service;
        bad.offload_config.offloading_queue_limit = 0;
        EXPECT_THROW(bad.Validate(), std::invalid_argument);
    }
    {
        MasterServiceConfig bad = service;
        bad.promotion_config.promotion_admission_threshold = 0;
        EXPECT_THROW(bad.Validate(), std::invalid_argument);
    }
    {
        MasterServiceConfig bad = service;
        bad.promotion_config.promotion_admission_threshold = 256;
        EXPECT_THROW(bad.Validate(), std::invalid_argument);
    }
    {
        MasterServiceConfig bad = service;
        bad.promotion_config.promotion_max_per_heartbeat = 0;
        EXPECT_THROW(bad.Validate(), std::invalid_argument);
    }

    // Supervisor config validation rejects invalid offload/promotion knobs.
    {
        MasterConfig bad_master{};
        bad_master.offload_cap_ratio = -0.1;
        EXPECT_THROW((void)MasterServiceSupervisorConfig(bad_master),
                     std::invalid_argument);
    }
    {
        MasterConfig bad_master{};
        bad_master.promotion_max_per_heartbeat = 0;
        EXPECT_THROW((void)MasterServiceSupervisorConfig(bad_master),
                     std::invalid_argument);
    }
}

}  // namespace
}  // namespace mooncake::test
