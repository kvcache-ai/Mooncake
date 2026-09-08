#include "master_config.h"

#include <gtest/gtest.h>

namespace mooncake::test {

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

TEST(MasterServiceConfigTest, ClientMassExpiryGuardIsArmedByDefault) {
    MasterConfig master_config;
    EXPECT_TRUE(master_config.client_mass_expiry_guard);
    EXPECT_EQ(DEFAULT_CLIENT_MASS_EXPIRY_GRACE_SEC,
              master_config.client_mass_expiry_grace_sec);

    MasterServiceConfig service_config;
    EXPECT_TRUE(service_config.client_mass_expiry_guard);
    EXPECT_EQ(DEFAULT_CLIENT_MASS_EXPIRY_GRACE_SEC,
              service_config.client_mass_expiry_grace_sec);
}

TEST(MasterServiceConfigTest, ClientMassExpiryBuilderOverridesAreRespected) {
    auto config = MasterServiceConfig::builder()
                      .set_client_mass_expiry_guard(false)
                      .set_client_mass_expiry_grace_sec(7)
                      .build();

    EXPECT_FALSE(config.client_mass_expiry_guard);
    EXPECT_EQ(7, config.client_mass_expiry_grace_sec);
}

// Every layer between the master's own config and the one MasterService is
// constructed with has to carry these, or an operator's flag reaches a
// serving master silently unchanged.
TEST(MasterServiceConfigTest, ClientMassExpiryPropagatesToServingConfig) {
    MasterConfig master_config{};
    master_config.client_mass_expiry_guard = false;
    master_config.client_mass_expiry_grace_sec = 11;
    MasterServiceSupervisorConfig supervisor_config(master_config);

    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_FALSE(supervisor_config.client_mass_expiry_guard);
    EXPECT_EQ(11, supervisor_config.client_mass_expiry_grace_sec);
    EXPECT_FALSE(wrapped_config.client_mass_expiry_guard);
    EXPECT_EQ(11, wrapped_config.client_mass_expiry_grace_sec);
    EXPECT_FALSE(service_config.client_mass_expiry_guard);
    EXPECT_EQ(11, service_config.client_mass_expiry_grace_sec);
}

// The command-line validator and the config-file path share this predicate,
// so a grace that arrives from a yaml file cannot skip the check the flag gets.
TEST(MasterServiceConfigTest, ClientMassExpiryGraceRejectsNegativeValues) {
    EXPECT_TRUE(IsValidClientMassExpiryGraceSec(0));
    EXPECT_TRUE(
        IsValidClientMassExpiryGraceSec(DEFAULT_CLIENT_MASS_EXPIRY_GRACE_SEC));
    EXPECT_FALSE(IsValidClientMassExpiryGraceSec(-1));
}

}  // namespace mooncake::test
