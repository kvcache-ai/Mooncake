#include <gtest/gtest.h>

#include "../src/config/shm_spdk_registration_config.h"
#include "environ.h"

namespace mooncake {
namespace {

TEST(ShmSpdkRegistrationConfigTest, OnlyExactOneEnablesRegistration) {
    MapEnvironSource source;
    const Environ env(source);
    EXPECT_FALSE(ShmSpdkRegistrationConfig::FromEnvironment(env).enabled);
    for (const char* value : {"", "0", "true", "yes", " 1", "1 ", "01"}) {
        source.Set("MC_STORE_REGISTER_SPDK", value);
        EXPECT_FALSE(ShmSpdkRegistrationConfig::FromEnvironment(env).enabled)
            << value;
    }
    source.Set("MC_STORE_REGISTER_SPDK", "1");
    EXPECT_TRUE(ShmSpdkRegistrationConfig::FromEnvironment(env).enabled);
}

}  // namespace
}  // namespace mooncake
