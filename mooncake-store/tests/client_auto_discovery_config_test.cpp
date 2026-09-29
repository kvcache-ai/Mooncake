#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "../src/config/client_auto_discovery_config.h"
#include "environ.h"
#include "../src/config/client_environment_variables.h"

namespace mooncake {
namespace {

class ClientAutoDiscoveryConfigTest : public ::testing::Test {
   protected:
    using Variables = ClientEnvironmentVariables::AutoDiscovery;

    ClientAutoDiscoveryConfig Load(std::string_view protocol,
                                   bool device_names_configured) const {
        return ClientAutoDiscoveryConfig::FromEnvironment(
            Environ(source_), protocol, device_names_configured);
    }

    void LoadFilters(ClientAutoDiscoveryConfig& config) const {
        config.LoadFiltersFromEnvironment(Environ(source_));
    }

    void SetAutoDiscover(const char* value) {
        source_.Set(Variables::MC_MS_AUTO_DISC.name, value);
    }

    void SetFilters(const char* value) {
        source_.Set(Variables::MC_MS_FILTERS.name, value);
    }

    MapEnvironSource source_;
};

TEST_F(ClientAutoDiscoveryConfigTest, UsesProtocolAndDeviceDefaults) {
    EXPECT_FALSE(Load("tcp", false).enabled);
    EXPECT_TRUE(Load("rdma", false).enabled);
    EXPECT_TRUE(Load("efa", false).enabled);
    EXPECT_FALSE(Load("rdma", true).enabled);
    EXPECT_FALSE(Load("efa", true).enabled);
}

TEST_F(ClientAutoDiscoveryConfigTest, ExplicitZeroAndOneOverrideDefaults) {
    SetAutoDiscover("0");
    EXPECT_FALSE(Load("rdma", false).enabled);

    SetAutoDiscover("1");
    EXPECT_TRUE(Load("tcp", true).enabled);
}

TEST_F(ClientAutoDiscoveryConfigTest, PreservesStoiAcceptedSyntax) {
    for (const char* value : {"1abc", " 1 ", "+1suffix", "01"}) {
        SetAutoDiscover(value);
        EXPECT_TRUE(Load("tcp", true).enabled) << value;
    }
    for (const char* value : {"0abc", " 0 ", "+0suffix", "00"}) {
        SetAutoDiscover(value);
        EXPECT_FALSE(Load("rdma", false).enabled) << value;
    }
}

TEST_F(ClientAutoDiscoveryConfigTest, InvalidValuesUseProtocolDefault) {
    for (const char* value : {"", "true", "-1", "2", "999999999999999999999"}) {
        SetAutoDiscover(value);
        EXPECT_TRUE(Load("efa", false).enabled) << value;
        EXPECT_FALSE(Load("tcp", false).enabled) << value;
    }
}

TEST_F(ClientAutoDiscoveryConfigTest, SplitsAndTrimsEnabledFilters) {
    SetAutoDiscover("1");
    SetFilters(" mlx5_0, ,mlx5_1,, ");

    auto config = Load("rdma", true);
    LoadFilters(config);

    EXPECT_EQ(config.filters,
              (std::vector<std::string>{"mlx5_0", "", "mlx5_1", "", ""}));
}

TEST_F(ClientAutoDiscoveryConfigTest, PreservesExplicitlyEmptyFilter) {
    SetAutoDiscover("1");
    SetFilters("");

    auto config = Load("tcp", true);
    LoadFilters(config);

    EXPECT_EQ(config.filters, (std::vector<std::string>{""}));
}

TEST_F(ClientAutoDiscoveryConfigTest, IgnoresFiltersWhenDiscoveryIsDisabled) {
    SetAutoDiscover("0");
    SetFilters("mlx5_0,mlx5_1");

    auto config = Load("rdma", false);
    LoadFilters(config);

    EXPECT_TRUE(config.filters.empty());
}

TEST_F(ClientAutoDiscoveryConfigTest, DefersFilterReadUntilFiltersAreLoaded) {
    SetAutoDiscover("1");
    SetFilters("before");
    auto config = Load("rdma", true);

    SetFilters("after");
    LoadFilters(config);

    EXPECT_EQ(config.filters, (std::vector<std::string>{"after"}));
}

TEST_F(ClientAutoDiscoveryConfigTest, ReadsEnvironmentForEachConfig) {
    SetAutoDiscover("1");
    SetFilters("first");
    auto first = Load("tcp", true);
    LoadFilters(first);

    SetAutoDiscover("0");
    SetFilters("second");
    auto second = Load("rdma", false);
    LoadFilters(second);

    EXPECT_TRUE(first.enabled);
    EXPECT_EQ(first.filters, (std::vector<std::string>{"first"}));
    EXPECT_FALSE(second.enabled);
    EXPECT_TRUE(second.filters.empty());
}

}  // namespace
}  // namespace mooncake
