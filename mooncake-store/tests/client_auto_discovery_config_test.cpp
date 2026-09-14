#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include "../src/config/client_auto_discovery_config.h"
#include "environment_variables.h"

namespace mooncake {
namespace {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) {
            original_ = value;
        }
        unsetenv(name);
    }

    ~ScopedEnvVar() {
        if (original_) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

class ClientAutoDiscoveryConfigTest : public ::testing::Test {
   protected:
    using Variables = ClientAutoDiscoveryEnvironmentVariables;

    ScopedEnvVar auto_discover{Variables::MC_MS_AUTO_DISC.name};
    ScopedEnvVar filters{Variables::MC_MS_FILTERS.name};
};

TEST_F(ClientAutoDiscoveryConfigTest, UsesProtocolAndDeviceDefaults) {
    EXPECT_FALSE(
        ClientAutoDiscoveryConfig::FromEnvironment("tcp", false).enabled);
    EXPECT_TRUE(
        ClientAutoDiscoveryConfig::FromEnvironment("rdma", false).enabled);
    EXPECT_TRUE(
        ClientAutoDiscoveryConfig::FromEnvironment("efa", false).enabled);
    EXPECT_FALSE(
        ClientAutoDiscoveryConfig::FromEnvironment("rdma", true).enabled);
    EXPECT_FALSE(
        ClientAutoDiscoveryConfig::FromEnvironment("efa", true).enabled);
}

TEST_F(ClientAutoDiscoveryConfigTest, ExplicitZeroAndOneOverrideDefaults) {
    auto_discover.Set("0");
    EXPECT_FALSE(
        ClientAutoDiscoveryConfig::FromEnvironment("rdma", false).enabled);

    auto_discover.Set("1");
    EXPECT_TRUE(
        ClientAutoDiscoveryConfig::FromEnvironment("tcp", true).enabled);
}

TEST_F(ClientAutoDiscoveryConfigTest, PreservesStoiAcceptedSyntax) {
    for (const char* value : {"1abc", " 1 ", "+1suffix", "01"}) {
        auto_discover.Set(value);
        EXPECT_TRUE(
            ClientAutoDiscoveryConfig::FromEnvironment("tcp", true).enabled)
            << value;
    }
    for (const char* value : {"0abc", " 0 ", "+0suffix", "00"}) {
        auto_discover.Set(value);
        EXPECT_FALSE(
            ClientAutoDiscoveryConfig::FromEnvironment("rdma", false).enabled)
            << value;
    }
}

TEST_F(ClientAutoDiscoveryConfigTest, InvalidValuesUseProtocolDefault) {
    for (const char* value : {"", "true", "-1", "2", "999999999999999999999"}) {
        auto_discover.Set(value);
        EXPECT_TRUE(
            ClientAutoDiscoveryConfig::FromEnvironment("efa", false).enabled)
            << value;
        EXPECT_FALSE(
            ClientAutoDiscoveryConfig::FromEnvironment("tcp", false).enabled)
            << value;
    }
}

TEST_F(ClientAutoDiscoveryConfigTest, SplitsAndTrimsEnabledFilters) {
    auto_discover.Set("1");
    filters.Set(" mlx5_0, ,mlx5_1,, ");

    auto config = ClientAutoDiscoveryConfig::FromEnvironment("rdma", true);
    config.LoadFiltersFromEnvironment();

    EXPECT_EQ(config.filters,
              (std::vector<std::string>{"mlx5_0", "", "mlx5_1", "", ""}));
}

TEST_F(ClientAutoDiscoveryConfigTest, PreservesExplicitlyEmptyFilter) {
    auto_discover.Set("1");
    filters.Set("");

    auto config = ClientAutoDiscoveryConfig::FromEnvironment("tcp", true);
    config.LoadFiltersFromEnvironment();

    EXPECT_EQ(config.filters, (std::vector<std::string>{""}));
}

TEST_F(ClientAutoDiscoveryConfigTest, IgnoresFiltersWhenDiscoveryIsDisabled) {
    auto_discover.Set("0");
    filters.Set("mlx5_0,mlx5_1");

    auto config = ClientAutoDiscoveryConfig::FromEnvironment("rdma", false);
    config.LoadFiltersFromEnvironment();

    EXPECT_TRUE(config.filters.empty());
}

TEST_F(ClientAutoDiscoveryConfigTest, DefersFilterReadUntilFiltersAreLoaded) {
    auto_discover.Set("1");
    filters.Set("before");
    auto config = ClientAutoDiscoveryConfig::FromEnvironment("rdma", true);

    filters.Set("after");
    config.LoadFiltersFromEnvironment();

    EXPECT_EQ(config.filters, (std::vector<std::string>{"after"}));
}

TEST_F(ClientAutoDiscoveryConfigTest, ReadsEnvironmentForEachConfig) {
    auto_discover.Set("1");
    filters.Set("first");
    auto first = ClientAutoDiscoveryConfig::FromEnvironment("tcp", true);
    first.LoadFiltersFromEnvironment();

    auto_discover.Set("0");
    filters.Set("second");
    auto second = ClientAutoDiscoveryConfig::FromEnvironment("rdma", false);
    second.LoadFiltersFromEnvironment();

    EXPECT_TRUE(first.enabled);
    EXPECT_EQ(first.filters, (std::vector<std::string>{"first"}));
    EXPECT_FALSE(second.enabled);
    EXPECT_TRUE(second.filters.empty());
}

}  // namespace
}  // namespace mooncake
