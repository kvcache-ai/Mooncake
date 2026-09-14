#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "client_auto_port_config.h"

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
        if (original_.has_value()) {
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

struct ClientAutoPortEnvironment {
    ScopedEnvVar setup_retries{"MC_STORE_CLIENT_SETUP_RETRIES"};
    ScopedEnvVar min_port{"MC_STORE_CLIENT_MIN_PORT"};
    ScopedEnvVar max_port{"MC_STORE_CLIENT_MAX_PORT"};
};

class ClientAutoPortConfigTest : public ::testing::Test {
   protected:
    ClientAutoPortEnvironment env;
};

TEST_F(ClientAutoPortConfigTest, UsesExistingDefaultsWhenEnvironmentIsUnset) {
    const auto config = ClientAutoPortConfig::FromEnvironment();

    EXPECT_EQ(config.max_retries, 20);
    EXPECT_EQ(config.min_port, 12300);
    EXPECT_EQ(config.max_port, 14300);
}

TEST_F(ClientAutoPortConfigTest, ReadsValidValues) {
    env.setup_retries.Set("7");
    env.min_port.Set("12000");
    env.max_port.Set("14000");

    const auto config = ClientAutoPortConfig::FromEnvironment();

    EXPECT_EQ(config.max_retries, 7);
    EXPECT_EQ(config.min_port, 12000);
    EXPECT_EQ(config.max_port, 14000);
}

TEST_F(ClientAutoPortConfigTest, InvalidIntegersUseIndividualFieldDefaults) {
    for (const char* value : {"", "invalid", "2147483648"}) {
        env.setup_retries.Set(value);
        env.min_port.Set(value);
        env.max_port.Set("15000");

        auto config = ClientAutoPortConfig::FromEnvironment();

        EXPECT_EQ(config.max_retries, 20) << value;
        EXPECT_EQ(config.min_port, 12300) << value;
        EXPECT_EQ(config.max_port, 15000) << value;

        env.min_port.Set("13000");
        env.max_port.Set(value);

        config = ClientAutoPortConfig::FromEnvironment();

        EXPECT_EQ(config.min_port, 13000) << value;
        EXPECT_EQ(config.max_port, 14300) << value;
    }
}

TEST_F(ClientAutoPortConfigTest, SupportsIndependentEndpointOverrides) {
    env.min_port.Set("13000");

    auto config = ClientAutoPortConfig::FromEnvironment();
    EXPECT_EQ(config.min_port, 13000);
    EXPECT_EQ(config.max_port, 14300);

    env.min_port.Set("12300");
    env.max_port.Set("15000");

    config = ClientAutoPortConfig::FromEnvironment();
    EXPECT_EQ(config.min_port, 12300);
    EXPECT_EQ(config.max_port, 15000);
}

TEST_F(ClientAutoPortConfigTest, InvalidPortPairsRestoreBothDefaults) {
    struct PortPair {
        const char* min_port;
        const char* max_port;
    };
    for (const auto& value :
         {PortPair{"14301", "14300"}, PortPair{"80", "443"},
          PortPair{"32768", "40000"}, PortPair{"61000", "65536"}}) {
        env.min_port.Set(value.min_port);
        env.max_port.Set(value.max_port);

        const auto config = ClientAutoPortConfig::FromEnvironment();

        EXPECT_EQ(config.min_port, 12300);
        EXPECT_EQ(config.max_port, 14300);
    }
}

TEST_F(ClientAutoPortConfigTest, PreservesNonPositiveRetryCounts) {
    env.setup_retries.Set("0");
    EXPECT_EQ(ClientAutoPortConfig::FromEnvironment().max_retries, 0);

    env.setup_retries.Set("-1");
    EXPECT_EQ(ClientAutoPortConfig::FromEnvironment().max_retries, -1);
}

TEST_F(ClientAutoPortConfigTest, PreservesAcceptedIntegerSyntax) {
    env.setup_retries.Set(" +7 ");
    env.min_port.Set(" +12000 ");
    env.max_port.Set(" +14000 ");

    const auto config = ClientAutoPortConfig::FromEnvironment();

    EXPECT_EQ(config.max_retries, 7);
    EXPECT_EQ(config.min_port, 12000);
    EXPECT_EQ(config.max_port, 14000);
}

}  // namespace
}  // namespace mooncake
