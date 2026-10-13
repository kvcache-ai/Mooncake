#include <gtest/gtest.h>

#include "client_auto_port_config.h"
#include "environ.h"

namespace mooncake {
namespace {

constexpr char kSetupRetries[] = "MC_STORE_CLIENT_SETUP_RETRIES";
constexpr char kMinPort[] = "MC_STORE_CLIENT_MIN_PORT";
constexpr char kMaxPort[] = "MC_STORE_CLIENT_MAX_PORT";

class ClientAutoPortConfigTest : public ::testing::Test {
   protected:
    ClientAutoPortConfig Load() const {
        return ClientAutoPortConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(ClientAutoPortConfigTest, UsesExistingDefaultsWhenEnvironmentIsUnset) {
    const auto config = Load();

    EXPECT_EQ(config.max_retries, 20);
    EXPECT_EQ(config.min_port, 12300);
    EXPECT_EQ(config.max_port, 14300);
}

TEST_F(ClientAutoPortConfigTest, ReadsValidValues) {
    source_.Set(kSetupRetries, "7");
    source_.Set(kMinPort, "12000");
    source_.Set(kMaxPort, "14000");

    const auto config = Load();

    EXPECT_EQ(config.max_retries, 7);
    EXPECT_EQ(config.min_port, 12000);
    EXPECT_EQ(config.max_port, 14000);
}

TEST_F(ClientAutoPortConfigTest, InvalidIntegersUseIndividualFieldDefaults) {
    for (const char* value : {"", "invalid", "2147483648"}) {
        source_.Set(kSetupRetries, value);
        source_.Set(kMinPort, value);
        source_.Set(kMaxPort, "15000");

        auto config = Load();

        EXPECT_EQ(config.max_retries, 20) << value;
        EXPECT_EQ(config.min_port, 12300) << value;
        EXPECT_EQ(config.max_port, 15000) << value;

        source_.Set(kMinPort, "13000");
        source_.Set(kMaxPort, value);

        config = Load();

        EXPECT_EQ(config.min_port, 13000) << value;
        EXPECT_EQ(config.max_port, 14300) << value;
    }
}

TEST_F(ClientAutoPortConfigTest, SupportsIndependentEndpointOverrides) {
    source_.Set(kMinPort, "13000");

    auto config = Load();
    EXPECT_EQ(config.min_port, 13000);
    EXPECT_EQ(config.max_port, 14300);

    source_.Set(kMinPort, "12300");
    source_.Set(kMaxPort, "15000");

    config = Load();
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
        source_.Set(kMinPort, value.min_port);
        source_.Set(kMaxPort, value.max_port);

        const auto config = Load();

        EXPECT_EQ(config.min_port, 12300);
        EXPECT_EQ(config.max_port, 14300);
    }
}

TEST_F(ClientAutoPortConfigTest, PreservesNonPositiveRetryCounts) {
    source_.Set(kSetupRetries, "0");
    EXPECT_EQ(Load().max_retries, 0);

    source_.Set(kSetupRetries, "-1");
    EXPECT_EQ(Load().max_retries, -1);
}

TEST_F(ClientAutoPortConfigTest, PreservesAcceptedIntegerSyntax) {
    source_.Set(kSetupRetries, " +7 ");
    source_.Set(kMinPort, " +12000 ");
    source_.Set(kMaxPort, " +14000 ");

    const auto config = Load();

    EXPECT_EQ(config.max_retries, 7);
    EXPECT_EQ(config.min_port, 12000);
    EXPECT_EQ(config.max_port, 14000);
}

}  // namespace
}  // namespace mooncake
