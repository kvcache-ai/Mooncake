#include <gtest/gtest.h>

#include <limits>
#include <string>

#include "../src/config/client_numa_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class ClientNumaConfigTest : public ::testing::Test {
   protected:
    ClientNumaConfig Load() const {
        return ClientNumaConfig::FromEnvironment(Environ(source_));
    }

    void Set(const char* value) { source_.Set(kEnvironmentVariable, value); }

    static constexpr const char* kEnvironmentVariable =
        "MC_STORE_NUMA_SOCKET_ID";

   private:
    MapEnvironSource source_;
};

TEST_F(ClientNumaConfigTest, UnsetAndEmptyUseAutoDetectionWithoutWarning) {
    ::testing::internal::CaptureStderr();
    EXPECT_FALSE(Load().socket_id.has_value());
    EXPECT_TRUE(::testing::internal::GetCapturedStderr().empty());

    Set("");
    ::testing::internal::CaptureStderr();
    EXPECT_FALSE(Load().socket_id.has_value());
    EXPECT_TRUE(::testing::internal::GetCapturedStderr().empty());
}

TEST_F(ClientNumaConfigTest, PreservesAcceptedDecimalSyntax) {
    struct Case {
        const char* value;
        int expected;
    };
    const Case cases[] = {{"0", 0},
                          {"7", 7},
                          {"+7", 7},
                          {" \t7", 7},
                          {"2147483647", std::numeric_limits<int>::max()}};

    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        Set(entry.value);
        EXPECT_EQ(Load().socket_id, entry.expected);
    }
}

TEST_F(ClientNumaConfigTest, InvalidValuesWarnAndUseAutoDetection) {
    for (const char* value :
         {"-1", "7suffix", "7 ", "2147483648", "999999999999999999999999"}) {
        SCOPED_TRACE(value);
        Set(value);
        ::testing::internal::CaptureStderr();
        EXPECT_FALSE(Load().socket_id.has_value());
        const std::string logs = ::testing::internal::GetCapturedStderr();
        EXPECT_NE(logs.find(std::string("Invalid MC_STORE_NUMA_SOCKET_ID=") +
                            value + ", falling back to auto-detect"),
                  std::string::npos);
    }
}

TEST_F(ClientNumaConfigTest, EachConfigReadsCurrentEnvironment) {
    Set("1");
    const auto first = Load();

    Set("2");
    const auto second = Load();

    EXPECT_EQ(first.socket_id, 1);
    EXPECT_EQ(second.socket_id, 2);
}

}  // namespace
}  // namespace mooncake
