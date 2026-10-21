#include <gtest/gtest.h>

#include <cstdlib>
#include <limits>
#include <optional>
#include <string>

#include "../src/config/client_numa_config.h"

namespace mooncake {
namespace {

class ClientNumaConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv(kEnvironmentVariable)) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv(kEnvironmentVariable), 0);
    }

    void TearDown() override {
        if (original_.has_value()) {
            EXPECT_EQ(setenv(kEnvironmentVariable, original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(kEnvironmentVariable), 0);
        }
    }

    void Set(const char* value) {
        ASSERT_EQ(setenv(kEnvironmentVariable, value, 1), 0);
    }

    static constexpr const char* kEnvironmentVariable =
        "MC_STORE_NUMA_SOCKET_ID";

   private:
    std::optional<std::string> original_;
};

TEST_F(ClientNumaConfigTest, UnsetAndEmptyUseAutoDetectionWithoutWarning) {
    ::testing::internal::CaptureStderr();
    EXPECT_FALSE(ClientNumaConfig::FromEnvironment().socket_id.has_value());
    EXPECT_TRUE(::testing::internal::GetCapturedStderr().empty());

    Set("");
    ::testing::internal::CaptureStderr();
    EXPECT_FALSE(ClientNumaConfig::FromEnvironment().socket_id.has_value());
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
        EXPECT_EQ(ClientNumaConfig::FromEnvironment().socket_id,
                  entry.expected);
    }
}

TEST_F(ClientNumaConfigTest, InvalidValuesWarnAndUseAutoDetection) {
    for (const char* value :
         {"-1", "7suffix", "7 ", "2147483648", "999999999999999999999999"}) {
        SCOPED_TRACE(value);
        Set(value);
        ::testing::internal::CaptureStderr();
        EXPECT_FALSE(ClientNumaConfig::FromEnvironment().socket_id.has_value());
        const std::string logs = ::testing::internal::GetCapturedStderr();
        EXPECT_NE(logs.find(std::string("Invalid MC_STORE_NUMA_SOCKET_ID=") +
                            value + ", falling back to auto-detect"),
                  std::string::npos);
    }
}

TEST_F(ClientNumaConfigTest, EachConfigReadsCurrentEnvironment) {
    Set("1");
    const auto first = ClientNumaConfig::FromEnvironment();

    Set("2");
    const auto second = ClientNumaConfig::FromEnvironment();

    EXPECT_EQ(first.socket_id, 1);
    EXPECT_EQ(second.socket_id, 2);
}

}  // namespace
}  // namespace mooncake
