#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <string>

#include "config/rpc_timeout_config.h"

namespace mooncake {
namespace {

class RpcTimeoutConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        for (int i = 0; i < 2; ++i) {
            if (const char* value = std::getenv(names_[i])) {
                original_[i] = value;
            }
        }
        for (const char* name : names_) {
            ASSERT_EQ(unsetenv(name), 0);
        }
    }

    void TearDown() override {
        for (int i = 0; i < 2; ++i) {
            if (original_[i].has_value()) {
                EXPECT_EQ(setenv(names_[i], original_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(names_[i]), 0);
            }
        }
    }

    static constexpr const char* names_[] = {"MC_RPC_TIMEOUT_MS",
                                             "MC_RPC_CONNECT_TIMEOUT_MS"};
    std::optional<std::string> original_[2];
};

TEST_F(RpcTimeoutConfigTest, UnsetVariablesLeaveOverridesAbsent) {
    const auto config = RpcTimeoutConfig::FromEnvironment();
    EXPECT_FALSE(config.request_timeout.has_value());
    EXPECT_FALSE(config.connect_timeout.has_value());
}

TEST_F(RpcTimeoutConfigTest, PreservesLegacyConversionForEachVariable) {
    struct Case {
        const char* value;
        int64_t expected;
    };
    const Case cases[] = {
        {"", 0},
        {"garbage", 0},
        {"0", 0},
        {"1500", 1500},
        {"-1", -1},
        {" +42 ", 42},
        {" \t", 0},
        {"1500ms", 1500},
        {"0x10", 0},
        {"9223372036854775807", std::numeric_limits<int64_t>::max()},
        {"-9223372036854775808", std::numeric_limits<int64_t>::min()},
    };
    for (int i = 0; i < 2; ++i) {
        SCOPED_TRACE(names_[i]);
        for (const auto& entry : cases) {
            SCOPED_TRACE(entry.value);
            ASSERT_EQ(setenv(names_[i], entry.value, 1), 0);
            const auto config = RpcTimeoutConfig::FromEnvironment();
            const auto& selected =
                i == 0 ? config.request_timeout : config.connect_timeout;
            const auto& other =
                i == 0 ? config.connect_timeout : config.request_timeout;
            ASSERT_TRUE(selected.has_value());
            EXPECT_EQ(*selected, std::chrono::milliseconds(entry.expected));
            EXPECT_FALSE(other.has_value());
        }
        ASSERT_EQ(unsetenv(names_[i]), 0);
    }
}

TEST_F(RpcTimeoutConfigTest, EachConstructionReadsCurrentEnvironment) {
    ASSERT_EQ(setenv("MC_RPC_TIMEOUT_MS", "1500", 1), 0);
    ASSERT_EQ(setenv("MC_RPC_CONNECT_TIMEOUT_MS", "1000", 1), 0);
    const auto original = RpcTimeoutConfig::FromEnvironment();

    ASSERT_EQ(setenv("MC_RPC_TIMEOUT_MS", "0", 1), 0);
    ASSERT_EQ(setenv("MC_RPC_CONNECT_TIMEOUT_MS", "-1", 1), 0);
    const auto changed = RpcTimeoutConfig::FromEnvironment();
    EXPECT_EQ(changed.request_timeout, std::chrono::milliseconds(0));
    EXPECT_EQ(changed.connect_timeout, std::chrono::milliseconds(-1));

    ASSERT_EQ(unsetenv("MC_RPC_TIMEOUT_MS"), 0);
    ASSERT_EQ(unsetenv("MC_RPC_CONNECT_TIMEOUT_MS"), 0);
    const auto cleared = RpcTimeoutConfig::FromEnvironment();
    EXPECT_FALSE(cleared.request_timeout.has_value());
    EXPECT_FALSE(cleared.connect_timeout.has_value());
    EXPECT_EQ(original.request_timeout, std::chrono::milliseconds(1500));
    EXPECT_EQ(original.connect_timeout, std::chrono::milliseconds(1000));
}

}  // namespace
}  // namespace mooncake
