#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <limits>

#include "config/rpc_timeout_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class RpcTimeoutConfigTest : public ::testing::Test {
   protected:
    RpcTimeoutConfig Load() const {
        return RpcTimeoutConfig::FromEnvironment(Environ(source_));
    }

    static constexpr const char* names_[] = {"MC_RPC_TIMEOUT_MS",
                                             "MC_RPC_CONNECT_TIMEOUT_MS"};
    MapEnvironSource source_;
};

TEST_F(RpcTimeoutConfigTest, UnsetVariablesLeaveOverridesAbsent) {
    const auto config = Load();
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
            source_.Set(names_[i], entry.value);
            const auto config = Load();
            const auto& selected =
                i == 0 ? config.request_timeout : config.connect_timeout;
            const auto& other =
                i == 0 ? config.connect_timeout : config.request_timeout;
            ASSERT_TRUE(selected.has_value());
            EXPECT_EQ(*selected, std::chrono::milliseconds(entry.expected));
            EXPECT_FALSE(other.has_value());
        }
        source_.Unset(names_[i]);
    }
}

TEST_F(RpcTimeoutConfigTest, EachConstructionReadsCurrentEnvironment) {
    source_.Set("MC_RPC_TIMEOUT_MS", "1500");
    source_.Set("MC_RPC_CONNECT_TIMEOUT_MS", "1000");
    const auto original = Load();

    source_.Set("MC_RPC_TIMEOUT_MS", "0");
    source_.Set("MC_RPC_CONNECT_TIMEOUT_MS", "-1");
    const auto changed = Load();
    EXPECT_EQ(changed.request_timeout, std::chrono::milliseconds(0));
    EXPECT_EQ(changed.connect_timeout, std::chrono::milliseconds(-1));

    source_.Unset("MC_RPC_TIMEOUT_MS");
    source_.Unset("MC_RPC_CONNECT_TIMEOUT_MS");
    const auto cleared = Load();
    EXPECT_FALSE(cleared.request_timeout.has_value());
    EXPECT_FALSE(cleared.connect_timeout.has_value());
    EXPECT_EQ(original.request_timeout, std::chrono::milliseconds(1500));
    EXPECT_EQ(original.connect_timeout, std::chrono::milliseconds(1000));
}

}  // namespace
}  // namespace mooncake
