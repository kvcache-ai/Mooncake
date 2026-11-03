#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "config/rpc_protocol_config.h"

namespace mooncake {
namespace {

class RpcProtocolConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv(kName)) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv(kName), 0);
    }

    void TearDown() override {
        if (original_.has_value()) {
            EXPECT_EQ(setenv(kName, original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(kName), 0);
        }
    }

    static constexpr const char* kName = "MC_RPC_PROTOCOL";
    std::optional<std::string> original_;
};

TEST_F(RpcProtocolConfigTest, OnlyExactRdmaTokenEnablesRdma) {
    EXPECT_FALSE(RpcProtocolConfig::FromEnvironment().use_rdma);

    struct Case {
        const char* value;
        bool expected;
    };
    const Case cases[] = {{"", false},      {"rdma", true},   {"RDMA", false},
                          {" rdma", false}, {"rdma ", false}, {"tcp", false},
                          {"1", false}};
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        ASSERT_EQ(setenv(kName, entry.value, 1), 0);
        EXPECT_EQ(RpcProtocolConfig::FromEnvironment().use_rdma,
                  entry.expected);
    }
}

TEST_F(RpcProtocolConfigTest, EachConstructionReadsCurrentEnvironment) {
    const auto unset = RpcProtocolConfig::FromEnvironment();

    ASSERT_EQ(setenv(kName, "rdma", 1), 0);
    const auto enabled = RpcProtocolConfig::FromEnvironment();

    ASSERT_EQ(setenv(kName, "tcp", 1), 0);
    const auto disabled = RpcProtocolConfig::FromEnvironment();

    EXPECT_FALSE(unset.use_rdma);
    EXPECT_TRUE(enabled.use_rdma);
    EXPECT_FALSE(disabled.use_rdma);
}

}  // namespace
}  // namespace mooncake
