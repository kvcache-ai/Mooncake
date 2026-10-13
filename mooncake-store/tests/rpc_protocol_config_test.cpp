#include <gtest/gtest.h>

#include "config/rpc_protocol_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class RpcProtocolConfigTest : public ::testing::Test {
   protected:
    RpcProtocolConfig Load() const {
        return RpcProtocolConfig::FromEnvironment(Environ(source_));
    }

    static constexpr const char* kName = "MC_RPC_PROTOCOL";
    MapEnvironSource source_;
};

TEST_F(RpcProtocolConfigTest, OnlyExactRdmaTokenEnablesRdma) {
    EXPECT_FALSE(Load().use_rdma);

    struct Case {
        const char* value;
        bool expected;
    };
    const Case cases[] = {{"", false},      {"rdma", true},   {"RDMA", false},
                          {" rdma", false}, {"rdma ", false}, {"tcp", false},
                          {"1", false}};
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        source_.Set(kName, entry.value);
        EXPECT_EQ(Load().use_rdma, entry.expected);
    }
}

TEST_F(RpcProtocolConfigTest, EachConstructionReadsCurrentEnvironment) {
    const auto unset = Load();

    source_.Set(kName, "rdma");
    const auto enabled = Load();

    source_.Set(kName, "tcp");
    const auto disabled = Load();

    EXPECT_FALSE(unset.use_rdma);
    EXPECT_TRUE(enabled.use_rdma);
    EXPECT_FALSE(disabled.use_rdma);
}

}  // namespace
}  // namespace mooncake
