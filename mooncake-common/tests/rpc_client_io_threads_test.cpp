#include "rpc_client_io_context.h"

#include <gtest/gtest.h>

#include "environ.h"

namespace mooncake {
namespace {

TEST(RpcClientIoThreadsTest, UsesComponentOverrides) {
    const MapEnvironSource source{{"MC_RPC_CLIENT_IO_THREADS", "8"},
                                  {"MC_STORE_RPC_CLIENT_IO_THREADS", "4"},
                                  {"MC_TE_RPC_CLIENT_IO_THREADS", "6"}};
    const auto config =
        RpcClientIoThreadsConfig::FromEnvironment(Environ(source));

    EXPECT_EQ(config.common, 8U);
    EXPECT_EQ(config.store, 4U);
    EXPECT_EQ(config.transfer_engine, 6U);
}

TEST(RpcClientIoThreadsTest, ComponentValuesUseCommonFallback) {
    const MapEnvironSource source{{"MC_RPC_CLIENT_IO_THREADS", "8"},
                                  {"MC_STORE_RPC_CLIENT_IO_THREADS", "0"},
                                  {"MC_TE_RPC_CLIENT_IO_THREADS", "invalid"}};
    const auto config =
        RpcClientIoThreadsConfig::FromEnvironment(Environ(source));

    EXPECT_EQ(config.common, 8U);
    EXPECT_EQ(config.store, 8U);
    EXPECT_EQ(config.transfer_engine, 8U);
}

TEST(RpcClientIoThreadsTest, DefaultsToBoundedHardwareConcurrency) {
    const MapEnvironSource source;
    const auto config =
        RpcClientIoThreadsConfig::FromEnvironment(Environ(source));

    EXPECT_GE(config.common, 1U);
    EXPECT_LE(config.common, 16U);
    EXPECT_EQ(config.store, config.common);
    EXPECT_EQ(config.transfer_engine, config.common);
}

TEST(RpcClientIoThreadsTest, ProcessValuesAreResolvedOnce) {
    const auto& first = RpcClientIoThreadsConfig::Process();
    EXPECT_EQ(&first, &RpcClientIoThreadsConfig::Process());
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
