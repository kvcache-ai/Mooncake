#include "environ.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <thread>

namespace mooncake {
namespace {

TEST(RpcClientIoThreadsTest, UsesConservativeDefault) {
    const unsigned expected =
        std::min(16U, std::max(1U, std::thread::hardware_concurrency()));
    const auto& env = Environ::Get();

    EXPECT_EQ(env.GetRpcClientIoThreads(), expected);
    EXPECT_EQ(env.GetStoreRpcClientIoThreads(), expected);
    EXPECT_EQ(env.GetTransferEngineRpcClientIoThreads(), expected);
}

TEST(RpcClientIoThreadsTest, InvalidCommonValueUsesConservativeDefault) {
    const unsigned expected =
        std::min(16U, std::max(1U, std::thread::hardware_concurrency()));
    const auto& env = Environ::Get();

    EXPECT_EQ(env.GetRpcClientIoThreads(), expected);
    EXPECT_EQ(env.GetStoreRpcClientIoThreads(), expected);
    EXPECT_EQ(env.GetTransferEngineRpcClientIoThreads(), expected);
}

TEST(RpcClientIoThreadsTest, UsesComponentOverridesAndFreezesValues) {
    const auto& env = Environ::Get();

    EXPECT_EQ(env.GetRpcClientIoThreads(), 8U);
    EXPECT_EQ(env.GetStoreRpcClientIoThreads(), 4U);
    EXPECT_EQ(env.GetTransferEngineRpcClientIoThreads(), 6U);

    ASSERT_EQ(setenv("MC_RPC_CLIENT_IO_THREADS", "12", 1), 0);
    ASSERT_EQ(setenv("MC_STORE_RPC_CLIENT_IO_THREADS", "10", 1), 0);
    ASSERT_EQ(setenv("MC_TE_RPC_CLIENT_IO_THREADS", "11", 1), 0);
    EXPECT_EQ(env.GetRpcClientIoThreads(), 8U);
    EXPECT_EQ(env.GetStoreRpcClientIoThreads(), 4U);
    EXPECT_EQ(env.GetTransferEngineRpcClientIoThreads(), 6U);
}

TEST(RpcClientIoThreadsTest, InvalidComponentValuesUseCommonFallback) {
    const auto& env = Environ::Get();

    EXPECT_EQ(env.GetRpcClientIoThreads(), 8U);
    EXPECT_EQ(env.GetStoreRpcClientIoThreads(), 8U);
    EXPECT_EQ(env.GetTransferEngineRpcClientIoThreads(), 8U);
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
