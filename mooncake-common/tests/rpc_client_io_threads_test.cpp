#include "environ.h"

#include <gtest/gtest.h>

#include <initializer_list>
#include <string>
#include <unordered_map>
#include <utility>

namespace mooncake {
namespace {

class MockEnvironSource : public EnvironSource {
   public:
    MockEnvironSource(
        std::initializer_list<std::pair<const std::string, std::string>> values)
        : values_(values) {}

    const char* Get(const char* name) const override {
        const auto it = values_.find(name);
        return it == values_.end() ? nullptr : it->second.c_str();
    }

    void Set(std::string name, std::string value) {
        values_[std::move(name)] = std::move(value);
    }

   private:
    std::unordered_map<std::string, std::string> values_;
};

TEST(RpcClientIoThreadsTest, UsesComponentOverridesAndFreezesValues) {
    MockEnvironSource source{{"MC_RPC_CLIENT_IO_THREADS", "8"},
                             {"MC_STORE_RPC_CLIENT_IO_THREADS", "4"},
                             {"MC_TE_RPC_CLIENT_IO_THREADS", "6"}};
    const Environ env(source);

    EXPECT_EQ(env.GetRpcClientIoThreads(), 8U);
    EXPECT_EQ(env.GetStoreRpcClientIoThreads(), 4U);
    EXPECT_EQ(env.GetTransferEngineRpcClientIoThreads(), 6U);

    source.Set("MC_RPC_CLIENT_IO_THREADS", "12");
    source.Set("MC_STORE_RPC_CLIENT_IO_THREADS", "10");
    source.Set("MC_TE_RPC_CLIENT_IO_THREADS", "11");
    EXPECT_EQ(env.GetRpcClientIoThreads(), 8U);
    EXPECT_EQ(env.GetStoreRpcClientIoThreads(), 4U);
    EXPECT_EQ(env.GetTransferEngineRpcClientIoThreads(), 6U);
}

TEST(RpcClientIoThreadsTest, ComponentValuesUseCommonFallback) {
    const MockEnvironSource source{{"MC_RPC_CLIENT_IO_THREADS", "8"},
                                   {"MC_STORE_RPC_CLIENT_IO_THREADS", "0"},
                                   {"MC_TE_RPC_CLIENT_IO_THREADS", "invalid"}};
    const Environ env(source);

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
