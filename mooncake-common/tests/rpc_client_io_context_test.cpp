#include "rpc_client_io_context.h"

#include <async_simple/coro/SyncAwait.h>
#include <gtest/gtest.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <string>

namespace mooncake {
namespace {

class AddressSwitchService {
   public:
    explicit AddressSwitchService(int id) : id_(id) {}

    int Identify() const { return id_; }

   private:
    int id_;
};

struct FirstTestRpcClientIoContextPoolTag {};
struct SecondTestRpcClientIoContextPoolTag {};

coro_io::io_context_pool& GetFirstTestRpcClientIoContextPool() {
    return GetRpcClientIoContextPool<FirstTestRpcClientIoContextPoolTag>(
        "first test", 2);
}

coro_io::io_context_pool& GetSecondTestRpcClientIoContextPool() {
    return GetRpcClientIoContextPool<SecondTestRpcClientIoContextPoolTag>(
        "second test", 3);
}

TEST(RpcClientIoContextPoolTest, UsesConfiguredSizeAndReusesPool) {
    auto& first_pool = GetFirstTestRpcClientIoContextPool();
    auto& second_pool = GetSecondTestRpcClientIoContextPool();
    ASSERT_EQ(first_pool.pool_size(), 2U);
    ASSERT_EQ(second_pool.pool_size(), 3U);

    EXPECT_EQ(&GetFirstTestRpcClientIoContextPool(), &first_pool);
    EXPECT_EQ(&GetSecondTestRpcClientIoContextPool(), &second_pool);
    EXPECT_NE(&first_pool, &second_pool);
}

TEST(RpcClientIoContextPoolTest, ReplacesPoolWhenTargetChanges) {
    RpcClientPool pools(GetFirstTestRpcClientIoContextPool());

    auto first = pools.GetOrCreateClientPool("127.0.0.1:10001");
    std::weak_ptr<RpcClientPool::ClientPool> old_pool = first;
    EXPECT_EQ(pools.GetOrCreateClientPool("127.0.0.1:10001"), first);

    auto second = pools.GetOrCreateClientPool("127.0.0.1:10002");
    EXPECT_NE(first, second);
    first.reset();
    EXPECT_TRUE(old_pool.expired());
    EXPECT_EQ(pools.GetClientPool(), second);
}

TEST(RpcClientIoContextPoolTest, SendsToNewAddressAfterSwitch) {
    AddressSwitchService first_service(1);
    AddressSwitchService second_service(2);
    coro_rpc::coro_rpc_server first_server(1, 0, "127.0.0.1");
    coro_rpc::coro_rpc_server second_server(1, 0, "127.0.0.1");
    first_server.register_handler<&AddressSwitchService::Identify>(
        &first_service);
    second_server.register_handler<&AddressSwitchService::Identify>(
        &second_service);
    ASSERT_FALSE(first_server.async_start().hasResult());
    ASSERT_FALSE(second_server.async_start().hasResult());

    RpcClientPool pools(GetFirstTestRpcClientIoContextPool());

    const auto call = [&](uint16_t port) {
        auto pool =
            pools.GetOrCreateClientPool("127.0.0.1:" + std::to_string(port));
        return async_simple::coro::syncAwait(pool->send_request(
            [](coro_rpc::coro_rpc_client& client)
                -> async_simple::coro::Lazy<coro_rpc::rpc_result<int>> {
                co_return co_await client
                    .call<&AddressSwitchService::Identify>();
            }));
    };

    auto first_result = call(first_server.port());
    ASSERT_TRUE(first_result);
    ASSERT_TRUE(first_result.value());
    EXPECT_EQ(first_result.value().value(), 1);

    auto second_result = call(second_server.port());
    ASSERT_TRUE(second_result);
    ASSERT_TRUE(second_result.value());
    EXPECT_EQ(second_result.value().value(), 2);

    first_server.stop();
    second_server.stop();
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
