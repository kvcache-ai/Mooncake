#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <ylt/struct_pack.hpp>

#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_rpc_service.h"

namespace mooncake {
namespace {

// Pin retained native RPC method IDs across internal refactors.
static_assert(coro_rpc::func_id<&P2PMasterRpcService::RegisterClient>() ==
              554672570u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::UnregisterClient>() ==
              3195950100u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::Heartbeat>() ==
              1220160476u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::QueryClientStatus>() ==
              2240222209u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::MountSegment>() ==
              1836414514u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::UnmountSegment>() ==
              1378916896u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::GetReadRoute>() ==
              4226816141u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::BatchGetReadRoute>() ==
              1029360511u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::GetWriteRoute>() ==
              3979753808u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::BatchGetWriteRoute>() ==
              3935710653u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::PublishRoute>() ==
              4114711953u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::WithdrawRoute>() ==
              1498064915u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::BatchWithdrawRoute>() ==
              1805314735u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::BatchSyncRoutes>() ==
              3376879306u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::CompleteRouteSync>() ==
              1587901567u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::ServiceReady>() ==
              1460324940u);
static_assert(
    coro_rpc::func_id<&P2PMasterRpcService::HeartbeatServiceReady>() ==
    1980024971u);

static_assert(
    std::is_same_v<decltype(P2PGetReadRouteRequest::key), std::string_view>);
static_assert(std::is_same_v<decltype(P2PBatchGetReadRouteRequest::keys),
                             std::vector<std::string_view>>);
static_assert(
    std::is_same_v<decltype(P2PGetWriteRouteRequest::key), std::string_view>);
static_assert(std::is_same_v<decltype(P2PBatchGetWriteRouteRequest::keys),
                             std::vector<std::string_view>>);
static_assert(
    std::is_same_v<decltype(P2PPublishRouteRequest::key), std::string_view>);
static_assert(
    std::is_same_v<decltype(P2PWithdrawRouteRequest::key), std::string_view>);
static_assert(std::is_same_v<decltype(P2PBatchWithdrawRouteRequest::key),
                             std::string_view>);
static_assert(
    std::is_same_v<decltype(P2PPublishRouteOperation::key), std::string_view>);
static_assert(
    std::is_same_v<decltype(P2PWithdrawRouteOperation::key), std::string_view>);

TEST(P2PRpcTypesTest, BatchRouteKeysDeserializeAsBufferViews) {
    const std::string publish_key = "publish-key";
    const std::string withdraw_key = "withdraw-key";
    P2PBatchSyncRoutesRequest request;
    request.client_id = {1, 2};
    request.publish_operations = {{
        .key = publish_key,
        .object_size = 1024,
        .segment_id = {3, 4},
    }};
    request.withdraw_operations = {{
        .key = withdraw_key,
        .segment_id = {5, 6},
    }};

    auto buffer = struct_pack::serialize(request);
    P2PBatchSyncRoutesRequest decoded;
    ASSERT_FALSE(struct_pack::deserialize_to(decoded, buffer));
    ASSERT_EQ(decoded.publish_operations.size(), 1);
    ASSERT_EQ(decoded.withdraw_operations.size(), 1);
    EXPECT_EQ(decoded.publish_operations[0].key, publish_key);
    EXPECT_EQ(decoded.withdraw_operations[0].key, withdraw_key);

    const auto buffer_begin = reinterpret_cast<uintptr_t>(buffer.data());
    const auto buffer_end = buffer_begin + buffer.size();
    const auto is_buffer_view = [&](std::string_view value) {
        const auto view_begin = reinterpret_cast<uintptr_t>(value.data());
        return view_begin >= buffer_begin &&
               view_begin + value.size() <= buffer_end;
    };
    EXPECT_TRUE(is_buffer_view(decoded.publish_operations[0].key));
    EXPECT_TRUE(is_buffer_view(decoded.withdraw_operations[0].key));
}

}  // namespace
}  // namespace mooncake
