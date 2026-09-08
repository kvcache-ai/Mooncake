#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <ylt/struct_pack.hpp>

#include "p2p/common/p2p_rpc_types.h"

namespace mooncake {
namespace {

static_assert(std::is_same_v<decltype(P2PGetReadRouteRequest::key),
                             std::string_view>);
static_assert(std::is_same_v<decltype(P2PBatchGetReadRouteRequest::keys),
                             std::vector<std::string_view>>);
static_assert(std::is_same_v<decltype(P2PGetWriteRouteRequest::key),
                             std::string_view>);
static_assert(std::is_same_v<decltype(P2PBatchGetWriteRouteRequest::keys),
                             std::vector<std::string_view>>);
static_assert(std::is_same_v<decltype(P2PPublishRouteRequest::key),
                             std::string_view>);
static_assert(std::is_same_v<decltype(P2PWithdrawRouteRequest::key),
                             std::string_view>);
static_assert(std::is_same_v<decltype(P2PBatchWithdrawRouteRequest::key),
                             std::string_view>);
static_assert(std::is_same_v<decltype(P2PPublishRouteOperation::key),
                             std::string_view>);
static_assert(std::is_same_v<decltype(P2PWithdrawRouteOperation::key),
                             std::string_view>);

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
