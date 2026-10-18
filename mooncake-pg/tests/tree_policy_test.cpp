#include "control_plane/collective_algorithm_policy.h"

#include <gtest/gtest.h>
#include <ylt/struct_pack.hpp>

namespace mooncake {
namespace {

// Exact #4372 shapes, including the nested endpoint and aggregate fields.
struct OldDeviceGroupEndpoint {
    uint64_t view_epoch_signal = 0;
    uint32_t view_epoch_signal_count = 0;
    std::optional<SimpleEndpoint> simple;
    std::optional<LLEndpoint> ll;
    std::optional<OneShotEndpoint> one_shot;
};
struct OldGroupEndpointInfo {
    uint64_t endpoint_epoch = 0;
    uint64_t send_buffer[2] = {};
    uint64_t recv_buffer[2] = {};
    uint64_t send_sync[2] = {};
    uint64_t recv_sync[2] = {};
    uint64_t p2p_credit_region = 0;
    uint64_t p2p_ack_region = 0;
    OldDeviceGroupEndpoint device_collective;
};

TEST(TreePolicyTest, ParsesAllOverridesAndRejectsInvalidValues) {
    auto automatic = parseCollectiveAlgorithm("auto");
    ASSERT_TRUE(automatic.has_value());
    EXPECT_FALSE(automatic->has_value());
    for (const auto& [text, algorithm] :
         {std::pair{"ring", DeviceAllReduceAlgorithm::Ring},
          std::pair{"oneshot", DeviceAllReduceAlgorithm::OneShot},
          std::pair{"tree", DeviceAllReduceAlgorithm::Tree}}) {
        const auto result = parseCollectiveAlgorithm(text);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, algorithm);
    }
    for (const auto value : {"", "Tree", "tree ", "ll", "invalid"}) {
        const auto result = parseCollectiveAlgorithm(value);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code, PGErrorCode::InvalidArgument);
    }
}

TEST(TreePolicyTest, CapabilityRequiresMatchingVersionAndSimpleEndpoint) {
    DeviceGroupEndpoint endpoint;
    endpoint.tree.emplace();
    EXPECT_FALSE(endpoint.supportsTree());
    endpoint.simple.emplace(SimpleEndpoint{16, 32});
    EXPECT_FALSE(endpoint.supportsTree());
    endpoint.view_epoch_signal_count = 4;
    EXPECT_TRUE(endpoint.supportsTree());
    endpoint.tree->version = 2;
    EXPECT_FALSE(endpoint.supportsTree());
    endpoint.tree.reset();
    EXPECT_FALSE(endpoint.supportsTree());
    EXPECT_TRUE(endpoint.supportsBackend(GpuCollectiveBackend::New));
}

TEST(TreePolicyTest, OldEndpointDecodesWithoutTreeAndNewEndpointDecodesAsOld) {
    OldDeviceGroupEndpoint old{128, 4, SimpleEndpoint{256, 640},
                               LLEndpoint{8192, 256, {}, {}},
                               OneShotEndpoint{65536, 1048576}};
    const auto decoded = struct_pack::deserialize<DeviceGroupEndpoint>(
        struct_pack::serialize(old));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->view_epoch_signal, old.view_epoch_signal);
    EXPECT_EQ(decoded->simple, old.simple);
    EXPECT_EQ(decoded->ll, old.ll);
    EXPECT_EQ(decoded->one_shot, old.one_shot);
    EXPECT_FALSE(decoded->tree.has_value());
    auto modern = *decoded;
    modern.tree.emplace();
    const auto roundtrip = struct_pack::deserialize<DeviceGroupEndpoint>(
        struct_pack::serialize(modern));
    ASSERT_TRUE(roundtrip.has_value());
    EXPECT_EQ(*roundtrip, modern);
    const auto old_reader = struct_pack::deserialize<OldDeviceGroupEndpoint>(
        struct_pack::serialize(modern));
    ASSERT_TRUE(old_reader.has_value());
    EXPECT_EQ(old_reader->simple, old.simple);
    EXPECT_EQ(old_reader->ll, old.ll);
    EXPECT_EQ(old_reader->one_shot, old.one_shot);
}

TEST(TreePolicyTest, NestedEndpointAndChoiceRetainWireCompatibility) {
    OldGroupEndpointInfo old;
    old.endpoint_epoch = 17;
    old.send_buffer[1] = 1024;
    old.device_collective.view_epoch_signal = 128;
    old.device_collective.view_epoch_signal_count = 4;
    old.device_collective.simple.emplace(SimpleEndpoint{256, 640});
    auto modern = struct_pack::deserialize<GroupEndpointInfo>(
        struct_pack::serialize(old));
    ASSERT_TRUE(modern.has_value());
    EXPECT_EQ(modern->endpoint_epoch, 17u);
    EXPECT_EQ(modern->send_buffer[1], 1024u);
    EXPECT_FALSE(modern->device_collective.tree);
    modern->device_collective.tree.emplace();
    const auto old_reader = struct_pack::deserialize<OldGroupEndpointInfo>(
        struct_pack::serialize(*modern));
    ASSERT_TRUE(old_reader.has_value());
    EXPECT_EQ(old_reader->device_collective.simple,
              old.device_collective.simple);
    const std::vector<DeviceAllReduceAlgorithmChoice> choices = {
        {UINT64_MAX, DeviceAllReduceAlgorithm::Tree}};
    const auto decoded = struct_pack::deserialize<decltype(choices)>(
        struct_pack::serialize(choices));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, choices);
}

}  // namespace
}  // namespace mooncake
