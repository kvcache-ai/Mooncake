// Mooncake's Release flags intentionally keep assertions enabled. Force the
// NDEBUG path that disables YLT type literals in production RPC builds.
#ifndef NDEBUG
#define NDEBUG
#endif

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake {

TEST(VariadicSerializationTest, PreservesAllCompatibleFields) {
    using RpcRequest =
        std::tuple<UUID, std::string, uint64_t, ReplicateConfig, std::string>;
    const auto round_trip = [](const ReplicateConfig& config) {
        auto wire = struct_pack::serialize(UUID{1, 2}, std::string("key"),
                                           uint64_t{4096}, config,
                                           std::string("default"));
        return struct_pack::deserialize<RpcRequest>(wire);
    };

    ReplicateConfig config;
    config.session_ids = std::vector<std::string>{"session-a"};
    config.preferred_hca_names = std::vector<std::string>{"mlx5_0"};
    config.per_key_preferred_hca_names =
        std::vector<std::vector<std::string>>{{"mlx5_1"}};
    config.residency_affinity_ids = std::vector<std::string>{"affinity-a"};
    auto decoded = round_trip(config);
    ASSERT_TRUE(decoded);
    const auto& actual = std::get<3>(*decoded);
    EXPECT_EQ(actual.session_ids, config.session_ids);
    EXPECT_EQ(actual.preferred_hca_names, config.preferred_hca_names);
    EXPECT_EQ(actual.per_key_preferred_hca_names,
              config.per_key_preferred_hca_names);
    EXPECT_EQ(actual.residency_affinity_ids, config.residency_affinity_ids);

    ReplicateConfig empty;
    empty.residency_affinity_ids = std::vector<std::string>{};
    auto empty_decoded = round_trip(empty);
    ASSERT_TRUE(empty_decoded);
    const auto& empty_actual = std::get<3>(*empty_decoded);
    ASSERT_TRUE(empty_actual.residency_affinity_ids);
    EXPECT_TRUE(empty_actual.residency_affinity_ids->empty());
}

// A PutStart names the part in the two compatible fields that follow every
// other argument; they are what a release master acts on.
TEST(VariadicSerializationTest, PreservesPartFields) {
    using RpcRequest =
        std::tuple<UUID, std::string, uint64_t, ReplicateConfig, std::string,
                   struct_pack::compatible<uint32_t>,
                   struct_pack::compatible<uint32_t>>;
    ReplicateConfig config;
    config.session_ids = std::vector<std::string>{"session-a"};
    const auto wire = struct_pack::serialize(
        UUID{1, 2}, std::string("key"), uint64_t{4096}, config,
        std::string("default"), struct_pack::compatible<uint32_t>{3},
        struct_pack::compatible<uint32_t>{16});
    auto decoded = struct_pack::deserialize<RpcRequest>(wire);
    ASSERT_TRUE(decoded);
    ASSERT_TRUE(std::get<5>(*decoded).has_value());
    EXPECT_EQ(*std::get<5>(*decoded), 3u);
    ASSERT_TRUE(std::get<6>(*decoded).has_value());
    EXPECT_EQ(*std::get<6>(*decoded), 16u);

    // A call that names no part still carries the fields, unset.
    const auto bare_wire = struct_pack::serialize(
        UUID{1, 2}, std::string("key"), uint64_t{4096}, ReplicateConfig{},
        std::string("default"), struct_pack::compatible<uint32_t>{},
        struct_pack::compatible<uint32_t>{});
    auto bare = struct_pack::deserialize<RpcRequest>(bare_wire);
    ASSERT_TRUE(bare);
    EXPECT_FALSE(std::get<5>(*bare).has_value());
    EXPECT_FALSE(std::get<6>(*bare).has_value());
}

}  // namespace mooncake
