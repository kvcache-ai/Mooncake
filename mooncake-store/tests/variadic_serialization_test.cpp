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

// Frozen wire layout from main bf3b95a9 (#5646), before the strict allowlist.
// Keep this independent of ReplicateConfig so extension-version collisions
// cannot pass a same-type round trip unnoticed.
struct FabricReplicateConfigV5 {
    size_t replica_num{1};
    size_t nof_replica_num{0};
    size_t dfs_replica_num{0};
    SoftPinAction soft_pin_action{SoftPinAction::PRESERVE};
    std::optional<uint64_t> soft_pin_ttl_ms{};
    bool with_hard_pin{false};
    std::vector<std::string> preferred_segments{};
    std::string preferred_segment{};
    std::vector<std::string> preferred_nof_segments{};
    bool prefer_alloc_in_same_node{false};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    std::string host_id{};
    std::optional<std::vector<std::string>> group_ids{};
    struct_pack::compatible<std::vector<std::string>, 1> session_ids;
    struct_pack::compatible<std::vector<std::string>, 2> preferred_hca_names;
    struct_pack::compatible<std::vector<std::vector<std::string>>, 3>
        per_key_preferred_hca_names;
    struct_pack::compatible<std::vector<std::string>, 4> residency_affinity_ids;
    struct_pack::compatible<std::string, 5> requester_self_id;
    struct_pack::compatible<std::string, 5> fabric_domain;
    struct_pack::compatible<PhysicalPlacement, 5> requester_placement;
};

template <typename Config>
Config FabricConfig() {
    Config config;
    config.session_ids = std::vector<std::string>{"session-a"};
    config.preferred_hca_names = std::vector<std::string>{"mlx5_0"};
    config.per_key_preferred_hca_names =
        std::vector<std::vector<std::string>>{{"mlx5_1"}};
    config.residency_affinity_ids = std::vector<std::string>{"affinity-a"};
    config.requester_self_id = "rack1.tray1#g3";
    config.fabric_domain = "fabric-a";
    config.requester_placement =
        PhysicalPlacement{"fabric-a", "rack1", "tray1", 3, 0};
    return config;
}

template <typename Actual, typename Expected>
void ExpectFabricConfig(const Actual& actual, const Expected& expected) {
    EXPECT_EQ(actual.session_ids, expected.session_ids);
    EXPECT_EQ(actual.preferred_hca_names, expected.preferred_hca_names);
    EXPECT_EQ(actual.per_key_preferred_hca_names,
              expected.per_key_preferred_hca_names);
    EXPECT_EQ(actual.residency_affinity_ids, expected.residency_affinity_ids);
    EXPECT_EQ(actual.requester_self_id, expected.requester_self_id);
    EXPECT_EQ(actual.fabric_domain, expected.fabric_domain);
    EXPECT_EQ(actual.requester_placement, expected.requester_placement);
}

template <typename Destination, typename Source>
auto DecodeRpcConfig(const Source& config) {
    using RpcRequest =
        std::tuple<UUID, std::string, uint64_t, Destination, std::string>;
    auto wire =
        struct_pack::serialize(UUID{1, 2}, std::string("key"), uint64_t{4096},
                               config, std::string("default"));
    return struct_pack::deserialize<RpcRequest>(wire);
}

TEST(VariadicSerializationTest, FabricV5DecodesWithoutStrictAllowlist) {
    const auto old = FabricConfig<FabricReplicateConfigV5>();
    auto decoded = DecodeRpcConfig<ReplicateConfig>(old);
    ASSERT_TRUE(decoded);
    const auto& actual = std::get<3>(*decoded);
    ExpectFabricConfig(actual, old);
    EXPECT_FALSE(actual.allowed_memory_segments);
}

TEST(VariadicSerializationTest, StrictExtensionPreservesFabricV5Fields) {
    auto config = FabricConfig<ReplicateConfig>();
    for (bool strict : {false, true}) {
        SCOPED_TRACE(strict);
        if (strict) {
            config.allowed_memory_segments = std::vector<std::string>{"home"};
        }
        auto old = DecodeRpcConfig<FabricReplicateConfigV5>(config);
        ASSERT_TRUE(old);
        ExpectFabricConfig(std::get<3>(*old), config);
        auto current = DecodeRpcConfig<ReplicateConfig>(config);
        ASSERT_TRUE(current);
        ExpectFabricConfig(std::get<3>(*current), config);
        EXPECT_EQ(std::get<3>(*current).allowed_memory_segments,
                  config.allowed_memory_segments);
    }
}

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

}  // namespace mooncake
