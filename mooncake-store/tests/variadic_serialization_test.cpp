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
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

TEST(VariadicSerializationTest, RoundTripsUnifiedApiTypesInNdebug) {
    PutStartItem range_item{
        .key = "k/range",
        .size = 4096,
        .target =
            RangeTarget{
                .offset = 4096, .total_size = 8192, .write_id = "run-1"},
        .group_id = "grp-1",
        .session_id = "sess-1",
        .residency_affinity_id = "aff-1",
        .preferred_hca_names = {"mlx5_0"},
    };
    auto range_wire = struct_pack::serialize(range_item);
    auto range_decoded = struct_pack::deserialize<PutStartItem>(range_wire);
    ASSERT_TRUE(range_decoded);
    EXPECT_EQ(range_decoded->key, "k/range");
    EXPECT_EQ(range_decoded->size, 4096u);
    EXPECT_EQ(range_decoded->group_id, "grp-1");
    EXPECT_EQ(range_decoded->session_id, "sess-1");
    EXPECT_EQ(range_decoded->residency_affinity_id, "aff-1");
    EXPECT_EQ(range_decoded->preferred_hca_names,
              (std::vector<std::string>{"mlx5_0"}));
    ASSERT_TRUE(std::holds_alternative<RangeTarget>(range_decoded->target));
    const auto& rt = std::get<RangeTarget>(range_decoded->target);
    EXPECT_EQ(rt.offset, 4096u);
    EXPECT_EQ(rt.total_size, 8192u);
    EXPECT_EQ(rt.write_id, "run-1");

    PutStartItem whole_item{
        .key = "k/whole",
        .size = 1024,
        .target = WholeObject{},
    };
    auto whole_wire = struct_pack::serialize(whole_item);
    auto whole_decoded = struct_pack::deserialize<PutStartItem>(whole_wire);
    ASSERT_TRUE(whole_decoded);
    EXPECT_TRUE(std::holds_alternative<WholeObject>(whole_decoded->target));

    PutFinishRequest finish_req{
        .token =
            WriteToken{.key = "k/range", .target = rt, .replica_ids = {11, 22}},
        .commit = true,
        .replica_type = ReplicaType::MEMORY,
        .checksum = std::nullopt,
    };
    auto finish_wire = struct_pack::serialize(finish_req);
    auto finish_decoded =
        struct_pack::deserialize<PutFinishRequest>(finish_wire);
    ASSERT_TRUE(finish_decoded);
    EXPECT_EQ(finish_decoded->token.key, "k/range");
    EXPECT_EQ(finish_decoded->token.replica_ids,
              (std::vector<ReplicaID>{11, 22}));
    EXPECT_TRUE(finish_decoded->commit);

    std::vector<GetReplicaRequest> get_reqs{
        {.key = "k/whole", .target = WholeObject{}},
        {.key = "k/part", .target = PartIndex{3}},
    };
    auto get_wire = struct_pack::serialize(get_reqs);
    auto get_decoded =
        struct_pack::deserialize<std::vector<GetReplicaRequest>>(get_wire);
    ASSERT_TRUE(get_decoded);
    ASSERT_EQ(get_decoded->size(), 2u);
    EXPECT_TRUE(std::holds_alternative<WholeObject>((*get_decoded)[0].target));
    ASSERT_TRUE(std::holds_alternative<PartIndex>((*get_decoded)[1].target));
    EXPECT_EQ(std::get<PartIndex>((*get_decoded)[1].target).index, 3u);

    GetReplicaResponse get_resp{
        .parts =
            {
                PartReplicaInfo{
                    .replicas = {}, .length = 4096, .checksum = 0x1111ULL},
                PartReplicaInfo{
                    .replicas = {}, .length = 8192, .checksum = 0x2222ULL},
            },
        .lease_ttl_ms = 5000,
        .part_count = 2,
        .resolved_tenant_id = "default",
    };
    auto resp_wire = struct_pack::serialize(get_resp);
    auto resp_decoded = struct_pack::deserialize<GetReplicaResponse>(resp_wire);
    ASSERT_TRUE(resp_decoded);
    EXPECT_EQ(resp_decoded->part_count, 2u);
    EXPECT_EQ(resp_decoded->lease_ttl_ms, 5000u);
    EXPECT_EQ(resp_decoded->resolved_tenant_id, "default");
    ASSERT_EQ(resp_decoded->parts.size(), 2u);
    EXPECT_EQ(resp_decoded->parts[0].length, 4096u);
    EXPECT_EQ(resp_decoded->parts[0].checksum, 0x1111ULL);
    EXPECT_EQ(resp_decoded->parts[1].length, 8192u);
    EXPECT_EQ(resp_decoded->parts[1].checksum, 0x2222ULL);
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
