#include <gtest/gtest.h>

#include "control_plane/host_identity.h"
#include "device_comm/device_collective/protocols/ll/ll_peer_scope.h"

namespace mooncake {
namespace {

struct EndpointWithoutHostId {
    uint64_t region_address;
    uint64_t region_size;
    std::vector<RouteEndpoint> routes;
    uint64_t version;
};

struct SnapshotWithoutHostIds {
    std::vector<std::optional<EndpointWithoutHostId>> endpoints;
    std::vector<uint64_t> rank_epochs;
    std::vector<GlobalRank> participants;
    uint64_t version;
};

TEST(PgHostIdentityTest, ExplicitOverrideWins) {
    EXPECT_EQ(makePGHostId("node-a", "boot-a", "image-a"),
              makePGHostId("node-a", "boot-b", "image-b"));
    EXPECT_NE(makePGHostId("node-a", "boot-a", "image-a"),
              makePGHostId("node-b", "boot-a", "image-a"));
}

TEST(PgHostIdentityTest, ClonedMachineIdsDoNotIdentifyTheSameHost) {
    EXPECT_NE(makePGHostId({}, "boot-a", "shared-image"),
              makePGHostId({}, "boot-b", "shared-image"));
    EXPECT_TRUE(makePGHostId({}, "boot-a", {}).has_value());
    EXPECT_FALSE(makePGHostId({}, {}, "shared-image").has_value());
}

TEST(PgHostIdentityTest, UsesApplicationSpecificMachineId) {
    const auto identity = makePGHostId({}, "boot-a", "shared-image");
    ASSERT_TRUE(identity.has_value());
    EXPECT_EQ(
        *identity,
        "boot:boot-a:"
        "089b28785a4ffbd7d3271e2f36b27e5012ba70da47894ff81f3cbf999a12baac");
    EXPECT_NE(identity, makePGHostId({}, "boot-a", "another-image"));
    EXPECT_EQ(makePGHostId({}, "boot-a", {}), "boot:boot-a:");
}

TEST(PgHostIdentityTest, SnapshotsDecodeAcrossEndpointVersions) {
    const SnapshotWithoutHostIds before{
        .endpoints = {EndpointWithoutHostId{0x1000, 0x100, {}, 2},
                      std::nullopt},
        .rank_epochs = {3, 0},
        .participants = {0},
        .version = 5,
    };
    auto updated = struct_pack::deserialize<DeviceTransferSnapshot>(
        struct_pack::serialize(before));
    ASSERT_TRUE(updated.has_value());
    ASSERT_TRUE(updated->endpoints[0].has_value());
    EXPECT_FALSE(updated->endpoints[0]->host_id.has_value());
    EXPECT_FALSE(updated->endpoints[1].has_value());
    EXPECT_EQ(updated->endpoints[0]->version, 2);
    updated->endpoints[0]->host_id.emplace("override:node-a");
    const auto encoded = struct_pack::serialize(*updated);

    const auto same_version =
        struct_pack::deserialize<DeviceTransferSnapshot>(encoded);
    ASSERT_TRUE(same_version.has_value());
    EXPECT_EQ(*same_version, *updated);

    const auto previous_version =
        struct_pack::deserialize<SnapshotWithoutHostIds>(encoded);
    ASSERT_TRUE(previous_version.has_value());
    ASSERT_TRUE(previous_version->endpoints[0].has_value());
    EXPECT_FALSE(previous_version->endpoints[1].has_value());
    EXPECT_EQ(previous_version->endpoints[0]->region_address, 0x1000);
    EXPECT_EQ(previous_version->endpoints[0]->region_size, 0x100);
    EXPECT_EQ(previous_version->endpoints[0]->version, 2);
    EXPECT_EQ(previous_version->rank_epochs, before.rank_epochs);
    EXPECT_EQ(previous_version->participants, before.participants);
    EXPECT_EQ(previous_version->version, before.version);
}

TEST(PgLLPeerScopeTest, UsesInstalledGlobalRanksWithMatchingHostId) {
    DeviceTransferSnapshot snapshot;
    snapshot.endpoints.resize(kMaxNumRanks);
    snapshot.participants = {0, 8, 16, 32, 63};
    for (const auto rank : {0, 16, 24, 63}) {
        snapshot.endpoints[rank].emplace().host_id.emplace("override:node-a");
    }
    snapshot.endpoints[8].emplace().host_id.emplace("override:node-b");
    snapshot.endpoints[32].emplace();
    const auto& local = *snapshot.endpoints[0];
    const auto peers = localHostPeerMask(local, snapshot);

    EXPECT_TRUE(llIncludesPeer(peers, 0));
    EXPECT_TRUE(llIncludesPeer(peers, 16));
    EXPECT_TRUE(llIncludesPeer(peers, 63));
    EXPECT_FALSE(llIncludesPeer(peers, 8));
    EXPECT_FALSE(llIncludesPeer(peers, 24));
    EXPECT_FALSE(llIncludesPeer(peers, 32));
    EXPECT_FALSE(llIncludesPeer(peers, 64));
    EXPECT_EQ(localHostPeerMask(*snapshot.endpoints[32], snapshot), 0);
}

}  // namespace
}  // namespace mooncake
