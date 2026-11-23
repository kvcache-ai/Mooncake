#include <algorithm>
#include <cstdint>
#include <numeric>
#include <vector>

#include <gtest/gtest.h>

#include "control_plane/hierarchical_topology.h"

namespace mooncake {
namespace {

GroupMember makeMember(uint32_t node_id, uint32_t local_rank) {
    GroupEndpointInfo endpoint;
    endpoint.device_collective.supported_algorithms.push_back(
        DeviceCollectiveAlgorithm::Hierarchical);
    endpoint.device_collective.algorithms.push_back(AlgorithmEndpoint::encode(
        kDeviceTopologyKey, kDeviceTopologyVersion,
        DeviceTopologyInfo{node_id, local_rank}));

    GroupMember member;
    member.status = GroupMemberState::Active;
    member.endpoint = endpoint;
    return member;
}

GroupView makeGroup(const std::vector<DeviceTopologyInfo>& topology) {
    GroupView view;
    view.rank_order.resize(topology.size());
    std::iota(view.rank_order.begin(), view.rank_order.end(), 0);
    view.members.resize(topology.size());
    for (size_t rank = 0; rank < topology.size(); ++rank) {
        view.members[rank] = makeMember(topology[rank].node_id,
                                        topology[rank].local_rank);
    }
    return view;
}

TEST(PgHierarchicalTopologyTest, AcceptsCompleteTwoNodeTopology) {
    const auto view = makeGroup({{0, 0}, {0, 1}, {1, 0}, {1, 1}});
    EXPECT_TRUE(groupSupportsHierarchical(view));
}

TEST(PgHierarchicalTopologyTest, RejectsSingleNodeTopology) {
    const auto view = makeGroup({{0, 0}, {0, 1}, {0, 2}, {0, 3}});
    EXPECT_FALSE(groupSupportsHierarchical(view));
}

TEST(PgHierarchicalTopologyTest, RejectsMissingTopologyEndpoint) {
    auto view = makeGroup({{0, 0}, {0, 1}, {1, 0}, {1, 1}});
    view.members[2].endpoint->device_collective.algorithms.clear();
    EXPECT_FALSE(groupSupportsHierarchical(view));
}

TEST(PgHierarchicalTopologyTest, RejectsNonContiguousLocalRanks) {
    const auto view = makeGroup({{0, 0}, {0, 2}, {1, 0}, {1, 1}});
    EXPECT_FALSE(groupSupportsHierarchical(view));
}

}  // namespace
}  // namespace mooncake
