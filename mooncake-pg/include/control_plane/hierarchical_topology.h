#ifndef MOONCAKE_PG_CONTROL_PLANE_HIERARCHICAL_TOPOLOGY_H
#define MOONCAKE_PG_CONTROL_PLANE_HIERARCHICAL_TOPOLOGY_H

#include <algorithm>
#include <map>
#include <vector>

#include "control_plane/control_types.h"

namespace mooncake {

// Hierarchical AllReduce needs a complete static topology: at least two nodes,
// at least two ranks per node, and contiguous local ranks inside each node.
inline bool groupSupportsHierarchical(const GroupView& view) {
    std::map<uint32_t, std::vector<uint32_t>> nodes;
    for (const auto rank : view.rank_order) {
        if (rank < 0 || static_cast<size_t>(rank) >= view.members.size()) {
            return false;
        }
        const auto& member = view.members[rank];
        if (!member.isActive() || !member.endpoint) return false;
        const auto& device = member.endpoint->device_collective;
        if (!device.supportsAlgorithm(DeviceCollectiveAlgorithm::Hierarchical)) {
            return false;
        }
        const auto* endpoint = findProviderEndpoint(
            device.algorithms, kDeviceTopologyKey, kDeviceTopologyVersion);
        if (!endpoint) return false;
        auto topology = endpoint->decode<DeviceTopologyInfo>(
            kDeviceTopologyKey, kDeviceTopologyVersion);
        if (!topology.has_value()) return false;
        nodes[topology->node_id].push_back(topology->local_rank);
    }
    if (nodes.size() < 2) return false;
    for (auto& entry : nodes) {
        auto& local_ranks = entry.second;
        if (local_ranks.size() < 2) return false;
        std::sort(local_ranks.begin(), local_ranks.end());
        if (local_ranks.front() != 0 ||
            local_ranks.back() != local_ranks.size() - 1) {
            return false;
        }
        if (std::adjacent_find(local_ranks.begin(), local_ranks.end()) !=
            local_ranks.end()) {
            return false;
        }
    }
    return true;
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_HIERARCHICAL_TOPOLOGY_H
