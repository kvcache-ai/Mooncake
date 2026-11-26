#pragma once

#include <algorithm>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "control_plane/control_types.h"

namespace mooncake {

// Indices refer to active_ranks. The first active rank on each host leads.
struct HierarchicalOneShotRingTopology {
    std::vector<std::vector<size_t>> nodes;
};

inline std::optional<HierarchicalOneShotRingTopology>
planHierarchicalOneShotRing(
    std::span<const GlobalRank> active_ranks,
    const DeviceTransferSnapshot& snapshot,
    std::span<const std::vector<DeviceTransferRouteInfo>> routes) {
    HierarchicalOneShotRingTopology plan;
    if (active_ranks.size() == 1) {
        plan.nodes = {{0}};
        return plan;
    }
    std::vector<std::string_view> hosts;
    for (size_t index = 0; index < active_ranks.size(); ++index) {
        const auto rank = active_ranks[index];
        if (rank < 0 || static_cast<size_t>(rank) >= snapshot.endpoints.size())
            return std::nullopt;
        const auto& endpoint = snapshot.endpoints[rank];
        if (!endpoint || endpoint->host_id.empty()) return std::nullopt;
        const auto host =
            std::find(hosts.begin(), hosts.end(), endpoint->host_id);
        if (host == hosts.end()) {
            hosts.push_back(endpoint->host_id);
            plan.nodes.push_back({index});
        } else {
            plan.nodes[host - hosts.begin()].push_back(index);
        }
    }
    if (plan.nodes.size() < 2) return std::nullopt;

    const auto route = [&](size_t from, size_t to) -> DeviceTransferRouteInfo {
        const auto source = static_cast<size_t>(active_ranks[from]);
        const auto target = static_cast<size_t>(active_ranks[to]);
        if (source >= routes.size() || target >= routes[source].size())
            return {};
        return routes[source][target];
    };
    std::vector<size_t> node_by_active_index(active_ranks.size());
    for (size_t node = 0; node < plan.nodes.size(); ++node) {
        for (const auto index : plan.nodes[node])
            node_by_active_index[index] = node;
    }
    // The direct LL barrier must use exactly the peers used by the local data
    // phases.
    for (size_t from = 0; from < active_ranks.size(); ++from) {
        for (size_t to = 0; to < active_ranks.size(); ++to) {
            if (from == to) continue;
            const bool same_node =
                node_by_active_index[from] == node_by_active_index[to];
            if (route(from, to).supportsDirectLl() != same_node)
                return std::nullopt;
        }
    }
    for (size_t node = 0; node < plan.nodes.size(); ++node) {
        const auto& local = plan.nodes[node];
        for (const auto from : local) {
            for (const auto to : local) {
                if (from == to) continue;
                const auto link = route(from, to);
                if (!link.directly_addressable || !link.native_atomics)
                    return std::nullopt;
            }
        }
        const auto& next = plan.nodes[(node + 1) % plan.nodes.size()];
        if (!route(local.front(), next.front()).reachable ||
            !route(next.front(), local.front()).reachable)
            return std::nullopt;
    }
    return plan;
}

}  // namespace mooncake
