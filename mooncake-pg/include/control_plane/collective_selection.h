#ifndef MOONCAKE_PG_CONTROL_PLANE_COLLECTIVE_SELECTION_H
#define MOONCAKE_PG_CONTROL_PLANE_COLLECTIVE_SELECTION_H

#include <optional>
#include <span>
#include <vector>

#include "control_plane/control_types.h"

namespace mooncake {

struct GpuCollectiveSelection {
    GpuCollectiveBackend backend;
    std::vector<DeviceCollectiveAlgorithmChoice> algorithm_choices;
};

[[nodiscard]] bool supportsGpuCollectiveBackend(
    const DeviceGroupEndpoint& endpoint, GpuCollectiveBackend backend) noexcept;

// Select the backend and algorithms for the active members. Return nullopt
// when there are no active members or the group has no GPU backend preference.
// Pass an empty span if installation is incomplete for any active member's rank
// epoch.
[[nodiscard]] std::optional<GpuCollectiveSelection> selectGpuCollective(
    const GroupView& view,
    std::span<const std::vector<DeviceTransferRouteInfo>> installed_routes,
    const std::optional<std::vector<DeviceCollectiveAlgorithmChoice>>&
        algorithm_choices_override);

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_COLLECTIVE_SELECTION_H
