#ifndef MOONCAKE_PG_CONTROL_PLANE_COLLECTIVE_ALGORITHM_POLICY_H
#define MOONCAKE_PG_CONTROL_PLANE_COLLECTIVE_ALGORITHM_POLICY_H

#include <optional>
#include <string_view>

#include "control_plane/control_types.h"
#include "error_types.h"

namespace mooncake {

inline PGResult<std::optional<DeviceAllReduceAlgorithm>>
parseCollectiveAlgorithm(std::string_view value) {
    if (value == "auto") return std::optional<DeviceAllReduceAlgorithm>{};
    if (value == "ring") return std::optional{DeviceAllReduceAlgorithm::Ring};
    if (value == "oneshot")
        return std::optional{DeviceAllReduceAlgorithm::OneShot};
    if (value == "tree") return std::optional{DeviceAllReduceAlgorithm::Tree};
    return makePGError(PGErrorCode::InvalidArgument,
                       "MOONCAKE_PG_COLLECTIVE_ALGORITHM must be 'auto', "
                       "'ring', 'oneshot', or 'tree'");
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONTROL_PLANE_COLLECTIVE_ALGORITHM_POLICY_H
