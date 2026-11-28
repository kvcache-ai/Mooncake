#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_TOPOLOGY_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_TOPOLOGY_H

#include <cstdint>

#include "device_comm/device_collective/algorithms/tree/tree_geometry.h"
#include "error_types.h"

namespace mooncake {

// Construct the first NCCL-style binary tree in dense participant order.
// The caller resolves these indices through ResolvedGroupView.participants.
// No membership state, protocol resources, or published device state changes.
PGResult<TreeTopologyIndices> buildTreeTopology(uint32_t participant_count,
                                                int32_t self_active_index);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_TREE_TOPOLOGY_H
