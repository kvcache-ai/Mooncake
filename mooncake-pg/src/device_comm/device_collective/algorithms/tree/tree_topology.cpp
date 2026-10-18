// SPDX-FileCopyrightText: 2016-2026 NVIDIA CORPORATION & AFFILIATES
//
// SPDX-License-Identifier: Apache-2.0
//
// Binary-tree construction adapted from ncclGetBtree in NCCL trees.cc,
// revision 12df1a11afad322be5a204a2db890161cbf8131d.

#include "device_comm/device_collective/algorithms/tree/tree_topology.h"

#include "common_types.h"

namespace mooncake {

PGResult<TreeTopologyIndices> buildTreeTopology(uint32_t participant_count,
                                                int32_t self_active_index) {
    PG_VALIDATE_ARG(participant_count != 0 && participant_count <= kMaxNumRanks,
                    "Tree participant count is outside the supported capacity");
    PG_VALIDATE_ARG(
        self_active_index >= 0 &&
            static_cast<uint32_t>(self_active_index) < participant_count,
        "Tree self active index is outside the participants");

    const auto index = static_cast<uint32_t>(self_active_index);
    uint32_t bit = 1;
    while (bit < participant_count && (index & bit) == 0) bit <<= 1;

    TreeTopologyIndices topology;
    if (index == 0) {
        if (participant_count > 1)
            topology.children[1] = static_cast<int32_t>(bit >> 1);
        return topology;
    }

    uint32_t parent = (index ^ bit) | (bit << 1);
    if (parent >= participant_count) parent = index ^ bit;
    topology.parent = static_cast<int32_t>(parent);
    topology.parent_child_slot = index < parent ? 0 : 1;

    uint32_t child_bit = bit >> 1;
    if (child_bit == 0) return topology;
    topology.children[0] = static_cast<int32_t>(index - child_bit);
    // In incomplete levels, choose the largest descendant below capacity.
    // A missing child stays in its logical slot rather than being compacted.
    while (child_bit != 0 && index + child_bit >= participant_count)
        child_bit >>= 1;
    if (child_bit != 0)
        topology.children[1] = static_cast<int32_t>(index + child_bit);
    return topology;
}

}  // namespace mooncake
