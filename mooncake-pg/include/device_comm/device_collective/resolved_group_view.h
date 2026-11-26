#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_RESOLVED_GROUP_VIEW_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_RESOLVED_GROUP_VIEW_H

#include <cstdint>
#include <vector>

#include "common_types.h"
#include "control_plane/control_types.h"
#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {

struct ResolvedParticipant {
    GlobalRank global_rank = kInvalidGlobalRank;
    InGroupRank in_group_rank = kInvalidInGroupRank;
    DeviceGroupEndpoint endpoint;
    DeviceCollectiveWorkspaceEndpoint workspace;
    // Remote word written by this communicator's rank.
    uint64_t view_epoch_signal_offset = 0;

    [[nodiscard]] CollectivePeer asPeer() const noexcept {
        return {
            .global_rank = global_rank,
            .in_group_rank = in_group_rank,
            .workspace_offset = workspace.buffer_offset,
            .view_epoch_signal_offset = view_epoch_signal_offset,
        };
    }
};

// Host-only input to protocol binding and algorithm Plan construction.
// Runtime resolves active peers and View-epoch bindings. Protocols and
// algorithms interpret their own endpoints; algorithms choose connections and
// payload layout.
struct ResolvedGroupView {
    uint64_t epoch = 0;
    int32_t self_active_index = -1;
    uint64_t buffer_size = 0;
    std::vector<ResolvedParticipant> participants;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_RESOLVED_GROUP_VIEW_H
