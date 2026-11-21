#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_RESOLVED_GROUP_VIEW_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_RESOLVED_GROUP_VIEW_H

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "common_types.h"
#include "control_plane/control_types.h"
#include "device_comm/device_collective/device_collective_types.cuh"

namespace mooncake {

struct ResolvedParticipant {
    GlobalRank global_rank = kInvalidGlobalRank;
    InGroupRank in_group_rank = kInvalidInGroupRank;
    std::vector<ProtocolEndpoint> protocols;
    std::vector<AlgorithmEndpoint> algorithms;
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

// Host input for collective execution. resolveGroupView validates membership
// and shared endpoint data, resolves workspace and signal offsets, and keeps
// only the data needed for protocol and algorithm setup.
struct ResolvedGroupView {
    uint64_t epoch = kInvalidViewEpoch;
    int32_t self_active_index = -1;
    uint64_t buffer_size = 0;
    std::vector<ResolvedParticipant> participants;
    std::vector<DeviceCollectiveAlgorithmChoice> collective_algorithm_choices;

    [[nodiscard]] DeviceGroupView asDeviceView() const noexcept {
        DeviceGroupView view{
            .epoch = epoch,
            .self_active_index = self_active_index,
            .participant_count = static_cast<uint32_t>(participants.size()),
        };
        for (std::size_t index = 0; index < participants.size(); ++index) {
            view.participants[index] = participants[index].asPeer();
        }
        return view;
    }

    [[nodiscard]] static ResolvedGroupView localOnly(
        InGroupRank self_rank, uint64_t buffer_size,
        std::span<const DeviceCollectiveAlgorithmChoice> choices) {
        return {
            .self_active_index = 0,
            .buffer_size = buffer_size,
            .participants = {ResolvedParticipant{.in_group_rank = self_rank}},
            .collective_algorithm_choices = {choices.begin(), choices.end()},
        };
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_RESOLVED_GROUP_VIEW_H
