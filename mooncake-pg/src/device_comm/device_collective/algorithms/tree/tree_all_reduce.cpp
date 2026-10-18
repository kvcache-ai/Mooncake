#include "device_comm/device_collective/algorithms/tree/tree_all_reduce.h"

#include <glog/logging.h>

#include "device_comm/device_collective/algorithms/tree/tree_topology.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/protocols/simple/simple_resources.h"

namespace mooncake {

PGResult<std::unique_ptr<TreeAllReduceAlgorithm>>
TreeAllReduceAlgorithm::create(int device_index,
                               CollectiveRuntimeBindings collective,
                               DeviceCollectiveWorkspace& workspace,
                               const SimpleResources& simple) {
    PG_VALIDATE_ARG(simple.state() && collective.transfer_handle &&
                        collective.invocation_state &&
                        collective.control_mailbox &&
                        collective.view_epoch_signals,
                    "Tree device bindings are incomplete");
    auto algorithm = std::unique_ptr<TreeAllReduceAlgorithm>(
        new TreeAllReduceAlgorithm(device_index, workspace, simple));
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&algorithm->state_),
                           sizeof(TreeAllReduceDeviceState)));
    const TreeAllReduceDeviceState initial{
        .plan = {}, .collective = collective, .simple = simple.state()};
    PG_TRY_CUDA(cudaMemcpy(algorithm->state_, &initial, sizeof(initial),
                           cudaMemcpyHostToDevice));
    return algorithm;
}

TreeAllReduceAlgorithm::~TreeAllReduceAlgorithm() noexcept {
    if (!state_) return;
    auto guard = GpuDeviceGuard::create(device_index_);
    if (!guard.has_value()) {
        LOG(ERROR)
            << "Failed to select CUDA device while releasing Tree state: "
            << guard.error().message;
        return;
    }
    const auto result = cudaFree(state_);
    state_ = nullptr;
    if (result != cudaSuccess)
        LOG(ERROR) << "Failed to free Tree state: "
                   << cudaGetErrorString(result);
}

PGResult<TreeAllReducePlan> TreeAllReduceAlgorithm::buildPlan(
    const ResolvedGroupView& view) const {
    if (view.self_active_index < 0) return TreeAllReducePlan{};
    PG_VALIDATE_STATE(
        static_cast<size_t>(view.self_active_index) < view.participants.size(),
        "Tree self index is outside the group");
    // Non-Tree groups may include old members. Keep their Tree plan unavailable
    // without preventing the existing Ring/OneShot configuration from updating.
    for (const auto& participant : view.participants) {
        if (participant.global_rank == kInvalidGlobalRank &&
            view.participants.size() == 1)
            continue;  // isolated preparation uses the runtime's own resources
        if (!participant.endpoint.supportsTree()) return TreeAllReducePlan{};
    }
    PG_VALIDATE_STATE(workspace_.buffer().addr() && view.buffer_size != 0 &&
                          view.buffer_size <= workspace_.buffer().size(),
                      "Tree workspace binding is invalid");
    PG_VALIDATE_STATE(
        TreeBufferLayout::make(view.buffer_size, 1).chunk_bytes != 0,
        "Tree workspace cannot hold its receive slots");
    PG_TRY(auto indices,
           buildTreeTopology(view.participants.size(), view.self_active_index));
    TreeAllReducePlan plan{
        .status = DevicePlanStatus::Ready,
        .view_epoch = view.epoch,
        .workspace = {static_cast<char*>(workspace_.buffer().addr()),
                      view.buffer_size, nullptr},
        .self_active_index = view.self_active_index,
        .participant_count = static_cast<uint32_t>(view.participants.size()),
        .tree_count = 1,
    };
    auto& topology = plan.topologies[0];
    topology.parent_child_slot = indices.parent_child_slot;
    const auto bind_peer = [&](int32_t index) -> PGResult<CollectivePeer> {
        if (index == kInvalidTreeActiveIndex) return CollectivePeer{};
        PG_VALIDATE_STATE(
            index >= 0 && static_cast<size_t>(index) < view.participants.size(),
            "Tree neighbor is outside the participants");
        const auto& participant = view.participants[index];
        PG_VALIDATE_STATE(
            participant.global_rank != kInvalidGlobalRank &&
                participant.in_group_rank != kInvalidInGroupRank &&
                participant.workspace.buffer_size >= view.buffer_size,
            "Tree neighbor endpoint is invalid");
        return participant.asPeer();
    };
    PG_TRY(topology.parent, bind_peer(indices.parent));
    if (indices.parent != kInvalidTreeActiveIndex) {
        PG_TRY(auto parent,
               buildTreeTopology(plan.participant_count, indices.parent));
        PG_VALIDATE_STATE(indices.parent_child_slot < kTreeChildCount &&
                              parent.children[indices.parent_child_slot] ==
                                  view.self_active_index,
                          "Tree parent receive slot is inconsistent");
    }
    for (uint32_t slot = 0; slot < kTreeChildCount; ++slot) {
        PG_TRY(topology.children[slot], bind_peer(indices.children[slot]));
        if (indices.children[slot] == kInvalidTreeActiveIndex) continue;
        PG_TRY(auto child, buildTreeTopology(plan.participant_count,
                                             indices.children[slot]));
        PG_VALIDATE_STATE(child.parent == view.self_active_index &&
                              child.parent_child_slot == slot,
                          "Tree child receive slot is inconsistent");
    }
    for (const auto& peer :
         {topology.parent, topology.children[0], topology.children[1]}) {
        if (peer.global_rank == kInvalidGlobalRank) continue;
        bool seen = false;
        for (uint32_t i = 0; i < plan.peer_count; ++i)
            seen |= plan.remote_peers[i].global_rank == peer.global_rank;
        if (seen) continue;
        plan.remote_peers[plan.peer_count++] = peer;
        PG_TRY(auto connection_workspace,
               simple_.bindWorkspace(workspace_, peer, view.buffer_size));
        if (connection_workspace.staging)
            plan.workspace.staging = connection_workspace.staging;
    }
    return plan;
}

PGResult<void> TreeAllReduceAlgorithm::appendPlanUpdate(
    ControlUpdateBuilder& builder, const TreeAllReducePlan& plan) const {
    return builder.copyBytes(&state_->plan, &plan, sizeof(plan));
}

PGResult<void> TreeAllReduceAlgorithm::enqueue(const AllReduceRequest& request,
                                               cudaStream_t stream) const {
    PG_TRY_CUDA(launchTreeAllReduceKernel(request, state_, stream));
    return {};
}

}  // namespace mooncake
