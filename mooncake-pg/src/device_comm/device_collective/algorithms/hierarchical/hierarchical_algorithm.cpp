#include "device_comm/device_collective/algorithms/hierarchical/hierarchical_algorithm.h"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/simple/simple_protocol.h"

namespace mooncake {
namespace {

PGResult<DeviceTopologyInfo> readTopology(const ResolvedParticipant& peer) {
    const auto* endpoint = findProviderEndpoint(
        peer.algorithms, kDeviceTopologyKey, kDeviceTopologyVersion);
    PG_VALIDATE_STATE(endpoint, "Active peer has no compatible topology endpoint");
    return endpoint->decode<DeviceTopologyInfo>(kDeviceTopologyKey,
                                                kDeviceTopologyVersion);
}

PGResult<void> buildSubviews(const ResolvedGroupView& view,
                             ResolvedGroupView& node_view,
                             ResolvedGroupView& leader_view,
                             uint32_t& local_size, uint32_t& node_count,
                             bool& self_is_leader) {
    const auto self = static_cast<size_t>(view.self_active_index);
    PG_VALIDATE_STATE(self < view.participants.size(),
                      "Hierarchical self rank is outside the group");

    std::vector<DeviceTopologyInfo> topology(view.participants.size());
    std::map<uint32_t, std::vector<size_t>> nodes;
    for (size_t index = 0; index < view.participants.size(); ++index) {
        PG_TRY(topology[index], readTopology(view.participants[index]));
        nodes[topology[index].node_id].push_back(index);
    }
    PG_VALIDATE_STATE(nodes.size() >= 2,
                      "Hierarchical AllReduce requires multiple nodes");

    for (auto& entry : nodes) {
        auto& members = entry.second;
        PG_VALIDATE_STATE(members.size() >= 2,
                          "Hierarchical AllReduce requires multiple ranks per node");
        std::sort(members.begin(), members.end(), [&](size_t left, size_t right) {
            return topology[left].local_rank < topology[right].local_rank;
        });
        for (size_t position = 0; position < members.size(); ++position) {
            PG_VALIDATE_STATE(
                topology[members[position]].local_rank == position,
                "Hierarchical node topology is not contiguous");
        }
    }

    const auto self_node = topology[self].node_id;
    const auto node_entry = nodes.find(self_node);
    PG_VALIDATE_STATE(node_entry != nodes.end(),
                      "Hierarchical self node is missing");
    const auto& local_members = node_entry->second;

    node_view = {
        .epoch = view.epoch,
        .buffer_size = view.buffer_size,
        .collective_algorithm_choices = view.collective_algorithm_choices,
    };
    for (size_t position = 0; position < local_members.size(); ++position) {
        const auto index = local_members[position];
        auto participant = view.participants[index];
        participant.layout_rank = topology[index].local_rank;
        if (index == self) {
            node_view.self_active_index = static_cast<int32_t>(position);
        }
        node_view.participants.push_back(participant);
    }

    leader_view = {
        .epoch = view.epoch,
        .buffer_size = view.buffer_size,
        .collective_algorithm_choices = view.collective_algorithm_choices,
    };
    uint32_t node_rank = 0;
    for (const auto& entry : nodes) {
        const auto index = entry.second.front();
        auto participant = view.participants[index];
        participant.layout_rank = node_rank;
        if (index == self) {
            leader_view.self_active_index =
                static_cast<int32_t>(leader_view.participants.size());
        }
        leader_view.participants.push_back(participant);
        ++node_rank;
    }

    local_size = static_cast<uint32_t>(local_members.size());
    node_count = static_cast<uint32_t>(nodes.size());
    self_is_leader = topology[self].local_rank == 0;
    return {};
}

}  // namespace

HierarchicalAlgorithm::HierarchicalAlgorithm(DeviceCollectiveWorkspace& workspace,
                                             const SimpleProtocol& simple)
    : AlgorithmProvider(DeviceCollectiveAlgorithm::Hierarchical, kAlgorithmKey,
                        kEndpointVersion, simple, {OpType::AllReduce}),
      workspace_(workspace),
      simple_(simple) {}

PGResult<std::unique_ptr<HierarchicalAlgorithm>> HierarchicalAlgorithm::create(
    int device_index, CollectiveRuntimeBindings collective,
    DeviceCollectiveWorkspace& workspace, const SimpleProtocol& simple) {
    PG_VALIDATE_ARG(simple.state() && collective.transfer_handle &&
                        collective.group_view && collective.invocation_state &&
                        collective.control_mailbox &&
                        collective.view_epoch_signals,
                    "Hierarchical device bindings are incomplete");

    auto algorithm = std::unique_ptr<HierarchicalAlgorithm>(
        new HierarchicalAlgorithm(workspace, simple));
    PG_TRY(algorithm->intra_reduce_,
           RingAlgorithm::create(device_index, collective, workspace, simple));
    PG_TRY(algorithm->inter_all_reduce_,
           RingAlgorithm::create(device_index, collective, workspace, simple));
    PG_TRY(algorithm->intra_broadcast_,
           RingAlgorithm::create(device_index, collective, workspace, simple));
    return algorithm;
}

AlgorithmEndpoint HierarchicalAlgorithm::localEndpoint() const {
    return {
        .key = std::string(key()),
        .version = endpointVersion(),
        .metadata = {},
    };
}

PGResult<void> HierarchicalAlgorithm::updatePlan(
    const ResolvedGroupView& view, ControlUpdateBuilder& builder) const {
    if (view.self_active_index < 0) {
        plan_ = {};
        PG_TRY(intra_reduce_->invalidatePlan(builder));
        PG_TRY(inter_all_reduce_->invalidatePlan(builder));
        return intra_broadcast_->invalidatePlan(builder);
    }

    ResolvedGroupView node_view;
    ResolvedGroupView leader_view;
    uint32_t local_size = 0;
    uint32_t node_count = 0;
    bool self_is_leader = false;
    PG_TRY(buildSubviews(view, node_view, leader_view, local_size, node_count,
                         self_is_leader));

    PG_TRY(intra_reduce_->updatePlan(node_view, builder));
    PG_TRY(inter_all_reduce_->updatePlan(leader_view, builder));
    PG_TRY(intra_broadcast_->updatePlan(node_view, builder));
    plan_ = {local_size, node_count, self_is_leader};
    return {};
}

PGResult<void> HierarchicalAlgorithm::invalidatePlan(
    ControlUpdateBuilder& builder) const {
    plan_ = {};
    PG_TRY(intra_reduce_->invalidatePlan(builder));
    PG_TRY(inter_all_reduce_->invalidatePlan(builder));
    return intra_broadcast_->invalidatePlan(builder);
}

PGResult<void> HierarchicalAlgorithm::enqueueImpl(
    const AllReduceRequest& request, cudaStream_t stream) const {
    PG_VALIDATE_STATE(plan_.local_size >= 2 && plan_.node_count >= 2,
                      "Hierarchical plan is not installed");

    const ReduceRequest reduce{
        .send_buffer = request.send_buffer,
        .recv_buffer = request.recv_buffer,
        .count = request.count,
        .datatype = request.datatype,
        .op = request.op,
        .root = 0,
        .failed_ranks_hint = request.failed_ranks_hint,
    };
    PG_TRY(intra_reduce_->enqueue(reduce, stream));

    if (plan_.self_is_leader) {
        const AllReduceRequest leaders{
            .send_buffer = request.recv_buffer,
            .recv_buffer = request.recv_buffer,
            .count = request.count,
            .datatype = request.datatype,
            .op = request.op,
            .failed_ranks_hint = request.failed_ranks_hint,
        };
        PG_TRY(inter_all_reduce_->enqueue(leaders, stream));
    }

    const BroadcastRequest broadcast{
        .send_buffer = request.recv_buffer,
        .recv_buffer = request.recv_buffer,
        .count = request.count,
        .datatype = request.datatype,
        .root = 0,
        .failed_ranks_hint = request.failed_ranks_hint,
    };
    return intra_broadcast_->enqueue(broadcast, stream);
}

}  // namespace mooncake
