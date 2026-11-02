#include "device_comm/device_collective/algorithms/pairwise/pairwise_algorithm.h"

#include <string>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/simple/simple_protocol.h"

namespace mooncake {

PairwiseAlgorithm::PairwiseAlgorithm(DeviceCollectiveWorkspace& workspace,
                                     const SimpleProtocol& simple)
    : AlgorithmProvider(DeviceCollectiveAlgorithm::Pairwise, kAlgorithmKey,
                        kEndpointVersion, simple, {OpType::AllToAll}),
      workspace_(workspace),
      simple_(simple) {}

PGResult<std::unique_ptr<PairwiseAlgorithm>> PairwiseAlgorithm::create(
    int device_index, CollectiveRuntimeBindings collective,
    DeviceCollectiveWorkspace& workspace, const SimpleProtocol& simple) {
    PG_VALIDATE_ARG(simple.state() && collective.transfer_handle &&
                        collective.group_view && collective.invocation_state &&
                        collective.control_mailbox &&
                        collective.view_epoch_signals,
                    "Pairwise device bindings are incomplete");
    auto algorithm = std::unique_ptr<PairwiseAlgorithm>(
        new PairwiseAlgorithm(workspace, simple));
    const PairwiseDeviceState initial_state{
        .plan = {},
        .collective = collective,
        .simple = simple.state(),
    };
    PG_TRY(algorithm->state_, GpuBuffer<PairwiseDeviceState>::fromHost(
                                  device_index, initial_state));
    return algorithm;
}

AlgorithmEndpoint PairwiseAlgorithm::localEndpoint() const {
    return {
        .key = std::string(key()),
        .version = endpointVersion(),
        // Pairwise uses the shared workspace and Simple protocol endpoints,
        // so it has no algorithm-specific metadata.
        .metadata = {},
    };
}

PGResult<void> PairwiseAlgorithm::updatePlan(
    const ResolvedGroupView& view, ControlUpdateBuilder& builder) const {
    PG_TRY(auto plan, buildPlan(view));
    return appendPlanUpdate(builder, plan);
}

PGResult<void> PairwiseAlgorithm::invalidatePlan(
    ControlUpdateBuilder& builder) const {
    return appendPlanUpdate(builder, PairwisePlan{});
}

PGResult<PairwisePlan> PairwiseAlgorithm::buildPlan(
    const ResolvedGroupView& view) const {
    if (view.self_active_index < 0) return PairwisePlan{};
    const auto index = static_cast<size_t>(view.self_active_index);
    const auto count = view.participants.size();
    PG_VALIDATE_STATE(index < count && count <= kMaxNumRanks,
                      "Pairwise participants are outside the group");
    PG_VALIDATE_STATE(workspace_.buffer().addr() && view.buffer_size != 0 &&
                          view.buffer_size <= workspace_.buffer().size(),
                      "Pairwise workspace binding is invalid");

    PairwisePlan plan{
        .status = DevicePlanStatus::Ready,
        .workspace = {static_cast<char*>(workspace_.buffer().addr()),
                      view.buffer_size, nullptr},
    };
    // Any remote participant may be a send target. Retain staging if at least
    // one of those routes needs it.
    for (size_t participant = 0; participant < count; ++participant) {
        if (participant == index) continue;
        const auto& peer = view.participants[participant];
        PG_VALIDATE_STATE(findEndpoint(peer.algorithms),
                          "Active peer has no compatible pairwise endpoint");
        PG_TRY(auto workspace,
               simple_.bindWorkspace(workspace_, peer.global_rank,
                                     view.buffer_size));
        if (workspace.staging) plan.workspace.staging = workspace.staging;
    }
    return plan;
}

PGResult<void> PairwiseAlgorithm::appendPlanUpdate(
    ControlUpdateBuilder& builder, const PairwisePlan& plan) const {
    return builder.copyBytes(&state_.get()->plan, &plan, sizeof(plan));
}

PGResult<void> PairwiseAlgorithm::enqueueImpl(const AllToAllRequest& request,
                                              cudaStream_t stream) const {
    PG_TRY(launchPairwiseAllToAllKernel(request, state_.get(), stream));
    return {};
}

}  // namespace mooncake
