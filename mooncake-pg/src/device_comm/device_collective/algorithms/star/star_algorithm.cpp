#include "device_comm/device_collective/algorithms/star/star_algorithm.h"

#include <string>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/simple/simple_protocol.h"

namespace mooncake {

StarAlgorithm::StarAlgorithm(DeviceCollectiveWorkspace& workspace,
                             const SimpleProtocol& simple)
    : AlgorithmProvider(
          DeviceCollectiveAlgorithm::Star, kAlgorithmKey, kEndpointVersion,
          simple,
          {OpType::Gather, OpType::Scatter, OpType::Broadcast, OpType::Reduce}),
      workspace_(workspace),
      simple_(simple) {}

PGResult<std::unique_ptr<StarAlgorithm>> StarAlgorithm::create(
    int device_index, CollectiveRuntimeBindings collective,
    DeviceCollectiveWorkspace& workspace, const SimpleProtocol& simple) {
    PG_VALIDATE_ARG(simple.state() && collective.transfer_handle &&
                        collective.group_view && collective.invocation_state &&
                        collective.control_mailbox &&
                        collective.view_epoch_signals,
                    "Star device bindings are incomplete");
    auto algorithm =
        std::unique_ptr<StarAlgorithm>(new StarAlgorithm(workspace, simple));
    const StarDeviceState initial_state{
        .plan = {},
        .collective = collective,
        .simple = simple.state(),
    };
    PG_TRY(algorithm->state_,
           GpuBuffer<StarDeviceState>::fromHost(device_index, initial_state));
    return algorithm;
}

AlgorithmEndpoint StarAlgorithm::localEndpoint() const {
    return {
        .key = std::string(key()),
        .version = endpointVersion(),
        // Star uses the shared workspace and Simple protocol endpoints,
        // so it has no algorithm-specific metadata.
        .metadata = {},
    };
}

PGResult<void> StarAlgorithm::updatePlan(const ResolvedGroupView& view,
                                         ControlUpdateBuilder& builder) const {
    PG_TRY(auto plan, buildPlan(view));
    return appendPlanUpdate(builder, plan);
}

PGResult<void> StarAlgorithm::invalidatePlan(
    ControlUpdateBuilder& builder) const {
    return appendPlanUpdate(builder, StarPlan{});
}

PGResult<StarPlan> StarAlgorithm::buildPlan(
    const ResolvedGroupView& view) const {
    if (view.self_active_index < 0) return StarPlan{};
    const auto index = static_cast<size_t>(view.self_active_index);
    const auto count = view.participants.size();
    PG_VALIDATE_STATE(index < count && count <= kMaxNumRanks,
                      "Star participants are outside the group");
    PG_VALIDATE_STATE(workspace_.buffer().addr() && view.buffer_size != 0 &&
                          view.buffer_size <= workspace_.buffer().size(),
                      "Star workspace binding is invalid");

    StarPlan plan{
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
                          "Active peer has no compatible star endpoint");
        PG_TRY(auto workspace,
               simple_.bindWorkspace(workspace_, peer.global_rank,
                                     view.buffer_size));
        if (workspace.staging) plan.workspace.staging = workspace.staging;
    }
    return plan;
}

PGResult<void> StarAlgorithm::appendPlanUpdate(ControlUpdateBuilder& builder,
                                               const StarPlan& plan) const {
    return builder.copyBytes(&state_.get()->plan, &plan, sizeof(plan));
}

PGResult<void> StarAlgorithm::enqueueImpl(const GatherRequest& request,
                                          cudaStream_t stream) const {
    PG_TRY(launchStarGatherKernel(request, state_.get(), stream));
    return {};
}

PGResult<void> StarAlgorithm::enqueueImpl(const ScatterRequest& request,
                                          cudaStream_t stream) const {
    PG_TRY(launchStarScatterKernel(request, state_.get(), stream));
    return {};
}

PGResult<void> StarAlgorithm::enqueueImpl(const BroadcastRequest& request,
                                          cudaStream_t stream) const {
    PG_TRY(launchStarBroadcastKernel(request, state_.get(), stream));
    return {};
}

PGResult<void> StarAlgorithm::enqueueImpl(const ReduceRequest& request,
                                          cudaStream_t stream) const {
    PG_TRY(launchStarReduceKernel(request, state_.get(), stream));
    return {};
}

}  // namespace mooncake
