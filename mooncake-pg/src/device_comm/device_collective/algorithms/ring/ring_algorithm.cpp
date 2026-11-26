#include "device_comm/device_collective/algorithms/ring/ring_algorithm.h"

#include <string>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/simple/simple_protocol.h"

namespace mooncake {

RingAlgorithm::RingAlgorithm(DeviceCollectiveWorkspace& workspace,
                             const SimpleProtocol& simple)
    : AlgorithmProvider(DeviceCollectiveAlgorithm::Ring, kAlgorithmKey,
                        kEndpointVersion, simple,
                        {OpType::AllReduce, OpType::AllGather}),
      workspace_(workspace),
      simple_(simple) {}

PGResult<std::unique_ptr<RingAlgorithm>> RingAlgorithm::create(
    int device_index, CollectiveRuntimeBindings collective,
    DeviceCollectiveWorkspace& workspace, const SimpleProtocol& simple) {
    PG_VALIDATE_ARG(simple.state() && collective.transfer_handle &&
                        collective.group_view && collective.invocation_state &&
                        collective.control_mailbox &&
                        collective.view_epoch_signals,
                    "Ring device bindings are incomplete");
    auto algorithm =
        std::unique_ptr<RingAlgorithm>(new RingAlgorithm(workspace, simple));
    const RingDeviceState initial_state{
        .plan = {},
        .collective = collective,
        .simple = simple.state(),
    };
    PG_TRY(algorithm->state_,
           GpuBuffer<RingDeviceState>::fromHost(device_index, initial_state));
    return algorithm;
}

AlgorithmEndpoint RingAlgorithm::localEndpoint() const {
    return {
        .key = std::string(key()),
        .version = endpointVersion(),
        // Ring uses the shared workspace and Simple protocol endpoints,
        // so it has no algorithm-specific metadata.
        .metadata = {},
    };
}

PGResult<void> RingAlgorithm::updatePlan(const ResolvedGroupView& view,
                                         ControlUpdateBuilder& builder) const {
    PG_TRY(auto plan, buildPlan(view));
    return appendPlanUpdate(builder, plan);
}

PGResult<void> RingAlgorithm::invalidatePlan(
    ControlUpdateBuilder& builder) const {
    return appendPlanUpdate(builder, RingPlan{});
}

PGResult<RingPlan> RingAlgorithm::buildPlan(
    const ResolvedGroupView& view) const {
    if (view.self_active_index < 0) return RingPlan{};
    const auto index = static_cast<size_t>(view.self_active_index);
    const auto count = view.participants.size();
    PG_VALIDATE_STATE(index < count && count <= kMaxNumRanks,
                      "Ring participants are outside the group");
    PG_VALIDATE_STATE(workspace_.buffer().addr() && view.buffer_size != 0 &&
                          view.buffer_size <= workspace_.buffer().size(),
                      "Ring workspace binding is invalid");

    RingPlan plan{
        .status = DevicePlanStatus::Ready,
        .workspace = {static_cast<char*>(workspace_.buffer().addr()),
                      view.buffer_size, nullptr},
    };
    for (size_t participant = 0; participant < count; ++participant) {
        const auto& peer = view.participants[participant];
        if (participant != index) {
            PG_VALIDATE_STATE(findEndpoint(peer.algorithms),
                              "Active peer has no compatible ring endpoint");
        }
    }
    if (count > 1) {
        plan.predecessor_index =
            static_cast<uint32_t>((index + count - 1) % count);
        plan.successor_index = static_cast<uint32_t>((index + 1) % count);
        const auto& successor = view.participants[plan.successor_index];
        PG_TRY(plan.workspace,
               simple_.bindWorkspace(workspace_, successor.global_rank,
                                     view.buffer_size));
    }
    return plan;
}

PGResult<void> RingAlgorithm::appendPlanUpdate(ControlUpdateBuilder& builder,
                                               const RingPlan& plan) const {
    return builder.copyBytes(&state_.get()->plan, &plan, sizeof(plan));
}

PGResult<void> RingAlgorithm::enqueueImpl(const AllReduceRequest& request,
                                          cudaStream_t stream) const {
    PG_TRY(launchRingAllReduceKernel(request, state_.get(), stream));
    return {};
}

PGResult<void> RingAlgorithm::enqueueImpl(const AllGatherRequest& request,
                                          cudaStream_t stream) const {
    PG_TRY(launchRingAllGatherKernel(request, state_.get(), stream));
    return {};
}

}  // namespace mooncake
