#include "device_comm/device_collective/algorithms/twoshot/two_shot_all_reduce.h"

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/simple/simple_resources.h"
#include "device_comm/device_transfer/transfer_service.h"

namespace mooncake {

PGResult<std::unique_ptr<TwoShotAllReduceAlgorithm>>
TwoShotAllReduceAlgorithm::create(DeviceTransferService& transfer_service,
                                  CollectiveRuntimeBindings collective,
                                  DeviceCollectiveWorkspace& workspace,
                                  const SimpleResources& simple) {
    PG_VALIDATE_ARG(simple.state() && collective.transfer_handle &&
                        collective.invocation_state &&
                        collective.control_mailbox &&
                        collective.view_epoch_signals,
                    "TwoShot device bindings are incomplete");
    const int device_index = transfer_service.deviceIndex();
    auto algorithm = std::unique_ptr<TwoShotAllReduceAlgorithm>(
        new TwoShotAllReduceAlgorithm(transfer_service, device_index,
                                      workspace));
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&algorithm->state_),
                           sizeof(TwoShotAllReduceDeviceState)));
    const TwoShotAllReduceDeviceState initial_state{
        .plan = {},
        .collective = collective,
        .simple = simple.state(),
    };
    PG_TRY_CUDA(cudaMemcpy(algorithm->state_, &initial_state,
                           sizeof(TwoShotAllReduceDeviceState),
                           cudaMemcpyHostToDevice));
    return algorithm;
}

TwoShotAllReduceAlgorithm::~TwoShotAllReduceAlgorithm() noexcept {
    if (!state_) return;
    auto device_guard = GpuDeviceGuard::create(device_index_);
    if (!device_guard.has_value()) {
        LOG(ERROR) << "Failed to select CUDA device while releasing TwoShot "
                      "state: "
                   << device_guard.error().message;
        return;
    }
    const auto result = cudaFree(state_);
    state_ = nullptr;
    if (result != cudaSuccess) {
        LOG(ERROR) << "Failed to free TwoShot device state: "
                   << cudaGetErrorString(result);
    }
}

PGResult<TwoShotAllReducePlan> TwoShotAllReduceAlgorithm::buildPlan(
    const ResolvedGroupView& view) const {
    if (view.self_active_index < 0) return TwoShotAllReducePlan{};
    const auto index = static_cast<size_t>(view.self_active_index);
    const auto count = view.participants.size();
    PG_VALIDATE_STATE(index < count,
                      "TwoShot participants are outside the group");
    PG_VALIDATE_STATE(workspace_.buffer().addr() && view.buffer_size != 0 &&
                          view.buffer_size <= workspace_.buffer().size(),
                      "TwoShot workspace binding is invalid");

    TwoShotAllReducePlan plan{
        .status = DevicePlanStatus::Ready,
        .view_epoch = view.epoch,
        .workspace = {static_cast<char*>(workspace_.buffer().addr()),
                      view.buffer_size, nullptr},
        .self_active_index = view.self_active_index,
        .participant_count = static_cast<uint32_t>(count),
    };
    PG_VALIDATE_STATE(count <= kMaxNumRanks &&
                          view.buffer_size / count >=
                              kSimplePipelineDepth * kSimplePayloadAlignment,
                      "TwoShot workspace cannot hold one slot per sender");
    uint32_t remote_index = 0;
    for (size_t peer = 0; peer < count; ++peer) {
        if (peer == index) continue;
        const auto& participant = view.participants[peer];
        PG_VALIDATE_STATE(participant.endpoint.two_shot &&
                              participant.endpoint.two_shot->version ==
                                  kTwoShotEndpointVersion,
                          "Active peer has no compatible two-shot capability");
        PG_TRY(auto route,
               transfer_service_.routeType(participant.global_rank));
        PG_VALIDATE_STATE(route == DeviceRouteType::P2p,
                          "TwoShot requires a direct P2P route to every peer");
        plan.remote_peers[remote_index++] = participant.asPeer();
    }
    return plan;
}

PGResult<void> TwoShotAllReduceAlgorithm::appendPlanUpdate(
    ControlUpdateBuilder& builder, const TwoShotAllReducePlan& plan) const {
    return builder.copyBytes(&state_->plan, &plan, sizeof(plan));
}

PGResult<void> TwoShotAllReduceAlgorithm::enqueue(
    const AllReduceRequest& request, cudaStream_t stream) const {
    PG_TRY_CUDA(launchTwoShotAllReduceKernel(request, state_, stream));
    return {};
}

}  // namespace mooncake
