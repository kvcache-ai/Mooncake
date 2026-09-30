#include "device_comm/device_collective/algorithms/ring/ring_all_reduce.h"

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/simple/simple_resources.h"

namespace mooncake {

PGResult<std::unique_ptr<RingAllReduceAlgorithm>>
RingAllReduceAlgorithm::create(int device_index,
                               CollectiveRuntimeBindings collective,
                               DeviceCollectiveWorkspace& workspace,
                               const SimpleResources& simple) {
    PG_VALIDATE_ARG(simple.state() && collective.transfer_handle &&
                        collective.invocation_state &&
                        collective.control_mailbox &&
                        collective.view_epoch_signals,
                    "Ring device bindings are incomplete");
    auto algorithm = std::unique_ptr<RingAllReduceAlgorithm>(
        new RingAllReduceAlgorithm(device_index, workspace, simple));
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&algorithm->state_),
                           sizeof(RingAllReduceDeviceState)));
    const RingAllReduceDeviceState initial_state{
        .plan = {},
        .collective = collective,
        .simple = simple.state(),
    };
    PG_TRY_CUDA(cudaMemcpy(algorithm->state_, &initial_state,
                           sizeof(RingAllReduceDeviceState),
                           cudaMemcpyHostToDevice));
    return algorithm;
}

RingAllReduceAlgorithm::~RingAllReduceAlgorithm() noexcept {
    if (!state_) return;
    auto device_guard = GpuDeviceGuard::create(device_index_);
    if (!device_guard.has_value()) {
        LOG(ERROR) << "Failed to select CUDA device while releasing Ring "
                      "state: "
                   << device_guard.error().message;
        return;
    }
    const auto result = cudaFree(state_);
    state_ = nullptr;
    if (result != cudaSuccess) {
        LOG(ERROR) << "Failed to free Ring device state: "
                   << cudaGetErrorString(result);
    }
}

PGResult<RingAllReducePlan> RingAllReduceAlgorithm::buildPlan(
    const ResolvedGroupView& view) const {
    if (view.self_active_index < 0) return RingAllReducePlan{};
    const auto index = static_cast<size_t>(view.self_active_index);
    const auto count = view.participants.size();
    PG_VALIDATE_STATE(index < count, "Ring participants are outside the group");
    PG_VALIDATE_STATE(workspace_.buffer().addr() && view.buffer_size != 0 &&
                          view.buffer_size <= workspace_.buffer().size(),
                      "Ring workspace binding is invalid");

    RingAllReducePlan plan{
        .status = DevicePlanStatus::Ready,
        .view_epoch = view.epoch,
        .workspace = {static_cast<char*>(workspace_.buffer().addr()),
                      view.buffer_size, nullptr},
        .self_active_index = view.self_active_index,
        .participant_count = static_cast<uint32_t>(count),
    };
    if (count > 1) {
        const auto& predecessor =
            view.participants[(index + count - 1) % count];
        const auto& successor = view.participants[(index + 1) % count];
        plan.remote_peers[plan.peer_count++] = predecessor.asPeer();
        if (successor.in_group_rank != predecessor.in_group_rank)
            plan.remote_peers[plan.peer_count++] = successor.asPeer();
        PG_TRY(plan.workspace,
               simple_.bindWorkspace(workspace_, successor.asPeer(),
                                     view.buffer_size));
    }
    return plan;
}

PGResult<void> RingAllReduceAlgorithm::appendPlanUpdate(
    ControlUpdateBuilder& builder, const RingAllReducePlan& plan) const {
    return builder.copyBytes(&state_->plan, &plan, sizeof(plan));
}

PGResult<void> RingAllReduceAlgorithm::enqueue(const AllReduceRequest& request,
                                               cudaStream_t stream) const {
    PG_TRY_CUDA(launchRingAllReduceKernel(request, state_, stream));
    return {};
}

}  // namespace mooncake
