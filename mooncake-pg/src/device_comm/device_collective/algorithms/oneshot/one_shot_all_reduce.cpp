#include "device_comm/device_collective/algorithms/oneshot/one_shot_all_reduce.h"

#include <utility>

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/protocols/ll/ll_resources.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "pg_utils.h"

namespace mooncake {

OneShotAllReduceAlgorithm::OneShotAllReduceAlgorithm(RegionSlice packets,
                                                     OneShotBufferLayout layout,
                                                     int device_index) noexcept
    : packets_(std::move(packets)),
      layout_(layout),
      device_index_(device_index) {}

PGResult<std::unique_ptr<OneShotAllReduceAlgorithm>>
OneShotAllReduceAlgorithm::create(DeviceTransferService& transfer_service,
                                  uint32_t max_group_size,
                                  CollectiveRuntimeBindings collective,
                                  const LLResources& ll) {
    PG_VALIDATE_ARG(max_group_size > 0 && max_group_size <= kMaxNumRanks,
                    "One-shot group capacity is outside the supported range");
    PG_VALIDATE_ARG(ll.state() && collective.transfer_handle &&
                        collective.control_mailbox &&
                        collective.invocation_state,
                    "One-shot device bindings are incomplete");
    const OneShotBufferLayout layout{max_group_size};
    PG_TRY(auto packets, transfer_service.allocatePeerAccessible(
                             layout.packetBytes(), alignof(LLPacket)));
    const int device_index = transfer_service.deviceIndex();
    auto algorithm = std::unique_ptr<OneShotAllReduceAlgorithm>(
        new OneShotAllReduceAlgorithm(std::move(packets), layout,
                                      device_index));
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    PG_TRY_CUDA(
        cudaMemset(algorithm->packets_.addr(), 0, algorithm->packets_.size()));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&algorithm->state_),
                           sizeof(OneShotAllReduceDeviceState)));
    const OneShotAllReduceDeviceState initial_state{
        .plan = {},
        .collective = collective,
        .ll = ll.state(),
    };
    PG_TRY_CUDA(cudaMemcpy(algorithm->state_, &initial_state,
                           sizeof(OneShotAllReduceDeviceState),
                           cudaMemcpyHostToDevice));
    return algorithm;
}

OneShotAllReduceAlgorithm::~OneShotAllReduceAlgorithm() noexcept {
    if (!state_) return;
    auto device_guard = GpuDeviceGuard::create(device_index_);
    if (!device_guard.has_value()) {
        LOG(ERROR) << "Failed to select CUDA device while releasing one-shot "
                      "state: "
                   << device_guard.error().message;
        return;
    }
    const auto result = cudaFree(state_);
    state_ = nullptr;
    if (result != cudaSuccess) {
        LOG(ERROR) << "Failed to free one-shot device state: "
                   << cudaGetErrorString(result);
    }
}

PGResult<OneShotAllReducePlan> OneShotAllReduceAlgorithm::buildPlan(
    const ResolvedGroupView& view) const {
    if (view.self_active_index < 0) return OneShotAllReducePlan{};
    const auto max_group_size = layout_.max_group_size;
    const auto index = static_cast<size_t>(view.self_active_index);
    PG_VALIDATE_STATE(index < view.participants.size() &&
                          view.participants.size() <= max_group_size &&
                          max_group_size <= kMaxNumRanks,
                      "One-shot participants are outside the group");
    OneShotAllReducePlan plan{
        .status = DevicePlanStatus::Ready,
        .view_epoch = view.epoch,
        .layout = layout_,
        .packets = static_cast<LLPacket*>(packets_.addr()),
        .self_rank = view.participants[index].in_group_rank,
        .self_active_index = static_cast<uint32_t>(index),
        .participant_count = static_cast<uint32_t>(view.participants.size()),
    };
    uint32_t remote_index = 0;
    for (size_t peer = 0; peer < view.participants.size(); ++peer) {
        if (peer == index) continue;
        const auto& participant = view.participants[peer];
        PG_VALIDATE_STATE(participant.endpoint.one_shot,
                          "Active peer has no one-shot endpoint");
        const auto& endpoint = *participant.endpoint.one_shot;
        const auto& workspace = participant.workspace;
        PG_VALIDATE_STATE(
            endpoint.buffer_offset % alignof(LLPacket) == 0 &&
                endpoint.buffer_size == layout_.packetBytes() &&
                !addOverflows(endpoint.buffer_offset, endpoint.buffer_size),
            "One-shot peer packet endpoint is invalid");
        auto& remote = plan.remote_peers[remote_index++];
        remote = participant.asPeer();
        remote.workspace_offset = endpoint.buffer_offset;
    }
    return plan;
}

PGResult<void> OneShotAllReduceAlgorithm::appendPlanUpdate(
    ControlUpdateBuilder& builder, const OneShotAllReducePlan& plan) const {
    return builder.copyBytes(&state_->plan, &plan, sizeof(plan));
}

PGResult<void> OneShotAllReduceAlgorithm::enqueue(
    const AllReduceRequest& request, cudaStream_t stream) const {
    PG_TRY_CUDA(launchOneShotAllReduceKernel(request, state_, stream));
    return {};
}

}  // namespace mooncake
