#include "device_comm/device_collective/algorithms/oneshot/one_shot_all_reduce.h"

#include <algorithm>

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/ll/ll_resources.h"
#include "device_comm/device_transfer/transfer_service.h"

namespace mooncake {

OneShotAllReduceAlgorithm::OneShotAllReduceAlgorithm(
    DeviceTransferService& transfer_service,
    DeviceCollectiveWorkspace& workspace, LLResources& ll,
    uint64_t timeout_ticks, int device_index, InGroupRank self_rank,
    uint32_t max_group_size) noexcept
    : transfer_service_(transfer_service),
      workspace_(workspace),
      ll_(ll),
      timeout_ticks_(timeout_ticks),
      device_index_(device_index),
      self_rank_(self_rank),
      max_group_size_(max_group_size) {}

PGResult<std::unique_ptr<OneShotAllReduceAlgorithm>>
OneShotAllReduceAlgorithm::create(DeviceTransferService& transfer_service,
                                  DeviceCollectiveWorkspace& workspace,
                                  LLResources& ll,
                                  const uint64_t* view_epoch_signals,
                                  InvocationState* invocation_state,
                                  ControlMailbox* control_mailbox,
                                  uint64_t timeout_ticks, int device_index,
                                  InGroupRank self_rank,
                                  uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size > 0 && max_group_size <= kMaxNumRanks,
                    "One-shot group capacity is outside the supported range");
    PG_VALIDATE_ARG(ll.maxGroupSize() == max_group_size,
                    "One-shot LL group capacity mismatch");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "One-shot self rank is outside the group");
    PG_VALIDATE_ARG(invocation_state && control_mailbox,
                    "One-shot invocation or control state is null");
    PG_VALIDATE_ARG(view_epoch_signals, "One-shot View-epoch signals are null");
    PG_VALIDATE_ARG(workspace.buffer().addr(), "One-shot buffer address is null");
    const auto layout =
        OneShotBufferLayout::make(workspace.buffer().size(), max_group_size);
    PG_VALIDATE_ARG(layout.slot_capacity >= sizeof(uint32_t),
                    "One-shot workspace is too small");

    auto algorithm = std::unique_ptr<OneShotAllReduceAlgorithm>(
        new OneShotAllReduceAlgorithm(transfer_service, workspace, ll,
                                      timeout_ticks, device_index, self_rank,
                                      max_group_size));
    PG_TRY(algorithm->initializeDeviceState(view_epoch_signals, invocation_state,
                                           control_mailbox));
    return algorithm;
}

OneShotAllReduceAlgorithm::~OneShotAllReduceAlgorithm() noexcept {
    releaseDeviceState();
}

PGResult<void> OneShotAllReduceAlgorithm::initializeDeviceState(
    const uint64_t* view_epoch_signals, InvocationState* invocation_state,
    ControlMailbox* control_mailbox) {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&state_),
                           sizeof(OneShotAllReduceDeviceState)));
    const OneShotAllReduceDeviceState initial_state{
        .plan = {},
        .view_epoch_signals = view_epoch_signals,
        .invocation_state = invocation_state,
        .control_mailbox = control_mailbox,
        .ll = ll_.state(),
    };
    PG_TRY_CUDA(cudaMemcpy(state_, &initial_state,
                           sizeof(OneShotAllReduceDeviceState),
                           cudaMemcpyHostToDevice));
    return {};
}

void OneShotAllReduceAlgorithm::releaseDeviceState() noexcept {
    if (!state_) return;
    auto device_guard = GpuDeviceGuard::create(device_index_);
    if (!device_guard.has_value()) {
        LOG(ERROR) << "Failed to select CUDA device while releasing One-shot "
                      "state: "
                   << device_guard.error().message;
        return;
    }
    const auto result = cudaFree(state_);
    state_ = nullptr;
    if (result != cudaSuccess) {
        LOG(ERROR) << "Failed to free One-shot device state: "
                   << cudaGetErrorString(result);
    }
}

OneShotAllReducePlan OneShotAllReduceAlgorithm::makePlan(
    uint64_t view_epoch, uint32_t self_active_index,
    uint32_t participant_count, uint64_t buffer_size) const {
    const auto layout = OneShotBufferLayout::make(buffer_size, max_group_size_);
    return OneShotAllReducePlan{
        .transfer_handle = transfer_service_.deviceHandle(),
        .timeout_ticks = timeout_ticks_,
        .view_epoch = view_epoch,
        .buffer_ptr = static_cast<char*>(workspace_.buffer().addr()),
        .layout = layout,
        .chunk_bytes = static_cast<uint32_t>(std::min<uint64_t>(
            kMaxOneShotChunkBytes, layout.slot_capacity)),
        .self_rank = self_rank_,
        .self_active_index = self_active_index,
        .participant_count = participant_count,
    };
}

PGResult<void> OneShotAllReduceAlgorithm::appendPlanUpdate(
    ControlUpdateBuilder& builder) const {
    return builder.copyBytes(&state_->plan, &host_plan_, sizeof(host_plan_));
}

void OneShotAllReduceAlgorithm::useLocalOnly(uint64_t view_epoch) {
    host_plan_ = OneShotAllReducePlanSlot{
        .status = DevicePlanStatus::Ready,
        .plan = makePlan(view_epoch, 0, 1, workspace_.buffer().size()),
    };
    host_plan_.plan.peers[0] = self_rank_;
    host_plan_.plan.peer_buffer_offsets[0] = workspace_.buffer().offset();
}

PGResult<void> OneShotAllReduceAlgorithm::applyGroupView(
    const DeviceCollectiveRuntime::ResolvedGroupView& view) {
    if (view.self_active_index < 0) {
        invalidateHostPlan();
        return {};
    }
    const auto& participants = view.participants;

    auto plan = makePlan(view.epoch,
                         static_cast<uint32_t>(view.self_active_index),
                         static_cast<uint32_t>(participants.size()),
                         view.buffer_size);
    PG_VALIDATE_STATE(plan.chunk_bytes >= sizeof(uint32_t),
                      "One-shot peer workspace is too small");
    for (size_t index = 0; index < participants.size(); ++index) {
        plan.peers[index] = participants[index].in_group_rank;
        plan.peer_buffer_offsets[index] =
            participants[index].workspace.buffer_offset;
    }
    host_plan_ = OneShotAllReducePlanSlot{
        .status = DevicePlanStatus::Ready,
        .plan = plan,
    };
    return {};
}

void OneShotAllReduceAlgorithm::invalidateHostPlan() noexcept {
    host_plan_ = {};
}

bool OneShotAllReduceAlgorithm::ready() const noexcept {
    return host_plan_.status == DevicePlanStatus::Ready;
}

PGResult<void> OneShotAllReduceAlgorithm::enqueue(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    ReduceOp op, cudaStream_t stream, int32_t* failed_ranks_hint) const {
    PG_VALIDATE_STATE(ready(), "One-shot AllReduce Plan is not ready");
    PG_VALIDATE_ARG(isOneShotAllReduceCombinationSupported(datatype, op),
                    "One-shot datatype or reduction is unsupported");
    const OneShotAllReduceKernelArgs request{
        .send_buffer = send_buffer,
        .recv_buffer = recv_buffer,
        .count = static_cast<uint64_t>(count),
        .datatype = datatype,
        .op = op,
        .failed_ranks_hint = failed_ranks_hint,
    };
    PG_TRY_CUDA(launchOneShotAllReduceKernel(request, state_, stream));
    return {};
}

}  // namespace mooncake
