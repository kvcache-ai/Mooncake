#include "device_comm/device_collective/algorithms/ring/ring_all_reduce.h"

#include <new>

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/simple/simple_resources.h"
#include "device_comm/device_transfer/transfer_service.h"

namespace mooncake {

RingAllReduceAlgorithm::RingAllReduceAlgorithm(
    DeviceCollectiveWorkspace& workspace, SimpleResources& simple,
    const DeviceTransferHandle* transfer_handle,
    const uint64_t* view_epoch_signals, InvocationState* invocation_state,
    ControlMailbox* control_mailbox, uint64_t timeout_ticks, int device_index,
    InGroupRank self_rank) noexcept
    : workspace_(workspace),
      simple_(simple),
      transfer_handle_(transfer_handle),
      view_epoch_signals_(view_epoch_signals),
      invocation_state_(invocation_state),
      control_mailbox_(control_mailbox),
      timeout_ticks_(timeout_ticks),
      device_index_(device_index),
      self_rank_(self_rank) {}

PGResult<std::unique_ptr<RingAllReduceAlgorithm>>
RingAllReduceAlgorithm::create(DeviceTransferService& transfer_service,
                               DeviceCollectiveWorkspace& workspace,
                               SimpleResources& simple,
                               const uint64_t* view_epoch_signals,
                               InvocationState* invocation_state,
                               ControlMailbox* control_mailbox,
                               uint64_t timeout_ticks, int device_index,
                               InGroupRank self_rank, uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size != 0, "Ring group capacity is zero");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "Ring self rank is outside the group");

    PG_VALIDATE_ARG(simple.maxGroupSize() == max_group_size,
                    "Ring Simple group capacity mismatch");
    const auto* const transfer_handle = transfer_service.deviceHandle();
    PG_VALIDATE_ARG(transfer_handle, "Ring transfer handle is null");
    PG_VALIDATE_ARG(view_epoch_signals, "Ring View-epoch signal slice is null");
    PG_VALIDATE_ARG(invocation_state, "Ring invocation state is null");
    PG_VALIDATE_ARG(control_mailbox, "Ring control mailbox is null");
    PG_VALIDATE_ARG(workspace.buffer().addr(), "Ring buffer address is null");

    auto algorithm =
        std::unique_ptr<RingAllReduceAlgorithm>(new RingAllReduceAlgorithm(
            workspace, simple, transfer_handle, view_epoch_signals,
            invocation_state, control_mailbox, timeout_ticks, device_index,
            self_rank));
    PG_TRY(algorithm->initializeDeviceState());
    return algorithm;
}

RingAllReduceAlgorithm::~RingAllReduceAlgorithm() noexcept {
    releaseDeviceState();
}

PGResult<void> RingAllReduceAlgorithm::initializeDeviceState() {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&state_),
                           sizeof(RingAllReduceDeviceState)));
    const RingAllReduceDeviceState initial_state{
        .plan = {},
        .view_epoch_signals = view_epoch_signals_,
        .invocation_state = invocation_state_,
        .control_mailbox = control_mailbox_,
        .simple = simple_.state(),
    };
    PG_TRY_CUDA(cudaMemcpy(state_, &initial_state,
                           sizeof(RingAllReduceDeviceState),
                           cudaMemcpyHostToDevice));
    return {};
}

void RingAllReduceAlgorithm::releaseDeviceState() noexcept {
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

RingAllReducePlan RingAllReduceAlgorithm::makePlan(
    uint64_t view_epoch, int32_t self_active_index, uint32_t participant_count,
    uint64_t buffer_size, InGroupRank predecessor, InGroupRank successor,
    uint64_t send_buffer_offset, char* staging_ptr) const {
    PG_ASSERT(buffer_size != 0 && buffer_size <= workspace_.buffer().size(),
              "Ring Plan buffer binding is invalid");

    return RingAllReducePlan{
        .transfer_handle = transfer_handle_,
        .timeout_ticks = timeout_ticks_,
        .view_epoch = view_epoch,
        .buffer_ptr = static_cast<char*>(workspace_.buffer().addr()),
        .buffer_size = buffer_size,
        .send_buffer_offset = send_buffer_offset,
        .staging_ptr = staging_ptr,
        .staging_size = staging_ptr ? buffer_size : 0,
        .self_active_index = self_active_index,
        .participant_count = participant_count,
        .predecessor = predecessor,
        .successor = successor,
    };
}

PGResult<void> RingAllReduceAlgorithm::appendPlanUpdate(
    ControlUpdateBuilder& builder) const {
    return builder.copyBytes(&state_->plan, &host_plan_, sizeof(host_plan_));
}

void RingAllReduceAlgorithm::useLocalOnly(uint64_t view_epoch) {
    host_plan_ = RingAllReducePlanSlot{
        .status = DevicePlanStatus::Ready,
        .plan =
            makePlan(view_epoch, 0, 1, workspace_.buffer().size(), self_rank_,
                     self_rank_, workspace_.buffer().offset(), nullptr),
    };
}

PGResult<void> RingAllReduceAlgorithm::applyGroupView(
    const DeviceCollectiveRuntime::ResolvedGroupView& view) {
    if (view.self_active_index < 0) {
        invalidateHostPlan();
        return {};
    }
    const auto& participants = view.participants;

    const auto active_index = static_cast<size_t>(view.self_active_index);
    const auto participant_count = participants.size();
    const auto predecessor =
        participants[(active_index + participant_count - 1) % participant_count]
            .in_group_rank;
    const auto& successor =
        participants[(active_index + 1) % participant_count];
    char* staging_ptr = nullptr;
    if (participant_count > 1) {
        PG_TRY(auto requires_staging,
               simple_.requiresStaging(successor.in_group_rank));
        if (requires_staging) {
            PG_TRY(auto staging, workspace_.staging());
            staging_ptr = static_cast<char*>(staging->addr());
        }
    }

    host_plan_ = RingAllReducePlanSlot{
        .status = DevicePlanStatus::Ready,
        .plan = makePlan(view.epoch, static_cast<int32_t>(active_index),
                         static_cast<uint32_t>(participant_count),
                         view.buffer_size, predecessor, successor.in_group_rank,
                         successor.workspace.buffer_offset, staging_ptr),
    };
    return {};
}

void RingAllReduceAlgorithm::invalidateHostPlan() noexcept { host_plan_ = {}; }

bool RingAllReduceAlgorithm::ready() const noexcept {
    return host_plan_.status == DevicePlanStatus::Ready;
}

PGResult<void> RingAllReduceAlgorithm::enqueue(
    const void* send_buffer, void* recv_buffer, size_t count, DataType datatype,
    ReduceOp op, cudaStream_t stream, int32_t* failed_ranks_hint) const {
    PG_VALIDATE_STATE(ready(), "Ring AllReduce Plan is not ready");
    const RingAllReduceKernelArgs request{
        .send_buffer = send_buffer,
        .recv_buffer = recv_buffer,
        .count = static_cast<uint64_t>(count),
        .datatype = datatype,
        .op = op,
        .failed_ranks_hint = failed_ranks_hint,
    };
    PG_TRY_CUDA(launchRingAllReduceKernel(request, state_, stream));
    return {};
}

}  // namespace mooncake
