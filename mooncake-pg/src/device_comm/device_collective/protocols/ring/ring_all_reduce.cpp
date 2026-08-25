#include "device_comm/device_collective/protocols/ring/ring_all_reduce.h"

#include <algorithm>
#include <new>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_primitives/payload_writer.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "pg_utils.h"

namespace mooncake {
namespace {

bool rangesOverlap(uint64_t first_offset, uint64_t first_size,
                   uint64_t second_offset, uint64_t second_size) noexcept {
    if (addOverflows(first_offset, first_size) ||
        addOverflows(second_offset, second_size)) {
        return true;
    }
    return first_offset < second_offset + second_size &&
           second_offset < first_offset + first_size;
}

bool endpointSupports(const DeviceCollectiveEndpoint& collective,
                      const RingAllReduceEndpoint& protocol,
                      uint64_t required_buffer_size,
                      uint32_t required_signal_count) noexcept {
    if (collective.buffer_size < required_buffer_size ||
        protocol.signal_count < required_signal_count) {
        return false;
    }
    const uint64_t signal_bytes =
        static_cast<uint64_t>(protocol.signal_count) * sizeof(uint64_t);
    return !rangesOverlap(collective.buffer_offset, collective.buffer_size,
                          protocol.signal_offset, signal_bytes);
}

}  // namespace

RingAllReduceProtocol::RingAllReduceProtocol(
    DeviceTransferService& transfer_service,
    DeviceCollectiveWorkspace& workspace,
    const DeviceTransferHandle* transfer_handle,
    DeviceCollectiveInvocationState* invocation_state,
    DeviceCollectiveRecoveryMailbox* recovery_mailbox, uint64_t timeout_ticks,
    int device_index, InGroupRank self_rank, uint32_t max_group_size,
    RegionSlice signals, RingSignalLayout signal_layout) noexcept
    : transfer_service_(transfer_service),
      workspace_(workspace),
      transfer_handle_(transfer_handle),
      invocation_state_(invocation_state),
      recovery_mailbox_(recovery_mailbox),
      timeout_ticks_(timeout_ticks),
      device_index_(device_index),
      self_rank_(self_rank),
      max_group_size_(max_group_size),
      signals_(std::move(signals)),
      signal_layout_(signal_layout),
      endpoint_{.signal_offset = signals_.offset(),
                .signal_count = signal_layout_.total_signal_count} {}

PGResult<std::unique_ptr<RingAllReduceProtocol>> RingAllReduceProtocol::create(
    DeviceTransferService& transfer_service,
    DeviceCollectiveWorkspace& workspace,
    DeviceCollectiveInvocationState* invocation_state,
    DeviceCollectiveRecoveryMailbox* recovery_mailbox, uint64_t timeout_ticks,
    int device_index, InGroupRank self_rank, uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size != 0, "Ring group capacity is zero");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "Ring self rank is outside the group");

    const auto signal_layout = RingSignalLayout::make(max_group_size);
    const auto* const transfer_handle = transfer_service.deviceHandle();
    PG_VALIDATE_ARG(transfer_handle, "Ring transfer handle is null");
    PG_VALIDATE_ARG(invocation_state, "Ring invocation state is null");
    PG_VALIDATE_ARG(recovery_mailbox, "Ring recovery mailbox is null");
    PG_VALIDATE_ARG(workspace.buffer().addr(), "Ring buffer address is null");

    const uint64_t signal_bytes =
        static_cast<uint64_t>(signal_layout.total_signal_count) *
        sizeof(uint64_t);
    PG_TRY(auto signals, transfer_service.allocatePeerAccessible(
                             signal_bytes, alignof(uint64_t)));

    auto protocol =
        std::unique_ptr<RingAllReduceProtocol>(new RingAllReduceProtocol(
            transfer_service, workspace, transfer_handle, invocation_state,
            recovery_mailbox, timeout_ticks, device_index, self_rank,
            max_group_size, std::move(signals), signal_layout));
    PG_TRY(protocol->initializeDeviceState());
    return protocol;
}

RingAllReduceProtocol::~RingAllReduceProtocol() noexcept {
    releaseDeviceState();
}

PGResult<void> RingAllReduceProtocol::initializeDeviceState() {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&state_),
                           sizeof(RingAllReduceDeviceState)));
    const RingAllReduceDeviceState initial_state{
        .plan = {},
        .invocation_state = invocation_state_,
        .recovery_mailbox = recovery_mailbox_,
    };
    PG_TRY_CUDA(cudaMemcpy(state_, &initial_state,
                           sizeof(RingAllReduceDeviceState),
                           cudaMemcpyHostToDevice));
    PG_TRY_CUDA(cudaMemset(signals_.addr(), 0, signals_.size()));
    return {};
}

void RingAllReduceProtocol::releaseDeviceState() noexcept {
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

RingAllReducePlan RingAllReduceProtocol::makePlan(int32_t self_active_index,
                                                  uint32_t participant_count,
                                                  uint64_t buffer_size,
                                                  RingPeerTarget predecessor,
                                                  RingPeerTarget successor,
                                                  char* staging_ptr) const {
    PG_ASSERT(buffer_size != 0 && buffer_size <= workspace_.buffer().size(),
              "Ring Plan buffer binding is invalid");

    return RingAllReducePlan{
        .transfer_handle = transfer_handle_,
        .timeout_ticks = timeout_ticks_,
        .buffer_ptr = static_cast<char*>(workspace_.buffer().addr()),
        .buffer_size = buffer_size,
        .signal_ptr = static_cast<uint64_t*>(signals_.addr()),
        .signal_layout = signal_layout_,
        .staging_ptr = staging_ptr,
        .staging_size = staging_ptr ? buffer_size : 0,
        .self_rank = self_rank_,
        .self_active_index = self_active_index,
        .participant_count = participant_count,
        .predecessor = predecessor,
        .successor = successor,
    };
}

PGResult<void> RingAllReduceProtocol::appendPlanUpdate(
    ControlUpdateBuilder& builder) const {
    // A Plan update also resets all protocol progress state. Keeping the reset
    // and Plan copy in one ControlUpdate makes replacing Published safe.
    PG_VALIDATE_STATE(signals_.size() % sizeof(uint64_t) == 0,
                      "Ring signal storage is not uint64-aligned");
    PG_TRY(builder.fillU64(static_cast<uint64_t*>(signals_.addr()), 0,
                           signals_.size() / sizeof(uint64_t)));
    PG_TRY(builder.fillU64(state_->next_step_sequences, 1,
                           kMaxDeviceCollectiveChannels * kRingPipelineSlots));
    PG_TRY(builder.fillU64(state_->next_recv_buffer_ready_sequences, 1,
                           kMaxDeviceCollectiveChannels));
    PG_TRY(builder.copyBytes(&state_->plan, &host_plan_, sizeof(host_plan_)));
    return {};
}

void RingAllReduceProtocol::useLocalOnly() {
    const RingPeerTarget self{.in_group_rank = self_rank_};
    host_plan_ = RingAllReducePlanSlot{
        .status = DevicePlanStatus::Ready,
        .plan = makePlan(0, 1, workspace_.buffer().size(), self, self, nullptr),
    };
}

PGResult<void> RingAllReduceProtocol::applyGroupView(const GroupView& view) {
    PG_ASSERT(view.max_group_size == static_cast<int32_t>(max_group_size_),
              "Ring group capacity changed");

    std::vector<InGroupRank> participants;
    participants.reserve(view.rank_order.size());
    for (InGroupRank in_group_rank = 0;
         static_cast<size_t>(in_group_rank) < view.rank_order.size();
         ++in_group_rank) {
        const auto global_rank = view.rank_order[in_group_rank];
        const auto& member = view.members[global_rank];
        if (!member.isActive()) continue;
        participants.push_back(in_group_rank);
    }

    const auto self =
        std::find(participants.begin(), participants.end(), self_rank_);
    if (self == participants.end()) {
        invalidateHostPlan();
        return {};
    }

    for (const InGroupRank in_group_rank : participants) {
        const auto global_rank = view.rank_order[in_group_rank];
        const auto& member = view.members[global_rank];
        if (member.endpoint &&
            member.endpoint->device_collective.ring_all_reduce) {
            continue;
        }
        // Manual membership management may temporarily keep a failed rank
        // Active after its endpoint disappears. This view cannot form a safe
        // topology, so make the Plan unavailable until a later view restores
        // the endpoint or deactivates that rank.
        invalidateHostPlan();
        return {};
    }

    // Every rank must derive the same channel and slot layout even though its
    // registered buffer has an independent offset and may have a different
    // capacity. Bind the largest prefix available on every active rank.
    uint64_t common_buffer_size = workspace_.buffer().size();
    for (const InGroupRank in_group_rank : participants) {
        const auto global_rank = view.rank_order[in_group_rank];
        PG_TRY(auto collective_endpoint, workspace_.endpoint(global_rank));
        common_buffer_size =
            std::min(common_buffer_size, collective_endpoint.buffer_size);
    }
    const auto active_index =
        static_cast<size_t>(std::distance(participants.begin(), self));
    const auto participant_count = participants.size();
    const auto predecessor =
        participants[(active_index + participant_count - 1) %
                     participant_count];
    const auto successor = participants[(active_index + 1) % participant_count];
    auto targetFor =
        [&](InGroupRank in_group_rank) -> PGResult<RingPeerTarget> {
        const auto global_rank = view.rank_order[in_group_rank];
        const auto& member = view.members[global_rank];
        const auto& protocol_endpoint =
            *member.endpoint->device_collective.ring_all_reduce;
        PG_TRY(auto collective_endpoint, workspace_.endpoint(global_rank));
        PG_ASSERT(endpointSupports(collective_endpoint, protocol_endpoint,
                                   common_buffer_size,
                                   signal_layout_.total_signal_count),
                  "active Ring peer ", in_group_rank,
                  " has insufficient or overlapping resources: "
                  "buffer_offset=",
                  collective_endpoint.buffer_offset,
                  ", buffer_size=", collective_endpoint.buffer_size,
                  ", signal_offset=", protocol_endpoint.signal_offset,
                  ", signal_count=", protocol_endpoint.signal_count);
        return RingPeerTarget{
            .global_rank = global_rank,
            .in_group_rank = in_group_rank,
            .buffer_offset = collective_endpoint.buffer_offset,
            .signal_offset = protocol_endpoint.signal_offset,
        };
    };

    PG_TRY(auto predecessor_target, targetFor(predecessor));
    PG_TRY(auto successor_target, targetFor(successor));
    char* staging_ptr = nullptr;
    if (participant_count > 1) {
        PG_TRY(auto requires_staging,
               payloadWriterRequiresStaging(transfer_service_,
                                            successor_target.global_rank));
        if (requires_staging) {
            PG_TRY(auto staging, workspace_.staging());
            staging_ptr = static_cast<char*>(staging->addr());
        }
    }

    host_plan_ = RingAllReducePlanSlot{
        .status = DevicePlanStatus::Ready,
        .plan = makePlan(static_cast<int32_t>(active_index),
                         static_cast<uint32_t>(participant_count),
                         common_buffer_size, predecessor_target,
                         successor_target, staging_ptr),
    };
    return {};
}

void RingAllReduceProtocol::invalidateHostPlan() noexcept { host_plan_ = {}; }

bool RingAllReduceProtocol::ready() const noexcept {
    return host_plan_.status == DevicePlanStatus::Ready;
}

const RingAllReduceEndpoint& RingAllReduceProtocol::localEndpoint()
    const noexcept {
    return endpoint_;
}

PGResult<void> RingAllReduceProtocol::enqueue(
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
