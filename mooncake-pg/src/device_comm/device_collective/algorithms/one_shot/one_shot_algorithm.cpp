#include "device_comm/device_collective/algorithms/one_shot/one_shot_algorithm.h"

#include <utility>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/protocols/ll/ll_protocol.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "pg_utils.h"

namespace mooncake {

namespace {

struct OneShotEndpointMetadata {
    uint64_t buffer_offset = 0;
    uint64_t buffer_size = 0;
};

}  // namespace

OneShotAlgorithm::OneShotAlgorithm(RegionSlice packets,
                                   OneShotBufferLayout layout,
                                   const LLProtocol& ll)
    : AlgorithmProvider(DeviceCollectiveAlgorithm::OneShot, kAlgorithmKey,
                        kEndpointVersion, ll,
                        {OpType::AllReduce, OpType::AllGather}),
      packets_(std::move(packets)),
      layout_(layout) {}

PGResult<std::unique_ptr<OneShotAlgorithm>> OneShotAlgorithm::create(
    DeviceTransferService& transfer_service, uint32_t max_group_size,
    CollectiveRuntimeBindings collective, const LLProtocol& ll) {
    PG_VALIDATE_ARG(max_group_size > 0 && max_group_size <= kMaxNumRanks,
                    "One-shot group capacity is outside the supported range");
    PG_VALIDATE_ARG(ll.state() && collective.transfer_handle &&
                        collective.group_view && collective.control_mailbox &&
                        collective.invocation_state &&
                        collective.view_epoch_signals,
                    "One-shot device bindings are incomplete");
    const OneShotBufferLayout layout{max_group_size};
    PG_TRY(auto packets, transfer_service.allocatePeerAccessible(
                             layout.packetBytes(), alignof(LLPacket)));
    const int device_index = transfer_service.deviceIndex();
    auto algorithm = std::unique_ptr<OneShotAlgorithm>(
        new OneShotAlgorithm(std::move(packets), layout, ll));
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    PG_TRY_CUDA(
        cudaMemset(algorithm->packets_.addr(), 0, algorithm->packets_.size()));
    const OneShotDeviceState initial_state{
        .plan = {},
        .collective = collective,
        .ll = ll.state(),
    };
    PG_TRY(algorithm->state_, GpuBuffer<OneShotDeviceState>::fromHost(
                                  device_index, initial_state));
    return algorithm;
}

AlgorithmEndpoint OneShotAlgorithm::localEndpoint() const {
    return encodeEndpoint(OneShotEndpointMetadata{
        .buffer_offset = packets_.offset(), .buffer_size = packets_.size()});
}

PGResult<void> OneShotAlgorithm::updatePlan(
    const ResolvedGroupView& view, ControlUpdateBuilder& builder) const {
    PG_TRY(auto plan, buildPlan(view));
    PG_TRY(builder.fill<uint64_t>(static_cast<uint64_t*>(packets_.addr()), 0,
                                  packets_.size() / sizeof(uint64_t)));
    return appendPlanUpdate(builder, plan);
}

PGResult<void> OneShotAlgorithm::invalidatePlan(
    ControlUpdateBuilder& builder) const {
    return appendPlanUpdate(builder, OneShotPlan{});
}

PGResult<OneShotPlan> OneShotAlgorithm::buildPlan(
    const ResolvedGroupView& view) const {
    if (view.self_active_index < 0) return OneShotPlan{};
    const auto max_group_size = layout_.max_group_size;
    const auto index = static_cast<size_t>(view.self_active_index);
    PG_VALIDATE_STATE(index < view.participants.size() &&
                          view.participants.size() <= max_group_size &&
                          max_group_size <= kMaxNumRanks,
                      "One-shot participants are outside the group");
    OneShotPlan plan{
        .status = DevicePlanStatus::Ready,
        .layout = layout_,
        .packets = static_cast<LLPacket*>(packets_.addr()),
    };
    for (size_t peer = 0; peer < view.participants.size(); ++peer) {
        if (peer == index) continue;
        const auto& participant = view.participants[peer];
        const auto* endpoint = findEndpoint(participant.algorithms);
        PG_VALIDATE_STATE(endpoint,
                          "Active peer has no compatible one_shot endpoint");
        PG_TRY(auto metadata,
               decodeEndpoint<OneShotEndpointMetadata>(*endpoint));
        PG_VALIDATE_STATE(
            metadata.buffer_offset % alignof(LLPacket) == 0 &&
                metadata.buffer_size == layout_.packetBytes() &&
                !addOverflows(metadata.buffer_offset, metadata.buffer_size),
            "One-shot peer packet endpoint is invalid");
        plan.packet_offsets[peer] = metadata.buffer_offset;
    }
    return plan;
}

PGResult<void> OneShotAlgorithm::appendPlanUpdate(
    ControlUpdateBuilder& builder, const OneShotPlan& plan) const {
    return builder.copyBytes(&state_.get()->plan, &plan, sizeof(plan));
}

PGResult<void> OneShotAlgorithm::enqueueImpl(const AllReduceRequest& request,
                                             cudaStream_t stream) const {
    PG_TRY(launchOneShotAllReduceKernel(request, state_.get(), stream));
    return {};
}

PGResult<void> OneShotAlgorithm::enqueueImpl(const AllGatherRequest& request,
                                             cudaStream_t stream) const {
    PG_TRY(launchOneShotAllGatherKernel(request, state_.get(), stream));
    return {};
}

}  // namespace mooncake
