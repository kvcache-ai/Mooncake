#include "device_comm/device_collective/algorithms/hierarchical/hierarchical_algorithm.h"

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_collective/protocols/ll/ll_protocol.h"
#include "device_comm/device_collective/protocols/simple/simple_protocol.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "pg_utils.h"

namespace mooncake {
namespace {
struct PacketEndpoint {
    uint64_t offset;
    uint64_t bytes;
};
}  // namespace

HierarchicalAlgorithm::HierarchicalAlgorithm(
    RegionSlice packets, OneShotBufferLayout layout,
    DeviceCollectiveWorkspace& workspace, const LLProtocol& ll,
    const SimpleProtocol& simple)
    : AlgorithmProvider(DeviceCollectiveAlgorithm::HierarchicalOneShotRing,
                        kAlgorithmKey, 1, {&ll, &simple}, {OpType::AllReduce}),
      packets_(std::move(packets)),
      layout_(layout),
      workspace_(workspace),
      simple_(simple) {}

PGResult<std::unique_ptr<HierarchicalAlgorithm>> HierarchicalAlgorithm::create(
    DeviceTransferService& transfer, uint32_t capacity,
    CollectiveRuntimeBindings bindings, DeviceCollectiveWorkspace& workspace,
    const LLProtocol& ll, const SimpleProtocol& simple) {
    const OneShotBufferLayout layout{capacity};
    PG_TRY(auto packets, transfer.allocatePeerAccessible(
                             layout.channelPacketBytes(), alignof(LLPacket)));
    auto provider =
        std::unique_ptr<HierarchicalAlgorithm>(new HierarchicalAlgorithm(
            std::move(packets), layout, workspace, ll, simple));
    const HierarchicalKernelState initial{.plan = {},
                                          .collective = bindings,
                                          .ll = ll.state(),
                                          .simple = simple.state()};
    PG_TRY(provider->state_, GpuBuffer<HierarchicalKernelState>::fromHost(
                                 transfer.deviceIndex(), initial));
    return provider;
}

AlgorithmEndpoint HierarchicalAlgorithm::localEndpoint() const {
    return encodeEndpoint(PacketEndpoint{packets_.offset(), packets_.size()});
}

PGResult<void> HierarchicalAlgorithm::updatePlan(
    const ResolvedGroupView& view, ControlUpdateBuilder& builder) const {
    HierarchicalKernelPlan plan;
    if (view.self_active_index < 0) return invalidatePlan(builder);
    plan.status = DevicePlanStatus::Ready;
    const auto count = view.participants.size();
    const auto self = static_cast<size_t>(view.self_active_index);
    if (count == 1)
        return builder.copyBytes(&state_.get()->plan, &plan, sizeof(plan));

    const auto& leaders = view.hierarchical_leader_indices;
    PG_VALIDATE_STATE(self < count && leaders.size() == count,
                      "Hierarchical leader assignments are unavailable");
    plan.local_leader = leaders[self];
    plan.layout = layout_;
    plan.packets = static_cast<LLPacket*>(packets_.addr());
    for (size_t peer = 0; peer < count; ++peer) {
        if (leaders[peer] != leaders[self]) {
            plan.remote_leader = leaders[peer];
            continue;
        }
        if (peer == self) continue;
        const auto* endpoint = findEndpoint(view.participants[peer].algorithms);
        PG_VALIDATE_STATE(endpoint,
                          "Hierarchical peer packet endpoint is missing");
        PG_TRY(auto metadata, decodeEndpoint<PacketEndpoint>(*endpoint));
        PG_VALIDATE_STATE(metadata.bytes == packets_.size() &&
                              metadata.offset % alignof(LLPacket) == 0 &&
                              !addOverflows(metadata.offset, metadata.bytes),
                          "Hierarchical peer packet endpoint is invalid");
        plan.packet_offsets[peer] = metadata.offset;
    }
    PG_VALIDATE_STATE(plan.remote_leader != leaders[self],
                      "Hierarchical prototype needs a remote leader");
    if (self == leaders[self]) {
        PG_VALIDATE_STATE(view.buffer_size >= OneShotBufferLayout::kChunkBytes *
                                                  kSimplePipelineDepth,
                          "Hierarchical Simple workspace is too small");
        PG_TRY(
            plan.workspace,
            simple_.bindWorkspace(
                workspace_, view.participants[plan.remote_leader].global_rank,
                view.buffer_size));
    }
    PG_TRY(builder.fill<uint64_t>(static_cast<uint64_t*>(packets_.addr()), 0,
                                  packets_.size() / sizeof(uint64_t)));
    return builder.copyBytes(&state_.get()->plan, &plan, sizeof(plan));
}

PGResult<void> HierarchicalAlgorithm::invalidatePlan(
    ControlUpdateBuilder& builder) const {
    const HierarchicalKernelPlan plan;
    return builder.copyBytes(&state_.get()->plan, &plan, sizeof(plan));
}

PGResult<void> HierarchicalAlgorithm::enqueueImpl(
    const AllReduceRequest& request, cudaStream_t stream) const {
    return launchHierarchicalSumKernel(request, state_.get(), stream);
}

}  // namespace mooncake
