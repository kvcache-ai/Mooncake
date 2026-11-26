#include "device_comm/device_collective/protocols/simple/simple_protocol.h"

#include <string>
#include <utility>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_collective/device_collective_workspace.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {

namespace {

struct SimpleEndpointMetadata {
    uint64_t signal_offset = 0;
    uint32_t signal_count = 0;
};

}  // namespace

SimpleProtocol::SimpleProtocol(RegionSlice signals,
                               SimplePipelineSignalLayout signal_layout,
                               DeviceTransferService& transfer_service,
                               InGroupRank self_rank) noexcept
    : ProtocolProvider(kProtocolKey, kEndpointVersion),
      signals_(std::move(signals)),
      signal_layout_(signal_layout),
      transfer_service_(transfer_service),
      self_rank_(self_rank) {}

PGResult<std::unique_ptr<SimpleProtocol>> SimpleProtocol::create(
    DeviceTransferService& transfer_service, InGroupRank self_rank,
    uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size > 0 && max_group_size <= kMaxNumRanks,
                    "Simple group capacity is outside the supported range");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "Simple self rank is outside the group");
    const auto signal_layout = SimplePipelineSignalLayout::make(max_group_size);
    const uint64_t signal_bytes =
        uint64_t{signal_layout.total_signal_count} * sizeof(uint64_t);
    PG_TRY(auto signals, transfer_service.allocatePeerAccessible(
                             signal_bytes, alignof(uint64_t)));
    auto protocol = std::unique_ptr<SimpleProtocol>(new SimpleProtocol(
        std::move(signals), signal_layout, transfer_service, self_rank));
    PG_TRY(auto device_guard,
           GpuDeviceGuard::create(transfer_service.deviceIndex()));
    PG_TRY(protocol->peer_progress_,
           GpuBuffer<SimplePeerProgress>::zeroed(
               transfer_service.deviceIndex(),
               size_t{kMaxDeviceCollectiveChannels} * max_group_size));
    PG_TRY_CUDA(cudaMemset(protocol->signals_.addr(), 0, signal_bytes));
    const SimpleState initial_state{
        .transfer_handle = transfer_service.deviceHandle(),
        .signals = static_cast<uint64_t*>(protocol->signals_.addr()),
        .signal_layout = signal_layout,
        .peer_progress = protocol->peer_progress_.get(),
    };
    PG_TRY(protocol->state_,
           GpuBuffer<SimpleState>::fromHost(transfer_service.deviceIndex(),
                                            initial_state));
    return protocol;
}

ProtocolEndpoint SimpleProtocol::localEndpoint() const {
    return encodeEndpoint(SimpleEndpointMetadata{
        .signal_offset = signals_.offset(),
        .signal_count = signal_layout_.total_signal_count});
}

PGResult<void> SimpleProtocol::updateState(
    const ResolvedGroupView& view, ControlUpdateBuilder& builder) const {
    PG_TRY(auto state, makeState(view));
    return appendStateUpdate(builder, state);
}

PGResult<SimpleState> SimpleProtocol::makeState(
    const ResolvedGroupView& view) const {
    SimpleState next{
        .transfer_handle = transfer_service_.deviceHandle(),
        .signals = static_cast<uint64_t*>(signals_.addr()),
        .signal_layout = signal_layout_,
        .peer_progress = peer_progress_.get(),
    };
    if (view.self_active_index >= 0) {
        for (size_t index = 0; index < view.participants.size(); ++index) {
            const auto& peer = view.participants[index];
            if (peer.in_group_rank == self_rank_) continue;
            PG_VALIDATE_STATE(peer.in_group_rank >= 0 &&
                                  static_cast<uint32_t>(peer.in_group_rank) <
                                      signal_layout_.max_group_size,
                              "Simple peer rank is outside the group");
            const auto* endpoint = findEndpoint(peer.protocols);
            PG_VALIDATE_STATE(endpoint,
                              "Active peer has no compatible simple endpoint");
            PG_TRY(auto metadata,
                   decodeEndpoint<SimpleEndpointMetadata>(*endpoint));
            const auto& workspace = peer.workspace;
            const uint64_t signal_bytes =
                uint64_t{metadata.signal_count} * sizeof(uint64_t);
            PG_VALIDATE_STATE(
                metadata.signal_count >= signal_layout_.total_signal_count &&
                    metadata.signal_offset % alignof(uint64_t) == 0 &&
                    !addOverflows(metadata.signal_offset, signal_bytes),
                "Simple peer signal endpoint is invalid");
            PG_VALIDATE_STATE(
                !rangesOverlap(workspace.buffer_offset, workspace.buffer_size,
                               metadata.signal_offset, signal_bytes),
                "Simple peer signals overlap the payload workspace");
            next.signal_offsets[index] = metadata.signal_offset;
        }
    }
    return next;
}

PGResult<SimpleWorkspace> SimpleProtocol::bindWorkspace(
    DeviceCollectiveWorkspace& workspace, GlobalRank send_rank,
    uint64_t bytes) const {
    PG_VALIDATE_STATE(bytes != 0 && bytes <= workspace.buffer().size(),
                      "Simple workspace capacity is invalid");
    SimpleWorkspace result{static_cast<char*>(workspace.buffer().addr()), bytes,
                           nullptr};
    PG_TRY(auto route, transfer_service_.routeInfo(send_rank));
    PG_VALIDATE_STATE(route.reachable, "transfer peer " +
                                           std::to_string(send_rank) +
                                           " has no device transfer route");
    if (!route.directly_addressable) {
        PG_TRY(auto staging, workspace.staging());
        result.staging = static_cast<char*>(staging->addr());
    }
    return result;
}

PGResult<void> SimpleProtocol::appendStateUpdate(
    ControlUpdateBuilder& builder, const SimpleState& state) const {
    PG_TRY(builder.copyBytes(state_.get(), &state, sizeof(state)));
    PG_TRY(builder.fill<uint64_t>(static_cast<uint64_t*>(signals_.addr()), 0,
                                  signal_layout_.total_signal_count));
    return builder.fill(peer_progress_.get(), SimplePeerProgress{},
                        peer_progress_.size());
}

}  // namespace mooncake
