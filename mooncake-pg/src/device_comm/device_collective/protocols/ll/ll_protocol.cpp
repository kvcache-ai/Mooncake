#include "device_comm/device_collective/protocols/ll/ll_protocol.h"

#include <utility>

#include "device_comm/device_collective/device_control_update.h"
#include "device_comm/device_transfer/transfer_service.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {

namespace {

struct LLEndpointMetadata {
    uint64_t signal_offset = 0;
    uint32_t signal_count = 0;
};

}  // namespace

LLProtocol::LLProtocol(RegionSlice tag_recycle_signals, InGroupRank self_rank,
                       uint32_t max_group_size,
                       DeviceTransferService& transfer_service) noexcept
    : ProtocolProvider(kProtocolKey, kEndpointVersion),
      tag_recycle_signals_(std::move(tag_recycle_signals)),
      max_group_size_(max_group_size),
      transfer_service_(transfer_service),
      self_rank_(self_rank) {}

PGResult<std::unique_ptr<LLProtocol>> LLProtocol::create(
    DeviceTransferService& transfer_service, InGroupRank self_rank,
    uint32_t max_group_size) {
    PG_VALIDATE_ARG(max_group_size > 0 && max_group_size <= kMaxNumRanks,
                    "LL group capacity is outside the supported range");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_group_size,
        "LL self rank is outside the group");
    const uint64_t signal_bytes = uint64_t{kMaxDeviceCollectiveChannels} *
                                  kLLBarrierCount * max_group_size *
                                  sizeof(uint64_t);
    PG_TRY(auto tag_recycle_signals, transfer_service.allocatePeerAccessible(
                                         signal_bytes, alignof(uint64_t)));
    auto protocol = std::unique_ptr<LLProtocol>(
        new LLProtocol(std::move(tag_recycle_signals), self_rank,
                       max_group_size, transfer_service));
    PG_TRY(auto device_guard,
           GpuDeviceGuard::create(transfer_service.deviceIndex()));
    PG_TRY(protocol->progress_,
           GpuBuffer<LLChannelProgress>::allocate(
               transfer_service.deviceIndex(), kMaxDeviceCollectiveChannels));
    const LLChannelProgress initial_progress[kMaxDeviceCollectiveChannels] = {};
    PG_TRY_CUDA(cudaMemcpy(protocol->progress_.get(), initial_progress,
                           sizeof(initial_progress), cudaMemcpyHostToDevice));
    const LLState initial_state{
        .transfer_handle = transfer_service.deviceHandle(),
        .max_group_size = max_group_size,
        .tag_recycle_signals =
            static_cast<uint64_t*>(protocol->tag_recycle_signals_.addr()),
        .progress = protocol->progress_.get(),
    };
    PG_TRY(protocol->state_,
           GpuBuffer<LLState>::fromHost(transfer_service.deviceIndex(),
                                        initial_state));
    PG_TRY_CUDA(
        cudaMemset(protocol->tag_recycle_signals_.addr(), 0, signal_bytes));
    return protocol;
}

ProtocolEndpoint LLProtocol::localEndpoint() const {
    return encodeEndpoint(LLEndpointMetadata{
        .signal_offset = tag_recycle_signals_.offset(),
        .signal_count = static_cast<uint32_t>(tag_recycle_signals_.size() /
                                              sizeof(uint64_t))});
}

PGResult<void> LLProtocol::updateState(const ResolvedGroupView& view,
                                       ControlUpdateBuilder& builder) const {
    PG_TRY(auto state, makeState(view));
    return appendStateUpdate(builder, state);
}

PGResult<LLState> LLProtocol::makeState(const ResolvedGroupView& view) const {
    LLState next{
        .transfer_handle = transfer_service_.deviceHandle(),
        .max_group_size = max_group_size_,
        .tag_recycle_signals =
            static_cast<uint64_t*>(tag_recycle_signals_.addr()),
        .progress = progress_.get(),
    };
    if (view.self_active_index >= 0) {
        for (size_t index = 0; index < view.participants.size(); ++index) {
            const auto& peer = view.participants[index];
            if (peer.in_group_rank == self_rank_) continue;
            PG_VALIDATE_STATE(
                peer.in_group_rank >= 0 &&
                    static_cast<uint32_t>(peer.in_group_rank) < max_group_size_,
                "LL peer rank is outside the group");
            PG_TRY(auto route, transfer_service_.routeInfo(peer.global_rank));
            PG_VALIDATE_STATE(
                route.directly_addressable && route.native_atomics,
                "LL requires direct memory access with native atomics");
            const auto* endpoint = findEndpoint(peer.protocols);
            PG_VALIDATE_STATE(endpoint,
                              "Active peer has no compatible ll endpoint");
            PG_TRY(auto metadata,
                   decodeEndpoint<LLEndpointMetadata>(*endpoint));
            const auto& workspace = peer.workspace;
            const uint64_t signal_bytes =
                uint64_t{metadata.signal_count} * sizeof(uint64_t);
            PG_VALIDATE_STATE(
                metadata.signal_offset % alignof(uint64_t) == 0 &&
                    signal_bytes == tag_recycle_signals_.size() &&
                    !addOverflows(metadata.signal_offset, signal_bytes),
                "LL peer signal endpoint is invalid");
            PG_VALIDATE_STATE(
                !rangesOverlap(workspace.buffer_offset, workspace.buffer_size,
                               metadata.signal_offset, signal_bytes),
                "LL peer signals overlap the shared payload workspace");
            next.tag_recycle_signal_offsets[index] = metadata.signal_offset;
        }
    }
    return next;
}

PGResult<void> LLProtocol::appendStateUpdate(ControlUpdateBuilder& builder,
                                             const LLState& state) const {
    PG_TRY(builder.copyBytes(state_.get(), &state, sizeof(state)));
    PG_TRY(builder.fill<uint64_t>(
        static_cast<uint64_t*>(tag_recycle_signals_.addr()), 0,
        tag_recycle_signals_.size() / sizeof(uint64_t)));
    return builder.fill(progress_.get(), LLChannelProgress{}, progress_.size());
}

}  // namespace mooncake
