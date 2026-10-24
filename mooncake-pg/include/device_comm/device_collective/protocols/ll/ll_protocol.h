#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PROTOCOL_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PROTOCOL_H

#include <cstdint>
#include <memory>

#include "control_plane/control_types.h"
#include "device_comm/device_collective/resolved_group_view.h"
#include "device_comm/device_collective/protocols/protocol_provider.h"
#include "device_comm/device_collective/protocols/ll/ll_types.cuh"
#include "device_comm/device_transfer/transfer_region.h"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class ControlUpdateBuilder;
class DeviceTransferService;

// One communicator's LL tag-recycle signals and per-channel progress.
class LLProtocol final : public ProtocolProvider {
   public:
    static constexpr std::string_view kProtocolKey = "ll";
    static constexpr uint32_t kEndpointVersion = 1;

    static PGResult<std::unique_ptr<LLProtocol>> create(
        DeviceTransferService& transfer_service, InGroupRank self_rank,
        uint32_t max_group_size);

    ~LLProtocol() noexcept override = default;
    LLProtocol(const LLProtocol&) = delete;
    LLProtocol& operator=(const LLProtocol&) = delete;

    [[nodiscard]] LLState* state() const noexcept { return state_.get(); }
    [[nodiscard]] ProtocolEndpoint localEndpoint() const override;

    PGResult<void> updateState(const ResolvedGroupView& view,
                               ControlUpdateBuilder& builder) const override;

   private:
    LLProtocol(RegionSlice tag_recycle_signals, InGroupRank self_rank,
               uint32_t max_group_size,
               DeviceTransferService& transfer_service) noexcept;

    PGResult<LLState> makeState(const ResolvedGroupView& view) const;
    PGResult<void> appendStateUpdate(ControlUpdateBuilder& builder,
                                     const LLState& state) const;

    RegionSlice tag_recycle_signals_;
    uint32_t max_group_size_;
    GpuBuffer<LLChannelProgress> progress_;
    GpuBuffer<LLState> state_;
    DeviceTransferService& transfer_service_;
    InGroupRank self_rank_ = kInvalidInGroupRank;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PROTOCOL_H
