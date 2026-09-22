#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_RESOURCES_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_RESOURCES_H

#include <cstdint>
#include <memory>

#include "control_plane/control_types.h"
#include "device_comm/device_collective/device_collective.h"
#include "device_comm/device_collective/protocols/ll/ll_types.cuh"
#include "device_comm/device_transfer/transfer_region.h"

namespace mooncake {

class ControlUpdateBuilder;
class DeviceTransferService;

// One communicator's LL control signals and progress. Payload storage and
// peer scheduling are supplied by the algorithm using them.
class LLResources {
   public:
    static PGResult<std::unique_ptr<LLResources>> create(
        DeviceTransferService& transfer_service, InGroupRank self_rank,
        uint32_t max_group_size);

    ~LLResources() noexcept;
    LLResources(const LLResources&) = delete;
    LLResources& operator=(const LLResources&) = delete;

    [[nodiscard]] uint32_t maxGroupSize() const noexcept {
        return signal_layout_.max_group_size;
    }
    [[nodiscard]] LLState* state() const noexcept { return state_; }
    [[nodiscard]] const LLEndpoint& localEndpoint() const noexcept {
        return endpoint_;
    }

    PGResult<void> applyGroupView(
        const DeviceCollectiveRuntime::ResolvedGroupView& view);

    // Reset with the communicator's Plans; the View handshake precedes reuse.
    PGResult<void> appendUpdate(ControlUpdateBuilder& builder) const;

   private:
    LLResources(RegionSlice signals, LLSignalLayout signal_layout,
                int device_index) noexcept;

    RegionSlice signals_;
    LLSignalLayout signal_layout_;
    LLEndpoint endpoint_;
    LLState host_state_;
    LLState* state_ = nullptr;
    int device_index_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_RESOURCES_H
