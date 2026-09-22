#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_RESOURCES_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_RESOURCES_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "control_plane/control_types.h"
#include "device_comm/device_collective/device_collective.h"
#include "device_comm/device_collective/protocols/simple/simple_types.cuh"
#include "device_comm/device_transfer/transfer_region.h"

namespace mooncake {

class ControlUpdateBuilder;
class DeviceTransferService;

// One communicator's Simple signals and local [channel][peer] progress.
// Algorithms borrow these resources; their Plans do not own or reset them.
class SimpleResources {
   public:
    static PGResult<std::unique_ptr<SimpleResources>> create(
        DeviceTransferService& transfer_service,
        InGroupRank self_rank, uint32_t max_group_size);

    ~SimpleResources() noexcept;
    SimpleResources(const SimpleResources&) = delete;
    SimpleResources& operator=(const SimpleResources&) = delete;

    [[nodiscard]] uint32_t maxGroupSize() const noexcept {
        return signal_layout_.max_group_size;
    }
    [[nodiscard]] SimpleState<>* state() const noexcept { return state_; }
    [[nodiscard]] const SimpleEndpoint& localEndpoint() const noexcept {
        return endpoint_;
    }

    PGResult<void> applyGroupView(
        const DeviceCollectiveRuntime::ResolvedGroupView& view);
    PGResult<bool> requiresStaging(InGroupRank peer) const;

    // Executed with the new Plans at a quiescent boundary of this communicator.
    // Peers synchronize the new View before using the reset state.
    PGResult<void> appendUpdate(ControlUpdateBuilder& builder) const;

   private:
    SimpleResources(RegionSlice signals,
                    SimplePipelineSignalLayout<> signal_layout,
                    DeviceTransferService& transfer_service) noexcept;

    [[nodiscard]] size_t connectionBytes() const noexcept {
        return size_t{kMaxDeviceCollectiveChannels} *
               signal_layout_.max_group_size * sizeof(SimpleConnectionState);
    }

    RegionSlice signals_;
    SimplePipelineSignalLayout<> signal_layout_;
    SimpleEndpoint endpoint_;
    SimpleConnectionState* connections_ = nullptr;
    SimpleState<> host_state_;
    SimpleState<>* state_ = nullptr;
    DeviceTransferService& transfer_service_;
    int device_index_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_RESOURCES_H
