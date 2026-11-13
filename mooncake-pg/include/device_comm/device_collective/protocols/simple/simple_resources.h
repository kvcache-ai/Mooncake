#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_RESOURCES_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_RESOURCES_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "control_plane/control_types.h"
#include "device_comm/device_collective/resolved_group_view.h"
#include "device_comm/device_collective/protocols/simple/simple_types.cuh"
#include "device_comm/device_transfer/transfer_region.h"
#include "error_types.h"

namespace mooncake {

class ControlUpdateBuilder;
class DeviceTransferService;
class DeviceCollectiveWorkspace;

// One communicator's Simple signals and local [channel][peer] progress.
class SimpleResources {
   public:
    static PGResult<std::unique_ptr<SimpleResources>> create(
        DeviceTransferService& transfer_service, InGroupRank self_rank,
        uint32_t max_group_size);

    ~SimpleResources() noexcept;
    SimpleResources(const SimpleResources&) = delete;
    SimpleResources& operator=(const SimpleResources&) = delete;

    [[nodiscard]] SimpleBindings* state() const noexcept { return state_; }
    [[nodiscard]] const SimpleEndpoint& localEndpoint() const noexcept {
        return endpoint_;
    }

    PGResult<SimpleBindings> bindGroupView(const ResolvedGroupView& view) const;
    PGResult<SimpleWorkspace> bindWorkspace(
        DeviceCollectiveWorkspace& workspace, const CollectivePeer& send_peer,
        uint64_t bytes) const;

    PGResult<void> appendUpdate(ControlUpdateBuilder& builder,
                                const SimpleBindings& bindings) const;

   private:
    SimpleResources(RegionSlice signals,
                    SimplePipelineSignalLayout signal_layout,
                    DeviceTransferService& transfer_service,
                    InGroupRank self_rank) noexcept;

    RegionSlice signals_;
    SimplePipelineSignalLayout signal_layout_;
    SimpleEndpoint endpoint_;
    SimplePeerProgress* peer_progress_ = nullptr;
    SimpleBindings* state_ = nullptr;
    DeviceTransferService& transfer_service_;
    InGroupRank self_rank_;
    int device_index_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_RESOURCES_H
