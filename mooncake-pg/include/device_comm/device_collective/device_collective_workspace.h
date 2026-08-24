#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_WORKSPACE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_WORKSPACE_H

#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "control_plane/control_types.h"
#include "device_comm/device_transfer/transfer_region.h"
#include "error_types.h"

namespace mooncake {

class DeviceTransferService;

// Context-wide payload storage shared by device collective protocols. The
// buffer is published once as the rank-level DeviceCollectiveEndpoint during
// registerAgent, alongside DeviceTransferEndpoint. Staging is local-only and
// allocated lazily when a route first needs it.
//
// StrongStream serializes protocol kernels that borrow these buffers, so one
// published buffer and one optional staging slice can be shared by every
// communicator in the context.
class DeviceCollectiveWorkspace {
   public:
    static PGResult<std::unique_ptr<DeviceCollectiveWorkspace>> create(
        DeviceTransferService& transfer_service, GlobalRank self_rank,
        uint32_t max_world_size, size_t buffer_size);

    ~DeviceCollectiveWorkspace() noexcept = default;

    DeviceCollectiveWorkspace(const DeviceCollectiveWorkspace&) = delete;
    DeviceCollectiveWorkspace& operator=(const DeviceCollectiveWorkspace&) =
        delete;

    [[nodiscard]] const RegionSlice& buffer() const noexcept;
    PGResult<const RegionSlice*> staging();

    [[nodiscard]] const DeviceCollectiveEndpoint& localEndpoint()
        const noexcept;

    PGResult<void> installPeerEndpoint(
        GlobalRank rank, const DeviceCollectiveEndpoint& endpoint);
    PGResult<DeviceCollectiveEndpoint> endpoint(GlobalRank rank) const;

   private:
    DeviceCollectiveWorkspace(DeviceTransferService& transfer_service,
                              RegionSlice buffer, GlobalRank self_rank,
                              uint32_t max_world_size,
                              DeviceCollectiveEndpoint local_endpoint);

    DeviceTransferService& transfer_service_;
    RegionSlice buffer_;
    std::optional<RegionSlice> staging_;
    GlobalRank self_rank_ = kInvalidGlobalRank;
    DeviceCollectiveEndpoint local_endpoint_;
    std::vector<std::optional<DeviceCollectiveEndpoint>> endpoints_;
    mutable std::mutex mutex_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_COLLECTIVE_WORKSPACE_H
