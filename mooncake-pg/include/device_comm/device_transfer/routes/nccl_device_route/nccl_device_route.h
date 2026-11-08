#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_NCCL_DEVICE_ROUTE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_NCCL_DEVICE_ROUTE_H

#include <memory>

#include "device_comm/device_transfer/routes/route_provider.h"

namespace mooncake {

struct NcclDeviceRouteOptions {
    bool enabled = false;
    uint32_t context_count = 4;
};

class NcclDeviceRoute : public RouteProvider {
   public:
    static constexpr std::string_view kRouteKey = "nccl-device";
    static constexpr uint32_t kEndpointVersion = 1;

    [[nodiscard]] static PGResult<std::unique_ptr<NcclDeviceRoute>> create(
        int device_index, GlobalRank self_rank, uint32_t max_world_size,
        const NcclDeviceRouteOptions& options);
    ~NcclDeviceRoute() noexcept override;

    // installEndpoints() synchronizes window registration across NCCL ranks;
    // both regions must exist even when local transfers do not use staging.
    bool requiresAllRegionsForInstallation() const noexcept override {
        return true;
    }
    PGResult<void> registerRegion(DeviceRegionKind kind, void* addr,
                                  size_t size) override;
    PGResult<void> unregisterRegion(DeviceRegionKind kind, void* addr,
                                    size_t size) override;
    std::optional<RouteEndpoint> localEndpoint() override;
    // Each coordinated installation prepares a new nccl communicator.
    PGResult<void> installEndpoints(const DeviceTransferSnapshot& snapshot,
                                    uint64_t reclaim_before_version) override;
    PGResult<std::vector<DeviceTransferRoute>> updateRoutes() override;
    void fillDeviceContext(DeviceRouteContext& context) const noexcept override;
    PGResult<void> shutdown() override;

   private:
    struct State;

    NcclDeviceRoute(GlobalRank self_rank, uint32_t max_world_size,
                    uint32_t context_count, std::unique_ptr<State> state);

    std::unique_ptr<State> state_;
    GlobalRank self_rank_;
    uint32_t max_world_size_;
    uint32_t context_count_;
    PGResult<void> drainCurrent();
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_NCCL_DEVICE_ROUTE_H
