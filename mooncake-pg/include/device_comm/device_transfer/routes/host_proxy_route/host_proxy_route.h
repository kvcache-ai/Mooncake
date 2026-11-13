#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "device_comm/device_transfer/routes/route_provider.h"

namespace mooncake {

class HostTransferProxy;
class LinkManager;
class TransferEngine;

struct HostProxyRouteOptions {
    bool enabled = true;
};

class HostProxyRoute : public RouteProvider {
   public:
    static constexpr std::string_view kRouteKey = "host-proxy";
    static constexpr uint32_t kEndpointVersion = 1;

    [[nodiscard]] static PGResult<std::unique_ptr<HostProxyRoute>> create(
        int device_index, TransferEngine& engine, LinkManager& link_manager,
        uint32_t max_world_size);
    ~HostProxyRoute() noexcept override;

    PGResult<void> registerRegion(DeviceRegionKind kind, void* addr,
                                  size_t size) override;
    PGResult<void> unregisterRegion(DeviceRegionKind kind, void* addr,
                                    size_t size) override;
    [[nodiscard]] std::optional<RouteEndpoint> localEndpoint() override;
    PGResult<void> installEndpoints(const DeviceTransferSnapshot& snapshot,
                                    uint64_t reclaim_before_version) override;
    [[nodiscard]] PGResult<std::vector<DeviceTransferRoute>> updateRoutes()
        override;
    void fillDeviceContext(DeviceRouteContext& context) const noexcept override;

    PGResult<void> shutdown() override;

   private:
    HostProxyRoute(TransferEngine& engine,
                   std::unique_ptr<HostTransferProxy> proxy,
                   std::string device_location, uint32_t max_world_size,
                   HostProxyCommandSlot* device_slots);

    TransferEngine& engine_;
    std::unique_ptr<HostTransferProxy> proxy_;
    std::string device_location_;
    uint32_t max_world_size_ = 0;
    HostProxyCommandSlot* device_slots_ = nullptr;
    bool shutdown_requested_ = false;
    std::vector<DeviceTransferRoute> pending_routes_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_H
