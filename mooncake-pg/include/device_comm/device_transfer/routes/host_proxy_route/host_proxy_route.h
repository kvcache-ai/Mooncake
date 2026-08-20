#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_H

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "device_comm/device_transfer/routes/route_provider.h"

namespace mooncake {

class HostTransferProxy;
class LinkManager;
class TransferEngine;
struct HostProxyCommandSlot;

class HostProxyRoute : public RouteProvider {
   public:
    static constexpr std::string_view kRouteKey = "host-proxy";
    static constexpr uint32_t kEndpointVersion = 1;

    HostProxyRoute(TransferEngine& engine, LinkManager& link_manager,
                   uint32_t max_world_size);
    ~HostProxyRoute() noexcept override;

    PGResult<void> initialize(int device_index);
    [[nodiscard]] HostProxyCommandSlot* deviceCommandSlots() const noexcept;

    [[nodiscard]] std::string_view routeKey() const noexcept override;
    [[nodiscard]] uint32_t routeVersion() const noexcept override;
    [[nodiscard]] std::optional<RouteEndpoint> localEndpoint() override;
    [[nodiscard]] PGResult<std::vector<DeviceTransferRoute>> resolveRoutes(
        std::span<const std::optional<DeviceTransferEndpoint>> endpoints)
        override;

    PGResult<void> quiesce() override;
    PGResult<void> shutdown() override;

   private:
    std::unique_ptr<HostTransferProxy> proxy_;
    uint32_t max_world_size_ = 0;
    HostProxyCommandSlot* device_slots_ = nullptr;
    bool initialized_ = false;
    bool shutdown_requested_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_H
