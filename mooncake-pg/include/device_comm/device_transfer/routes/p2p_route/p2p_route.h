#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_H

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "device_comm/device_transfer/routes/route_provider.h"

namespace mooncake {

namespace device {
class P2pTransport;
}

class P2pRoute : public RouteProvider {
   public:
    static constexpr std::string_view kRouteKey = "p2p";
    static constexpr uint32_t kEndpointVersion = 1;

    P2pRoute(device::P2pTransport& transport, void* local_region,
             int device_index, GlobalRank self_rank, uint32_t max_world_size);

    [[nodiscard]] std::string_view routeKey() const noexcept override;
    [[nodiscard]] uint32_t routeVersion() const noexcept override;
    [[nodiscard]] std::optional<RouteEndpoint> localEndpoint() override;
    [[nodiscard]] PGResult<std::vector<DeviceTransferRoute>> resolveRoutes(
        std::span<const std::optional<DeviceTransferEndpoint>> endpoints)
        override;

   private:
    [[nodiscard]] std::vector<int32_t> localHandle() const;

    device::P2pTransport& transport_;
    void* local_region_ = nullptr;
    int device_index_ = -1;
    GlobalRank self_rank_ = kInvalidGlobalRank;
    uint32_t max_world_size_ = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_H
