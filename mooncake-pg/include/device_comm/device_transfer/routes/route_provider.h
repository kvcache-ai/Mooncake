#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "control_plane/control_types.h"
#include "device_comm/device_transfer/transfer_types.cuh"
#include "error_types.h"

namespace mooncake {

// Host-side control path for one way of reaching peers. Implementations manage
// route-specific resources and metadata, but only borrow the service region.
// Device execution remains statically dispatched through DeviceRouteKind.
class RouteProvider {
   public:
    virtual ~RouteProvider() = default;

    [[nodiscard]] virtual std::string_view routeKey() const noexcept = 0;
    [[nodiscard]] virtual uint32_t routeVersion() const noexcept = 0;

    // An unavailable local route returns std::nullopt instead of publishing an
    // unusable endpoint.
    [[nodiscard]] virtual std::optional<RouteEndpoint> localEndpoint() = 0;

    // Resolve the service-owned endpoint snapshot as one batch.
    // A missing peer or a missing matching route means that the provider
    // must clear any state previously associated with that slot and return
    // an Unreachable entry for it.
    [[nodiscard]] virtual PGResult<std::vector<DeviceTransferRoute>>
    resolveRoutes(
        std::span<const std::optional<DeviceTransferEndpoint>> endpoints) = 0;

    virtual PGResult<void> quiesce() { return {}; }
    virtual PGResult<void> shutdown() { return {}; }

   protected:
    [[nodiscard]] PGResult<const RouteEndpoint*> findEndpoint(
        const std::optional<DeviceTransferEndpoint>& endpoint) const {
        if (!endpoint) return nullptr;

        const RouteEndpoint* match = nullptr;
        for (const auto& route_endpoint : endpoint->routes) {
            if (route_endpoint.route_key != routeKey() ||
                route_endpoint.version != routeVersion()) {
                continue;
            }
            PG_ASSERT(
                !match,
                "peer endpoint contains a duplicate route key and version");
            match = &route_endpoint;
        }
        return match;
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H
