#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "control_plane/control_types.h"
#include "device_comm/device_transfer/transfer_types.cuh"
#include "error_types.h"

namespace mooncake {

// Host-side role of one region registered with a route. PeerAccessible is the
// published remote target region; LocalStaging is a source-only local region.
enum class DeviceRegionKind : uint32_t {
    PeerAccessible = 0,
    LocalStaging = 1,
};

// Host-side control path for one way of reaching peers. Implementations manage
// route-specific resources and metadata, but only borrow DTS backing regions.
// Device execution remains statically dispatched through DeviceRouteType.
class RouteProvider {
   public:
    virtual ~RouteProvider() = default;

    [[nodiscard]] DeviceRouteType type() const noexcept { return route_type_; }
    [[nodiscard]] std::string_view key() const noexcept { return route_key_; }
    [[nodiscard]] uint32_t endpointVersion() const noexcept {
        return endpoint_version_;
    }

    // Whether all backing regions must be supplied through registerRegion()
    // before installEndpoints(), including regions not used by local transfers.
    [[nodiscard]] virtual bool requiresAllRegionsForInstallation()
        const noexcept {
        return false;
    }

    // Prepare one complete DTS backing region for this route. Providers only
    // borrow the allocation and must undo the same preparation in
    // unregisterRegion().
    virtual PGResult<void> registerRegion(DeviceRegionKind kind, void* addr,
                                          size_t size) = 0;
    virtual PGResult<void> unregisterRegion(DeviceRegionKind kind, void* addr,
                                            size_t size) = 0;

    // An unavailable local route returns std::nullopt instead of publishing an
    // unusable endpoint.
    [[nodiscard]] virtual std::optional<RouteEndpoint> localEndpoint() = 0;

    // Prepare resources for the snapshot while current routes remain usable.
    // Reclaim generations with versions below reclaim_before_version.
    // Return routes indexed by GlobalRank, with Unreachable entries for missing
    // peers or missing matching routes.
    [[nodiscard]] virtual PGResult<std::vector<DeviceTransferRoute>>
    installEndpoints(const DeviceTransferSnapshot& snapshot,
                     uint64_t reclaim_before_version) = 0;

    // Switch resources after local DTS users stop.
    virtual PGResult<void> updateRoutes() = 0;

    // Fill this provider's fields, preserving other providers' fields.
    virtual void fillDeviceContext(DeviceRouteContext&) const noexcept {}

    virtual PGResult<void> shutdown() = 0;

   protected:
    RouteProvider(DeviceRouteType route_type, std::string_view route_key,
                  uint32_t endpoint_version) noexcept
        : route_type_(route_type),
          route_key_(route_key),
          endpoint_version_(endpoint_version) {}

    template <typename T>
    [[nodiscard]] RouteEndpoint encodeEndpoint(const T& metadata) const {
        return RouteEndpoint::encode(key(), endpointVersion(), metadata);
    }

    [[nodiscard]] const RouteEndpoint* findEndpoint(
        std::span<const RouteEndpoint> endpoints) const {
        return findProviderEndpoint(endpoints, key(), endpointVersion());
    }

    template <typename T>
    [[nodiscard]] PGResult<T> decodeEndpoint(
        const RouteEndpoint& endpoint) const {
        return endpoint.decode<T>(key(), endpointVersion());
    }

   private:
    DeviceRouteType route_type_;
    std::string_view route_key_;
    uint32_t endpoint_version_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H
