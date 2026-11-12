#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/struct_pack.hpp>

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

    [[nodiscard]] DeviceRouteType routeType() const noexcept {
        return route_type_;
    }
    [[nodiscard]] std::string_view routeKey() const noexcept {
        return route_key_;
    }
    [[nodiscard]] uint32_t routeVersion() const noexcept {
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
    virtual PGResult<void> installEndpoints(
        const DeviceTransferSnapshot& snapshot,
        uint64_t reclaim_before_version) = 0;

    // Switch resources after local DTS users stop.
    // Resolve the service-owned endpoint snapshot as one batch.
    // A missing peer or a missing matching route means that the provider
    // must clear any state previously associated with that slot and return
    // an Unreachable entry for it.
    [[nodiscard]] virtual PGResult<std::vector<DeviceTransferRoute>>
    updateRoutes() = 0;

    // Fill this provider's fields, preserving other providers' fields.
    virtual void fillDeviceContext(DeviceRouteContext&) const noexcept {}

    virtual PGResult<void> shutdown() = 0;

   protected:
    RouteProvider(DeviceRouteType route_type, std::string_view route_key,
                  uint32_t endpoint_version) noexcept
        : route_type_(route_type),
          route_key_(route_key),
          endpoint_version_(endpoint_version) {}

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

    template <typename T>
    [[nodiscard]] static std::vector<uint8_t> encodeEndpointMetadata(
        const T& metadata) {
        return struct_pack::serialize<std::vector<uint8_t>>(metadata);
    }

    template <typename T>
    [[nodiscard]] static PGResult<T> decodeEndpointMetadata(
        const RouteEndpoint& endpoint) {
        PG_VALIDATE_ARG(!endpoint.metadata.empty(), "serialized data is empty");
        T metadata;
        size_t consumed = 0;
        const auto error = struct_pack::deserialize_to(
            metadata, reinterpret_cast<const char*>(endpoint.metadata.data()),
            endpoint.metadata.size(), consumed);
        PG_VALIDATE_ARG(
            !error, "deserialization failed: " + std::string(error.message()));
        PG_VALIDATE_ARG(consumed == endpoint.metadata.size(),
                        "serialized data contains trailing bytes");
        return metadata;
    }

   private:
    DeviceRouteType route_type_;
    std::string_view route_key_;
    uint32_t endpoint_version_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H
