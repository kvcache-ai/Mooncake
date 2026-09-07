#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
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
// Device execution remains statically dispatched through DeviceRouteKind.
class RouteProvider {
   public:
    virtual ~RouteProvider() = default;

    [[nodiscard]] virtual std::string_view routeKey() const noexcept = 0;
    [[nodiscard]] virtual uint32_t routeVersion() const noexcept = 0;

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

    // Resolve the service-owned endpoint snapshot as one batch.
    // A missing peer or a missing matching route means that the provider
    // must clear any state previously associated with that slot and return
    // an Unreachable entry for it.
    [[nodiscard]] virtual PGResult<std::vector<DeviceTransferRoute>>
    resolveRoutes(
        std::span<const std::optional<DeviceTransferEndpoint>> endpoints) = 0;

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
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_ROUTE_PROVIDER_H
