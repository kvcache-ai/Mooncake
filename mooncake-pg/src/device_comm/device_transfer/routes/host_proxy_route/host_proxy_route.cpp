#include <chrono>
#include <string>

#include "device_comm/device_transfer/routes/host_proxy_route/host_proxy_route.h"
#include "device_comm/device_transfer/routes/host_proxy_route/host_transfer_proxy.h"
#include "memory_location.h"

namespace mooncake {
HostProxyRoute::HostProxyRoute(TransferEngine& engine,
                               LinkManager& link_manager,
                               uint32_t max_world_size)
    : engine_(engine),
      proxy_(std::make_unique<HostTransferProxy>(engine, link_manager,
                                                 max_world_size)),
      max_world_size_(max_world_size) {}

HostProxyRoute::~HostProxyRoute() noexcept = default;

PGResult<void> HostProxyRoute::initialize(int device_index) {
    PG_VALIDATE_STATE(!shutdown_requested_, "HostProxyRoute is shutting down");
    if (initialized_) return {};
    PG_TRY(proxy_->start());
    PG_TRY(device_slots_, proxy_->initializeDevice(device_index));
    device_location_ = GPU_PREFIX + std::to_string(device_index);
    initialized_ = true;
    return {};
}

HostProxyCommandSlot* HostProxyRoute::deviceCommandSlots() const noexcept {
    return device_slots_;
}

std::string_view HostProxyRoute::routeKey() const noexcept { return kRouteKey; }

uint32_t HostProxyRoute::routeVersion() const noexcept {
    return kEndpointVersion;
}

std::optional<RouteEndpoint> HostProxyRoute::localEndpoint() {
    return RouteEndpoint{
        .route_key = std::string(kRouteKey),
        .version = routeVersion(),
        // HostProxy currently resolves TE peers through LinkManager, so its
        // endpoint carries no route-specific metadata.
        .metadata = {},
    };
}

PGResult<std::vector<DeviceTransferRoute>> HostProxyRoute::resolveRoutes(
    std::span<const std::optional<DeviceTransferEndpoint>> endpoints) {
    PG_VALIDATE_STATE(initialized_, "HostProxyRoute is not initialized");
    PG_VALIDATE_ARG(
        endpoints.size() == max_world_size_,
        "host-proxy route endpoint snapshot size does not match max world "
        "size");

    std::vector<DeviceTransferRoute> routes(max_world_size_);
    for (GlobalRank rank = 0; rank < static_cast<GlobalRank>(max_world_size_);
         ++rank) {
        PG_TRY(auto endpoint, findEndpoint(endpoints[rank]));
        if (!endpoint) continue;

        routes[rank] = DeviceTransferRoute{
            .kind = DeviceRouteKind::HostProxy,
            .region_size = endpoints[rank]->region_size,
            .host_proxy =
                {
                    .remote_region_address = endpoints[rank]->region_address,
                },
        };
    }
    return routes;
}

PGResult<void> HostProxyRoute::registerRegion(DeviceRegionKind kind, void* addr,
                                              size_t size) {
    PG_VALIDATE_STATE(initialized_, "HostProxyRoute is not initialized");
    PG_VALIDATE_ARG(addr && size != 0, "host-proxy region is empty");

    switch (kind) {
        case DeviceRegionKind::PeerAccessible: {
            PG_TRY_TE(engine_.registerLocalMemory(addr, size, device_location_,
                                                  /*remote_accessible=*/true,
                                                  /*update_metadata=*/true));
            return {};
        }
        case DeviceRegionKind::LocalStaging: {
            // Source-only staging is local metadata: peers never address it
            // and no registration update needs to be published.
            PG_TRY_TE(engine_.registerLocalMemory(addr, size, device_location_,
                                                  /*remote_accessible=*/false,
                                                  /*update_metadata=*/false));
            return {};
        }
    }
    return makePGError(PGErrorCode::InvalidArgument,
                       "unknown host-proxy device region kind");
}

PGResult<void> HostProxyRoute::unregisterRegion(DeviceRegionKind kind,
                                                void* addr, size_t size) {
    PG_VALIDATE_STATE(initialized_, "HostProxyRoute is not initialized");
    PG_VALIDATE_ARG(addr && size != 0, "host-proxy region is empty");
    switch (kind) {
        case DeviceRegionKind::PeerAccessible:
            PG_TRY_TE(
                engine_.unregisterLocalMemory(addr, /*update_metadata=*/true));
            return {};
        case DeviceRegionKind::LocalStaging:
            PG_TRY_TE(
                engine_.unregisterLocalMemory(addr, /*update_metadata=*/false));
            return {};
    }
    return makePGError(PGErrorCode::InvalidArgument,
                       "unknown host-proxy device region kind");
}

PGResult<void> HostProxyRoute::quiesce() {
    PG_VALIDATE_STATE(initialized_, "HostProxyRoute is not initialized");
    return proxy_->waitUntilIdle();
}

PGResult<void> HostProxyRoute::shutdown() {
    if (shutdown_requested_) return {};
    if (!initialized_) {
        PG_TRY(proxy_->shutdown());
        shutdown_requested_ = true;
        return {};
    }
    PG_TRY(proxy_->waitUntilIdle(std::chrono::milliseconds(0)));
    PG_TRY(proxy_->shutdown());
    shutdown_requested_ = true;
    device_slots_ = nullptr;
    device_location_.clear();
    initialized_ = false;
    return {};
}

}  // namespace mooncake
