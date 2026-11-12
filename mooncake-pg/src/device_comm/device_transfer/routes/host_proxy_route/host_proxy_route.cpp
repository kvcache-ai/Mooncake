#include <string>
#include <utility>

#include "device_comm/device_transfer/routes/host_proxy_route/host_proxy_route.h"
#include "device_comm/device_transfer/routes/host_proxy_route/host_transfer_proxy.h"
#include "memory_location.h"

namespace mooncake {
HostProxyRoute::HostProxyRoute(TransferEngine& engine,
                               std::unique_ptr<HostTransferProxy> proxy,
                               std::string device_location,
                               uint32_t max_world_size,
                               HostProxyCommandSlot* device_slots)
    : RouteProvider(DeviceRouteType::HostProxy, kRouteKey, kEndpointVersion),
      engine_(engine),
      proxy_(std::move(proxy)),
      device_location_(std::move(device_location)),
      max_world_size_(max_world_size),
      device_slots_(device_slots) {}

PGResult<std::unique_ptr<HostProxyRoute>> HostProxyRoute::create(
    int device_index, TransferEngine& engine, LinkManager& link_manager,
    uint32_t max_world_size) {
    auto proxy = std::make_unique<HostTransferProxy>(engine, link_manager,
                                                     max_world_size);
    PG_TRY(proxy->start());
    PG_TRY(auto device_slots, proxy->initializeDevice(device_index));
    return std::unique_ptr<HostProxyRoute>(new HostProxyRoute(
        engine, std::move(proxy), GPU_PREFIX + std::to_string(device_index),
        max_world_size, device_slots));
}

HostProxyRoute::~HostProxyRoute() noexcept = default;

std::optional<RouteEndpoint> HostProxyRoute::localEndpoint() {
    return RouteEndpoint{
        .route_key = std::string(kRouteKey),
        .version = routeVersion(),
        // HostProxy currently resolves TE peers through LinkManager, so its
        // endpoint carries no route-specific metadata.
        .metadata = {},
    };
}

PGResult<void> HostProxyRoute::installEndpoints(
    const DeviceTransferSnapshot& snapshot, uint64_t) {
    const auto& endpoints = snapshot.endpoints;
    PG_VALIDATE_STATE(!shutdown_requested_, "HostProxyRoute is shutting down");
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
            .type = DeviceRouteType::HostProxy,
            .region_size = endpoints[rank]->region_size,
            .host_proxy =
                {
                    .remote_region_address = endpoints[rank]->region_address,
                },
        };
    }
    pending_routes_ = std::move(routes);
    return {};
}

PGResult<std::vector<DeviceTransferRoute>> HostProxyRoute::updateRoutes() {
    return std::move(pending_routes_);
}

void HostProxyRoute::fillDeviceContext(
    DeviceRouteContext& context) const noexcept {
    context.host_proxy = DeviceHostProxyContext{.command_slots = device_slots_};
}

PGResult<void> HostProxyRoute::registerRegion(DeviceRegionKind kind, void* addr,
                                              size_t size) {
    PG_VALIDATE_STATE(!shutdown_requested_, "HostProxyRoute is shutting down");
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

PGResult<void> HostProxyRoute::shutdown() {
    if (shutdown_requested_) return {};
    PG_TRY(proxy_->shutdown());
    shutdown_requested_ = true;
    device_slots_ = nullptr;
    device_location_.clear();
    return {};
}

}  // namespace mooncake
