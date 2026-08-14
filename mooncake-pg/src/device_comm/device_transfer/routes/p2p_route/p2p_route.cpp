#include "device_comm/device_transfer/routes/p2p_route/p2p_route.h"

#include <cstring>

#include <transport/device/device_transport.h>

#include "gpu_runtime.h"

namespace mooncake {
namespace {

std::vector<uint8_t> encodeHandle(const std::vector<int32_t>& handle) {
    std::vector<uint8_t> metadata(handle.size() * sizeof(int32_t));
    if (!metadata.empty()) {
        std::memcpy(metadata.data(), handle.data(), metadata.size());
    }
    return metadata;
}

PGResult<std::vector<int32_t>> decodeHandle(const RouteEndpoint& endpoint) {
    PG_VALIDATE_ARG(!endpoint.metadata.empty() &&
                        endpoint.metadata.size() % sizeof(int32_t) == 0,
                    "P2P route endpoint metadata is invalid");

    std::vector<int32_t> handle(endpoint.metadata.size() / sizeof(int32_t));
    std::memcpy(handle.data(), endpoint.metadata.data(),
                endpoint.metadata.size());
    return handle;
}

}  // namespace

P2pRoute::P2pRoute(device::P2pTransport& transport, void* local_region,
                   int device_index, GlobalRank self_rank,
                   uint32_t max_world_size)
    : transport_(transport),
      local_region_(local_region),
      device_index_(device_index),
      self_rank_(self_rank),
      max_world_size_(max_world_size) {}

std::string_view P2pRoute::routeKey() const noexcept { return kRouteKey; }

uint32_t P2pRoute::routeVersion() const noexcept { return kEndpointVersion; }

std::optional<RouteEndpoint> P2pRoute::localEndpoint() {
    const auto handle = localHandle();
    if (handle.empty()) return std::nullopt;
    return RouteEndpoint{
        .route_key = std::string(kRouteKey),
        .version = routeVersion(),
        .metadata = encodeHandle(handle),
    };
}

std::vector<int32_t> P2pRoute::localHandle() const {
    return transport_.exportIpcHandle(local_region_);
}

PGResult<std::vector<DeviceTransferRoute>> P2pRoute::resolveRoutes(
    std::span<const std::optional<DeviceTransferEndpoint>> endpoints) {
    PG_VALIDATE_ARG(endpoints.size() == max_world_size_,
                    "P2P route endpoint snapshot size does not match max world "
                    "size");

    // Decode the complete snapshot before changing the imported mappings.
    std::vector<std::vector<int32_t>> handles(max_world_size_);
    std::vector<int> active(max_world_size_, 0);
    for (GlobalRank rank = 0; rank < static_cast<GlobalRank>(max_world_size_);
         ++rank) {
        PG_TRY(auto endpoint, findEndpoint(endpoints[rank]));
        if (!endpoint) continue;
        PG_TRY(handles[rank], decodeHandle(*endpoint));
        active[rank] = 1;
    }

    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    transport_.importPeerHandles(local_region_, self_rank_, max_world_size_,
                                 handles, active);

    std::vector<int32_t> available(max_world_size_, 0);
    std::vector<void*> region_bases(max_world_size_, nullptr);
    PG_TRY_CUDA(cudaMemcpy(available.data(), transport_.availableTablePtr(),
                           available.size() * sizeof(int32_t),
                           cudaMemcpyDeviceToHost));
    PG_TRY_CUDA(cudaMemcpy(region_bases.data(), transport_.peerPtrsTablePtr(),
                           region_bases.size() * sizeof(void*),
                           cudaMemcpyDeviceToHost));

    std::vector<DeviceTransferRoute> routes(max_world_size_);
    for (GlobalRank rank = 0; rank < static_cast<GlobalRank>(max_world_size_);
         ++rank) {
        if (!active[rank]) continue;

        uint64_t mapped_region_address = 0;
        if (rank == self_rank_) {
            mapped_region_address = reinterpret_cast<uint64_t>(local_region_);
        } else if (available[rank] && region_bases[rank]) {
            mapped_region_address =
                reinterpret_cast<uint64_t>(region_bases[rank]);
        }
        if (mapped_region_address == 0) continue;

        routes[rank] = DeviceTransferRoute{
            .kind = DeviceRouteKind::P2p,
            .region_size = endpoints[rank]->region_size,
            .p2p = {.mapped_region_address = mapped_region_address},
        };
    }
    return routes;
}

}  // namespace mooncake
