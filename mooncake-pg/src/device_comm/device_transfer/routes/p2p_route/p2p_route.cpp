#include "device_comm/device_transfer/routes/p2p_route/p2p_route.h"

#include <utility>

#include <transport/device/device_transport.h>

#include "gpu_runtime.h"

namespace mooncake {
namespace {

struct P2pEndpointMetadata {
    std::vector<int32_t> handle;
    GpuUuid device_uuid{};
};

}  // namespace

P2pRoute::P2pRoute(int device_index, GlobalRank self_rank,
                   uint32_t max_world_size)
    : RouteProvider(DeviceRouteType::P2p, kRouteKey, kEndpointVersion),
      device_index_(device_index),
      self_rank_(self_rank),
      max_world_size_(max_world_size) {}

PGResult<std::unique_ptr<P2pRoute>> P2pRoute::create(int device_index,
                                                     GlobalRank self_rank,
                                                     uint32_t max_world_size) {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    auto route = std::unique_ptr<P2pRoute>(
        new P2pRoute(device_index, self_rank, max_world_size));
    PG_TRY(route->device_uuid_, queryGpuUuid(device_index));
    return route;
}

std::optional<RouteEndpoint> P2pRoute::localEndpoint() {
    if (local_handle_.empty()) return std::nullopt;
    return encodeEndpoint(P2pEndpointMetadata{local_handle_, device_uuid_});
}

PGResult<std::vector<DeviceTransferRoute>> P2pRoute::installEndpoints(
    const DeviceTransferSnapshot& snapshot, uint64_t reclaim_before_version) {
    PG_VALIDATE_STATE(local_ptr_, "P2P region is not registered");
    PG_VALIDATE_ARG(snapshot.endpoints.size() == max_world_size_,
                    "P2P route endpoint snapshot size does not match max world "
                    "size");
    const auto& endpoints = snapshot.endpoints;
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));

    // Decode the complete snapshot before changing the imported mappings.
    std::vector<P2pEndpointMetadata> peers(max_world_size_);
    for (GlobalRank rank = 0; rank < static_cast<GlobalRank>(max_world_size_);
         ++rank) {
        if (!endpoints[rank]) continue;
        const auto* endpoint = findEndpoint(endpoints[rank]->routes);
        if (!endpoint) continue;
        PG_TRY(peers[rank], decodeEndpoint<P2pEndpointMetadata>(*endpoint));
        PG_VALIDATE_ARG(!peers[rank].handle.empty(),
                        "P2P route endpoint handle is empty");
    }

    if (standby_) {
        PG_VALIDATE_STATE(
            standby_->installation_version < reclaim_before_version,
            "previous P2P generation is not reclaimable");
        standby_.reset();
    }

    auto next = std::make_unique<State>();
    next->installation_version = snapshot.version;
    next->peers.resize(max_world_size_);
    std::vector<DeviceTransferRoute> routes(max_world_size_);
    for (GlobalRank rank = 0; rank < static_cast<GlobalRank>(max_world_size_);
         ++rank) {
        if (peers[rank].handle.empty()) continue;
        void* address = nullptr;
        if (rank == self_rank_) {
            address = local_ptr_;
        } else {
            auto& peer = next->peers[rank];
            const auto* previous = current_ ? &current_->peers[rank] : nullptr;
            peer.handle = std::move(peers[rank].handle);
            if (previous && previous->mapping &&
                previous->handle == peer.handle)
                peer.mapping = previous->mapping;
            else
                peer.mapping = device::importP2pMemory(peer.handle);
            if (!peer.mapping) continue;
            address = peer.mapping->address();
        }

        PG_TRY(const bool native_atomics,
               gpuPeerSupportsNativeAtomics(device_uuid_,
                                            peers[rank].device_uuid));
        routes[rank] = DeviceTransferRoute{
            .type = DeviceRouteType::P2p,
            .native_atomics = native_atomics,
            .region_size = endpoints[rank]->region_size,
            .p2p = {.mapped_region_address =
                        reinterpret_cast<uint64_t>(address)},
        };
    }
    standby_ = std::move(next);
    return routes;
}

PGResult<void> P2pRoute::updateRoutes() {
    PG_VALIDATE_STATE(standby_, "P2P route update requires prepared endpoints");
    current_.swap(standby_);
    return {};
}

PGResult<void> P2pRoute::registerRegion(DeviceRegionKind kind, void* addr,
                                        size_t size) {
    PG_VALIDATE_ARG(addr && size != 0, "P2P region is empty");
    PG_ASSERT(kind == DeviceRegionKind::PeerAccessible ||
                  kind == DeviceRegionKind::LocalStaging,
              "P2P route received an unknown device region kind");

    if (kind == DeviceRegionKind::LocalStaging) return {};
    PG_VALIDATE_STATE(!local_ptr_, "P2P region is already registered");
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));
    auto handle = device::exportP2pMemory(addr, size);
    if (handle.empty()) {
        return makePGError(PGErrorCode::NotSupported,
                           "P2P region cannot be exported");
    }
    local_ptr_ = addr;
    local_size_ = size;
    local_handle_ = std::move(handle);
    return {};
}

PGResult<void> P2pRoute::unregisterRegion(DeviceRegionKind kind, void* addr,
                                          size_t size) {
    PG_VALIDATE_ARG(addr && size != 0, "P2P region is empty");
    if (kind == DeviceRegionKind::LocalStaging) return {};
    PG_VALIDATE_STATE(local_ptr_ == addr && local_size_ == size,
                      "P2P region does not match the exported allocation");
    local_handle_.clear();
    local_ptr_ = nullptr;
    local_size_ = 0;
    return {};
}

PGResult<void> P2pRoute::shutdown() {
    standby_.reset();
    current_.reset();
    return {};
}

}  // namespace mooncake
