#include "device_comm/device_transfer/routes/p2p_route/p2p_route.h"

#include <cstring>
#include <utility>

#include <transport/device/device_transport.h>

#include "gpu_runtime.h"

namespace mooncake {

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

    // Scan visible hardware once; peer mappings are resolved separately.
    cudaDeviceProp properties{};
    PG_TRY_CUDA(cudaGetDeviceProperties(&properties, device_index));
    static_assert(sizeof(properties.uuid.bytes) == sizeof(DeviceUUID));
    std::memcpy(route->device_uuid_.data(), properties.uuid.bytes,
                route->device_uuid_.size());
    int device_count = 0;
    PG_TRY_CUDA(cudaGetDeviceCount(&device_count));
    for (int peer = 0; peer < device_count; ++peer) {
        if (peer == device_index) continue;
        int accessible = 0;
        PG_TRY_CUDA(cudaDeviceCanAccessPeer(&accessible, device_index, peer));
        if (!accessible) continue;
        int native_atomics = 0;
        PG_TRY_CUDA(cudaDeviceGetP2PAttribute(
            &native_atomics, cudaDevP2PAttrNativeAtomicSupported, device_index,
            peer));
        if (!native_atomics) continue;
        PG_TRY_CUDA(cudaGetDeviceProperties(&properties, peer));
        DeviceUUID uuid;
        std::memcpy(uuid.data(), properties.uuid.bytes, uuid.size());
        route->native_atomic_peer_uuids_.push_back(uuid);
    }

    return route;
}

std::optional<RouteEndpoint> P2pRoute::localEndpoint() {
    if (local_handle_.empty()) return std::nullopt;
    return RouteEndpoint{
        .route_key = std::string(kRouteKey),
        .version = routeVersion(),
        .metadata = encodeEndpointMetadata(local_handle_),
    };
}

PGResult<void> P2pRoute::installEndpoints(
    const DeviceTransferSnapshot& snapshot, uint64_t reclaim_before_version) {
    PG_VALIDATE_STATE(local_ptr_, "P2P region is not registered");
    PG_VALIDATE_ARG(snapshot.endpoints.size() == max_world_size_,
                    "P2P route endpoint snapshot size does not match max world "
                    "size");
    const auto& endpoints = snapshot.endpoints;
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));

    // Decode the complete snapshot before changing the imported mappings.
    std::vector<std::vector<int32_t>> handles(max_world_size_);
    for (GlobalRank rank = 0; rank < static_cast<GlobalRank>(max_world_size_);
         ++rank) {
        PG_TRY(auto endpoint, findEndpoint(endpoints[rank]));
        if (!endpoint) continue;
        PG_TRY(handles[rank],
               decodeEndpointMetadata<std::vector<int32_t>>(*endpoint));
        PG_VALIDATE_ARG(!handles[rank].empty(),
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
    next->routes.resize(max_world_size_);
    for (GlobalRank rank = 0; rank < static_cast<GlobalRank>(max_world_size_);
         ++rank) {
        if (handles[rank].empty()) continue;
        void* address = nullptr;
        if (rank == self_rank_) {
            address = local_ptr_;
        } else {
            auto& peer = next->peers[rank];
            const auto* previous = current_ ? &current_->peers[rank] : nullptr;
            peer.handle = std::move(handles[rank]);
            if (previous && previous->mapping &&
                previous->handle == peer.handle)
                peer.mapping = previous->mapping;
            else
                peer.mapping = device::importP2pMemory(peer.handle);
            if (!peer.mapping) continue;
            address = peer.mapping->address();
        }

        next->routes[rank] = DeviceTransferRoute{
            .type = DeviceRouteType::P2p,
            .region_size = endpoints[rank]->region_size,
            .p2p = {.mapped_region_address =
                        reinterpret_cast<uint64_t>(address)},
        };
    }
    standby_ = std::move(next);
    return {};
}

PGResult<std::vector<DeviceTransferRoute>> P2pRoute::updateRoutes() {
    PG_VALIDATE_STATE(standby_, "P2P route update requires prepared endpoints");
    current_.swap(standby_);
    return current_->routes;
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
