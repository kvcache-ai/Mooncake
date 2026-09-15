#include "device_comm/device_transfer/transfer_service.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <transfer_engine.h>
#include <transport/device/device_transport.h>

#include "device_comm/device_transfer/transfer_types.cuh"
#include "device_comm/device_transfer/routes/host_proxy_route/host_proxy_route.h"
#include "device_comm/device_transfer/routes/p2p_route/p2p_route.h"
#include "device_comm/device_transfer/routes/route_provider.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {
namespace {

bool validEndpoint(const DeviceTransferEndpoint& endpoint) {
    if (endpoint.region_address == 0 ||
        endpoint.region_address % alignof(uint64_t) != 0 ||
        endpoint.region_size == 0 ||
        addOverflows(endpoint.region_address, endpoint.region_size)) {
        return false;
    }
    if (endpoint.routes.empty()) return false;
    for (const auto& route : endpoint.routes) {
        if (route.route_key.empty()) return false;
    }
    return true;
}

template <typename Item>
uint64_t reserveLayoutItems(uint64_t& cursor, uint64_t count = 1) {
    cursor += alignmentPadding(cursor, alignof(Item));
    const uint64_t offset = cursor;
    cursor += count * sizeof(Item);
    return offset;
}

template <typename Item>
Item* layoutItemsAt(void* base, uint64_t offset) noexcept {
    return reinterpret_cast<Item*>(static_cast<char*>(base) + offset);
}

// Adapt TE's P2P allocation API at the DTS boundary. DeviceTransferRegion
// remains independent of both P2pTransport and its allocation modes.
class P2pRegionAllocator {
   public:
    explicit P2pRegionAllocator(device::P2pTransport& transport) noexcept
        : transport_(&transport) {}

    [[nodiscard]] void* allocate(size_t bytes) const {
        return transport_->allocateBuffer(bytes);
    }

    void deallocate(void* ptr, size_t) const noexcept {
        transport_->freeBuffer(ptr);
    }

   private:
    device::P2pTransport* transport_;
};

}  // namespace

struct DeviceTransferService::DeviceState {
    // One local-only CUDA allocation and its typed device subviews.
    struct DeviceMetadata {
        void* allocation = nullptr;
        DeviceTransferHandle* handle = nullptr;
        DeviceTransferRoute* routes = nullptr;
        uint64_t* lane_results = nullptr;
    };

    struct DeviceMetadataLayout {
        uint64_t size = 0;
        uint64_t handle_offset = 0;
        uint64_t routes_offset = 0;
        uint64_t lane_results_offset = 0;

        static DeviceMetadataLayout make(uint32_t max_world_size);

        [[nodiscard]] DeviceMetadata bind(void* allocation) const noexcept;
    };

    static PGResult<std::unique_ptr<DeviceState>> create(
        int device_index, GlobalRank self_rank, uint32_t max_world_size,
        size_t peer_accessible_capacity, size_t local_staging_capacity,
        TransferEngine& engine, LinkManager& link_manager) {
        // P2P is an optional route. When it is unavailable, the published
        // region remains usable through host-proxy routes.
        auto* p2p_transport =
            engine.getOrCreateP2pTransport(static_cast<int>(max_world_size));
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        PG_TRY(auto route_stream, GpuStream::createNonBlocking(device_index));

        // A P2P allocation must be released by the same P2P object. With
        // no P2P route, an ordinary device allocation is sufficient.
        std::optional<DeviceTransferRegion> peer_accessible_region;
        if (p2p_transport) {
            PG_TRY(auto region, DeviceTransferRegion::create(
                                    device_index, peer_accessible_capacity,
                                    P2pRegionAllocator(*p2p_transport)));
            peer_accessible_region.emplace(std::move(region));
        } else {
            PG_TRY(auto region, DeviceTransferRegion::create(
                                    device_index, peer_accessible_capacity));
            peer_accessible_region.emplace(std::move(region));
        }

        // Every fallible acquisition below is recorded in state, so an early
        // return unwinds it through DeviceState::shutdown().
        auto state = std::unique_ptr<DeviceState>(new DeviceState(
            device_index, self_rank, max_world_size, local_staging_capacity,
            std::move(route_stream), std::move(*peer_accessible_region)));

        // Initialize every route before registering the shared backing region.
        if (p2p_transport) {
            state->p2p_route = std::make_unique<P2pRoute>(
                *p2p_transport, state->peer_accessible_region.addr(),
                device_index, self_rank, max_world_size);
            PG_TRY(state->p2p_route->initialize());
            state->route_providers.push_back(state->p2p_route.get());
        }
        state->host_proxy_route = std::make_unique<HostProxyRoute>(
            engine, link_manager, max_world_size);
        PG_TRY(state->host_proxy_route->initialize(device_index));
        state->route_providers.push_back(state->host_proxy_route.get());
        PG_TRY(state->registerRegion(DeviceRegionKind::PeerAccessible,
                                     state->peer_accessible_region));
        state->peer_accessible_region_registered = true;

        state->local_endpoint = DeviceTransferEndpoint{
            .region_address = reinterpret_cast<uint64_t>(
                state->peer_accessible_region.addr()),
            .region_size = state->peer_accessible_region.size(),
            .routes = {},
        };
        for (auto* provider : state->route_providers) {
            if (auto endpoint = provider->localEndpoint()) {
                state->local_endpoint.routes.push_back(std::move(*endpoint));
            }
        }
        PG_VALIDATE_STATE(validEndpoint(state->local_endpoint),
                          "device transfer service has no available route");
        state->endpoints.resize(max_world_size);
        state->endpoints[self_rank] = state->local_endpoint;

        // Route selection and lane results are local device metadata, not
        // remotely addressed memory, so keep them outside the registered
        // region.
        const auto layout = DeviceMetadataLayout::make(max_world_size);
        void* metadata_allocation = nullptr;
        PG_TRY_CUDA(cudaMalloc(&metadata_allocation, layout.size));
        state->device_metadata = layout.bind(metadata_allocation);
        PG_TRY_CUDA(
            cudaMemset(state->device_metadata.allocation, 0, layout.size));

        // Recovery publishes this image while a failed kernel is parked. Keep
        // the source pinned.
        const size_t route_table_size =
            static_cast<size_t>(max_world_size) * sizeof(DeviceTransferRoute);
        PG_TRY_CUDA(
            cudaHostAlloc(reinterpret_cast<void**>(&state->host_route_image),
                          route_table_size, cudaHostAllocPortable));
        std::uninitialized_value_construct_n(state->host_route_image,
                                             max_world_size);

        // Resolve every provider from the same endpoint snapshot, select by
        // provider order, and publish the initial table once.
        PG_TRY(state->resolveRoutes());
        PG_TRY(state->publishRoutes());

        // Publish the kernel entry point last, after every pointer it exposes
        // refers to initialized state.
        const DeviceTransferHandle handle_image{
            .peer_accessible_region =
                {
                    .addr = state->peer_accessible_region.addr(),
                    .size = state->peer_accessible_region.size(),
                },
            .local_staging_region = {},
            .routes = state->device_metadata.routes,
            .lane_results = state->device_metadata.lane_results,
            .host_proxy_command_slots =
                state->host_proxy_route->deviceCommandSlots(),
            .max_world_size = max_world_size,
        };
        PG_TRY_CUDA(cudaMemcpy(state->device_metadata.handle, &handle_image,
                               sizeof(handle_image), cudaMemcpyHostToDevice));
        return state;
    }

    DeviceState(int device_index, GlobalRank self_rank, uint32_t max_world_size,
                size_t local_staging_capacity, GpuStream route_stream,
                DeviceTransferRegion peer_accessible_region)
        : device_index(device_index),
          self_rank(self_rank),
          max_world_size(max_world_size),
          local_staging_capacity(local_staging_capacity),
          peer_accessible_region(std::move(peer_accessible_region)),
          route_stream(std::move(route_stream)) {}

    ~DeviceState() noexcept {
        auto result = shutdown();
        if (!result.has_value()) {
            LOG(ERROR) << "DeviceTransferService device shutdown failed: "
                       << result.error().message;
        }
    }

    DeviceState(const DeviceState&) = delete;
    DeviceState& operator=(const DeviceState&) = delete;

    PGResult<void> resolveRoutes() {
        PG_VALIDATE_STATE(endpoints[self_rank].has_value(),
                          "local transfer endpoint is missing");

        // Providers are ordered by preference. Build the complete next image
        // separately, selecting the first reachable candidate for each peer.
        std::vector<DeviceTransferRoute> selected_routes(max_world_size);
        for (auto* provider : route_providers) {
            PG_TRY(auto candidates, provider->resolveRoutes(endpoints));
            PG_VALIDATE_STATE(
                candidates.size() == max_world_size,
                "route provider returned an invalid global-rank route table");

            for (GlobalRank rank = 0;
                 rank < static_cast<GlobalRank>(max_world_size); ++rank) {
                if (!endpoints[rank]) {
                    PG_VALIDATE_STATE(
                        candidates[rank].kind == DeviceRouteKind::Unreachable,
                        "route provider returned a route for a missing peer");
                    continue;
                }
                auto& selected = selected_routes[rank];
                if (selected.kind == DeviceRouteKind::Unreachable &&
                    candidates[rank].kind != DeviceRouteKind::Unreachable) {
                    selected = candidates[rank];
                }
            }
        }
        PG_VALIDATE_STATE(
            selected_routes[self_rank].kind != DeviceRouteKind::Unreachable,
            "no route is available for the local transfer peer");
        std::copy(selected_routes.begin(), selected_routes.end(),
                  host_route_image);
        return {};
    }

    PGResult<void> quiesceRoutes() {
        for (auto* provider : route_providers) PG_TRY(provider->quiesce());
        return {};
    }

    PGResult<void> registerRegion(DeviceRegionKind kind,
                                  const DeviceTransferRegion& region) {
        for (auto* provider : route_providers) {
            PG_TRY(
                provider->registerRegion(kind, region.addr(), region.size()));
        }
        return {};
    }

    PGResult<void> unregisterRegion(DeviceRegionKind kind,
                                    const DeviceTransferRegion& region) {
        for (auto current = route_providers.rbegin();
             current != route_providers.rend(); ++current) {
            auto* provider = *current;
            PG_TRY(
                provider->unregisterRegion(kind, region.addr(), region.size()));
        }
        return {};
    }

    PGResult<void> publishRoutes() {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        PG_TRY_CUDA(cudaMemcpyAsync(
            device_metadata.routes, host_route_image,
            static_cast<size_t>(max_world_size) * sizeof(DeviceTransferRoute),
            cudaMemcpyHostToDevice, route_stream.get()));
        return route_stream.synchronize();
    }

    PGResult<RegionSlice> allocateLocalStaging(size_t size, size_t alignment) {
        if (!local_staging_region) {
            PG_TRY(auto staging, DeviceTransferRegion::create(
                                     device_index, local_staging_capacity));
            PG_TRY(registerRegion(DeviceRegionKind::LocalStaging, staging));
            local_staging_region.emplace(std::move(staging));
        }

        if (!local_staging_handle_initialized) {
            const DeviceLocalRegion device_region{
                .addr = local_staging_region->addr(),
                .size = local_staging_region->size(),
            };
            PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
            PG_TRY_CUDA(cudaMemcpy(
                &device_metadata.handle->local_staging_region, &device_region,
                sizeof(device_region), cudaMemcpyHostToDevice));
            local_staging_handle_initialized = true;
        }
        return local_staging_region->allocate(size, alignment);
    }

    PGResult<void> shutdown() {
        if (shutdown_requested_) return {};

        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        PG_TRY(quiesceRoutes());
        if (local_staging_region) {
            PG_TRY(unregisterRegion(DeviceRegionKind::LocalStaging,
                                    *local_staging_region));
            PG_TRY(local_staging_region->release());
            local_staging_region.reset();
            local_staging_handle_initialized = false;
        }
        if (peer_accessible_region_registered) {
            PG_TRY(unregisterRegion(DeviceRegionKind::PeerAccessible,
                                    peer_accessible_region));
            peer_accessible_region_registered = false;
        }
        PG_TRY(peer_accessible_region.release());
        for (auto* provider : route_providers) PG_TRY(provider->shutdown());

        if (host_route_image) {
            const auto result = cudaFreeHost(host_route_image);
            if (result != cudaSuccess) {
                LOG(ERROR) << "Failed to free transfer-service host route "
                              "image: "
                           << cudaGetErrorString(result);
            }
            host_route_image = nullptr;
        }
        if (device_metadata.allocation) {
            const auto result = cudaFree(device_metadata.allocation);
            if (result != cudaSuccess) {
                LOG(ERROR) << "Failed to free transfer-service device "
                              "metadata: "
                           << cudaGetErrorString(result);
            }
            device_metadata = {};
        }
        route_providers.clear();
        p2p_route.reset();
        host_proxy_route.reset();
        shutdown_requested_ = true;
        return {};
    }

    int device_index;
    GlobalRank self_rank;
    uint32_t max_world_size;
    size_t local_staging_capacity;
    DeviceTransferRegion peer_accessible_region;
    std::optional<DeviceTransferRegion> local_staging_region;
    bool peer_accessible_region_registered = false;
    bool local_staging_handle_initialized = false;
    GpuStream route_stream;
    std::unique_ptr<P2pRoute> p2p_route;
    std::unique_ptr<HostProxyRoute> host_proxy_route;
    // Selection order is policy order: direct P2P before host fallback.
    std::vector<RouteProvider*> route_providers;
    DeviceMetadata device_metadata;
    // Pinned host image copied into device_metadata.routes.
    DeviceTransferRoute* host_route_image = nullptr;
    DeviceTransferEndpoint local_endpoint;
    std::vector<std::optional<DeviceTransferEndpoint>> endpoints;
    bool shutdown_requested_ = false;
};

DeviceTransferService::DeviceState::DeviceMetadataLayout
DeviceTransferService::DeviceState::DeviceMetadataLayout::make(
    uint32_t max_world_size) {
    DeviceMetadataLayout layout;
    uint64_t cursor = 0;

    layout.handle_offset = reserveLayoutItems<DeviceTransferHandle>(cursor);
    layout.routes_offset =
        reserveLayoutItems<DeviceTransferRoute>(cursor, max_world_size);
    layout.lane_results_offset =
        reserveLayoutItems<uint64_t>(cursor, kTransferLaneCount);
    layout.size = cursor;
    return layout;
}

DeviceTransferService::DeviceState::DeviceMetadata
DeviceTransferService::DeviceState::DeviceMetadataLayout::bind(
    void* allocation) const noexcept {
    return DeviceMetadata{
        .allocation = allocation,
        .handle =
            layoutItemsAt<DeviceTransferHandle>(allocation, handle_offset),
        .routes = layoutItemsAt<DeviceTransferRoute>(allocation, routes_offset),
        .lane_results =
            layoutItemsAt<uint64_t>(allocation, lane_results_offset),
    };
}

DeviceTransferService::DeviceTransferService() = default;

DeviceTransferService::~DeviceTransferService() noexcept {
    auto result = shutdown();
    if (!result.has_value()) {
        LOG(ERROR) << "DeviceTransferService shutdown failed during "
                      "destruction: "
                   << result.error().message;
    }
}

PGResult<void> DeviceTransferService::initialize(
    GlobalRank self_rank, uint32_t max_world_size, int device_index,
    TransferEngine& transfer_engine, LinkManager& link_manager,
    size_t peer_accessible_capacity, size_t local_staging_capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "DeviceTransferService is shutting down");
    PG_VALIDATE_STATE(!device_, "DeviceTransferService is already initialized");
    PG_VALIDATE_ARG(max_world_size != 0 &&
                        max_world_size <= static_cast<uint32_t>(kMaxNumRanks),
                    "device transfer max world size is invalid");
    PG_VALIDATE_ARG(
        self_rank >= 0 && static_cast<uint32_t>(self_rank) < max_world_size,
        "device transfer self rank is out of range");
    PG_VALIDATE_ARG(device_index >= 0, "invalid CUDA device");
    PG_VALIDATE_ARG(peer_accessible_capacity != 0,
                    "peer-accessible region is empty");
    PG_VALIDATE_ARG(local_staging_capacity != 0,
                    "local staging region is empty");

    PG_TRY(auto device,
           DeviceState::create(device_index, self_rank, max_world_size,
                               peer_accessible_capacity, local_staging_capacity,
                               transfer_engine, link_manager));

    device_ = std::move(device);
    return {};
}

DeviceTransferService::DeviceState& DeviceTransferService::deviceState() {
    return *device_;
}

const DeviceTransferHandle* DeviceTransferService::deviceHandle() {
    std::lock_guard<std::mutex> lock(mutex_);
    return deviceState().device_metadata.handle;
}

PGResult<DeviceRouteKind> DeviceTransferService::routeKind(GlobalRank rank) {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(device_, "DeviceTransferService is not initialized");
    const auto& state = deviceState();
    PG_VALIDATE_ARG(
        rank >= 0 && static_cast<uint32_t>(rank) < state.max_world_size,
        "transfer rank is out of range");
    return state.host_route_image[rank].kind;
}

const DeviceTransferEndpoint& DeviceTransferService::localEndpoint()
    const noexcept {
    return device_->local_endpoint;
}

int DeviceTransferService::deviceIndex() const noexcept {
    return device_->device_index;
}

PGResult<RegionSlice> DeviceTransferService::allocatePeerAccessible(
    size_t size, size_t alignment) {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(device_, "DeviceTransferService is not initialized");
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "DeviceTransferService is shutting down");
    return deviceState().peer_accessible_region.allocate(size, alignment);
}

PGResult<RegionSlice> DeviceTransferService::allocateLocalStaging(
    size_t size, size_t alignment) {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(device_, "DeviceTransferService is not initialized");
    PG_VALIDATE_STATE(!shutdown_requested_,
                      "DeviceTransferService is shutting down");
    return deviceState().allocateLocalStaging(size, alignment);
}

PGResult<void> DeviceTransferService::installPeerEndpoint(
    GlobalRank rank, const DeviceTransferEndpoint& endpoint) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& state = deviceState();
    PG_VALIDATE_ARG(
        rank >= 0 && static_cast<uint32_t>(rank) < state.max_world_size,
        "transfer rank is out of range");
    PG_VALIDATE_ARG(rank != state.self_rank,
                    "cannot replace the local transfer endpoint");
    PG_VALIDATE_ARG(validEndpoint(endpoint),
                    "transfer-service endpoint is invalid");

    state.endpoints[rank] = endpoint;
    PG_TRY(state.resolveRoutes());
    return state.publishRoutes();
}

PGResult<void> DeviceTransferService::waitUntilIdle() {
    std::lock_guard<std::mutex> lock(mutex_);
    return deviceState().quiesceRoutes();
}

PGResult<void> DeviceTransferService::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdown_requested_ = true;

    if (device_) {
        PG_TRY(device_->shutdown());
        device_.reset();
    }

    return {};
}

}  // namespace mooncake
