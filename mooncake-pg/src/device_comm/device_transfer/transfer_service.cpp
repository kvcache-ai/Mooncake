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
#include "gpu_runtime.h"
#include "memory_location.h"
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

// Concrete owner for the one stable device region exposed by the service.
// Routes and DeviceArena only borrow this memory.
class ServiceRegion {
   public:
    static PGResult<ServiceRegion> create(int device_index, size_t size,
                                          device::P2pTransport* p2p_allocator) {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        ServiceRegion region(device_index, size);
        if (p2p_allocator) {
            region.addr_ = p2p_allocator->allocateBuffer(size);
            PG_VALIDATE_STATE(region.addr_,
                              "failed to allocate shareable service region");
            region.p2p_allocator_ = p2p_allocator;
        } else {
            PG_TRY_CUDA(cudaMalloc(&region.addr_, size));
        }
        return region;
    }

    ~ServiceRegion() noexcept {
        auto result = release();
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to release device transfer service region: "
                       << result.error().message;
        }
    }

    ServiceRegion(const ServiceRegion&) = delete;
    ServiceRegion& operator=(const ServiceRegion&) = delete;
    ServiceRegion& operator=(ServiceRegion&&) = delete;

    ServiceRegion(ServiceRegion&& other) noexcept
        : device_index_(std::exchange(other.device_index_, -1)),
          addr_(std::exchange(other.addr_, nullptr)),
          size_(std::exchange(other.size_, 0)),
          p2p_allocator_(std::exchange(other.p2p_allocator_, nullptr)),
          host_proxy_engine_(std::exchange(other.host_proxy_engine_, nullptr)),
          registered_for_host_proxy_(
              std::exchange(other.registered_for_host_proxy_, false)) {}

    PGResult<void> registerForHostProxy(TransferEngine& engine,
                                        const std::string& location) {
        PG_VALIDATE_STATE(addr_, "service region is not allocated");
        PG_VALIDATE_STATE(!registered_for_host_proxy_,
                          "service region is already registered for the "
                          "host-proxy route");
        PG_TRY_TE(engine.registerLocalMemory(addr_, size_, location));
        host_proxy_engine_ = &engine;
        registered_for_host_proxy_ = true;
        return {};
    }

    PGResult<void> release() {
        if (!addr_) return {};
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));

        if (registered_for_host_proxy_) {
            const int result = host_proxy_engine_->unregisterLocalMemory(addr_);
            if (result != 0) {
                LOG(ERROR) << "Failed to unregister device transfer service "
                              "region, rc="
                           << result;
            }
            host_proxy_engine_ = nullptr;
            registered_for_host_proxy_ = false;
        }

        if (p2p_allocator_) {
            p2p_allocator_->freeBuffer(addr_);
        } else {
            PG_TRY_CUDA(cudaFree(addr_));
        }
        addr_ = nullptr;
        size_ = 0;
        p2p_allocator_ = nullptr;
        device_index_ = -1;
        return {};
    }

    [[nodiscard]] void* addr() const noexcept { return addr_; }
    [[nodiscard]] size_t size() const noexcept { return size_; }

   private:
    ServiceRegion(int device_index, size_t size) noexcept
        : device_index_(device_index), size_(size) {}

    int device_index_ = -1;
    void* addr_ = nullptr;
    size_t size_ = 0;
    device::P2pTransport* p2p_allocator_ = nullptr;
    TransferEngine* host_proxy_engine_ = nullptr;
    bool registered_for_host_proxy_ = false;
};

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
        size_t region_size, TransferEngine& engine, LinkManager& link_manager) {
        // P2P is an optional route. When it is unavailable, the service region
        // remains usable through host-proxy routes.
        auto* p2p_transport =
            engine.getOrCreateP2pTransport(static_cast<int>(max_world_size));
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        PG_TRY(auto route_stream, GpuStream::createNonBlocking(device_index));

        // A P2P allocation must be released by the same P2P object. With
        // no P2P route, an ordinary device allocation is sufficient.
        PG_TRY(auto region,
               ServiceRegion::create(device_index, region_size, p2p_transport));
        // Every fallible acquisition below is recorded in state, so an early
        // return unwinds it through DeviceState::shutdown().
        auto state = std::unique_ptr<DeviceState>(
            new DeviceState(device_index, self_rank, max_world_size,
                            std::move(route_stream), std::move(region)));
        const auto location = GPU_PREFIX + std::to_string(device_index);
        PG_TRY(state->region.registerForHostProxy(engine, location));

        // Initialize all route providers before publishing any kernel-facing
        // pointers or endpoint metadata.
        if (p2p_transport) {
            state->p2p_route = std::make_unique<P2pRoute>(
                *p2p_transport, state->region.addr(), device_index, self_rank,
                max_world_size);
            state->route_providers.push_back(state->p2p_route.get());
        }
        state->host_proxy_route = std::make_unique<HostProxyRoute>(
            engine, link_manager, max_world_size);
        PG_TRY(state->host_proxy_route->initialize(device_index));
        state->route_providers.push_back(state->host_proxy_route.get());

        state->local_endpoint = DeviceTransferEndpoint{
            .region_address = reinterpret_cast<uint64_t>(state->region.addr()),
            .region_size = state->region.size(),
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
            .local_region = state->region.addr(),
            .local_region_size = state->region.size(),
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
                GpuStream route_stream, ServiceRegion region)
        : device_index(device_index),
          self_rank(self_rank),
          max_world_size(max_world_size),
          region(std::move(region)),
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

    PGResult<void> publishRoutes() {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        PG_TRY_CUDA(cudaMemcpyAsync(
            device_metadata.routes, host_route_image,
            static_cast<size_t>(max_world_size) * sizeof(DeviceTransferRoute),
            cudaMemcpyHostToDevice, route_stream.get()));
        return route_stream.synchronize();
    }

    PGResult<void> shutdown() {
        if (shutdown_requested_) return {};

        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
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
        PG_TRY(region.release());
        shutdown_requested_ = true;
        return {};
    }

    int device_index;
    GlobalRank self_rank;
    uint32_t max_world_size;
    ServiceRegion region;
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
    size_t region_size) {
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
    PG_VALIDATE_ARG(region_size != 0, "transfer-service region is empty");

    PG_TRY(auto device,
           DeviceState::create(device_index, self_rank, max_world_size,
                               region_size, transfer_engine, link_manager));

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

const DeviceTransferEndpoint& DeviceTransferService::localEndpoint()
    const noexcept {
    return device_->local_endpoint;
}

void* DeviceTransferService::regionAddr() const noexcept {
    return device_->region.addr();
}

size_t DeviceTransferService::regionSize() const noexcept {
    return device_->region.size();
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
