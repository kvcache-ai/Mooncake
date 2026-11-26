#include "device_comm/device_transfer/transfer_service.h"

#include <algorithm>
#include <thread>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <transfer_engine.h>
#include <transport/device/device_transport.h>

#include "device_comm/device_collective/strong_stream.h"
#include "device_comm/device_utils/d2h_request_slot.h"
#include "device_comm/device_utils/h2d_request_slot.h"
#include "device_comm/device_transfer/transfer_types.cuh"
#include "device_comm/device_transfer/routes/p2p_route/p2p_route.h"
#include "device_comm/device_transfer/routes/rdma_route/rdma_route.h"
#include "device_comm/device_transfer/routes/nccl_device_route/nccl_device_route.h"
#include "device_comm/device_transfer/routes/host_proxy_route/host_proxy_route.h"
#include "device_comm/device_transfer/routes/route_provider.h"
#include "gpu_runtime.h"
#include "pg_utils.h"

namespace mooncake {
namespace {

const char* routeTypeName(DeviceRouteType type) {
    switch (type) {
        case DeviceRouteType::Unreachable:
            return "Unreachable";
        case DeviceRouteType::P2p:
            return "P2P";
        case DeviceRouteType::Rdma:
            return "RDMA";
        case DeviceRouteType::NcclDevice:
            return "NCCL device";
        case DeviceRouteType::HostProxy:
            return "HostProxy";
    }
    return "Unknown";
}

bool validEndpoint(const DeviceTransferEndpoint& endpoint) {
    if (endpoint.region_address == 0 ||
        endpoint.region_address % alignof(uint64_t) != 0 ||
        endpoint.region_size == 0 ||
        addOverflows(endpoint.region_address, endpoint.region_size)) {
        return false;
    }
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

}  // namespace

struct DeviceTransferService::DeviceState {
    // One local-only CUDA allocation and its typed device subviews.
    struct DeviceMetadata {
        void* allocation = nullptr;
        DeviceTransferHandle* handle = nullptr;
        DeviceTransferRoute* routes = nullptr;
    };

    struct DeviceMetadataLayout {
        uint64_t size = 0;
        uint64_t handle_offset = 0;
        uint64_t routes_offset = 0;

        static DeviceMetadataLayout make(uint32_t max_world_size);

        [[nodiscard]] DeviceMetadata bind(void* allocation) const noexcept;
    };

    static PGResult<std::unique_ptr<DeviceState>> create(
        int device_index, GlobalRank self_rank, uint32_t max_world_size,
        size_t peer_accessible_capacity, size_t local_staging_capacity,
        TransferEngine& engine, LinkManager& link_manager,
        StrongStream& strong_stream, const DeviceRouteConfig& config) {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        PG_TRY(auto route_stream, GpuStream::createNonBlocking(device_index));
        PG_TRY(auto region, DeviceTransferRegion::create(
                                device_index, peer_accessible_capacity));

        // Every fallible acquisition below is recorded in state, so an early
        // return unwinds it through DeviceState::shutdown().
        auto state = std::unique_ptr<DeviceState>(new DeviceState(
            device_index, self_rank, max_world_size, local_staging_capacity,
            std::move(route_stream), std::move(region), strong_stream));

        state->loadRoutes(engine, link_manager, config);
        state->peer_accessible_region_registered = true;

        state->local_endpoint = DeviceTransferEndpoint{
            .region_address = reinterpret_cast<uint64_t>(
                state->peer_accessible_region.addr()),
            .region_size = state->peer_accessible_region.size(),
            .routes = {},
        };
        for (const auto& provider : state->route_providers) {
            if (auto endpoint = provider->localEndpoint()) {
                state->local_endpoint.routes.push_back(std::move(*endpoint));
            }
        }
        PG_VALIDATE_STATE(validEndpoint(state->local_endpoint),
                          "device transfer service endpoint is invalid");

        // These are local device metadata, not remotely addressed memory, so
        // keep them outside the registered region.
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

        PG_TRY_CUDA(cudaHostAlloc(
            reinterpret_cast<void**>(&state->pause_mailbox),
            sizeof(DeviceTransferHandle::PauseMailbox), cudaHostAllocMapped));
        new (state->pause_mailbox) DeviceTransferHandle::PauseMailbox{};
        DeviceTransferHandle::PauseMailbox* device_pause_mailbox = nullptr;
        PG_TRY_CUDA(cudaHostGetDevicePointer(
            reinterpret_cast<void**>(&device_pause_mailbox),
            state->pause_mailbox, 0));

        // Initialize the stable handle fields before publishing contexts and
        // routes. Callers receive the handle only after create succeeds.
        PG_TRY(auto drain_timeout_ticks,
               gpuTimeoutTicks(device_index, kTransferDrainTimeoutMs * 1000));
        const DeviceTransferHandle handle_image{
            .peer_accessible_region =
                {
                    .addr = state->peer_accessible_region.addr(),
                    .size = state->peer_accessible_region.size(),
                },
            .local_staging_region = {},
            .routes = state->device_metadata.routes,
            .drain_timeout_ticks = drain_timeout_ticks,
            .max_world_size = max_world_size,
            .pause_mailbox = device_pause_mailbox,
        };
        PG_TRY_CUDA(cudaMemcpy(state->device_metadata.handle, &handle_image,
                               sizeof(handle_image), cudaMemcpyHostToDevice));
        PG_TRY(state->publishRoutes());
        return state;
    }

    DeviceState(int device_index, GlobalRank self_rank, uint32_t max_world_size,
                size_t local_staging_capacity, GpuStream route_stream,
                DeviceTransferRegion peer_accessible_region,
                StrongStream& strong_stream)
        : device_index(device_index),
          self_rank(self_rank),
          max_world_size(max_world_size),
          local_staging_capacity(local_staging_capacity),
          peer_accessible_region(std::move(peer_accessible_region)),
          route_stream(std::move(route_stream)),
          strong_stream(strong_stream),
          published_route_types(max_world_size, DeviceRouteType::Unreachable) {}

    ~DeviceState() noexcept {
        auto result = shutdown();
        if (!result.has_value()) {
            LOG(ERROR) << "DeviceTransferService device shutdown failed: "
                       << result.error().message;
        }
    }

    DeviceState(const DeviceState&) = delete;
    DeviceState& operator=(const DeviceState&) = delete;

    template <typename Route, typename... Args>
    void tryLoadRoute(bool enabled, Args&&... args) {
        if (!enabled) {
            LOG(INFO) << "[PG] " << Route::kRouteKey
                      << " device-transfer route is disabled";
            return;
        }
        auto result = [&]() -> PGResult<void> {
            PG_TRY(auto route, Route::create(std::forward<Args>(args)...));
            PG_TRY(route->registerRegion(DeviceRegionKind::PeerAccessible,
                                         peer_accessible_region.addr(),
                                         peer_accessible_region.size()));
            route_providers.push_back(std::move(route));
            return {};
        }();
        if (!result.has_value()) {
            const auto& error = result.error();
            if (error.code == PGErrorCode::NotSupported) {
                LOG(INFO) << "[PG] " << Route::kRouteKey
                          << " device-transfer route is unavailable: "
                          << error.message;
            } else {
                LOG(WARNING) << "[PG] " << Route::kRouteKey
                             << " device-transfer route initialization failed; "
                                "omitting route: "
                             << error.message;
            }
            return;
        }
    }

    void loadRoutes(TransferEngine& engine, LinkManager& link_manager,
                    const DeviceRouteConfig& config) {
        // Route preference: P2P > RDMA > NCCL device > host proxy.
        tryLoadRoute<P2pRoute>(config.p2p.enabled, device_index, self_rank,
                               max_world_size);
        tryLoadRoute<RdmaRoute>(config.rdma.enabled, device_index,
                                route_stream.get(), self_rank, max_world_size,
                                config.rdma);
        tryLoadRoute<NcclDeviceRoute>(config.nccl_device.enabled, device_index,
                                      self_rank, max_world_size,
                                      config.nccl_device);
        tryLoadRoute<HostProxyRoute>(config.host_proxy.enabled, device_index,
                                     engine, link_manager, max_world_size);
    }

    void installEndpoints(const DeviceTransferSnapshot& snapshot,
                          uint64_t reclaim_before_version) {
        for (auto it = route_providers.begin(); it != route_providers.end();) {
            auto result =
                (*it)->installEndpoints(snapshot, reclaim_before_version);
            if (result.has_value()) {
                ++it;
                continue;
            }
            LOG(WARNING) << "[PG] " << (*it)->routeKey()
                         << " device-transfer route is poisoned by endpoint "
                            "installation failure: "
                         << result.error().message;
            poisoned_routes.push_back(
                {std::move(*it), local_staging_region.has_value()});
            it = route_providers.erase(it);
        }
    }

    PGResult<void> updateRoutes() {
        // Providers are ordered by preference. Build the complete next image
        // separately, selecting the first reachable candidate for each peer.
        std::vector<DeviceTransferRoute> selected_routes(max_world_size);
        for (const auto& provider : route_providers) {
            PG_TRY(auto candidates, provider->updateRoutes());
            PG_VALIDATE_STATE(
                candidates.size() == max_world_size,
                "route provider returned an invalid global-rank route table");

            for (GlobalRank rank = 0;
                 rank < static_cast<GlobalRank>(max_world_size); ++rank) {
                auto& selected = selected_routes[rank];
                if (selected.type == DeviceRouteType::Unreachable &&
                    candidates[rank].type != DeviceRouteType::Unreachable) {
                    selected = candidates[rank];
                }
            }
        }
        std::copy(selected_routes.begin(), selected_routes.end(),
                  host_route_image);
        return {};
    }

    PGResult<void> registerRegion(DeviceRegionKind kind,
                                  const DeviceTransferRegion& region) {
        size_t registered = 0;
        for (; registered < route_providers.size(); ++registered) {
            auto result = route_providers[registered]->registerRegion(
                kind, region.addr(), region.size());
            if (result.has_value()) continue;

            while (registered != 0) {
                --registered;
                auto rollback = route_providers[registered]->unregisterRegion(
                    kind, region.addr(), region.size());
                if (!rollback.has_value()) {
                    LOG(ERROR) << "Failed to roll back device route region: "
                               << rollback.error().message;
                }
            }
            return makePGError(std::move(result).error());
        }
        return {};
    }

    PGResult<void> unregisterRegion(DeviceRegionKind kind,
                                    const DeviceTransferRegion& region) {
        for (auto current = route_providers.rbegin();
             current != route_providers.rend(); ++current) {
            const auto& provider = *current;
            PG_TRY(
                provider->unregisterRegion(kind, region.addr(), region.size()));
        }
        for (auto& route : poisoned_routes) {
            if (kind == DeviceRegionKind::LocalStaging &&
                !route.staging_registered)
                continue;
            PG_TRY(route.provider->unregisterRegion(kind, region.addr(),
                                                    region.size()));
            if (kind == DeviceRegionKind::LocalStaging)
                route.staging_registered = false;
        }
        return {};
    }

    [[nodiscard]] DeviceRouteContext deviceRouteContext() const noexcept {
        DeviceRouteContext context{};
        for (const auto& provider : route_providers)
            provider->fillDeviceContext(context);
        return context;
    }

    PGResult<void> publishRoutes() {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        const auto context = deviceRouteContext();
        DeviceLocalRegion staging{};
        if (local_staging_region) {
            staging = {.addr = local_staging_region->addr(),
                       .size = local_staging_region->size()};
        }
        PG_TRY_CUDA(cudaMemcpyAsync(
            &device_metadata.handle->route_context, &context, sizeof(context),
            cudaMemcpyHostToDevice, route_stream.get()));
        PG_TRY_CUDA(cudaMemcpyAsync(
            &device_metadata.handle->local_staging_region, &staging,
            sizeof(staging), cudaMemcpyHostToDevice, route_stream.get()));
        PG_TRY_CUDA(cudaMemcpyAsync(
            device_metadata.routes, host_route_image,
            static_cast<size_t>(max_world_size) * sizeof(DeviceTransferRoute),
            cudaMemcpyHostToDevice, route_stream.get()));
        PG_TRY(route_stream.synchronize());
        local_staging_handle_initialized = local_staging_region.has_value();

        // Report route-type changes only after publication succeeds.
        for (GlobalRank rank = 0;
             rank < static_cast<GlobalRank>(max_world_size); ++rank) {
            const auto type = host_route_image[rank].type;
            if (type == published_route_types[rank]) continue;
            LOG(INFO) << "[PG] Device-transfer route selected: rank="
                      << self_rank << " device=" << device_index
                      << " peer=" << rank << " route=" << routeTypeName(type);
            published_route_types[rank] = type;
        }
        return {};
    }

    PGResult<void> prepareLocalStaging() {
        if (local_staging_region) return {};
        PG_TRY(auto staging, DeviceTransferRegion::create(
                                 device_index, local_staging_capacity));
        PG_TRY(registerRegion(DeviceRegionKind::LocalStaging, staging));
        local_staging_region.emplace(std::move(staging));
        return {};
    }

    PGResult<RegionSlice> allocateLocalStaging(size_t size, size_t alignment) {
        PG_TRY(prepareLocalStaging());
        if (!local_staging_handle_initialized) {
            PG_ASSERT_OK(pause());
            PG_ASSERT_OK(publishRoutes());
            resume();
        }
        return local_staging_region->allocate(size, alignment);
    }

    // Wait for the device to park or submitted work to finish.
    // Later entrants remain paused until resume().
    PGResult<void> pause() {
        // Publish pause before recording the completion event, so it covers
        // work already past the pause check. Later work waits there until
        // resume().
        pause_request = pause_mailbox->pause.publish({}, /*pinned=*/true);
        std::optional<GpuEvent> completion;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (true) {
            bool stopped = pause_request.poll();
            if (!stopped) {
                if (completion) {
                    PG_TRY(stopped, completion->query());
                } else {
                    auto result = strong_stream.tryRecordCompletionEvent();
                    if (result.has_value()) {
                        completion = std::move(result).value();
                    } else if (result.error().code !=
                               PGErrorCode::ResourceBusy) {
                        return makePGError(std::move(result).error());
                    }
                }
            }
            if (stopped) return {};
            if (std::chrono::steady_clock::now() >= deadline)
                return makePGError(PGErrorCode::Timeout, "DTS pause timed out");
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }

    void resume() {
        // If no kernel ran during the update, pause may still be unconsumed.
        // Withdraw the request and return if successful.
        if (pause_request.tryWithdraw()) return;

        // Otherwise, a kernel consumed pause and will request resumption
        // through the resume slot. Reply to let it continue.
        DeviceTransferHandle::PauseMailbox::ResumeSlot::ReceivedRequest
            resume_request;
        while (!pause_mailbox->resume.tryReceive(resume_request))
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        resume_request.handle.reply({});
    }

    PGResult<void> shutdown() {
        if (shutdown_requested_) return {};

        PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
        // Kernel completion permits payload reuse but need not empty transport
        // queues. Stop their remaining work before releasing registered memory.
        for (const auto& provider : route_providers)
            PG_TRY(provider->shutdown());
        for (const auto& route : poisoned_routes)
            PG_TRY(route.provider->shutdown());
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
        if (pause_mailbox) {
            PG_TRY_CUDA(cudaFreeHost(pause_mailbox));
            pause_mailbox = nullptr;
        }
        route_providers.clear();
        poisoned_routes.clear();
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
    StrongStream& strong_stream;
    DeviceTransferHandle::PauseMailbox* pause_mailbox = nullptr;
    DeviceTransferHandle::PauseMailbox::PauseSlot::RequestHandle pause_request;
    std::optional<uint64_t> installed_version;
    // Selection follows provider order: P2P, RDMA, NCCL device, host proxy.
    std::vector<std::unique_ptr<RouteProvider>> route_providers;
    // These route providers initialized successfully, but a subsequent
    // operation failed. Because they may have already published their routes
    // and device kernels might still be consuming them, we keep them alive in
    // `poisoned_routes` until shutdown rather than destroying them immediately.
    struct PoisonedRoute {
        std::unique_ptr<RouteProvider> provider;
        bool staging_registered;
    };
    std::vector<PoisonedRoute> poisoned_routes;
    DeviceMetadata device_metadata;
    // Pinned host image copied into device_metadata.routes.
    DeviceTransferRoute* host_route_image = nullptr;
    std::vector<DeviceRouteType> published_route_types;
    DeviceTransferEndpoint local_endpoint;
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
    };
}

// Host metadata from the last successful GPU publication.
struct DeviceTransferService::PublishedState {
    int device_index = -1;
    const DeviceTransferHandle* device_handle = nullptr;
    DeviceTransferEndpoint local_endpoint;
    std::vector<DeviceTransferRoute> routes;
    std::vector<const RouteProvider*> route_providers;
    DeviceTransferRegion* peer_accessible_region = nullptr;
    DeviceTransferRegion* local_staging_region = nullptr;
};

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
    size_t peer_accessible_capacity, size_t local_staging_capacity,
    StrongStream& strong_stream, const DeviceRouteConfig& config) {
    std::lock_guard lock(resource_mutex_);
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
                               transfer_engine, link_manager, strong_stream,
                               config));

    device_ = std::move(device);
    publishHostState();
    return {};
}

DeviceTransferService::DeviceState& DeviceTransferService::deviceState() {
    return *device_;
}

void DeviceTransferService::publishHostState() {
    auto& state = deviceState();
    auto next = std::make_unique<PublishedState>();
    next->device_index = state.device_index;
    next->device_handle = state.device_metadata.handle;
    next->local_endpoint = state.local_endpoint;
    next->routes.assign(state.host_route_image,
                        state.host_route_image + state.max_world_size);
    for (const auto& provider : state.route_providers)
        next->route_providers.push_back(provider.get());
    next->peer_accessible_region = &state.peer_accessible_region;
    if (state.local_staging_handle_initialized)
        next->local_staging_region = &*state.local_staging_region;

    std::lock_guard lock(published_mutex_);
    published_.swap(next);
}

const DeviceTransferHandle* DeviceTransferService::deviceHandle() {
    std::lock_guard lock(published_mutex_);
    return published_ ? published_->device_handle : nullptr;
}

PGResult<bool> DeviceTransferService::isDirectlyAddressable(GlobalRank rank) {
    std::lock_guard lock(published_mutex_);
    PG_VALIDATE_STATE(published_, "DTS is unavailable");
    PG_VALIDATE_ARG(
        rank >= 0 && static_cast<size_t>(rank) < published_->routes.size(),
        "transfer rank is out of range");
    const auto& route = published_->routes[rank];
    PG_VALIDATE_STATE(route.type != DeviceRouteType::Unreachable,
                      "transfer peer " + std::to_string(rank) +
                          " has no device transfer route");
    return route.mappedRegionAddress() != 0;
}

DeviceTransferEndpoint DeviceTransferService::localEndpoint() const {
    std::lock_guard lock(published_mutex_);
    PG_ASSERT(published_, "DTS is unavailable");
    return published_->local_endpoint;
}

int DeviceTransferService::deviceIndex() const noexcept {
    std::lock_guard lock(published_mutex_);
    return published_ ? published_->device_index : -1;
}

const RouteProvider* DeviceTransferService::findRoute(
    std::string_view route_key) const noexcept {
    std::lock_guard lock(published_mutex_);
    if (!published_) return nullptr;
    for (const auto* provider : published_->route_providers) {
        if (provider->routeKey() == route_key) return provider;
    }
    return nullptr;
}

PGResult<RegionSlice> DeviceTransferService::allocatePeerAccessible(
    size_t size, size_t alignment) {
    std::lock_guard lock(published_mutex_);
    PG_VALIDATE_STATE(published_, "DTS is unavailable");
    return published_->peer_accessible_region->allocate(size, alignment);
}

PGResult<RegionSlice> DeviceTransferService::allocateLocalStaging(
    size_t size, size_t alignment) {
    {
        std::lock_guard lock(published_mutex_);
        PG_VALIDATE_STATE(published_, "DTS is unavailable");
        if (auto* region = published_->local_staging_region)
            return region->allocate(size, alignment);
    }

    // Preparing/registering backing memory and publishing its device handle
    // may issue CUDA calls. Release published_mutex_ before taking the resource
    // lock.
    std::lock_guard lock(resource_mutex_);
    PG_VALIDATE_STATE(device_ && !shutdown_requested_, "DTS is unavailable");
    PG_TRY(auto slice, deviceState().allocateLocalStaging(size, alignment));
    publishHostState();
    return slice;
}

PGResult<DeviceTransferEndpoint> DeviceTransferService::installEndpoints(
    const DeviceTransferSnapshot& snapshot, uint64_t reclaim_before_version) {
    std::lock_guard lock(resource_mutex_);
    PG_VALIDATE_STATE(device_ && !shutdown_requested_, "DTS is unavailable");
    auto& state = deviceState();
    PG_VALIDATE_STATE(
        !state.installed_version || snapshot.version > *state.installed_version,
        "endpoint installation must advance the current version");
    PG_VALIDATE_ARG(reclaim_before_version < snapshot.version,
                    "invalid endpoint reclaim watermark");
    PG_VALIDATE_ARG(snapshot.endpoints.size() == state.max_world_size,
                    "endpoint snapshot size does not match max world size");
    for (const auto& endpoint : snapshot.endpoints)
        PG_VALIDATE_ARG(!endpoint || validEndpoint(*endpoint),
                        "transfer-service endpoint is invalid");
    PG_TRY(auto guard, GpuDeviceGuard::create(state.device_index));

    // Prepare staging eagerly when endpoint installation requires all regions.
    if (std::any_of(state.route_providers.begin(), state.route_providers.end(),
                    [](const auto& provider) {
                        return provider->requiresAllRegionsForInstallation();
                    })) {
        PG_TRY(state.prepareLocalStaging());
    }

    state.installEndpoints(snapshot, reclaim_before_version);

    PG_ASSERT_OK(state.pause());
    PG_ASSERT_OK(state.updateRoutes());
    PG_ASSERT_OK(state.publishRoutes());
    state.installed_version = snapshot.version;
    std::vector<RouteEndpoint> routes;
    for (const auto& route : state.route_providers) {
        if (auto endpoint = route->localEndpoint())
            routes.push_back(std::move(*endpoint));
    }
    if (routes != state.local_endpoint.routes) {
        state.local_endpoint.routes = std::move(routes);
        ++state.local_endpoint.version;
    }
    // Publish the host view before resuming device users.
    publishHostState();
    state.resume();
    return state.local_endpoint;
}

PGResult<void> DeviceTransferService::shutdown() {
    std::lock_guard lock(resource_mutex_);
    shutdown_requested_ = true;
    {
        // Stop new queries and suballocations before destroying their backing
        // resources, without holding the host-state lock across CUDA cleanup.
        std::lock_guard published_lock(published_mutex_);
        published_.reset();
    }
    if (device_) {
        PG_TRY(device_->pause());
        PG_TRY(device_->shutdown());
        device_.reset();
    }
    return {};
}

}  // namespace mooncake
