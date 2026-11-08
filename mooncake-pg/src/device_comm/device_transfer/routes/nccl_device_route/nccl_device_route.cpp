#include "device_comm/device_transfer/routes/nccl_device_route/nccl_device_route.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

#ifdef USE_NCCL_DEVICE
#include <cuda.h>
#include <nccl.h>
#include <transport/device/nccl_device_transport.h>
#endif
#include "gpu_runtime.h"

namespace mooncake {
#ifdef USE_NCCL_DEVICE

// Defined in the CUDA translation unit
cudaError_t preloadNcclDeviceDrainKernel();
cudaError_t launchNcclDeviceDrainKernel(
    const device::NcclDeviceContext* context, int num_ranks,
    int gin_context_count, uint64_t timeout_ticks, cudaStream_t stream);

namespace {

struct NcclDeviceEndpointMetadata {
    uint32_t context_count;
    std::vector<int32_t> next_unique_id;
};

PGResult<std::vector<int32_t>> createUniqueId() {
    auto transport = device::createNcclDeviceTransport();
    PG_VALIDATE_STATE(transport, "NCCL device transport is unavailable");
    auto unique_id = transport->createUniqueId();
    PG_VALIDATE_STATE(!unique_id.empty(), "NCCL unique ID creation failed");
    return unique_id;
}

}  // namespace

struct NcclDeviceRoute::State {
    struct Generation {
        struct DeviceContexts {
            device::NcclDeviceContext peer_accessible;
            device::NcclDeviceContext local_staging;
        };

        ~Generation() noexcept {
            auto result = release();
            if (!result.has_value())
                LOG(ERROR) << "NCCL generation cleanup failed: "
                           << result.error().message;
        }

        PGResult<void> release() {
            PG_TRY(auto guard, GpuDeviceGuard::create(device_index));
            if (transport) {
                PG_TRY_TE(transport->abort());
                transport.reset();
            }
            if (device_contexts) {
                PG_TRY_CUDA(cudaFree(device_contexts));
                device_contexts = nullptr;
            }
            return {};
        }

        int device_index = -1;
        // Installation this generation belongs to.
        uint64_t installation_version = 0;
        std::unique_ptr<device::NcclTransport> transport;
        DeviceContexts* device_contexts = nullptr;
        std::vector<DeviceTransferRoute> routes;
    };

    int device_index = -1;
    // Unique ID included in the local endpoint for communicator creation.
    // When this rank provides the ID, installEndpoints() consumes it and
    // generates a replacement for a future installation.
    std::vector<int32_t> next_unique_id;
    DeviceLocalRegion peer_accessible_region;
    DeviceLocalRegion staging_region;
    std::optional<GpuStream> stream;
    uint64_t drain_timeout_ticks = 0;
    // installEndpoints() prepares pending; updateRoutes() promotes it to
    // current and moves the previous current into retired_generations.
    std::unique_ptr<Generation> pending;
    std::unique_ptr<Generation> current;
    // Eager destruction can deadlock endpoint installation when cudaFree waits
    // for GPU work that depends on the new endpoints. Keep retired generations
    // alive until shutdown.
    // FIXME: Find a safe time to reclaim them; otherwise NCCL communicators
    // and their resources accumulate with repeated endpoint installations.
    std::vector<std::unique_ptr<Generation>> retired_generations;
};

NcclDeviceRoute::NcclDeviceRoute(GlobalRank self_rank, uint32_t max_world_size,
                                 uint32_t context_count,
                                 std::unique_ptr<State> state)
    : RouteProvider(DeviceRouteType::NcclDevice, kRouteKey, kEndpointVersion),
      state_(std::move(state)),
      self_rank_(self_rank),
      max_world_size_(max_world_size),
      context_count_(context_count) {}

PGResult<std::unique_ptr<NcclDeviceRoute>> NcclDeviceRoute::create(
    int device_index, GlobalRank self_rank, uint32_t max_world_size,
    const NcclDeviceRouteOptions& options) {
    PG_VALIDATE_ARG(options.context_count > 0,
                    "NCCL GIN context count must be positive");
    PG_TRY(auto guard, GpuDeviceGuard::create(device_index));
    // Preload the drain kernel to avoid deadlock
    PG_TRY_CUDA(preloadNcclDeviceDrainKernel());
    auto state = std::make_unique<State>();
    state->device_index = device_index;
    PG_TRY(state->stream, GpuStream::createNonBlocking(device_index));
    PG_TRY(state->drain_timeout_ticks,
           gpuTimeoutTicks(device_index, kTransferDrainTimeoutMs * 1000));
    PG_TRY(state->next_unique_id, createUniqueId());
    return std::unique_ptr<NcclDeviceRoute>(new NcclDeviceRoute(
        self_rank, max_world_size, options.context_count, std::move(state)));
}

NcclDeviceRoute::~NcclDeviceRoute() noexcept {
    auto result = shutdown();
    if (!result.has_value())
        LOG(ERROR) << "NCCL device route shutdown failed: "
                   << result.error().message;
}

PGResult<void> NcclDeviceRoute::registerRegion(DeviceRegionKind kind,
                                               void* addr, size_t size) {
    PG_VALIDATE_STATE(state_ && !state_->pending && !state_->current &&
                          state_->retired_generations.empty(),
                      "registerRegion() must be called after create() and "
                      "before the first installEndpoints()");
    PG_VALIDATE_ARG(addr && size != 0, "NCCL device region is empty");
    // Validate locally before any participant enters collective registration.
    CUmemGenericAllocationHandle allocation;
    if (cuMemRetainAllocationHandle(&allocation, addr) != CUDA_SUCCESS)
        return makePGError(PGErrorCode::NotSupported,
                           "NCCL device route requires a VMM-backed region");
    PG_VALIDATE_STATE(cuMemRelease(allocation) == CUDA_SUCCESS,
                      "NCCL device allocation handle release failed");
    auto& region = kind == DeviceRegionKind::PeerAccessible
                       ? state_->peer_accessible_region
                       : state_->staging_region;
    PG_VALIDATE_STATE(!region.addr, "NCCL device region is already prepared");
    region = {addr, size};
    return {};
}

PGResult<void> NcclDeviceRoute::unregisterRegion(DeviceRegionKind kind,
                                                 void* addr, size_t size) {
    if (!state_) return {};
    PG_VALIDATE_STATE(!state_->pending && !state_->current &&
                          state_->retired_generations.empty(),
                      "stop the NCCL route before releasing its regions");
    auto& region = kind == DeviceRegionKind::PeerAccessible
                       ? state_->peer_accessible_region
                       : state_->staging_region;
    PG_VALIDATE_STATE(region.addr == addr && region.size == size,
                      "NCCL device region is not registered");
    region = {};
    return {};
}

std::optional<RouteEndpoint> NcclDeviceRoute::localEndpoint() {
    if (!state_ || state_->next_unique_id.empty()) return std::nullopt;
    return RouteEndpoint{
        .route_key = std::string(kRouteKey),
        .version = kEndpointVersion,
        .metadata = encodeEndpointMetadata(
            NcclDeviceEndpointMetadata{context_count_, state_->next_unique_id}),
    };
}

PGResult<void> NcclDeviceRoute::installEndpoints(
    const DeviceTransferSnapshot& snapshot, uint64_t) {
    PG_VALIDATE_STATE(state_, "NCCL device route is unavailable");
    PG_VALIDATE_STATE(!state_->pending,
                      "previous NCCL generation has not been published");
    PG_TRY(auto guard, GpuDeviceGuard::create(state_->device_index));
    auto next = std::make_unique<State::Generation>();
    next->device_index = state_->device_index;
    next->installation_version = snapshot.version;
    next->transport = device::createNcclDeviceTransport();
    PG_VALIDATE_STATE(next->transport, "NCCL device transport is unavailable");
    next->routes.resize(max_world_size_);
    std::vector<GlobalRank> nccl_rank_order;
    std::vector<int32_t> unique_id;
    for (const auto rank : snapshot.participants) {
        PG_TRY(auto endpoint, findEndpoint(snapshot.endpoints[rank]));
        if (!endpoint) continue;
        PG_TRY(auto metadata,
               decodeEndpointMetadata<NcclDeviceEndpointMetadata>(*endpoint));
        PG_VALIDATE_ARG(metadata.context_count == context_count_,
                        "NCCL device context counts differ");
        // The first participant with an NCCL endpoint provides the unique ID.
        if (nccl_rank_order.empty())
            unique_id = std::move(metadata.next_unique_id);
        nccl_rank_order.push_back(rank);
    }
    const auto self_rank_it =
        std::find(nccl_rank_order.begin(), nccl_rank_order.end(), self_rank_);
    PG_VALIDATE_ARG(self_rank_it != nccl_rank_order.end() && !unique_id.empty(),
                    "NCCL bootstrap endpoint is incomplete");
    device::NcclTransportConfig nccl_config;
    nccl_config.rank = static_cast<int>(self_rank_it - nccl_rank_order.begin());
    nccl_config.num_ranks = static_cast<int>(nccl_rank_order.size());
    nccl_config.gin_connection_type = device::NcclGinConnectionType::kFull;
    nccl_config.gin_context_count = context_count_;
    auto& transport = *next->transport;
    const bool this_rank_provides_unique_id =
        nccl_rank_order.front() == self_rank_;
    if (this_rank_provides_unique_id) {
        PG_VALIDATE_STATE(
            state_->next_unique_id == unique_id,
            "NCCL unique ID in snapshot differs from the locally prepared ID");
        // Consume before initialization; failed attempts never return the ID.
        unique_id = std::exchange(state_->next_unique_id, {});
    }
    PG_TRY_TE(transport.initialize(nccl_config, unique_id));

    // Generations register the same PG allocations, so writes and VA
    // signals through either communicator reach the same receiver storage.
    device::NcclBufferRegistration peer_registration;
    device::NcclBufferRegistration staging_registration;
    PG_TRY_TE(transport.registerBuffer(state_->peer_accessible_region.addr,
                                       state_->peer_accessible_region.size,
                                       &peer_registration));
    PG_TRY_TE(transport.registerBuffer(state_->staging_region.addr,
                                       state_->staging_region.size,
                                       &staging_registration));
    for (int nccl_rank = 0; nccl_rank < nccl_config.num_ranks; ++nccl_rank) {
        const auto global_rank = nccl_rank_order[nccl_rank];
        auto* const mapped_region =
            transport.peerPointer(peer_registration, nccl_rank);
        next->routes[global_rank] = DeviceTransferRoute{
            .type = DeviceRouteType::NcclDevice,
            .region_size = snapshot.endpoints[global_rank]->region_size,
            .nccl = {.nccl_rank = nccl_rank,
                     .mapped_region_address =
                         reinterpret_cast<uint64_t>(mapped_region)},
        };
    }
    const State::Generation::DeviceContexts device_contexts{
        .peer_accessible = transport.deviceContext(peer_registration),
        .local_staging = transport.deviceContext(staging_registration),
    };
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&next->device_contexts),
                           sizeof(device_contexts)));
    PG_TRY_CUDA(cudaMemcpy(next->device_contexts, &device_contexts,
                           sizeof(device_contexts), cudaMemcpyHostToDevice));
    if (this_rank_provides_unique_id) {
        // The completion ACK carries the next unique ID in the local endpoint.
        PG_TRY(state_->next_unique_id, createUniqueId());
    }
    state_->pending = std::move(next);
    return {};
}

PGResult<void> NcclDeviceRoute::drainCurrent() {
    if (!state_ || !state_->current) return {};
    auto& current = *state_->current;
    PG_TRY(auto guard, GpuDeviceGuard::create(state_->device_index));
    const auto properties = current.transport->properties();
    PG_TRY_CUDA(launchNcclDeviceDrainKernel(
        &current.device_contexts->peer_accessible, properties.num_ranks,
        properties.gin_context_count, state_->drain_timeout_ticks,
        state_->stream->get()));
    PG_TRY(state_->stream->synchronize());
    return {};
}

PGResult<std::vector<DeviceTransferRoute>> NcclDeviceRoute::updateRoutes() {
    PG_ASSERT(state_ && state_->pending,
              "updateRoutes() requires a pending generation from "
              "installEndpoints()");
    // DTS's pause stops producers from submitting new work, but GIN operations
    // already queued on the NIC may still be in flight. Try to drain the
    // current communicator's queues before switching to the replacement.
    auto drained = drainCurrent();
    if (!drained.has_value())
        LOG(WARNING) << "NCCL generation drain failed; continuing: "
                     << drained.error().message;
    if (state_->current) {
        const auto retired_version = state_->current->installation_version;
        state_->retired_generations.push_back(std::move(state_->current));
        LOG(INFO) << "[PG] Retaining retired NCCL generation: rank="
                  << self_rank_ << " version=" << retired_version
                  << " installing_version="
                  << state_->pending->installation_version
                  << " retired_count=" << state_->retired_generations.size();
    }
    state_->current = std::move(state_->pending);
    return state_->current->routes;
}

void NcclDeviceRoute::fillDeviceContext(
    DeviceRouteContext& context) const noexcept {
    context.nccl = {};
    if (!state_ || !state_->current) return;
    const auto* device_contexts = state_->current->device_contexts;
    context.nccl = {
        .peer_accessible_ctx = &device_contexts->peer_accessible,
        .local_staging_ctx = &device_contexts->local_staging,
        .peer_accessible_region = state_->peer_accessible_region,
    };
}

PGResult<void> NcclDeviceRoute::shutdown() {
    if (!state_) return {};
    PG_TRY(auto guard, GpuDeviceGuard::create(state_->device_index));
    if (state_->pending) {
        PG_TRY(state_->pending->release());
        state_->pending.reset();
    }
    if (state_->current) {
        PG_TRY(state_->current->release());
        state_->current.reset();
    }
    for (auto& retired : state_->retired_generations) {
        PG_TRY(retired->release());
    }
    state_->retired_generations.clear();
    state_.reset();
    return {};
}

#else

struct NcclDeviceRoute::State {};

NcclDeviceRoute::NcclDeviceRoute(GlobalRank self_rank, uint32_t max_world_size,
                                 uint32_t context_count,
                                 std::unique_ptr<State> state)
    : RouteProvider(DeviceRouteType::NcclDevice, kRouteKey, kEndpointVersion),
      state_(std::move(state)),
      self_rank_(self_rank),
      max_world_size_(max_world_size),
      context_count_(context_count) {}

PGResult<std::unique_ptr<NcclDeviceRoute>> NcclDeviceRoute::create(
    int, GlobalRank, uint32_t, const NcclDeviceRouteOptions&) {
    return makePGError(PGErrorCode::NotSupported,
                       "NCCL device route requires USE_NCCL_DEVICE=ON");
}

NcclDeviceRoute::~NcclDeviceRoute() noexcept = default;

PGResult<void> NcclDeviceRoute::registerRegion(DeviceRegionKind, void*,
                                               size_t) {
    return {};
}

PGResult<void> NcclDeviceRoute::unregisterRegion(DeviceRegionKind, void*,
                                                 size_t) {
    return {};
}

std::optional<RouteEndpoint> NcclDeviceRoute::localEndpoint() {
    return std::nullopt;
}

PGResult<void> NcclDeviceRoute::installEndpoints(const DeviceTransferSnapshot&,
                                                 uint64_t) {
    return {};
}

PGResult<void> NcclDeviceRoute::drainCurrent() { return {}; }

PGResult<std::vector<DeviceTransferRoute>> NcclDeviceRoute::updateRoutes() {
    return {};
}

void NcclDeviceRoute::fillDeviceContext(DeviceRouteContext&) const noexcept {}

PGResult<void> NcclDeviceRoute::shutdown() { return {}; }

#endif

}  // namespace mooncake
