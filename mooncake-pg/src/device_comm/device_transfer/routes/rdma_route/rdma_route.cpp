#include "device_comm/device_transfer/routes/rdma_route/rdma_route.h"

#include <algorithm>
#include <optional>
#include <utility>

#include <glog/logging.h>
#include <transport/device/device_transport.h>

#include "gpu_runtime.h"

namespace mooncake {
namespace {

constexpr uint32_t kPreferredQpsPerRank = 8;
constexpr uint32_t kMaxQueuePairs = 256;

uint32_t qpsPerRank(uint32_t max_world_size) {
    return std::max<uint32_t>(
        1, std::min(kPreferredQpsPerRank, kMaxQueuePairs / max_world_size));
}

struct RdmaEndpointMetadata {
    bool is_roce = false;
    uint32_t qps_per_rank = 0;
    int32_t remote_key = 0;
    int64_t subnet_prefix = 0;
    int64_t interface_id = 0;
    std::vector<int32_t> qpns;
    std::vector<int32_t> lids;
};

}  // namespace

struct RdmaRoute::State {
    struct PeerConnection {
        bool connected = false;
        std::vector<uint8_t> endpoint_metadata;
    };

    ~State() noexcept {
        auto guard_result = GpuDeviceGuard::create(device_index);
        if (!guard_result.has_value()) {
            LOG(ERROR) << "Failed to select the RDMA route CUDA device during "
                          "cleanup: "
                       << guard_result.error().message;
        }
        releaseResources();
    }

    void releaseResources() noexcept {
        // The transport destroys QPs and any remaining registrations before
        // route-owned CUDA allocations are released.
        transport.reset();
        if (atomic_sink) {
            const auto result = cudaFree(atomic_sink);
            if (result != cudaSuccess) {
                LOG(ERROR) << "Failed to release RDMA atomic sink: "
                           << cudaGetErrorString(result);
            }
        }
    }

    std::unique_ptr<device::RdmaTransport> transport;
    device::RdmaMemoryRegion peer_accessible_region;
    device::RdmaMemoryRegion local_staging_region;
    device::RdmaMemoryRegion atomic_sink_region;
    uint64_t* atomic_sink = nullptr;
    int device_index = -1;
    cudaStream_t stream = nullptr;
    uint32_t qps_per_rank = 0;
    uint32_t num_qps = 0;
    std::vector<PeerConnection> peers;
};

RdmaRoute::RdmaRoute(GlobalRank self_rank, uint32_t max_world_size,
                     RdmaRouteOptions options)
    : self_rank_(self_rank),
      max_world_size_(max_world_size),
      options_(std::move(options)) {}

RdmaRoute::~RdmaRoute() noexcept {
    auto result = shutdown();
    if (!result.has_value()) {
        LOG(ERROR) << "RDMA route shutdown failed during destruction: "
                   << result.error().message;
    }
}

PGResult<void> RdmaRoute::initialize(int device_index, cudaStream_t stream) {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    auto state = std::make_unique<State>();
    state->device_index = device_index;
    state->stream = stream;
    state->qps_per_rank = qpsPerRank(max_world_size_);
    state->num_qps = max_world_size_ * state->qps_per_rank;
    state->peers.resize(max_world_size_);
    state->transport =
        device::createIbgdaDeviceTransport(options_.device_filter);
    if (!state->transport) {
        return makePGError(PGErrorCode::NotSupported,
                           "device-initiated RDMA transport is unavailable");
    }
    PG_TRY_TE(state->transport->initialize("",
                                           static_cast<int>(max_world_size_),
                                           static_cast<int>(state->num_qps)));
    PG_TRY_TE(state->transport->allocateControlBuffer());
    PG_TRY_TE(state->transport->createQueuePairs(stream));

    constexpr size_t atomic_sink_bytes = sizeof(uint64_t);
    PG_TRY_CUDA(cudaMalloc(reinterpret_cast<void**>(&state->atomic_sink),
                           atomic_sink_bytes));
    PG_TRY_TE(state->transport->registerMemory(
        state->atomic_sink, atomic_sink_bytes, state->atomic_sink_region));
    state_ = std::move(state);
    LOG(INFO) << "[PG] Device-initiated RDMA route initialized with "
              << state_->qps_per_rank << " QPs per rank";
    return {};
}

DeviceRdmaContext RdmaRoute::deviceContext() const noexcept {
    if (!state_) return {};
    return DeviceRdmaContext{
        .qp_devctxs = state_->transport->qpDevCtxsPtr(),
        .peer_accessible_region =
            {
                .addr = state_->peer_accessible_region.addr,
                .size = state_->peer_accessible_region.size,
            },
        .local_staging_region =
            {
                .addr = state_->local_staging_region.addr,
                .size = state_->local_staging_region.size,
            },
        .peer_accessible_lkey = state_->peer_accessible_region.lkey,
        .local_staging_lkey = state_->local_staging_region.lkey,
        .qps_per_rank = state_->qps_per_rank,
        .atomic_sink = state_->atomic_sink,
        .atomic_sink_lkey = state_->atomic_sink_region.lkey,
    };
}

std::string_view RdmaRoute::routeKey() const noexcept { return kRouteKey; }

uint32_t RdmaRoute::routeVersion() const noexcept { return kEndpointVersion; }

PGResult<void> RdmaRoute::registerRegion(DeviceRegionKind kind, void* addr,
                                         size_t size) {
    if (!state_) return {};
    auto* region = kind == DeviceRegionKind::PeerAccessible
                       ? &state_->peer_accessible_region
                       : &state_->local_staging_region;
    PG_TRY_TE(state_->transport->registerMemory(addr, size, *region));
    return {};
}

PGResult<void> RdmaRoute::unregisterRegion(DeviceRegionKind kind, void*,
                                           size_t) {
    if (!state_) return {};
    auto* region = kind == DeviceRegionKind::PeerAccessible
                       ? &state_->peer_accessible_region
                       : &state_->local_staging_region;
    PG_TRY_TE(state_->transport->unregisterMemory(*region));
    *region = {};
    return {};
}

std::optional<RouteEndpoint> RdmaRoute::localEndpoint() {
    if (!state_) return std::nullopt;
    const auto metadata =
        state_->transport->localMetadata(state_->peer_accessible_region);
    return RouteEndpoint{
        .route_key = std::string(kRouteKey),
        .version = routeVersion(),
        .metadata = encodeEndpointMetadata(RdmaEndpointMetadata{
            .is_roce = state_->transport->isRoce(),
            .qps_per_rank = state_->qps_per_rank,
            .remote_key = metadata.rkey,
            .subnet_prefix = metadata.subnet_prefix,
            .interface_id = metadata.interface_id,
            .qpns = metadata.qpns,
            .lids = metadata.lids,
        }),
    };
}

PGResult<std::vector<DeviceTransferRoute>> RdmaRoute::resolveRoutes(
    std::span<const std::optional<DeviceTransferEndpoint>> endpoints) {
    std::vector<DeviceTransferRoute> routes(max_world_size_);
    if (!state_) return routes;

    const auto local_metadata =
        state_->transport->localMetadata(state_->peer_accessible_region);
    // Prepare TE's per-rank memory/GID tables and per-QP QPN/LID tables.
    std::vector<const RouteEndpoint*> route_endpoints(max_world_size_, nullptr);
    std::vector<int64_t> remote_addrs(max_world_size_);
    std::vector<int32_t> remote_keys(max_world_size_);
    std::vector<int32_t> remote_qpns(state_->num_qps);
    std::vector<int32_t> remote_lids(state_->num_qps);
    std::vector<int64_t> subnet_prefixes(max_world_size_);
    std::vector<int64_t> interface_ids(max_world_size_);
    std::vector<int> active(max_world_size_);
    std::vector<GlobalRank> pending;

    for (GlobalRank rank = 0; rank < static_cast<GlobalRank>(max_world_size_);
         ++rank) {
        // Decode this peer once and check link/QP layout compatibility.
        PG_TRY(auto endpoint, findEndpoint(endpoints[rank]));
        if (!endpoint) continue;
        PG_TRY(auto candidate,
               decodeEndpointMetadata<RdmaEndpointMetadata>(*endpoint));
        const bool compatible =
            candidate.is_roce == state_->transport->isRoce() &&
            (candidate.is_roce ||
             candidate.subnet_prefix == local_metadata.subnet_prefix) &&
            candidate.qps_per_rank == state_->qps_per_rank &&
            candidate.qpns.size() == state_->num_qps &&
            candidate.lids.size() == state_->num_qps &&
            candidate.remote_key != 0;
        if (!compatible) continue;

        auto& peer = state_->peers[rank];
        if (!peer.connected || peer.endpoint_metadata != endpoint->metadata) {
            // Collect new and changed peers for the connect batch.
            route_endpoints[rank] = endpoint;
            remote_addrs[rank] =
                static_cast<int64_t>(endpoints[rank]->region_address);
            remote_keys[rank] = candidate.remote_key;
            subnet_prefixes[rank] = candidate.subnet_prefix;
            interface_ids[rank] = candidate.interface_id;
            active[rank] = 1;
            pending.push_back(rank);

            // QP groups are indexed by destination rank: our group for this
            // peer connects to its group for self_rank_.
            for (uint32_t q = 0; q < state_->qps_per_rank; ++q) {
                const size_t local_index =
                    static_cast<size_t>(rank) * state_->qps_per_rank + q;
                const size_t remote_index =
                    static_cast<size_t>(self_rank_) * state_->qps_per_rank + q;
                remote_qpns[local_index] = candidate.qpns[remote_index];
                remote_lids[local_index] = candidate.lids[remote_index];
            }
        }

        // Prepare a device route for both existing and new connections.
        routes[rank] = DeviceTransferRoute{
            .kind = DeviceRouteKind::Rdma,
            .region_size = endpoints[rank]->region_size,
            .rdma =
                {
                    .remote_region_address = endpoints[rank]->region_address,
                    .remote_key = static_cast<uint32_t>(candidate.remote_key),
                    .qp_offset =
                        static_cast<uint32_t>(rank) * state_->qps_per_rank,
                },
        };
    }

    if (!pending.empty()) {
        PG_TRY(auto device_guard, GpuDeviceGuard::create(state_->device_index));
        // Peer reset: reuse our advertised QPNs when a peer restarts.
        for (const auto rank : pending) {
            if (!state_->peers[rank].connected) continue;
            PG_TRY_TE(state_->transport->resetQueuePairs(
                rank * state_->qps_per_rank, state_->qps_per_rank,
                state_->stream));
            LOG(INFO) << "[PG] Device RDMA QPs reset for peer=" << rank;
        }

        // Connect the pending QP groups in one batch.
        PG_TRY_TE(state_->transport->connectPeers(
            self_rank_, state_->transport->isRoce(),
            state_->peer_accessible_region.lkey, remote_addrs, remote_keys,
            remote_qpns, remote_lids, subnet_prefixes, interface_ids, active));

        // Save connection state only after the batch reports success.
        for (const auto rank : pending) {
            auto& peer = state_->peers[rank];
            peer.connected = true;
            peer.endpoint_metadata = route_endpoints[rank]->metadata;
        }
    }

    return routes;
}

PGResult<void> RdmaRoute::shutdown() {
    if (shutdown_requested_) return {};
    if (!state_) {
        shutdown_requested_ = true;
        return {};
    }

    PG_TRY(auto device_guard, GpuDeviceGuard::create(state_->device_index));
    // Kernel completion stops producers but may leave signal WQEs in flight.
    // Deliver trailing acknowledgments before stopping QPs, since a peer may
    // still need them to finish its last collective.
    if (state_->transport->drainQueuePairs(state_->stream,
                                           kTransferDrainTimeoutMs) != 0) {
        LOG(WARNING) << "[PG] RDMA shutdown drain failed; destroying QPs";
    }
    // State destroys the transport's QPs before unregistering memory and
    // freeing the atomic sink. DTS can then release its backing allocations.
    state_.reset();
    shutdown_requested_ = true;
    return {};
}

}  // namespace mooncake
