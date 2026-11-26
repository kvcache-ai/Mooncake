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

    struct ConnectionUpdate {
        std::vector<GlobalRank> ranks;
        std::vector<std::vector<uint8_t>> endpoint_metadata;
        std::vector<int64_t> remote_addrs;
        std::vector<int32_t> remote_keys;
        std::vector<int32_t> remote_qpns;
        std::vector<int32_t> remote_lids;
        std::vector<int64_t> subnet_prefixes;
        std::vector<int64_t> interface_ids;
        std::vector<int> active;
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
        atomic_sink.reset();
    }

    GpuBuffer<uint64_t> atomic_sink;
    std::unique_ptr<device::RdmaTransport> transport;
    device::RdmaMemoryRegion peer_accessible_region;
    device::RdmaMemoryRegion local_staging_region;
    device::RdmaMemoryRegion atomic_sink_region;
    int device_index = -1;
    cudaStream_t stream = nullptr;
    uint32_t qps_per_rank = 0;
    uint32_t num_qps = 0;
    std::vector<PeerConnection> peers;
    // installEndpoints() fills these parameters; updateRoutes() changes QPs
    // only while DTS users are paused.
    ConnectionUpdate pending;
};

RdmaRoute::RdmaRoute(GlobalRank self_rank, uint32_t max_world_size,
                     std::unique_ptr<State> state)
    : RouteProvider(DeviceRouteType::Rdma, kRouteKey, kEndpointVersion),
      self_rank_(self_rank),
      max_world_size_(max_world_size),
      state_(std::move(state)) {}

PGResult<std::unique_ptr<RdmaRoute>> RdmaRoute::create(
    int device_index, cudaStream_t stream, GlobalRank self_rank,
    uint32_t max_world_size, const RdmaRouteOptions& options) {
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    auto state = std::make_unique<State>();
    state->device_index = device_index;
    state->stream = stream;
    state->qps_per_rank = qpsPerRank(max_world_size);
    state->num_qps = max_world_size * state->qps_per_rank;
    state->peers.resize(max_world_size);
    state->transport =
        device::createIbgdaDeviceTransport(options.device_filter);
    if (!state->transport) {
        return makePGError(PGErrorCode::NotSupported,
                           "device-initiated RDMA transport is unavailable");
    }
    PG_TRY_TE(state->transport->initialize("", static_cast<int>(max_world_size),
                                           static_cast<int>(state->num_qps)));
    PG_TRY_TE(state->transport->allocateControlBuffer());
    PG_TRY_TE(state->transport->createQueuePairs(stream));

    PG_TRY(state->atomic_sink, GpuBuffer<uint64_t>::allocate(device_index));
    PG_TRY_TE(state->transport->registerMemory(state->atomic_sink.get(),
                                               state->atomic_sink.sizeBytes(),
                                               state->atomic_sink_region));
    LOG(INFO) << "[PG] Device-initiated RDMA route initialized with "
              << state->qps_per_rank << " QPs per rank";
    return std::unique_ptr<RdmaRoute>(
        new RdmaRoute(self_rank, max_world_size, std::move(state)));
}

RdmaRoute::~RdmaRoute() noexcept {
    auto result = shutdown();
    if (!result.has_value()) {
        LOG(ERROR) << "RDMA route shutdown failed during destruction: "
                   << result.error().message;
    }
}

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
    return encodeEndpoint(RdmaEndpointMetadata{
        .is_roce = state_->transport->isRoce(),
        .qps_per_rank = state_->qps_per_rank,
        .remote_key = metadata.rkey,
        .subnet_prefix = metadata.subnet_prefix,
        .interface_id = metadata.interface_id,
        .qpns = metadata.qpns,
        .lids = metadata.lids,
    });
}

PGResult<std::vector<DeviceTransferRoute>> RdmaRoute::installEndpoints(
    const DeviceTransferSnapshot& snapshot, uint64_t) {
    std::vector<DeviceTransferRoute> routes(max_world_size_);
    if (!state_) return routes;
    const auto& endpoints = snapshot.endpoints;

    const auto local_metadata =
        state_->transport->localMetadata(state_->peer_accessible_region);
    // Prepare TE's per-rank memory/GID tables and per-QP QPN/LID tables.
    State::ConnectionUpdate pending;
    pending.endpoint_metadata.resize(max_world_size_);
    pending.remote_addrs.resize(max_world_size_);
    pending.remote_keys.resize(max_world_size_);
    pending.remote_qpns.resize(state_->num_qps);
    pending.remote_lids.resize(state_->num_qps);
    pending.subnet_prefixes.resize(max_world_size_);
    pending.interface_ids.resize(max_world_size_);
    pending.active.resize(max_world_size_);

    for (GlobalRank rank = 0; rank < static_cast<GlobalRank>(max_world_size_);
         ++rank) {
        // Decode this peer once and check link/QP layout compatibility.
        if (!endpoints[rank]) continue;
        const auto* endpoint = findEndpoint(endpoints[rank]->routes);
        if (!endpoint) continue;
        PG_TRY(auto candidate, decodeEndpoint<RdmaEndpointMetadata>(*endpoint));
        const bool compatible =
            candidate.is_roce == state_->transport->isRoce() &&
            (candidate.is_roce ||
             candidate.subnet_prefix == local_metadata.subnet_prefix) &&
            candidate.qps_per_rank == state_->qps_per_rank &&
            candidate.qpns.size() == state_->num_qps &&
            candidate.lids.size() == state_->num_qps &&
            candidate.remote_key != 0;
        if (!compatible) continue;

        const auto& peer = state_->peers[rank];
        if (!peer.connected || peer.endpoint_metadata != endpoint->metadata) {
            // Collect new and changed peers for the connect batch.
            pending.endpoint_metadata[rank] = endpoint->metadata;
            pending.remote_addrs[rank] =
                static_cast<int64_t>(endpoints[rank]->region_address);
            pending.remote_keys[rank] = candidate.remote_key;
            pending.subnet_prefixes[rank] = candidate.subnet_prefix;
            pending.interface_ids[rank] = candidate.interface_id;
            pending.active[rank] = 1;
            pending.ranks.push_back(rank);

            // QP groups are indexed by destination rank: our group for this
            // peer connects to its group for self_rank_.
            for (uint32_t q = 0; q < state_->qps_per_rank; ++q) {
                const size_t local_index =
                    static_cast<size_t>(rank) * state_->qps_per_rank + q;
                const size_t remote_index =
                    static_cast<size_t>(self_rank_) * state_->qps_per_rank + q;
                pending.remote_qpns[local_index] = candidate.qpns[remote_index];
                pending.remote_lids[local_index] = candidate.lids[remote_index];
            }
        }

        // Prepare a device route for both existing and new connections.
        routes[rank] = DeviceTransferRoute{
            .type = DeviceRouteType::Rdma,
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

    state_->pending = std::move(pending);
    return routes;
}

PGResult<void> RdmaRoute::updateRoutes() {
    if (!state_ || state_->pending.ranks.empty()) return {};
    auto& pending = state_->pending;
    PG_TRY(auto device_guard, GpuDeviceGuard::create(state_->device_index));
    // Peer reset: reuse our advertised QPNs when a peer restarts.
    for (const auto rank : pending.ranks) {
        if (!state_->peers[rank].connected) continue;
        PG_TRY_TE(state_->transport->resetQueuePairs(
            rank * state_->qps_per_rank, state_->qps_per_rank, state_->stream));
        LOG(INFO) << "[PG] Device RDMA QPs reset for peer=" << rank;
    }

    // Connect the pending QP groups in one batch.
    PG_TRY_TE(state_->transport->connectPeers(
        self_rank_, state_->transport->isRoce(),
        state_->peer_accessible_region.lkey, pending.remote_addrs,
        pending.remote_keys, pending.remote_qpns, pending.remote_lids,
        pending.subnet_prefixes, pending.interface_ids, pending.active));

    // Save connection state only after the batch reports success.
    for (const auto rank : pending.ranks) {
        auto& peer = state_->peers[rank];
        peer.connected = true;
        peer.endpoint_metadata = std::move(pending.endpoint_metadata[rank]);
    }
    pending = {};
    return {};
}

void RdmaRoute::fillDeviceContext(DeviceRouteContext& context) const noexcept {
    if (!state_) return;
    context.rdma = DeviceRdmaContext{
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
        .atomic_sink = state_->atomic_sink.get(),
        .atomic_sink_lkey = state_->atomic_sink_region.lkey,
    };
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
