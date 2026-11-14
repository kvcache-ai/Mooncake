#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_SERVICE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_SERVICE_H

#include <cstddef>
#include <cstdint>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>

#include "control_plane/control_types.h"
#include "device_comm/device_transfer/routes/p2p_route/p2p_route.h"
#include "device_comm/device_transfer/routes/rdma_route/rdma_route.h"
#include "device_comm/device_transfer/routes/nccl_device_route/nccl_device_route.h"
#include "device_comm/device_transfer/routes/host_proxy_route/host_proxy_route.h"
#include "device_comm/device_transfer/routes/route_provider.h"
#include "device_comm/device_transfer/transfer_region.h"
#include "device_comm/device_transfer/transfer_types.cuh"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class LinkManager;
class StrongStream;
class TransferEngine;
struct DeviceTransferHandle;

struct DeviceRouteConfig {
    P2pRouteOptions p2p;
    RdmaRouteOptions rdma;
    NcclDeviceRouteOptions nccl_device;
    HostProxyRouteOptions host_proxy;
};

class DeviceTransferService {
   public:
    DeviceTransferService();
    ~DeviceTransferService() noexcept;

    DeviceTransferService(const DeviceTransferService&) = delete;
    DeviceTransferService& operator=(const DeviceTransferService&) = delete;

    PGResult<void> initialize(GlobalRank self_rank, uint32_t max_world_size,
                              int device_index, TransferEngine& transfer_engine,
                              LinkManager& link_manager,
                              size_t peer_accessible_capacity,
                              size_t local_staging_capacity,
                              StrongStream& strong_stream,
                              const DeviceRouteConfig& config = {});

    // Allocate a slice from the stable peer-accessible region. The backing
    // region is published once through DeviceTransferEndpoint; individual
    // slices require no additional publication.
    PGResult<RegionSlice> allocatePeerAccessible(size_t size, size_t alignment);

    // Allocate a slice from a local-only source region, preparing its backing
    // memory if needed. Staging addresses are never published to peers.
    PGResult<RegionSlice> allocateLocalStaging(size_t size, size_t alignment);

    [[nodiscard]] int deviceIndex() const noexcept;
    [[nodiscard]] DeviceTransferEndpoint localEndpoint() const;
    // Borrow a loaded provider, or nullptr when it is unavailable.
    template <typename Route>
    [[nodiscard]] const Route* findRoute() const noexcept {
        return static_cast<const Route*>(findRoute(Route::kRouteKey));
    }

    // Device address of the stable kernel-facing service handle.
    const DeviceTransferHandle* deviceHandle();

    // Query whether the selected route has a direct memory mapping. This is a
    // control-path query; it does not synchronize with or copy from the GPU.
    PGResult<bool> isDirectlyAddressable(GlobalRank rank);

    // Install the snapshot and return the local endpoint after installation.
    // Local DTS users are paused while routes are switched and published.
    // Retired resources below reclaim_before_version may be released.
    PGResult<DeviceTransferEndpoint> installEndpoints(
        const DeviceTransferSnapshot& snapshot,
        uint64_t reclaim_before_version);

    PGResult<void> shutdown();

   private:
    struct DeviceState;
    struct PublishedState;

    const RouteProvider* findRoute(std::string_view route_key) const noexcept;

    // Caller holds resource_mutex_.
    DeviceState& deviceState();

    // Caller holds resource_mutex_. Device publication must have completed.
    // Acquires published_mutex_ without issuing CUDA calls.
    void publishHostState();

    // Serialize resource preparation, route installation and shutdown. CUDA
    // calls may block here, so recovery worker must not need this lock for
    // queries or suballocation from already published regions.
    std::mutex resource_mutex_;
    std::unique_ptr<DeviceState> device_;
    bool shutdown_requested_ = false;

    // Protect the last successfully published host view and region lifetimes
    // during suballocation. Never issue CUDA calls while holding this lock.
    // When both locks are needed, acquire resource_mutex_ first.
    mutable std::mutex published_mutex_;
    std::unique_ptr<PublishedState> published_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_SERVICE_H
