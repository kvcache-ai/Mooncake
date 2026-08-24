#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_SERVICE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_SERVICE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "control_plane/control_types.h"
#include "device_comm/device_transfer/transfer_region.h"
#include "device_comm/device_transfer/transfer_types.cuh"
#include "error_types.h"

namespace mooncake {

class LinkManager;
class TransferEngine;
struct DeviceTransferHandle;

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
                              size_t local_staging_capacity);

    // Allocate a slice from the stable peer-accessible region. The backing
    // region is published once through DeviceTransferEndpoint; individual
    // slices require no additional publication.
    PGResult<RegionSlice> allocatePeerAccessible(size_t size, size_t alignment);

    // Allocate a slice from a local-only source region. Its backing memory is
    // allocated and registered lazily on the first request and is never
    // published to peers.
    PGResult<RegionSlice> allocateLocalStaging(size_t size, size_t alignment);

    // Immutable bootstrap metadata for the initialized CUDA device.
    [[nodiscard]] int deviceIndex() const noexcept;
    [[nodiscard]] const DeviceTransferEndpoint& localEndpoint() const noexcept;

    // Device address of the stable kernel-facing service handle.
    const DeviceTransferHandle* deviceHandle();

    // Read the selected route from the service's host route image. This is a
    // control-path query; it does not synchronize with or copy from the GPU.
    PGResult<DeviceRouteKind> routeKind(GlobalRank rank);

    // Install the immutable endpoint published by one peer for its current
    // rank epoch. Rank-epoch validation remains in the control plane.
    PGResult<void> installPeerEndpoint(GlobalRank rank,
                                       const DeviceTransferEndpoint& endpoint);

    PGResult<void> waitUntilIdle();

    PGResult<void> shutdown();

   private:
    struct DeviceState;

    // Caller holds mutex_.
    DeviceState& deviceState();

    std::mutex mutex_;
    std::unique_ptr<DeviceState> device_;
    bool shutdown_requested_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_SERVICE_H
