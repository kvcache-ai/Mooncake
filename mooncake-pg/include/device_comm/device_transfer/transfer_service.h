#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_SERVICE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_SERVICE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "control_plane/control_types.h"
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
                              LinkManager& link_manager, size_t region_size);

    // The service owns one stable registered region from initialize() until
    // shutdown(). DeviceArena may sub-allocate it but does not own it.
    [[nodiscard]] void* regionAddr() const noexcept;
    [[nodiscard]] size_t regionSize() const noexcept;

    // Immutable bootstrap metadata for the initialized CUDA device.
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
