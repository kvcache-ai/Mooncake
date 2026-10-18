#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include <transport/device/device_transport.h>

#include "device_comm/device_transfer/routes/route_provider.h"
#include "gpu_runtime.h"

namespace mooncake {

struct P2pRouteOptions {
    bool enabled = true;
};

class P2pRoute : public RouteProvider {
   public:
    static constexpr std::string_view kRouteKey = "p2p";
    static constexpr uint32_t kEndpointVersion = 1;

    [[nodiscard]] static PGResult<std::unique_ptr<P2pRoute>> create(
        int device_index, GlobalRank self_rank, uint32_t max_world_size);

    PGResult<void> registerRegion(DeviceRegionKind kind, void* addr,
                                  size_t size) override;
    PGResult<void> unregisterRegion(DeviceRegionKind kind, void* addr,
                                    size_t size) override;
    [[nodiscard]] std::optional<RouteEndpoint> localEndpoint() override;
    [[nodiscard]] PGResult<std::vector<DeviceTransferRoute>> installEndpoints(
        const DeviceTransferSnapshot& snapshot,
        uint64_t reclaim_before_version) override;
    PGResult<void> updateRoutes() override;
    PGResult<void> shutdown() override;

   private:
    P2pRoute(int device_index, GlobalRank self_rank, uint32_t max_world_size);

    struct PeerMapping {
        std::vector<int32_t> handle;
        std::shared_ptr<device::P2pMapping> mapping;
    };

    struct State {
        uint64_t installation_version = 0;
        std::vector<PeerMapping> peers;
    };

    void* local_ptr_ = nullptr;
    size_t local_size_ = 0;
    std::vector<int32_t> local_handle_;
    GpuUuid device_uuid_{};
    std::unique_ptr<State> current_;
    std::unique_ptr<State> standby_;
    int device_index_ = -1;
    GlobalRank self_rank_ = kInvalidGlobalRank;
    uint32_t max_world_size_ = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_H
