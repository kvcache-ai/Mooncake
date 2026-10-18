#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_RDMA_ROUTE_RDMA_ROUTE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_RDMA_ROUTE_RDMA_ROUTE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "device_comm/device_transfer/routes/route_provider.h"

namespace mooncake {

struct RdmaRouteOptions {
    bool enabled = false;

    // Optional RDMA device names, such as mlx5_0. An empty filter delegates
    // device selection to the RDMA implementation.
    std::vector<std::string> device_filter;
};

class RdmaRoute : public RouteProvider {
   public:
    static constexpr std::string_view kRouteKey = "rdma";
    static constexpr uint32_t kEndpointVersion = 1;

    [[nodiscard]] static PGResult<std::unique_ptr<RdmaRoute>> create(
        int device_index, cudaStream_t stream, GlobalRank self_rank,
        uint32_t max_world_size, const RdmaRouteOptions& options);
    ~RdmaRoute() noexcept override;

    PGResult<void> registerRegion(DeviceRegionKind kind, void* addr,
                                  size_t size) override;
    PGResult<void> unregisterRegion(DeviceRegionKind kind, void* addr,
                                    size_t size) override;
    [[nodiscard]] std::optional<RouteEndpoint> localEndpoint() override;
    [[nodiscard]] PGResult<std::vector<DeviceTransferRoute>> installEndpoints(
        const DeviceTransferSnapshot& snapshot,
        uint64_t reclaim_before_version) override;
    PGResult<void> updateRoutes() override;
    void fillDeviceContext(DeviceRouteContext& context) const noexcept override;

    PGResult<void> shutdown() override;

   private:
    struct State;

    RdmaRoute(GlobalRank self_rank, uint32_t max_world_size,
              std::unique_ptr<State> state);

    GlobalRank self_rank_ = kInvalidGlobalRank;
    uint32_t max_world_size_ = 0;
    std::unique_ptr<State> state_;
    bool shutdown_requested_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_RDMA_ROUTE_RDMA_ROUTE_H
