#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_LANE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_LANE_CUH

#include <cstdint>

#include <cooperative_groups.h>
#include <transport/device/device_ops.cuh>

#include "common_types.h"
#include "device_comm/device_assert.cuh"
#include "device_comm/device_transfer/transfer_types.cuh"
#include "device_comm/device_transfer/routes/host_proxy_route/host_proxy_route.cuh"
#include "device_comm/device_transfer/routes/p2p_route/p2p_route.cuh"
#include "device_comm/device_transfer/routes/rdma_route/rdma_route.cuh"

namespace mooncake {

__device__ __forceinline__ bool transferTimedOut(uint64_t start,
                                                 uint64_t timeout_ticks) {
    return timeout_ticks != 0 && clock64() - start >= timeout_ticks;
}

// Rolling comparison keeps "reached" meaningful across uint64_t overflow as
// long as producer and consumer remain less than half the range apart.
__device__ __forceinline__ bool signalReached(uint64_t observed,
                                              uint64_t least) {
    return observed - least < (uint64_t{1} << 63);
}

__device__ __forceinline__ void validateRemoteSignal(
    const SignalAction& signal, uint64_t remote_region_size) {
    PG_DEVICE_ASSERT(signal.kind == SignalAction::Kind::None ||
                     signal.kind == SignalAction::Kind::Add ||
                     signal.kind == SignalAction::Kind::Set);
    if (signal.kind == SignalAction::Kind::None) return;

    PG_DEVICE_ASSERT(signal.remote_offset % alignof(uint64_t) == 0);
    PG_DEVICE_ASSERT(signal.remote_offset <= remote_region_size &&
                     sizeof(uint64_t) <=
                         remote_region_size - signal.remote_offset);
    if (signal.kind == SignalAction::Kind::Add) {
        PG_DEVICE_ASSERT(signal.add.delta != 0 &&
                         signal.add.delta < (uint64_t{1} << 63));
    }
}

class TransferTicket {
   public:
    // Submission already started the operation; dropping this lightweight
    // ticket does not cancel it. wait() only proves local completion, using
    // the route-specific mechanism behind this common API.
    __device__ __forceinline__ TransferResult
    wait(cooperative_groups::thread_block block) const {
        switch (route_) {
            case DeviceRouteKind::P2p:
                return p2p_.wait(block);
            case DeviceRouteKind::Rdma:
                return rdma_.wait(block);
            case DeviceRouteKind::HostProxy:
                return host_proxy_.wait(block);
            case DeviceRouteKind::Unreachable:
                return TransferResult::RouteUnavailable;
        }
        PG_DEVICE_UNREACHABLE();
        return TransferResult::Failed;
    }

   private:
    friend class TransferLane;

    __device__ __forceinline__ TransferTicket() : p2p_{} {}

    __device__ __forceinline__ explicit TransferTicket(P2pTransferTicket ticket)
        : route_(DeviceRouteKind::P2p), p2p_(ticket) {}

    __device__ __forceinline__ explicit TransferTicket(
        RdmaTransferTicket ticket)
        : route_(DeviceRouteKind::Rdma), rdma_(ticket) {}

    __device__ __forceinline__ explicit TransferTicket(
        HostProxyTransferTicket ticket)
        : route_(DeviceRouteKind::HostProxy), host_proxy_(ticket) {}

    DeviceRouteKind route_ = DeviceRouteKind::Unreachable;
    union {
        P2pTransferTicket p2p_;
        RdmaTransferTicket rdma_;
        HostProxyTransferTicket host_proxy_;
    };
};

class TransferLane {
   public:
    __device__ __forceinline__ TransferTicket
    put(GlobalRank rank, const PutRequest& request,
        cooperative_groups::thread_block block) const {
        PG_DEVICE_ASSERT(rank >= 0 && static_cast<uint32_t>(rank) <
                                          service_->max_world_size);
        const auto& route = service_->routes[rank];
        PG_DEVICE_ASSERT(service_->peer_accessible_region.contains(
                             request.local_ptr, request.size) ||
                         service_->local_staging_region.contains(
                             request.local_ptr, request.size));
        if (route.kind != DeviceRouteKind::Unreachable) {
            PG_DEVICE_ASSERT(request.remote_offset <= route.region_size &&
                             request.size <=
                                 route.region_size - request.remote_offset);
            validateRemoteSignal(request.signal, route.region_size);
        }

        switch (route.kind) {
            case DeviceRouteKind::P2p:
                return TransferTicket(
                    p2pPut(route.p2p, request.local_ptr, request.remote_offset,
                           request.size, request.signal, block));

            case DeviceRouteKind::Rdma:
                return TransferTicket(rdmaPut(
                    route.rdma, service_->route_context.rdma, request.local_ptr,
                    request.remote_offset, request.size, request.signal,
                    request.timeout_ticks, lane_index_,
                    service_->lane_results + lane_index_, block));

            case DeviceRouteKind::HostProxy:
                return TransferTicket(hostProxyPut(
                    route.host_proxy, service_->route_context.host_proxy, rank,
                    request.local_ptr, request.remote_offset, request.size,
                    request.signal, request.timeout_ticks, lane_index_,
                    service_->lane_results + lane_index_, block));

            case DeviceRouteKind::Unreachable:
                return TransferTicket();
        }
        PG_DEVICE_UNREACHABLE();
        return TransferTicket();
    }

    __device__ __forceinline__ TransferTicket
    signal(GlobalRank rank, const SignalRequest& request,
           cooperative_groups::thread_block block) const {
        PG_DEVICE_ASSERT(rank >= 0 && static_cast<uint32_t>(rank) <
                                          service_->max_world_size);
        const auto& route = service_->routes[rank];
        if (route.kind != DeviceRouteKind::Unreachable) {
            validateRemoteSignal(request.signal, route.region_size);
        }
        switch (route.kind) {
            case DeviceRouteKind::P2p:
                return TransferTicket(
                    p2pSignal(route.p2p, request.signal, block));

            case DeviceRouteKind::Rdma:
                return TransferTicket(rdmaSignal(
                    route.rdma, service_->route_context.rdma, request.signal,
                    request.timeout_ticks, lane_index_,
                    service_->lane_results + lane_index_, block));

            case DeviceRouteKind::HostProxy:
                return TransferTicket(hostProxySignal(
                    route.host_proxy, service_->route_context.host_proxy, rank,
                    request.signal, request.timeout_ticks, lane_index_,
                    service_->lane_results + lane_index_, block));

            case DeviceRouteKind::Unreachable:
                return TransferTicket();
        }
        PG_DEVICE_UNREACHABLE();
        return TransferTicket();
    }

    __device__ __forceinline__ SignalWaitResult
    waitSignal(const SignalWaitRequest& request,
               cooperative_groups::thread_block block) const {
        __shared__ uint64_t block_wait_result;
        if (block.thread_rank() == 0) {
            const uint64_t start_ticks = clock64();
            uint64_t observed = 0;
            while (true) {
                observed = device::mc_ld_volatile_u64(request.local_ptr);
                if (signalReached(observed, request.least)) {
                    // Poll cheaply, then acquire once before the CTA consumes
                    // the payload published by the matching release signal.
                    observed = device::mc_ld_acquire_u64(request.local_ptr);
                    if (signalReached(observed, request.least)) {
                        break;
                    }
                }
                if (transferTimedOut(start_ticks, request.timeout_ticks)) {
                    break;
                }
            }
            block_wait_result = observed;
        }
        block.sync();
        const uint64_t observed = block_wait_result;
        block.sync();
        return SignalWaitResult{
            .status = signalReached(observed, request.least)
                          ? SignalWaitStatus::Reached
                          : SignalWaitStatus::TimedOut,
            .observed = observed,
        };
    }

   private:
    friend struct DeviceTransferHandle;

    __device__ __forceinline__ TransferLane(const DeviceTransferHandle* service,
                                            uint32_t lane_index)
        : service_(service), lane_index_(lane_index) {
        PG_DEVICE_ASSERT(service_ && lane_index_ < kTransferLaneCount);
    }

    const DeviceTransferHandle* service_ = nullptr;
    uint32_t lane_index_ = 0;
};

__device__ __forceinline__ TransferLane
DeviceTransferHandle::lane(uint32_t lane_index) const {
    return TransferLane(this, lane_index);
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_LANE_CUH
