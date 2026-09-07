#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_RDMA_ROUTE_RDMA_ROUTE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_RDMA_ROUTE_RDMA_ROUTE_CUH

#include <cstdint>

#include <cooperative_groups.h>
#include <transport/device/device_ops.cuh>
#include <transport/device/ibgda_device.cuh>

#include "device_comm/device_assert.cuh"
#include "device_comm/device_transfer/transfer_types.cuh"

namespace mooncake {

__device__ __forceinline__ TransferResult
toTransferResult(device::IbgdaPollResult result) {
    switch (result) {
        case device::IbgdaPollResult::Completed:
            return TransferResult::Succeeded;
        case device::IbgdaPollResult::TimedOut:
            return TransferResult::TimedOut;
        case device::IbgdaPollResult::Failed:
            return TransferResult::Failed;
    }
    return TransferResult::Failed;
}

class RdmaTransferTicket {
   public:
    __device__ __forceinline__ RdmaTransferTicket() = default;

    __device__ __forceinline__ explicit RdmaTransferTicket(
        uint64_t* wait_result)
        : wait_result_(wait_result) {}

    __device__ __forceinline__
    RdmaTransferTicket(device::IbgdaPollResult result, uint64_t* wait_result)
        : wait_result_(wait_result), result_(result) {}

    __device__ __forceinline__ RdmaTransferTicket(mlx5gda_qp_devctx* qp,
                                                  uint16_t expected_wqe,
                                                  uint64_t deadline_ticks,
                                                  uint64_t* wait_result)
        : submitted_(true),
          qp_(qp),
          expected_wqe_(expected_wqe),
          deadline_ticks_(deadline_ticks),
          wait_result_(wait_result) {}

    __device__ __forceinline__ TransferResult
    wait(cooperative_groups::thread_block block) const {
        if (block.thread_rank() == 0) {
            device::mc_st_release_u64(wait_result_,
                                      static_cast<uint64_t>(waitLeader()));
        }
        block.sync();
        const auto result = static_cast<TransferResult>(
            device::mc_ld_acquire_u64(wait_result_));
        block.sync();
        return result;
    }

   private:
    __device__ __forceinline__ TransferResult waitLeader() const {
        auto result = result_;
        if (submitted_) {
            // Serialize qp completion state with submissions and other waiters.
            device::mc_ibgda_lock(qp_);
            result =
                device::mc_ibgda_poll_cq(qp_, expected_wqe_, deadline_ticks_);
            device::mc_ibgda_unlock(qp_);
        }
        return toTransferResult(result);
    }

    bool submitted_ = false;
    mlx5gda_qp_devctx* qp_ = nullptr;
    uint16_t expected_wqe_ = 0;
    uint64_t deadline_ticks_ = 0;
    uint64_t* wait_result_ = nullptr;
    device::IbgdaPollResult result_ = device::IbgdaPollResult::Completed;
};

__device__ __forceinline__ mlx5gda_qp_devctx* rdmaQueuePair(
    const DeviceRdmaRoute& route, const DeviceRdmaContext& context,
    uint32_t lane) {
    auto* qps = static_cast<mlx5gda_qp_devctx*>(context.qp_devctxs);
    return qps + route.qp_offset + lane % context.qps_per_rank;
}

__device__ __forceinline__ uint32_t rdmaLocalKey(
    const DeviceRdmaContext& context, const void* source, uint64_t size) {
    if (context.peer_accessible_region.contains(source, size)) {
        return context.peer_accessible_lkey;
    }
    PG_DEVICE_ASSERT(context.local_staging_region.contains(source, size));
    return context.local_staging_lkey;
}

__device__ __forceinline__ void appendRdmaSignal(
    const DeviceRdmaRoute& route, const DeviceRdmaContext& context,
    mlx5gda_qp_devctx* qp, const SignalAction& signal) {
    if (signal.kind == SignalAction::Kind::None) return;

    const uint64_t remote_address =
        route.remote_region_address + signal.remote_offset;
    switch (signal.kind) {
        case SignalAction::Kind::Add: {
            device::mc_ibgda_write_rdma_atomic_add_64_wqe(
                qp, signal.add.delta,
                reinterpret_cast<uint64_t>(context.atomic_sink),
                device::mc_bswap32(context.atomic_sink_lkey), remote_address,
                device::mc_bswap32(route.remote_key));
            return;
        }
        case SignalAction::Kind::Set:
            device::mc_ibgda_write_rdma_write_inline_u64_wqe(
                qp, signal.set.value, remote_address,
                device::mc_bswap32(route.remote_key));
            return;
        case SignalAction::Kind::None:
            return;
    }
    PG_DEVICE_UNREACHABLE();
}

static __device__ __noinline__ RdmaTransferTicket
rdmaPut(const DeviceRdmaRoute& route, const DeviceRdmaContext& context,
        const void* source, uint64_t remote_payload_offset, uint64_t size,
        const SignalAction& signal, uint64_t timeout_ticks, uint32_t lane,
        uint64_t* wait_result, cooperative_groups::thread_block block) {
    RdmaTransferTicket ticket(wait_result);

    // Every producer makes its source writes visible before the leader exposes
    // the source address to the NIC.
    __threadfence_system();
    block.sync();
    if (block.thread_rank() != 0) return ticket;

    if (size == 0 && signal.kind == SignalAction::Kind::None) return ticket;

    auto* qp = rdmaQueuePair(route, context, lane);
    const uint64_t deadline_ticks =
        timeout_ticks == 0 ? 0 : clock64() + timeout_ticks;
    device::mc_ibgda_lock(qp);
    // mlx5gda creates these QPs with log_msg_max=30.
    constexpr uint32_t kMaxWriteBytes = uint32_t{1} << 30;
    const uint64_t write_count =
        size / kMaxWriteBytes + (size % kMaxWriteBytes != 0 ? 1 : 0);
    const uint64_t batch_size =
        write_count + (signal.kind != SignalAction::Kind::None ? 1 : 0);
    const auto space_result = device::mc_ibgda_wait_for_sq_space(
        qp, static_cast<uint32_t>(batch_size), deadline_ticks);
    if (space_result != device::IbgdaPollResult::Completed) {
        device::mc_ibgda_unlock(qp);
        return RdmaTransferTicket(space_result, wait_result);
    }

    if (size != 0) {
        const uint32_t local_key = rdmaLocalKey(context, source, size);
        uint64_t remaining = size;
        uint64_t local_address = reinterpret_cast<uint64_t>(source);
        uint64_t remote_address =
            route.remote_region_address + remote_payload_offset;
        while (remaining != 0) {
            const uint32_t chunk = remaining > kMaxWriteBytes
                                       ? kMaxWriteBytes
                                       : static_cast<uint32_t>(remaining);
            device::mc_ibgda_write_rdma_write_wqe(
                qp, local_address, device::mc_bswap32(local_key),
                remote_address, device::mc_bswap32(route.remote_key), chunk);
            local_address += chunk;
            remote_address += chunk;
            remaining -= chunk;
        }
    }
    appendRdmaSignal(route, context, qp, signal);
    const uint16_t expected_wqe = device::mc_ibgda_submit(qp);
    return RdmaTransferTicket(qp, expected_wqe, deadline_ticks, wait_result);
}

__device__ __forceinline__ RdmaTransferTicket
rdmaSignal(const DeviceRdmaRoute& route, const DeviceRdmaContext& context,
           const SignalAction& signal, uint64_t timeout_ticks, uint32_t lane,
           uint64_t* wait_result, cooperative_groups::thread_block block) {
    return rdmaPut(route, context, nullptr, 0, 0, signal, timeout_ticks, lane,
                   wait_result, block);
}

__device__ __forceinline__ void drainRdmaTransfers(
    const DeviceTransferHandle& handle, const GlobalRank* peers,
    uint32_t peer_count) {
    const auto& context = handle.route_context.rdma;
    const uint32_t num_qps = peer_count * context.qps_per_rank;
    for (uint32_t index = 0; index < num_qps; ++index) {
        const auto& route = handle.routes[peers[index / context.qps_per_rank]];
        if (route.kind != DeviceRouteKind::Rdma) continue;

        const uint32_t lane = index % context.qps_per_rank;
        auto* qp = rdmaQueuePair(route.rdma, context, lane);
        device::mc_ibgda_lock(qp);
        auto result = device::IbgdaPollResult::Completed;
        const uint16_t head = qp->wq_head;
        if (head != qp->wq_tail) {
            result = device::mc_ibgda_poll_cq(
                qp, static_cast<uint16_t>(head - 1),
                clock64() + handle.drain_timeout_ticks);
        }
        device::mc_ibgda_unlock(qp);

        // Drain is best-effort, including when the failed peer cannot complete
        // outstanding work. Do not prevent recovery or kernel exit.
        if (result != device::IbgdaPollResult::Completed) {
            printf("[PG] Device RDMA QP %u drain %s; continuing\n",
                   route.rdma.qp_offset + lane,
                   result == device::IbgdaPollResult::TimedOut ? "timed out"
                                                               : "failed");
        }
    }
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_RDMA_ROUTE_RDMA_ROUTE_CUH
