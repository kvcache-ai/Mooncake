#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_CUH

#include <cstdint>

#include <cooperative_groups.h>
#include <transport/device/device_ops.cuh>

#include "device_comm/device_utils/d2h_request_slot.cuh"
#include "device_comm/device_utils/device_assert.cuh"
#include "device_comm/device_transfer/transfer_types.cuh"
#include "device_comm/device_transfer/routes/host_proxy_route/host_proxy_types.cuh"

namespace mooncake {

class HostProxyTransferTicket {
   public:
    __device__ __forceinline__ HostProxyTransferTicket() = default;

    __device__ __forceinline__
    HostProxyTransferTicket(HostProxyCommandSlot::RequestHandle handle,
                            uint64_t start_ticks, uint64_t timeout_ticks)
        : handle_(handle),
          start_ticks_(start_ticks),
          timeout_ticks_(timeout_ticks) {}

    // Every thread in the lane CTA must enter wait() together.
    __device__ __forceinline__ TransferResult
    wait(cooperative_groups::thread_block block) const {
        __shared__ TransferResult block_wait_result;
        if (block.thread_rank() == 0) {
            block_wait_result = waitLeader();
        }
        block.sync();
        const auto result = block_wait_result;
        block.sync();
        return result;
    }

   private:
    __device__ __forceinline__ TransferResult waitLeader() const;

    HostProxyCommandSlot::RequestHandle handle_;
    uint64_t start_ticks_ = 0;
    uint64_t timeout_ticks_ = 0;
};

static __device__ __noinline__ HostProxyTransferTicket hostProxyPut(
    const DeviceHostProxyRoute& route, const DeviceHostProxyContext& context,
    GlobalRank target_rank, const void* source, uint64_t remote_payload_offset,
    uint64_t size, const SignalAction& signal, uint64_t timeout_ticks,
    uint32_t lane, cooperative_groups::thread_block block) {
    // Every producer publishes its source writes to system scope before the
    // leader hands the device address to the host worker.
    __threadfence_system();
    block.sync();

    // Only the leader submits; wait() broadcasts the result from its private
    // ticket state to the CTA.
    if (block.thread_rank() != 0) return {};

    PG_DEVICE_ASSERT(context.command_slots && lane < kTransferLaneCount &&
                     route.remote_region_address != 0);
    auto* const slot = context.command_slots + lane;
    const uint64_t start_ticks = clock64();

    HostProxyCommand command;
    command.local_addr = reinterpret_cast<uint64_t>(source);
    command.remote_region_addr = route.remote_region_address;
    command.remote_offset = remote_payload_offset;
    command.size = size;
    command.signal = signal;
    command.target_rank = target_rank;
    const auto handle = slot->submit(command, start_ticks, timeout_ticks);
    return HostProxyTransferTicket(handle, start_ticks, timeout_ticks);
}

__device__ __forceinline__ HostProxyTransferTicket hostProxySignal(
    const DeviceHostProxyRoute& route, const DeviceHostProxyContext& context,
    GlobalRank target_rank, const SignalAction& signal, uint64_t timeout_ticks,
    uint32_t lane, cooperative_groups::thread_block block) {
    return hostProxyPut(route, context, target_rank,
                        /*source=*/nullptr, /*remote_payload_offset=*/0,
                        /*size=*/0, signal, timeout_ticks, lane, block);
}

__device__ __forceinline__ TransferResult
HostProxyTransferTicket::waitLeader() const {
    if (!handle_) {
        return TransferResult::TimedOut;
    }
    HostProxyCommandResult result;
    if (!handle_.wait(start_ticks_, timeout_ticks_, &result)) {
        return TransferResult::TimedOut;
    }
    switch (result) {
        case HostProxyCommandResult::Succeeded:
            return TransferResult::Succeeded;
        case HostProxyCommandResult::Failed:
            return TransferResult::Failed;
        case HostProxyCommandResult::Pending:
            PG_DEVICE_UNREACHABLE();
            return TransferResult::Failed;
    }
    PG_DEVICE_UNREACHABLE();
    return TransferResult::Failed;
}

__device__ __forceinline__ void drainHostProxyTransfers(
    const DeviceHostProxyContext& context, uint64_t timeout_ticks) {
    if (!context.command_slots) return;

    // Producers have stopped. The worker publishes completion after finishing
    // each command; one timeout bounds the wait across all lanes.
    const uint64_t start_ticks = clock64();
    for (uint32_t lane = 0; lane < kTransferLaneCount; ++lane) {
        const auto& slot = context.command_slots[lane];
        if (!slot.waitUntilIdle(start_ticks, timeout_ticks)) {
            printf("[PG] Host-proxy drain timed out at lane %u; continuing\n",
                   lane);
            return;
        }
    }
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_CUH
