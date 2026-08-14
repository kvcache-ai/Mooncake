#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_CUH

#include <cstdint>

#include <cooperative_groups.h>
#include <transport/device/device_ops.cuh>

#include "device_comm/device_transfer/transfer_types.cuh"
#include "device_comm/device_transfer/routes/host_proxy_route/host_proxy_types.cuh"

namespace mooncake {

class HostProxyTransferTicket {
   public:
    enum class State : uint32_t {
        Submitted,
        TimedOut,
    };

    __device__ __forceinline__ HostProxyTransferTicket() = default;

    __device__ __forceinline__ explicit HostProxyTransferTicket(
        uint64_t* wait_result)
        : wait_result_(wait_result) {}

    __device__ __forceinline__ HostProxyTransferTicket(
        State state, HostProxyCommandSlot* slot, uint64_t expected_sequence,
        uint64_t start_ticks, uint64_t timeout_ticks, uint64_t* wait_result)
        : state_(state),
          slot_(slot),
          expected_sequence_(expected_sequence),
          start_ticks_(start_ticks),
          timeout_ticks_(timeout_ticks),
          wait_result_(wait_result) {}

    // Every thread in the lane CTA must enter wait() together.
    __device__ __forceinline__ TransferResult
    wait(cooperative_groups::thread_block block) const {
        if (!wait_result_) {
            __trap();
            return TransferResult::Failed;
        }

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
    __device__ __forceinline__ TransferResult waitLeader() const;

    State state_ = State::TimedOut;
    HostProxyCommandSlot* slot_ = nullptr;
    uint64_t expected_sequence_ = 0;
    uint64_t start_ticks_ = 0;
    uint64_t timeout_ticks_ = 0;
    uint64_t* wait_result_ = nullptr;
};

__device__ __forceinline__ bool hostProxyTimedOut(uint64_t start,
                                                  uint64_t timeout_ticks) {
    return timeout_ticks != 0 && clock64() - start >= timeout_ticks;
}

__device__ __forceinline__ void publishHostProxyCommand(
    HostProxyCommandSlot& slot, const HostProxyCommand& command,
    uint64_t sequence) {
    slot.command = command;
    slot.result = HostProxyCommandResult::Pending;
    __threadfence_system();
    device::mc_st_release_u64(&slot.submitted_sequence, sequence);
}

__device__ __forceinline__ HostProxyTransferTicket hostProxyPutAndSignal(
    HostProxyCommandSlot* command_slots, uint64_t remote_region_address,
    GlobalRank target_rank, const void* source, uint64_t remote_payload_offset,
    uint64_t size, uint64_t remote_signal_offset, uint64_t signal_delta,
    uint64_t timeout_ticks, uint32_t lane, uint64_t* wait_result,
    cooperative_groups::thread_block block) {
    HostProxyTransferTicket ticket(wait_result);
    // Submission is leader-only. Other threads only carry wait_result; wait()
    // reads the leader's private ticket state and broadcasts its result.
    if (block.thread_rank() != 0) return ticket;

    if (!command_slots || lane >= kTransferLaneCount ||
        remote_region_address == 0 || !wait_result) {
        __trap();
        return ticket;
    }
    auto* const slot = command_slots + lane;
    const uint64_t start_ticks = clock64();

    const uint64_t submitted =
        device::mc_ld_acquire_u64(&slot->submitted_sequence);
    while (true) {
        const uint64_t completed =
            device::mc_ld_acquire_u64(&slot->completed_sequence);
        if (completed == submitted) break;
        if (completed > submitted) {
            __trap();
            return ticket;
        }
        if (hostProxyTimedOut(start_ticks, timeout_ticks)) return ticket;
    }
    if (submitted == UINT64_MAX) {
        __trap();
        return ticket;
    }

    const uint64_t sequence = submitted + 1;
    HostProxyCommand command;
    command.local_addr = reinterpret_cast<uint64_t>(source);
    command.remote_addr = remote_region_address + remote_payload_offset;
    command.size = size;
    command.signal_remote_addr = remote_region_address + remote_signal_offset;
    command.signal_delta = signal_delta;
    command.target_rank = target_rank;
    publishHostProxyCommand(*slot, command, sequence);
    return HostProxyTransferTicket(HostProxyTransferTicket::State::Submitted,
                                   slot, sequence, start_ticks, timeout_ticks,
                                   wait_result);
}

__device__ __forceinline__ HostProxyTransferTicket hostProxySignal(
    HostProxyCommandSlot* command_slots, uint64_t remote_region_address,
    GlobalRank target_rank, uint64_t remote_signal_offset,
    uint64_t signal_delta, uint64_t timeout_ticks, uint32_t lane,
    uint64_t* wait_result, cooperative_groups::thread_block block) {
    return hostProxyPutAndSignal(
        command_slots, remote_region_address, target_rank,
        /*source=*/nullptr, /*remote_payload_offset=*/0, /*size=*/0,
        remote_signal_offset, signal_delta, timeout_ticks, lane, wait_result,
        block);
}

__device__ __forceinline__ TransferResult
HostProxyTransferTicket::waitLeader() const {
    if (state_ == State::TimedOut) {
        return TransferResult::TimedOut;
    }
    if (!slot_ || expected_sequence_ == 0) {
        __trap();
        return TransferResult::Failed;
    }
    while (true) {
        const uint64_t completed =
            device::mc_ld_acquire_u64(&slot_->completed_sequence);
        if (completed == expected_sequence_) break;
        if (completed > expected_sequence_) {
            __trap();
            return TransferResult::Failed;
        }
        if (hostProxyTimedOut(start_ticks_, timeout_ticks_)) {
            return TransferResult::TimedOut;
        }
    }
    switch (slot_->result) {
        case HostProxyCommandResult::Succeeded:
            return TransferResult::Succeeded;
        case HostProxyCommandResult::Failed:
            return TransferResult::Failed;
        case HostProxyCommandResult::Pending:
            __trap();
            return TransferResult::Failed;
    }
    __trap();
    return TransferResult::Failed;
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_HOST_PROXY_ROUTE_HOST_PROXY_ROUTE_CUH
