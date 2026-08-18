#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_CUH

#include <cstdint>

#include <cooperative_groups.h>
#include <transport/device/device_ops.cuh>

#include "device_comm/device_assert.cuh"
#include "device_comm/device_transfer/transfer_types.cuh"

namespace mooncake {

class P2pTransferTicket {
   public:
    // P2P stores complete before putAndSignal() returns. Keep a ticket-shaped
    // result so callers use the same submit/wait API as asynchronous routes.
    __device__ __forceinline__ TransferResult
    wait(cooperative_groups::thread_block) const {
        return TransferResult::Succeeded;
    }
};

__device__ __forceinline__ void addToP2pSignal(
    char* remote_region, uint64_t remote_signal_offset, uint64_t signal_delta,
    cooperative_groups::thread_block block) {
    if (block.thread_rank() == 0) {
        auto* const signal =
            reinterpret_cast<uint64_t*>(remote_region + remote_signal_offset);
        const uint64_t current = device::mc_ld_acquire_u64(signal);
        device::mc_st_release_u64(signal, current + signal_delta);
    }
    block.sync();
}

__device__ __forceinline__ void copyP2pPayload(
    void* destination, const void* source, uint64_t size,
    cooperative_groups::thread_block block) {
    auto* destination_bytes = static_cast<uint8_t*>(destination);
    const auto* source_bytes = static_cast<const uint8_t*>(source);
    const auto lane = static_cast<uint64_t>(block.thread_rank());
    const auto width = static_cast<uint64_t>(block.size());

    const auto combined = reinterpret_cast<uintptr_t>(destination) |
                          reinterpret_cast<uintptr_t>(source) | size;
    if ((combined & (alignof(int4) - 1)) == 0) {
        auto* destination_vectors = reinterpret_cast<int4*>(destination);
        const auto* source_vectors = reinterpret_cast<const int4*>(source);
        const uint64_t count = size / sizeof(int4);
        for (uint64_t index = lane; index < count; index += width) {
            destination_vectors[index] = source_vectors[index];
        }
    } else {
        for (uint64_t index = lane; index < size; index += width) {
            destination_bytes[index] = source_bytes[index];
        }
    }

    // Publish the payload before the leader updates the remote notification
    // counter. The CTA barrier keeps the leader behind every copying thread.
    __threadfence_system();
    block.sync();
}

__device__ __forceinline__ P2pTransferTicket
p2pPutAndSignal(uint64_t mapped_region_address, const void* source,
                uint64_t remote_payload_offset, uint64_t size,
                uint64_t remote_signal_offset, uint64_t signal_delta,
                cooperative_groups::thread_block block) {
    PG_DEVICE_ASSERT(mapped_region_address != 0);
    auto* const remote_region =
        reinterpret_cast<char*>(static_cast<uintptr_t>(mapped_region_address));
    if (size != 0) {
        copyP2pPayload(remote_region + remote_payload_offset, source, size,
                       block);
    }
    addToP2pSignal(remote_region, remote_signal_offset, signal_delta, block);
    return {};
}

__device__ __forceinline__ P2pTransferTicket
p2pSignal(uint64_t mapped_region_address, uint64_t remote_signal_offset,
          uint64_t signal_delta, cooperative_groups::thread_block block) {
    PG_DEVICE_ASSERT(mapped_region_address != 0);
    auto* const remote_region =
        reinterpret_cast<char*>(static_cast<uintptr_t>(mapped_region_address));
    addToP2pSignal(remote_region, remote_signal_offset, signal_delta, block);
    return {};
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_CUH
