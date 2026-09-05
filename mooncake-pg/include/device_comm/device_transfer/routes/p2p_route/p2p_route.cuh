#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_CUH

#include <cstdint>

#include <cooperative_groups.h>
#include <transport/device/device_ops.cuh>

#include "device_comm/device_assert.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"
#include "device_comm/device_transfer/transfer_types.cuh"

namespace mooncake {

class P2pTransferTicket {
   public:
    // p2pPut() and p2pSignal() synchronize the CTA before the leader publishes
    // the peer-visible signal, ordering every payload store before that signal.
    // wait() supplies the separate completion barrier: no thread returns until
    // the leader has issued the signal. Callers that only require submission
    // may drop the ticket and overlap independent work with the leader's store.
    __device__ __forceinline__ TransferResult
    wait(cooperative_groups::thread_block block) const {
        block.sync();
        return TransferResult::Succeeded;
    }
};

__device__ __forceinline__ void applyP2pSignalAction(
    char* remote_region, const SignalAction& signal,
    cooperative_groups::thread_block block) {
    // Publish every calling thread's preceding direct memory accesses before
    // applying the peer-visible action.
    device::mc_fence_barrier_fence();
    if (signal.kind == SignalAction::Kind::None) return;
    PG_DEVICE_ASSERT(signal.kind == SignalAction::Kind::Add ||
                     signal.kind == SignalAction::Kind::Set);

    if (block.thread_rank() == 0) {
        auto* const target =
            reinterpret_cast<uint64_t*>(remote_region + signal.remote_offset);
        uint64_t value;
        if (signal.kind == SignalAction::Kind::Add) {
            value = device::mc_ld_acquire_u64(target) + signal.add.delta;
        } else {
            value = signal.set.value;
        }
        device::mc_st_release_u64(target, value);
    }
}

__device__ __forceinline__ P2pTransferTicket
p2pPut(const DeviceP2pRoute& route, const void* source,
       uint64_t remote_payload_offset, uint64_t size,
       const SignalAction& signal, cooperative_groups::thread_block block) {
    PG_DEVICE_ASSERT(route.mapped_region_address != 0);
    auto* const remote_region = reinterpret_cast<char*>(
        static_cast<uintptr_t>(route.mapped_region_address));
    if (size != 0) {
        copyValuesTo(
            static_cast<const uint8_t*>(source), size, block,
            reinterpret_cast<uint8_t*>(remote_region + remote_payload_offset));
    }
    applyP2pSignalAction(remote_region, signal, block);
    return {};
}

__device__ __forceinline__ P2pTransferTicket
p2pSignal(const DeviceP2pRoute& route, const SignalAction& signal,
          cooperative_groups::thread_block block) {
    PG_DEVICE_ASSERT(route.mapped_region_address != 0);
    auto* const remote_region = reinterpret_cast<char*>(
        static_cast<uintptr_t>(route.mapped_region_address));
    applyP2pSignalAction(remote_region, signal, block);
    return {};
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_ROUTES_P2P_ROUTE_P2P_ROUTE_CUH
