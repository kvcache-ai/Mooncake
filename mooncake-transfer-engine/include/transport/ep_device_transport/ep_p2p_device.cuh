// EP P2P device context and operations — unified NVLink (CUDA) / MTLink (MUSA).
//
// EpP2PContext holds the device-visible tables populated by P2pTransport on
// the host side.  The kernel constructs one from the raw pointers passed in
// and calls ep_p2p_* helpers instead of accessing the tables directly.
#pragma once

#include <cstdint>
#include "transport/ep_device_transport/ep_device_ops.cuh"

namespace mooncake {
namespace ep {

struct EpP2PContext {
    const int32_t* available;  // device ptr: [num_ranks], 1 = P2P reachable
    void* const* peer_ptrs;    // device ptr: [num_ranks], peer GDR base ptrs
    void* local_base;          // this rank's GDR buffer base (for offset math)
};

__device__ __forceinline__ bool ep_p2p_available(const EpP2PContext& ctx,
                                                 int dst_rank) {
    return ctx.available[dst_rank] != 0 && ctx.peer_ptrs[dst_rank] != nullptr;
}

// Translate a local pointer (within the GDR buffer) to the peer's mapped VA.
__device__ __forceinline__ void* ep_p2p_peer_ptr(const EpP2PContext& ctx,
                                                 int dst_rank,
                                                 const void* local_ptr) {
    const auto offset = reinterpret_cast<const char*>(local_ptr) -
                        reinterpret_cast<const char*>(ctx.local_base);
    return reinterpret_cast<char*>(ctx.peer_ptrs[dst_rank]) + offset;
}

// Write a 32-bit signal value to the peer's signal slot via P2P store.
// Single-writer assumption: uses release store, not atomic add.
__device__ __forceinline__ void ep_p2p_signal(const EpP2PContext& ctx,
                                              int dst_rank,
                                              const int* local_sig_ptr,
                                              int32_t val) {
    auto* peer_sig =
        reinterpret_cast<int*>(ep_p2p_peer_ptr(ctx, dst_rank, local_sig_ptr));
    ep_st_release(peer_sig, val);
}

}  // namespace ep
}  // namespace mooncake
