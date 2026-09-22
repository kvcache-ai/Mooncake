#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PRIMITIVES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PRIMITIVES_CUH

#include <cstdio>

#include <cuda/atomic>

#include "device_comm/device_collective/protocols/ll/ll_types.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"
#include "device_comm/device_utils/device_assert.cuh"
#include "device_comm/device_utils/device_timeout.cuh"

namespace mooncake {

// CTA-collective control operations. The algorithm chooses the peers and
// initializes its payload slots before publishing BufferReady or WrapReady.
class LLSignalPrimitives {
   public:
    __device__ __forceinline__ LLSignalPrimitives(
        const TransferLane& lane, const LLState& state, uint64_t timeout_ticks)
        : lane_(lane), state_(state), timeout_ticks_(timeout_ticks) {}

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult preparePeer(
        InGroupRank rank, uint64_t buffer_offset, uint64_t buffer_bytes,
        uint64_t view_epoch,
        cooperative_groups::thread_block block) const {
        PG_DEVICE_ASSERT(rank >= 0 &&
                         static_cast<uint32_t>(rank) <
                             state_.signal_layout.max_group_size);
        const auto& peer = state_.peers[rank];
        const auto* handle = state_.transfer_handle;
        PG_DEVICE_ASSERT(peer.global_rank != kInvalidGlobalRank);
        if (handle->routeType(peer.global_rank) == DeviceRouteType::Unreachable)
            return {rank};
        if (!handle->supportsNativeAtomics(peer.global_rank)) {
            if (block.thread_rank() == 0) {
                printf("LL requires native peer atomics: rank %d, peer %d, "
                       "view %llu\n",
                       state_.self_rank, rank,
                       static_cast<unsigned long long>(view_epoch));
            }
            PG_DEVICE_UNREACHABLE();
        }
        const auto region_size = handle->routes[peer.global_rank].region_size;
        PG_DEVICE_ASSERT(buffer_offset <= region_size &&
                         buffer_bytes <= region_size - buffer_offset);
        PG_DEVICE_ASSERT(peer.signal_offset <= region_size &&
                         state_.signal_layout.signalBytes() <=
                             region_size - peer.signal_offset);
        return {};
    }

    __device__ __forceinline__ void resetWrap(
        cooperative_groups::thread_block block) const {
        auto* wrap_signals = reinterpret_cast<uint64_t*>(
            reinterpret_cast<char*>(state_.signals) +
            state_.signal_layout.controlOffset(LLControlWord::WrapReady, 0));
        for (uint32_t rank = block.thread_rank();
             rank < state_.signal_layout.max_group_size;
             rank += block.size())
            wrap_signals[rank] = 0;
        // The next signal() publishes these stores from every calling thread.
    }

    [[nodiscard]] __device__ __forceinline__ TransferResult signal(
        InGroupRank rank, LLControlWord kind, uint64_t sequence,
        cooperative_groups::thread_block block) const {
        const auto& peer = state_.peers[rank];
        SignalRequest request;
        request.signal.kind = SignalAction::Kind::Set;
        request.signal.remote_offset =
            peer.signal_offset +
            state_.signal_layout.controlOffset(kind, state_.self_rank);
        request.signal.set.value = sequence;
        request.timeout_ticks = timeout_ticks_;
        return lane_.signal(peer.global_rank, request, block).wait(block);
    }

    [[nodiscard]] __device__ __forceinline__ bool wait(
        InGroupRank rank, LLControlWord kind, uint64_t sequence,
        cooperative_groups::thread_block block) const {
        const auto* signal = reinterpret_cast<const uint64_t*>(
            reinterpret_cast<const char*>(state_.signals) +
            state_.signal_layout.controlOffset(kind, rank));
        const auto result = lane_.waitSignal(
            SignalWaitRequest{signal, sequence, timeout_ticks_}, block);
        return result.status == SignalWaitStatus::Reached &&
               result.observed == sequence;
    }

   private:
    TransferLane lane_;
    const LLState& state_;
    uint64_t timeout_ticks_;
};

// Per-thread LL packet operations. One system-scope atomic scalar carries a
// 32-bit payload and generation tag, so a consumer can poll each word directly.
// The caller must establish native peer atomic support, own the receive buffer
// lifetime, and clear old tags before the 32-bit generation repeats.
class LLPrimitives {
   public:
    __device__ __forceinline__ LLPrimitives(const LLState& state,
                                            uint64_t sequence,
                                            uint64_t timeout_ticks)
        : state_(state),
          tag_(static_cast<uint32_t>(sequence)),
          timeout_ticks_(timeout_ticks) {}

    // The algorithm supplies a byte offset in the peer's DTS region.
    [[nodiscard]] __device__ __forceinline__ uint64_t* remotePayload(
        InGroupRank rank, uint64_t region_offset) const {
        const auto& peer = state_.peers[rank];
        return static_cast<uint64_t*>(state_.transfer_handle->remotePtr(
            peer.global_rank, region_offset));
    }

    __device__ __forceinline__ void store(uint64_t* destination,
                                          uint32_t value) const {
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> packet(
            *destination);
        // The data and tag are one scalar; no other memory is published here.
        packet.store((uint64_t{tag_} << 32) | value,
                     cuda::memory_order_relaxed);
    }

    [[nodiscard]] __device__ __forceinline__ bool load(const uint64_t* source,
                                                       uint32_t& value) const {
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> packet(
            *const_cast<uint64_t*>(source));
        uint64_t observed = packet.load(cuda::memory_order_relaxed);
        if (static_cast<uint32_t>(observed >> 32) == tag_) {
            value = static_cast<uint32_t>(observed);
            return true;
        }
        // The ready case needs no clock access.
        const uint64_t start = clock64();
        do {
            observed = packet.load(cuda::memory_order_relaxed);
            if (static_cast<uint32_t>(observed >> 32) == tag_) {
                value = static_cast<uint32_t>(observed);
                return true;
            }
        } while (!deviceTimedOut(start, timeout_ticks_));
        return false;
    }

   private:
    const LLState& state_;
    uint32_t tag_;
    uint64_t timeout_ticks_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PRIMITIVES_CUH
