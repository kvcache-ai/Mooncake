#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PRIMITIVES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PRIMITIVES_CUH

#include <cuda/atomic>

#include "device_comm/device_collective/protocols/ll/ll_types.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"
#include "pg_assert.h"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {

// CTA-collective LL control for one invocation.
class LLControl {
   public:
    __device__ __forceinline__ LLControl(LLState& state,
                                         const DeviceGroupView& view,
                                         uint32_t channel,
                                         uint64_t timeout_ticks)
        : state_(state),
          view_(view),
          progress_(state.progress[channel]),
          channel_(channel),
          timeout_ticks_(timeout_ticks) {
        PG_ASSERT(channel < kMaxDeviceCollectiveChannels);
    }

    // Packet storage is borrowed from the algorithm for tag recycling.
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    begin(LLPacket* packets, uint64_t packet_bytes,
          cooperative_groups::thread_block block) const {
        if (progress_.failed_rank != kInvalidInGroupRank)
            return {progress_.failed_rank};
        if (LLPacket::tagFor(progress_.next_sequence) == 0)
            return recycleTags(packets, packet_bytes, block);
        return {};
    }

    // Advance after all receives finish; failure requires a new View.
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    finish(uint64_t sequence, InGroupRank failed,
           cooperative_groups::thread_block block) const {
        if (__syncthreads_or(failed != kInvalidInGroupRank)) {
            if (failed != kInvalidInGroupRank)
                atomicCAS(&progress_.failed_rank, kInvalidInGroupRank, failed);
        } else if (block.thread_rank() == 0) {
            // 64-bit rollover takes ~580,000 years at one step per microsecond.
            PG_ASSERT(sequence != UINT64_MAX);
            progress_.next_sequence = sequence + 1;
        }
        block.sync();
        return {progress_.failed_rank};
    }

   private:
    [[nodiscard]] __device__ __forceinline__ uint64_t
    signalIndex(LLBarrierType type, InGroupRank sender) const {
        return (uint64_t{channel_} * kLLBarrierCount +
                static_cast<uint32_t>(type)) *
                   state_.max_group_size +
               static_cast<uint32_t>(sender);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    barrier(LLBarrierType type, uint64_t value,
            cooperative_groups::thread_block block) const {
        const auto lane = state_.transfer_handle->lane(channel_);
        for (uint32_t index = 0; index < view_.participant_count; ++index) {
            if (index == static_cast<uint32_t>(view_.self_active_index))
                continue;
            const auto& peer = view_.participants[index];
            const auto rank = peer.in_group_rank;
            PG_ASSERT(rank >= 0 &&
                      static_cast<uint32_t>(rank) < state_.max_group_size);
            SignalRequest request;
            request.signal.kind = SignalAction::Kind::Set;
            request.signal.remote_offset =
                state_.tag_recycle_signal_offsets[index] +
                signalIndex(type, view_.self().in_group_rank) *
                    sizeof(uint64_t);
            request.signal.set.value = value;
            request.timeout_ticks = timeout_ticks_;
            if (lane.signal(peer.global_rank, request, block).wait(block) !=
                TransferResult::Succeeded)
                return {rank};
        }
        for (uint32_t index = 0; index < view_.participant_count; ++index) {
            if (index == static_cast<uint32_t>(view_.self_active_index))
                continue;
            const auto& peer = view_.participants[index];
            const auto rank = peer.in_group_rank;
            const auto* signal =
                state_.tag_recycle_signals + signalIndex(type, rank);
            const auto result = lane.waitSignal(
                SignalWaitRequest{signal, value, timeout_ticks_}, block);
            if (result.status != SignalWaitStatus::Reached ||
                result.observed != value)
                return {rank};
        }
        return {};
    }

    __device__ __forceinline__ void clearPackets(
        LLPacket* packets, uint64_t packet_bytes,
        cooperative_groups::thread_block block) const {
        PG_ASSERT(packets && packet_bytes != 0 &&
                  packet_bytes % sizeof(LLPacket) == 0);
        for (uint64_t index = block.thread_rank();
             index < packet_bytes / sizeof(LLPacket); index += block.size()) {
            // Clear tags with the same atomic width used to publish packets.
            cuda::atomic_ref<uint64_t, cuda::thread_scope_system>(
                packets[index].bits)
                .store(0, cuda::memory_order_relaxed);
        }
    }

    [[nodiscard]] __device__ __noinline__ CollectiveResult
    recycleTags(LLPacket* packets, uint64_t packet_bytes,
                cooperative_groups::thread_block block) const {
        // Quiesce senders and clear stale tails before reusing 32-bit tags.
        const uint64_t sequence = progress_.next_sequence;
        PG_ASSERT(sequence != 0);
        auto result = barrier(LLBarrierType::TagRecycleEnter, sequence, block);
        if (result.succeeded()) {
            clearPackets(packets, packet_bytes, block);
            // Publish every clearing thread's stores before the exit barrier.
            __threadfence_system();
            block.sync();
            result = barrier(LLBarrierType::TagRecycleExit, sequence, block);
        }
        if (block.thread_rank() == 0) {
            if (result.succeeded()) ++progress_.next_sequence;
            progress_.failed_rank = result.failed_rank;
        }
        block.sync();
        return result;
    }

    const LLState& state_;
    const DeviceGroupView& view_;
    LLChannelProgress& progress_;
    uint32_t channel_;
    uint64_t timeout_ticks_;
};

// Per-thread LL packet operations.
class LLPacketOps {
   public:
    __device__ __forceinline__ LLPacketOps(uint64_t sequence,
                                           uint64_t timeout_ticks)
        : tag_(LLPacket::tagFor(sequence)), timeout_ticks_(timeout_ticks) {}

    template <typename Pack>
    __device__ __forceinline__ void storePack(
        LLPacket* destination, typename Pack::Value value) const {
        static_assert(Pack::kPayloadWords == 1 || Pack::kPayloadWords == 2);
        storeWord(destination, static_cast<uint32_t>(value));
        if constexpr (Pack::kPayloadWords == 2)
            storeWord(destination + 1, static_cast<uint32_t>(value >> 32));
    }

    template <typename Pack>
    [[nodiscard]] __device__ __forceinline__ bool loadPack(
        const LLPacket* source, typename Pack::Value& value) const {
        static_assert(Pack::kPayloadWords == 1 || Pack::kPayloadWords == 2);
        uint32_t low;
        if (!loadWord(source, low)) return false;
        value = low;
        if constexpr (Pack::kPayloadWords == 2) {
            uint32_t high;
            if (!loadWord(source + 1, high)) return false;
            value |= static_cast<typename Pack::Value>(high) << 32;
        }
        return true;
    }

   private:
    __device__ __forceinline__ void storeWord(LLPacket* destination,
                                              uint32_t value) const {
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> packet(
            destination->bits);
        // The data and tag are one scalar; no other memory is published here.
        packet.store((uint64_t{tag_} << 32) | value,
                     cuda::memory_order_relaxed);
    }

    [[nodiscard]] __device__ __forceinline__ bool loadWord(
        const LLPacket* source, uint32_t& value) const {
        cuda::atomic_ref<uint64_t, cuda::thread_scope_system> packet(
            const_cast<uint64_t&>(source->bits));
        uint64_t observed = packet.load(cuda::memory_order_relaxed);

        // The ready case needs no clock access.
        if (static_cast<uint32_t>(observed >> 32) == tag_) {
            value = static_cast<uint32_t>(observed);
            return true;
        }

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

    uint32_t tag_;
    uint64_t timeout_ticks_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PRIMITIVES_CUH
