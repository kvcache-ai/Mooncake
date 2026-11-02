#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PRIMITIVES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_LL_PRIMITIVES_CUH

#include <cuda/atomic>

#include "device_comm/device_collective/protocols/ll/ll_types.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"
#include "pg_assert.h"
#include "device_comm/device_utils/device_timeout.cuh"

namespace mooncake {

// CTA-collective LL control for one invocation.
class LLControl {
   public:
    __device__ __forceinline__ LLControl(LLState& state, uint32_t channel,
                                         uint64_t timeout_ticks)
        : bindings_(state.bindings),
          progress_(state.progress[channel]),
          channel_(channel),
          timeout_ticks_(timeout_ticks) {
        PG_ASSERT(channel < kMaxDeviceCollectiveChannels);
    }

    // Packet storage is borrowed from the algorithm for cold-path resets.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    begin(uint64_t view_epoch, RemotePeerList remote_peers, LLPacket* packets,
          uint64_t packet_bytes, cooperative_groups::thread_block block) const {
        if (progress_.view_epoch != view_epoch)
            return resetForView(view_epoch, remote_peers, packets, packet_bytes,
                                block);
        if (progress_.failed_rank != kInvalidInGroupRank)
            return {progress_.failed_rank};
        if (LLPacket::tagFor(progress_.next_sequence) == 0)
            return recycleTags(remote_peers, packets, packet_bytes, block);
        return {};
    }

    // Advance after all receives finish; failure requires a new View.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
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
                   bindings_.max_group_size +
               static_cast<uint32_t>(sender);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    barrier(LLBarrierType type, uint64_t value, RemotePeerList remote_peers,
            cooperative_groups::thread_block block) const {
        const auto lane = bindings_.transfer_handle->lane(channel_);
        for (const auto& peer : remote_peers) {
            const auto rank = peer.in_group_rank;
            PG_ASSERT(rank >= 0 &&
                      static_cast<uint32_t>(rank) < bindings_.max_group_size);
            const auto& binding = bindings_.peer_bindings[rank];
            PG_ASSERT(binding.global_rank == peer.global_rank);
            SignalRequest request;
            request.signal.kind = SignalAction::Kind::Set;
            request.signal.remote_offset =
                binding.signal_offset +
                signalIndex(type, bindings_.self_rank) * sizeof(uint64_t);
            request.signal.set.value = value;
            request.timeout_ticks = timeout_ticks_;
            if (lane.signal(binding.global_rank, request, block).wait(block) !=
                TransferResult::Succeeded)
                return {rank};
        }
        for (const auto& peer : remote_peers) {
            const auto rank = peer.in_group_rank;
            const auto* signal = bindings_.signals + signalIndex(type, rank);
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
            // Removed peers may still write storage excluded by the new View.
            cuda::atomic_ref<uint64_t, cuda::thread_scope_system>(
                packets[index].bits)
                .store(0, cuda::memory_order_relaxed);
        }
    }

    __device__ __forceinline__ void clearSignals(
        LLBarrierType type, cooperative_groups::thread_block block) const {
        auto* signals = bindings_.signals + signalIndex(type, 0);
        for (uint32_t index = block.thread_rank();
             index < bindings_.max_group_size; index += block.size()) {
            cuda::atomic_ref<uint64_t, cuda::thread_scope_system>(
                signals[index])
                .store(0, cuda::memory_order_relaxed);
        }
    }

    [[nodiscard]] __device__ __noinline__ CollectiveStepResult resetForView(
        uint64_t view_epoch, RemotePeerList remote_peers, LLPacket* packets,
        uint64_t packet_bytes, cooperative_groups::thread_block block) const {
        PG_ASSERT(view_epoch != kInvalidViewEpoch);
        // All active senders enter the reset before storage is cleared.
        const uint64_t marker = view_epoch + 1;  // Zero means never published.
        auto result =
            barrier(LLBarrierType::ViewResetEnter, marker, remote_peers, block);
        if (result.succeeded()) {
            clearPackets(packets, packet_bytes, block);
            clearSignals(LLBarrierType::TagRecycleEnter, block);
            clearSignals(LLBarrierType::TagRecycleExit, block);
            // Publish every clearing thread's stores before the exit barrier.
            __threadfence_system();
            block.sync();
            result = barrier(LLBarrierType::ViewResetExit, marker, remote_peers,
                             block);
        }
        if (block.thread_rank() == 0) {
            progress_.view_epoch = view_epoch;
            progress_.next_sequence = LLPacket::kFirstSequence;
            progress_.failed_rank = result.failed_rank;
        }
        block.sync();
        return result;
    }

    [[nodiscard]] __device__ __noinline__ CollectiveStepResult recycleTags(
        RemotePeerList remote_peers, LLPacket* packets, uint64_t packet_bytes,
        cooperative_groups::thread_block block) const {
        // Quiesce senders and clear stale tails before reusing 32-bit tags.
        const uint64_t sequence = progress_.next_sequence;
        PG_ASSERT(sequence != 0);
        auto result = barrier(LLBarrierType::TagRecycleEnter, sequence,
                              remote_peers, block);
        if (result.succeeded()) {
            clearPackets(packets, packet_bytes, block);
            // Publish every clearing thread's stores before the exit barrier.
            __threadfence_system();
            block.sync();
            result = barrier(LLBarrierType::TagRecycleExit, sequence,
                             remote_peers, block);
        }
        if (block.thread_rank() == 0) {
            if (result.succeeded()) ++progress_.next_sequence;
            progress_.failed_rank = result.failed_rank;
        }
        block.sync();
        return result;
    }

    const LLBindings& bindings_;
    LLProgress& progress_;
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
