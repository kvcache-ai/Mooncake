#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_KERNEL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_KERNEL_CUH

#include <cooperative_groups.h>

#include "device_comm/device_collective/algorithms/one_shot/one_shot_algorithm.cuh"
#include "device_comm/device_collective/protocols/ll/ll_primitives.cuh"
#include "device_comm/device_collective/protocols/ll/ll_value_pack.cuh"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {

// One channel's contiguous input range, measured in T elements. Channel
// boundaries align to LL packs so CTAs never share a pack.
struct OneShotChannelPartition {
    uint64_t offset;
    uint64_t count;

    // Channel capacity is ceil(pack_count / channel_count) packs. Clip each
    // range at count; trailing channels may be empty.
    //
    // For count = 10, pack size = 4 and channel_count = 4:
    //
    //             channel 0  channel 1  channel 2  channel 3
    // offset              0          4          8         12
    // count               4          4          2          0
    template <typename T>
    [[nodiscard]] __device__ __forceinline__ static OneShotChannelPartition
    make(uint64_t count, uint32_t channel, uint32_t channel_count) {
        if (channel_count == 1) return {0, count};
        const uint64_t packs = ceilDiv(count, LLValuePack<T>::kValueCount);
        const uint64_t capacity =
            ceilDiv(packs, channel_count) * LLValuePack<T>::kValueCount;
        const uint64_t offset = uint64_t{channel} * capacity;
        if (offset >= count) return {offset, 0};
        return {offset, minimum(count - offset, capacity)};
    }
};

// Send to every peer and finish all receives before advancing LL progress.
template <typename T>
class OneShotExchange {
    using Pack = LLValuePack<T>;

   public:
    __device__ __forceinline__ OneShotExchange(const DeviceGroupView& view,
                                               const OneShotPlan& plan,
                                               LLState& ll, uint32_t channel,
                                               uint64_t timeout_ticks)
        : view_(view),
          plan_(plan),
          ll_(ll),
          channel_(channel),
          timeout_ticks_(timeout_ticks) {}

    [[nodiscard]] __device__ __forceinline__ uint64_t
    chunkElementCapacity() const {
        return OneShotBufferLayout::kChunkBytes / sizeof(T);
    }

    [[nodiscard]] __device__ __forceinline__ static uint64_t packCount(
        uint64_t count) {
        return ceilDiv(count, Pack::kValueCount);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    begin(cooperative_groups::thread_block block) {
        const auto result =
            LLControl(ll_, view_, channel_, timeout_ticks_)
                .begin(plan_.packets + plan_.layout.packetIndex(channel_, 0, 0),
                       plan_.layout.channelPacketBytes(), block);
        sequence_ = ll_.progress[channel_].next_sequence;
        return result;
    }

    // Before X sends n+2, it receives Y's n+1, so Y has consumed X's n.
    // Empty/local-only calls consume no sequence, preserving this dependency.
    __device__ __forceinline__ void sendAll(
        CollectiveChunk<T> chunk,
        cooperative_groups::thread_block block) const {
        const uint64_t pack_count = packCount(chunk.count);
        const uint64_t work = pack_count * view_.participant_count;
        const LLPacketOps packets(sequence_, timeout_ticks_);
        for (uint64_t index = block.thread_rank(); index < work;
             index += block.size()) {
            const auto active_index = static_cast<uint32_t>(index / pack_count);
            if (active_index == static_cast<uint32_t>(view_.self_active_index))
                continue;
            const auto& peer = view_.participants[active_index];
            const uint64_t pack_index = index % pack_count;
            const auto value =
                Pack::load(chunk.source, chunk.count, pack_index);
            const uint64_t remote_offset =
                plan_.packet_offsets[active_index] +
                plan_.layout.packetIndex(
                    channel_, sequence_ % OneShotBufferLayout::kSlots,
                    view_.self().in_group_rank,
                    pack_index * Pack::kPayloadWords) *
                    LLPacket::kStorageBytes;
            auto* destination =
                static_cast<LLPacket*>(ll_.transfer_handle->remotePtr(
                    peer.global_rank, remote_offset));
            packets.storePack<Pack>(destination, value);
        }
        block.sync();
    }

    [[nodiscard]] __device__ __forceinline__ bool readReceived(
        uint32_t active_index, uint64_t pack_index,
        typename Pack::Value& value) const {
        const auto& peer = view_.participants[active_index];
        const auto* packet =
            plan_.packets +
            plan_.layout.packetIndex(
                channel_, sequence_ % OneShotBufferLayout::kSlots,
                peer.in_group_rank, pack_index * Pack::kPayloadWords);
        return LLPacketOps(sequence_, timeout_ticks_)
            .loadPack<Pack>(packet, value);
    }

    // Reduce every participant's contribution into chunk.destination. This
    // rank's contribution comes from chunk.source; others come from packets.
    template <ReduceOp Op>
    [[nodiscard]] __device__ __forceinline__ InGroupRank
    reducePeers(CollectiveChunk<T> chunk,
                cooperative_groups::thread_block block) const {
        const uint64_t pack_count = packCount(chunk.count);
        // Fill each tile with peers and give it only one pack.
        const bool tiled = view_.participant_count >= kReductionTileSize &&
                           pack_count <= block.size() / kReductionTileSize;
        return tiled ? reducePeersTiled<Op>(chunk, pack_count, block)
                     : reducePeersScalar<Op>(chunk, pack_count, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    finish(InGroupRank failed, cooperative_groups::thread_block block) {
        return LLControl(ll_, view_, channel_, timeout_ticks_)
            .finish(sequence_, failed, block);
    }

   private:
    static constexpr int kReductionTileSize = 8;

    template <ReduceOp Op>
    [[nodiscard]] __device__ __forceinline__ InGroupRank
    reducePeersScalar(CollectiveChunk<T> chunk, uint64_t pack_count,
                      cooperative_groups::thread_block block) const {
        for (uint64_t pack_index = block.thread_rank(); pack_index < pack_count;
             pack_index += block.size()) {
            typename Pack::Value reduced = 0;
            for (uint32_t peer = 0; peer < view_.participant_count; ++peer) {
                typename Pack::Value value;
                if (peer == static_cast<uint32_t>(view_.self_active_index)) {
                    value = Pack::load(chunk.source, chunk.count, pack_index);
                } else if (!readReceived(peer, pack_index, value)) {
                    return view_.participants[peer].in_group_rank;
                }
                // Start with the first participant; no identity value is
                // needed.
                reduced = peer == 0 ? value
                                    : Pack::template reduce<Op>(reduced, value);
            }
            Pack::store(chunk.destination, chunk.count, pack_index, reduced);
        }
        return kInvalidInGroupRank;
    }

    template <ReduceOp Op>
    [[nodiscard]] __device__ __forceinline__ InGroupRank
    reducePeersTiled(CollectiveChunk<T> chunk, uint64_t pack_count,
                     cooperative_groups::thread_block block) const {
        const auto tile =
            cooperative_groups::tiled_partition<kReductionTileSize>(block);
        for (uint64_t pack_index = tile.meta_group_rank();
             pack_index < pack_count; pack_index += tile.meta_group_size()) {
            typename Pack::Value reduced = 0;
            for (uint32_t base = 0; base < view_.participant_count;
                 base += kReductionTileSize) {
                const uint32_t peer = base + tile.thread_rank();
                typename Pack::Value value = 0;
                InGroupRank failed = kInvalidInGroupRank;
                if (peer < view_.participant_count) {
                    if (peer ==
                        static_cast<uint32_t>(view_.self_active_index)) {
                        value =
                            Pack::load(chunk.source, chunk.count, pack_index);
                    } else if (!readReceived(peer, pack_index, value)) {
                        failed = view_.participants[peer].in_group_rank;
                    }
                }
                const auto failures =
                    tile.ballot(failed != kInvalidInGroupRank);
                if (failures) return tile.shfl(failed, __ffs(failures) - 1);
#pragma unroll
                for (uint32_t lane = 0; lane < kReductionTileSize; ++lane) {
                    const uint32_t low =
                        tile.shfl(static_cast<uint32_t>(value), lane);
                    typename Pack::Value received = low;
                    if constexpr (Pack::kPayloadWords == 2) {
                        const uint32_t high =
                            tile.shfl(static_cast<uint32_t>(value >> 32), lane);
                        received |= static_cast<typename Pack::Value>(high)
                                    << 32;
                    }
                    if (tile.thread_rank() == 0 &&
                        base + lane < view_.participant_count)
                        reduced = base + lane == 0 ? received
                                                   : Pack::template reduce<Op>(
                                                         reduced, received);
                }
            }
            if (tile.thread_rank() == 0) {
                Pack::store(chunk.destination, chunk.count, pack_index,
                            reduced);
            }
        }
        return kInvalidInGroupRank;
    }

    const DeviceGroupView& view_;
    const OneShotPlan& plan_;
    LLState& ll_;
    uint32_t channel_;
    uint64_t timeout_ticks_;
    uint64_t sequence_ = 0;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ONE_SHOT_KERNEL_CUH
