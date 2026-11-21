#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_STAR_KERNEL_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_STAR_KERNEL_CUH

#include <type_traits>

#include "device_comm/device_collective/algorithms/star/star_algorithm.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {

// Keep enough data per channel to amortize signaling each peer.
inline constexpr uint64_t kMinStarBytesPerChannel = 8ull << 10;
inline constexpr int kStarBlockThreads = 512;

// One channel's contiguous range, in elements or bytes as chosen by the caller.
struct StarChannelPartition {
    uint64_t offset;
    uint64_t count;

    [[nodiscard]] __device__ __forceinline__ static uint32_t chooseChannelCount(
        uint64_t peer_bytes, uint64_t workspace_bytes) {
        uint32_t channel_count = kMaxDeviceCollectiveChannels;
        while (channel_count > 1) {
            const auto buffers = SimpleBufferLayout::make(
                workspace_bytes / channel_count, kMaxStarChunkBytes);
            if (peer_bytes / channel_count >= kMinStarBytesPerChannel &&
                buffers.chunk_bytes != 0)
                break;
            channel_count /= 2;
        }
        return channel_count;
    }

    // The first count % channel_count channels receive one extra unit.
    [[nodiscard]] __device__ __forceinline__ static StarChannelPartition make(
        uint64_t count, uint32_t channel, uint32_t channel_count) {
        const uint64_t per_channel = count / channel_count;
        const uint64_t extra = count % channel_count;
        return {uint64_t{channel} * per_channel + minimum(channel, extra),
                per_channel + (channel < extra ? 1 : 0)};
    }
};

// The root visits each remote participant once. Every other rank visits only
// the root; all indices refer to the current DeviceGroupView.
struct StarPeerSchedule {
    const DeviceGroupView& view;
    uint32_t root_index;

    [[nodiscard]] __device__ __forceinline__ bool isRoot() const {
        return static_cast<uint32_t>(view.self_active_index) == root_index;
    }

    [[nodiscard]] __device__ __forceinline__ uint32_t peerCount() const {
        return isRoot() ? view.participant_count - 1 : 1;
    }

    [[nodiscard]] __device__ __forceinline__ uint32_t
    peerIndex(uint32_t step) const {
        return isRoot() ? step + (step >= root_index ? 1 : 0) : root_index;
    }
};

// Transforms for each contribution and the root's partial and final reductions.
template <typename SendOp, typename ReduceTransform, typename FinalOp>
struct StarReductionOps {
    SendOp send_op;
    ReduceTransform reduce_op;
    FinalOp final_op;
};

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ auto makeStarReductionOps(
    uint32_t participant_count) {
    if constexpr (Op == ReduceOp::Avg && !std::is_integral_v<T>) {
        // Scale each contribution before adding it to the root's accumulator.
        return StarReductionOps<Mul<T>, Reduce<T, ReduceOp::Sum>,
                                Reduce<T, ReduceOp::Sum>>{
            .send_op = {reciprocal<T>(participant_count)},
            .reduce_op = {},
            .final_op = {},
        };
    } else if constexpr (Op == ReduceOp::Avg) {
        // Divide only after the last contribution has been added.
        return StarReductionOps<Identity, Reduce<T, ReduceOp::Sum>, SumDiv<T>>{
            .send_op = {},
            .reduce_op = {},
            .final_op = {participant_count},
        };
    } else {
        return StarReductionOps<Identity, Reduce<T, Op>, Reduce<T, Op>>{};
    }
}

// The root sends one range to each peer. A zero source stride broadcasts the
// same range; otherwise in-group rank selects a segment of the root's input.
template <typename Request>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
runStarSendChannel(const Request& request, uint64_t rank_bytes,
                   uint64_t source_stride, StarDeviceState* state,
                   uint32_t channel, cooperative_groups::thread_block block) {
    const auto& view = *state->collective.group_view;
    const auto root_index = view.activeIndex(request.root);
    if (root_index < 0) {
        return {
            .failed_rank = request.root,
            .failure_type = CollectiveFailureType::UnavailableParticipant,
        };
    }
    if (rank_bytes == 0) return {};

    const auto& plan = state->plan;
    const uint32_t channel_count = StarChannelPartition::chooseChannelCount(
        rank_bytes, plan.workspace.bytes);
    if (channel >= channel_count) return {};
    const auto partition =
        StarChannelPartition::make(rank_bytes, channel, channel_count);
    const StarPeerSchedule schedule{view, static_cast<uint32_t>(root_index)};
    // Non-root callers need not supply a send buffer.
    const auto* input = schedule.isRoot()
                            ? static_cast<const uint8_t*>(request.send_buffer) +
                                  partition.offset
                            : nullptr;
    auto* output =
        static_cast<uint8_t*>(request.recv_buffer) + partition.offset;
    if (schedule.isRoot()) {
        const auto* source =
            input + static_cast<uint64_t>(request.root) * source_stride;
        if (source != output)
            copyValuesTo(source, partition.count, block, output);
        block.sync();
    }

    PG_ASSERT(state->simple);
    for (uint32_t step = 0; step < schedule.peerCount(); ++step) {
        const uint32_t peer_index = schedule.peerIndex(step);
        const auto rank = view.participants[peer_index].in_group_rank;
        auto primitives = SimplePrimitives<uint8_t>::bind(
            *state->simple, plan.workspace, view, peer_index, peer_index,
            channel, channel_count, kMaxStarChunkBytes,
            state->collective.timeout_ticks);
        PG_COLL_TRY(primitives.begin(block));
        for (uint64_t offset = 0; offset < partition.count;) {
            const CollectiveChunk<uint8_t> chunk{
                .source =
                    schedule.isRoot()
                        ? input + static_cast<uint64_t>(rank) * source_stride +
                              offset
                        : nullptr,
                .destination = output + offset,
                .count = minimum(primitives.chunkElements(),
                                 partition.count - offset),
            };
            if (schedule.isRoot()) {
                PG_COLL_TRY(primitives.send(chunk, block));
            } else {
                PG_COLL_TRY(primitives.recvCopy(chunk, block));
            }
            offset += chunk.count;
        }
        PG_COLL_TRY(primitives.finish(block));
    }
    return {};
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_STAR_KERNEL_CUH
