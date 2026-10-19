#include "device_comm/device_collective/algorithms/star/star_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"

namespace mooncake {
namespace {

[[nodiscard]] __device__ __forceinline__ CollectiveResult runStarChannel(
    const GatherRequest& request, uint64_t rank_bytes, StarDeviceState* state,
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
    const auto* input =
        static_cast<const uint8_t*>(request.send_buffer) + partition.offset;
    auto* output =
        schedule.isRoot()
            ? static_cast<uint8_t*>(request.recv_buffer) + partition.offset
            : nullptr;
    if (schedule.isRoot()) {
        auto* destination =
            output + static_cast<uint64_t>(request.root) * rank_bytes;
        if (input != destination)
            copyValuesTo(input, partition.count, block, destination);
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
                .source = input + offset,
                .destination =
                    schedule.isRoot()
                        ? output + static_cast<uint64_t>(rank) * rank_bytes +
                              offset
                        : nullptr,
                .count = minimum(primitives.chunkElements(),
                                 partition.count - offset),
            };
            if (schedule.isRoot()) {
                PG_COLL_TRY(primitives.recvCopy(chunk, block));
            } else {
                PG_COLL_TRY(primitives.send(chunk, block));
            }
            offset += chunk.count;
        }
        // Complete the peer before reusing this channel's receive workspace.
        PG_COLL_TRY(primitives.finish(block));
    }
    return {};
}

__global__ __launch_bounds__(kStarBlockThreads,
                             1) void starGatherKernel(GatherRequest request,
                                                      uint64_t rank_bytes,
                                                      StarDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<CollectiveExecution::MultiCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runStarChannel(request, rank_bytes, state, blockIdx.x,
                                  block);
        });
}

}  // namespace

PGResult<void> launchStarGatherKernel(const GatherRequest& request,
                                      StarDeviceState* state,
                                      cudaStream_t stream) {
    const uint64_t rank_bytes = request.count * elementSize(request.datatype);
    const dim3 grid(kMaxDeviceCollectiveChannels);
    starGatherKernel<<<grid, kStarBlockThreads, 0, stream>>>(request,
                                                             rank_bytes, state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace mooncake
