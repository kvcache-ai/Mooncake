#include "device_comm/device_collective/algorithms/pairwise/pairwise_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"

namespace mooncake {
namespace {

[[nodiscard]] __device__ __forceinline__ CollectiveResult
runPairwiseChannel(const AllToAllRequest& request, uint64_t peer_bytes,
                   PairwiseDeviceState* state, uint32_t channel,
                   cooperative_groups::thread_block block) {
    if (peer_bytes == 0) return {};

    const auto& plan = state->plan;
    const auto& view = *state->collective.group_view;
    const uint32_t channel_count = PairwiseChannelPartition::chooseChannelCount(
        peer_bytes, plan.workspace.bytes);
    if (channel >= channel_count) return {};
    const auto partition =
        PairwiseChannelPartition::make(peer_bytes, channel, channel_count);
    const auto* input =
        static_cast<const uint8_t*>(request.send_buffer) + partition.offset;
    auto* output =
        static_cast<uint8_t*>(request.recv_buffer) + partition.offset;

    PG_ASSERT(state->simple);
    for (uint32_t round = 0; round < view.participant_count; ++round) {
        const uint32_t peer_index = pairwisePeerAtRound(view, round);
        const auto peer_rank = view.participants[peer_index].in_group_rank;
        PG_ASSERT(peer_rank >= 0);
        const uint64_t peer_offset =
            static_cast<uint64_t>(peer_rank) * peer_bytes;
        const auto* source = input + peer_offset;
        auto* destination = output + peer_offset;
        if (peer_index == static_cast<uint32_t>(view.self_active_index)) {
            if (source != destination)
                copyValuesTo(source, partition.count, block, destination);
            block.sync();
            continue;
        }

        auto primitives = SimplePrimitives<uint8_t>::bind(
            *state->simple, plan.workspace, view, peer_index, peer_index,
            channel, channel_count, kMaxPairwiseChunkBytes,
            state->collective.timeout_ticks);
        PG_COLL_TRY(primitives.begin(block));
        for (uint64_t offset = 0; offset < partition.count;) {
            const CollectiveChunk<uint8_t> chunk{
                .source = source + offset,
                .destination = destination + offset,
                .count = minimum(primitives.chunkElements(),
                                 partition.count - offset),
            };
            // With in-place buffers, source and destination are the same
            // chunk. send() copies it into Simple's payload storage (peer
            // workspace or local staging), so recvCopy() can then overwrite
            // the user buffer.
            PG_COLL_TRY(primitives.send(chunk, block));
            PG_COLL_TRY(primitives.recvCopy(chunk, block));
            offset += chunk.count;
        }
        PG_COLL_TRY(primitives.finish(block));
    }
    return {};
}

__global__ __launch_bounds__(
    kPairwiseBlockThreads,
    1) void pairwiseAllToAllKernel(AllToAllRequest request, uint64_t peer_bytes,
                                   PairwiseDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<CollectiveExecution::MultiCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runPairwiseChannel(request, peer_bytes, state, blockIdx.x,
                                      block);
        });
}

}  // namespace

PGResult<void> launchPairwiseAllToAllKernel(const AllToAllRequest& request,
                                            PairwiseDeviceState* state,
                                            cudaStream_t stream) {
    const uint64_t peer_bytes = request.count * elementSize(request.datatype);
    const dim3 grid(kMaxDeviceCollectiveChannels);
    pairwiseAllToAllKernel<<<grid, kPairwiseBlockThreads, 0, stream>>>(
        request, peer_bytes, state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace mooncake
