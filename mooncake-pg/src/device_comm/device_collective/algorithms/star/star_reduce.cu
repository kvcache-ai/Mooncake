#include "device_comm/device_collective/algorithms/star/star_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"

namespace mooncake {
namespace {

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
runStarChannel(const ReduceRequest& request, StarDeviceState* state,
               uint32_t channel, cooperative_groups::thread_block block) {
    const auto& view = *state->collective.group_view;
    const auto root_index = view.activeIndex(request.root);
    if (root_index < 0) {
        return {
            .failed_rank = request.root,
            .failure_type = CollectiveFailureType::UnavailableParticipant,
        };
    }
    if (request.count == 0) return {};

    const auto& plan = state->plan;
    const uint32_t channel_count = StarChannelPartition::chooseChannelCount(
        request.count * sizeof(T), plan.workspace.bytes);
    if (channel >= channel_count) return {};
    const auto partition =
        StarChannelPartition::make(request.count, channel, channel_count);
    const StarPeerSchedule schedule{view, static_cast<uint32_t>(root_index)};
    const auto* input =
        static_cast<const T*>(request.send_buffer) + partition.offset;
    auto* output = schedule.isRoot()
                       ? static_cast<T*>(request.recv_buffer) + partition.offset
                       : nullptr;
    const auto [send_op, reduce_op, final_op] =
        makeStarReductionOps<T, Op>(view.participant_count);
    if (schedule.isRoot()) {
        // Seed the accumulator with the root's transformed contribution.
        transformValuesTo(input, partition.count, block, send_op, output);
        block.sync();
    }

    PG_ASSERT(state->simple);
    for (uint32_t step = 0; step < schedule.peerCount(); ++step) {
        const uint32_t peer_index = schedule.peerIndex(step);
        auto primitives = SimplePrimitives<T>::bind(
            *state->simple, plan.workspace, view, peer_index, peer_index,
            channel, channel_count, kMaxStarChunkBytes,
            state->collective.timeout_ticks);
        PG_COLL_TRY(primitives.begin(block));
        for (uint64_t offset = 0; offset < partition.count;) {
            const CollectiveChunk<T> chunk{
                .source = schedule.isRoot() ? output + offset : input + offset,
                .destination = schedule.isRoot() ? output + offset : nullptr,
                .count = minimum(primitives.chunkElements(),
                                 partition.count - offset),
            };
            if (schedule.isRoot()) {
                if (step + 1 == schedule.peerCount()) {
                    PG_COLL_TRY(
                        primitives.recvTransformCopy(chunk, block, final_op));
                } else {
                    PG_COLL_TRY(
                        primitives.recvTransformCopy(chunk, block, reduce_op));
                }
            } else {
                PG_COLL_TRY(primitives.transformSend(chunk, block, send_op));
            }
            offset += chunk.count;
        }
        PG_COLL_TRY(primitives.finish(block));
    }
    return {};
}

template <typename T, ReduceOp Op>
__global__ __launch_bounds__(kStarBlockThreads,
                             1) void starReduceKernel(ReduceRequest request,
                                                      StarDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<CollectiveExecution::MultiCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runStarChannel<T, Op>(request, state, blockIdx.x, block);
        });
}

template <typename T, ReduceOp Op>
PGResult<void> launchKernel(const ReduceRequest& request,
                            StarDeviceState* state, cudaStream_t stream) {
    const dim3 grid(kMaxDeviceCollectiveChannels);
    starReduceKernel<T, Op>
        <<<grid, kStarBlockThreads, 0, stream>>>(request, state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace

PGResult<void> launchStarReduceKernel(const ReduceRequest& request,
                                      StarDeviceState* state,
                                      cudaStream_t stream) {
    PG_DISPATCH_REDUCTION(launchKernel, request, state, stream);
}

}  // namespace mooncake
