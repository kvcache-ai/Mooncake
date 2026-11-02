#include "device_comm/device_collective/algorithms/ring/ring_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"
#include "device_comm/device_utils/device_utils.cuh"

namespace mooncake {
namespace {

[[nodiscard]] __device__ __forceinline__ CollectiveResult runRingChannel(
    const BroadcastRequest& request, uint64_t bytes, RingDeviceState* state,
    uint32_t channel, cooperative_groups::thread_block block) {
    const auto& view = *state->collective.group_view;
    // A captured request may outlive its root's membership.
    if (view.activeIndex(request.root) < 0) {
        return {
            .failed_rank = request.root,
            .failure_type = CollectiveFailureType::UnavailableParticipant,
        };
    }
    if (bytes == 0) return {};

    const auto& plan = state->plan;
    const uint32_t channel_count =
        RingChannelPartition::chooseChannelCount(bytes, plan.workspace.bytes);
    if (channel >= channel_count) return {};
    const auto partition =
        RingChannelPartition::make(bytes, channel, channel_count);
    const bool is_root = view.self().in_group_rank == request.root;
    const bool is_last =
        view.participants[plan.successor_index].in_group_rank == request.root;
    // Non-root callers need no send buffer. Never form an address from it.
    const auto* input = is_root
                            ? static_cast<const uint8_t*>(request.send_buffer) +
                                  partition.offset
                            : nullptr;
    auto* output =
        static_cast<uint8_t*>(request.recv_buffer) + partition.offset;

    PG_ASSERT(state->simple);
    auto primitives = SimplePrimitives<uint8_t>::bind(
        *state->simple, plan.workspace, view, plan.predecessor_index,
        plan.successor_index, channel, channel_count, kMaxRingChunkBytes,
        state->collective.timeout_ticks);
    // Every rank exchanges workspace readiness. Payload starts at the root
    // and stops at its predecessor, without traversing the final ring edge.
    PG_COLL_TRY(primitives.begin(block));
    for (uint64_t offset = 0; offset < partition.count;) {
        const CollectiveChunk<uint8_t> chunk{
            .source = is_root ? input + offset : nullptr,
            .destination = output + offset,
            .count =
                minimum(primitives.chunkElements(), partition.count - offset),
        };
        if (is_root) {
            PG_COLL_TRY(input == output ? primitives.send(chunk, block)
                                        : primitives.copySend(chunk, block));
        } else if (is_last) {
            PG_COLL_TRY(primitives.recvCopy(chunk, block));
        } else {
            PG_COLL_TRY(primitives.recvCopySend(chunk, block));
        }
        offset += chunk.count;
    }
    return primitives.finish(block);
}

__global__ __launch_bounds__(kRingBlockThreads, 1) void ringBroadcastKernel(
    BroadcastRequest request, uint64_t bytes, RingDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<CollectiveExecution::MultiCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runRingChannel(request, bytes, state, blockIdx.x, block);
        });
}

}  // namespace

PGResult<void> launchRingBroadcastKernel(const BroadcastRequest& request,
                                         RingDeviceState* state,
                                         cudaStream_t stream) {
    const uint64_t bytes = request.count * elementSize(request.datatype);
    const dim3 grid(kMaxDeviceCollectiveChannels);
    ringBroadcastKernel<<<grid, kRingBlockThreads, 0, stream>>>(request, bytes,
                                                                state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace mooncake
