#include "device_comm/device_collective/algorithms/ring/ring_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"

namespace mooncake {
namespace {

[[nodiscard]] __device__ __forceinline__ CollectiveResult
runRingChannel(RingDeviceState* state, cooperative_groups::thread_block block) {
    const auto& view = *state->collective.group_view;
    const auto& plan = state->plan;
    PG_ASSERT(state->simple);
    auto primitives = SimplePrimitives<uint8_t>::bind(
        *state->simple, plan.workspace, view, plan.predecessor_index,
        plan.successor_index, /*channel=*/0, /*channel_count=*/1,
        kMaxRingChunkBytes, state->collective.timeout_ticks);
    PG_COLL_TRY(primitives.begin(block));

    // Active index 0 starts both rounds; others forward each message.
    // Round 1: the message returns to 0 only after everyone has entered.
    // Round 2: 0 then tells everyone they can leave the barrier.
    // Messages carry no payload; Simple still sends signals and ACKs.
    const CollectiveChunk<uint8_t> token{};
    for (uint32_t round = 0; round < 2; ++round) {
        if (view.self_active_index == 0) {
            PG_COLL_TRY(primitives.send(token, block));
            PG_COLL_TRY(primitives.recvCopy(token, block));
        } else {
            PG_COLL_TRY(primitives.recvCopySend(token, block));
        }
    }
    return primitives.finish(block);
}

__global__ __launch_bounds__(kRingBlockThreads,
                             1) void ringBarrierKernel(BarrierRequest request,
                                                       RingDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<CollectiveExecution::SingleCTA>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runRingChannel(state, block);
        });
}

}  // namespace

PGResult<void> launchRingBarrierKernel(const BarrierRequest& request,
                                       RingDeviceState* state,
                                       cudaStream_t stream) {
    ringBarrierKernel<<<1, kRingBlockThreads, 0, stream>>>(request, state);
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace mooncake
