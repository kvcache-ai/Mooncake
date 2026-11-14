#include "device_comm/device_collective/algorithms/one_shot/one_shot_kernel.cuh"

#include <cstdint>

#include <cooperative_groups.h>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/ll/ll_value_pack.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"
#include "device_comm/device_utils/device_utils.cuh"
#include "pg_assert.h"

namespace mooncake {
namespace {

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
runOneShotChunks(CollectiveChunk<T> values, OneShotExchange<T>& exchange,
                 cooperative_groups::thread_block block) {
    uint64_t offset = 0;
    while (offset < values.count) {
        PG_COLL_TRY(exchange.begin(block));
        const uint64_t remaining = values.count - offset;
        const CollectiveChunk<T> chunk{
            .source = values.source + offset,
            .destination = values.destination + offset,
            .count = remaining < exchange.chunkElementCapacity()
                         ? remaining
                         : exchange.chunkElementCapacity(),
        };
        exchange.sendAll(chunk.source, chunk.count, 0, block);
        const auto failed = exchange.template reducePeers<Op>(chunk, block);
        PG_COLL_TRY(exchange.finish(failed, block));
        offset += chunk.count;
    }
    return {};
}

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveResult
runOneShotChannel(const AllReduceRequest& request, OneShotDeviceState* state,
                  uint32_t channel, uint32_t channel_count,
                  cooperative_groups::thread_block block) {
    const auto& plan = state->plan;
    const auto& view = *state->collective.group_view;
    const auto partition =
        OneShotChannelPartition::make<T>(request.count, channel, channel_count);
    if (partition.count == 0) return {};
    const CollectiveChunk<T> values{
        .source = static_cast<const T*>(request.send_buffer) + partition.offset,
        .destination = static_cast<T*>(request.recv_buffer) + partition.offset,
        .count = partition.count,
    };
    PG_ASSERT(state->ll);
    OneShotExchange<T> exchange(view, plan, *state->ll, channel,
                                state->collective.timeout_ticks);
    return runOneShotChunks<T, Op>(values, exchange, block);
}

template <typename T, ReduceOp Op, CollectiveExecution Execution>
__global__ __launch_bounds__(kOneShotMaxThreads) void oneShotAllReduceKernel(
    AllReduceRequest request, OneShotDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<Execution>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runOneShotChannel<T, Op>(request, state, blockIdx.x,
                                            gridDim.x, block);
        });
}

template <typename T, ReduceOp Op>
PGResult<void> launchKernel(const AllReduceRequest& request,
                            OneShotDeviceState* state, int threads,
                            uint32_t channels, cudaStream_t stream) {
    if (channels > 1) {
        oneShotAllReduceKernel<T, Op, CollectiveExecution::MultiCTA>
            <<<channels, threads, 0, stream>>>(request, state);
    } else {
        oneShotAllReduceKernel<T, Op, CollectiveExecution::SingleCTA>
            <<<1, threads, 0, stream>>>(request, state);
    }
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace

PGResult<void> launchOneShotAllReduceKernel(const AllReduceRequest& request,
                                            OneShotDeviceState* state,
                                            cudaStream_t stream) {
    const uint64_t bytes = request.count * elementSize(request.datatype);
    // Size is fixed in a captured call; smaller blocks suit tiny messages.
    const int threads = bytes <= 64    ? 128
                        : bytes <= 128 ? 256
                                       : kOneShotMaxThreads;
    const uint32_t channels = bytes <= 2048 ? 1 : kOneShotMaxChannels;
    PG_DISPATCH_REDUCTION(launchKernel, request, state, threads, channels,
                          stream);
}

}  // namespace mooncake
