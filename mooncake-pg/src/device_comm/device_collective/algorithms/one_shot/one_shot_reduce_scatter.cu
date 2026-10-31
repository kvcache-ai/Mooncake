#include "device_comm/device_collective/algorithms/one_shot/one_shot_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"

namespace mooncake {
namespace {

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveResult runOneShotChannel(
    const ReduceScatterRequest& request, OneShotDeviceState* state,
    uint32_t channel, uint32_t channel_count,
    cooperative_groups::thread_block block) {
    const auto& plan = state->plan;
    const auto& view = *state->collective.group_view;
    const auto partition =
        OneShotChannelPartition::make<T>(request.count, channel, channel_count);
    if (partition.count == 0) return {};

    const auto* input =
        static_cast<const T*>(request.send_buffer) + partition.offset;
    const auto* local_input =
        input +
        static_cast<uint64_t>(view.self().in_group_rank) * request.count;
    auto* output = static_cast<T*>(request.recv_buffer) + partition.offset;
    PG_ASSERT(state->ll);
    OneShotExchange<T> exchange(view, plan, *state->ll, channel,
                                state->collective.timeout_ticks);
    uint64_t offset = 0;
    while (offset < partition.count) {
        PG_COLL_TRY(exchange.begin(block));
        const CollectiveChunk<T> chunk{
            .source = local_input + offset,
            .destination = output + offset,
            .count = minimum(partition.count - offset,
                             exchange.chunkElementCapacity()),
        };
        // Send each peer its in-group-rank input segment before writing local
        // output.
        exchange.sendAll(input + offset, chunk.count, request.count, block);
        const auto failed = exchange.template reducePeers<Op>(chunk, block);
        PG_COLL_TRY(exchange.finish(failed, block));
        offset += chunk.count;
    }
    return {};
}

template <typename T, ReduceOp Op, CollectiveExecution Execution>
__global__
__launch_bounds__(kOneShotMaxThreads) void oneShotReduceScatterKernel(
    ReduceScatterRequest request, OneShotDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<Execution>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runOneShotChannel<T, Op>(request, state, blockIdx.x,
                                            gridDim.x, block);
        });
}

template <typename T, ReduceOp Op>
PGResult<void> launchKernel(const ReduceScatterRequest& request,
                            OneShotDeviceState* state, int threads,
                            uint32_t channels, cudaStream_t stream) {
    if (channels > 1) {
        oneShotReduceScatterKernel<T, Op, CollectiveExecution::MultiCTA>
            <<<channels, threads, 0, stream>>>(request, state);
    } else {
        oneShotReduceScatterKernel<T, Op, CollectiveExecution::SingleCTA>
            <<<1, threads, 0, stream>>>(request, state);
    }
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace

PGResult<void> launchOneShotReduceScatterKernel(
    const ReduceScatterRequest& request, OneShotDeviceState* state,
    cudaStream_t stream) {
    // The per-rank result size is fixed in a captured call.
    const uint64_t bytes = request.count * elementSize(request.datatype);
    const int threads = bytes <= 64    ? 128
                        : bytes <= 128 ? 256
                                       : kOneShotMaxThreads;
    const uint32_t channels = bytes <= 2048 ? 1 : kOneShotMaxChannels;
    PG_DISPATCH_REDUCTION(launchKernel, request, state, threads, channels,
                          stream);
}

}  // namespace mooncake
