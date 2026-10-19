#include "device_comm/device_collective/algorithms/one_shot/one_shot_kernel.cuh"

#include <cstdint>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"

namespace mooncake {
namespace {

[[nodiscard]] __device__ __forceinline__ InGroupRank gatherPeers(
    const CollectiveChunk<uint8_t>& chunk, uint8_t* output, uint64_t rank_bytes,
    const DeviceGroupView& view, const OneShotExchange<uint8_t>& exchange,
    cooperative_groups::thread_block block) {
    using Pack = LLValuePack<uint8_t>;
    const uint64_t pack_count =
        OneShotExchange<uint8_t>::packCount(chunk.count);
    // Each (participant, pack) pair writes a disjoint part of the output.
    const uint64_t work = uint64_t{view.participant_count} * pack_count;
    for (uint64_t index = block.thread_rank(); index < work;
         index += block.size()) {
        const auto active_index = static_cast<uint32_t>(index / pack_count);
        const uint64_t pack_index = index % pack_count;
        const auto rank = view.participants[active_index].in_group_rank;
        Pack::Value value;
        if (active_index == static_cast<uint32_t>(view.self_active_index)) {
            value = Pack::load(chunk.source, chunk.count, pack_index);
        } else if (!exchange.readReceived(active_index, pack_index, value)) {
            return rank;
        }
        auto* destination = output + static_cast<uint64_t>(rank) * rank_bytes;
        Pack::store(destination, chunk.count, pack_index, value);
    }
    return kInvalidInGroupRank;
}

[[nodiscard]] __device__ __forceinline__ CollectiveResult runOneShotChannel(
    const AllGatherRequest& request, uint64_t rank_bytes,
    OneShotDeviceState* state, uint32_t channel, uint32_t channel_count,
    cooperative_groups::thread_block block) {
    const auto& plan = state->plan;
    const auto& view = *state->collective.group_view;
    const auto partition = OneShotChannelPartition::make<uint8_t>(
        rank_bytes, channel, channel_count);
    if (partition.count == 0) return {};

    const auto* input =
        static_cast<const uint8_t*>(request.send_buffer) + partition.offset;
    auto* output =
        static_cast<uint8_t*>(request.recv_buffer) + partition.offset;
    PG_ASSERT(state->ll);
    OneShotExchange<uint8_t> exchange(view, plan, *state->ll, channel,
                                      state->collective.timeout_ticks);
    uint64_t offset = 0;
    while (offset < partition.count) {
        PG_COLL_TRY(exchange.begin(block));
        const CollectiveChunk<uint8_t> chunk{
            .source = input + offset,
            .count = minimum(partition.count - offset,
                             exchange.chunkElementCapacity()),
        };
        // Publish the local contribution before writing any output, including
        // its own slot when the request is in-place.
        exchange.sendAll(chunk.source, chunk.count, 0, block);
        const auto failed = gatherPeers(chunk, output + offset, rank_bytes,
                                        view, exchange, block);
        PG_COLL_TRY(exchange.finish(failed, block));
        offset += chunk.count;
    }
    return {};
}

template <CollectiveExecution Execution>
__global__ __launch_bounds__(kOneShotMaxThreads) void oneShotAllGatherKernel(
    AllGatherRequest request, uint64_t rank_bytes, OneShotDeviceState* state) {
    PG_ASSERT(state);
    runCollectiveInvocation<Execution>(
        request, &state->plan, state->collective,
        [&](cooperative_groups::thread_block block) {
            return runOneShotChannel(request, rank_bytes, state, blockIdx.x,
                                     gridDim.x, block);
        });
}

}  // namespace

PGResult<void> launchOneShotAllGatherKernel(const AllGatherRequest& request,
                                            OneShotDeviceState* state,
                                            cudaStream_t stream) {
    const uint64_t rank_bytes = request.count * elementSize(request.datatype);
    const int threads = rank_bytes <= 64    ? 128
                        : rank_bytes <= 128 ? 256
                                            : kOneShotMaxThreads;
    const uint32_t channels = rank_bytes <= 2048 ? 1 : kOneShotMaxChannels;
    if (channels > 1) {
        oneShotAllGatherKernel<CollectiveExecution::MultiCTA>
            <<<channels, threads, 0, stream>>>(request, rank_bytes, state);
    } else {
        oneShotAllGatherKernel<CollectiveExecution::SingleCTA>
            <<<1, threads, 0, stream>>>(request, rank_bytes, state);
    }
    PG_TRY_CUDA(cudaGetLastError());
    return {};
}

}  // namespace mooncake
