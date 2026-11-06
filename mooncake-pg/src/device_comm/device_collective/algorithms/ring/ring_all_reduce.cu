#include "device_comm/device_collective/algorithms/ring/ring_types.cuh"

#include <cstdint>

#include <cooperative_groups.h>

#include "pg_assert.h"
#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_collective/protocols/simple/simple_primitives.cuh"

namespace mooncake {
namespace {

// Keep each participant's Ring shard large enough to amortize step signaling
// while allowing small collectives to use more of the fixed channel grid.
inline constexpr uint64_t kMinBytesPerChannelStep = 8ull << 10;
inline constexpr int kBlockThreads = 512;

template <typename T>
[[nodiscard]] __device__ __forceinline__ uint32_t
chooseChannelCount(uint64_t element_count, const RingAllReducePlan& plan) {
    PG_ASSERT(plan.participant_count != 0);
    const uint64_t payload_size = element_count * sizeof(T);

    // Launch is plan-independent. Each CTA derives the same active channel
    // prefix after the startup leader has applied the latest Plan.
    uint32_t channel_count = kMaxDeviceCollectiveChannels;
    while (channel_count > 1) {
        const bool shard_is_too_small =
            payload_size / plan.participant_count / channel_count <
            kMinBytesPerChannelStep;
        const auto buffers = SimpleBufferLayout::make(
            plan.workspace.bytes / channel_count, kMaxRingChunkBytes);
        if (!shard_is_too_small && buffers.chunk_bytes != 0) break;
        channel_count /= 2;
    }
    return channel_count;
}

[[nodiscard]] __device__ __forceinline__ uint64_t minimum(uint64_t left,
                                                          uint64_t right) {
    return left < right ? left : right;
}

[[nodiscard]] __device__ __forceinline__ uint64_t
divideRoundUp(uint64_t value, uint64_t divisor) {
    return value / divisor + (value % divisor != 0 ? 1 : 0);
}

// Ring maps each logical tile to local input/output operands. Simple receives
// the resulting addresses without knowing the Ring's shard or channel layout.
template <typename T>
struct RingTileLayout {
    const T* input = nullptr;
    T* output = nullptr;
    uint64_t channel_elements = 0;
    uint64_t shard_element_capacity = 0;
    uint64_t tile_element_capacity = 0;

    [[nodiscard]] __device__ __forceinline__ CollectiveChunk<T> tile(
        uint32_t shard_index, uint64_t tile_index) const {
        // With S = shard_element_capacity, shard i covers the channel-relative
        // interval [i * S, min((i + 1) * S, channel_elements)).
        const uint64_t shard_begin =
            static_cast<uint64_t>(shard_index) * shard_element_capacity;
        const uint64_t shard_elements =
            shard_begin < channel_elements
                ? minimum(shard_element_capacity,
                          channel_elements - shard_begin)
                : 0;

        // With K = tile_element_capacity, tile j covers the shard-relative
        // interval [j * K, min((j + 1) * K, shard_elements)).
        const uint64_t tile_begin = tile_index * tile_element_capacity;
        const uint64_t count =
            tile_begin < shard_elements
                ? minimum(tile_element_capacity, shard_elements - tile_begin)
                : 0;

        // Short shards still execute the common message schedule. Keep the
        // base pointers for an empty tile instead of forming addresses beyond
        // the channel's input/output ranges.
        if (count == 0) {
            return CollectiveChunk<T>{
                .source = input,
                .destination = output,
                .count = 0,
            };
        }

        const uint64_t offset = shard_begin + tile_begin;
        return CollectiveChunk<T>{
            .source = input + offset,
            .destination = output + offset,
            .count = count,
        };
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t tileCount() const {
        // Count tiles from the common shard capacity, rather than one shard's
        // clipped length, so every rank executes the same number of actions.
        return divideRoundUp(shard_element_capacity, tile_element_capacity);
    }
};

[[nodiscard]] __device__ __forceinline__ uint32_t
wrapActiveIndex(int64_t value, uint32_t participants) {
    value %= static_cast<int64_t>(participants);
    if (value < 0) value += participants;
    return static_cast<uint32_t>(value);
}

// A rank at active-index r handles shard (r - distance + P) % P. Each
// rank follows one column of the rotation below, while every row still covers
// all shards. The columns are active-indices, not GlobalRank values.
//
// For P = 4:
//
//              rank 0   rank 1   rank 2   rank 3
// distance 0   shard 0  shard 1  shard 2  shard 3
// distance 1   shard 3  shard 0  shard 1  shard 2
// distance 2   shard 2  shard 3  shard 0  shard 1
// distance 3   shard 1  shard 2  shard 3  shard 0
[[nodiscard]] __device__ __forceinline__ uint32_t ringShardAtDistance(
    int32_t self_active_index, uint32_t participant_count, uint64_t distance) {
    return wrapActiveIndex(static_cast<int64_t>(self_active_index) -
                               static_cast<int64_t>(distance),
                           participant_count);
}

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveStepResult
runRingTile(const RingAllReducePlan& plan, SimplePrimitives<T, Op>& primitives,
            const RingTileLayout<T>& layout, uint64_t tile_index,
            cooperative_groups::thread_block block) {
    const uint64_t ring_steps = plan.participant_count - 1;
    // Start reduce-scatter by injecting this rank's contribution for its own
    // shard into the ring.
    const uint32_t first_shard_index =
        ringShardAtDistance(plan.self_active_index, plan.participant_count, 0);
    const auto first_tile = layout.tile(first_shard_index, tile_index);
    auto result = primitives.send(first_tile, block);
    if (!result.succeeded()) return result;

    // Each intermediate reduce-scatter step receives a partially reduced
    // shard, adds this rank's contribution, and forwards the new partial.
    for (uint64_t distance = 1; distance < ring_steps; ++distance) {
        const uint32_t shard_index = ringShardAtDistance(
            plan.self_active_index, plan.participant_count, distance);
        const auto tile = layout.tile(shard_index, tile_index);
        result = primitives.recvReduceSend(tile, block);
        if (!result.succeeded()) return result;
    }

    // The final reduce-scatter receive completes one shard. Store the reduced
    // value in local output and forward it as the first all-gather send.
    const uint32_t completed_shard_index = ringShardAtDistance(
        plan.self_active_index, plan.participant_count, ring_steps);
    const auto completed_tile = layout.tile(completed_shard_index, tile_index);
    result = primitives.recvReduceCopySend(completed_tile, block);
    if (!result.succeeded()) return result;

    // The remaining all-gather steps receive fully reduced shards, copy them
    // into local output, and forward them without another reduction.
    for (uint64_t distance = 0; distance + 1 < ring_steps; ++distance) {
        const uint32_t shard_index = ringShardAtDistance(
            plan.self_active_index, plan.participant_count, distance);
        const auto tile = layout.tile(shard_index, tile_index);
        result = primitives.recvCopySend(tile, block);
        if (!result.succeeded()) return result;
    }

    // Receive the last fully reduced shard without forwarding it.
    const uint32_t last_shard_index = ringShardAtDistance(
        plan.self_active_index, plan.participant_count, ring_steps - 1);
    const auto last_tile = layout.tile(last_shard_index, tile_index);
    return primitives.recvCopy(last_tile, block);
}

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveStepResult
runRingChannel(const AllReduceRequest& request, RingAllReduceDeviceState* state,
               uint32_t channel, cooperative_groups::thread_block block) {
    // Recovery may update this host-constructed Plan between Graph replays.
    const auto plan = state->plan;
    if (request.count == 0) {
        return {};
    }

    const uint32_t channel_count = chooseChannelCount<T>(request.count, plan);
    if (channel >= channel_count) {
        // The fixed launch capacity keeps host submission independent of the
        // Plan image. Inactive CTAs still participate in invocation completion.
        return {};
    }
    const uint64_t elements_per_channel = request.count / channel_count;
    const uint64_t extra_elements = request.count % channel_count;
    const uint64_t channel_elements =
        elements_per_channel + (channel < extra_elements ? 1 : 0);
    const uint64_t channel_offset =
        static_cast<uint64_t>(channel) * elements_per_channel +
        minimum(channel, extra_elements);

    const auto* input =
        static_cast<const T*>(request.send_buffer) + channel_offset;
    auto* output = static_cast<T*>(request.recv_buffer) + channel_offset;

    if (plan.participant_count == 1) {
        if (input != output) {
            copyValuesTo(input, channel_elements, block, output);
        }
        return {};
    }
    auto primitives = SimplePrimitives<T, Op>::bind(
        *state->simple, plan.workspace, plan.predecessor(), plan.successor(),
        channel, channel_count, kMaxRingChunkBytes,
        state->collective.timeout_ticks);

    // full request -> channel -> ring shard -> tile
    const RingTileLayout<T> tile_layout{
        .input = input,
        .output = output,
        .channel_elements = channel_elements,
        .shard_element_capacity =
            divideRoundUp(channel_elements, plan.participant_count),
        .tile_element_capacity = primitives.chunkElements(),
    };

    const auto begin_result = primitives.begin(block);
    if (!begin_result.succeeded()) {
        return begin_result;
    }

    const uint64_t tile_count = tile_layout.tileCount();

    // A tile index selects the same-sized position within every ring shard.
    // Complete the reduce-scatter and all-gather traversal for that position
    // across all shards before advancing to the next tile index.
    for (uint64_t tile_index = 0; tile_index < tile_count; ++tile_index) {
        const auto result =
            runRingTile(plan, primitives, tile_layout, tile_index, block);
        if (!result.succeeded()) {
            return result;
        }
    }

    return primitives.finish(block);
}

template <typename T, ReduceOp Op>
__global__ __launch_bounds__(kBlockThreads, 1) void ringAllReduceKernel(
    AllReduceRequest request, RingAllReduceDeviceState* state) {
    const auto block = cooperative_groups::this_thread_block();
    const uint32_t channel = blockIdx.x;
    PG_ASSERT(state && state->simple);
    auto result = beginCollectiveInvocation<CollectiveExecution::MultiCTA>(
        &state->plan, state->collective, block, [&] {
            const auto& plan = state->plan;
            const auto& collective = state->collective;
            PG_ASSERT(collective.transfer_handle);
            return detail::synchronizeCollectiveViewEpoch(
                plan.view_epoch, collective.timeout_ticks,
                collective.view_epoch_signals, plan.remotePeers(),
                collective.transfer_handle->lane(channel), block);
        });
    if (result.succeeded())
        result = runRingChannel<T, Op>(request, state, channel, block);
    finishCollectiveInvocation<CollectiveExecution::MultiCTA>(
        state->collective, state->plan.remotePeers(), block, result.failed_rank,
        request.failed_ranks_hint);
}

template <typename T, ReduceOp Op>
cudaError_t launchKernel(const AllReduceRequest& request,
                         RingAllReduceDeviceState* state, cudaStream_t stream) {
    const dim3 grid(kMaxDeviceCollectiveChannels);
    ringAllReduceKernel<T, Op>
        <<<grid, kBlockThreads, 0, stream>>>(request, state);
    return cudaGetLastError();
}

template <typename T>
cudaError_t launchForType(const AllReduceRequest& request,
                          RingAllReduceDeviceState* state,
                          cudaStream_t stream) {
    switch (request.op) {
        case ReduceOp::Sum:
            return launchKernel<T, ReduceOp::Sum>(request, state, stream);
        case ReduceOp::Product:
            return launchKernel<T, ReduceOp::Product>(request, state, stream);
        case ReduceOp::Min:
            return launchKernel<T, ReduceOp::Min>(request, state, stream);
        case ReduceOp::Max:
            return launchKernel<T, ReduceOp::Max>(request, state, stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace

cudaError_t launchRingAllReduceKernel(const AllReduceRequest& request,
                                      RingAllReduceDeviceState* state,
                                      cudaStream_t stream) {
    switch (request.datatype) {
        case DataType::Float16:
            return launchForType<__half>(request, state, stream);
        case DataType::Uint8:
            return launchForType<uint8_t>(request, state, stream);
        case DataType::Int8:
            return launchForType<int8_t>(request, state, stream);
        case DataType::Int16:
            return launchForType<int16_t>(request, state, stream);
        case DataType::Int32:
            return launchForType<int32_t>(request, state, stream);
        case DataType::Int64:
            return launchForType<int64_t>(request, state, stream);
        case DataType::Bfloat16:
            return launchForType<__nv_bfloat16>(request, state, stream);
        case DataType::Float32:
            return launchForType<float>(request, state, stream);
        case DataType::Float64:
            return launchForType<double>(request, state, stream);
        case DataType::Bool:
            return launchForType<bool>(request, state, stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace mooncake
