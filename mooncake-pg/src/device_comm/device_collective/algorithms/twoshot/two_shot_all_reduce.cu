#include "device_comm/device_collective/algorithms/twoshot/two_shot_types.cuh"

#include <cstdint>

#include <cooperative_groups.h>

#include "pg_assert.h"
#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_collective/protocols/simple/simple_peer_group.cuh"

namespace mooncake {
namespace {

// Keep each participant's shard large enough to amortize peer signaling
// while allowing small collectives to use more of the fixed channel grid.
inline constexpr uint64_t kMinBytesPerChannelStep = 8ull << 10;
inline constexpr int kBlockThreads = 256;

template <typename T>
[[nodiscard]] __device__ __forceinline__ uint32_t
chooseChannelCount(uint64_t element_count, const TwoShotAllReducePlan& plan) {
    PG_ASSERT(plan.participant_count != 0);
    const uint64_t payload_size = element_count * sizeof(T);

    // Launch is plan-independent. Each CTA derives the same active channel
    // prefix after the startup leader has applied the latest Plan.
    uint32_t channel_count = gridDim.x;
    while (channel_count > 1) {
        const bool shard_is_too_small =
            payload_size / plan.participant_count / channel_count <
            kMinBytesPerChannelStep;
        const auto buffers = SimpleBufferLayout::make(
            plan.workspace.bytes / channel_count / plan.participant_count,
            kMaxTwoShotChunkBytes);
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

// Map each channel to owner shards, then tile each shard to fit one slot.
template <typename T>
struct TwoShotTileLayout {
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

// Keep each pack's partial sum in registers across all input peers, then
// write that same value to local output and every outgoing AllGather slot.
template <typename T, ReduceOp Op>
__device__ __forceinline__ void reduceAndBroadcast(
    const CollectiveChunk<T>& owned, const SimplePeerGroup& group,
    uint32_t self, uint32_t participants,
    cooperative_groups::thread_block block) {
    const uint64_t packs =
        packCountIfAligned(owned.count, owned.source, owned.destination);
    for (uint64_t index = block.thread_rank(); index < packs;
         index += block.size()) {
        auto value = ValuePack<T>::fromValues(owned.source)[index];
        for (uint32_t peer = 0; peer < participants; ++peer) {
            if (peer == self) continue;
            value = DeviceValuePackReductionTraits<T, Op>::apply(
                value,
                ValuePack<T>::fromValues(group.recvBuffer<T>(peer))[index]);
        }
        ValuePack<T>::fromValues(owned.destination)[index] = value;
        for (uint32_t peer = 0; peer < participants; ++peer) {
            if (peer != self)
                ValuePack<T>::fromValues(group.sendBuffer<T>(peer))[index] =
                    value;
        }
    }
    for (uint64_t index =
             packs * ValuePack<T>::kValueCount + block.thread_rank();
         index < owned.count; index += block.size()) {
        T value = owned.source[index];
        for (uint32_t peer = 0; peer < participants; ++peer) {
            if (peer != self)
                value = DeviceReductionTraits<T, Op>::apply(
                    value, group.recvBuffer<T>(peer)[index]);
        }
        owned.destination[index] = value;
        for (uint32_t peer = 0; peer < participants; ++peer) {
            if (peer != self) group.sendBuffer<T>(peer)[index] = value;
        }
    }
}

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveStepResult runTwoShotChannel(
    const AllReduceRequest& request, TwoShotAllReduceDeviceState* state,
    uint32_t channel, SimplePeerGroupStorage& storage,
    cooperative_groups::thread_block block) {
    // Recovery may update this host-constructed Plan between Graph replays.
    const auto& plan = state->plan;
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
    const auto buffers = SimpleBufferLayout::make(
        plan.workspace.bytes / channel_count / plan.participant_count,
        kMaxTwoShotChunkBytes);
    const TwoShotTileLayout<T> tile_layout{
        .input = input,
        .output = output,
        .channel_elements = channel_elements,
        .shard_element_capacity =
            divideRoundUp(channel_elements, plan.participant_count),
        .tile_element_capacity = buffers.chunk_bytes / sizeof(T),
    };
    const auto self = static_cast<uint32_t>(plan.self_active_index);
    SimplePeerGroup group(storage, self, plan.participant_count,
                          buffers.chunk_bytes, state->collective.timeout_ticks);
    group.bind(*state->simple, plan.workspace, plan.remotePeers(), channel,
               channel_count, block);
    auto result = group.begin(block);
    if (!result.succeeded()) return result;

    for (uint64_t tile = 0; tile < tile_layout.tileCount(); ++tile) {
        result = group.wait(SimplePeerGroup::SendAvailable, block);
        if (!result.succeeded()) return result;
        // No per-peer CTA handshake. Finish all input reads before any
        // in-place output write, including short/empty owner shards.
        for (uint32_t distance = 1; distance < plan.participant_count;
             ++distance) {
            const uint32_t peer = (self + distance) % plan.participant_count;
            const auto chunk = tile_layout.tile(peer, tile);
            copyValuesTo(chunk.source, chunk.count, block,
                         group.sendBuffer<T>(peer));
        }
        group.advance<true, false>(block);

        // The second slot can receive AllGather while the first still holds
        // ReduceScatter inputs. Consume all input peers before acknowledging.
        result = group.wait(
            SimplePeerGroup::ReceiveReady | SimplePeerGroup::SendAvailable,
            block);
        if (!result.succeeded()) return result;
        reduceAndBroadcast<T, Op>(tile_layout.tile(self, tile), group, self,
                                  plan.participant_count, block);
        group.advance<true, true>(block);

        result = group.wait(SimplePeerGroup::ReceiveReady, block);
        if (!result.succeeded()) return result;
        for (uint32_t peer = 0; peer < plan.participant_count; ++peer) {
            if (peer == self) continue;
            const auto chunk = tile_layout.tile(peer, tile);
            copyValuesTo(group.recvBuffer<T>(peer), chunk.count, block,
                         chunk.destination);
        }
        group.advance<false, true>(block);
    }
    return group.finish(block);
}

template <typename T, ReduceOp Op, CollectiveExecution Execution>
__global__ __launch_bounds__(kBlockThreads, 1) void twoShotAllReduceKernel(
    AllReduceRequest request, TwoShotAllReduceDeviceState* state) {
    const auto block = cooperative_groups::this_thread_block();
    const uint32_t channel = blockIdx.x;
    PG_ASSERT(state && state->simple);
    __shared__ SimplePeerGroupStorage storage;
    auto result = beginCollectiveInvocation<Execution>(
        &state->plan, state->collective, block, [&] {
            const auto& plan = state->plan;
            const auto& collective = state->collective;
            PG_ASSERT(collective.transfer_handle);
            return SimplePeerGroup::synchronizeView(
                plan.view_epoch, collective.timeout_ticks,
                collective.view_epoch_signals, plan.remotePeers(),
                *collective.transfer_handle, storage.failed_rank, block);
        });
    if (result.succeeded())
        result =
            runTwoShotChannel<T, Op>(request, state, channel, storage, block);
    finishCollectiveInvocation<Execution>(
        state->collective, state->plan.remotePeers(), block, result.failed_rank,
        request.failed_ranks_hint);
}

template <typename T, ReduceOp Op>
cudaError_t launchKernel(const AllReduceRequest& request,
                         TwoShotAllReduceDeviceState* state,
                         cudaStream_t stream) {
    // Request size is stable during Graph replay; membership is not. Bound
    // the grid using size alone, then select active channels on the device.
    const uint64_t bytes = request.count * sizeof(T);
    uint32_t channels = kMaxDeviceCollectiveChannels;
    while (channels > 1 && bytes / channels < 2 * kMinBytesPerChannelStep)
        channels /= 2;
    const int threads = bytes <= 4096 ? 128 : kBlockThreads;
    if (channels == 1) {
        twoShotAllReduceKernel<T, Op, CollectiveExecution::SingleCTA>
            <<<1, threads, 0, stream>>>(request, state);
    } else {
        twoShotAllReduceKernel<T, Op, CollectiveExecution::MultiCTA>
            <<<channels, threads, 0, stream>>>(request, state);
    }
    return cudaGetLastError();
}

template <typename T>
cudaError_t launchForType(const AllReduceRequest& request,
                          TwoShotAllReduceDeviceState* state,
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

cudaError_t launchTwoShotAllReduceKernel(const AllReduceRequest& request,
                                         TwoShotAllReduceDeviceState* state,
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
