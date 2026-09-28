#include "device_comm/device_collective/algorithms/oneshot/one_shot_types.cuh"

#include <cstdint>

#include <cooperative_groups.h>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/ll/ll_primitives.cuh"
#include "device_comm/device_collective/protocols/ll/ll_value_pack.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"
#include "pg_assert.h"

namespace mooncake {
namespace {

inline constexpr int kOneShotTileSize = 8;

// Send to every peer and finish all receives before advancing LL progress.
template <typename T>
class OneShotExchange {
    using Pack = LLValuePack<T>;

   public:
    __device__ __forceinline__ OneShotExchange(const OneShotAllReducePlan& plan,
                                               LLState& ll, uint32_t channel,
                                               uint64_t timeout_ticks)
        : plan_(plan),
          ll_(ll),
          channel_(channel),
          timeout_ticks_(timeout_ticks) {}

    [[nodiscard]] __device__ __forceinline__ uint64_t
    chunkElementCapacity() const {
        return OneShotBufferLayout::kChunkBytes / sizeof(T);
    }

    [[nodiscard]] __device__ __forceinline__ static uint64_t packCount(
        uint64_t count) {
        return count / Pack::kValueCount + (count % Pack::kValueCount != 0);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    begin(cooperative_groups::thread_block block) {
        const auto result =
            LLControl(ll_, channel_, timeout_ticks_)
                .begin(plan_.view_epoch, plan_.remotePeers(),
                       plan_.packets + plan_.layout.packetIndex(channel_, 0, 0),
                       plan_.layout.channelPacketBytes(), block);
        sequence_ = ll_.progress[channel_].next_sequence;
        return result;
    }

    // Before X sends n+2, it receives Y's n+1, so Y has consumed X's n.
    // Empty/local-only calls consume no sequence, preserving this dependency.
    __device__ __forceinline__ void sendAll(
        CollectiveChunk<T> chunk,
        cooperative_groups::thread_block block) const {
        const uint64_t pack_count = packCount(chunk.count);
        const auto remote_peers = plan_.remotePeers();
        const uint64_t work = pack_count * remote_peers.size();
        const LLPacketOps packets(sequence_, timeout_ticks_);
        for (uint64_t index = block.thread_rank(); index < work;
             index += block.size()) {
            const auto& peer = remote_peers.atIndex(index / pack_count);
            const uint64_t pack_index = index % pack_count;
            const auto value =
                Pack::load(chunk.source, chunk.count, pack_index);
            const uint64_t remote_offset =
                peer.workspace_offset +
                plan_.layout.packetIndex(
                    channel_, sequence_ % OneShotBufferLayout::kSlots,
                    plan_.self_rank, pack_index * Pack::kPayloadWords) *
                    LLPacket::kStorageBytes;
            auto* destination =
                static_cast<LLPacket*>(ll_.bindings.transfer_handle->remotePtr(
                    peer.global_rank, remote_offset));
            packets.storePack<Pack>(destination, value);
        }
        block.sync();
    }

    [[nodiscard]] __device__ __forceinline__ bool readReceived(
        uint32_t active_index, uint64_t pack_index,
        typename Pack::Value& value) const {
        const auto& peer = plan_.remoteParticipant(active_index);
        const auto* packet =
            plan_.packets +
            plan_.layout.packetIndex(
                channel_, sequence_ % OneShotBufferLayout::kSlots,
                peer.in_group_rank, pack_index * Pack::kPayloadWords);
        return LLPacketOps(sequence_, timeout_ticks_)
            .loadPack<Pack>(packet, value);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    finish(InGroupRank failed, cooperative_groups::thread_block block) {
        return LLControl(ll_, channel_, timeout_ticks_)
            .finish(sequence_, failed, block);
    }

   private:
    const OneShotAllReducePlan& plan_;
    LLState& ll_;
    uint32_t channel_;
    uint64_t timeout_ticks_;
    uint64_t sequence_ = 0;
};

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ InGroupRank
reducePeersScalar(CollectiveChunk<T> chunk, const OneShotAllReducePlan& plan,
                  const OneShotExchange<T>& exchange, uint64_t pack_count,
                  cooperative_groups::thread_block block) {
    using Pack = LLValuePack<T>;
    for (uint64_t pack_index = block.thread_rank(); pack_index < pack_count;
         pack_index += block.size()) {
        typename Pack::Value reduced = 0;
        for (uint32_t peer = 0; peer < plan.participant_count; ++peer) {
            typename Pack::Value value;
            if (peer == plan.self_active_index) {
                value = Pack::load(chunk.source, chunk.count, pack_index);
            } else if (!exchange.readReceived(peer, pack_index, value)) {
                return plan.remoteParticipant(peer).in_group_rank;
            }
            // Start with the first participant; no identity value is needed.
            reduced =
                peer == 0 ? value : Pack::template reduce<Op>(reduced, value);
        }
        Pack::store(chunk.destination, chunk.count, pack_index, reduced);
    }
    return kInvalidInGroupRank;
}

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ InGroupRank
reducePeersTiled(CollectiveChunk<T> chunk, const OneShotAllReducePlan& plan,
                 const OneShotExchange<T>& exchange, uint64_t pack_count,
                 cooperative_groups::thread_block block) {
    using Pack = LLValuePack<T>;
    const auto tile =
        cooperative_groups::tiled_partition<kOneShotTileSize>(block);
    for (uint64_t pack_index = tile.meta_group_rank(); pack_index < pack_count;
         pack_index += tile.meta_group_size()) {
        typename Pack::Value reduced = 0;
        for (uint32_t base = 0; base < plan.participant_count;
             base += kOneShotTileSize) {
            const uint32_t peer = base + tile.thread_rank();
            typename Pack::Value value = 0;
            InGroupRank failed = kInvalidInGroupRank;
            if (peer < plan.participant_count) {
                if (peer == plan.self_active_index) {
                    value = Pack::load(chunk.source, chunk.count, pack_index);
                } else if (!exchange.readReceived(peer, pack_index, value)) {
                    failed = plan.remoteParticipant(peer).in_group_rank;
                }
            }
            const auto failures = tile.ballot(failed != kInvalidInGroupRank);
            if (failures) return tile.shfl(failed, __ffs(failures) - 1);
#pragma unroll
            for (uint32_t lane = 0; lane < kOneShotTileSize; ++lane) {
                const uint32_t low =
                    tile.shfl(static_cast<uint32_t>(value), lane);
                typename Pack::Value received = low;
                if constexpr (Pack::kPayloadWords == 2) {
                    const uint32_t high =
                        tile.shfl(static_cast<uint32_t>(value >> 32), lane);
                    received |= static_cast<typename Pack::Value>(high) << 32;
                }
                if (tile.thread_rank() == 0 &&
                    base + lane < plan.participant_count)
                    reduced = base + lane == 0 ? received
                                               : Pack::template reduce<Op>(
                                                     reduced, received);
            }
        }
        if (tile.thread_rank() == 0) {
            Pack::store(chunk.destination, chunk.count, pack_index, reduced);
        }
    }
    return kInvalidInGroupRank;
}

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveStepResult runOneShotChunks(
    CollectiveChunk<T> values, const OneShotAllReducePlan& plan,
    OneShotExchange<T>& exchange, cooperative_groups::thread_block block) {
    uint64_t offset = 0;
    while (offset < values.count) {
        const auto ready = exchange.begin(block);
        if (!ready.succeeded()) return ready;
        const uint64_t remaining = values.count - offset;
        const CollectiveChunk<T> chunk{
            .source = values.source + offset,
            .destination = values.destination + offset,
            .count = remaining < exchange.chunkElementCapacity()
                         ? remaining
                         : exchange.chunkElementCapacity(),
        };
        exchange.sendAll(chunk, block);
        const uint64_t pack_count = OneShotExchange<T>::packCount(chunk.count);
        // Fill each tile with peers and give it only one pack.
        const bool tiled = plan.participant_count >= kOneShotTileSize &&
                           pack_count <= block.size() / kOneShotTileSize;
        const auto failed = tiled
                                ? reducePeersTiled<T, Op>(chunk, plan, exchange,
                                                          pack_count, block)
                                : reducePeersScalar<T, Op>(
                                      chunk, plan, exchange, pack_count, block);
        const auto result = exchange.finish(failed, block);
        if (!result.succeeded()) return result;
        offset += chunk.count;
    }
    return {};
}

template <typename T, ReduceOp Op, CollectiveExecution Execution>
[[nodiscard]] __device__ __forceinline__ CollectiveStepResult
runOneShot(const AllReduceRequest& request, OneShotAllReduceDeviceState* state,
           cooperative_groups::thread_block block) {
    const auto& plan = state->plan;
    PG_ASSERT(plan.participant_count > 0 &&
              plan.participant_count <= kMaxNumRanks);
    if (request.count == 0) return {};
    uint64_t offset = 0;
    uint64_t count = request.count;
    uint32_t channel = 0;
    if constexpr (Execution == CollectiveExecution::MultiCTA) {
        channel = blockIdx.x;
        // Split on pack boundaries so CTAs never share an pack.
        const uint64_t packs = OneShotExchange<T>::packCount(request.count);
        const uint64_t capacity =
            ((packs + gridDim.x - 1) / gridDim.x) * LLValuePack<T>::kValueCount;
        offset = uint64_t{channel} * capacity;
        if (offset >= request.count) return {};
        count = request.count - offset;
        if (count > capacity) count = capacity;
    }
    const CollectiveChunk<T> values{
        .source = static_cast<const T*>(request.send_buffer) + offset,
        .destination = static_cast<T*>(request.recv_buffer) + offset,
        .count = count,
    };
    if (plan.participant_count == 1) {
        if (values.source != values.destination)
            copyValuesTo(values.source, values.count, block,
                         values.destination);
        return {};
    }

    PG_ASSERT(state->ll);
    OneShotExchange<T> exchange(plan, *state->ll, channel,
                                state->collective.timeout_ticks);
    return runOneShotChunks<T, Op>(values, plan, exchange, block);
}

template <typename T, ReduceOp Op, CollectiveExecution Execution>
__global__ __launch_bounds__(kOneShotMaxThreads) void oneShotAllReduceKernel(
    AllReduceRequest request, OneShotAllReduceDeviceState* state) {
    const auto block = cooperative_groups::this_thread_block();
    PG_ASSERT(state);
    auto result = beginCollectiveInvocation<Execution>(
        &state->plan, state->collective, block);
    if (result.succeeded())
        result = runOneShot<T, Op, Execution>(request, state, block);
    finishCollectiveInvocation<Execution>(
        state->collective, state->plan.remotePeers(), block, result.failed_rank,
        request.failed_ranks_hint);
}

template <typename T, ReduceOp Op>
cudaError_t launchKernel(const AllReduceRequest& request,
                         OneShotAllReduceDeviceState* state, int threads,
                         uint32_t channels, cudaStream_t stream) {
    if (channels > 1) {
        oneShotAllReduceKernel<T, Op, CollectiveExecution::MultiCTA>
            <<<channels, threads, 0, stream>>>(request, state);
    } else {
        oneShotAllReduceKernel<T, Op, CollectiveExecution::SingleCTA>
            <<<1, threads, 0, stream>>>(request, state);
    }
    return cudaGetLastError();
}

template <typename T>
cudaError_t launchForType(const AllReduceRequest& request,
                          OneShotAllReduceDeviceState* state, int threads,
                          uint32_t channels, cudaStream_t stream) {
    switch (request.op) {
        case ReduceOp::Sum:
            return launchKernel<T, ReduceOp::Sum>(request, state, threads,
                                                  channels, stream);
        case ReduceOp::Product:
            return launchKernel<T, ReduceOp::Product>(request, state, threads,
                                                      channels, stream);
        case ReduceOp::Min:
            return launchKernel<T, ReduceOp::Min>(request, state, threads,
                                                  channels, stream);
        case ReduceOp::Max:
            return launchKernel<T, ReduceOp::Max>(request, state, threads,
                                                  channels, stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace

cudaError_t launchOneShotAllReduceKernel(const AllReduceRequest& request,
                                         OneShotAllReduceDeviceState* state,
                                         cudaStream_t stream) {
    if (!isDeviceAllReduceCombinationSupported(request.datatype, request.op))
        return cudaErrorInvalidValue;
    const uint64_t bytes = request.count * elementSize(request.datatype);
    // Size is fixed in a captured call; smaller blocks suit tiny messages.
    const int threads = bytes <= 64    ? 128
                        : bytes <= 128 ? 256
                                       : kOneShotMaxThreads;
    const uint32_t channels = bytes <= 2048 ? 1 : kOneShotMaxChannels;
    switch (request.datatype) {
        case DataType::Float16:
            return launchForType<__half>(request, state, threads, channels,
                                         stream);
        case DataType::Uint8:
            return launchForType<uint8_t>(request, state, threads, channels,
                                          stream);
        case DataType::Int8:
            return launchForType<int8_t>(request, state, threads, channels,
                                         stream);
        case DataType::Int16:
            return launchForType<int16_t>(request, state, threads, channels,
                                          stream);
        case DataType::Int64:
            return launchForType<int64_t>(request, state, threads, channels,
                                          stream);
        case DataType::Bfloat16:
            return launchForType<__nv_bfloat16>(request, state, threads,
                                                channels, stream);
        case DataType::Float32:
            return launchForType<float>(request, state, threads, channels,
                                        stream);
        case DataType::Float64:
            return launchForType<double>(request, state, threads, channels,
                                         stream);
        case DataType::Int32:
            return launchForType<int32_t>(request, state, threads, channels,
                                          stream);
        case DataType::Bool:
            return launchForType<bool>(request, state, threads, channels,
                                       stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace mooncake
