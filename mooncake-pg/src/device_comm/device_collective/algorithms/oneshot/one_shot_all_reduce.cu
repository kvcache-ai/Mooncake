#include "device_comm/device_collective/algorithms/oneshot/one_shot_types.cuh"

#include <cstdint>
#include <type_traits>

#include <cooperative_groups.h>

#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/ll/ll_primitives.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"
#include "device_comm/device_utils/device_assert.cuh"

namespace mooncake {
namespace {

inline constexpr int kOneShotThreads = 512;

class OneShotExchangeControl {
   public:
    __device__ __forceinline__ OneShotExchangeControl(
        const OneShotAllReducePlan& plan, const LLState& ll, uint32_t lane_index)
        : plan_(plan),
          ll_(ll),
          lane_(plan.transfer_handle->lane(lane_index)),
          signals_(lane_, ll, plan.timeout_ticks) {}

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    prepareView(const uint64_t* view_epoch_signals,
                cooperative_groups::thread_block block) const {
        InGroupRank peers[kMaxNumRanks];
        uint32_t peer_count = 0;
        for (uint32_t peer = 0; peer < plan_.participant_count; ++peer) {
            if (peer == plan_.self_active_index) continue;
            const auto rank = plan_.peers[peer];
            const auto prepared = signals_.preparePeer(
                rank, plan_.peer_buffer_offsets[peer], plan_.layout.bufferBytes(),
                plan_.view_epoch, block);
            if (!prepared.succeeded()) return prepared;
            peers[peer_count++] = rank;
        }
        const auto result = synchronizeCollectiveViewEpoch(
            plan_.view_epoch, plan_.timeout_ticks, view_epoch_signals,
            ll_.peers, peers, peer_count, lane_, block);
        return {result.failed_rank};
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult begin(
        uint64_t sequence, uint64_t word_count,
        cooperative_groups::thread_block block) const {
        // StrongStream has released the local workspace. Arbitrary previous
        // payload could look like an LL tag, so clear before publishing ready.
        // Wrap signals also restart with the invocation's chunk sequence.
        signals_.resetWrap(block);
        clearPayload(word_count, block);
        // Peers may send only after our LL slots are initialized and no other
        // local collective is using them. Our sends wait for the same grant.
        return {barrier(LLControlWord::BufferReady, sequence, block)};
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult wrap(
        uint64_t sequence, uint64_t word_count,
        cooperative_groups::thread_block block) const {
        clearPayload(word_count, block);
        return {barrier(LLControlWord::WrapReady, sequence, block)};
    }

   private:
    [[nodiscard]] __device__ __forceinline__ InGroupRank
    barrier(LLControlWord kind, uint64_t sequence,
            cooperative_groups::thread_block block) const {
        for (uint32_t peer = 0; peer < plan_.participant_count; ++peer) {
            if (peer == plan_.self_active_index) continue;
            const auto target = plan_.peers[peer];
            if (signals_.signal(target, kind, sequence, block) !=
                TransferResult::Succeeded)
                return target;
        }
        for (uint32_t peer = 0; peer < plan_.participant_count; ++peer) {
            if (peer == plan_.self_active_index) continue;
            const auto target = plan_.peers[peer];
            if (!signals_.wait(target, kind, sequence, block))
                return target;
        }
        return kInvalidInGroupRank;
    }

    __device__ __forceinline__ void clearPayload(
        uint64_t word_count, cooperative_groups::thread_block block) const {
        // Only active senders and the used prefix of each slot need clearing.
        const uint32_t peer_count = plan_.participant_count - 1;
        const uint64_t work = uint64_t{kOneShotSlots} * peer_count * word_count;
        for (uint64_t index = block.thread_rank(); index < work;
             index += block.size()) {
            const uint64_t slot_peer = index / word_count;
            uint32_t peer = static_cast<uint32_t>(slot_peer % peer_count);
            peer += peer >= plan_.self_active_index;
            const uint32_t slot = static_cast<uint32_t>(slot_peer / peer_count);
            const auto sender = plan_.peers[peer];
            auto* packet = reinterpret_cast<uint64_t*>(plan_.buffer_ptr) +
                           plan_.layout.packetIndex(slot, sender,
                                                    index % word_count);
            *packet = 0;
        }
        block.sync();
    }

    const OneShotAllReducePlan& plan_;
    const LLState& ll_;
    TransferLane lane_;
    LLSignalPrimitives signals_;
};

struct OneShotWordRange {
    uint64_t begin = 0;
    uint64_t end = 0;
};

// Each chunk sends input to EVERY peer before reducing, and finishes every
// receive before advancing to the next chunk.
// Sending n+2 therefore proves every peer completed n before sending n+1:
// two exchange slots suffice without consumed ACKs within a call. Separate
// calls acquire the workspace through the BufferReady handshake above.
class OneShotExchange {
   public:
    __device__ __forceinline__
    OneShotExchange(const OneShotAllReducePlan& plan, const LLState& ll,
                    uint64_t sequence, OneShotWordRange words)
        : plan_(plan),
          slot_(sequence % kOneShotSlots),
          words_(words),
          ll_(ll, sequence, plan.timeout_ticks) {}

    template <typename LoadWord>
    __device__ __forceinline__ void sendAll(
        LoadWord load_word, cooperative_groups::thread_block block) const {
        const uint64_t word_count = words_.end - words_.begin;
        const uint64_t work = word_count * (plan_.participant_count - 1);
        for (uint64_t index = block.thread_rank(); index < work;
             index += block.size()) {
            uint32_t peer = static_cast<uint32_t>(index / word_count);
            peer += peer >= plan_.self_active_index;
            const uint64_t word = index % word_count;
            const uint64_t remote_offset =
                plan_.peer_buffer_offsets[peer] +
                plan_.layout.packetIndex(slot_, plan_.self_rank, words_.begin) *
                    sizeof(uint64_t);
            auto* packets = ll_.remotePayload(plan_.peers[peer], remote_offset);
            ll_.store(packets + word, load_word(words_.begin + word));
        }
        // Every outgoing input read completes before any in-place output
        // write. Packet stores themselves publish the data.
        block.sync();
    }

    [[nodiscard]] __device__ __forceinline__ bool readReceived(
        uint32_t peer, uint64_t word, uint32_t& value) const {
        const auto sender = plan_.peers[peer];
        const auto* packet = reinterpret_cast<const uint64_t*>(plan_.buffer_ptr) +
                             plan_.layout.packetIndex(slot_, sender, word);
        return ll_.load(packet, value);
    }

    // Converge per-thread timeouts before advancing or completing the call.
    [[nodiscard]] __device__ __forceinline__ InGroupRank finishReads(
        InGroupRank failed, cooperative_groups::thread_block block) const {
        __shared__ int block_failed;
        if (block.thread_rank() == 0) block_failed = kInvalidInGroupRank;
        block.sync();
        if (failed != kInvalidInGroupRank)
            atomicCAS(&block_failed, kInvalidInGroupRank, failed);
        block.sync();
        return block_failed;
    }

   private:
    const OneShotAllReducePlan& plan_;
    uint32_t slot_;
    OneShotWordRange words_;
    LLPrimitives ll_;
};

struct PrepareOneShotCollective {
    OneShotAllReduceDeviceState* state;
    uint64_t word_count;

    [[nodiscard]] __device__ __forceinline__ CollectivePreparationResult
    operator()(const OneShotAllReducePlan& plan,
               const uint64_t* view_epoch_signals, uint32_t lane_index,
               cooperative_groups::thread_block block) const {
        PG_DEVICE_ASSERT(plan.participant_count > 0 &&
                         plan.participant_count <= kMaxNumRanks);
        if (plan.participant_count == 1) return {};

        PG_DEVICE_ASSERT(state->ll);
        const OneShotExchangeControl control(plan, *state->ll, lane_index);
        const auto prepared = control.prepareView(view_epoch_signals, block);
        if (!prepared.succeeded()) return {.failed_rank = prepared.failed_rank};
        // Empty calls neither use the workspace nor advance its ready signal.
        if (word_count == 0) return {};

        const uint64_t sequence = state->ll->buffer_ready_sequence + 1;
        PG_DEVICE_ASSERT(sequence != 0);
        const uint64_t chunk_words = plan.chunk_bytes / sizeof(uint32_t);
        const auto result = control.begin(
            sequence, word_count < chunk_words ? word_count : chunk_words, block);
        if (result.succeeded() && block.thread_rank() == 0)
            state->ll->buffer_ready_sequence = sequence;
        return {.failed_rank = result.failed_rank};
    }
};

struct DrainOneShotTransfers {
    const OneShotAllReducePlan* plan;
    const LLState* ll;

    __device__ __forceinline__ void operator()() const {
        if (plan->participant_count <= 1) return;
        InGroupRank peers[kMaxNumRanks];
        uint32_t peer_count = 0;
        for (uint32_t peer = 0; peer < plan->participant_count; ++peer) {
            if (peer != plan->self_active_index) {
                peers[peer_count++] = plan->peers[peer];
            }
        }
        drainCollectiveTransfers(*plan->transfer_handle, ll->peers, peers,
                                 peer_count);
    }
};

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveStepResult
runOneShotChunk(CollectiveChunk<T> chunk, const OneShotAllReducePlan& plan,
                const LLState& ll, uint64_t sequence, OneShotWordRange words,
                cooperative_groups::thread_block block) {
    InGroupRank failed_rank = kInvalidInGroupRank;
    using Word = ValueWord<T>;
    const PackedReduction<T, Op> reduce;
    const ValueWordSource<T> input{chunk.source, chunk.count};
    const OneShotExchange exchange(plan, ll, sequence, words);

    // Publish every input before consuming any received value. In-place
    // output is safe because sendAll() completes every local source read.
    exchange.sendAll(input, block);
    // Seed each reduction from the first peer, so all operations share the
    // same fold without an identity value.
    if (words.end - words.begin <= 128) {
        // For small messages, load peers concurrently within an eight-lane
        // tile. Lane zero folds values in the same peer order as the scalar
        // path, so packed half/bfloat16 rounding is unchanged.
        const auto tile = cooperative_groups::tiled_partition<8>(block);
        for (uint64_t word = words.begin + tile.meta_group_rank();
             word < words.end; word += tile.meta_group_size()) {
            uint32_t reduced = 0;
            for (uint32_t base = 0; base < plan.participant_count; base += 8) {
                const uint32_t peer = base + tile.thread_rank();
                uint32_t value = 0;
                InGroupRank failed = kInvalidInGroupRank;
                if (peer < plan.participant_count) {
                    if (peer == plan.self_active_index) {
                        value = input(word);
                    } else if (!exchange.readReceived(peer, word, value)) {
                        failed = plan.peers[peer];
                    }
                }
#pragma unroll
                for (uint32_t lane = 0; lane < 8; ++lane) {
                    const auto received = tile.shfl(value, lane);
                    const auto peer_failed = tile.shfl(failed, lane);
                    if (peer_failed != kInvalidInGroupRank)
                        failed_rank = peer_failed;
                    if (tile.thread_rank() == 0 &&
                        base + lane < plan.participant_count)
                        reduced = base + lane == 0 ? received
                                                  : reduce(reduced, received);
                }
                if (failed_rank != kInvalidInGroupRank) break;
            }
            if (failed_rank != kInvalidInGroupRank) break;
            if (tile.thread_rank() == 0) {
                const uint64_t element = word * Word::kValueCount;
                Word::store(chunk.destination + element, chunk.count - element,
                            reduced);
            }
        }
    } else {
        for (uint64_t word = words.begin + block.thread_rank();
             word < words.end; word += block.size()) {
            uint32_t reduced = 0;
            for (uint32_t peer = 0; peer < plan.participant_count; ++peer) {
                uint32_t value;
                if (peer == plan.self_active_index) {
                    value = input(word);
                } else if (!exchange.readReceived(peer, word, value)) {
                    failed_rank = plan.peers[peer];
                    break;
                }
                reduced = peer == 0 ? value : reduce(reduced, value);
            }
            if (failed_rank != kInvalidInGroupRank) break;
            const uint64_t element = word * Word::kValueCount;
            Word::store(chunk.destination + element, chunk.count - element,
                        reduced);
        }
    }
    return {exchange.finishReads(failed_rank, block)};
}

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ CollectiveStepResult
runOneShotChannel(const OneShotAllReduceKernelArgs& request,
                   const OneShotAllReducePlan& plan, const LLState& ll,
                   cooperative_groups::thread_block block) {
    const uint64_t chunk_elements = plan.chunk_bytes / sizeof(T);
    // One exchange can split its words across CTAs. Multiple exchanges use
    // one CTA to finish all local receives before advancing the chunk sequence,
    // without a grid barrier that could wait for unscheduled CTAs. Inactive
    // CTAs still join common completion.
    const uint32_t channel_count =
        request.count <= chunk_elements ? gridDim.x : 1;
    const uint32_t channel = blockIdx.x;
    if (channel >= channel_count) return {};

    using Word = ValueWord<T>;
    // BufferReady grants freshly cleared slots for this invocation.
    uint64_t sequence = 1;
    for (uint64_t offset = 0; offset < request.count;) {
        if (static_cast<uint32_t>(sequence) == 0) {
            const OneShotExchangeControl control(plan, ll, channel);
            const auto prepared = control.wrap(
                sequence, plan.chunk_bytes / sizeof(uint32_t), block);
            if (!prepared.succeeded()) return prepared;
            // Tag zero denotes an empty packet. The barrier permits reusing
            // the previous odd slot when skipping it.
            ++sequence;
        }

        const uint64_t remaining = request.count - offset;
        const CollectiveChunk<T> chunk{
            .source = static_cast<const T*>(request.send_buffer) + offset,
            .destination = static_cast<T*>(request.recv_buffer) + offset,
            .count = remaining < chunk_elements ? remaining : chunk_elements,
        };
        const uint64_t word_count =
            (chunk.count + Word::kValueCount - 1) / Word::kValueCount;
        const OneShotWordRange words{word_count * channel / channel_count,
                                     word_count * (channel + 1) / channel_count};
        const auto result =
            runOneShotChunk<T, Op>(chunk, plan, ll, sequence, words, block);
        if (!result.succeeded()) return result;
        offset += chunk.count;
        ++sequence;
    }
    return {};
}

template <typename T, ReduceOp Op, typename Scope>
__global__ __launch_bounds__(kOneShotThreads) void oneShotAllReduceKernel(
    OneShotAllReduceKernelArgs request, OneShotAllReduceDeviceState* state) {
    const auto block = cooperative_groups::this_thread_block();
    const uint32_t channel = blockIdx.x;
    PG_DEVICE_ASSERT(state);
    auto* const invocation = state->invocation_state;
    auto* const control_mailbox = state->control_mailbox;
    PG_DEVICE_ASSERT(invocation);
    PG_DEVICE_ASSERT(control_mailbox);
    const auto* const plan_slot = &state->plan;
    const DrainOneShotTransfers drain_transfers{&plan_slot->plan, state->ll};

    // 1. Apply control updates, synchronize the View and acquire LL workspace.
    const uint64_t word_count =
        request.count / ValueWord<T>::kValueCount +
        (request.count % ValueWord<T>::kValueCount != 0);
    const auto preparation = prepareCollectiveInvocation<Scope>(
        &state->plan, state->view_epoch_signals, invocation, control_mailbox,
        channel, PrepareOneShotCollective{state, word_count}, block);
    if (!preparation.succeeded()) {
        completeChannel(invocation, control_mailbox, drain_transfers, block,
                        preparation.failed_rank, request.failed_ranks_hint);
        return;
    }

    PG_DEVICE_ASSERT(plan_slot->status == DevicePlanStatus::Ready);
    const auto& plan = plan_slot->plan;
    PG_DEVICE_ASSERT(plan.chunk_bytes > 0 &&
                     plan.chunk_bytes % sizeof(uint32_t) == 0 &&
                     plan.chunk_bytes <= plan.layout.slot_capacity);
    PG_DEVICE_ASSERT(gridDim.x <= kMaxOneShotChannels);

    // 2. Publish each chunk to every peer.
    // 3. Reduce readable LL packets into the chunk's output range. Outgoing
    //    source reads finish before output writes, including in-place calls.
    CollectiveStepResult result;
    if (plan.participant_count == 1) {
        if (request.count && request.send_buffer != request.recv_buffer) {
            copyValuesTo<Scope>(static_cast<const T*>(request.send_buffer),
                                request.count, block,
                                static_cast<T*>(request.recv_buffer));
        }
    } else if (request.count != 0) {
        result = runOneShotChannel<T, Op>(request, plan, *state->ll, block);
    }

    // 4. The common completion path waits for every CTA to stop using Plan.
    //    On failure it drains asynchronous transfers before notifying the host
    //    and consuming the pinned recovery update.
    if constexpr (std::is_same_v<Scope, SingleCta>) {
        if (result.succeeded()) return;
    }
    completeChannel(invocation, control_mailbox, drain_transfers, block,
                    result.failed_rank, request.failed_ranks_hint);
}

template <typename T, ReduceOp Op>
cudaError_t launchKernel(const OneShotAllReduceKernelArgs& request,
                         OneShotAllReduceDeviceState* state, dim3 grid,
                         int threads, cudaStream_t stream) {
    if (grid.x == 1) {
        oneShotAllReduceKernel<T, Op, SingleCta>
            <<<grid, threads, 0, stream>>>(request, state);
    } else {
        oneShotAllReduceKernel<T, Op, MultiCta>
            <<<grid, threads, 0, stream>>>(request, state);
    }
    return cudaGetLastError();
}

template <typename T>
cudaError_t launchReduction(const OneShotAllReduceKernelArgs& request,
                            OneShotAllReduceDeviceState* state, dim3 grid,
                            int threads, cudaStream_t stream) {
    switch (request.op) {
        case ReduceOp::Sum:
            return launchKernel<T, ReduceOp::Sum>(request, state, grid, threads,
                                                  stream);
        case ReduceOp::Product:
            return launchKernel<T, ReduceOp::Product>(request, state, grid,
                                                      threads, stream);
        case ReduceOp::Min:
            return launchKernel<T, ReduceOp::Min>(request, state, grid, threads,
                                                  stream);
        case ReduceOp::Max:
            return launchKernel<T, ReduceOp::Max>(request, state, grid, threads,
                                                  stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace

cudaError_t launchOneShotAllReduceKernel(
    const OneShotAllReduceKernelArgs& request,
    OneShotAllReduceDeviceState* state, cudaStream_t stream) {
    if (!isOneShotAllReduceCombinationSupported(request.datatype, request.op))
        return cudaErrorInvalidValue;
    const uint64_t bytes = request.count * elementSize(request.datatype);
    // Size is fixed in a captured call. Keep tiny calls on one CTA; each extra
    // CTA owns a contiguous word range when the Plan permits one exchange.
    const int threads = bytes <= 512    ? 128
                        : bytes <= 1024 ? 256
                                        : kOneShotThreads;
    const uint64_t requested_blocks =
        bytes <= 4096 ? 1 : bytes / 2048 + (bytes % 2048 != 0);
    const dim3 grid(requested_blocks < kMaxOneShotChannels
                        ? requested_blocks
                        : kMaxOneShotChannels);
    switch (request.datatype) {
        case DataType::Float16:
            return launchReduction<__half>(request, state, grid, threads,
                                           stream);
        case DataType::Bfloat16:
            return launchReduction<__nv_bfloat16>(request, state, grid,
                                                  threads, stream);
        case DataType::Float32:
            return launchReduction<float>(request, state, grid, threads,
                                          stream);
        case DataType::Int32:
            return launchReduction<int32_t>(request, state, grid, threads,
                                            stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace mooncake
