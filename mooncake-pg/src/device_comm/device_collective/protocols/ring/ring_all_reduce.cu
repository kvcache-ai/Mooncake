#include "device_comm/device_collective/protocols/ring/ring_types.cuh"

#include <cstdint>

#include <cooperative_groups.h>

#include "device_comm/device_assert.cuh"
#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/ring/ring_primitives.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"

namespace mooncake {
namespace {

template <typename T, ReduceOp Op>
[[nodiscard]] __device__ __forceinline__ RingStepResult runRingTile(
    const RingAllReducePlan& plan,
    const RingPrimitives<T, Op>& primitives,
    const RingTileLayout& layout, uint64_t tile_index,
    uint64_t* payload_slot_sequences,
    cooperative_groups::thread_block block) {
    const uint64_t ring_steps = plan.participant_count - 1;
    // Reduce-scatter and all-gather share one alternating payload-slot
    // schedule for the whole tile traversal.
    RingPayloadSlotSchedule slot_schedule{
        .next_sequences = payload_slot_sequences,
    };

    // Start reduce-scatter by injecting this rank's contribution for its own
    // shard into the ring.
    const uint32_t first_shard_index = ringShardAtDistance(
        plan.self_active_index, plan.participant_count, 0);
    const RingTile first_tile = layout.tile(first_shard_index, tile_index);
    auto curr_slot = slot_schedule.next();
    auto result = primitives.send(first_tile, curr_slot, block);
    if (!result.succeeded()) return result;

    // Every rank follows the same logical slot schedule. After the initial
    // send, curr_slot also identifies the payload expected on this rank's
    // distinct incoming edge.
    RingPayloadSlot next_slot;

    // Each intermediate reduce-scatter step receives a partially reduced
    // shard, adds this rank's contribution, and forwards the new partial.
    for (uint64_t distance = 1; distance < ring_steps; ++distance) {
        const uint32_t shard_index = ringShardAtDistance(
            plan.self_active_index, plan.participant_count, distance);
        const RingTile tile = layout.tile(shard_index, tile_index);
        next_slot = slot_schedule.next();
        result =
            primitives.recvReduceSend(tile, curr_slot, next_slot, block);
        if (!result.succeeded()) return result;
        curr_slot = next_slot;
    }

    // The final reduce-scatter receive completes one shard. Store the reduced
    // value in local output and forward it as the first all-gather send.
    const uint32_t completed_shard_index = ringShardAtDistance(
        plan.self_active_index, plan.participant_count, ring_steps);
    const RingTile completed_tile =
        layout.tile(completed_shard_index, tile_index);
    next_slot = slot_schedule.next();
    result = primitives.recvReduceCopySend(
        completed_tile, curr_slot, next_slot, block);
    if (!result.succeeded()) return result;
    curr_slot = next_slot;

    // The remaining all-gather steps receive fully reduced shards, copy them
    // into local output, and forward them without another reduction.
    for (uint64_t distance = 0; distance + 1 < ring_steps; ++distance) {
        const uint32_t shard_index = ringShardAtDistance(
            plan.self_active_index, plan.participant_count, distance);
        const RingTile tile = layout.tile(shard_index, tile_index);
        next_slot = slot_schedule.next();
        result = primitives.recvCopySend(tile, curr_slot, next_slot, block);
        if (!result.succeeded()) return result;
        curr_slot = next_slot;
    }

    // Receive the last fully reduced shard without forwarding it, then wait
    // until the final outstanding send has been consumed.
    const uint32_t last_shard_index = ringShardAtDistance(
        plan.self_active_index, plan.participant_count, ring_steps - 1);
    const RingTile last_tile = layout.tile(last_shard_index, tile_index);
    return primitives.recvCopyAndDrain(last_tile, curr_slot, block);
}

template <typename T, ReduceOp Op>
__global__ void ringAllReduceKernel(
    RingAllReduceKernelArgs request,
    RingAllReduceDeviceState* state) {
    const auto block = cooperative_groups::this_thread_block();
    const uint32_t channel = blockIdx.x;
    PG_DEVICE_ASSERT(state);
    const auto* const plan_slot = &state->plan;

    // Recovery may update this host-constructed Plan between Graph replays.
    PG_DEVICE_ASSERT(plan_slot->status == DevicePlanStatus::Ready);
    const auto plan = plan_slot->plan;
    if (request.count == 0) {
        completeChannel(plan.invocation_state, plan.recovery_mailbox, block);
        return;
    }

    const uint32_t channel_count = gridDim.x;
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
        completeChannel(plan.invocation_state, plan.recovery_mailbox, block);
        return;
    }
    block.sync();

    // Ring binds one algorithm channel to one fixed transfer lane. The current
    // Plan supplies the buffer, optional staging and signal as offsets in
    // the DTS arena.
    const auto* const transfer_handle = plan.transfer_handle;
    PG_DEVICE_ASSERT(transfer_handle);
    PG_DEVICE_ASSERT(transfer_handle->local_region);
    const uint64_t local_region_size = transfer_handle->local_region_size;

    PG_DEVICE_ASSERT(plan.buffer_offset <= local_region_size);
    PG_DEVICE_ASSERT(plan.buffer_size >= kRingBufferBytes);
    PG_DEVICE_ASSERT(plan.buffer_size <=
                     local_region_size - plan.buffer_offset);
    if (plan.staging_size != 0) {
        PG_DEVICE_ASSERT(plan.staging_offset <= local_region_size);
        PG_DEVICE_ASSERT(plan.staging_size >= kRingStagingBytes);
        PG_DEVICE_ASSERT(plan.staging_size <=
                         local_region_size - plan.staging_offset);
    }
    const uint64_t signal_bytes =
        static_cast<uint64_t>(plan.signal_layout.total_signal_count) *
        sizeof(uint64_t);
    PG_DEVICE_ASSERT(plan.signal_offset <= local_region_size);
    PG_DEVICE_ASSERT(signal_bytes <=
                     local_region_size - plan.signal_offset);

    const uint64_t channel_buffer_size =
        kRingBufferBytes / channel_count;
    PG_DEVICE_ASSERT(channel_buffer_size % kRingPipelineSlots == 0 &&
                     channel_buffer_size / kRingPipelineSlots >=
                         sizeof(T));
    const uint64_t payload_slot_size =
        channel_buffer_size / kRingPipelineSlots;

    // full request -> channel -> ring shard -> tile
    const RingTileLayout tile_layout{
        .channel_elements = channel_elements,
        .shard_element_capacity =
            divideRoundUp(channel_elements, plan.participant_count),
        .tile_element_capacity = payload_slot_size / sizeof(T),
    };

    auto* const next_step_sequences =
        state->next_step_sequences +
        static_cast<uint64_t>(channel) * kRingPipelineSlots;
    uint64_t step_sequences[kRingPipelineSlots];
#pragma unroll
    for (uint32_t slot = 0;
         slot < kRingPipelineSlots; ++slot) {
        step_sequences[slot] =
            device::mc_ld_acquire_u64(next_step_sequences + slot);
    }
    auto* const next_recv_buffer_ready_sequence =
        state->next_recv_buffer_ready_sequences + channel;
    const uint64_t recv_buffer_ready_sequence =
        device::mc_ld_acquire_u64(next_recv_buffer_ready_sequence);

    const RingPrimitives<T, Op> primitives(
        plan, input, output, channel_buffer_size, channel);
    primitives.signalRecvBufferReady(block);
    const auto buffer_ready =
        primitives.waitRecvBufferReady(recv_buffer_ready_sequence, block);
    if (!buffer_ready.succeeded()) {
        completeChannel(
            plan.invocation_state, plan.recovery_mailbox, block,
            buffer_ready.failed_rank, request.failed_ranks_hint);
        return;
    }

    const uint64_t tile_count = tile_layout.tileCount();

    // A tile index selects the same-sized position within every ring shard.
    // Complete the reduce-scatter and all-gather traversal for that position
    // across all shards before advancing to the next tile index. Each channel
    // buffer is divided into two pipeline slots. Ring steps alternate between
    // them, and later tile indices reuse the same two slots.
    for (uint64_t tile_index = 0; tile_index < tile_count; ++tile_index) {
        const auto result = runRingTile(
            plan, primitives, tile_layout, tile_index, step_sequences,
            block);
        if (!result.succeeded()) {
            completeChannel(
                plan.invocation_state, plan.recovery_mailbox, block,
                result.failed_rank, request.failed_ranks_hint);
            return;
        }
    }

    if (block.thread_rank() == 0) {
#pragma unroll
        for (uint32_t slot = 0;
             slot < kRingPipelineSlots; ++slot) {
            device::mc_st_release_u64(next_step_sequences + slot,
                                      step_sequences[slot]);
        }
        device::mc_st_release_u64(next_recv_buffer_ready_sequence,
                                  recv_buffer_ready_sequence + 1);
    }
    completeChannel(plan.invocation_state, plan.recovery_mailbox, block);
}

template <typename T>
cudaError_t launchReduction(
    const RingAllReduceKernelArgs& request, RingAllReduceDeviceState* state,
    dim3 grid, int protocol_threads, cudaStream_t stream) {
    switch (request.op) {
        case ReduceOp::Sum:
            ringAllReduceKernel<T, ReduceOp::Sum>
                <<<grid, protocol_threads, 0, stream>>>(request, state);
            break;
        case ReduceOp::Product:
            ringAllReduceKernel<T, ReduceOp::Product>
                <<<grid, protocol_threads, 0, stream>>>(request, state);
            break;
        case ReduceOp::Min:
            ringAllReduceKernel<T, ReduceOp::Min>
                <<<grid, protocol_threads, 0, stream>>>(request, state);
            break;
        case ReduceOp::Max:
            ringAllReduceKernel<T, ReduceOp::Max>
                <<<grid, protocol_threads, 0, stream>>>(request, state);
            break;
        default:
            return cudaErrorInvalidValue;
    }
    return cudaGetLastError();
}

}  // namespace

cudaError_t launchRingAllReduceKernel(
    const RingAllReduceKernelArgs& request, RingAllReduceDeviceState* state,
    uint32_t channel_count, cudaStream_t stream) {
    constexpr int kProtocolThreads = 256;
    const dim3 grid(channel_count);
    switch (request.datatype) {
        case DataType::Float16:
            return launchReduction<__half>(request, state, grid,
                                           kProtocolThreads, stream);
        case DataType::Uint8:
            return launchReduction<uint8_t>(request, state, grid,
                                            kProtocolThreads, stream);
        case DataType::Int8:
            return launchReduction<int8_t>(request, state, grid,
                                           kProtocolThreads, stream);
        case DataType::Int16:
            return launchReduction<int16_t>(request, state, grid,
                                            kProtocolThreads, stream);
        case DataType::Int32:
            return launchReduction<int32_t>(request, state, grid,
                                            kProtocolThreads, stream);
        case DataType::Int64:
            return launchReduction<int64_t>(request, state, grid,
                                            kProtocolThreads, stream);
        case DataType::Bfloat16:
            return launchReduction<__nv_bfloat16>(
                request, state, grid, kProtocolThreads, stream);
        case DataType::Float32:
            return launchReduction<float>(request, state, grid,
                                          kProtocolThreads, stream);
        case DataType::Float64:
            return launchReduction<double>(request, state, grid,
                                           kProtocolThreads, stream);
        case DataType::Bool:
            return launchReduction<bool>(request, state, grid,
                                         kProtocolThreads, stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace mooncake
