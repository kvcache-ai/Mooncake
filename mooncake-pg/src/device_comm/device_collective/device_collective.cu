#include "device_comm/device_collective/device_collective_types.cuh"
#include "device_comm/device_collective/reduction_traits.cuh"

#include <cstdint>

#include <cooperative_groups.h>

#include "device_comm/device_assert.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"

namespace mooncake {
namespace cg = cooperative_groups;

namespace {

__device__ __forceinline__ uint64_t minimum(uint64_t left, uint64_t right) {
    return left < right ? left : right;
}

__device__ __forceinline__ uint64_t divideRoundUp(uint64_t value,
                                                  uint64_t divisor) {
    return value / divisor + (value % divisor != 0 ? 1 : 0);
}

__device__ __forceinline__ uint32_t wrapActiveIndex(int64_t value,
                                                    uint32_t participants) {
    value %= static_cast<int64_t>(participants);
    if (value < 0) value += participants;
    return static_cast<uint32_t>(value);
}

__device__ __forceinline__ bool invocationFailed(
    const DeviceCollectiveKernelResources& resources, uint32_t* shared_result,
    cg::thread_block block) {
    bool failed = false;
    if (block.thread_rank() == 0) {
        auto* const latched = reinterpret_cast<unsigned int*>(
            &resources.control.invocation->failure_latched);
        failed = atomicAdd(latched, 0u) != 0;
        *shared_result = failed ? 1 : 0;
    }
    block.sync();
    failed = *shared_result != 0;
    block.sync();
    return failed;
}

// Called by thread 0 of the CTA that performs the final arrived_channels
// increment. Failed invocations wait here until the host replaces the shared
// collective state.
__device__ __forceinline__ void finalizeInvocation(
    const DeviceCollectiveKernelResources& resources) {
    auto* const invocation = resources.control.invocation;
    auto* const failed =
        reinterpret_cast<unsigned int*>(&invocation->failure_latched);
    auto* const arrived =
        reinterpret_cast<unsigned int*>(&invocation->arrived_channels);

    if (atomicAdd(failed, 0u) != 0u) {
        auto* const mailbox = resources.recovery;
        const uint64_t generation =
            device::mc_ld_acquire_u64(&mailbox->failure_generation) + 1;

        // Each channel ordered its prior writes before incrementing
        // arrived_channels. This CTA observed the final increment, so this
        // system fence makes those writes system-visible before the release
        // store notifies the host. After acquiring the new generation, the
        // host may read the failure metadata and replace the Plan and transfer
        // state.
        __threadfence_system();
        device::mc_st_release_u64(&mailbox->failure_generation, generation);
        while (device::mc_ld_acquire_u64(&mailbox->ready_generation) <
               generation) {
        }
    }

    atomicExch(failed, 0u);
    __threadfence();
    atomicExch(arrived, 0u);
}

// Completes this channel after success, a failure detected by another channel,
// or a new failure detected by this channel. Only the detecting channel
// supplies a failed rank; the first detector records the failure metadata.
__device__ __forceinline__ void completeChannel(
    const DeviceAllReduceKernelArgs& request,
    const DeviceCollectiveKernelResources& resources, cg::thread_block block,
    InGroupRank detected_failed_rank = kInvalidInGroupRank) {
    // No thread may publish channel completion while another thread in the CTA
    // can still access the current Plan or transfer buffers.
    block.sync();
    if (block.thread_rank() == 0) {
        auto* const invocation = resources.control.invocation;
        auto* const failed =
            reinterpret_cast<unsigned int*>(&invocation->failure_latched);
        auto* const arrived =
            reinterpret_cast<unsigned int*>(&invocation->arrived_channels);

        if (detected_failed_rank != kInvalidInGroupRank &&
            atomicCAS(failed, 0u, 1u) == 0u) {
            auto* const mailbox = resources.recovery;
            mailbox->failed_rank = detected_failed_rank;
            mailbox->failed_hint_address =
                reinterpret_cast<uint64_t>(request.failed_ranks_hint);
        }

        // Order this CTA's prior writes before its atomic arrived_channels
        // increment. All CTAs update the same counter; the CTA whose increment
        // reaches channel_count runs finalizeInvocation(), whose system fence
        // makes every channel's preceding writes visible before notifying the
        // host.
        __threadfence();

        const uint32_t previous = atomicAdd(arrived, 1u);
        if (previous + 1 == request.channel_count) {
            finalizeInvocation(resources);
        }
    }
    block.sync();
}

template <typename T>
__device__ __forceinline__ void copyValues(T* destination, const T* source,
                                           uint64_t count,
                                           cg::thread_block block) {
    for (uint64_t index = block.thread_rank(); index < count;
         index += block.size()) {
        destination[index] = source[index];
    }
}

template <typename T, ReduceOp Op>
__device__ __forceinline__ void reduceValues(T* destination, const T* source,
                                             uint64_t count,
                                             cg::thread_block block) {
    for (uint64_t index = block.thread_rank(); index < count;
         index += block.size()) {
        destination[index] = DeviceReductionTraits<T, Op>::apply(
            destination[index], source[index]);
    }
}

enum class RingStepResult : uint32_t {
    Succeeded,
    DataSignalTimedOut,
    ConsumedAckTimedOut,
};

// The data buffer is shared across communicators. StrongStream guarantees that
// the previous collective kernel on this rank has completed before this kernel
// starts, so the shared recv buffer is no longer in use by local GPU work.
// Remote puts are not ordered by StrongStream, so explicitly notify only the
// peer that sends to this rank that the recv buffer is ready.
__device__ __forceinline__ void publishRecvBufferReady(
    const DeviceCollectiveKernelResources& resources,
    const TransferLane& transfer_lane, const DeviceCollectivePeerTarget& sender,
    InGroupRank recv_rank, uint32_t channel_index, cg::thread_block block) {
    SignalRequest ready;
    ready.signal.remote_offset =
        sender.remote_control_offset +
        resources.control.recv_ready_slots.remoteSlotOffset(channel_index,
                                                            recv_rank);
    ready.timeout_ticks = resources.timeout_ticks;
    // The remote sender gates its put on observing this signal, so publishing
    // it does not need to block this rank's independent wait below.
    (void)transfer_lane.signal(sender.global_rank, ready, block);
}

// A ring rank writes only its successor's recv buffer. Wait only for that peer,
// without coupling this edge to unrelated peers in the group.
__device__ __forceinline__ SignalWaitStatus waitForRecvBufferReady(
    const DeviceCollectiveKernelResources& resources,
    const TransferLane& transfer_lane, InGroupRank recv_rank,
    uint64_t ready_sequence, uint32_t channel_index, cg::thread_block block) {
    const auto recv_ready = transfer_lane.waitSignal(
        SignalWaitRequest{
            .local_offset = resources.control.recv_ready_slots.localSlotOffset(
                channel_index, recv_rank),
            .least = ready_sequence,
            .timeout_ticks = resources.timeout_ticks,
        },
        block);
    if (recv_ready.status == SignalWaitStatus::TimedOut) {
        return SignalWaitStatus::TimedOut;
    }

    // Exactly one peer publishes this slot once per invocation. A later
    // value means peers entered collectives in different orders.
    PG_DEVICE_ASSERT(recv_ready.observed == ready_sequence);
    return SignalWaitStatus::Reached;
}

__device__ __forceinline__ InGroupRank
failedRankForRingStep(const DeviceAllReducePlan& plan, RingStepResult result) {
    switch (result) {
        case RingStepResult::DataSignalTimedOut:
            return plan.predecessor.in_group_rank;
        case RingStepResult::ConsumedAckTimedOut:
            return plan.successor.in_group_rank;
        case RingStepResult::Succeeded:
            PG_DEVICE_UNREACHABLE();
            return kInvalidInGroupRank;
    }
    PG_DEVICE_UNREACHABLE();
    return kInvalidInGroupRank;
}

template <typename T, ReduceOp Op>
__device__ __forceinline__ RingStepResult
runRingStep(const DeviceCollectiveKernelResources& resources,
            const TransferLane& transfer_lane, const DeviceAllReducePlan& plan,
            const DeviceCollectivePeerTarget& succ,
            const DeviceCollectivePeerTarget& pred, T* output,
            const DeviceCollectiveTransferBuffer& send_region,
            const DeviceCollectiveTransferBuffer& recv_region,
            uint64_t channel_elements, uint64_t shard_element_capacity,
            uint32_t send_shard_index, uint32_t recv_shard_index,
            uint64_t tile_index, uint64_t tile_element_capacity,
            uint64_t step_sequence, uint32_t channel_index,
            bool reduce_received_values, cg::thread_block block) {
    const uint64_t send_shard_begin =
        static_cast<uint64_t>(send_shard_index) * shard_element_capacity;
    const uint64_t recv_shard_begin =
        static_cast<uint64_t>(recv_shard_index) * shard_element_capacity;
    const uint64_t send_shard_elements =
        send_shard_begin < channel_elements
            ? minimum(shard_element_capacity,
                      channel_elements - send_shard_begin)
            : 0;
    const uint64_t recv_shard_elements =
        recv_shard_begin < channel_elements
            ? minimum(shard_element_capacity,
                      channel_elements - recv_shard_begin)
            : 0;
    const uint64_t tile_begin = tile_index * tile_element_capacity;
    const uint64_t send_count =
        tile_begin < send_shard_elements
            ? minimum(tile_element_capacity, send_shard_elements - tile_begin)
            : 0;
    const uint64_t recv_count =
        tile_begin < recv_shard_elements
            ? minimum(tile_element_capacity, recv_shard_elements - tile_begin)
            : 0;
    // Every rank executes the same phase/step/tile schedule. A tail shard may
    // contribute zero elements to this tile, but its peers still exchange the
    // arrival and consumed notifications. Use offset zero for an empty
    // direction so we never form a pointer outside the caller buffer.
    const uint64_t send_begin =
        send_count == 0 ? 0 : send_shard_begin + tile_begin;
    const uint64_t recv_begin =
        recv_count == 0 ? 0 : recv_shard_begin + tile_begin;

    auto* const send_buffer = reinterpret_cast<T*>(send_region.addr);
    const auto* const recv_buffer =
        reinterpret_cast<const T*>(recv_region.addr);
    copyValues(send_buffer, output + send_begin, send_count, block);
    block.sync();

    PutAndSignalRequest data_send;
    data_send.local_offset = send_region.region_offset;
    data_send.remote_offset = recv_region.region_offset;
    data_send.size = send_count * sizeof(T);
    data_send.signal.remote_offset =
        succ.remote_control_offset +
        resources.control.signal_slots.remoteSlotOffset(channel_index,
                                                        plan.self_rank);
    data_send.timeout_ticks = resources.timeout_ticks;

    (void)transfer_lane.putAndSignal(succ.global_rank, data_send, block);

    const auto arrival = transfer_lane.waitSignal(
        SignalWaitRequest{
            .local_offset = resources.control.signal_slots.localSlotOffset(
                channel_index, plan.predecessor.in_group_rank),
            .least = step_sequence,
            .timeout_ticks = resources.timeout_ticks,
        },
        block);
    if (arrival.status == SignalWaitStatus::TimedOut) {
        return RingStepResult::DataSignalTimedOut;
    }

    // This Ring permits only one outstanding step per peer and channel. The
    // generic Service accepts any value at least `step_sequence`; observing a
    // later value here means the collective schedules have diverged.
    PG_DEVICE_ASSERT(arrival.observed == step_sequence);

    if (reduce_received_values) {
        reduceValues<T, Op>(output + recv_begin, recv_buffer, recv_count,
                            block);
    } else {
        copyValues(output + recv_begin, recv_buffer, recv_count, block);
    }
    block.sync();

    // Arrival means the receive buffer is readable. This second signal says
    // every thread has consumed it and the sender may reuse that channel.
    SignalRequest consumed_ack;
    consumed_ack.signal.remote_offset =
        pred.remote_control_offset +
        resources.control.consumed_ack_slots.remoteSlotOffset(channel_index,
                                                              plan.self_rank);
    consumed_ack.timeout_ticks = resources.timeout_ticks;
    (void)transfer_lane.signal(pred.global_rank, consumed_ack, block);

    const auto ack = transfer_lane.waitSignal(
        SignalWaitRequest{
            .local_offset =
                resources.control.consumed_ack_slots.localSlotOffset(
                    channel_index, plan.successor.in_group_rank),
            .least = step_sequence,
            .timeout_ticks = resources.timeout_ticks,
        },
        block);
    if (ack.status == SignalWaitStatus::TimedOut) {
        return RingStepResult::ConsumedAckTimedOut;
    }
    PG_DEVICE_ASSERT(ack.observed == step_sequence);
    return RingStepResult::Succeeded;
}

template <typename T, ReduceOp Op>
__global__ void flatRingAllReduceKernel(
    DeviceAllReduceKernelArgs request,
    DeviceCollectiveKernelResources resources) {
    const auto block = cg::this_thread_block();
    __shared__ uint32_t shared_result;
    const uint32_t channel = blockIdx.x;
    const auto* const plan_slot = resources.control.all_reduce_plan;
    // Recovery may update the host-constructed Plan between Graph replays, so
    // status must be read on every execution.
    // A non-Ready Plan is an internal launch-contract violation, not a peer
    // failure.
    PG_DEVICE_ASSERT(plan_slot->status == DevicePlanStatus::Ready);
    const auto plan = plan_slot->plan;
    if (request.count == 0) {
        completeChannel(request, resources, block);
        return;
    }

    // Split the full request evenly across the active channel CTAs. Channel
    // sizes differ by at most one element; any remainder is assigned to the
    // leading channels.
    const uint64_t elements_per_channel = request.count / request.channel_count;
    const uint64_t extra_elements = request.count % request.channel_count;
    const uint64_t channel_elements =
        elements_per_channel + (channel < extra_elements ? 1 : 0);
    // Let r = extra_elements and c = channel. The first r channels each get
    // one extra element, so the number of extra elements before channel c is:
    // - c < r: all c preceding channels have one extra, giving c;
    // - c >= r: only the first r channels have extras, giving r.
    // Therefore the offset adjustment is min(c, r).
    const uint64_t channel_offset =
        static_cast<uint64_t>(channel) * elements_per_channel +
        minimum(channel, extra_elements);

    const auto* input =
        static_cast<const T*>(request.send_buffer) + channel_offset;
    auto* output = static_cast<T*>(request.recv_buffer) + channel_offset;
    if (input != output) {
        copyValues(output, input, channel_elements, block);
    }

    // completeChannel() supplies the copy barrier for this terminal path.
    if (plan.participant_count == 1) {
        completeChannel(request, resources, block);
        return;
    }
    block.sync();

    // This AllReduce binds each algorithm channel one-to-one to a Service
    // lane. `channel` partitions data and collective control tables; the lane
    // selects fixed device-transfer execution resources.
    const auto transfer_lane = resources.transfer_handle->lane(channel);
    const uint64_t channel_buffer_size =
        resources.send_buffer.size / request.channel_count;
    const DeviceCollectiveTransferBuffer send_region{
        .addr = static_cast<char*>(resources.send_buffer.addr) +
                static_cast<uint64_t>(channel) * channel_buffer_size,
        .region_offset = resources.send_buffer.region_offset +
                         static_cast<uint64_t>(channel) * channel_buffer_size,
        .size = channel_buffer_size,
    };
    const DeviceCollectiveTransferBuffer recv_region{
        .addr = static_cast<char*>(resources.recv_buffer.addr) +
                static_cast<uint64_t>(channel) * channel_buffer_size,
        .region_offset = resources.recv_buffer.region_offset +
                         static_cast<uint64_t>(channel) * channel_buffer_size,
        .size = channel_buffer_size,
    };

    // This CTA processes one channel range. The ring further partitions that
    // range, and the fixed transfer buffers may require one more subdivision:
    //
    //   full request
    //     -> channel range          one per CTA
    //       -> ring shard           one per participant
    //         -> transfer tile      fits one channel send/recv buffer
    //
    // Each participant owns one fixed-capacity shard slot. Tail slots may be
    // partially filled or empty, but every rank uses the same capacity and
    // therefore the same tile-loop bound and step sequence.
    const uint64_t shard_element_capacity =
        divideRoundUp(channel_elements, plan.participant_count);
    const uint64_t tile_element_capacity = send_region.size / sizeof(T);
    const uint64_t tile_iterations_per_shard =
        divideRoundUp(shard_element_capacity, tile_element_capacity);

    // ReduceScatter and AllGather each contain participant_count - 1 ring
    // steps. Each tile advances this channel's persistent step sequence once.
    const uint64_t steps = plan.participant_count - 1;
    auto* const next_step_sequence =
        resources.control.next_step_sequences + channel;
    uint64_t step_sequence = device::mc_ld_acquire_u64(next_step_sequence);
    auto* const next_recv_ready_sequence =
        resources.control.next_recv_ready_sequences + channel;
    const uint64_t recv_ready_sequence =
        device::mc_ld_acquire_u64(next_recv_ready_sequence);

    const auto& succ = plan.successor;
    const auto& pred = plan.predecessor;

    publishRecvBufferReady(resources, transfer_lane, pred, plan.self_rank,
                           channel, block);
    if (waitForRecvBufferReady(resources, transfer_lane, succ.in_group_rank,
                               recv_ready_sequence, channel,
                               block) != SignalWaitStatus::Reached) {
        completeChannel(request, resources, block, succ.in_group_rank);
        return;
    }

    // ReduceScatter: forward one shard around the ring per step and reduce the
    // shard received from the predecessor into this rank's output.
    for (uint64_t step = 0; step < steps; ++step) {
        const uint32_t send_shard =
            wrapActiveIndex(static_cast<int64_t>(plan.self_active_index) - step,
                            plan.participant_count);
        const uint32_t recv_shard = wrapActiveIndex(
            static_cast<int64_t>(plan.self_active_index) - step - 1,
            plan.participant_count);
        for (uint64_t tile = 0; tile < tile_iterations_per_shard; ++tile) {
            if (invocationFailed(resources, &shared_result, block)) {
                completeChannel(request, resources, block);
                return;
            }
            const auto result = runRingStep<T, Op>(
                resources, transfer_lane, plan, succ, pred, output, send_region,
                recv_region, channel_elements, shard_element_capacity,
                send_shard, recv_shard, tile, tile_element_capacity,
                step_sequence, channel, true, block);
            if (result != RingStepResult::Succeeded) {
                completeChannel(request, resources, block,
                                failedRankForRingStep(plan, result));
                return;
            }
            ++step_sequence;
        }
    }

    // AllGather: circulate the reduced shards without modifying their values.
    for (uint64_t step = 0; step < steps; ++step) {
        const uint32_t send_shard = wrapActiveIndex(
            static_cast<int64_t>(plan.self_active_index) + 1 - step,
            plan.participant_count);
        const uint32_t recv_shard =
            wrapActiveIndex(static_cast<int64_t>(plan.self_active_index) - step,
                            plan.participant_count);
        for (uint64_t tile = 0; tile < tile_iterations_per_shard; ++tile) {
            if (invocationFailed(resources, &shared_result, block)) {
                completeChannel(request, resources, block);
                return;
            }
            const auto result = runRingStep<T, Op>(
                resources, transfer_lane, plan, succ, pred, output, send_region,
                recv_region, channel_elements, shard_element_capacity,
                send_shard, recv_shard, tile, tile_element_capacity,
                step_sequence, channel, false, block);
            if (result != RingStepResult::Succeeded) {
                completeChannel(request, resources, block,
                                failedRankForRingStep(plan, result));
                return;
            }
            ++step_sequence;
        }
    }

    // Failed invocations return above without committing; recovery resets the
    // step-sequence and signal state before another invocation can use the
    // new view.
    if (block.thread_rank() == 0) {
        device::mc_st_release_u64(next_step_sequence, step_sequence);
        device::mc_st_release_u64(next_recv_ready_sequence,
                                  recv_ready_sequence + 1);
    }
    completeChannel(request, resources, block);
}

template <typename T>
cudaError_t launchReduction(const DeviceAllReduceKernelArgs& request,
                            const DeviceCollectiveKernelResources& resources,
                            dim3 grid, int protocol_threads,
                            cudaStream_t stream) {
    switch (request.op) {
        case ReduceOp::Sum:
            flatRingAllReduceKernel<T, ReduceOp::Sum>
                <<<grid, protocol_threads, 0, stream>>>(request, resources);
            break;
        case ReduceOp::Product:
            flatRingAllReduceKernel<T, ReduceOp::Product>
                <<<grid, protocol_threads, 0, stream>>>(request, resources);
            break;
        case ReduceOp::Min:
            flatRingAllReduceKernel<T, ReduceOp::Min>
                <<<grid, protocol_threads, 0, stream>>>(request, resources);
            break;
        case ReduceOp::Max:
            flatRingAllReduceKernel<T, ReduceOp::Max>
                <<<grid, protocol_threads, 0, stream>>>(request, resources);
            break;
        default:
            return cudaErrorInvalidValue;
    }
    return cudaGetLastError();
}

}  // namespace

cudaError_t launchDeviceAllReduceKernel(
    const DeviceAllReduceKernelArgs& request,
    const DeviceCollectiveKernelResources& resources, cudaStream_t stream) {
    constexpr int kProtocolThreads = 256;
    const dim3 grid(request.channel_count);
    switch (request.datatype) {
        case DataType::Float16:
            return launchReduction<__half>(request, resources, grid,
                                           kProtocolThreads, stream);
        case DataType::Uint8:
            return launchReduction<uint8_t>(request, resources, grid,
                                            kProtocolThreads, stream);
        case DataType::Int8:
            return launchReduction<int8_t>(request, resources, grid,
                                           kProtocolThreads, stream);
        case DataType::Int16:
            return launchReduction<int16_t>(request, resources, grid,
                                            kProtocolThreads, stream);
        case DataType::Int32:
            return launchReduction<int32_t>(request, resources, grid,
                                            kProtocolThreads, stream);
        case DataType::Int64:
            return launchReduction<int64_t>(request, resources, grid,
                                            kProtocolThreads, stream);
        case DataType::Bfloat16:
            return launchReduction<__nv_bfloat16>(request, resources, grid,
                                                  kProtocolThreads, stream);
        case DataType::Float32:
            return launchReduction<float>(request, resources, grid,
                                          kProtocolThreads, stream);
        case DataType::Float64:
            return launchReduction<double>(request, resources, grid,
                                           kProtocolThreads, stream);
        case DataType::Bool:
            return launchReduction<bool>(request, resources, grid,
                                         kProtocolThreads, stream);
        default:
            return cudaErrorInvalidValue;
    }
}

}  // namespace mooncake
