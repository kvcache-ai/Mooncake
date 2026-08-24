#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_PRIMITIVES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_PRIMITIVES_CUH

#include <cstdint>

#include <cooperative_groups.h>

#include "device_comm/device_assert.cuh"
#include "device_comm/device_collective/device_collective_kernel.cuh"
#include "device_comm/device_collective/protocols/ring/ring_types.cuh"
#include "device_comm/device_primitives/payload_writer.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"

namespace mooncake {

[[nodiscard]] __device__ __forceinline__ uint64_t minimum(uint64_t left,
                                                          uint64_t right) {
    return left < right ? left : right;
}

[[nodiscard]] __device__ __forceinline__ uint64_t divideRoundUp(
    uint64_t value, uint64_t divisor) {
    return value / divisor + (value % divisor != 0 ? 1 : 0);
}

struct RingStepResult {
    InGroupRank failed_rank = kInvalidInGroupRank;

    [[nodiscard]] __device__ __forceinline__ bool succeeded() const {
        return failed_rank == kInvalidInGroupRank;
    }
};

struct RingTile {
    uint64_t begin = 0;
    uint64_t count = 0;
};

struct RingTileLayout {
    uint64_t channel_elements = 0;
    uint64_t shard_element_capacity = 0;
    uint64_t tile_element_capacity = 0;

    [[nodiscard]] __device__ __forceinline__ RingTile tile(
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
                ? minimum(tile_element_capacity,
                          shard_elements - tile_begin)
                : 0;

        // Short shards still execute the common lock-step tile schedule. Use
        // offset zero for an empty tile so callers never form an out-of-range
        // pointer even though the resulting operation has count zero.
        if (count == 0) {
            return RingTile{
                .begin = 0,
                .count = 0,
            };
        }

        return RingTile{
            .begin = shard_begin + tile_begin,
            .count = count,
        };
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t tileCount() const {
        // Count tiles from the common shard capacity, rather than one shard's
        // clipped length, so every rank executes the same number of actions.
        return divideRoundUp(shard_element_capacity, tile_element_capacity);
    }
};

struct RingPayloadSlot {
    uint32_t index = 0;
    uint64_t sequence = 0;
};

struct RingPayloadSlotSchedule {
    uint64_t* next_sequences = nullptr;
    uint64_t action_index = 0;

    // Advance the common alternating slot schedule and allocate the next
    // generation of the selected physical payload slot.
    [[nodiscard]] __device__ __forceinline__ RingPayloadSlot next() {
        const uint32_t slot_index = static_cast<uint32_t>(
            action_index % kRingPipelineSlots);
        ++action_index;

        const uint64_t sequence = next_sequences[slot_index];
        next_sequences[slot_index] = sequence + 1;
        return RingPayloadSlot{
            .index = slot_index,
            .sequence = sequence,
        };
    }
};

[[nodiscard]] __device__ __forceinline__ uint32_t wrapActiveIndex(
    int64_t value, uint32_t participants) {
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
    int32_t self_active_index, uint32_t participant_count,
    uint64_t distance) {
    return wrapActiveIndex(static_cast<int64_t>(self_active_index) -
                               static_cast<int64_t>(distance),
                           participant_count);
}

[[nodiscard]] __device__ __forceinline__ StagingRegion stagingForChannel(
    const RingAllReducePlan& plan, uint64_t channel_offset,
    uint64_t channel_size) {
    if (plan.staging_size == 0) return {};

    PG_DEVICE_ASSERT(channel_offset <= plan.staging_size);
    PG_DEVICE_ASSERT(channel_size <= plan.staging_size - channel_offset);
    PG_DEVICE_ASSERT(plan.staging_offset <= UINT64_MAX - channel_offset);
    return StagingRegion{
        .region_offset = plan.staging_offset + channel_offset,
        .size = channel_size,
    };
}

// CTA-collective operations fused around one ring receive/send step.
template <typename T, ReduceOp Op>
class RingPrimitives {
   public:
    __device__ __forceinline__ RingPrimitives(
        const RingAllReducePlan& plan, const T* input, T* output,
        uint64_t channel_buffer_size, uint32_t channel_index)
        : transfer_lane_(plan.transfer_handle->lane(channel_index)),
          self_rank_(plan.self_rank),
          successor_(plan.successor),
          predecessor_(plan.predecessor),
          input_(input),
          output_(output),
          recv_payload_buffer_(plan.transfer_handle->localPtr(
              plan.buffer_offset + static_cast<uint64_t>(channel_index) *
                                       channel_buffer_size)),
          send_writer_(
              *plan.transfer_handle, transfer_lane_,
              plan.successor.global_rank,
              stagingForChannel(
                  plan,
                  static_cast<uint64_t>(channel_index) * channel_buffer_size,
                  channel_buffer_size),
              RemotePayloadRegion{
                  .region_offset =
                      plan.successor.buffer_offset +
                      static_cast<uint64_t>(channel_index) *
                          channel_buffer_size,
                  .size = channel_buffer_size,
              }),
          payload_slot_size_(channel_buffer_size / kRingPipelineSlots),
          signal_offset_(plan.signal_offset),
          timeout_ticks_(plan.timeout_ticks),
          signal_layout_(plan.signal_layout),
          channel_index_(channel_index) {}

    // The shared buffer may have been used by another communicator.
    // StrongStream orders local kernels, while this edge-local signal prevents
    // the predecessor from overwriting our buffer too early.
    __device__ __forceinline__ void signalRecvBufferReady(
        cooperative_groups::thread_block block) const {
        SignalRequest ready;
        ready.signal.kind = SignalAction::Kind::Add;
        ready.signal.add.remote_offset =
            signal_layout_.recvBufferReadyOffset(
                predecessor_.signal_offset, channel_index_, self_rank_);
        ready.timeout_ticks = timeout_ticks_;
        (void)transfer_lane_.signal(predecessor_.global_rank, ready, block);
    }

    [[nodiscard]] __device__ __forceinline__ RingStepResult
    waitRecvBufferReady(uint64_t sequence,
                        cooperative_groups::thread_block block) const {
        const auto ready = transfer_lane_.waitSignal(
            SignalWaitRequest{
                .local_offset = signal_layout_.recvBufferReadyOffset(
                    signal_offset_, channel_index_,
                    successor_.in_group_rank),
                .least = sequence,
                .timeout_ticks = timeout_ticks_,
            },
            block);
        if (ready.status == SignalWaitStatus::TimedOut) {
            return RingStepResult{
                .failed_rank = successor_.in_group_rank,
            };
        }
        PG_DEVICE_ASSERT(ready.observed == sequence);
        return {};
    }

    [[nodiscard]] __device__ __forceinline__ RingStepResult send(
        const RingTile& tile, RingPayloadSlot send_slot,
        cooperative_groups::thread_block block) const {
        const auto available = waitPreviousPayloadConsumed(send_slot, block);
        if (!available.succeeded()) return available;

        const auto outgoing_payload = outgoingPayload(send_slot);
        copyValuesTo(input_ + tile.begin, tile.count, block,
                     outgoing_payload.template dataAs<T>());
        publishPayload(outgoing_payload, send_slot, tile.count, block);
        return {};
    }

    // FIXME: On Staging paths, the recv-and-forward primitives below could
    // reduce or copy into local staging before the successor has consumed the
    // previous remote slot generation, delaying
    // waitPreviousPayloadConsumed() until immediately before publishPayload.
    // Reusing the local staging slot must first wait for the previous transfer
    // sourced from that slot to complete. A Direct path cannot move the
    // consumed wait because it materializes the new payload directly in the
    // successor's slot.
    [[nodiscard]] __device__ __forceinline__ RingStepResult recvReduceSend(
        const RingTile& tile, RingPayloadSlot recv_slot,
        RingPayloadSlot send_slot,
        cooperative_groups::thread_block block) const {
        const auto arrived = waitPayloadReady(recv_slot, block);
        if (!arrived.succeeded()) return arrived;
        const auto available = waitPreviousPayloadConsumed(send_slot, block);
        if (!available.succeeded()) return available;

        const auto* const received_payload = receivedPayload(recv_slot);
        const auto outgoing_payload = outgoingPayload(send_slot);
        reduceValuesTo<T, Op>(input_ + tile.begin, received_payload,
                              tile.count, block,
                              outgoing_payload.template dataAs<T>());
        publishPayload(outgoing_payload, send_slot, tile.count, block);
        signalPayloadConsumed(recv_slot, block);
        return {};
    }

    [[nodiscard]] __device__ __forceinline__ RingStepResult
    recvReduceCopySend(const RingTile& tile, RingPayloadSlot recv_slot,
                       RingPayloadSlot send_slot,
                       cooperative_groups::thread_block block) const {
        const auto arrived = waitPayloadReady(recv_slot, block);
        if (!arrived.succeeded()) return arrived;
        const auto available = waitPreviousPayloadConsumed(send_slot, block);
        if (!available.succeeded()) return available;

        const auto* const received_payload = receivedPayload(recv_slot);
        const auto outgoing_payload = outgoingPayload(send_slot);
        reduceValuesTo<T, Op>(input_ + tile.begin, received_payload,
                              tile.count, block, output_ + tile.begin,
                              outgoing_payload.template dataAs<T>());
        publishPayload(outgoing_payload, send_slot, tile.count, block);
        signalPayloadConsumed(recv_slot, block);
        return {};
    }

    [[nodiscard]] __device__ __forceinline__ RingStepResult recvCopySend(
        const RingTile& tile, RingPayloadSlot recv_slot,
        RingPayloadSlot send_slot,
        cooperative_groups::thread_block block) const {
        const auto arrived = waitPayloadReady(recv_slot, block);
        if (!arrived.succeeded()) return arrived;
        const auto available = waitPreviousPayloadConsumed(send_slot, block);
        if (!available.succeeded()) return available;

        const auto* const received_payload = receivedPayload(recv_slot);
        const auto outgoing_payload = outgoingPayload(send_slot);
        copyValuesTo(received_payload, tile.count, block, output_ + tile.begin,
                     outgoing_payload.template dataAs<T>());
        publishPayload(outgoing_payload, send_slot, tile.count, block);
        signalPayloadConsumed(recv_slot, block);
        return {};
    }

    [[nodiscard]] __device__ __forceinline__ RingStepResult recvCopyAndDrain(
        const RingTile& tile, RingPayloadSlot final_slot,
        cooperative_groups::thread_block block) const {
        const auto arrived = waitPayloadReady(final_slot, block);
        if (!arrived.succeeded()) return arrived;

        copyValuesTo(receivedPayload(final_slot), tile.count, block,
                     output_ + tile.begin);
        signalPayloadConsumed(final_slot, block);

        // The predecessor's last recvCopySend publishes to this rank with the
        // same slot descriptor that this rank's last recvCopySend used to
        // publish to the successor. After consuming the former above, wait
        // here for the successor to consume the latter. The payloads belong to
        // different ring edges, but the symmetric action schedule assigns the
        // same slot index and sequence to both.
        return waitPayloadConsumed(final_slot, block);
    }

   private:
    [[nodiscard]] __device__ __forceinline__ const T* receivedPayload(
        RingPayloadSlot recv_slot) const {
        return reinterpret_cast<const T*>(
            recv_payload_buffer_ +
            static_cast<uint64_t>(recv_slot.index) * payload_slot_size_);
    }

    [[nodiscard]] __device__ __forceinline__ PayloadWriteView outgoingPayload(
        RingPayloadSlot send_slot) const {
        const uint64_t staging_offset =
            static_cast<uint64_t>(send_slot.index) * payload_slot_size_;
        const uint64_t remote_offset =
            static_cast<uint64_t>(send_slot.index) * payload_slot_size_;
        return send_writer_.view(staging_offset, remote_offset,
                                 payload_slot_size_);
    }

    __device__ __forceinline__ void publishPayload(
        const PayloadWriteView& payload, RingPayloadSlot send_slot,
        uint64_t count, cooperative_groups::thread_block block) const {
        PayloadPublishRequest publication;
        publication.size = count * sizeof(T);
        publication.signal.kind = SignalAction::Kind::Add;
        publication.signal.add.remote_offset =
            signal_layout_.payloadReadyOffset(
                successor_.signal_offset, channel_index_, self_rank_,
                send_slot.index);
        publication.signal.add.delta = 1;
        publication.timeout_ticks = timeout_ticks_;
        (void)payload.publish(publication, block);
    }

    [[nodiscard]] __device__ __forceinline__ RingStepResult waitPayloadReady(
        RingPayloadSlot recv_slot,
        cooperative_groups::thread_block block) const {
        const auto arrival = transfer_lane_.waitSignal(
            SignalWaitRequest{
                .local_offset = signal_layout_.payloadReadyOffset(
                    signal_offset_, channel_index_,
                    predecessor_.in_group_rank, recv_slot.index),
                .least = recv_slot.sequence,
                .timeout_ticks = timeout_ticks_,
            },
            block);
        if (arrival.status == SignalWaitStatus::TimedOut) {
            return RingStepResult{
                .failed_rank = predecessor_.in_group_rank,
            };
        }
        PG_DEVICE_ASSERT(arrival.observed == recv_slot.sequence);
        return {};
    }

    __device__ __forceinline__ void signalPayloadConsumed(
        RingPayloadSlot recv_slot,
        cooperative_groups::thread_block block) const {
        SignalRequest ack;
        ack.signal.kind = SignalAction::Kind::Add;
        ack.signal.add.remote_offset =
            signal_layout_.payloadConsumedOffset(
                predecessor_.signal_offset, channel_index_, self_rank_,
                recv_slot.index);
        ack.timeout_ticks = timeout_ticks_;
        (void)transfer_lane_.signal(predecessor_.global_rank, ack, block);
    }

    [[nodiscard]] __device__ __forceinline__ RingStepResult
    waitPreviousPayloadConsumed(RingPayloadSlot send_slot,
                                cooperative_groups::thread_block block) const {
        return waitConsumedSequence(send_slot.index, send_slot.sequence - 1,
                                    block);
    }

    [[nodiscard]] __device__ __forceinline__ RingStepResult
    waitPayloadConsumed(RingPayloadSlot send_slot,
                        cooperative_groups::thread_block block) const {
        return waitConsumedSequence(send_slot.index, send_slot.sequence,
                                    block);
    }

    [[nodiscard]] __device__ __forceinline__ RingStepResult
    waitConsumedSequence(uint32_t payload_slot, uint64_t sequence,
                         cooperative_groups::thread_block block) const {
        const auto ack = transfer_lane_.waitSignal(
            SignalWaitRequest{
                .local_offset = signal_layout_.payloadConsumedOffset(
                    signal_offset_, channel_index_,
                    successor_.in_group_rank, payload_slot),
                .least = sequence,
                .timeout_ticks = timeout_ticks_,
            },
            block);
        if (ack.status == SignalWaitStatus::TimedOut) {
            return RingStepResult{
                .failed_rank = successor_.in_group_rank,
            };
        }
        PG_DEVICE_ASSERT(ack.observed == sequence);
        return {};
    }

    TransferLane transfer_lane_;
    InGroupRank self_rank_;
    RingPeerTarget successor_;
    RingPeerTarget predecessor_;
    const T* input_;
    T* output_;
    const char* recv_payload_buffer_;
    PayloadWriter send_writer_;
    uint64_t payload_slot_size_;
    uint64_t signal_offset_;
    uint64_t timeout_ticks_;
    RingSignalLayout signal_layout_;
    uint32_t channel_index_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_RING_PRIMITIVES_CUH
