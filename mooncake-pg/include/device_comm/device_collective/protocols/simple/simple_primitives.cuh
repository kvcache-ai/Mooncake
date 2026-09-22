#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_PRIMITIVES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_PRIMITIVES_CUH

#include "device_comm/device_collective/protocols/simple/simple_types.cuh"
#include "device_comm/device_primitives/payload_writer.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"

namespace mooncake {

// Resource binding for one channel's incoming and outgoing connections.
template <uint32_t PipelineDepth = kDefaultSimplePipelineDepth>
struct SimpleBindings {
    TransferLane lane;
    uint64_t timeout_ticks;
    StagingRegion staging;
    const char* recv_buffer;
    // Byte offset in send_peer's DTS peer-accessible region.
    uint64_t send_buffer_offset;
    SimpleBufferLayout<PipelineDepth> buffers;
    InGroupRank recv_peer;
    InGroupRank send_peer;
    uint32_t channel;
};

// One invocation's Simple communication primitives, currently with one receive
// and one send connection. Every method is CTA-collective; each chunk must fit
// a payload slot. send() and Reduce operations read chunk.source; Copy
// operations write chunk.destination.
template <typename T, ReduceOp Op,
          uint32_t PipelineDepth = kDefaultSimplePipelineDepth>
class SimplePrimitives {
   public:
    __device__ __forceinline__
    SimplePrimitives(const SimpleBindings<PipelineDepth>& bindings,
                     const SimpleState<PipelineDepth>& state)
        : bindings_(bindings),
          state_(state),
          recv_peer_(peerAt(state, bindings.recv_peer)),
          send_peer_(peerAt(state, bindings.send_peer)),
          send_writer_(*state.transfer_handle, bindings.lane,
                       send_peer_.global_rank, bindings.staging,
                       RemotePayloadRegion{
                           bindings.send_buffer_offset,
                           bindings.buffers.bufferBytes()}) {
        static_assert(kSimplePayloadAlignment == alignof(ValuePack<T>));
        const uint32_t max_group_size = state.signal_layout.max_group_size;
        const auto send_rank = bindings.send_peer;
        const auto recv_rank = bindings.recv_peer;
        PG_DEVICE_ASSERT(state.connections &&
                         bindings.channel < kMaxDeviceCollectiveChannels);
        auto* channel_connections =
            state.connections + uint64_t{bindings.channel} * max_group_size;
        PG_DEVICE_ASSERT(state.transfer_handle->peer_accessible_region.contains(
            state.signals,
            uint64_t{state.signal_layout.total_signal_count} * sizeof(uint64_t)));
        send_state_ = channel_connections + send_rank;
        recv_state_ = channel_connections + recv_rank;
        send_cursor_ = device::mc_ld_acquire_u64(&send_state_->send_cursor);
        recv_cursor_ = device::mc_ld_acquire_u64(&recv_state_->recv_cursor);
        buffer_ready_sequence_ =
            device::mc_ld_acquire_u64(&send_state_->buffer_ready_sequence);
    }

    // Stream ordering makes our shared workspace available locally. Notify
    // the sender, then wait for the receiver to make its workspace available.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    begin(cooperative_groups::thread_block block) const {
        const uint64_t ready_offset =
            state_.signal_layout.recvBufferReadyOffset(
                recv_peer_.signal_offset, bindings_.channel, state_.self_rank);
        signalPeer(recv_peer_, ready_offset, block);

        const auto* ready_signal = state_.signal_layout.recvBufferReadyPtr(
            state_.signals, bindings_.channel, bindings_.send_peer);
        return waitForSignal(ready_signal, buffer_ready_sequence_ + 1,
                             bindings_.send_peer, block);
    }

    // Send source[0:count] to the bound outgoing connection.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    send(const CollectiveChunk<T>& chunk,
         cooperative_groups::thread_block block) {
        PG_DEVICE_ASSERT(chunk.count <=
                         bindings_.buffers.chunk_bytes / sizeof(T));
        const auto send = slotFor(send_cursor_);
        const auto available = waitPrevSendConsumed(send, block);
        if (!available.succeeded()) return available;
        const auto payload = outgoingPayload(send);
        copyValuesTo(chunk.source, chunk.count, block,
                     payload.template dataAs<T>());
        publishSend(send, payload, chunk.count, block);
        return {};
    }

    // Receive the next message and send Op(source, received). No local copy.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    recvReduceSend(const CollectiveChunk<T>& chunk,
                   cooperative_groups::thread_block block) {
        PG_DEVICE_ASSERT(chunk.count <=
                         bindings_.buffers.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        const auto send = slotFor(send_cursor_);
        const auto arrived = waitRecvReady(recv, block);
        if (!arrived.succeeded()) return arrived;
        const auto available = waitPrevSendConsumed(send, block);
        if (!available.succeeded()) return available;

        const auto payload = outgoingPayload(send);
        reduceValuesTo<T, Op>(chunk.source, receivedPayload(recv), chunk.count,
                              block, payload.template dataAs<T>());
        publishSend(send, payload, chunk.count, block);
        releaseRecv(recv, block);
        return {};
    }

    // Receive the next message and store Op(source, received) to destination.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    recvReduceCopy(const CollectiveChunk<T>& chunk,
                   cooperative_groups::thread_block block) {
        PG_DEVICE_ASSERT(chunk.count <=
                         bindings_.buffers.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        const auto arrived = waitRecvReady(recv, block);
        if (!arrived.succeeded()) return arrived;

        reduceValuesTo<T, Op>(chunk.source, receivedPayload(recv), chunk.count,
                              block, chunk.destination);
        releaseRecv(recv, block);
        return {};
    }

    // Store Op(source, received) to destination and send the same result.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    recvReduceCopySend(const CollectiveChunk<T>& chunk,
                       cooperative_groups::thread_block block) {
        PG_DEVICE_ASSERT(chunk.count <=
                         bindings_.buffers.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        const auto send = slotFor(send_cursor_);
        const auto arrived = waitRecvReady(recv, block);
        if (!arrived.succeeded()) return arrived;
        const auto available = waitPrevSendConsumed(send, block);
        if (!available.succeeded()) return available;

        const auto payload = outgoingPayload(send);
        reduceValuesTo<T, Op>(chunk.source, receivedPayload(recv), chunk.count,
                              block, chunk.destination,
                              payload.template dataAs<T>());
        publishSend(send, payload, chunk.count, block);
        releaseRecv(recv, block);
        return {};
    }

    // Copy the next received message to destination; source is unused.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    recvCopy(const CollectiveChunk<T>& chunk,
             cooperative_groups::thread_block block) {
        PG_DEVICE_ASSERT(chunk.count <=
                         bindings_.buffers.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        const auto arrived = waitRecvReady(recv, block);
        if (!arrived.succeeded()) return arrived;

        copyValuesTo(receivedPayload(recv), chunk.count, block,
                     chunk.destination);
        releaseRecv(recv, block);
        return {};
    }

    // Copy the next received message to destination and forward it.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    recvCopySend(const CollectiveChunk<T>& chunk,
                 cooperative_groups::thread_block block) {
        PG_DEVICE_ASSERT(chunk.count <=
                         bindings_.buffers.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        const auto send = slotFor(send_cursor_);
        const auto arrived = waitRecvReady(recv, block);
        if (!arrived.succeeded()) return arrived;
        const auto available = waitPrevSendConsumed(send, block);
        if (!available.succeeded()) return available;

        const auto payload = outgoingPayload(send);
        copyValuesTo(receivedPayload(recv), chunk.count, block,
                     chunk.destination, payload.template dataAs<T>());
        publishSend(send, payload, chunk.count, block);
        releaseRecv(recv, block);
        return {};
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    drain(cooperative_groups::thread_block block) {
        if (!send_pending_) return {};
        // Receives release slots in order. The last ACK proves all previous
        // payloads were consumed, including their transfer's local source read.
        const auto result = waitSendConsumed(slotFor(send_cursor_ - 1), block);
        if (result.succeeded()) send_pending_ = false;
        return result;
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    finish(cooperative_groups::thread_block block) {
        const auto result = drain(block);
        if (!result.succeeded()) return result;
        if (block.thread_rank() == 0) {
            device::mc_st_release_u64(&send_state_->send_cursor, send_cursor_);
            device::mc_st_release_u64(&recv_state_->recv_cursor, recv_cursor_);
            device::mc_st_release_u64(&send_state_->buffer_ready_sequence,
                                      buffer_ready_sequence_ + 1);
        }
        return {};
    }

   private:
    [[nodiscard]] __device__ __forceinline__ static const SimplePeer& peerAt(
        const SimpleState<PipelineDepth>& state, InGroupRank rank) {
        PG_DEVICE_ASSERT(state.transfer_handle);
        PG_DEVICE_ASSERT(rank >= 0 &&
                         static_cast<uint32_t>(rank) <
                             state.signal_layout.max_group_size);
        const auto& peer = state.peers[rank];
        PG_DEVICE_ASSERT(peer.global_rank != kInvalidGlobalRank);
        return peer;
    }

    struct SlotPosition {
        uint32_t index;
        uint64_t generation;
    };

    [[nodiscard]] __device__ __forceinline__ static SlotPosition slotFor(
        uint64_t cursor) {
        return {static_cast<uint32_t>(cursor % PipelineDepth),
                cursor / PipelineDepth + 1};
    }

    __device__ __forceinline__ void signalPeer(
        SimplePeer peer, uint64_t offset,
        cooperative_groups::thread_block block) const {
        SignalRequest request;
        request.signal.kind = SignalAction::Kind::Add;
        request.signal.remote_offset = offset;
        request.signal.add.delta = 1;
        request.timeout_ticks = bindings_.timeout_ticks;
        (void)bindings_.lane.signal(peer.global_rank, request, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    waitForSignal(const uint64_t* signal, uint64_t expected, InGroupRank peer,
                  cooperative_groups::thread_block block) const {
        const auto result = bindings_.lane.waitSignal(
            SignalWaitRequest{signal, expected, bindings_.timeout_ticks},
            block);
        if (result.status != SignalWaitStatus::Reached ||
            result.observed != expected) {
            return {peer};
        }
        return {};
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult waitRecvReady(
        SlotPosition slot, cooperative_groups::thread_block block) const {
        const auto* ready_signal = state_.signal_layout.payloadReadyPtr(
            state_.signals, bindings_.channel, bindings_.recv_peer, slot.index);
        return waitForSignal(ready_signal, slot.generation,
                             bindings_.recv_peer, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    waitPrevSendConsumed(SlotPosition slot,
                         cooperative_groups::thread_block block) const {
        // The ACK permits both remote slot overwrite and local staging reuse.
        return waitConsumedSequence(slot.index, slot.generation - 1, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    waitSendConsumed(SlotPosition slot,
                     cooperative_groups::thread_block block) const {
        return waitConsumedSequence(slot.index, slot.generation, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    waitConsumedSequence(uint32_t slot_index, uint64_t sequence,
                         cooperative_groups::thread_block block) const {
        const auto* consumed_signal =
            state_.signal_layout.payloadConsumedPtr(
                state_.signals, bindings_.channel, bindings_.send_peer,
                slot_index);
        return waitForSignal(consumed_signal, sequence, bindings_.send_peer,
                             block);
    }

    [[nodiscard]] __device__ __forceinline__ const T* receivedPayload(
        SlotPosition slot) const {
        return reinterpret_cast<const T*>(
            bindings_.recv_buffer + slot.index * bindings_.buffers.chunk_bytes);
    }

    [[nodiscard]] __device__ __forceinline__ PayloadWriteView
    outgoingPayload(SlotPosition slot) const {
        const uint64_t offset = slot.index * bindings_.buffers.chunk_bytes;
        return send_writer_.view(offset, offset, bindings_.buffers.chunk_bytes);
    }

    __device__ __forceinline__ void publishSend(
        SlotPosition slot, const PayloadWriteView& payload, uint64_t count,
        cooperative_groups::thread_block block) {
        PayloadPublishRequest request;
        request.size = count * sizeof(T);
        request.signal.kind = SignalAction::Kind::Add;
        request.signal.remote_offset =
            state_.signal_layout.payloadReadyOffset(
                send_peer_.signal_offset, bindings_.channel,
                state_.self_rank, slot.index);
        request.signal.add.delta = 1;
        request.timeout_ticks = bindings_.timeout_ticks;
        // Submission stays asynchronous; consumed ACKs gate storage reuse.
        (void)payload.publish(request, block);
        ++send_cursor_;
        send_pending_ = true;
    }

    __device__ __forceinline__ void releaseRecv(
        SlotPosition slot, cooperative_groups::thread_block block) {
        const uint64_t consumed_offset =
            state_.signal_layout.payloadConsumedOffset(
                recv_peer_.signal_offset, bindings_.channel,
                state_.self_rank, slot.index);
        signalPeer(recv_peer_, consumed_offset, block);
        ++recv_cursor_;
    }

    SimpleBindings<PipelineDepth> bindings_;
    const SimpleState<PipelineDepth>& state_;
    SimplePeer recv_peer_;
    SimplePeer send_peer_;
    PayloadWriter send_writer_;
    SimpleConnectionState* send_state_;
    SimpleConnectionState* recv_state_;
    uint64_t send_cursor_;
    uint64_t recv_cursor_;
    uint64_t buffer_ready_sequence_;
    bool send_pending_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_PRIMITIVES_CUH
