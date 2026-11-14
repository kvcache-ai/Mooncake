#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_PRIMITIVES_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_PRIMITIVES_CUH

#include "device_comm/device_collective/protocols/simple/simple_types.cuh"
#include "device_comm/device_primitives/payload_writer.cuh"
#include "device_comm/device_primitives/value_primitives.cuh"

namespace mooncake {

// One invocation's Simple communication primitives, currently with one receive
// and one send connection. send() and Reduce operations read chunk.source;
// Copy operations write chunk.destination. Call begin() once before data
// operations and finish() once after them.
template <typename T, ReduceOp Op>
class SimplePrimitives {
   public:
    [[nodiscard]] __device__ __forceinline__ static SimplePrimitives bind(
        const SimpleBindings& state, const SimpleWorkspace& workspace,
        const CollectivePeer& recv_peer, const CollectivePeer& send_peer,
        uint32_t channel, uint32_t channel_count, uint64_t max_chunk_bytes,
        uint64_t timeout_ticks) {
        PG_ASSERT(state.transfer_handle && channel_count > 0 &&
                  channel_count <= kMaxDeviceCollectiveChannels &&
                  channel < channel_count);
        const auto& handle = *state.transfer_handle;
        PG_ASSERT(handle.peer_accessible_region.contains(workspace.buffer,
                                                         workspace.bytes));
        if (workspace.staging) {
            PG_ASSERT(handle.local_staging_region.contains(workspace.staging,
                                                           workspace.bytes));
        }
        PG_ASSERT(peerAt(state, recv_peer.in_group_rank).global_rank ==
                  recv_peer.global_rank);
        PG_ASSERT(peerAt(state, send_peer.in_group_rank).global_rank ==
                  send_peer.global_rank);
        const auto buffers = SimpleBufferLayout::make(
            workspace.bytes / channel_count, max_chunk_bytes);
        PG_ASSERT(buffers.chunk_bytes >= sizeof(T));
        const uint64_t channel_bytes = buffers.bufferBytes();
        const uint64_t offset = uint64_t{channel} * channel_bytes;
        const auto staging =
            workspace.staging
                ? StagingRegion{workspace.staging + offset, channel_bytes}
                : StagingRegion{};
        return SimplePrimitives(state, buffers, workspace.buffer + offset,
                                recv_peer.in_group_rank,
                                send_peer.in_group_rank, channel, timeout_ticks,
                                staging, send_peer.workspace_offset + offset);
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t chunkElements() const {
        return buffers_.chunk_bytes / sizeof(T);
    }

    // StrongStream ordering makes our shared workspace available at this point.
    // Notify the sender, then wait for the receiver to make its workspace
    // available.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    begin(cooperative_groups::thread_block block) const {
        const uint64_t ready_offset =
            state_.signal_layout.recvBufferReadyOffset(
                recv_peer_.signal_offset, channel_, state_.self_rank);
        signalPeer(recv_peer_, ready_offset, block);

        const auto* ready_signal = state_.signal_layout.recvBufferReadyPtr(
            state_.signals, channel_, send_rank_);
        return waitForSignal(ready_signal, buffer_ready_sequence_ + 1,
                             send_rank_, block);
    }

    // Send source[0:count] to the bound outgoing connection.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    send(const CollectiveChunk<T>& chunk,
         cooperative_groups::thread_block block) {
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
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
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
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
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
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
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
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
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
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
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
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

    // Wait until the last published send has been consumed.
    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    waitSendsConsumed(cooperative_groups::thread_block block) {
        if (!send_pending_) return {};
        // Receives release slots in order. The last ACK proves all previous
        // payloads were consumed, including their transfer's local source read.
        const auto result = waitSendConsumed(slotFor(send_cursor_ - 1), block);
        if (result.succeeded()) send_pending_ = false;
        return result;
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    finish(cooperative_groups::thread_block block) {
        const auto result = waitSendsConsumed(block);
        if (!result.succeeded()) return result;
        if (block.thread_rank() == 0) {
            device::mc_st_release_u64(&send_progress_->send_cursor,
                                      send_cursor_);
            device::mc_st_release_u64(&recv_progress_->recv_cursor,
                                      recv_cursor_);
            device::mc_st_release_u64(&send_progress_->buffer_ready_sequence,
                                      buffer_ready_sequence_ + 1);
        }
        return {};
    }

   private:
    __device__ __forceinline__ SimplePrimitives(
        const SimpleBindings& state, SimpleBufferLayout buffers,
        const char* recv_buffer, InGroupRank recv_rank, InGroupRank send_rank,
        uint32_t channel, uint64_t timeout_ticks, StagingRegion staging,
        uint64_t send_buffer_offset)
        : state_(state),
          lane_(state.transfer_handle->lane(channel)),
          timeout_ticks_(timeout_ticks),
          recv_buffer_(recv_buffer),
          buffers_(buffers),
          recv_rank_(recv_rank),
          send_rank_(send_rank),
          channel_(channel),
          recv_peer_(peerAt(state, recv_rank)),
          send_peer_(peerAt(state, send_rank)),
          send_writer_(
              *state.transfer_handle, lane_, send_peer_.global_rank, staging,
              RemotePayloadRegion{send_buffer_offset, buffers.bufferBytes()}) {
        static_assert(kSimplePayloadAlignment == alignof(ValuePack<T>));
        const uint32_t max_group_size = state.signal_layout.max_group_size;
        PG_ASSERT(state.peer_progress &&
                  channel < kMaxDeviceCollectiveChannels);
        auto* channel_peer_progress =
            state.peer_progress + uint64_t{channel} * max_group_size;
        PG_ASSERT(state.transfer_handle->peer_accessible_region.contains(
            state.signals, uint64_t{state.signal_layout.total_signal_count} *
                               sizeof(uint64_t)));
        send_progress_ = channel_peer_progress + send_rank;
        recv_progress_ = channel_peer_progress + recv_rank;
        send_cursor_ = device::mc_ld_acquire_u64(&send_progress_->send_cursor);
        recv_cursor_ = device::mc_ld_acquire_u64(&recv_progress_->recv_cursor);
        buffer_ready_sequence_ =
            device::mc_ld_acquire_u64(&send_progress_->buffer_ready_sequence);
    }

    [[nodiscard]] __device__ __forceinline__ static const SimplePeer& peerAt(
        const SimpleBindings& state, InGroupRank rank) {
        PG_ASSERT(state.transfer_handle);
        PG_ASSERT(rank >= 0 && static_cast<uint32_t>(rank) <
                                   state.signal_layout.max_group_size);
        const auto& peer = state.peer_bindings[rank];
        PG_ASSERT(peer.global_rank != kInvalidGlobalRank);
        return peer;
    }

    struct SlotPosition {
        uint32_t index;
        uint64_t generation;
    };

    [[nodiscard]] __device__ __forceinline__ static SlotPosition slotFor(
        uint64_t cursor) {
        return {static_cast<uint32_t>(cursor % kSimplePipelineDepth),
                cursor / kSimplePipelineDepth + 1};
    }

    __device__ __forceinline__ void signalPeer(
        SimplePeer peer, uint64_t offset,
        cooperative_groups::thread_block block) const {
        SignalRequest request;
        request.signal.kind = SignalAction::Kind::Add;
        request.signal.remote_offset = offset;
        request.signal.add.delta = 1;
        request.timeout_ticks = timeout_ticks_;
        (void)lane_.signal(peer.global_rank, request, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    waitForSignal(const uint64_t* signal, uint64_t expected, InGroupRank peer,
                  cooperative_groups::thread_block block) const {
        const auto result = lane_.waitSignal(
            SignalWaitRequest{signal, expected, timeout_ticks_}, block);
        if (result.status != SignalWaitStatus::Reached ||
            result.observed != expected) {
            return {peer};
        }
        return {};
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult waitRecvReady(
        SlotPosition slot, cooperative_groups::thread_block block) const {
        const auto* ready_signal = state_.signal_layout.payloadReadyPtr(
            state_.signals, channel_, recv_rank_, slot.index);
        return waitForSignal(ready_signal, slot.generation, recv_rank_, block);
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
        const auto* consumed_signal = state_.signal_layout.payloadConsumedPtr(
            state_.signals, channel_, send_rank_, slot_index);
        return waitForSignal(consumed_signal, sequence, send_rank_, block);
    }

    [[nodiscard]] __device__ __forceinline__ const T* receivedPayload(
        SlotPosition slot) const {
        return reinterpret_cast<const T*>(recv_buffer_ +
                                          slot.index * buffers_.chunk_bytes);
    }

    [[nodiscard]] __device__ __forceinline__ PayloadWriteView
    outgoingPayload(SlotPosition slot) const {
        const uint64_t offset = slot.index * buffers_.chunk_bytes;
        return send_writer_.view(offset, offset, buffers_.chunk_bytes);
    }

    __device__ __forceinline__ void publishSend(
        SlotPosition slot, const PayloadWriteView& payload, uint64_t count,
        cooperative_groups::thread_block block) {
        PayloadPublishRequest request;
        request.size = count * sizeof(T);
        request.signal.kind = SignalAction::Kind::Add;
        request.signal.remote_offset = state_.signal_layout.payloadReadyOffset(
            send_peer_.signal_offset, channel_, state_.self_rank, slot.index);
        request.signal.add.delta = 1;
        request.timeout_ticks = timeout_ticks_;
        // Submission stays asynchronous; consumed ACKs gate storage reuse.
        (void)payload.publish(request, block);
        ++send_cursor_;
        send_pending_ = true;
    }

    __device__ __forceinline__ void releaseRecv(
        SlotPosition slot, cooperative_groups::thread_block block) {
        const uint64_t consumed_offset =
            state_.signal_layout.payloadConsumedOffset(
                recv_peer_.signal_offset, channel_, state_.self_rank,
                slot.index);
        signalPeer(recv_peer_, consumed_offset, block);
        ++recv_cursor_;
    }

    const SimpleBindings& state_;
    TransferLane lane_;
    uint64_t timeout_ticks_;
    const char* recv_buffer_;
    SimpleBufferLayout buffers_;
    InGroupRank recv_rank_;
    InGroupRank send_rank_;
    uint32_t channel_;
    SimplePeer recv_peer_;
    SimplePeer send_peer_;
    PayloadWriter send_writer_;
    SimplePeerProgress* send_progress_;
    SimplePeerProgress* recv_progress_;
    uint64_t send_cursor_;
    uint64_t recv_cursor_;
    uint64_t buffer_ready_sequence_;
    bool send_pending_ = false;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_PRIMITIVES_CUH
