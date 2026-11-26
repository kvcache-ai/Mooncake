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
template <typename T>
class SimplePrimitives {
   public:
    [[nodiscard]] __device__ __forceinline__ static SimplePrimitives bind(
        const SimpleState& state, const SimpleWorkspace& workspace,
        const DeviceGroupView& view, uint32_t recv_active_index,
        uint32_t send_active_index, uint32_t channel, uint32_t channel_count,
        uint64_t max_chunk_bytes, uint64_t timeout_ticks) {
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
        PG_ASSERT(recv_active_index < view.participant_count &&
                  send_active_index < view.participant_count);
        const auto buffers = SimpleBufferLayout::make(
            workspace.bytes / channel_count, max_chunk_bytes);
        PG_ASSERT(buffers.chunk_bytes >= sizeof(T));
        const uint64_t channel_bytes = buffers.bufferBytes();
        const uint64_t offset = uint64_t{channel} * channel_bytes;
        const auto staging =
            workspace.staging
                ? StagingRegion{workspace.staging + offset, channel_bytes}
                : StagingRegion{};
        return SimplePrimitives(
            state, view, buffers, workspace.buffer + offset, recv_active_index,
            send_active_index, channel, timeout_ticks, staging,
            view.participants[send_active_index].workspace_offset + offset);
    }

    [[nodiscard]] __device__ __forceinline__ uint64_t chunkElements() const {
        return buffers_.chunk_bytes / sizeof(T);
    }

    // StrongStream ordering makes our shared workspace available at this point.
    // Notify the sender, then wait for the receiver to make its workspace
    // available.
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    begin(cooperative_groups::thread_block block) const {
        const uint64_t ready_offset =
            state_.signal_layout.recvBufferReadyOffset(
                recv_signal_offset_, channel_, view_.self().in_group_rank);
        signalPeer(recv_peer_, ready_offset, block);

        const auto* ready_signal = state_.signal_layout.recvBufferReadyPtr(
            state_.signals, channel_, send_peer_.in_group_rank);
        return waitForSignal(ready_signal, buffer_ready_sequence_ + 1,
                             send_peer_.in_group_rank, block);
    }

    // Send source[0:count] to the bound outgoing connection.
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    send(const CollectiveChunk<T>& chunk,
         cooperative_groups::thread_block block) {
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
        const auto send = slotFor(send_cursor_);
        PG_COLL_TRY(waitPrevSendConsumed(send, block));
        const auto payload = outgoingPayload(send);
        copyValuesTo(chunk.source, chunk.count, block,
                     payload.template dataAs<T>());
        publishSend(send, payload, chunk.count, block);
        return {};
    }

    // Store source to destination and send the same values.
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    copySend(const CollectiveChunk<T>& chunk,
             cooperative_groups::thread_block block) {
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
        const auto send = slotFor(send_cursor_);
        PG_COLL_TRY(waitPrevSendConsumed(send, block));

        const auto payload = outgoingPayload(send);
        copyValuesTo(chunk.source, chunk.count, block, chunk.destination,
                     payload.template dataAs<T>());
        publishSend(send, payload, chunk.count, block);
        return {};
    }

    // Receive the next message and send Op(source, received). No local copy.
    template <ReduceOp Op>
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    recvReduceSend(const CollectiveChunk<T>& chunk,
                   cooperative_groups::thread_block block) {
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        const auto send = slotFor(send_cursor_);
        PG_COLL_TRY(waitRecvReady(recv, block));
        PG_COLL_TRY(waitPrevSendConsumed(send, block));

        const auto payload = outgoingPayload(send);
        reduceValuesTo<T, Op>(chunk.source, receivedPayload(recv), chunk.count,
                              block, payload.template dataAs<T>());
        publishSend(send, payload, chunk.count, block);
        releaseRecv(recv, block);
        return {};
    }

    // Receive the next message and store Op(source, received) to destination.
    template <ReduceOp Op>
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    recvReduceCopy(const CollectiveChunk<T>& chunk,
                   cooperative_groups::thread_block block) {
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        PG_COLL_TRY(waitRecvReady(recv, block));

        reduceValuesTo<T, Op>(chunk.source, receivedPayload(recv), chunk.count,
                              block, chunk.destination);
        releaseRecv(recv, block);
        return {};
    }

    // Store Op(source, received) to destination and send the same result.
    template <ReduceOp Op>
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    recvReduceCopySend(const CollectiveChunk<T>& chunk,
                       cooperative_groups::thread_block block) {
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        const auto send = slotFor(send_cursor_);
        PG_COLL_TRY(waitRecvReady(recv, block));
        PG_COLL_TRY(waitPrevSendConsumed(send, block));

        const auto payload = outgoingPayload(send);
        reduceValuesTo<T, Op>(chunk.source, receivedPayload(recv), chunk.count,
                              block, chunk.destination,
                              payload.template dataAs<T>());
        publishSend(send, payload, chunk.count, block);
        releaseRecv(recv, block);
        return {};
    }

    // Copy the next received message to destination; source is unused.
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    recvCopy(const CollectiveChunk<T>& chunk,
             cooperative_groups::thread_block block) {
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        PG_COLL_TRY(waitRecvReady(recv, block));

        copyValuesTo(receivedPayload(recv), chunk.count, block,
                     chunk.destination);
        releaseRecv(recv, block);
        return {};
    }

    // Copy the next received message to destination and forward it.
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    recvCopySend(const CollectiveChunk<T>& chunk,
                 cooperative_groups::thread_block block) {
        PG_ASSERT(chunk.count <= buffers_.chunk_bytes / sizeof(T));
        const auto recv = slotFor(recv_cursor_);
        const auto send = slotFor(send_cursor_);
        PG_COLL_TRY(waitRecvReady(recv, block));
        PG_COLL_TRY(waitPrevSendConsumed(send, block));

        const auto payload = outgoingPayload(send);
        copyValuesTo(receivedPayload(recv), chunk.count, block,
                     chunk.destination, payload.template dataAs<T>());
        publishSend(send, payload, chunk.count, block);
        releaseRecv(recv, block);
        return {};
    }

    // Wait until the last published send has been consumed.
    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    waitSendsConsumed(cooperative_groups::thread_block block) {
        if (!send_pending_) return {};
        // Receives release slots in order. The last ACK proves all previous
        // payloads were consumed, including their transfer's local source read.
        const auto result = waitSendConsumed(slotFor(send_cursor_ - 1), block);
        if (result.succeeded()) send_pending_ = false;
        return result;
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    finish(cooperative_groups::thread_block block) {
        PG_COLL_TRY(waitSendsConsumed(block));
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
    __device__ __forceinline__
    SimplePrimitives(const SimpleState& state, const DeviceGroupView& view,
                     SimpleBufferLayout buffers, const char* recv_buffer,
                     uint32_t recv_active_index, uint32_t send_active_index,
                     uint32_t channel, uint64_t timeout_ticks,
                     StagingRegion staging, uint64_t send_buffer_offset)
        : state_(state),
          view_(view),
          lane_(state.transfer_handle->lane(channel)),
          timeout_ticks_(timeout_ticks),
          recv_buffer_(recv_buffer),
          buffers_(buffers),
          channel_(channel),
          recv_peer_(view.participants[recv_active_index]),
          send_peer_(view.participants[send_active_index]),
          recv_signal_offset_(state.signal_offsets[recv_active_index]),
          send_signal_offset_(state.signal_offsets[send_active_index]),
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
        send_progress_ = channel_peer_progress + send_peer_.in_group_rank;
        recv_progress_ = channel_peer_progress + recv_peer_.in_group_rank;
        send_cursor_ = device::mc_ld_acquire_u64(&send_progress_->send_cursor);
        recv_cursor_ = device::mc_ld_acquire_u64(&recv_progress_->recv_cursor);
        buffer_ready_sequence_ =
            device::mc_ld_acquire_u64(&send_progress_->buffer_ready_sequence);
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
        const CollectivePeer& peer, uint64_t offset,
        cooperative_groups::thread_block block) const {
        SignalRequest request;
        request.signal.kind = SignalAction::Kind::Add;
        request.signal.remote_offset = offset;
        request.signal.add.delta = 1;
        request.timeout_ticks = timeout_ticks_;
        (void)lane_.signal(peer.global_rank, request, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveResult
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

    [[nodiscard]] __device__ __forceinline__ CollectiveResult waitRecvReady(
        SlotPosition slot, cooperative_groups::thread_block block) const {
        const auto* ready_signal = state_.signal_layout.payloadReadyPtr(
            state_.signals, channel_, recv_peer_.in_group_rank, slot.index);
        return waitForSignal(ready_signal, slot.generation,
                             recv_peer_.in_group_rank, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    waitPrevSendConsumed(SlotPosition slot,
                         cooperative_groups::thread_block block) const {
        // The ACK permits both remote slot overwrite and local staging reuse.
        return waitConsumedSequence(slot.index, slot.generation - 1, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveResult waitSendConsumed(
        SlotPosition slot, cooperative_groups::thread_block block) const {
        return waitConsumedSequence(slot.index, slot.generation, block);
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveResult
    waitConsumedSequence(uint32_t slot_index, uint64_t sequence,
                         cooperative_groups::thread_block block) const {
        const auto* consumed_signal = state_.signal_layout.payloadConsumedPtr(
            state_.signals, channel_, send_peer_.in_group_rank, slot_index);
        return waitForSignal(consumed_signal, sequence,
                             send_peer_.in_group_rank, block);
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
            send_signal_offset_, channel_, view_.self().in_group_rank,
            slot.index);
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
                recv_signal_offset_, channel_, view_.self().in_group_rank,
                slot.index);
        signalPeer(recv_peer_, consumed_offset, block);
        ++recv_cursor_;
    }

    const SimpleState& state_;
    const DeviceGroupView& view_;
    TransferLane lane_;
    uint64_t timeout_ticks_;
    const char* recv_buffer_;
    SimpleBufferLayout buffers_;
    uint32_t channel_;
    const CollectivePeer& recv_peer_;
    const CollectivePeer& send_peer_;
    uint64_t recv_signal_offset_;
    uint64_t send_signal_offset_;
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
