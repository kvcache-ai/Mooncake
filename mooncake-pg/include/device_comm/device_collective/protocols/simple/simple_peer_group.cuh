#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_PEER_GROUP_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_PEER_GROUP_CUH

#include "device_comm/device_collective/protocols/simple/simple_types.cuh"
#include "device_comm/device_transfer/transfer_lane.cuh"

namespace mooncake {

// CTA-local bindings for a full-mesh Simple exchange. One control thread owns
// each peer; data threads share the cached pointers and cursors between phases.
struct SimplePeerGroupStorage {
    struct Connection {
        char* send_buffer;
        const char* recv_buffer;
        uint64_t* remote_ready;
        uint64_t* remote_payload_ready[kSimplePipelineDepth];
        uint64_t* remote_consumed[kSimplePipelineDepth];
        const uint64_t* local_ready;
        const uint64_t* local_payload_ready[kSimplePipelineDepth];
        const uint64_t* local_consumed[kSimplePipelineDepth];
        SimplePeerProgress* persistent;
        uint64_t send_cursor;
        uint64_t recv_cursor;
        uint64_t ready_sequence;
        InGroupRank rank;
    };
    Connection peers[kMaxNumRanks];
    InGroupRank failed_rank;
};

// This is the P2P specialization of Simple's payload/ready/consumed contract.
// It shares progress with Ring, but batches the CTA barriers across peers.
// All methods except the pointer accessors must be called by the whole CTA.
class SimplePeerGroup {
   public:
    enum Wait : uint32_t {
        SendAvailable = 1,
        ReceiveReady = 2,
        SendConsumed = 4,
    };

    __device__ __forceinline__ SimplePeerGroup(SimplePeerGroupStorage& storage,
                                               uint32_t self,
                                               uint32_t participants,
                                               uint64_t chunk_bytes,
                                               uint64_t timeout_ticks)
        : storage_(storage),
          self_(self),
          participants_(participants),
          chunk_bytes_(chunk_bytes),
          timeout_ticks_(timeout_ticks) {}

    __device__ __forceinline__ void bind(
        const SimpleBindings& state, const SimpleWorkspace& workspace,
        RemotePeerList peers, uint32_t channel, uint32_t channels,
        cooperative_groups::thread_block block) const {
        PG_ASSERT(participants_ == peers.size() + 1 && self_ < participants_);
        PG_ASSERT(channel < channels &&
                  channels <= kMaxDeviceCollectiveChannels);
        PG_ASSERT(chunk_bytes_ != 0 && chunk_bytes_ * kSimplePipelineDepth *
                                               participants_ * channels <=
                                           workspace.bytes);
        if (block.thread_rank() == 0)
            storage_.failed_rank = kInvalidInGroupRank;
        const uint64_t slice_bytes = chunk_bytes_ * kSimplePipelineDepth;
        const uint64_t channel_offset = channel * participants_ * slice_bytes;
        const auto& layout = state.signal_layout;
        for (uint32_t i = block.thread_rank(); i < participants_;
             i += block.size()) {
            if (i == self_) continue;
            const auto& peer = peers.atIndex(i < self_ ? i : i - 1);
            PG_ASSERT(state.transfer_handle->routeType(peer.global_rank) ==
                      DeviceRouteType::P2p);
            const auto remote = [&](uint64_t offset) {
                return static_cast<uint64_t*>(
                    state.transfer_handle->remotePtr(peer.global_rank, offset));
            };
            auto& c = storage_.peers[i];
            c.rank = peer.in_group_rank;
            c.send_buffer = static_cast<char*>(state.transfer_handle->remotePtr(
                peer.global_rank,
                peer.workspace_offset + channel_offset + self_ * slice_bytes));
            c.recv_buffer = workspace.buffer + channel_offset + i * slice_bytes;
            c.persistent = state.peer_progress +
                           uint64_t{channel} * layout.max_group_size + c.rank;
            c.send_cursor =
                device::mc_ld_acquire_u64(&c.persistent->send_cursor);
            c.recv_cursor =
                device::mc_ld_acquire_u64(&c.persistent->recv_cursor);
            c.ready_sequence =
                device::mc_ld_acquire_u64(&c.persistent->buffer_ready_sequence);
            const uint64_t signal_offset =
                state.peer_bindings[c.rank].signal_offset;
            c.remote_ready = remote(layout.recvBufferReadyOffset(
                signal_offset, channel, state.self_rank));
            c.local_ready =
                layout.recvBufferReadyPtr(state.signals, channel, c.rank);
            for (uint32_t slot = 0; slot < kSimplePipelineDepth; ++slot) {
                c.remote_payload_ready[slot] = remote(layout.payloadReadyOffset(
                    signal_offset, channel, state.self_rank, slot));
                c.remote_consumed[slot] = remote(layout.payloadConsumedOffset(
                    signal_offset, channel, state.self_rank, slot));
                c.local_payload_ready[slot] = layout.payloadReadyPtr(
                    state.signals, channel, c.rank, slot);
                c.local_consumed[slot] = layout.payloadConsumedPtr(
                    state.signals, channel, c.rank, slot);
            }
        }
        block.sync();
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    begin(cooperative_groups::thread_block block) const {
        device::mc_fence_barrier_fence();
        for (uint32_t i = block.thread_rank(); i < participants_;
             i += block.size()) {
            if (i == self_) continue;
            auto& c = storage_.peers[i];
            // Ring may have used different send and receive peers. Retain its
            // ready counter history; payload/ACK generations below are known
            // locally and can be published without reading the remote counter.
            device::mc_st_release_u64(
                c.remote_ready, device::mc_ld_acquire_u64(c.remote_ready) + 1);
        }
        block.sync();
        for (uint32_t i = block.thread_rank(); i < participants_;
             i += block.size()) {
            if (i != self_) {
                const auto& c = storage_.peers[i];
                check(waitSignal(c.local_ready, c.ready_sequence + 1,
                                 timeout_ticks_),
                      c.rank);
            }
        }
        block.sync();
        return {storage_.failed_rank};
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    wait(uint32_t operations, cooperative_groups::thread_block block) const {
        for (uint32_t i = block.thread_rank(); i < participants_;
             i += block.size()) {
            if (i == self_) continue;
            const auto& c = storage_.peers[i];
            if (operations & ReceiveReady)
                check(waitSignal(c.local_payload_ready[c.recv_cursor %
                                                       kSimplePipelineDepth],
                                 c.recv_cursor / kSimplePipelineDepth + 1,
                                 timeout_ticks_),
                      c.rank);
            if (operations & SendAvailable)
                check(
                    waitSignal(
                        c.local_consumed[c.send_cursor % kSimplePipelineDepth],
                        c.send_cursor / kSimplePipelineDepth, timeout_ticks_),
                    c.rank);
            if ((operations & SendConsumed) && c.send_cursor != 0) {
                const uint64_t last = c.send_cursor - 1;
                check(
                    waitSignal(c.local_consumed[last % kSimplePipelineDepth],
                               last / kSimplePipelineDepth + 1, timeout_ticks_),
                    c.rank);
            }
        }
        // Each peer's acquire is carried to the consuming data threads.
        block.sync();
        return {storage_.failed_rank};
    }

    template <typename T>
    [[nodiscard]] __device__ __forceinline__ T* sendBuffer(
        uint32_t peer) const {
        const auto& c = storage_.peers[peer];
        return reinterpret_cast<T*>(c.send_buffer +
                                    (c.send_cursor % kSimplePipelineDepth) *
                                        chunk_bytes_);
    }

    template <typename T>
    [[nodiscard]] __device__ __forceinline__ const T* recvBuffer(
        uint32_t peer) const {
        const auto& c = storage_.peers[peer];
        return reinterpret_cast<const T*>(
            c.recv_buffer +
            (c.recv_cursor % kSimplePipelineDepth) * chunk_bytes_);
    }

    // Publish payloads and/or acknowledge consumed inputs with one CTA fence.
    // No control thread updates a cursor while data threads still use it.
    template <bool Send, bool Receive>
    __device__ __forceinline__ void advance(
        cooperative_groups::thread_block block) const {
        device::mc_fence_barrier_fence();
        for (uint32_t i = block.thread_rank(); i < participants_;
             i += block.size()) {
            if (i == self_) continue;
            auto& c = storage_.peers[i];
            if constexpr (Receive) {
                device::mc_st_release_u64(
                    c.remote_consumed[c.recv_cursor % kSimplePipelineDepth],
                    c.recv_cursor / kSimplePipelineDepth + 1);
                ++c.recv_cursor;
            }
            if constexpr (Send) {
                device::mc_st_release_u64(
                    c.remote_payload_ready[c.send_cursor %
                                           kSimplePipelineDepth],
                    c.send_cursor / kSimplePipelineDepth + 1);
                ++c.send_cursor;
            }
        }
        block.sync();
    }

    [[nodiscard]] __device__ __forceinline__ CollectiveStepResult
    finish(cooperative_groups::thread_block block) const {
        const auto result = wait(SendConsumed, block);
        if (!result.succeeded()) return result;
        for (uint32_t i = block.thread_rank(); i < participants_;
             i += block.size()) {
            if (i == self_) continue;
            const auto& c = storage_.peers[i];
            device::mc_st_release_u64(&c.persistent->send_cursor,
                                      c.send_cursor);
            device::mc_st_release_u64(&c.persistent->recv_cursor,
                                      c.recv_cursor);
            device::mc_st_release_u64(&c.persistent->buffer_ready_sequence,
                                      c.ready_sequence + 1);
        }
        block.sync();
        return {};
    }

    // The startup CTA uses the same parallel control threads for View sync.
    [[nodiscard]] __device__ __forceinline__ static CollectiveStepResult
    synchronizeView(uint64_t epoch, uint64_t timeout_ticks,
                    const uint64_t* local_signals, RemotePeerList peers,
                    const DeviceTransferHandle& transfer,
                    InGroupRank& failed_rank,
                    cooperative_groups::thread_block block) {
        if (block.thread_rank() == 0) failed_rank = kInvalidInGroupRank;
        device::mc_fence_barrier_fence();
        for (uint32_t i = block.thread_rank(); i < peers.size();
             i += block.size()) {
            const auto& peer = peers.atIndex(i);
            auto* signal = static_cast<uint64_t*>(transfer.remotePtr(
                peer.global_rank, peer.view_epoch_signal_offset));
            PG_ASSERT(signal);
            device::mc_st_release_u64(signal, epoch);
        }
        block.sync();
        for (uint32_t i = block.thread_rank(); i < peers.size();
             i += block.size()) {
            const auto& peer = peers.atIndex(i);
            if (!waitSignal(local_signals + peer.in_group_rank, epoch,
                            timeout_ticks))
                atomicCAS(&failed_rank, kInvalidInGroupRank,
                          peer.in_group_rank);
        }
        block.sync();
        return {failed_rank};
    }

   private:
    __device__ __forceinline__ void check(bool success,
                                          InGroupRank rank) const {
        if (!success)
            atomicCAS(&storage_.failed_rank, kInvalidInGroupRank, rank);
    }

    [[nodiscard]] __device__ __forceinline__ static bool waitSignal(
        const uint64_t* signal, uint64_t expected, uint64_t timeout_ticks) {
        const uint64_t start = clock64();
        do {
            if (signalReached(device::mc_ld_volatile_u64(signal), expected)) {
                const uint64_t observed = device::mc_ld_acquire_u64(signal);
                if (signalReached(observed, expected))
                    return observed == expected;
            }
        } while (!deviceTimedOut(start, timeout_ticks));
        return false;
    }

    SimplePeerGroupStorage& storage_;
    uint32_t self_;
    uint32_t participants_;
    uint64_t chunk_bytes_;
    uint64_t timeout_ticks_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_SIMPLE_PEER_GROUP_CUH
