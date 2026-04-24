#ifndef MOONCAKE_P2P_PROXY_H
#define MOONCAKE_P2P_PROXY_H

#include <mooncake_worker.cuh>
#include <torch/torch.h>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mooncake {

// Memory layout of one P2PProxy (shown for chunk_size=8MiB, num_chunks=32):
//
//   +---------------------------------------------------------+
//   |  SendPool  (32 x 8 MiB = 256 MiB)                       |
//   |  +---------+  +---------+        +---------+            |
//   |  | chunk 0 |  | chunk 1 |  ...   | chunk 31|            |
//   |  | 8 MiB   |  | 8 MiB   |        | 8 MiB   |            |
//   |  +---------+  +---------+        +---------+            |
//   +---------------------------------------------------------+
//   |  RecvPool (32 x 8 MiB = 256 MiB)                        |
//   |  +---------+  +---------+        +---------+            |
//   |  | chunk 0 |  | chunk 1 |  ...   | chunk 31|            |
//   |  | 8 MiB   |  | 8 MiB   |        | 8 MiB   |            |
//   |  +---------+  +---------+        +---------+            |
//   +---------------------------------------------------------+
//   |  Publish Control Region                                 |
//   |  [peer 0 lane : 64 slots][peer 1 lane : 64 slots] ...   |
//   +---------------------------------------------------------+
//   |  Completion Control Region                              |
//   |  [peer 0 lane : 64 slots][peer 1 lane : 64 slots] ...   |
//   +---------------------------------------------------------+
//
// Control lane addressing (isolated per sender-receiver pair):
//
//      Rank R wants to write a PublishSlot to peer P
//      -----------------------------------------------------
//      Target = P's publish base
//               + (R * kP2PControlRingSize + seq % 64) * sizeof(PublishSlot)
//
//      Rank R wants to write a CompletionSlot to peer P
//      -----------------------------------------------------
//      Target = P's completion base
//               + (R * kP2PControlRingSize + seq % 64) * sizeof(CompletionSlot)
//
//
// P2PProxy implements a credit-based RDMA pull protocol.
//
// Protocol overview:
//   - The RECEIVER drives the flow. It allocates chunks from its RecvPool and
//     writes PublishSlots into the sender's PublishLane to invite the sender
//     to write data into the designated RecvPool offset.
//   - The SENDER is passive. It polls its local PublishLane (written by the
//     receiver via RDMA). Only after receiving an invitation does it allocate
//     a staging buffer from SendPool, copy user tensor data into it, and
//     perform the RDMA Write to the remote RecvPool.
//   - After the RDMA Write finishes, the sender writes a CompletionSlot back
//     to the receiver's CompletionLane to acknowledge the transfer.
//
// Memory model:
//   - SendPool / RecvPool are fixed-size chunk pools.
//   - Control lanes are ring buffers.
//
// Per-chunk state machines:
//   Sender:  Fetch Invite -> Copy-In (staging) -> RDMA Write -> Acknowledge
//   Receiver: Advertise -> Poll Completion -> Copy-Out -> Free Chunk
//
// Concurrency rules:
//   - All pool allocations are non-blocking. If a pool is exhausted the step
//     function returns false immediately and retries on the next polling
//     iteration. This prevents deadlocks.
//   - SendPool and RecvPool are strictly separated so that a deadlock where
//     "all chunks are reserved for receiving and none are left for sending"
//     can never happen.
//
// Data flow example -- Rank 0 sends a 24 MiB tensor to Rank 1.
// The five steps below are separated between Sender (Rank 0) and
// Receiver (Rank 1) to make the protocol explicit.
//
//   Step 1 -- Rank 1 advertises free RecvPool chunks to Rank 0
//   ------------------------------------------------------------------------
//   Rank 1 (Receiver)                RDMA Write
//   +-----------------------+              +-------------------------------+
//   | RecvPool              |              | Rank 0's PublishLane[1]       |
//   | chunk 7 : free        | -----------> | slot 0 : {seq=0,off=7,len=8M} |
//   | chunk 2 : free        | -----------> | slot 1 : {seq=1,off=2,len=8M} |
//   | chunk 1 : free        | -----------> | slot 2 : {seq=2,off=1,len=8M} |
//   +-----------------------+              +-------------------------------+
//
//   Step 2 -- Rank 0 stages user tensor into SendPool
//   ------------------------------------------------------------------------
//   Rank 0 (Sender)
//   +-----------------------+
//   | User tensor           |
//   |[0..8M)[8..16M)[16..24)|
//   +-----------------------+
//            |
//            | cudaMemcpyAsync (device-to-device)
//            v
//   +----------------------------+
//   | SendPool                   |
//   | chunk 3 : bytes [0..8M)    |
//   | chunk 0 : bytes [8..16M)   |
//   | chunk 5 : bytes [16..24M)  |
//   +----------------------------+
//
//   Step 3 -- Rank 0 RDMA-writes SendPool -> Rank 1's RecvPool
//   ------------------------------------------------------------------------
//   Rank 0's SendPool                    RDMA Write       Rank 1's RecvPool
//   +---------------------+                           +---------------------+
//   | chunk 3 : [0..8M)   | ------------------------> | chunk 7 : [0..8M)   |
//   | chunk 0 : [8..16M)  | -------------------- ---> | chunk 2 : [8..16M)  |
//   | chunk 5 : [16..24M) | ------------------------> | chunk 1 : [16..24M) |
//   +---------------------+                           +---------------------+
//
//   Step 4 -- Rank 0 acknowledges with CompletionSlots
//   ------------------------------------------------------------------------
//   Rank 0 (Sender)                      RDMA Write       Rank 1 (Receiver)
//   +-----------------------+               +-------------------------------+
//   | CompletionLane[0]     |               | Rank 1's CompletionLane[0]    |
//   | (local cache only)    | ------------> | slot 0 : {seq=0,len=8M,gen=G} |
//   +-----------------------+               | slot 1 : {seq=1,len=8M,gen=G} |
//                                           | slot 2 : {seq=2,len=8M,gen=G} |
//                                           +-------------------------------+
//
//   Step 5 -- Rank 1 copies out and recycles chunks
//   ------------------------------------------------------------------------
//   Rank 1 (Receiver)
//   +-----------------------+
//   | RecvPool              |
//   | chunk 7 : [0..8M)     |
//   | chunk 2 : [8..16M)    |
//   | chunk 1 : [16..24M)   |
//   +-----------------------+
//            |
//            | cudaMemcpyAsync (RecvPool -> user tensor)
//            v
//   +-----------------------+
//   | User tensor (ready)   |
//   |[0..8M)[8..16M)[16..24)|
//   +-----------------------+
//            |
//            | Free chunks back to pool
//            v
//   +-----------------------+
//   | RecvPool              |
//   | chunk 7 : free        |
//   | chunk 2 : free        |
//   | chunk 1 : free        |
//   +-----------------------+
//
// ---------------------------------------------------------------------------
inline constexpr uint32_t kP2PControlRingSize = 64;
// default config: pool_size=128M, chunk_size=16M, num_chunks=8
inline constexpr uint64_t kDefaultPoolSize = 128u * 1024 * 1024;  // 128 M
inline constexpr uint64_t kDefaultChunkSize = 16u * 1024 * 1024;  // 16 M

// Single-word atomic publication token.
// Combines generation and sequence to avoid torn reads.
// kInvalidControlToken (all 1s) means "slot empty / not yet published".
using ControlToken = uint64_t;
inline constexpr ControlToken kInvalidControlToken =
    std::numeric_limits<uint64_t>::max();

inline ControlToken makeControlToken(uint32_t generation, uint32_t sequence) {
    return (static_cast<ControlToken>(generation) << 32) |
           static_cast<ControlToken>(sequence & 0xFFFFFFFFu);
}

// PublishSlot
//
// Header-Footer double-token guard:
//   We store the same token at both ends of the 64-byte block.
//     - Producer: write payload -> write footer -> write header (release).
//     - Consumer: read header -> read payload -> read footer -> accept only
//                 when header == footer.
// If the NIC has only partially written the slot, header and footer will
// mismatch (or one of them will still be kInvalidControlToken) and the
// consumer safely retries on the next poll iteration.
//
// Layout:
//   [0..7]   header_token  (generation << 32 | sequence)
//   [8..15]  recv_addr     (payload)
//   [16..19] chunk_len     (payload)
//   [20..55] padding       (kept zero, reserved for future use)
//   [56..63] footer_token  (identical to header_token)
struct alignas(64) PublishSlot {
   private:
    uint64_t header_token = kInvalidControlToken;
    uint64_t recv_addr = 0;
    uint32_t chunk_len = 0;
    uint32_t _reserved = 0;
    uint8_t _padding[32]{};
    uint64_t footer_token = kInvalidControlToken;

   public:
    // Consumer-side reliable read.  Returns true when a consistent slot is
    // observed (header == footer != kInvalidControlToken).
    bool tryLoad(uint64_t& out_recv_addr, uint32_t& out_chunk_len,
                 uint32_t& out_generation, uint32_t& out_sequence) const;

    // Producer-side reliable publish.  Writes payload and footer first,
    // then releases header so consumers see a consistent slot.
    void publish(uint32_t gen, uint32_t seq, uint64_t addr, uint32_t len);

    // Reset both tokens to kInvalidControlToken.
    void reset();
};

// CompletionSlot Layout:
//   [0..7]   header_token  (generation << 32 | sequence)
//   [8..11]  chunk_len     (payload)
//   [12..55] padding       (kept zero, reserved for future use)
//   [56..63] footer_token  (identical to header_token)
struct alignas(64) CompletionSlot {
   private:
    uint64_t header_token = kInvalidControlToken;
    uint32_t chunk_len = 0;
    uint32_t _reserved = 0;
    uint8_t _padding[40]{};
    uint64_t footer_token = kInvalidControlToken;

   public:
    // See PublishSlot::tryLoad().
    bool tryLoad(uint32_t& out_chunk_len, uint32_t& out_generation,
                 uint32_t& out_sequence) const;

    // See PublishSlot::publish().
    void publish(uint32_t gen, uint32_t seq, uint32_t len);

    // Reset both tokens to kInvalidControlToken.
    void reset();
};

// P2PChunkPool
// No lock needed because it is only accessed by one worker thread.
class P2PChunkPool {
   public:
    P2PChunkPool() = default;

    // Initialize pool with given base address, chunk size and number of chunks.
    void init(void* base_addr, size_t chunk_size, uint32_t num_chunks);

    // Acquire a free chunk. Returns nullptr if the pool is exhausted.
    void* acquire();

    // Release a chunk back to the pool.
    void release(void* ptr);

   private:
    void* base_addr_ = nullptr;
    size_t chunk_size_ = 0;
    std::vector<void*> free_stack_;
};

class P2PDeviceWorker;
class P2PProxy {
    static constexpr size_t kDrainTasksTimeoutMs = 5000;  // 5s

   public:
    friend class P2PDeviceWorker;

    enum class OpStatus : uint8_t { kPending = 0, kSuccess = 1, kFailed = 2 };

    struct Options {
        bool is_cpu = false;
        int rank = 0;
        int size = 0;
        int cuda_device_index = -1;
        std::string location;
        std::chrono::milliseconds transfer_timeout_ms{30000};
    };

    struct SendOp {
        at::Tensor tensor_;
        int peer_rank_ = -1;
        cudaStream_t cuda_stream_ = nullptr;
        std::shared_ptr<std::atomic<OpStatus>> status_;
    };

    struct RecvOp {
        at::Tensor tensor_;
        at::Tensor original_tensor_;
        int peer_rank_ = -1;
        cudaStream_t cuda_stream_ = nullptr;
        std::shared_ptr<std::atomic<OpStatus>> status_;
    };

    P2PProxy(TransferEngine* engine, const Options& options);
    ~P2PProxy();

    void* send_pool_base() const { return resources_.send_pool_base_; }
    void* recv_pool_base() const { return resources_.recv_pool_base_; }
    PublishSlot* publish_region() const { return resources_.publish_region_; }
    CompletionSlot* completion_region() const {
        return resources_.completion_region_;
    }
    void bindMeta(const std::shared_ptr<TransferGroupMeta>& meta);
    void extendGroupSizeTo(int new_size);

    void enqueueSend(SendOp op);
    void enqueueRecv(RecvOp op);

    void resetPeerState(int peer_rank);

    /**
     * @brief Waits for all active P2P send and receive tasks to complete.
     *
     * Used during graceful shutdown to ensure no pending P2P operations
     * are active before releasing resources. Blocks until all tasks complete
     * or the timeout expires.
     *
     * @return True if all tasks completed within the timeout; false if timed
     * out.
     */
    bool drainTasks() const;

    /**
     * @brief Abandons resources instead of releasing them properly.
     *
     * When a hung operation prevents clean shutdown, this method marks
     * resources as abandoned to prevent crashes during destructor.
     */
    void abandonResources();

    // Generation for fault recovery.  All control slots carry this
    // value so that stale messages from before a Reset can be detected.
    uint32_t getGeneration(int peer_rank) const {
        return peer_generation_[peer_rank].load(std::memory_order_acquire);
    }
    void setGeneration(int peer_rank, uint32_t generation) {
        peer_generation_[peer_rank].store(generation,
                                          std::memory_order_release);
    }

   private:
    // Sender-side per-chunk state machine.
    enum class SendTaskState {
        kCopyIn,  // Copying tensor slice into local SendPool staging buffer.
                  // On CPU this is synchronous; on GPU we record a
                  // cudaEvent on the op's stream and poll it.
        kWriteRemote,  // Write from SendPool -> remote RecvPool.
        kCompletion,   // Writing CompletionSlot back to the receiver.
        kFinished,     // CompletionSlot write done, staging buffer freed.
        kFailed,       // Transport error or timeout; staging buffer freed.
    };

    // Receiver-side per-chunk state machine.
    enum class RecvTaskState {
        kAdvertise,       // PublishSlot Write is in flight.
        kWaitCompletion,  // Waiting for sender's CompletionSlot.
        kCopyOut,         // GPU: cudaMemcpyAsync RecvPool -> tensor in flight.
        kFinished,        // Copy-out done, chunk returned to pool.
        kFailed,          // Transport error or timeout; chunk returned to pool.
    };

    struct SendOpContext;
    struct RecvOpContext;

    // One chunk of a SendOp.  The sender copies data from the user tensor
    // into a SendPool staging buffer and then RDMA-writes it to the remote
    // RecvPool offset that the receiver advertised in the PublishSlot.
    struct SendTransferTask {
        SendTransferTask() = default;
        SendTransferTask(uint64_t tensor_offset_in, uint32_t chunk_len_in,
                         void* staging_addr_in, uint64_t remote_addr_in,
                         uint32_t sequence_in, uint32_t generation_in);

        SendTaskState state_ = SendTaskState::kCopyIn;
        uint64_t tensor_offset_ = 0;  // Offset inside the user tensor.
        uint32_t chunk_len_ = 0;      // Bytes in this chunk (<= kP2PChunkSize).
        void* staging_addr_ = nullptr;  // Address inside SendPool.
        uint64_t remote_addr_ = 0;      // Address inside REMOTE RecvPool.
        uint32_t sequence_ = 0;         // Sequence number in the control ring.
        uint32_t generation_ = 0;       // Generation for Reset detection.
        std::optional<BatchID> transfer_batch_id_;  // RDMA Write batch id.
        std::optional<BatchID>
            completion_batch_id_;  // CompletionSlot write batch id.
        cudaEvent_t copy_ready_event_ = nullptr;  // Signals Copy-In done (GPU).
        std::chrono::steady_clock::time_point last_update_time_;
    };

    // Active send operation state.  Tasks are created in order and advance
    // through the sender state machine (kCopyIn -> kWriteRemote -> kCompletion
    // -> kFinished).
    struct SendOpContext {
        SendOpContext() = default;
        SendOpContext(SendOp&& op_in);

        std::deque<SendTransferTask> tasks_;
        std::shared_ptr<std::atomic<OpStatus>> status_;

        at::Tensor tensor_;
        int peer_rank_ = -1;
        cudaStream_t cuda_stream_ = nullptr;
        uint64_t total_bytes_ = 0;
        // Number of bytes already pulled from the user tensor into SendPool
        // staging buffers.  When bytes_staged_ == total_bytes_ every chunk
        // has at least entered the Copy-In stage.
        uint64_t bytes_staged_ = 0;

        std::chrono::steady_clock::time_point last_update_time_;
    };

    // One chunk of a RecvOp.  The receiver allocates a RecvPool chunk,
    // advertises it to the sender via a PublishSlot, waits for the sender
    // to RDMA-write data into it, and finally copies the data into the user
    // tensor before returning the chunk to RecvPool.
    struct RecvTransferTask {
        RecvTransferTask() = default;
        RecvTransferTask(uint64_t tensor_offset_in, uint32_t chunk_len_in,
                         void* local_addr_in, uint32_t sequence_in,
                         uint32_t generation_in);

        RecvTaskState state_ = RecvTaskState::kAdvertise;
        uint64_t tensor_offset_ = 0;  // Offset inside the user tensor.
        uint32_t chunk_len_ = 0;      // Bytes in this chunk.
        void* local_addr_ = nullptr;  // Address inside local RecvPool.
        uint32_t sequence_ = 0;       // Sequence number in the control ring.
        uint32_t generation_ = 0;     // Generation for Reset detection.
        std::optional<BatchID>
            publish_batch_id_;  // PublishSlot RDMA Write batch id.
        cudaEvent_t copy_ready_event_ =
            nullptr;  // Signals Copy-Out done (GPU).
        std::chrono::steady_clock::time_point last_update_time_;
    };

    // Active receive operation state.  Tasks are created in order and
    // advance through the receiver state machine (kAdvertise ->
    // kWaitCompletion -> kCopyOut -> kFinished).
    struct RecvOpContext {
        RecvOpContext() = default;
        RecvOpContext(RecvOp&& op_in);

        std::deque<RecvTransferTask> tasks_;
        std::shared_ptr<std::atomic<OpStatus>> status_;

        at::Tensor tensor_;
        at::Tensor original_tensor_;
        int peer_rank_ = -1;
        cudaStream_t cuda_stream_ = nullptr;
        uint64_t total_bytes_ = 0;
        // Number of bytes for which a RecvPool chunk has been reserved and a
        // PublishSlot has been sent to the peer.  When bytes_advertised_ ==
        // total_bytes_ the entire tensor has been offered to the sender.
        uint64_t bytes_advertised_ = 0;
    };

    // Per-peer sender state.  The sender consumes PublishSlots that the
    // receiver writes into our local PublishLane for this peer.
    struct SendPeerLane {
        std::deque<SendOpContext> pending_send_ops_;
        std::optional<SendOpContext> active_send_op_;
        // Sequence number of the next PublishSlot to consume from this peer.
        // Monotonically increases; wraps around the ring via modulo.
        uint64_t publish_consume_seq_ = 0;
        std::array<cudaEvent_t, kP2PControlRingSize> copy_ready_events_;
    };

    // Per-peer receiver state.  The receiver issues PublishSlots to invite
    // the peer to write data, and consumes CompletionSlots that the peer
    // writes back to acknowledge finished transfers.
    struct RecvPeerLane {
        std::deque<RecvOp> pending_recv_ops_;
        std::optional<RecvOpContext> active_recv_op_;
        // Sequence number of the next PublishSlot to issue to this peer.
        uint64_t publish_issue_seq_ = 0;
        // Sequence number of the next CompletionSlot to consume from this peer.
        uint64_t completion_consume_seq_ = 0;
        std::array<cudaEvent_t, kP2PControlRingSize> copy_ready_events_;
    };

    // Resources are allocated and released by constructor/destructor
    void allocateResources();
    void releaseResources();

    // For P2PDeviceWorker
    bool stepSend();
    bool stepRecv();
    void setDeviceWorker(P2PDeviceWorker*);
    bool hasActiveSendWork() const;
    bool hasActiveRecvWork() const;

    bool tryIssueRecvTask(RecvOpContext& op_ctx, RecvPeerLane& lane);
    bool stepRecvTask(RecvTransferTask& task);
    bool stepRecvCopyOut(RecvTransferTask& task);
    bool stepRecvAdvertise(RecvTransferTask& task);
    bool pollRecvCompletionSlot(RecvOpContext& op_ctx, RecvPeerLane& lane,
                                RecvTransferTask& head_task);
    bool isRecvOpCompleted(const RecvOpContext& op_ctx) const;
    void performRecvReset(int peer_rank);

    bool tryIssueSendTask(SendOpContext& op_ctx, SendPeerLane& lane);
    bool stepSendTask(SendOpContext& op_ctx, SendTransferTask& task);
    bool stepSendCopyIn(SendTransferTask& task);
    bool stepSendWriteRemote(SendOpContext& op_ctx, SendTransferTask& task);
    bool stepSendCompletion(SendOpContext& op_ctx, SendTransferTask& task);
    bool isSendOpCompleted(const SendOpContext& op_ctx) const;
    void performSendReset(int peer_rank);

    void reportBrokenPeer(int peer_rank);

    // Control lane addressing.
    //
    // Each rank owns one contiguous Publish region and one Completion region.
    // Within each region there are kMaxNumRanks lanes, one per peer.
    // Lane index == peer rank.  Each lane has kP2PControlRingSize slots.
    //
    // Example: rank R wants to write a PublishSlot to peer P.
    //   target = P's publish base + (R * kRingSize + seq % kRingSize) * sizeof
    //
    // This isolates control traffic per (sender, receiver) pair.
    PublishSlot* getLocalPublishLane(int peer_rank) const;
    CompletionSlot* getLocalCompletionLane(int peer_rank) const;
    uint64_t getRemotePublishSlot(int peer_rank, uint32_t sequence) const;
    uint64_t getRemoteCompletionSlot(int peer_rank, uint32_t sequence) const;

    PublishSlot* getLocalPublishStagingBuf(int peer_rank,
                                           uint32_t sequence) const;
    CompletionSlot* getLocalCompletionStagingBuf(int peer_rank,
                                                 uint32_t sequence) const;

    template <typename T>
    bool isTimeout(const T& obj) const {
        return std::chrono::steady_clock::now() - obj.last_update_time_ >
               transfer_timeout_ms_;
    }

   private:
    struct P2PResources {
        void* send_pool_base_ = nullptr;
        void* recv_pool_base_ = nullptr;
        PublishSlot* publish_region_ = nullptr;
        CompletionSlot* completion_region_ = nullptr;
        // Serve as RDMA write source address
        PublishSlot* publish_staging_buf_ = nullptr;
        CompletionSlot* completion_staging_buf_ = nullptr;
    };

    P2PDeviceWorker* device_worker_ = nullptr;

    TransferEngine* engine_ = nullptr;
    std::shared_ptr<TransferGroupMeta> meta_;
    bool is_cpu_ = false;
    int rank_ = 0;
    int size_ = 0;
    int cuda_device_index_ = -1;
    std::string location_;
    std::chrono::milliseconds transfer_timeout_ms_{5000};  // 5s
    P2PResources resources_;
    bool resource_abandoned_{false};

    size_t chunk_size_ = 0;
    uint32_t num_chunks_ = 0;
    size_t pool_bytes_ = 0;

    P2PChunkPool send_pool_;
    P2PChunkPool recv_pool_;

    std::queue<SendOpContext> send_queue_;
    std::mutex send_queue_mutex_;

    std::queue<RecvOp> recv_queue_;
    std::mutex recv_queue_mutex_;

    std::array<std::atomic<bool>, kMaxNumRanks> reset_send_req_;
    std::array<std::atomic<bool>, kMaxNumRanks> reset_recv_req_;

    std::atomic<int> active_send_tasks_{0};
    std::atomic<int> active_recv_tasks_{0};

    // Per-peer generation for fault recovery.  Incremented in resetPeerState
    // and performSend/RecvReset so that stale messages from a previous epoch
    // can be detected on a per-peer basis.
    std::array<std::atomic<uint32_t>, kMaxNumRanks> peer_generation_;

    std::array<SendPeerLane, kMaxNumRanks> send_peer_lanes_;
    std::array<RecvPeerLane, kMaxNumRanks> recv_peer_lanes_;
};

// P2PDeviceWorker instances are shared across multiple backends within the same
// process. Therefore, they must not be instantiated directly. Instead, obtain
// an instance through P2PDeviceWorkerManager.
class P2PDeviceWorker {
   public:
    friend class P2PDeviceWorkerManager;
    void registerProxy(const std::shared_ptr<P2PProxy>&);
    void removeProxy(const std::shared_ptr<P2PProxy>&);

    P2PDeviceWorker(bool is_cpu, int cuda_device_index)
        : is_cpu_(is_cpu), cuda_device_index_(cuda_device_index) {
        start();
    }

    ~P2PDeviceWorker() { stop(); }

    void wakeUpSend();
    void wakeUpRecv();

   private:
    void start();
    void stop();

    void sendWorkerMainloop();
    void recvWorkerMainloop();

    std::mutex send_wakeup_mutex_;
    std::condition_variable send_wakeup_cv_;

    std::mutex recv_wakeup_mutex_;
    std::condition_variable recv_wakeup_cv_;

    std::atomic<bool> send_worker_running_{false};
    std::thread send_worker_thread_;

    std::atomic<bool> recv_worker_running_{false};
    std::thread recv_worker_thread_;

    std::mutex proxies_mutex_;
    std::atomic<uint64_t> proxies_version_{0};
    std::vector<std::shared_ptr<P2PProxy>> proxies_;

    bool is_cpu_;
    int cuda_device_index_;
};

class P2PDeviceWorkerManager {
   public:
    static P2PDeviceWorkerManager& getInstance() {
        // leaky singleton to avoid destructor fiasco problem
        static P2PDeviceWorkerManager* manager = new P2PDeviceWorkerManager;
        return *manager;
    }

    std::shared_ptr<P2PDeviceWorker> getCPUWorker();
    std::shared_ptr<P2PDeviceWorker> getCUDAWorker(int cuda_device_index);

   private:
    static constexpr int CPUWorkerID = -1;
    std::mutex manager_mutex_;
    std::unordered_map<int, std::weak_ptr<P2PDeviceWorker>> workers_;
};

}  // namespace mooncake

#endif  // MOONCAKE_P2P_PROXY_H
