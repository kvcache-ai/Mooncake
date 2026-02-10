#ifndef MOONCAKE_P2P_PROXY_HH
#define MOONCAKE_P2P_PROXY_HH

#include <mooncake_worker.cuh>
#include <torch/torch.h>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

namespace mooncake {

inline constexpr size_t kP2PBufferSize = 1u << 24;
inline constexpr size_t kP2PNumSlots = 8;
inline constexpr size_t kP2PSlotSize = kP2PBufferSize / kP2PNumSlots;
inline constexpr size_t kP2PTotalBufferSize = kP2PBufferSize * kMaxNumRanks;

struct alignas(64) AtomicHeadTail {
    uint32_t load(
        std::memory_order order = std::memory_order_seq_cst) const noexcept {
        uint32_t& mutable_value = const_cast<uint32_t&>(value);
        return std::atomic_ref<uint32_t>(mutable_value).load(order);
    }

    void store(uint32_t new_value,
               std::memory_order order = std::memory_order_seq_cst) noexcept {
        std::atomic_ref<uint32_t>(value).store(new_value, order);
    }

    uint32_t value{0};
};

// Ring-control metadata for one peer lane.
//
//   slot index:   0 -> 1 -> 2 -> ... -> N-1 -> 0   (N = kP2PNumSlots)
//                   ^h (producer publish cursor)
//                   ^t (consumer reclaim cursor)
//
// State by (head, tail), modulo N:
// - empty: head == tail
// - full : (head + 1) % N == tail   (one slot reserved to distinguish full/empty)
// - ready: head != tail
//
// Protocol:
// - producer writes slot[head], then advances head (publish)
// - consumer reads slot[tail], then advances tail (reclaim)
struct P2PControlSlot {
    AtomicHeadTail head;
    AtomicHeadTail tail;
};

class P2PProxy {
   public:
    struct Options {
        bool is_cpu = false;
        int rank = 0;
        int size = 0;
        int cuda_device_index = -1;
    };

    struct SendOp {
        at::Tensor tensor_;
        int peer_rank_ = -1;
        cudaStream_t cuda_stream_ = nullptr;
        std::shared_ptr<std::atomic<bool>> completed_;
    };

    struct RecvOp {
        at::Tensor tensor_;
        at::Tensor original_tensor_;
        int peer_rank_ = -1;
        cudaStream_t cuda_stream_ = nullptr;
        std::shared_ptr<std::atomic<bool>> completed_;
    };

    P2PProxy(TransferEngine* engine, const Options& options);

    ~P2PProxy();

    void BindMeta(TransferGroupMeta* meta);
    void AllocateResources();
    void ReleaseResources();

    void* send_buffer() const { return resources_.send_buffer_; }
    void* recv_buffer() const { return resources_.recv_buffer_; }
    P2PControlSlot* ctrl_send_region() const {
        return resources_.ctrl_send_region_;
    }
    P2PControlSlot* ctrl_recv_region() const {
        return resources_.ctrl_recv_region_;
    }

    void Start();
    void Stop();

    void EnqueueSend(SendOp op);
    void EnqueueRecv(RecvOp op);

   private:
    enum class TransferState {
        kDataCopy,
        kTransfer,
        kDone,
    };

    struct SendOpContext;
    struct RecvOpContext;

    struct SendTransferTask {
        SendTransferTask() = default;
        SendTransferTask(uint64_t chunk_offset_in, uint64_t chunk_bytes_in,
                         void* source_in, uint64_t target_offset_in);

        TransferState state_ = TransferState::kDataCopy;
        uint64_t chunk_offset_ = 0;
        uint64_t chunk_bytes_ = 0;
        void* source_ = nullptr;
        uint64_t target_offset_ = 0;
        std::optional<BatchID> transfer_batch_id_;
        cudaEvent_t copy_ready_event_ = nullptr;
    };

    struct SendOpContext {
        SendOpContext() = default;
        SendOpContext(SendOp&& op_in);

        at::Tensor tensor_;
        int peer_rank_ = -1;
        cudaStream_t cuda_stream_ = nullptr;
        std::shared_ptr<std::atomic<bool>> completed_;
        uint64_t total_bytes_ = 0;
        uint64_t bytes_issued_ = 0;
        std::optional<BatchID> head_update_batch_id_;
        std::deque<SendTransferTask> tasks_;
    };

    struct RecvTransferTask {
        RecvTransferTask() = default;
        RecvTransferTask(uint64_t chunk_offset_in, uint64_t chunk_bytes_in,
                         void* source_in, void* target_in);

        TransferState state_ = TransferState::kDataCopy;
        uint64_t chunk_offset_ = 0;
        uint64_t chunk_bytes_ = 0;
        void* source_ = nullptr;
        void* target_ = nullptr;
        cudaEvent_t copy_ready_event_ = nullptr;
    };

    struct RecvOpContext {
        RecvOpContext() = default;
        RecvOpContext(RecvOp&& op_in);

        at::Tensor tensor_;
        at::Tensor original_tensor_;
        int peer_rank_ = -1;
        cudaStream_t cuda_stream_ = nullptr;
        std::shared_ptr<std::atomic<bool>> completed_;
        uint64_t total_bytes_ = 0;
        uint64_t bytes_issued_ = 0;
        std::optional<BatchID> tail_update_batch_id_;
        std::deque<RecvTransferTask> tasks_;
    };

    struct SendPeerLane {
        std::deque<SendOpContext> pending_send_ops_;
        std::optional<SendOpContext> active_send_op_;
        uint32_t local_head_ = 0;
        std::array<cudaEvent_t, kP2PNumSlots> copy_ready_events_;
    };

    struct RecvPeerLane {
        std::deque<RecvOp> pending_recv_ops_;
        std::optional<RecvOpContext> active_recv_op_;
        uint32_t local_tail_ = 0;
        std::array<cudaEvent_t, kP2PNumSlots> copy_ready_events_;
    };

    bool TryIssueSendTask(SendOpContext& op_ctx, uint32_t capacity);
    bool StepSendTransferTask(SendOpContext& op_ctx, SendTransferTask& task);
    bool StepSendDataCopy(SendTransferTask& task);
    bool StepSendTransfer(SendOpContext& op_ctx, SendTransferTask& task);
    bool StepSendHeadCommit(SendOpContext& op_ctx, uint32_t capacity);
    bool IsSendDataPathCompleted(const SendOpContext& op_ctx) const;
    bool IsSendOpCompleted(const SendOpContext& op_ctx) const;

    bool TryIssueRecvTask(RecvOpContext& op_ctx, uint32_t capacity);
    bool StepRecvTransferTask(RecvTransferTask& task);
    bool StepRecvDataCopy(RecvTransferTask& task);
    bool StepRecvTailCommit(RecvOpContext& op_ctx, uint32_t capacity);
    bool IsRecvDataPathCompleted(const RecvOpContext& op_ctx) const;
    uint64_t GetLocalSendSlotAddress(int peer_rank, uint32_t slot_index) const;
    uint64_t GetLocalRecvSlotAddress(int peer_rank, uint32_t slot_index) const;
    uint64_t GetRemoteRecvSlotAddress(int peer_rank, uint32_t slot_index) const;
    uint64_t GetRemoteCtrlRecvHeadOffset(int peer_rank) const;
    uint64_t GetRemoteCtrlSendTailOffset(int peer_rank) const;

    void SendWorkerThread();
    void RecvWorkerThread();

   private:
    struct P2PResources {
        void* send_buffer_ = nullptr;
        void* recv_buffer_ = nullptr;
        P2PControlSlot* ctrl_send_region_ = nullptr;
        P2PControlSlot* ctrl_recv_region_ = nullptr;
    };

    TransferEngine* engine_ = nullptr;
    TransferGroupMeta* meta_ = nullptr;
    bool is_cpu_ = false;
    int rank_ = 0;
    int size_ = 0;
    int cuda_device_index_ = -1;
    P2PResources resources_;

    std::queue<SendOpContext> send_queue_;
    std::mutex send_queue_mutex_;
    std::atomic<bool> send_worker_running_{false};
    std::thread send_worker_thread_;

    std::queue<RecvOp> recv_queue_;
    std::mutex recv_queue_mutex_;
    std::atomic<bool> recv_worker_running_{false};
    std::thread recv_worker_thread_;

    std::array<SendPeerLane, kMaxNumRanks> send_peer_lanes_;
    std::array<RecvPeerLane, kMaxNumRanks> recv_peer_lanes_;
};

}  // namespace mooncake

#endif  // MOONCAKE_P2P_PROXY_HH
