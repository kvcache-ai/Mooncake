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
#include <unordered_map>

namespace mooncake {

inline constexpr size_t kP2PBufferSize = 1u << 24;
inline constexpr size_t kP2PNumSlots = 8;
inline constexpr size_t kP2PSlotSize = kP2PBufferSize / kP2PNumSlots;

struct alignas(64) AtomicHeadTail {
    uint32_t load(
        std::memory_order order = std::memory_order_seq_cst) const noexcept {
        return std::atomic_ref<uint32_t>(value).load(order);
    }

    void store(uint32_t new_value,
               std::memory_order order = std::memory_order_seq_cst) noexcept {
        std::atomic_ref<uint32_t>(value).store(new_value, order);
    }

    mutable uint32_t value{0};
};

// Ring-control metadata for one peer lane.
//
//   slot index:   0 -> 1 -> 2 -> ... -> N-1 -> 0   (N = kP2PNumSlots)
//                   ^h (producer publish cursor)
//                   ^t (consumer reclaim cursor)
//
// State by (head, tail), modulo N:
// - empty: head == tail
// - full : (head + 1) % N == tail   (one slot reserved to distinguish
// full/empty)
// - ready: head != tail
//
// Protocol:
// - producer writes slot[head], then advances head (publish)
// - consumer reads slot[tail], then advances tail (reclaim)
struct P2PControlSlot {
    AtomicHeadTail head;
    AtomicHeadTail tail;
};

class P2PDeviceWorker;
class P2PProxy {
   public:
    friend class P2PDeviceWorker;

    struct Options {
        bool is_cpu = false;
        int rank = 0;
        int size = 0;
        int cuda_device_index = -1;
        std::string location;
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

    void BindMeta(const std::shared_ptr<TransferGroupMeta>& meta);
    void* send_buffer() const { return resources_.send_buffer_; }
    void* recv_buffer() const { return resources_.recv_buffer_; }
    P2PControlSlot* ctrl_send_region() const {
        return resources_.ctrl_send_region_;
    }
    P2PControlSlot* ctrl_recv_region() const {
        return resources_.ctrl_recv_region_;
    }

    void EnqueueSend(SendOp op);
    void EnqueueRecv(RecvOp op);

    void ResetPeerState(int peer_rank);

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

    // Resources are allocated and released by constructor/destructor
    void AllocateResources();
    void ReleaseResources();

    // For P2PDeviceWorker
    bool StepSend();
    bool StepRecv();
    void SetDeviceWorker(P2PDeviceWorker*);
    bool HasActiveSendWork() const;
    bool HasActiveRecvWork() const;

    // Internal Steps
    bool TryIssueSendTask(SendOpContext& op_ctx, uint32_t capacity);
    bool StepSendTransferTask(SendOpContext& op_ctx, SendTransferTask& task);
    bool StepSendDataCopy(SendTransferTask& task);
    bool StepSendTransfer(SendOpContext& op_ctx, SendTransferTask& task);
    bool StepSendHeadCommit(SendOpContext& op_ctx, uint32_t capacity);
    bool IsSendDataPathCompleted(const SendOpContext& op_ctx) const;
    bool IsSendOpCompleted(const SendOpContext& op_ctx) const;
    void PerformSendReset(int peer_rank);

    bool TryIssueRecvTask(RecvOpContext& op_ctx, uint32_t capacity);
    bool StepRecvTransferTask(RecvTransferTask& task);
    bool StepRecvDataCopy(RecvTransferTask& task);
    bool StepRecvTailCommit(RecvOpContext& op_ctx, uint32_t capacity);
    bool IsRecvDataPathCompleted(const RecvOpContext& op_ctx) const;
    void PerformRecvReset(int peer_rank);

    uint64_t GetLocalSendSlotAddress(int peer_rank, uint32_t slot_index) const;
    uint64_t GetLocalRecvSlotAddress(int peer_rank, uint32_t slot_index) const;
    uint64_t GetRemoteRecvSlotAddress(int peer_rank, uint32_t slot_index) const;
    uint64_t GetRemoteCtrlRecvHeadOffset(int peer_rank) const;
    uint64_t GetRemoteCtrlSendTailOffset(int peer_rank) const;

   private:
    struct P2PResources {
        void* send_buffer_ = nullptr;
        void* recv_buffer_ = nullptr;
        P2PControlSlot* ctrl_send_region_ = nullptr;
        P2PControlSlot* ctrl_recv_region_ = nullptr;
    };

    P2PDeviceWorker* device_worker_ = nullptr;

    TransferEngine* engine_ = nullptr;
    std::shared_ptr<TransferGroupMeta> meta_;
    bool is_cpu_ = false;
    int rank_ = 0;
    int size_ = 0;
    int cuda_device_index_ = -1;
    std::string location_;
    P2PResources resources_;

    std::queue<SendOpContext> send_queue_;
    std::mutex send_queue_mutex_;

    std::queue<RecvOp> recv_queue_;
    std::mutex recv_queue_mutex_;

    std::array<std::atomic<bool>, kMaxNumRanks> reset_send_req_;
    std::array<std::atomic<bool>, kMaxNumRanks> reset_recv_req_;

    std::atomic<int> active_send_tasks_{0};
    std::atomic<int> active_recv_tasks_{0};

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
        Start();
    }

    ~P2PDeviceWorker() { Stop(); }

    void WakeUpSend();
    void WakeUpRecv();

   private:
    void Start();
    void Stop();

    void SendWorkerThread();
    void RecvWorkerThread();

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
    static P2PDeviceWorkerManager& GetInstance() {
        // leaky singleton to avoid destructor fiasco problem
        static P2PDeviceWorkerManager* manager = new P2PDeviceWorkerManager;
        return *manager;
    }

    std::shared_ptr<P2PDeviceWorker> GetCPUWorker();
    std::shared_ptr<P2PDeviceWorker> GetCUDAWorker(int cuda_device_index);

   private:
    static constexpr int CPUWorkerID = -1;
    std::mutex manager_mutex_;
    std::unordered_map<int, std::weak_ptr<P2PDeviceWorker>> workers_;
};

}  // namespace mooncake

#endif  // MOONCAKE_P2P_PROXY_H
