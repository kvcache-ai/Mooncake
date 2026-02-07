#ifndef MOONCAKE_P2P_PROXY_HH
#define MOONCAKE_P2P_PROXY_HH

#include <mooncake_worker.cuh>
#include <torch/torch.h>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

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

struct P2PControlSlot {
    AtomicHeadTail head;
    AtomicHeadTail tail;
};

static_assert(sizeof(AtomicHeadTail) == 64,
              "AtomicHeadTail must occupy one cacheline");
static_assert(alignof(AtomicHeadTail) == 64,
              "AtomicHeadTail must be cacheline aligned");
static_assert(sizeof(P2PControlSlot) == 128,
              "P2PControlSlot must keep head/tail on separate cachelines");

class P2PProxy {
   public:
    struct Options {
        bool is_cpu = false;
        int rank = 0;
        int size = 0;
    };

    struct RecvOp {
        at::Tensor tensor_;
        at::Tensor original_tensor_;
        int peer_rank_ = -1;
        int64_t seq_ = 0;
        cudaStream_t cuda_stream_ = nullptr;
        int cuda_device_index_ = -1;
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

    void EnqueueSend(at::Tensor tensor, int peer_rank, cudaStream_t cuda_stream,
                     int cuda_device_index,
                     std::shared_ptr<std::atomic<bool>> completed);
    void EnqueueRecv(RecvOp op);

   private:
    enum class TransferTaskState {
        kDataCopy,
        kTransfer,
        kUpdateHead,
        kDone,
    };

    struct SendOpContext;
    struct RecvOpContext;

    enum class RecvTaskState {
        kDataCopy,
        kUpdateTail,
        kDone,
    };

    enum class RecvOpState {
        kDataCopy,
        kUpdateTail,
        kDone,
    };

    struct TransferTask {
        TransferTask() = default;
        TransferTask(uint32_t seq_in, uint64_t chunk_offset_in,
                     uint64_t chunk_bytes_in, void* source_in,
                     uint64_t target_offset_in);

        void cleanup_resources(int cuda_device_index);

        TransferTaskState state_ = TransferTaskState::kDataCopy;
        uint32_t seq_ = 0;
        uint64_t chunk_offset_ = 0;
        uint64_t chunk_bytes_ = 0;
        void* source_ = nullptr;
        uint64_t target_offset_ = 0;
        std::optional<BatchID> transfer_batch_id_;
        std::optional<BatchID> head_update_batch_id_;
        cudaEvent_t copy_ready_event_ = nullptr;
    };

    struct SendOpContext {
        SendOpContext() = default;
        SendOpContext(at::Tensor&& tensor_in, int peer_rank_in,
                      cudaStream_t cuda_stream_in, int cuda_device_index_in,
                      std::shared_ptr<std::atomic<bool>> completed_in);

        at::Tensor tensor_;
        int peer_rank_ = -1;
        cudaStream_t cuda_stream_ = nullptr;
        int cuda_device_index_ = -1;
        std::shared_ptr<std::atomic<bool>> completed_;
        uint64_t total_bytes_ = 0;
        uint64_t bytes_issued_ = 0;
        uint32_t issued_seq_ = 0;
        uint32_t outstanding_tasks_ = 0;
        uint32_t next_head_seq_ = 0;
        std::vector<TransferTask> tasks_;
    };

    struct RecvTransferTask {
        RecvTransferTask() = default;
        RecvTransferTask(uint32_t seq_in, uint32_t slot_in,
                         uint64_t chunk_offset_in, uint64_t chunk_bytes_in,
                         void* source_in, void* target_in);

        void cleanup_resources(int cuda_device_index);

        RecvTaskState state_ = RecvTaskState::kDataCopy;
        uint32_t seq_ = 0;
        uint32_t slot_ = 0;
        uint64_t chunk_offset_ = 0;
        uint64_t chunk_bytes_ = 0;
        void* source_ = nullptr;
        void* target_ = nullptr;
        std::optional<BatchID> tail_update_batch_id_;
        cudaEvent_t copy_ready_event_ = nullptr;
    };

    struct RecvOpContext {
        RecvOpContext() = default;
        RecvOpContext(RecvOp&& op_in, uint32_t local_tail_in);

        at::Tensor tensor_;
        at::Tensor original_tensor_;
        int peer_rank_ = -1;
        int64_t seq_ = 0;
        cudaStream_t cuda_stream_ = nullptr;
        int cuda_device_index_ = -1;
        std::shared_ptr<std::atomic<bool>> completed_;
        uint64_t total_bytes_ = 0;
        uint64_t bytes_issued_ = 0;
        uint32_t issued_seq_ = 0;
        uint32_t outstanding_tasks_ = 0;
        uint32_t next_tail_seq_ = 0;
        uint32_t local_tail_ = 0;
        std::vector<RecvTransferTask> tasks_;
        RecvOpState state_ = RecvOpState::kDataCopy;
        bool op_copy_started_ = false;
        cudaEvent_t op_copy_event_ = nullptr;
    };

    struct SendPeerLane {
        std::deque<SendOpContext> pending_send_ops_;
        std::optional<SendOpContext> active_send_op_;
        uint32_t local_head_ = 0;
    };

    struct RecvPeerLane {
        std::deque<RecvOp> pending_recv_ops_;
        std::optional<RecvOpContext> active_recv_op_;
    };

    bool HasLocalSendWork() const;
    bool HasLocalRecvWork() const;
    bool TryIssueTask(SendOpContext& op_ctx, uint32_t capacity);
    bool StepSendTask(SendOpContext& op_ctx, TransferTask& task,
                      uint32_t capacity);
    bool StepSendDataCopy(SendOpContext& op_ctx, TransferTask& task);
    bool StepSendTransfer(SendOpContext& op_ctx, TransferTask& task);
    bool StepSendHeadUpdate(SendOpContext& op_ctx, TransferTask& task,
                            uint32_t capacity);
    void FinalizeTask(SendOpContext& op_ctx, TransferTask& task);
    bool IsSendOpCompleted(const SendOpContext& op_ctx) const;

    bool TryIssueRecvTask(RecvOpContext& op_ctx, uint32_t capacity);
    bool StepRecvTask(RecvOpContext& op_ctx, RecvTransferTask& task,
                      uint32_t capacity);
    bool StepRecvDataCopy(RecvOpContext& op_ctx, RecvTransferTask& task);
    bool StepRecvTailUpdate(RecvOpContext& op_ctx, RecvTransferTask& task,
                            uint32_t capacity);
    void FinalizeRecvTask(RecvOpContext& op_ctx, RecvTransferTask& task);
    bool IsRecvDataCopyCompleted(const RecvOpContext& op_ctx) const;
    bool StepRecvOpState(RecvOpContext& op_ctx);
    bool StepRecvOpUpdateTail(RecvOpContext& op_ctx);

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
    P2PResources resources_;

    std::queue<SendOpContext> send_queue_;
    std::mutex send_queue_mutex_;
    std::condition_variable send_queue_cv_;
    std::atomic<bool> send_worker_running_{false};
    std::thread send_worker_thread_;

    std::queue<RecvOp> recv_queue_;
    std::mutex recv_queue_mutex_;
    std::condition_variable recv_queue_cv_;
    std::atomic<bool> recv_worker_running_{false};
    std::thread recv_worker_thread_;

    std::array<SendPeerLane, kMaxNumRanks> send_peer_lanes_{};
    std::array<RecvPeerLane, kMaxNumRanks> recv_peer_lanes_{};
};

}  // namespace mooncake

#endif  // MOONCAKE_P2P_PROXY_HH
