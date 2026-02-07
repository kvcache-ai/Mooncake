#include <p2p_proxy.hh>
#include <ATen/cuda/CUDAContext.h>
#include <c10/cuda/CUDAGuard.h>
#include <c10/cuda/CUDAStream.h>
#include <cuda_runtime.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <thread>

namespace mooncake {

namespace {

constexpr size_t kP2PBytesPerRank = kP2PBufferSize;
constexpr size_t kP2PNumSlotsPerRank = kP2PNumSlots;

static_assert(kP2PBufferSize % kP2PNumSlots == 0,
              "kP2PBufferSize must be divisible by kP2PNumSlots");
static_assert(kP2PNumSlots > 1, "P2P ring requires at least 2 slots per rank");

}  // namespace

P2PProxy::P2PProxy(TransferEngine* engine, const Options& options)
    : engine_(engine),
      is_cpu_(options.is_cpu),
      rank_(options.rank),
      size_(options.size) {}

P2PProxy::~P2PProxy() { ReleaseResources(); }

void P2PProxy::BindMeta(TransferGroupMeta* meta) { meta_ = meta; }

void P2PProxy::AllocateResources() {
    TORCH_CHECK(engine_, "P2PProxy engine is null.");
    if (resources_.send_buffer_ != nullptr ||
        resources_.recv_buffer_ != nullptr ||
        resources_.ctrl_send_region_ != nullptr ||
        resources_.ctrl_recv_region_ != nullptr) {
        return;
    }

    if (is_cpu_) {
        resources_.send_buffer_ = std::malloc(kP2PTotalBufferSize);
        TORCH_CHECK(resources_.send_buffer_ != nullptr,
                    "Failed to allocate CPU P2P send buffer");
        int rc = engine_->registerLocalMemory(
            resources_.send_buffer_, kP2PTotalBufferSize, kWildcardLocation);
        TORCH_CHECK(rc == 0, "Failed to register CPU P2P send buffer");

        resources_.recv_buffer_ = std::malloc(kP2PTotalBufferSize);
        TORCH_CHECK(resources_.recv_buffer_ != nullptr,
                    "Failed to allocate CPU P2P recv buffer");
        rc = engine_->registerLocalMemory(
            resources_.recv_buffer_, kP2PTotalBufferSize, kWildcardLocation);
        TORCH_CHECK(rc == 0, "Failed to register CPU P2P recv buffer");
    } else {
        cudaError_t err =
            cudaMalloc(&resources_.send_buffer_, kP2PTotalBufferSize);
        TORCH_CHECK(err == cudaSuccess,
                    "Failed to allocate CUDA P2P send buffer");
        int rc = engine_->registerLocalMemory(
            resources_.send_buffer_, kP2PTotalBufferSize, kWildcardLocation);
        TORCH_CHECK(rc == 0, "Failed to register CUDA P2P send buffer");

        err = cudaMalloc(&resources_.recv_buffer_, kP2PTotalBufferSize);
        TORCH_CHECK(err == cudaSuccess,
                    "Failed to allocate CUDA P2P recv buffer");
        rc = engine_->registerLocalMemory(
            resources_.recv_buffer_, kP2PTotalBufferSize, kWildcardLocation);
        TORCH_CHECK(rc == 0, "Failed to register CUDA P2P recv buffer");
    }

    resources_.ctrl_send_region_ = new P2PControlSlot[kMaxNumRanks]{};
    resources_.ctrl_recv_region_ = new P2PControlSlot[kMaxNumRanks]{};
    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        resources_.ctrl_send_region_[i].head.store(0,
                                                   std::memory_order_relaxed);
        resources_.ctrl_send_region_[i].tail.store(0,
                                                   std::memory_order_relaxed);
        resources_.ctrl_recv_region_[i].head.store(0,
                                                   std::memory_order_relaxed);
        resources_.ctrl_recv_region_[i].tail.store(0,
                                                   std::memory_order_relaxed);
        send_peer_lanes_[i].local_head_ = 0;
        send_peer_lanes_[i].pending_send_ops_.clear();
        send_peer_lanes_[i].active_send_op_.reset();
        recv_peer_lanes_[i].pending_recv_ops_.clear();
        recv_peer_lanes_[i].active_recv_op_.reset();
    }

    int rc = engine_->registerLocalMemory(resources_.ctrl_send_region_,
                                          kMaxNumRanks * sizeof(P2PControlSlot),
                                          kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P ctrl send region");

    rc = engine_->registerLocalMemory(resources_.ctrl_recv_region_,
                                      kMaxNumRanks * sizeof(P2PControlSlot),
                                      kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P ctrl recv region");
}

void P2PProxy::ReleaseResources() {
    Stop();
    if (!engine_) {
        return;
    }

    if (resources_.ctrl_send_region_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.ctrl_send_region_);
        delete[] resources_.ctrl_send_region_;
        resources_.ctrl_send_region_ = nullptr;
    }

    if (resources_.ctrl_recv_region_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.ctrl_recv_region_);
        delete[] resources_.ctrl_recv_region_;
        resources_.ctrl_recv_region_ = nullptr;
    }

    if (resources_.send_buffer_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.send_buffer_);
        if (is_cpu_) {
            std::free(resources_.send_buffer_);
        } else {
            cudaFree(resources_.send_buffer_);
        }
        resources_.send_buffer_ = nullptr;
    }

    if (resources_.recv_buffer_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.recv_buffer_);
        if (is_cpu_) {
            std::free(resources_.recv_buffer_);
        } else {
            cudaFree(resources_.recv_buffer_);
        }
        resources_.recv_buffer_ = nullptr;
    }
}

void P2PProxy::Start() {
    TORCH_CHECK(meta_ != nullptr, "P2PProxy meta is not bound.");
    TORCH_CHECK(resources_.send_buffer_ != nullptr &&
                    resources_.recv_buffer_ != nullptr &&
                    resources_.ctrl_send_region_ != nullptr &&
                    resources_.ctrl_recv_region_ != nullptr,
                "P2P resources are not allocated.");

    bool expected_send = false;
    if (send_worker_running_.compare_exchange_strong(expected_send, true)) {
        send_worker_thread_ = std::thread(&P2PProxy::SendWorkerThread, this);
    }

    bool expected_recv = false;
    if (recv_worker_running_.compare_exchange_strong(expected_recv, true)) {
        recv_worker_thread_ = std::thread(&P2PProxy::RecvWorkerThread, this);
    }
}

void P2PProxy::Stop() {
    bool expected_send = true;
    if (send_worker_running_.compare_exchange_strong(expected_send, false)) {
        send_queue_cv_.notify_all();
        if (send_worker_thread_.joinable()) {
            send_worker_thread_.join();
        }
    }

    bool expected_recv = true;
    if (recv_worker_running_.compare_exchange_strong(expected_recv, false)) {
        recv_queue_cv_.notify_all();
        if (recv_worker_thread_.joinable()) {
            recv_worker_thread_.join();
        }
    }
}

void P2PProxy::EnqueueSend(at::Tensor tensor, int peer_rank,
                           cudaStream_t cuda_stream, int cuda_device_index,
                           std::shared_ptr<std::atomic<bool>> completed) {
    TORCH_CHECK(send_worker_running_.load(std::memory_order_acquire),
                "P2P send worker is not running.");
    at::Tensor contiguous_tensor =
        tensor.is_contiguous() ? tensor : tensor.contiguous();
    {
        std::lock_guard<std::mutex> lock(send_queue_mutex_);
        send_queue_.emplace(std::move(contiguous_tensor), peer_rank,
                            cuda_stream, cuda_device_index,
                            std::move(completed));
    }
    send_queue_cv_.notify_one();
}

void P2PProxy::EnqueueRecv(RecvOp op) {
    TORCH_CHECK(recv_worker_running_.load(std::memory_order_acquire),
                "P2P recv worker is not running.");
    {
        std::lock_guard<std::mutex> lock(recv_queue_mutex_);
        recv_queue_.push(std::move(op));
    }
    recv_queue_cv_.notify_one();
}

P2PProxy::TransferTask::TransferTask(uint64_t chunk_offset_in,
                                     uint64_t chunk_bytes_in, void* source_in,
                                     uint64_t target_offset_in)
    : chunk_offset_(chunk_offset_in),
      chunk_bytes_(chunk_bytes_in),
      source_(source_in),
      target_offset_(target_offset_in) {}

void P2PProxy::TransferTask::cleanup_resources(int cuda_device_index) {
    if (copy_ready_event_ != nullptr) {
        if (cuda_device_index >= 0) {
            c10::cuda::CUDAGuard device_guard(cuda_device_index);
            cudaEventDestroy(copy_ready_event_);
        } else {
            cudaEventDestroy(copy_ready_event_);
        }
        copy_ready_event_ = nullptr;
    }
}

P2PProxy::SendOpContext::SendOpContext(
    at::Tensor&& tensor_in, int peer_rank_in, cudaStream_t cuda_stream_in,
    int cuda_device_index_in, std::shared_ptr<std::atomic<bool>> completed_in)
    : tensor_(std::move(tensor_in)),
      peer_rank_(peer_rank_in),
      cuda_stream_(cuda_stream_in),
      cuda_device_index_(cuda_device_index_in),
      completed_(std::move(completed_in)) {
    total_bytes_ =
        tensor_.numel() * static_cast<uint64_t>(tensor_.element_size());
}

P2PProxy::RecvTransferTask::RecvTransferTask(uint64_t chunk_offset_in,
                                             uint64_t chunk_bytes_in,
                                             void* source_in, void* target_in)
    : chunk_offset_(chunk_offset_in),
      chunk_bytes_(chunk_bytes_in),
      source_(source_in),
      target_(target_in) {}

void P2PProxy::RecvTransferTask::cleanup_resources(int cuda_device_index) {
    if (copy_ready_event_ != nullptr) {
        if (cuda_device_index >= 0) {
            c10::cuda::CUDAGuard device_guard(cuda_device_index);
            cudaEventDestroy(copy_ready_event_);
        } else {
            cudaEventDestroy(copy_ready_event_);
        }
        copy_ready_event_ = nullptr;
    }
}

P2PProxy::RecvOpContext::RecvOpContext(RecvOp&& op_in, uint32_t local_tail_in)
    : tensor_(std::move(op_in.tensor_)),
      original_tensor_(std::move(op_in.original_tensor_)),
      peer_rank_(op_in.peer_rank_),
      seq_(op_in.seq_),
      cuda_stream_(op_in.cuda_stream_),
      cuda_device_index_(op_in.cuda_device_index_),
      completed_(std::move(op_in.completed_)),
      local_tail_(local_tail_in) {
    total_bytes_ =
        tensor_.numel() * static_cast<uint64_t>(tensor_.element_size());
}

bool P2PProxy::HasLocalSendWork() const {
    for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
        const auto& lane = send_peer_lanes_[peer_rank];
        if (!lane.pending_send_ops_.empty() ||
            lane.active_send_op_.has_value()) {
            return true;
        }
    }
    return false;
}

bool P2PProxy::HasLocalRecvWork() const {
    for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
        const auto& lane = recv_peer_lanes_[peer_rank];
        if (!lane.pending_recv_ops_.empty() ||
            lane.active_recv_op_.has_value()) {
            return true;
        }
    }
    return false;
}

bool P2PProxy::TryIssueTask(SendOpContext& op_ctx, uint32_t capacity) {
    if (op_ctx.bytes_issued_ >= op_ctx.total_bytes_) {
        return false;
    }

    const int peer_rank = op_ctx.peer_rank_;
    auto& lane = send_peer_lanes_[peer_rank];
    const uint32_t head = lane.local_head_;
    const uint32_t remote_tail =
        resources_.ctrl_send_region_[peer_rank].tail.load(
            std::memory_order_acquire);
    if (((head + 1) % capacity) == remote_tail) {
        return false;
    }

    const uint64_t chunk_bytes =
        std::min(static_cast<uint64_t>(kP2PSlotSize),
                 op_ctx.total_bytes_ - op_ctx.bytes_issued_);
    const uint64_t send_addr_base =
        meta_->segmentInfos[rank_].p2p_send_buffer +
        static_cast<uint64_t>(peer_rank) * kP2PBytesPerRank;
    const uint64_t remote_recv_addr_base =
        meta_->segmentInfos[peer_rank].p2p_recv_buffer +
        static_cast<uint64_t>(rank_) * kP2PBytesPerRank;
    const uint64_t send_addr =
        send_addr_base + static_cast<uint64_t>(head) * kP2PSlotSize;
    const uint64_t target_offset =
        remote_recv_addr_base + static_cast<uint64_t>(head) * kP2PSlotSize;
    op_ctx.tasks_.emplace_back(op_ctx.bytes_issued_, chunk_bytes,
                               reinterpret_cast<void*>(send_addr),
                               target_offset);
    auto& task = op_ctx.tasks_.back();
    const auto* tensor_ptr =
        static_cast<const uint8_t*>(op_ctx.tensor_.data_ptr());

    if (is_cpu_) {
        std::memcpy(task.source_, tensor_ptr + task.chunk_offset_,
                    task.chunk_bytes_);
        task.state_ = TransferTaskState::kTransfer;
    } else {
        c10::cuda::CUDAGuard device_guard(op_ctx.cuda_device_index_);
        cudaError_t copy_error = cudaMemcpyAsync(
            task.source_, tensor_ptr + task.chunk_offset_, task.chunk_bytes_,
            cudaMemcpyDeviceToDevice, op_ctx.cuda_stream_);
        TORCH_CHECK(!copy_error, "P2P send cudaMemcpyAsync failed: ",
                    cudaGetErrorString(copy_error));
        copy_error = cudaEventCreateWithFlags(&task.copy_ready_event_,
                                              cudaEventDisableTiming);
        TORCH_CHECK(!copy_error, "P2P send cudaEventCreateWithFlags failed: ",
                    cudaGetErrorString(copy_error));
        copy_error =
            cudaEventRecord(task.copy_ready_event_, op_ctx.cuda_stream_);
        if (copy_error != cudaSuccess) {
            task.cleanup_resources(op_ctx.cuda_device_index_);
            TORCH_CHECK(false, "P2P send cudaEventRecord failed: ",
                        cudaGetErrorString(copy_error));
        }
    }

    op_ctx.bytes_issued_ += task.chunk_bytes_;
    lane.local_head_ = (head + 1) % capacity;
    return true;
}

bool P2PProxy::StepSendTask(SendOpContext& op_ctx, TransferTask& task) {
    bool did_work = false;

    if (task.state_ == TransferTaskState::kDataCopy &&
        StepSendDataCopy(op_ctx, task)) {
        did_work = true;
    }
    if (task.state_ == TransferTaskState::kTransfer &&
        StepSendTransfer(op_ctx, task)) {
        did_work = true;
    }

    return did_work;
}

bool P2PProxy::StepSendDataCopy(SendOpContext& op_ctx, TransferTask& task) {
    if (task.copy_ready_event_ == nullptr) {
        task.state_ = TransferTaskState::kTransfer;
        return true;
    }

    cudaError_t query_error = cudaSuccess;
    if (op_ctx.cuda_device_index_ >= 0) {
        c10::cuda::CUDAGuard device_guard(op_ctx.cuda_device_index_);
        query_error = cudaEventQuery(task.copy_ready_event_);
    } else {
        query_error = cudaEventQuery(task.copy_ready_event_);
    }

    if (query_error == cudaSuccess) {
        task.cleanup_resources(op_ctx.cuda_device_index_);
        task.state_ = TransferTaskState::kTransfer;
        return true;
    }
    if (query_error == cudaErrorNotReady) {
        return false;
    }

    task.cleanup_resources(op_ctx.cuda_device_index_);
    TORCH_CHECK(false, "P2P send cudaEventQuery failed: ",
                cudaGetErrorString(query_error));
    return false;
}

bool P2PProxy::StepSendTransfer(SendOpContext& op_ctx, TransferTask& task) {
    bool did_work = false;
    if (!task.transfer_batch_id_.has_value()) {
        const BatchID batch_id = engine_->allocateBatchID(1);
        engine_->submitTransfer(
            batch_id, {TransferRequest{
                          .opcode = TransferRequest::WRITE,
                          .source = task.source_,
                          .target_id = meta_->segmentIDs[op_ctx.peer_rank_],
                          .target_offset = task.target_offset_,
                          .length = task.chunk_bytes_,
                      }});
        task.transfer_batch_id_ = batch_id;
        did_work = true;
    }

    TransferStatus transfer_status;
    engine_->getTransferStatus(task.transfer_batch_id_.value(), 0,
                               transfer_status);
    if (transfer_status.s == TransferStatusEnum::COMPLETED) {
        engine_->freeBatchID(task.transfer_batch_id_.value());
        task.transfer_batch_id_.reset();
        task.state_ = TransferTaskState::kDone;
        return true;
    }
    if (transfer_status.s == TransferStatusEnum::FAILED) {
        engine_->freeBatchID(task.transfer_batch_id_.value());
        task.transfer_batch_id_.reset();
        TORCH_CHECK(false, "P2P send transfer failed.");
        return false;
    }

    return did_work;
}

bool P2PProxy::StepSendHeadCommit(SendOpContext& op_ctx, uint32_t capacity) {
    bool did_work = false;
    if (op_ctx.head_update_batch_id_.has_value()) {
        TransferStatus head_status;
        engine_->getTransferStatus(op_ctx.head_update_batch_id_.value(), 0,
                                   head_status);
        if (head_status.s == TransferStatusEnum::COMPLETED) {
            engine_->freeBatchID(op_ctx.head_update_batch_id_.value());
            op_ctx.head_update_batch_id_.reset();
            did_work = true;
        } else if (head_status.s == TransferStatusEnum::FAILED) {
            engine_->freeBatchID(op_ctx.head_update_batch_id_.value());
            op_ctx.head_update_batch_id_.reset();
            TORCH_CHECK(false, "P2P ctrl head update failed.");
            return false;
        }
    }

    if (op_ctx.head_update_batch_id_.has_value()) {
        return did_work;
    }

    uint32_t committed_tasks = 0;
    while (!op_ctx.tasks_.empty() &&
           op_ctx.tasks_.front().state_ == TransferTaskState::kDone) {
        op_ctx.tasks_.pop_front();
        ++committed_tasks;
        did_work = true;
    }

    if (committed_tasks == 0) {
        return did_work;
    }

    const uint32_t current_head =
        resources_.ctrl_send_region_[op_ctx.peer_rank_].head.load(
            std::memory_order_relaxed);
    const uint32_t next_head = (current_head + committed_tasks) % capacity;
    resources_.ctrl_send_region_[op_ctx.peer_rank_].head.store(
        next_head, std::memory_order_release);
    void* head_source = static_cast<void*>(
        &resources_.ctrl_send_region_[op_ctx.peer_rank_].head.value);
    const uint64_t remote_head_offset =
        meta_->segmentInfos[op_ctx.peer_rank_].p2p_ctrl_recv +
        rank_ * sizeof(P2PControlSlot) + offsetof(P2PControlSlot, head);

    const BatchID batch_id = engine_->allocateBatchID(1);
    engine_->submitTransfer(
        batch_id, {TransferRequest{
                      .opcode = TransferRequest::WRITE,
                      .source = head_source,
                      .target_id = meta_->segmentIDs[op_ctx.peer_rank_],
                      .target_offset = remote_head_offset,
                      .length = sizeof(uint32_t),
                  }});
    op_ctx.head_update_batch_id_ = batch_id;
    did_work = true;

    return did_work;
}

bool P2PProxy::IsSendOpCompleted(const SendOpContext& op_ctx) const {
    return op_ctx.bytes_issued_ == op_ctx.total_bytes_ &&
           op_ctx.tasks_.empty() && !op_ctx.head_update_batch_id_.has_value();
}

bool P2PProxy::TryIssueRecvTask(RecvOpContext& op_ctx, uint32_t capacity) {
    if (op_ctx.bytes_issued_ >= op_ctx.total_bytes_) {
        return false;
    }

    auto* local_ctrl = &resources_.ctrl_recv_region_[op_ctx.peer_rank_];
    const uint32_t head = local_ctrl->head.load(std::memory_order_acquire);
    const uint32_t tail = op_ctx.local_tail_;
    if (head == tail) {
        return false;
    }

    const uint64_t chunk_bytes =
        std::min(static_cast<uint64_t>(kP2PSlotSize),
                 op_ctx.total_bytes_ - op_ctx.bytes_issued_);
    const uint64_t recv_addr_base =
        meta_->segmentInfos[rank_].p2p_recv_buffer +
        static_cast<uint64_t>(op_ctx.peer_rank_) * kP2PBytesPerRank;
    const uint64_t recv_addr =
        recv_addr_base + static_cast<uint64_t>(tail) * kP2PSlotSize;
    auto* tensor_ptr = static_cast<uint8_t*>(op_ctx.tensor_.data_ptr());
    void* target_ptr = static_cast<void*>(tensor_ptr + op_ctx.bytes_issued_);

    op_ctx.tasks_.emplace_back(op_ctx.bytes_issued_, chunk_bytes,
                               reinterpret_cast<void*>(recv_addr), target_ptr);
    auto& task = op_ctx.tasks_.back();

    if (is_cpu_) {
        std::memcpy(task.target_, task.source_, task.chunk_bytes_);
        task.state_ = RecvTaskState::kDone;
    } else {
        c10::cuda::CUDAGuard device_guard(op_ctx.cuda_device_index_);
        cudaError_t copy_error =
            cudaMemcpyAsync(task.target_, task.source_, task.chunk_bytes_,
                            cudaMemcpyDeviceToDevice, op_ctx.cuda_stream_);
        TORCH_CHECK(!copy_error, "P2P recv cudaMemcpyAsync failed: ",
                    cudaGetErrorString(copy_error));
        copy_error = cudaEventCreateWithFlags(&task.copy_ready_event_,
                                              cudaEventDisableTiming);
        TORCH_CHECK(!copy_error, "P2P recv cudaEventCreateWithFlags failed: ",
                    cudaGetErrorString(copy_error));
        copy_error =
            cudaEventRecord(task.copy_ready_event_, op_ctx.cuda_stream_);
        if (copy_error != cudaSuccess) {
            task.cleanup_resources(op_ctx.cuda_device_index_);
            TORCH_CHECK(false, "P2P recv cudaEventRecord failed: ",
                        cudaGetErrorString(copy_error));
        }
    }

    op_ctx.bytes_issued_ += task.chunk_bytes_;
    op_ctx.local_tail_ = (tail + 1) % capacity;
    return true;
}

bool P2PProxy::StepRecvTask(RecvOpContext& op_ctx, RecvTransferTask& task) {
    bool did_work = false;

    if (task.state_ == RecvTaskState::kDataCopy &&
        StepRecvDataCopy(op_ctx, task)) {
        did_work = true;
    }

    return did_work;
}

bool P2PProxy::StepRecvDataCopy(RecvOpContext& op_ctx, RecvTransferTask& task) {
    if (task.copy_ready_event_ == nullptr) {
        task.state_ = RecvTaskState::kDone;
        return true;
    }

    cudaError_t query_error = cudaSuccess;
    if (op_ctx.cuda_device_index_ >= 0) {
        c10::cuda::CUDAGuard device_guard(op_ctx.cuda_device_index_);
        query_error = cudaEventQuery(task.copy_ready_event_);
    } else {
        query_error = cudaEventQuery(task.copy_ready_event_);
    }

    if (query_error == cudaSuccess) {
        task.cleanup_resources(op_ctx.cuda_device_index_);
        task.state_ = RecvTaskState::kDone;
        return true;
    }
    if (query_error == cudaErrorNotReady) {
        return false;
    }

    task.cleanup_resources(op_ctx.cuda_device_index_);
    TORCH_CHECK(false, "P2P recv cudaEventQuery failed: ",
                cudaGetErrorString(query_error));
    return false;
}

bool P2PProxy::StepRecvTailCommit(RecvOpContext& op_ctx, uint32_t capacity) {
    bool did_work = false;
    if (op_ctx.tail_update_batch_id_.has_value()) {
        TransferStatus tail_status;
        engine_->getTransferStatus(op_ctx.tail_update_batch_id_.value(), 0,
                                   tail_status);
        if (tail_status.s == TransferStatusEnum::COMPLETED) {
            engine_->freeBatchID(op_ctx.tail_update_batch_id_.value());
            op_ctx.tail_update_batch_id_.reset();
            did_work = true;
        } else if (tail_status.s == TransferStatusEnum::FAILED) {
            engine_->freeBatchID(op_ctx.tail_update_batch_id_.value());
            op_ctx.tail_update_batch_id_.reset();
            TORCH_CHECK(false, "P2P ctrl tail update failed.");
            return false;
        }
    }

    if (op_ctx.tail_update_batch_id_.has_value()) {
        return did_work;
    }

    uint32_t committed_tasks = 0;
    while (!op_ctx.tasks_.empty() &&
           op_ctx.tasks_.front().state_ == RecvTaskState::kDone) {
        op_ctx.tasks_.pop_front();
        ++committed_tasks;
        did_work = true;
    }

    if (committed_tasks == 0) {
        return did_work;
    }

    auto* local_ctrl = &resources_.ctrl_recv_region_[op_ctx.peer_rank_];
    const uint32_t current_tail =
        local_ctrl->tail.load(std::memory_order_relaxed);
    const uint32_t next_tail = (current_tail + committed_tasks) % capacity;
    local_ctrl->tail.store(next_tail, std::memory_order_release);

    const uint64_t remote_tail_offset =
        meta_->segmentInfos[op_ctx.peer_rank_].p2p_ctrl_send +
        rank_ * sizeof(P2PControlSlot) + offsetof(P2PControlSlot, tail);
    void* tail_source = static_cast<void*>(&local_ctrl->tail.value);

    const BatchID batch_id = engine_->allocateBatchID(1);
    engine_->submitTransfer(
        batch_id, {TransferRequest{
                      .opcode = TransferRequest::WRITE,
                      .source = tail_source,
                      .target_id = meta_->segmentIDs[op_ctx.peer_rank_],
                      .target_offset = remote_tail_offset,
                      .length = sizeof(uint32_t),
                  }});
    op_ctx.tail_update_batch_id_ = batch_id;
    did_work = true;

    return did_work;
}

bool P2PProxy::IsRecvDataCopyCompleted(const RecvOpContext& op_ctx) const {
    return op_ctx.bytes_issued_ == op_ctx.total_bytes_ &&
           op_ctx.tasks_.empty() && !op_ctx.tail_update_batch_id_.has_value();
}

bool P2PProxy::StepRecvOpUpdateTail(RecvOpContext& op_ctx) {
    if (op_ctx.original_tensor_.is_contiguous()) {
        op_ctx.state_ = RecvOpState::kDone;
        return true;
    }

    if (is_cpu_) {
        op_ctx.original_tensor_.copy_(op_ctx.tensor_);
        op_ctx.state_ = RecvOpState::kDone;
        return true;
    }

    if (!op_ctx.op_copy_started_) {
        TORCH_CHECK(op_ctx.cuda_stream_ != nullptr,
                    "P2P recv op CUDA stream is null.");
        TORCH_CHECK(op_ctx.cuda_device_index_ >= 0,
                    "P2P recv op CUDA device index is invalid.");
        c10::cuda::CUDAGuard device_guard(op_ctx.cuda_device_index_);
        const auto stream = c10::cuda::getStreamFromExternal(
            op_ctx.cuda_stream_, op_ctx.cuda_device_index_);
        c10::cuda::CUDAStreamGuard stream_guard(stream);
        op_ctx.original_tensor_.copy_(op_ctx.tensor_);

        cudaError_t event_error = cudaEventCreateWithFlags(
            &op_ctx.op_copy_event_, cudaEventDisableTiming);
        TORCH_CHECK(event_error == cudaSuccess,
                    "P2P recv final copy cudaEventCreateWithFlags failed: ",
                    cudaGetErrorString(event_error));
        event_error =
            cudaEventRecord(op_ctx.op_copy_event_, op_ctx.cuda_stream_);
        if (event_error != cudaSuccess) {
            cudaEventDestroy(op_ctx.op_copy_event_);
            op_ctx.op_copy_event_ = nullptr;
            TORCH_CHECK(false, "P2P recv final copy cudaEventRecord failed: ",
                        cudaGetErrorString(event_error));
        }
        op_ctx.op_copy_started_ = true;
        return true;
    }

    TORCH_CHECK(op_ctx.op_copy_event_ != nullptr,
                "P2P recv final copy event is null.");
    cudaError_t query_error = cudaSuccess;
    {
        c10::cuda::CUDAGuard device_guard(op_ctx.cuda_device_index_);
        query_error = cudaEventQuery(op_ctx.op_copy_event_);
    }

    if (query_error == cudaSuccess) {
        cudaEventDestroy(op_ctx.op_copy_event_);
        op_ctx.op_copy_event_ = nullptr;
        op_ctx.op_copy_started_ = false;
        op_ctx.state_ = RecvOpState::kDone;
        return true;
    }
    if (query_error == cudaErrorNotReady) {
        return false;
    }

    cudaEventDestroy(op_ctx.op_copy_event_);
    op_ctx.op_copy_event_ = nullptr;
    op_ctx.op_copy_started_ = false;
    TORCH_CHECK(false, "P2P recv final copy cudaEventQuery failed: ",
                cudaGetErrorString(query_error));
    return false;
}

bool P2PProxy::StepRecvOpState(RecvOpContext& op_ctx) {
    bool did_work = false;

    if (op_ctx.state_ == RecvOpState::kDataCopy &&
        IsRecvDataCopyCompleted(op_ctx)) {
        op_ctx.state_ = RecvOpState::kUpdateTail;
        did_work = true;
    }

    if (op_ctx.state_ == RecvOpState::kUpdateTail &&
        StepRecvOpUpdateTail(op_ctx)) {
        did_work = true;
    }

    return did_work;
}

void P2PProxy::SendWorkerThread() {
    const uint32_t capacity = static_cast<uint32_t>(kP2PNumSlotsPerRank);

    while (true) {
        bool did_work = false;

        {
            std::unique_lock<std::mutex> lock(send_queue_mutex_);
            while (send_queue_.empty() && send_worker_running_.load() &&
                   !HasLocalSendWork()) {
                send_queue_cv_.wait_for(lock, std::chrono::microseconds(50));
            }

            while (!send_queue_.empty()) {
                SendOpContext op_ctx = std::move(send_queue_.front());
                send_queue_.pop();
                send_peer_lanes_[op_ctx.peer_rank_].pending_send_ops_.push_back(
                    std::move(op_ctx));
                did_work = true;
            }
        }

        if (!send_worker_running_.load()) {
            break;
        }

        for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
            auto& lane = send_peer_lanes_[peer_rank];
            if (lane.active_send_op_.has_value() ||
                lane.pending_send_ops_.empty()) {
                continue;
            }

            SendOpContext op_ctx = std::move(lane.pending_send_ops_.front());
            lane.pending_send_ops_.pop_front();
            if (op_ctx.total_bytes_ == 0) {
                op_ctx.completed_->store(true, std::memory_order_release);
                did_work = true;
                continue;
            }

            lane.active_send_op_ = std::move(op_ctx);
            did_work = true;
        }

        for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
            auto& lane = send_peer_lanes_[peer_rank];
            if (!lane.active_send_op_.has_value()) {
                continue;
            }

            auto& op_ctx = lane.active_send_op_.value();

            while (TryIssueTask(op_ctx, capacity)) {
                did_work = true;
            }

            for (auto& task : op_ctx.tasks_) {
                if (task.state_ == TransferTaskState::kDone) {
                    continue;
                }
                if (StepSendTask(op_ctx, task)) {
                    did_work = true;
                }
            }

            if (StepSendHeadCommit(op_ctx, capacity)) {
                did_work = true;
            }

            if (IsSendOpCompleted(op_ctx)) {
                op_ctx.completed_->store(true, std::memory_order_release);
                lane.active_send_op_.reset();
                did_work = true;
            }
        }

        if (!did_work) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }
}

void P2PProxy::RecvWorkerThread() {
    const uint32_t capacity = static_cast<uint32_t>(kP2PNumSlotsPerRank);

    while (true) {
        bool did_work = false;

        {
            std::unique_lock<std::mutex> lock(recv_queue_mutex_);
            while (recv_queue_.empty() &&
                   recv_worker_running_.load(std::memory_order_acquire) &&
                   !HasLocalRecvWork()) {
                recv_queue_cv_.wait_for(lock, std::chrono::microseconds(50));
            }

            while (!recv_queue_.empty()) {
                RecvOp op = std::move(recv_queue_.front());
                recv_queue_.pop();
                recv_peer_lanes_[op.peer_rank_].pending_recv_ops_.push_back(
                    std::move(op));
                did_work = true;
            }
        }

        if (!recv_worker_running_.load(std::memory_order_acquire)) {
            break;
        }

        for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
            auto& lane = recv_peer_lanes_[peer_rank];
            if (lane.active_recv_op_.has_value() ||
                lane.pending_recv_ops_.empty()) {
                continue;
            }

            if (lane.pending_recv_ops_.front().seq_ !=
                meta_->p2pRecvNextExpected[peer_rank]) {
                continue;
            }

            RecvOp recv_op = std::move(lane.pending_recv_ops_.front());
            lane.pending_recv_ops_.pop_front();

            const uint32_t initial_tail =
                resources_.ctrl_recv_region_[peer_rank].tail.load(
                    std::memory_order_acquire);
            RecvOpContext op_ctx(std::move(recv_op), initial_tail);
            lane.active_recv_op_ = std::move(op_ctx);
            did_work = true;
        }

        for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
            auto& lane = recv_peer_lanes_[peer_rank];
            if (!lane.active_recv_op_.has_value()) {
                continue;
            }

            auto& op_ctx = lane.active_recv_op_.value();
            while (TryIssueRecvTask(op_ctx, capacity)) {
                did_work = true;
            }

            for (auto& task : op_ctx.tasks_) {
                if (task.state_ == RecvTaskState::kDone) {
                    continue;
                }
                if (StepRecvTask(op_ctx, task)) {
                    did_work = true;
                }
            }

            if (StepRecvTailCommit(op_ctx, capacity)) {
                did_work = true;
            }

            if (StepRecvOpState(op_ctx)) {
                did_work = true;
            }

            if (op_ctx.state_ == RecvOpState::kDone) {
                op_ctx.completed_->store(true, std::memory_order_release);
                meta_->p2pRecvNextExpected[peer_rank] = op_ctx.seq_ + 1;
                lane.active_recv_op_.reset();
                did_work = true;
            }
        }

        if (!did_work) {
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    }
}

}  // namespace mooncake
