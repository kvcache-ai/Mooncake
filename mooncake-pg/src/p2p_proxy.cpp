#include <memory>
#include <mutex>
#include <p2p_proxy.h>
#include <ATen/cuda/CUDAContext.h>
#include <cuda_runtime.h>
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <thread>
#include "cuda_alike.h"
#include "memory_location.h"
#include "pg_utils.h"

namespace mooncake {

namespace {

size_t getEnv_size_t(const char* name, size_t default_val) {
    const char* val = std::getenv(name);
    return val ? std::strtoull(val, nullptr, 10) : default_val;
}

void setCudaDeviceIfNeeded(bool is_cpu, int cuda_device_index,
                           const char* context) {
    if (is_cpu) {
        return;
    }
    TORCH_CHECK(cuda_device_index >= 0, context,
                ": invalid CUDA device index.");
    const cudaError_t set_device_error = cudaSetDevice(cuda_device_index);
    TORCH_CHECK(set_device_error == cudaSuccess, context, ": ",
                cudaGetErrorString(set_device_error));
}

}  // namespace

void P2PChunkPool::init(void* base_addr, size_t chunk_size,
                        uint32_t num_chunks) {
    base_addr_ = base_addr;
    chunk_size_ = chunk_size;
    free_stack_.clear();
    free_stack_.reserve(num_chunks);
    for (uint32_t idx = 0; idx < num_chunks; ++idx) {
        free_stack_.push_back(static_cast<uint8_t*>(base_addr_) +
                              (num_chunks - 1 - idx) * chunk_size_);
    }
}

void* P2PChunkPool::acquire() {
    if (free_stack_.empty()) {
        return nullptr;
    }
    void* ptr = free_stack_.back();
    free_stack_.pop_back();
    return ptr;
}

void P2PChunkPool::release(void* ptr) { free_stack_.push_back(ptr); }

// Consumer-side reliable read.
//
// 1. Load header_token with acquire semantics.
// 2. If it is kInvalidControlToken the slot is empty -> fail.
// 3. Optimistically copy the payload.
// 4. Issue an acquire fence so the payload reads cannot be reordered past
//    the footer load that follows.
// 5. Load footer_token.
// 6. Accept the payload only when header == footer.
bool CreditSlot::tryLoad(uint64_t& out_recv_addr, uint32_t& out_chunk_len,
                         uint32_t& out_epoch, uint32_t& out_sequence) const {
    uint64_t h = std::atomic_ref(header_token).load(std::memory_order_acquire);
    if (h == kInvalidControlToken) return false;

    // Optimistic payload copy – may be torn if the NIC is still writing.
    uint64_t addr = std::atomic_ref(recv_addr).load(std::memory_order_relaxed);
    uint32_t len = std::atomic_ref(chunk_len).load(std::memory_order_relaxed);

    // Avoid moving payload reads below the footer check.
    std::atomic_thread_fence(std::memory_order_acquire);

    uint64_t f = std::atomic_ref(footer_token).load(std::memory_order_relaxed);

    if (h == f) {
        out_recv_addr = addr;
        out_chunk_len = len;
        out_epoch = static_cast<uint32_t>(h >> 32);
        out_sequence = static_cast<uint32_t>(h);
        return true;
    }
    // Torn write – caller retries next poll.
    return false;
}

// Producer-side reliable publish.
//
// On the local CPU the store order matters:
//   payload -> footer (relaxed) -> header (release).
// The release store to header_token guarantees that every prior store is
// visible to any consumer that sees the new header value.
void CreditSlot::publish(uint32_t epoch, uint32_t seq, uint64_t addr,
                         uint32_t len) {
    uint64_t new_token = makeControlToken(epoch, seq);

    recv_addr = addr;
    chunk_len = len;

    // Write footer first so it is never ahead of the header.
    std::atomic_ref(footer_token).store(new_token, std::memory_order_relaxed);
    // Release the header last
    std::atomic_ref(header_token).store(new_token, std::memory_order_release);
}

void CreditSlot::reset() {
    recv_addr = 0;
    chunk_len = 0;
    uint64_t inv = kInvalidControlToken;
    std::atomic_ref(footer_token).store(inv, std::memory_order_relaxed);
    std::atomic_ref(header_token).store(inv, std::memory_order_release);
}

// See CreditSlot::tryLoad() for the detailed description.
bool AckSlot::tryLoad(uint32_t& out_chunk_len, uint32_t& out_epoch,
                      uint32_t& out_sequence) const {
    uint64_t h = std::atomic_ref(header_token).load(std::memory_order_acquire);
    if (h == kInvalidControlToken) return false;

    uint32_t len = std::atomic_ref(chunk_len).load(std::memory_order_relaxed);

    std::atomic_thread_fence(std::memory_order_acquire);

    uint64_t f = std::atomic_ref(footer_token).load(std::memory_order_relaxed);

    if (h == f) {
        out_chunk_len = len;
        out_epoch = static_cast<uint32_t>(h >> 32);
        out_sequence = static_cast<uint32_t>(h);
        return true;
    }
    return false;
}

// See CreditSlot::publish() for the detailed description.
void AckSlot::publish(uint32_t epoch, uint32_t seq, uint32_t len) {
    uint64_t new_token = makeControlToken(epoch, seq);

    chunk_len = len;

    std::atomic_ref(footer_token).store(new_token, std::memory_order_relaxed);
    std::atomic_ref(header_token).store(new_token, std::memory_order_release);
}

void AckSlot::reset() {
    chunk_len = 0;
    uint64_t inv = kInvalidControlToken;
    std::atomic_ref(footer_token).store(inv, std::memory_order_relaxed);
    std::atomic_ref(header_token).store(inv, std::memory_order_release);
}

P2PProxy::P2PProxy(TransferEngine* engine, const Options& options)
    : engine_(engine),
      is_cpu_(options.is_cpu),
      rank_(options.rank),
      size_(options.size),
      cuda_device_index_(options.cuda_device_index),
      transfer_timeout_ms_(options.transfer_timeout_ms) {
    if (!is_cpu_ && cuda_device_index_ < 0) {
        int current_device = -1;
        const cudaError_t get_device_error = cudaGetDevice(&current_device);
        TORCH_CHECK(get_device_error == cudaSuccess,
                    "P2PProxy cudaGetDevice failed: ",
                    cudaGetErrorString(get_device_error));
        cuda_device_index_ = current_device;
    }
    allocateResources();
}

P2PProxy::~P2PProxy() {
    if (resource_abandoned_) {
        LOG(WARNING) << "Resource leak in P2PProxy: cleanup skipped due to "
                        "hung operations.";
        return;
    }

    releaseResources();
}

void P2PProxy::bindMeta(const std::shared_ptr<TransferGroupMeta>& meta) {
    meta_ = meta;
}

void P2PProxy::extendGroupSizeTo(int new_size) {
    TORCH_CHECK(new_size >= size_, "extendGroupSizeTo: new_size < size_");
    size_ = new_size;
}

// Allocate control regions only.
// Send/Recv chunk pools are owned by P2PDeviceWorker and shared across
// multiple P2PProxy instances on the same device.
void P2PProxy::allocateResources() {
    TORCH_CHECK(engine_, "P2PProxy engine is null.");
    if (resources_.credit_region_ != nullptr ||
        resources_.ack_region_ != nullptr) {
        return;
    }

    TORCH_CHECK(size_ > 0, "P2PProxy invalid group size: ", size_);
    TORCH_CHECK(static_cast<size_t>(size_) <= kMaxNumRanks,
                "P2PProxy group size exceeds kMaxNumRanks: ", size_);

    const size_t ctrl_slots =
        kMaxNumRanks * static_cast<size_t>(kP2PControlRingSize);
    resources_.credit_region_ = new CreditSlot[ctrl_slots]{};
    resources_.ack_region_ = new AckSlot[ctrl_slots]{};

    for (size_t i = 0; i < ctrl_slots; ++i) {
        resources_.credit_region_[i].reset();
        resources_.ack_region_[i].reset();
    }

    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        peer_epoch_[i].store(1, std::memory_order_release);
    }

    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        send_peer_lanes_[i].pending_send_ops_.clear();
        send_peer_lanes_[i].active_send_op_.reset();
        send_peer_lanes_[i].credit_consume_seq_ = 0;
        send_peer_lanes_[i].copy_ready_events_.fill(nullptr);

        recv_peer_lanes_[i].pending_recv_ops_.clear();
        recv_peer_lanes_[i].active_recv_op_.reset();
        recv_peer_lanes_[i].credit_issue_seq_ = 0;
        recv_peer_lanes_[i].ack_consume_seq_ = 0;
        recv_peer_lanes_[i].copy_ready_events_.fill(nullptr);
    }

    if (!is_cpu_) {
        for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
            for (auto& copy_ready_event :
                 send_peer_lanes_[peer_rank].copy_ready_events_) {
                const cudaError_t create_error = cudaEventCreateWithFlags(
                    &copy_ready_event, cudaEventDisableTiming);
                TORCH_CHECK(create_error == cudaSuccess,
                            "Failed to create pooled send copy-ready event: ",
                            cudaGetErrorString(create_error));
            }

            for (auto& copy_ready_event :
                 recv_peer_lanes_[peer_rank].copy_ready_events_) {
                const cudaError_t create_error = cudaEventCreateWithFlags(
                    &copy_ready_event, cudaEventDisableTiming);
                TORCH_CHECK(create_error == cudaSuccess,
                            "Failed to create pooled recv copy-ready event: ",
                            cudaGetErrorString(create_error));
            }
        }
    }

    int rc = engine_->registerLocalMemory(resources_.credit_region_,
                                          ctrl_slots * sizeof(CreditSlot),
                                          kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P credit region");

    rc = engine_->registerLocalMemory(resources_.ack_region_,
                                      ctrl_slots * sizeof(AckSlot),
                                      kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P ack region");

    // Staging buffers for control messages.  RDMA requires every
    // transfer source to live in a registered MR.
    resources_.credit_staging_buf_ = new CreditSlot[ctrl_slots]{};
    rc = engine_->registerLocalMemory(resources_.credit_staging_buf_,
                                      ctrl_slots * sizeof(CreditSlot),
                                      kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P credit staging region");

    resources_.ack_staging_buf_ = new AckSlot[ctrl_slots]{};
    rc = engine_->registerLocalMemory(resources_.ack_staging_buf_,
                                      ctrl_slots * sizeof(AckSlot),
                                      kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P ack staging region");
}

void P2PProxy::resetPeerState(int peer_rank) {
    TORCH_CHECK(peer_rank >= 0 && peer_rank < size_,
                "ResetPeerState: peer_rank out of range: ", peer_rank,
                " size: ", size_);

    // Epoch update:
    //   We bump epoch_ so that any slots still in flight from the
    //   old session (written by the sender/receiver before it learned about
    //   the Reset) are recognized as stale and skipped.
    peer_epoch_[peer_rank].fetch_add(1, std::memory_order_acq_rel);

    // Request reset
    reset_send_req_[peer_rank].store(true, std::memory_order_release);
    reset_recv_req_[peer_rank].store(true, std::memory_order_release);

    // Wake up worker
    if (device_worker_) {
        device_worker_->wakeUpSend();
        device_worker_->wakeUpRecv();
    }
}

// Reset sender state for peer_rank.
void P2PProxy::performSendReset(int peer_rank) {
    resetSendLane(send_peer_lanes_[peer_rank]);
    resetPeerControlLanes(peer_rank);
}

// Reset receiver state for peer_rank.
void P2PProxy::performRecvReset(int peer_rank) {
    resetRecvLane(recv_peer_lanes_[peer_rank]);
    resetPeerControlLanes(peer_rank);
}

void P2PProxy::reportBrokenPeer(int peer_rank) {
    resetPeerState(peer_rank);
    // Set peerConnected to notify the connection poller to reconnect it.
    meta_->peerConnected[peer_rank] = false;
    meta_->activeRanks[peer_rank] = false;
    meta_->activeRanksTensor[peer_rank] = 0;
    LOG(ERROR) << "Rank " << meta_->rank << " marking peer " << peer_rank
               << " as broken during P2P transfer.";
}

void P2PProxy::releaseSendTaskResources(SendTransferTask& task) const {
    if (task.staging_addr_ != nullptr) {
        if (send_pool_) send_pool_->release(task.staging_addr_);
        task.staging_addr_ = nullptr;
    }
    if (task.transfer_batch_id_.has_value()) {
        if (engine_) engine_->freeBatchID(task.transfer_batch_id_.value());
        task.transfer_batch_id_.reset();
    }
    if (task.ack_batch_id_.has_value()) {
        if (engine_) engine_->freeBatchID(task.ack_batch_id_.value());
        task.ack_batch_id_.reset();
    }
}

void P2PProxy::releaseRecvTaskResources(RecvTransferTask& task) const {
    if (task.local_addr_ != nullptr) {
        if (recv_pool_) recv_pool_->release(task.local_addr_);
        task.local_addr_ = nullptr;
    }
    if (task.credit_batch_id_.has_value()) {
        if (engine_) engine_->freeBatchID(task.credit_batch_id_.value());
        task.credit_batch_id_.reset();
    }
}

void P2PProxy::resetSendLane(SendPeerLane& lane) {
    for (auto& pending : lane.pending_send_ops_) {
        pending.status_->store(OpStatus::kFailed, std::memory_order_release);
        active_send_tasks_.fetch_sub(1, std::memory_order_release);
    }
    lane.pending_send_ops_.clear();

    if (lane.active_send_op_.has_value()) {
        auto& op_ctx = lane.active_send_op_.value();
        for (auto& task : op_ctx.tasks_) {
            releaseSendTaskResources(task);
        }
        op_ctx.tasks_.clear();
        op_ctx.status_->store(OpStatus::kFailed, std::memory_order_release);
        active_send_tasks_.fetch_sub(1, std::memory_order_release);
        lane.active_send_op_.reset();
    }
    lane.credit_consume_seq_ = 0;
}

void P2PProxy::resetRecvLane(RecvPeerLane& lane) {
    for (auto& pending : lane.pending_recv_ops_) {
        pending.status_->store(OpStatus::kFailed, std::memory_order_release);
        active_recv_tasks_.fetch_sub(1, std::memory_order_release);
    }
    lane.pending_recv_ops_.clear();

    if (lane.active_recv_op_.has_value()) {
        auto& op_ctx = lane.active_recv_op_.value();
        for (auto& task : op_ctx.tasks_) {
            releaseRecvTaskResources(task);
        }
        op_ctx.tasks_.clear();
        op_ctx.status_->store(OpStatus::kFailed, std::memory_order_release);
        active_recv_tasks_.fetch_sub(1, std::memory_order_release);
        lane.active_recv_op_.reset();
    }
    lane.credit_issue_seq_ = 0;
    lane.ack_consume_seq_ = 0;
}

void P2PProxy::resetPeerControlLanes(int peer_rank) {
    auto* credit_lane = getLocalCreditLane(peer_rank);
    auto* ack_lane = getLocalAckLane(peer_rank);
    for (uint32_t i = 0; i < kP2PControlRingSize; ++i) {
        credit_lane[i].reset();
        ack_lane[i].reset();
    }
}

void P2PProxy::releaseResources() {
    TORCH_CHECK(!resource_abandoned_,
                "Should not release abandoned resources.");

    setCudaDeviceIfNeeded(is_cpu_, cuda_device_index_,
                          "P2PProxy ReleaseResources cudaSetDevice failed");

    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        // Reset lane
        auto& send_lane = send_peer_lanes_[i];
        auto& recv_lane = recv_peer_lanes_[i];
        resetSendLane(send_lane);
        resetRecvLane(recv_lane);

        // Destroy cuda events
        auto destroyEvents = [](auto& events) {
            for (auto& ev : events) {
                if (ev == nullptr) continue;
                const cudaError_t err = cudaEventDestroy(ev);
                TORCH_CHECK(err == cudaSuccess,
                            "Failed to destroy pooled copy-ready event: ",
                            cudaGetErrorString(err));
                ev = nullptr;
            }
        };
        destroyEvents(send_lane.copy_ready_events_);
        destroyEvents(recv_lane.copy_ready_events_);
    }

    if (resources_.credit_staging_buf_ != nullptr) {
        if (engine_)
            engine_->unregisterLocalMemory(resources_.credit_staging_buf_);
        delete[] resources_.credit_staging_buf_;
        resources_.credit_staging_buf_ = nullptr;
    }

    if (resources_.ack_staging_buf_ != nullptr) {
        if (engine_)
            engine_->unregisterLocalMemory(resources_.ack_staging_buf_);
        delete[] resources_.ack_staging_buf_;
        resources_.ack_staging_buf_ = nullptr;
    }

    if (resources_.credit_region_ != nullptr) {
        if (engine_) engine_->unregisterLocalMemory(resources_.credit_region_);
        delete[] resources_.credit_region_;
        resources_.credit_region_ = nullptr;
    }

    if (resources_.ack_region_ != nullptr) {
        if (engine_) engine_->unregisterLocalMemory(resources_.ack_region_);
        delete[] resources_.ack_region_;
        resources_.ack_region_ = nullptr;
    }
}

void P2PProxy::abandonResources() { resource_abandoned_ = true; }

void P2PProxy::enqueueSend(SendOp op) {
    op.tensor_ =
        op.tensor_.is_contiguous() ? op.tensor_ : op.tensor_.contiguous();

    {
        std::lock_guard<std::mutex> lock(send_queue_mutex_);
        send_queue_.emplace(std::move(op));
    }
    active_send_tasks_.fetch_add(1, std::memory_order_release);
    if (device_worker_) device_worker_->wakeUpSend();
}

void P2PProxy::enqueueRecv(RecvOp op) {
    {
        std::lock_guard<std::mutex> lock(recv_queue_mutex_);
        recv_queue_.push(std::move(op));
    }
    active_recv_tasks_.fetch_add(1, std::memory_order_release);
    if (device_worker_) device_worker_->wakeUpRecv();
}

P2PProxy::SendTransferTask::SendTransferTask(
    uint64_t tensor_offset_in, uint32_t chunk_len_in, void* staging_addr_in,
    uint64_t remote_addr_in, uint32_t sequence_in, uint32_t epoch_in)
    : tensor_offset_(tensor_offset_in),
      chunk_len_(chunk_len_in),
      staging_addr_(staging_addr_in),
      remote_addr_(remote_addr_in),
      sequence_(sequence_in),
      epoch_(epoch_in) {
    last_update_time_ = std::chrono::steady_clock::now();
}

P2PProxy::SendOpContext::SendOpContext(SendOp&& op_in)
    : status_(std::move(op_in.status_)),
      tensor_(std::move(op_in.tensor_)),
      peer_rank_(op_in.peer_rank_),
      cuda_stream_(op_in.cuda_stream_) {
    total_bytes_ =
        tensor_.numel() * static_cast<uint64_t>(tensor_.element_size());
    last_update_time_ = std::chrono::steady_clock::now();
}

P2PProxy::RecvTransferTask::RecvTransferTask(uint64_t tensor_offset_in,
                                             uint32_t chunk_len_in,
                                             void* local_addr_in,
                                             uint32_t sequence_in,
                                             uint32_t epoch_in)
    : tensor_offset_(tensor_offset_in),
      chunk_len_(chunk_len_in),
      local_addr_(local_addr_in),
      sequence_(sequence_in),
      epoch_(epoch_in) {
    last_update_time_ = std::chrono::steady_clock::now();
}

P2PProxy::RecvOpContext::RecvOpContext(RecvOp&& op_in)
    : status_(std::move(op_in.status_)),
      tensor_(std::move(op_in.tensor_)),
      original_tensor_(std::move(op_in.original_tensor_)),
      peer_rank_(op_in.peer_rank_),
      cuda_stream_(op_in.cuda_stream_) {
    total_bytes_ =
        tensor_.numel() * static_cast<uint64_t>(tensor_.element_size());
}

CreditSlot* P2PProxy::getLocalCreditLane(int peer_rank) const {
    return resources_.credit_region_ +
           static_cast<size_t>(peer_rank) * kP2PControlRingSize;
}

AckSlot* P2PProxy::getLocalAckLane(int peer_rank) const {
    return resources_.ack_region_ +
           static_cast<size_t>(peer_rank) * kP2PControlRingSize;
}

uint64_t P2PProxy::getRemoteCreditSlot(int peer_rank, uint32_t sequence) const {
    const uint64_t slot_index = sequence % kP2PControlRingSize;
    return meta_->segmentInfos[peer_rank].p2p_credit_region +
           (static_cast<uint64_t>(rank_) * kP2PControlRingSize + slot_index) *
               sizeof(CreditSlot);
}

uint64_t P2PProxy::getRemoteAckSlot(int peer_rank, uint32_t sequence) const {
    const uint64_t slot_index = sequence % kP2PControlRingSize;
    return meta_->segmentInfos[peer_rank].p2p_ack_region +
           (static_cast<uint64_t>(rank_) * kP2PControlRingSize + slot_index) *
               sizeof(AckSlot);
}

CreditSlot* P2PProxy::getLocalCreditStagingBuf(int peer_rank,
                                               uint32_t sequence) const {
    const size_t staging_idx =
        static_cast<size_t>(peer_rank) * kP2PControlRingSize +
        sequence % kP2PControlRingSize;
    return resources_.credit_staging_buf_ + staging_idx;
}
AckSlot* P2PProxy::getLocalAckStagingBuf(int peer_rank,
                                         uint32_t sequence) const {
    const size_t staging_idx =
        static_cast<size_t>(peer_rank) * kP2PControlRingSize +
        static_cast<size_t>(sequence % kP2PControlRingSize);
    return resources_.ack_staging_buf_ + staging_idx;
}

// Receiver issues credit.
//
// Grab a free chunk from RecvPool and grant the sender credit to write into it.
// We write a CreditSlot into the sender's CreditLane so the sender knows:
//   (a) which sequence this is,
//   (b) the RecvPool offset to write to,
//   (c) how many bytes we expect.
// If the pool is exhausted we return false and retry on the next polling
// iteration (non-blocking).
bool P2PProxy::tryIssueRecvTask(RecvOpContext& op_ctx, RecvPeerLane& lane) {
    if (op_ctx.bytes_credited_ >= op_ctx.total_bytes_) {
        return false;
    }

    if (op_ctx.tasks_.size() >= kP2PControlRingSize) {
        // No free control slot, retry on the next iteration
        return false;
    }

    void* local_addr = recv_pool_->acquire();
    if (local_addr == nullptr) {
        // No free recv chunk, retry on the next iteration
        return false;
    }

    const uint32_t chunk_len = static_cast<uint32_t>(std::min<uint64_t>(
        chunk_size_, op_ctx.total_bytes_ - op_ctx.bytes_credited_));
    const uint64_t seq = lane.credit_issue_seq_;
    const uint64_t remote_credit_offset =
        getRemoteCreditSlot(op_ctx.peer_rank_, seq);

    const uint32_t curr_epoch =
        peer_epoch_[op_ctx.peer_rank_].load(std::memory_order_acquire);
    op_ctx.tasks_.emplace_back(op_ctx.bytes_credited_, chunk_len, local_addr,
                               seq, curr_epoch);
    auto& task = op_ctx.tasks_.back();

    auto* credit_staging_buf = getLocalCreditStagingBuf(op_ctx.peer_rank_, seq);
    credit_staging_buf->publish(
        curr_epoch, seq, reinterpret_cast<uint64_t>(local_addr), chunk_len);
    const BatchID batch_id = engine_->allocateBatchID(1);
    engine_->submitTransfer(
        batch_id, {TransferRequest{
                      .opcode = TransferRequest::WRITE,
                      .source = static_cast<void*>(credit_staging_buf),
                      .target_id = meta_->segmentIDs[op_ctx.peer_rank_],
                      .target_offset = remote_credit_offset,
                      .length = sizeof(CreditSlot),
                  }});
    task.credit_batch_id_ = batch_id;
    task.last_update_time_ = std::chrono::steady_clock::now();

    ++lane.credit_issue_seq_;
    op_ctx.bytes_credited_ += chunk_len;
    return true;
}

// Drive a single receiver chunk through its state machine.
//
// kIssueCredit -> kWaitAck -> kCopyOut -> kFinished -> erase.
bool P2PProxy::stepRecvTask(RecvTransferTask& task) {
    switch (task.state_) {
        case RecvTaskState::kIssueCredit:
            return stepRecvIssueCredit(task);

        case RecvTaskState::kWaitAck:
            // kWaitAck is polled in stepRecv
            return false;

        case RecvTaskState::kCopyOut:
            return stepRecvCopyOut(task);

        case RecvTaskState::kFinished:
        case RecvTaskState::kFailed:
            return false;
    }
    return false;
}

bool P2PProxy::stepRecvIssueCredit(RecvTransferTask& task) {
    TORCH_CHECK(task.credit_batch_id_.has_value(),
                "Expected a credit_batch_id in tryIssueRecvTask");

    TransferStatus credit_status;
    engine_->getTransferStatus(task.credit_batch_id_.value(), 0, credit_status);

    if (credit_status.s == TransferStatusEnum::COMPLETED) {
        engine_->freeBatchID(task.credit_batch_id_.value());
        task.credit_batch_id_.reset();
        task.state_ = RecvTaskState::kWaitAck;
        task.last_update_time_ = std::chrono::steady_clock::now();
        return true;
    }

    if (credit_status.s == TransferStatusEnum::FAILED || isTimeout(task)) {
        LOG(ERROR) << "P2P credit transfer failed/timeout, seq="
                   << task.sequence_;
        engine_->freeBatchID(task.credit_batch_id_.value());
        task.credit_batch_id_.reset();
        task.state_ = RecvTaskState::kFailed;
        return true;
    }
    return false;
}

// Poll the GPU Copy-Out event.
//
// After the AckSlot arrives we initiate cudaMemcpyAsync from the
// RecvPool chunk to the user tensor and record an event.  This function
// waits for that event and returns the chunk to RecvPool.
bool P2PProxy::stepRecvCopyOut(RecvTransferTask& task) {
    TORCH_CHECK(task.copy_ready_event_ != nullptr,
                "Expected a copy_ready_event");
    cudaError_t query_error = cudaEventQuery(task.copy_ready_event_);
    if (query_error == cudaSuccess) {
        task.copy_ready_event_ = nullptr;
        recv_pool_->release(task.local_addr_);
        task.local_addr_ = nullptr;
        task.state_ = RecvTaskState::kFinished;
        return true;
    }
    if (query_error == cudaErrorNotReady) {
        return false;
    }

    task.copy_ready_event_ = nullptr;
    TORCH_CHECK(false, "P2P recv cudaEventQuery failed: ",
                cudaGetErrorString(query_error));
    return false;
}

// Sender fetches credits.
//
// Poll the local CreditLane for the next expected sequence.  If the slot
// matches our consume cursor we accept the credit: allocate a staging
// chunk from SendPool, copy the corresponding slice of the user tensor into
// it, and advance the cursor.  If the pool is full or no credit has
// arrived we return false immediately (non-blocking).
bool P2PProxy::tryIssueSendTask(SendOpContext& op_ctx, SendPeerLane& lane) {
    // Check for timeout while waiting for the peer's CreditSlot.
    if (isTimeout(op_ctx)) {
        LOG(ERROR) << "P2P wait-for-credit timeout, peer=" << op_ctx.peer_rank_;
        op_ctx.status_->store(OpStatus::kFailed, std::memory_order_release);
        return false;
    }

    if (op_ctx.bytes_staged_ >= op_ctx.total_bytes_) {
        return false;
    }

    if (op_ctx.tasks_.size() >= kP2PControlRingSize) {
        return false;
    }

    CreditSlot* local_credit = getLocalCreditLane(op_ctx.peer_rank_);
    const uint64_t seq = lane.credit_consume_seq_;
    auto& slot = local_credit[static_cast<size_t>(seq % kP2PControlRingSize)];

    // Step 1 -- Try load the slot
    uint64_t recv_addr = 0;
    uint32_t chunk_len = 0;
    uint32_t slot_epoch = 0;
    uint32_t slot_seq = 0;
    if (!slot.tryLoad(recv_addr, chunk_len, slot_epoch, slot_seq)) {
        // Slot is either empty or torn (partial RDMA write). Retry next poll.
        return false;
    }

    // Step 2 -- Stale packet: the slot carries data from a previous epoch
    // (before a Reset).  Clear it so the fresh credit can land safely.
    const uint32_t curr_epoch =
        peer_epoch_[op_ctx.peer_rank_].load(std::memory_order_acquire);
    if (slot_epoch != curr_epoch) {
        LOG(WARNING) << "[P2PProxy][Send] tryIssueSendTask peer="
                     << op_ctx.peer_rank_ << " STALE_EPOCH seq=" << seq
                     << " slot.epoch=" << slot_epoch
                     << " curr_epoch=" << curr_epoch;
        slot.reset();
        return true;
    }

    // Step 3 -- Sequence check: make sure this is exactly the slot we expect.
    if (slot_seq != static_cast<uint32_t>(seq)) {
        return false;
    }

    void* staging_addr = send_pool_->acquire();
    if (staging_addr == nullptr) {
        return false;
    }

    const uint32_t expected_chunk_len =
        static_cast<uint32_t>(std::min<uint64_t>(
            chunk_size_, op_ctx.total_bytes_ - op_ctx.bytes_staged_));
    TORCH_CHECK(chunk_len <= expected_chunk_len,
                "P2P send got invalid chunk_len in credit slot.");

    op_ctx.tasks_.emplace_back(op_ctx.bytes_staged_, chunk_len, staging_addr,
                               recv_addr, seq, slot_epoch);
    auto& task = op_ctx.tasks_.back();

    slot.reset();

    const auto* tensor_ptr =
        static_cast<const uint8_t*>(op_ctx.tensor_.data_ptr());
    if (is_cpu_) {
        std::memcpy(staging_addr, tensor_ptr + task.tensor_offset_,
                    task.chunk_len_);
        task.state_ = SendTaskState::kWriteRemote;
    } else {
        cudaError_t copy_error = cudaMemcpyAsync(
            staging_addr, tensor_ptr + task.tensor_offset_, task.chunk_len_,
            cudaMemcpyDeviceToDevice, op_ctx.cuda_stream_);
        TORCH_CHECK(!copy_error, "P2P send cudaMemcpyAsync failed: ",
                    cudaGetErrorString(copy_error));

        const cudaEvent_t pooled_copy_ready_event =
            lane.copy_ready_events_[static_cast<size_t>(seq %
                                                        kP2PControlRingSize)];
        TORCH_CHECK(pooled_copy_ready_event != nullptr,
                    "P2P send pooled copy-ready event is not initialized.");
        task.copy_ready_event_ = pooled_copy_ready_event;
        copy_error =
            cudaEventRecord(task.copy_ready_event_, op_ctx.cuda_stream_);
        if (copy_error != cudaSuccess) {
            task.copy_ready_event_ = nullptr;
            TORCH_CHECK(false, "P2P send cudaEventRecord failed: ",
                        cudaGetErrorString(copy_error));
        }
    }

    ++lane.credit_consume_seq_;
    op_ctx.bytes_staged_ += task.chunk_len_;
    task.last_update_time_ = std::chrono::steady_clock::now();
    return true;
}

// Drive a single sender chunk through its state machine.
//
// kCopyIn -> kWriteRemote -> kAck -> kFinished -> erase.
bool P2PProxy::stepSendTask(SendOpContext& op_ctx, SendTransferTask& task) {
    switch (task.state_) {
        case SendTaskState::kCopyIn:
            return stepSendCopyIn(task);

        case SendTaskState::kWriteRemote:
            return stepSendWriteRemote(op_ctx, task);

        case SendTaskState::kAck:
            return stepSendAck(op_ctx, task);

        case SendTaskState::kFinished:
        case SendTaskState::kFailed:
            return false;
    }
    return false;
}

// Poll the GPU Copy-In event (or skip on CPU).
//
// When the event signals we transition from kCopyIn to kWriteRemote so the
// (RDMA) Write can be submitted.
bool P2PProxy::stepSendCopyIn(SendTransferTask& task) {
    if (task.copy_ready_event_ == nullptr) {
        // CPU case
        task.state_ = SendTaskState::kWriteRemote;
        task.last_update_time_ = std::chrono::steady_clock::now();
        return true;
    }

    cudaError_t query_error = cudaEventQuery(task.copy_ready_event_);
    if (query_error == cudaErrorNotReady) {
        return false;
    }

    task.copy_ready_event_ = nullptr;

    if (query_error == cudaSuccess) {
        task.state_ = SendTaskState::kWriteRemote;
        task.last_update_time_ = std::chrono::steady_clock::now();
        return true;
    }

    TORCH_CHECK(false, "P2P send cudaEventQuery failed: ",
                cudaGetErrorString(query_error));
    return false;
}

// Submit the Write from SendPool staging to remote RecvPool, then
// poll for its completion.
bool P2PProxy::stepSendWriteRemote(SendOpContext& op_ctx,
                                   SendTransferTask& task) {
    bool did_work = false;
    if (!task.transfer_batch_id_.has_value()) {
        const BatchID batch_id = engine_->allocateBatchID(1);
        engine_->submitTransfer(
            batch_id, {TransferRequest{
                          .opcode = TransferRequest::WRITE,
                          .source = task.staging_addr_,
                          .target_id = meta_->segmentIDs[op_ctx.peer_rank_],
                          .target_offset = task.remote_addr_,
                          .length = task.chunk_len_,
                      }});
        task.transfer_batch_id_ = batch_id;
        task.last_update_time_ = std::chrono::steady_clock::now();
        did_work = true;
    }

    TransferStatus transfer_status;
    engine_->getTransferStatus(task.transfer_batch_id_.value(), 0,
                               transfer_status);
    if (transfer_status.s == TransferStatusEnum::COMPLETED) {
        engine_->freeBatchID(task.transfer_batch_id_.value());
        task.transfer_batch_id_.reset();
        task.state_ = SendTaskState::kAck;
        task.last_update_time_ = std::chrono::steady_clock::now();
        did_work = true;
    } else if (transfer_status.s == TransferStatusEnum::FAILED ||
               isTimeout(task)) {
        LOG(ERROR) << "P2P send transfer failed/timeout, peer="
                   << op_ctx.peer_rank_ << ", seq=" << task.sequence_;
        engine_->freeBatchID(task.transfer_batch_id_.value());
        task.transfer_batch_id_.reset();
        task.state_ = SendTaskState::kFailed;
        did_work = true;
    }

    return did_work;
}

// Write the AckSlot back to the receiver.
//
// This tells the receiver that the data is present in its RecvPool and it
// may begin the Copy-Out.  When the AckSlot write finishes we free
// the staging chunk and transition to kFinished.
bool P2PProxy::stepSendAck(SendOpContext& op_ctx, SendTransferTask& task) {
    bool did_work = false;
    if (!task.ack_batch_id_.has_value()) {
        auto* ack_staging_buf =
            getLocalAckStagingBuf(op_ctx.peer_rank_, task.sequence_);
        ack_staging_buf->publish(task.epoch_, task.sequence_, task.chunk_len_);

        const BatchID batch_id = engine_->allocateBatchID(1);
        engine_->submitTransfer(
            batch_id, {TransferRequest{
                          .opcode = TransferRequest::WRITE,
                          .source = static_cast<void*>(ack_staging_buf),
                          .target_id = meta_->segmentIDs[op_ctx.peer_rank_],
                          .target_offset = getRemoteAckSlot(op_ctx.peer_rank_,
                                                            task.sequence_),
                          .length = sizeof(AckSlot),
                      }});
        task.ack_batch_id_ = batch_id;
        task.last_update_time_ = std::chrono::steady_clock::now();
        did_work = true;
    }

    TransferStatus ack_status;
    engine_->getTransferStatus(task.ack_batch_id_.value(), 0, ack_status);
    if (ack_status.s == TransferStatusEnum::COMPLETED) {
        engine_->freeBatchID(task.ack_batch_id_.value());
        task.ack_batch_id_.reset();
        send_pool_->release(task.staging_addr_);
        task.staging_addr_ = nullptr;
        task.state_ = SendTaskState::kFinished;
        did_work = true;
    } else if (ack_status.s == TransferStatusEnum::FAILED || isTimeout(task)) {
        LOG(ERROR) << "P2P ack transfer failed/timeout, peer="
                   << op_ctx.peer_rank_ << ", seq=" << task.sequence_;
        engine_->freeBatchID(task.ack_batch_id_.value());
        task.ack_batch_id_.reset();
        task.state_ = SendTaskState::kFailed;
        did_work = true;
    }

    return did_work;
}

bool P2PProxy::isSendOpCompleted(const SendOpContext& op_ctx) const {
    return op_ctx.bytes_staged_ == op_ctx.total_bytes_ && op_ctx.tasks_.empty();
}

bool P2PProxy::isRecvOpCompleted(const RecvOpContext& op_ctx) const {
    return op_ctx.bytes_credited_ == op_ctx.total_bytes_ &&
           op_ctx.tasks_.empty();
}

// Sender state machine
//
// Pipeline per peer:
//   1. Drain the shared send_queue_ into the peer's pending_send_ops_.
//   2. Promote the first pending op to active_send_op_.
//   3. While we have credits (CreditSlots) and staging buffers,
//      pull more tensor slices into SendPool (TryIssueSendTask).
//   4. Advance every active chunk through its state machine
//      (Copy-In -> Write Remote -> Ack write).
//   5. Erase fully-acknowledged chunks.  When the op is empty and all
//      bytes have been staged, mark it complete.
bool P2PProxy::stepSend() {
    bool did_work = false;

    // Handle reset first
    for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
        if (reset_send_req_[peer_rank].exchange(false,
                                                std::memory_order_acquire)) {
            performSendReset(peer_rank);
            did_work = true;
        }
    }

    // Drain send_queue_
    {
        std::lock_guard<std::mutex> lock(send_queue_mutex_);
        while (!send_queue_.empty()) {
            SendOpContext op_ctx = std::move(send_queue_.front());
            send_queue_.pop();
            send_peer_lanes_[op_ctx.peer_rank_].pending_send_ops_.push_back(
                std::move(op_ctx));
            did_work = true;
        }
    }

    // Promote active op
    for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
        auto& lane = send_peer_lanes_[peer_rank];
        if (lane.active_send_op_.has_value() ||
            lane.pending_send_ops_.empty()) {
            continue;
        }
        SendOpContext op_ctx = std::move(lane.pending_send_ops_.front());
        lane.pending_send_ops_.pop_front();
        if (op_ctx.total_bytes_ == 0) {
            op_ctx.status_->store(OpStatus::kSuccess,
                                  std::memory_order_release);
            active_send_tasks_.fetch_sub(1, std::memory_order_release);
            did_work = true;
            continue;
        }
        lane.active_send_op_ = std::move(op_ctx);
        did_work = true;
    }

    // Advance state machine
    for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
        auto& lane = send_peer_lanes_[peer_rank];
        if (!lane.active_send_op_.has_value()) {
            continue;
        }
        auto& op_ctx = lane.active_send_op_.value();

        // Pull as many credits as we have free chunks
        while (tryIssueSendTask(op_ctx, lane)) {
            did_work = true;
        }

        // Advance every chunk through the sender state machine.
        // send op may fail due to credit-wait timeout in tryIssueSendTask.
        auto op_status = op_ctx.status_->load(std::memory_order_acquire);
        bool op_failed = op_status == OpStatus::kFailed;
        if (!op_failed) {
            for (auto it = op_ctx.tasks_.begin(); it != op_ctx.tasks_.end();) {
                if (stepSendTask(op_ctx, *it)) {
                    did_work = true;
                }
                if (it->state_ == SendTaskState::kFinished) {
                    it = op_ctx.tasks_.erase(it);
                    did_work = true;
                } else if (it->state_ == SendTaskState::kFailed) {
                    op_failed = true;
                    break;
                } else {
                    ++it;
                }
            }
        }

        if (op_failed) {
            reportBrokenPeer(peer_rank);
            did_work = true;
            continue;
        }

        if (isSendOpCompleted(op_ctx)) {
            op_ctx.status_->store(OpStatus::kSuccess,
                                  std::memory_order_release);
            lane.active_send_op_.reset();
            active_send_tasks_.fetch_sub(1, std::memory_order_release);
            did_work = true;
        }
    }

    return did_work;
}

// Poll the local AckLane for the head task and initiate Copy-Out.
//
// Returns true if an ack was processed (including stale-slot clearing).
bool P2PProxy::pollRecvAckSlot(RecvOpContext& op_ctx, RecvPeerLane& lane,
                               RecvTransferTask& head_task) {
    // Check for timeout while waiting for the peer's AckSlot.
    if (isTimeout(head_task)) {
        LOG(ERROR) << "P2P wait-for-ack timeout, peer=" << op_ctx.peer_rank_
                   << " seq=" << head_task.sequence_;
        head_task.state_ = RecvTaskState::kFailed;
        return true;
    }

    auto* ack_lane = getLocalAckLane(op_ctx.peer_rank_);
    auto& slot = ack_lane[head_task.sequence_ % kP2PControlRingSize];

    // Step 1 -- Try load the slot
    uint32_t ack_len = 0;
    uint32_t slot_epoch = 0;
    uint32_t slot_seq = 0;
    if (!slot.tryLoad(ack_len, slot_epoch, slot_seq)) {
        // Slot is either empty or torn. Retry next poll.
        return false;
    }

    const uint32_t curr_epoch =
        peer_epoch_[op_ctx.peer_rank_].load(std::memory_order_acquire);

    // Step 2 -- Stale packet: data from a previous epoch (before Reset).
    // Clear the slot so the fresh ack can land safely.
    if (slot_epoch != curr_epoch) {
        LOG(WARNING) << "[P2PProxy][Recv] pollRecvAckSlot peer="
                     << op_ctx.peer_rank_
                     << " front-seq=" << head_task.sequence_
                     << " EPOCH_MISMATCH slot.epoch=" << slot_epoch
                     << " curr_epoch=" << curr_epoch;
        slot.reset();
        return true;
    }

    // Step 3 -- Sequence check: make sure this is the exact ack
    // we are waiting for.
    if (slot_seq != head_task.sequence_) {
        return false;
    }

    void* src_ptr = head_task.local_addr_;
    auto* tensor_ptr = static_cast<uint8_t*>(op_ctx.tensor_.data_ptr());
    void* dst_ptr = tensor_ptr + head_task.tensor_offset_;

    if (is_cpu_) {
        std::memcpy(dst_ptr, src_ptr, head_task.chunk_len_);
        recv_pool_->release(head_task.local_addr_);
        head_task.local_addr_ = nullptr;
        head_task.state_ = RecvTaskState::kFinished;
    } else {
        cudaError_t copy_error =
            cudaMemcpyAsync(dst_ptr, src_ptr, head_task.chunk_len_,
                            cudaMemcpyDeviceToDevice, op_ctx.cuda_stream_);
        TORCH_CHECK(!copy_error, "P2P recv cudaMemcpyAsync failed: ",
                    cudaGetErrorString(copy_error));
        const cudaEvent_t pooled_copy_ready_event =
            lane.copy_ready_events_[static_cast<size_t>(head_task.sequence_ %
                                                        kP2PControlRingSize)];
        TORCH_CHECK(pooled_copy_ready_event != nullptr,
                    "P2P recv pooled copy-ready event is not initialized.");
        head_task.copy_ready_event_ = pooled_copy_ready_event;
        copy_error =
            cudaEventRecord(head_task.copy_ready_event_, op_ctx.cuda_stream_);
        if (copy_error != cudaSuccess) {
            head_task.copy_ready_event_ = nullptr;
            TORCH_CHECK(false, "P2P recv cudaEventRecord failed: ",
                        cudaGetErrorString(copy_error));
        }
        head_task.state_ = RecvTaskState::kCopyOut;
    }

    head_task.last_update_time_ = std::chrono::steady_clock::now();
    slot.reset();
    ++lane.ack_consume_seq_;
    return true;
}

// Receiver state machine
//
// Pipeline per peer:
//   1. Drain the shared recv_queue_ into the peer's pending_recv_ops_.
//   2. Promote the first pending op to active_recv_op_.
//   3. While we have free RecvPool chunks, issue CreditSlots to the sender
//      (TryIssueRecvTask).
//   4. Poll the local AckLane in order.  When an AckSlot
//      arrives, initiate Copy-Out from RecvPool to the user tensor.
//   5. Advance every active chunk through its state machine
//      (IssueCredit -> Copy-Out -> erase).
//   6. When all chunks are copied out, mark the op complete.
bool P2PProxy::stepRecv() {
    bool did_work = false;

    // Handle reset first
    for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
        if (reset_recv_req_[peer_rank].exchange(false,
                                                std::memory_order_acquire)) {
            performRecvReset(peer_rank);
            did_work = true;
        }
    }

    // Drain recv_queue_
    {
        std::lock_guard<std::mutex> lock(recv_queue_mutex_);
        while (!recv_queue_.empty()) {
            RecvOp op = std::move(recv_queue_.front());
            recv_queue_.pop();
            recv_peer_lanes_[op.peer_rank_].pending_recv_ops_.push_back(
                std::move(op));
            did_work = true;
        }
    }

    // Promote active op
    for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
        auto& lane = recv_peer_lanes_[peer_rank];
        if (lane.active_recv_op_.has_value() ||
            lane.pending_recv_ops_.empty()) {
            continue;
        }
        RecvOp recv_op = std::move(lane.pending_recv_ops_.front());
        lane.pending_recv_ops_.pop_front();
        RecvOpContext op_ctx(std::move(recv_op));
        lane.active_recv_op_ = std::move(op_ctx);
        did_work = true;
    }

    // Advance state machine
    for (int peer_rank = 0; peer_rank < size_; ++peer_rank) {
        auto& lane = recv_peer_lanes_[peer_rank];
        if (!lane.active_recv_op_.has_value()) {
            continue;
        }

        auto& op_ctx = lane.active_recv_op_.value();

        // Offer as many RecvPool chunks as we have free buffers and tensor
        // bytes remaining.
        while (tryIssueRecvTask(op_ctx, lane)) {
            did_work = true;
        }

        // In-order ack polling: we only look at the head of the queue
        // because the sender acknowledges chunks in the same order it
        // consumes credits.
        if (!op_ctx.tasks_.empty()) {
            auto& head = op_ctx.tasks_.front();
            if (head.state_ == RecvTaskState::kWaitAck) {
                if (pollRecvAckSlot(op_ctx, lane, head)) {
                    did_work = true;
                }
            }
        }

        // Advance every chunk through the receiver state machine.
        bool op_failed = false;
        for (auto it = op_ctx.tasks_.begin(); it != op_ctx.tasks_.end();) {
            did_work |= stepRecvTask(*it);
            if (it->state_ == RecvTaskState::kFinished) {
                it = op_ctx.tasks_.erase(it);
                did_work = true;
            } else if (it->state_ == RecvTaskState::kFailed) {
                op_failed = true;
                break;
            } else {
                ++it;
            }
        }

        if (op_failed) {
            reportBrokenPeer(peer_rank);
            did_work = true;
            continue;
        }

        if (isRecvOpCompleted(op_ctx)) {
            if (!op_ctx.original_tensor_.is_contiguous()) {
                (void)op_ctx.original_tensor_.copy_(op_ctx.tensor_);
                if (!is_cpu_) {
                    const cudaError_t sync_error = cudaDeviceSynchronize();
                    TORCH_CHECK(sync_error == cudaSuccess,
                                "P2P recv final copy cudaDeviceSynchronize "
                                "failed: ",
                                cudaGetErrorString(sync_error));
                }
            }
            op_ctx.status_->store(OpStatus::kSuccess,
                                  std::memory_order_release);
            lane.active_recv_op_.reset();
            active_recv_tasks_.fetch_sub(1, std::memory_order_release);
            did_work = true;
        }
    }

    return did_work;
}

void P2PProxy::attachToWorker(P2PDeviceWorker* worker, P2PChunkPool* send,
                              P2PChunkPool* recv, size_t chunk_size) {
    device_worker_ = worker;
    send_pool_ = send;
    recv_pool_ = recv;
    chunk_size_ = chunk_size;
}

bool P2PProxy::hasActiveSendWork() const {
    for (int i = 0; i < size_; ++i) {
        if (reset_send_req_[i].load(std::memory_order_acquire)) return true;
    }
    return active_send_tasks_.load(std::memory_order_acquire) > 0;
}

bool P2PProxy::hasActiveRecvWork() const {
    for (int i = 0; i < size_; ++i) {
        if (reset_recv_req_[i].load(std::memory_order_acquire)) return true;
    }
    return active_recv_tasks_.load(std::memory_order_acquire) > 0;
}

bool P2PProxy::drainTasks() const {
    BackoffWaiter waiter;
    return waiter.wait_for(
        std::chrono::milliseconds(kDrainTasksTimeoutMs),
        [this] { return !hasActiveSendWork() && !hasActiveRecvWork(); });
}

void P2PDeviceWorker::start() {
    bool expected_send = false;
    if (send_worker_running_.compare_exchange_strong(expected_send, true)) {
        send_worker_thread_ =
            std::thread(&P2PDeviceWorker::sendWorkerMainloop, this);
    }

    bool expected_recv = false;
    if (recv_worker_running_.compare_exchange_strong(expected_recv, true)) {
        recv_worker_thread_ =
            std::thread(&P2PDeviceWorker::recvWorkerMainloop, this);
    }
}

void P2PDeviceWorker::stop() {
    bool expected_send = true;
    std::unique_lock<std::mutex> s_lock(send_wakeup_mutex_);
    if (send_worker_running_.compare_exchange_strong(expected_send, false)) {
        s_lock.unlock();
        send_wakeup_cv_.notify_all();
        if (send_worker_thread_.joinable()) {
            send_worker_thread_.join();
        }
    }

    bool expected_recv = true;
    std::unique_lock<std::mutex> r_lock(recv_wakeup_mutex_);
    if (recv_worker_running_.compare_exchange_strong(expected_recv, false)) {
        r_lock.unlock();
        recv_wakeup_cv_.notify_all();
        if (recv_worker_thread_.joinable()) {
            recv_worker_thread_.join();
        }
    }
}

P2PDeviceWorker::P2PDeviceWorker(TransferEngine* engine,
                                 const std::string& location, bool is_cpu,
                                 int cuda_device_index)
    : is_cpu_(is_cpu), cuda_device_index_(cuda_device_index) {
    initPools(engine, location);
    start();
}

P2PDeviceWorker::~P2PDeviceWorker() {
    stop();
    releasePools();
}

void P2PDeviceWorker::registerProxy(const std::shared_ptr<P2PProxy>& proxy) {
    proxy->attachToWorker(this, &send_pool_, &recv_pool_, chunk_size_);
    {
        std::lock_guard<std::mutex> lock(proxies_mutex_);
        proxies_.emplace_back(proxy);

        std::lock_guard<std::mutex> s_lock(send_wakeup_mutex_);
        std::lock_guard<std::mutex> r_lock(recv_wakeup_mutex_);
        proxies_version_.fetch_add(1, std::memory_order_release);
    }

    send_wakeup_cv_.notify_one();
    recv_wakeup_cv_.notify_one();
}

void P2PDeviceWorker::removeProxy(const std::shared_ptr<P2PProxy>& proxy) {
    {
        std::lock_guard<std::mutex> lock(proxies_mutex_);
        proxies_.erase(std::remove(proxies_.begin(), proxies_.end(), proxy),
                       proxies_.end());

        std::lock_guard<std::mutex> s_lock(send_wakeup_mutex_);
        std::lock_guard<std::mutex> r_lock(recv_wakeup_mutex_);
        proxies_version_.fetch_add(1, std::memory_order_release);
    }
    send_wakeup_cv_.notify_one();
    recv_wakeup_cv_.notify_one();
}

void P2PDeviceWorker::initPools(TransferEngine* engine,
                                const std::string& location) {
    engine_ = engine;

    // Parse from environment variables
    pool_bytes_ = getEnv_size_t("MOONCAKE_P2P_POOL_SIZE", kDefaultPoolSize);
    chunk_size_ = getEnv_size_t("MOONCAKE_P2P_CHUNK_SIZE", kDefaultChunkSize);

    // chunk_len is uint32_t fields in control slots
    TORCH_CHECK(
        chunk_size_ > 0 && chunk_size_ <= std::numeric_limits<uint32_t>::max(),
        "Invalid MOONCAKE_P2P_CHUNK_SIZE: must be > 0 and <= 4GB");

    TORCH_CHECK(
        pool_bytes_ > 0 && pool_bytes_ % chunk_size_ == 0,
        "Invalid pool size and chunk size (must hold 'pool_bytes_ > 0 && "
        "pool_bytes_ % chunk_size_ == 0')");

    num_chunks_ = static_cast<uint32_t>(pool_bytes_ / chunk_size_);
    TORCH_CHECK(num_chunks_ > 0, "P2PDeviceWorker: num_chunks_ must be > 0");

    if (is_cpu_) {
        send_pool_base_ = std::malloc(pool_bytes_);
        TORCH_CHECK(send_pool_base_ != nullptr,
                    "Failed to allocate CPU P2P send pool");
        int rc =
            engine->registerLocalMemory(send_pool_base_, pool_bytes_, location);
        TORCH_CHECK(rc == 0, "Failed to register CPU P2P send pool");

        recv_pool_base_ = std::malloc(pool_bytes_);
        TORCH_CHECK(recv_pool_base_ != nullptr,
                    "Failed to allocate CPU P2P recv pool");
        rc =
            engine->registerLocalMemory(recv_pool_base_, pool_bytes_, location);
        TORCH_CHECK(rc == 0, "Failed to register CPU P2P recv pool");
    } else {
        setCudaDeviceIfNeeded(is_cpu_, cuda_device_index_,
                              "P2PDeviceWorker initPools cudaSetDevice failed");
        cudaError_t err = cudaMalloc(&send_pool_base_, pool_bytes_);
        TORCH_CHECK(
            err == cudaSuccess,
            "Failed to allocate CUDA P2P send pool: ", cudaGetErrorString(err));
        int rc =
            engine->registerLocalMemory(send_pool_base_, pool_bytes_, location);
        TORCH_CHECK(rc == 0, "Failed to register CUDA P2P send pool");

        err = cudaMalloc(&recv_pool_base_, pool_bytes_);
        TORCH_CHECK(
            err == cudaSuccess,
            "Failed to allocate CUDA P2P recv pool: ", cudaGetErrorString(err));
        rc =
            engine->registerLocalMemory(recv_pool_base_, pool_bytes_, location);
        TORCH_CHECK(rc == 0, "Failed to register CUDA P2P recv pool");
    }

    send_pool_.init(send_pool_base_, chunk_size_, num_chunks_);
    recv_pool_.init(recv_pool_base_, chunk_size_, num_chunks_);
}

void P2PDeviceWorker::releasePools() {
    setCudaDeviceIfNeeded(is_cpu_, cuda_device_index_,
                          "P2PDeviceWorker releasePools cudaSetDevice failed");

    if (send_pool_base_ != nullptr) {
        if (engine_) engine_->unregisterLocalMemory(send_pool_base_);
        if (is_cpu_) {
            std::free(send_pool_base_);
        } else {
            cudaFree(send_pool_base_);
        }
        send_pool_base_ = nullptr;
    }

    if (recv_pool_base_ != nullptr) {
        if (engine_) engine_->unregisterLocalMemory(recv_pool_base_);
        if (is_cpu_) {
            std::free(recv_pool_base_);
        } else {
            cudaFree(recv_pool_base_);
        }
        recv_pool_base_ = nullptr;
    }
}

template <typename HasWorkFn, typename StepWorkFn>
void workerThreadLoop(bool is_cpu, int cuda_device_index,
                      std::atomic<bool>& worker_running,
                      std::atomic<uint64_t>& proxies_version,
                      std::mutex& proxies_mutex,
                      std::vector<std::shared_ptr<P2PProxy>>& proxies,
                      std::mutex& wakeup_mutex,
                      std::condition_variable& wakeup_cv, HasWorkFn has_work,
                      StepWorkFn step_work) {
    setCudaDeviceIfNeeded(is_cpu, cuda_device_index,
                          "P2PDeviceWorker::WorkerThread cudaSetDevice failed");

    // A thread-local cache for proxies to avoid locking too frequently.
    // This is efficient because proxy registration/removal is rare after
    // backend initialization.
    std::vector<std::shared_ptr<P2PProxy>> local_proxies;
    uint64_t local_version = std::numeric_limits<uint64_t>::max();

    // Some proxies might be removed (caused by backend shutdown) before
    // all its transfers completed. We must keep them explicitly to avoid its
    // resources released, and step their transfers till completion.
    std::vector<std::shared_ptr<P2PProxy>> zombie_proxies;

    while (true) {
        // Refresh the local cache if proxy membership changed.
        const auto current_version =
            proxies_version.load(std::memory_order_acquire);
        if (local_version != current_version) {
            std::vector<std::shared_ptr<P2PProxy>> new_proxies;
            {
                std::lock_guard<std::mutex> lock(proxies_mutex);
                new_proxies = proxies;
                // Reload version under `proxies_mutex` to ensure version
                // matches `new_proxies`.
                local_version = proxies_version.load(std::memory_order_acquire);
            }

            // Find zombie proxies.
            for (const auto& old_p : local_proxies) {
                auto it =
                    std::find(new_proxies.begin(), new_proxies.end(), old_p);
                if (it == new_proxies.end() && has_work(*old_p)) {
                    zombie_proxies.push_back(old_p);
                }
            }

            local_proxies = std::move(new_proxies);
        }

        bool did_work = false;
        bool has_active_work = false;

        for (const auto& proxy : local_proxies) {
            if (has_work(*proxy)) {
                did_work |= step_work(*proxy);
                has_active_work = true;
            }
        }

        for (auto it = zombie_proxies.begin(); it != zombie_proxies.end();) {
            auto& proxy = *it;
            if (has_work(*proxy)) {
                did_work |= step_work(*proxy);
                has_active_work = true;
                ++it;
            } else {
                // Now all transfers in this proxy is completed, remove it.
                it = zombie_proxies.erase(it);
            }
        }

        bool running = worker_running.load(std::memory_order_relaxed);
        if (!running && !has_active_work) {
            break;
        }

        // If we did work this iteration, just continue.
        if (did_work) continue;
        // Otherwise, if the queue has uncompleted tasks, pause.
        if (has_active_work) {
            PAUSE();
        }
        // The queue has no active work. Block on the condition variable.
        else {
            std::unique_lock<std::mutex> lock(wakeup_mutex);
            wakeup_cv.wait(lock, [&]() {
                if (!worker_running.load(std::memory_order_relaxed))
                    return true;

                const auto current_version =
                    proxies_version.load(std::memory_order_acquire);
                if (local_version != current_version) return true;

                for (const auto& proxy : local_proxies) {
                    if (has_work(*proxy)) return true;
                }
                for (const auto& proxy : zombie_proxies) {
                    if (has_work(*proxy)) return true;
                }
                return false;
            });
        }
    }
}

void P2PDeviceWorker::sendWorkerMainloop() {
    workerThreadLoop(
        is_cpu_, cuda_device_index_, send_worker_running_, proxies_version_,
        proxies_mutex_, proxies_, send_wakeup_mutex_, send_wakeup_cv_,
        [](P2PProxy& p) { return p.hasActiveSendWork(); },
        [](P2PProxy& p) { return p.stepSend(); });
}

void P2PDeviceWorker::recvWorkerMainloop() {
    workerThreadLoop(
        is_cpu_, cuda_device_index_, recv_worker_running_, proxies_version_,
        proxies_mutex_, proxies_, recv_wakeup_mutex_, recv_wakeup_cv_,
        [](P2PProxy& p) { return p.hasActiveRecvWork(); },
        [](P2PProxy& p) { return p.stepRecv(); });
}

// Standard practice is to update states shared with condition_variables under
// the lock, but locking right before the notify should be enough to prevent
// lost wakeups.
// (active_send/recv_tasks_ are modified in EnqueueSend/Recv, while the lock is
//  acquired in WakeUpSend/Recv)
//
// Ref: https://stackoverflow.com/a/21439617
void P2PDeviceWorker::wakeUpSend() {
    std::lock_guard<std::mutex> lock(send_wakeup_mutex_);
    send_wakeup_cv_.notify_one();
}
void P2PDeviceWorker::wakeUpRecv() {
    std::lock_guard<std::mutex> lock(recv_wakeup_mutex_);
    recv_wakeup_cv_.notify_one();
}

std::shared_ptr<P2PDeviceWorker> P2PDeviceWorkerManager::getCPUWorker(
    TransferEngine* engine) {
    std::lock_guard<std::mutex> lock(manager_mutex_);

    auto it = workers_.find(CPUWorkerID);
    if (it != workers_.end()) {
        if (auto ptr = it->second.lock()) return ptr;
    }

    auto worker = std::make_shared<P2PDeviceWorker>(
        engine, kWildcardLocation, /* is_cpu */ true, CPUWorkerID);
    workers_[CPUWorkerID] = worker;
    return worker;
}

std::shared_ptr<P2PDeviceWorker> P2PDeviceWorkerManager::getCUDAWorker(
    int cuda_device_index, TransferEngine* engine) {
    std::lock_guard<std::mutex> lock(manager_mutex_);

    auto it = workers_.find(cuda_device_index);
    if (it != workers_.end()) {
        if (auto ptr = it->second.lock()) return ptr;
    }

    auto worker = std::make_shared<P2PDeviceWorker>(
        engine, GPU_PREFIX + std::to_string(cuda_device_index),
        /* is_cpu */ false, cuda_device_index);
    workers_[cuda_device_index] = worker;
    return worker;
}

}  // namespace mooncake
