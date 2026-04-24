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
#include "memory_location.h"
#include "pg_utils.h"

namespace mooncake {

namespace {

constexpr size_t kP2PPoolBytes =
    static_cast<size_t>(kP2PPoolNumChunks) * kP2PChunkSize;

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

P2PProxy::P2PProxy(TransferEngine* engine, const Options& options)
    : engine_(engine),
      is_cpu_(options.is_cpu),
      rank_(options.rank),
      size_(options.size),
      cuda_device_index_(options.cuda_device_index),
      location_(options.location),
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

// Allocate fixed-size SendPool, RecvPool and control regions.
void P2PProxy::allocateResources() {
    TORCH_CHECK(engine_, "P2PProxy engine is null.");
    if (resources_.send_pool_base_ != nullptr ||
        resources_.recv_pool_base_ != nullptr ||
        resources_.publish_region_ != nullptr ||
        resources_.completion_region_ != nullptr) {
        return;
    }

    TORCH_CHECK(size_ > 0, "P2PProxy invalid group size: ", size_);
    TORCH_CHECK(static_cast<size_t>(size_) <= kMaxNumRanks,
                "P2PProxy group size exceeds kMaxNumRanks: ", size_);

    if (is_cpu_) {
        resources_.send_pool_base_ = std::malloc(kP2PPoolBytes);
        TORCH_CHECK(resources_.send_pool_base_ != nullptr,
                    "Failed to allocate CPU P2P send pool");
        int rc = engine_->registerLocalMemory(resources_.send_pool_base_,
                                              kP2PPoolBytes, location_);
        TORCH_CHECK(rc == 0, "Failed to register CPU P2P send pool");

        resources_.recv_pool_base_ = std::malloc(kP2PPoolBytes);
        TORCH_CHECK(resources_.recv_pool_base_ != nullptr,
                    "Failed to allocate CPU P2P recv pool");
        rc = engine_->registerLocalMemory(resources_.recv_pool_base_,
                                          kP2PPoolBytes, location_);
        TORCH_CHECK(rc == 0, "Failed to register CPU P2P recv pool");
    } else {
        setCudaDeviceIfNeeded(
            is_cpu_, cuda_device_index_,
            "P2PProxy AllocateResources cudaSetDevice failed");
        cudaError_t err =
            cudaMalloc(&resources_.send_pool_base_, kP2PPoolBytes);
        TORCH_CHECK(
            err == cudaSuccess,
            "Failed to allocate CUDA P2P send pool: ", cudaGetErrorString(err));
        int rc = engine_->registerLocalMemory(resources_.send_pool_base_,
                                              kP2PPoolBytes, location_);
        TORCH_CHECK(rc == 0, "Failed to register CUDA P2P send pool");

        err = cudaMalloc(&resources_.recv_pool_base_, kP2PPoolBytes);
        TORCH_CHECK(
            err == cudaSuccess,
            "Failed to allocate CUDA P2P recv pool: ", cudaGetErrorString(err));
        rc = engine_->registerLocalMemory(resources_.recv_pool_base_,
                                          kP2PPoolBytes, location_);
        TORCH_CHECK(rc == 0, "Failed to register CUDA P2P recv pool");
    }

    send_pool_.init(resources_.send_pool_base_, kP2PChunkSize,
                    kP2PPoolNumChunks);
    recv_pool_.init(resources_.recv_pool_base_, kP2PChunkSize,
                    kP2PPoolNumChunks);

    const size_t ctrl_slots =
        kMaxNumRanks * static_cast<size_t>(kP2PControlRingSize);
    resources_.publish_region_ = new PublishSlot[ctrl_slots]{};
    resources_.completion_region_ = new CompletionSlot[ctrl_slots]{};

    for (size_t i = 0; i < ctrl_slots; ++i) {
        resources_.publish_region_[i].reset();
        resources_.completion_region_[i].reset();
    }

    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        peer_generation_[i].store(1, std::memory_order_release);
    }

    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        send_peer_lanes_[i].pending_send_ops_.clear();
        send_peer_lanes_[i].active_send_op_.reset();
        send_peer_lanes_[i].publish_consume_seq_ = 0;
        send_peer_lanes_[i].copy_ready_events_.fill(nullptr);

        recv_peer_lanes_[i].pending_recv_ops_.clear();
        recv_peer_lanes_[i].active_recv_op_.reset();
        recv_peer_lanes_[i].publish_issue_seq_ = 0;
        recv_peer_lanes_[i].completion_consume_seq_ = 0;
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

    int rc = engine_->registerLocalMemory(resources_.publish_region_,
                                          ctrl_slots * sizeof(PublishSlot),
                                          kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P publish region");

    rc = engine_->registerLocalMemory(resources_.completion_region_,
                                      ctrl_slots * sizeof(CompletionSlot),
                                      kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P completion region");

    // Staging buffers for control messages.  RDMA requires every
    // transfer source to live in a registered MR.
    resources_.publish_staging_buf_ = new PublishSlot[ctrl_slots]{};
    rc = engine_->registerLocalMemory(resources_.publish_staging_buf_,
                                      ctrl_slots * sizeof(PublishSlot),
                                      kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P publish staging region");

    resources_.completion_staging_buf_ = new CompletionSlot[ctrl_slots]{};
    rc = engine_->registerLocalMemory(resources_.completion_staging_buf_,
                                      ctrl_slots * sizeof(CompletionSlot),
                                      kWildcardLocation);
    TORCH_CHECK(rc == 0, "Failed to register P2P completion staging region");
}

void P2PProxy::resetPeerState(int peer_rank) {
    TORCH_CHECK(peer_rank >= 0 && peer_rank < size_,
                "ResetPeerState: peer_rank out of range: ", peer_rank,
                " size: ", size_);

    // Generation update:
    //   We bump generation_ so that any slots still in flight from the
    //   old session (written by the sender/receiver before it learned about
    //   the Reset) are recognized as stale and skipped.
    peer_generation_[peer_rank].fetch_add(1, std::memory_order_acq_rel);

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
    auto& lane = send_peer_lanes_[peer_rank];

    // Mark all pending ops as failed and clear them.
    for (auto& pending : lane.pending_send_ops_) {
        pending.status_->store(OpStatus::kFailed, std::memory_order_release);
        active_send_tasks_.fetch_sub(1, std::memory_order_release);
    }
    lane.pending_send_ops_.clear();

    if (lane.active_send_op_.has_value()) {
        auto& op_ctx = lane.active_send_op_.value();
        // Free all remaining batch IDs and staging buffers.
        for (auto& task : op_ctx.tasks_) {
            if (task.staging_addr_ != nullptr) {
                send_pool_.release(task.staging_addr_);
                task.staging_addr_ = nullptr;
            }
            if (task.transfer_batch_id_.has_value()) {
                engine_->freeBatchID(task.transfer_batch_id_.value());
            }
            if (task.completion_batch_id_.has_value()) {
                engine_->freeBatchID(task.completion_batch_id_.value());
            }
        }
        op_ctx.tasks_.clear();
        op_ctx.status_->store(OpStatus::kFailed, std::memory_order_release);
        active_send_tasks_.fetch_sub(1, std::memory_order_release);
    }

    lane.active_send_op_.reset();
    lane.publish_consume_seq_ = 0;

    auto* publish_lane = getLocalPublishLane(peer_rank);
    auto* completion_lane = getLocalCompletionLane(peer_rank);
    for (uint32_t i = 0; i < kP2PControlRingSize; ++i) {
        publish_lane[i].reset();
        completion_lane[i].reset();
    }
}

// Reset receiver state for peer_rank.
void P2PProxy::performRecvReset(int peer_rank) {
    auto& lane = recv_peer_lanes_[peer_rank];

    // Mark all pending ops as failed and clear them.
    for (auto& pending : lane.pending_recv_ops_) {
        pending.status_->store(OpStatus::kFailed, std::memory_order_release);
        active_recv_tasks_.fetch_sub(1, std::memory_order_release);
    }
    lane.pending_recv_ops_.clear();

    if (lane.active_recv_op_.has_value()) {
        auto& op_ctx = lane.active_recv_op_.value();
        // Free all remaining batch IDs and recv pool chunks.
        for (auto& task : op_ctx.tasks_) {
            if (task.local_addr_ != nullptr) {
                recv_pool_.release(task.local_addr_);
            }
            if (task.publish_batch_id_.has_value()) {
                engine_->freeBatchID(task.publish_batch_id_.value());
            }
        }
        op_ctx.tasks_.clear();
        op_ctx.status_->store(OpStatus::kFailed, std::memory_order_release);
        active_recv_tasks_.fetch_sub(1, std::memory_order_release);
    }

    lane.active_recv_op_.reset();
    lane.publish_issue_seq_ = 0;
    lane.completion_consume_seq_ = 0;

    auto* publish_lane = getLocalPublishLane(peer_rank);
    auto* completion_lane = getLocalCompletionLane(peer_rank);
    for (uint32_t i = 0; i < kP2PControlRingSize; ++i) {
        publish_lane[i].reset();
        completion_lane[i].reset();
    }
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

void P2PProxy::releaseResources() {
    TORCH_CHECK(!resource_abandoned_,
                "Should not release abandoned resources.");

    setCudaDeviceIfNeeded(is_cpu_, cuda_device_index_,
                          "P2PProxy ReleaseResources cudaSetDevice failed");

    for (size_t i = 0; i < kMaxNumRanks; ++i) {
        auto& send_lane = send_peer_lanes_[i];
        for (auto& copy_ready_event : send_lane.copy_ready_events_) {
            if (copy_ready_event == nullptr) {
                continue;
            }
            const cudaError_t destroy_error =
                cudaEventDestroy(copy_ready_event);
            TORCH_CHECK(destroy_error == cudaSuccess,
                        "Failed to destroy pooled send copy-ready event: ",
                        cudaGetErrorString(destroy_error));
            copy_ready_event = nullptr;
        }
        send_lane.pending_send_ops_.clear();

        if (send_lane.active_send_op_.has_value()) {
            auto& op_ctx = send_lane.active_send_op_.value();
            for (auto& task : op_ctx.tasks_) {
                if (task.staging_addr_ != nullptr) {
                    send_pool_.release(task.staging_addr_);
                    task.staging_addr_ = nullptr;
                }
            }
        }
        send_lane.active_send_op_.reset();

        auto& recv_lane = recv_peer_lanes_[i];
        for (auto& copy_ready_event : recv_lane.copy_ready_events_) {
            if (copy_ready_event == nullptr) {
                continue;
            }
            const cudaError_t destroy_error =
                cudaEventDestroy(copy_ready_event);
            TORCH_CHECK(destroy_error == cudaSuccess,
                        "Failed to destroy pooled recv copy-ready event: ",
                        cudaGetErrorString(destroy_error));
            copy_ready_event = nullptr;
        }
        recv_lane.pending_recv_ops_.clear();
        recv_lane.active_recv_op_.reset();
    }

    if (!engine_) {
        return;
    }

    if (resources_.publish_staging_buf_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.publish_staging_buf_);
        delete[] resources_.publish_staging_buf_;
        resources_.publish_staging_buf_ = nullptr;
    }

    if (resources_.completion_staging_buf_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.completion_staging_buf_);
        delete[] resources_.completion_staging_buf_;
        resources_.completion_staging_buf_ = nullptr;
    }

    if (resources_.publish_region_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.publish_region_);
        delete[] resources_.publish_region_;
        resources_.publish_region_ = nullptr;
    }

    if (resources_.completion_region_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.completion_region_);
        delete[] resources_.completion_region_;
        resources_.completion_region_ = nullptr;
    }

    if (resources_.send_pool_base_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.send_pool_base_);
        if (is_cpu_) {
            std::free(resources_.send_pool_base_);
        } else {
            cudaFree(resources_.send_pool_base_);
        }
        resources_.send_pool_base_ = nullptr;
    }

    if (resources_.recv_pool_base_ != nullptr) {
        engine_->unregisterLocalMemory(resources_.recv_pool_base_);
        if (is_cpu_) {
            std::free(resources_.recv_pool_base_);
        } else {
            cudaFree(resources_.recv_pool_base_);
        }
        resources_.recv_pool_base_ = nullptr;
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
    uint64_t remote_addr_in, uint32_t sequence_in, uint32_t generation_in)
    : tensor_offset_(tensor_offset_in),
      chunk_len_(chunk_len_in),
      staging_addr_(staging_addr_in),
      remote_addr_(remote_addr_in),
      sequence_(sequence_in),
      generation_(generation_in) {
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
                                             uint32_t generation_in)
    : tensor_offset_(tensor_offset_in),
      chunk_len_(chunk_len_in),
      local_addr_(local_addr_in),
      sequence_(sequence_in),
      generation_(generation_in) {
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

PublishSlot* P2PProxy::getLocalPublishLane(int peer_rank) const {
    return resources_.publish_region_ +
           static_cast<size_t>(peer_rank) * kP2PControlRingSize;
}

CompletionSlot* P2PProxy::getLocalCompletionLane(int peer_rank) const {
    return resources_.completion_region_ +
           static_cast<size_t>(peer_rank) * kP2PControlRingSize;
}

uint64_t P2PProxy::getRemotePublishSlot(int peer_rank,
                                        uint32_t sequence) const {
    const uint64_t slot_index = sequence % kP2PControlRingSize;
    return meta_->segmentInfos[peer_rank].p2p_publish_region +
           (static_cast<uint64_t>(rank_) * kP2PControlRingSize + slot_index) *
               sizeof(PublishSlot);
}

uint64_t P2PProxy::getRemoteCompletionSlot(int peer_rank,
                                           uint32_t sequence) const {
    const uint64_t slot_index = sequence % kP2PControlRingSize;
    return meta_->segmentInfos[peer_rank].p2p_completion_region +
           (static_cast<uint64_t>(rank_) * kP2PControlRingSize + slot_index) *
               sizeof(CompletionSlot);
}

PublishSlot* P2PProxy::getLocalPublishStagingBuf(int peer_rank,
                                                 uint32_t sequence) const {
    const size_t staging_idx =
        static_cast<size_t>(peer_rank) * kP2PControlRingSize +
        sequence % kP2PControlRingSize;
    return resources_.publish_staging_buf_ + staging_idx;
}
CompletionSlot* P2PProxy::getLocalCompletionStagingBuf(
    int peer_rank, uint32_t sequence) const {
    const size_t staging_idx =
        static_cast<size_t>(peer_rank) * kP2PControlRingSize +
        static_cast<size_t>(sequence % kP2PControlRingSize);
    return resources_.completion_staging_buf_ + staging_idx;
}

// Receiver advertise.
//
// Grab a free chunk from RecvPool and invite the sender to write into it.
// We (RDMA) write a PublishSlot into the sender's PublishLane so the sender
// knows:
//   (a) which sequence this is,
//   (b) the RecvPool offset to write to,
//   (c) how many bytes we expect.
// If the pool is exhausted we return false and retry on the next polling
// iteration (non-blocking).
bool P2PProxy::tryIssueRecvTask(RecvOpContext& op_ctx, RecvPeerLane& lane) {
    if (op_ctx.bytes_advertised_ >= op_ctx.total_bytes_) {
        return false;
    }

    if (op_ctx.tasks_.size() >= kP2PControlRingSize) {
        // No free control slot, retry on the next iteration
        return false;
    }

    void* local_addr = recv_pool_.acquire();
    if (local_addr == nullptr) {
        // No free recv chunk, retry on the next iteration
        return false;
    }

    const uint32_t chunk_len = static_cast<uint32_t>(std::min<uint64_t>(
        kP2PChunkSize, op_ctx.total_bytes_ - op_ctx.bytes_advertised_));
    const uint64_t seq = lane.publish_issue_seq_;
    const uint64_t remote_publish_offset =
        getRemotePublishSlot(op_ctx.peer_rank_, seq);

    const uint32_t current_gen =
        peer_generation_[op_ctx.peer_rank_].load(std::memory_order_acquire);
    op_ctx.tasks_.emplace_back(op_ctx.bytes_advertised_, chunk_len, local_addr,
                               seq, current_gen);
    auto& task = op_ctx.tasks_.back();

    auto* publish_buf = getLocalPublishStagingBuf(op_ctx.peer_rank_, seq);
    publish_buf->publish(current_gen, seq,
                         reinterpret_cast<uint64_t>(local_addr), chunk_len);
    const BatchID batch_id = engine_->allocateBatchID(1);
    engine_->submitTransfer(
        batch_id, {TransferRequest{
                      .opcode = TransferRequest::WRITE,
                      .source = static_cast<void*>(publish_buf),
                      .target_id = meta_->segmentIDs[op_ctx.peer_rank_],
                      .target_offset = remote_publish_offset,
                      .length = sizeof(PublishSlot),
                  }});
    task.publish_batch_id_ = batch_id;
    task.last_update_time_ = std::chrono::steady_clock::now();

    ++lane.publish_issue_seq_;
    op_ctx.bytes_advertised_ += chunk_len;
    return true;
}

// Drive a single receiver chunk through its state machine.
//
// kAdvertise -> kWaitCompletion -> kCopyOut -> kFinished -> erase.
bool P2PProxy::stepRecvTask(RecvTransferTask& task) {
    switch (task.state_) {
        case RecvTaskState::kAdvertise:
            return stepRecvAdvertise(task);

        case RecvTaskState::kWaitCompletion:
            // kWaitCompletion is polled in stepRecv
            return false;

        case RecvTaskState::kCopyOut:
            return stepRecvCopyOut(task);

        case RecvTaskState::kFinished:
        case RecvTaskState::kFailed:
            return false;
    }
    return false;
}

bool P2PProxy::stepRecvAdvertise(RecvTransferTask& task) {
    TORCH_CHECK(task.publish_batch_id_.has_value(),
                "Expected a publish_batch_id in tryIssueRecvTask");

    TransferStatus publish_status;
    engine_->getTransferStatus(task.publish_batch_id_.value(), 0,
                               publish_status);

    if (publish_status.s == TransferStatusEnum::COMPLETED) {
        engine_->freeBatchID(task.publish_batch_id_.value());
        task.publish_batch_id_.reset();
        task.state_ = RecvTaskState::kWaitCompletion;
        task.last_update_time_ = std::chrono::steady_clock::now();
        return true;
    }

    if (publish_status.s == TransferStatusEnum::FAILED || isTimeout(task)) {
        LOG(ERROR) << "P2P publish transfer failed/timeout, seq="
                   << task.sequence_;
        engine_->freeBatchID(task.publish_batch_id_.value());
        task.publish_batch_id_.reset();
        task.state_ = RecvTaskState::kFailed;
        return true;
    }
    return false;
}

// Poll the GPU Copy-Out event.
//
// After the CompletionSlot arrives we initiate cudaMemcpyAsync from the
// RecvPool chunk to the user tensor and record an event.  This function
// waits for that event and returns the chunk to RecvPool.
bool P2PProxy::stepRecvCopyOut(RecvTransferTask& task) {
    TORCH_CHECK(task.copy_ready_event_ != nullptr,
                "Expected a copy_ready_event");
    cudaError_t query_error = cudaEventQuery(task.copy_ready_event_);
    if (query_error == cudaSuccess) {
        task.copy_ready_event_ = nullptr;
        recv_pool_.release(task.local_addr_);
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

// Sender fetch invitation
//
// Poll the local PublishLane for the next expected sequence.  If the slot
// matches our consume cursor we accept the invitation: allocate a staging
// chunk from SendPool, copy the corresponding slice of the user tensor into
// it, and advance the cursor.  If the pool is full or no invitation has
// arrived we return false immediately (non-blocking).
bool P2PProxy::tryIssueSendTask(SendOpContext& op_ctx, SendPeerLane& lane) {
    // Check for timeout while waiting for the peer's PublishSlot.
    if (isTimeout(op_ctx)) {
        LOG(ERROR) << "P2P wait invitation timeout peer=" << op_ctx.peer_rank_;
        op_ctx.status_->store(OpStatus::kFailed, std::memory_order_release);
        return false;
    }

    if (op_ctx.bytes_staged_ >= op_ctx.total_bytes_) {
        return false;
    }

    if (op_ctx.tasks_.size() >= kP2PControlRingSize) {
        return false;
    }

    PublishSlot* local_publish = getLocalPublishLane(op_ctx.peer_rank_);
    const uint64_t seq = lane.publish_consume_seq_;
    auto& slot = local_publish[static_cast<size_t>(seq % kP2PControlRingSize)];

    // Step 1 -- Try load the slot
    uint64_t recv_addr = 0;
    uint32_t chunk_len = 0;
    uint32_t slot_gen = 0;
    uint32_t slot_seq = 0;
    if (!slot.tryLoad(recv_addr, chunk_len, slot_gen, slot_seq)) {
        // Slot is either empty or torn (partial RDMA write). Retry next poll.
        return false;
    }

    // Step 2 -- Stale packet: the slot carries data from a previous epoch
    // (before a Reset).  Clear it so the fresh invitation can land safely,
    // but DO NOT advance the consume cursor -- the peer will re-send the
    // same sequence after the Reset.
    const uint32_t current_gen =
        peer_generation_[op_ctx.peer_rank_].load(std::memory_order_acquire);
    if (slot_gen != current_gen) {
        LOG(WARNING) << "[P2PProxy][Send] tryIssueSendTask peer="
                     << op_ctx.peer_rank_ << " STALE_GEN seq=" << seq
                     << " slot.gen=" << slot_gen
                     << " current_gen=" << current_gen;
        slot.reset();
        return true;
    }

    // Step 3 -- Sequence check: make sure this is exactly the slot we expect.
    if (slot_seq != static_cast<uint32_t>(seq)) {
        return false;
    }

    void* staging_addr = send_pool_.acquire();
    if (staging_addr == nullptr) {
        return false;
    }

    const uint32_t expected_chunk_len =
        static_cast<uint32_t>(std::min<uint64_t>(
            kP2PChunkSize, op_ctx.total_bytes_ - op_ctx.bytes_staged_));
    TORCH_CHECK(chunk_len <= expected_chunk_len,
                "P2P send got invalid chunk_len in publish slot.");

    op_ctx.tasks_.emplace_back(op_ctx.bytes_staged_, chunk_len, staging_addr,
                               recv_addr, seq, slot_gen);
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

    ++lane.publish_consume_seq_;
    op_ctx.bytes_staged_ += task.chunk_len_;
    task.last_update_time_ = std::chrono::steady_clock::now();
    return true;
}

// Drive a single sender chunk through its state machine.
//
// kCopyIn -> kWriteRemote -> kCompletion -> kFinished -> erase.
bool P2PProxy::stepSendTask(SendOpContext& op_ctx, SendTransferTask& task) {
    switch (task.state_) {
        case SendTaskState::kCopyIn:
            return stepSendCopyIn(task);

        case SendTaskState::kWriteRemote:
            return stepSendWriteRemote(op_ctx, task);

        case SendTaskState::kCompletion:
            return stepSendCompletion(op_ctx, task);

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
        task.state_ = SendTaskState::kCompletion;
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

// Write the CompletionSlot back to the receiver.
//
// This tells the receiver that the data is present in its RecvPool and it
// may begin the Copy-Out.  When the CompletionSlot write finishes we free
// the staging chunk and transition to kFinished.
bool P2PProxy::stepSendCompletion(SendOpContext& op_ctx,
                                  SendTransferTask& task) {
    bool did_work = false;
    if (!task.completion_batch_id_.has_value()) {
        auto* completion_buf =
            getLocalCompletionStagingBuf(op_ctx.peer_rank_, task.sequence_);
        completion_buf->publish(task.generation_, task.sequence_,
                                task.chunk_len_);

        const BatchID batch_id = engine_->allocateBatchID(1);
        engine_->submitTransfer(
            batch_id, {TransferRequest{
                          .opcode = TransferRequest::WRITE,
                          .source = static_cast<void*>(completion_buf),
                          .target_id = meta_->segmentIDs[op_ctx.peer_rank_],
                          .target_offset = getRemoteCompletionSlot(
                              op_ctx.peer_rank_, task.sequence_),
                          .length = sizeof(CompletionSlot),
                      }});
        task.completion_batch_id_ = batch_id;
        task.last_update_time_ = std::chrono::steady_clock::now();
        did_work = true;
    }

    TransferStatus completion_status;
    engine_->getTransferStatus(task.completion_batch_id_.value(), 0,
                               completion_status);
    if (completion_status.s == TransferStatusEnum::COMPLETED) {
        engine_->freeBatchID(task.completion_batch_id_.value());
        task.completion_batch_id_.reset();
        send_pool_.release(task.staging_addr_);
        task.staging_addr_ = nullptr;
        task.state_ = SendTaskState::kFinished;
        did_work = true;
    } else if (completion_status.s == TransferStatusEnum::FAILED ||
               isTimeout(task)) {
        LOG(ERROR) << "P2P completion transfer failed/timeout, peer="
                   << op_ctx.peer_rank_ << ", seq=" << task.sequence_;
        engine_->freeBatchID(task.completion_batch_id_.value());
        task.completion_batch_id_.reset();
        task.state_ = SendTaskState::kFailed;
        did_work = true;
    }

    return did_work;
}

bool P2PProxy::isSendOpCompleted(const SendOpContext& op_ctx) const {
    return op_ctx.bytes_staged_ == op_ctx.total_bytes_ && op_ctx.tasks_.empty();
}

bool P2PProxy::isRecvOpCompleted(const RecvOpContext& op_ctx) const {
    return op_ctx.bytes_advertised_ == op_ctx.total_bytes_ &&
           op_ctx.tasks_.empty();
}

// Sender state machine
//
// Pipeline per peer:
//   1. Drain the shared send_queue_ into the peer's pending_send_ops_.
//   2. Promote the first pending op to active_send_op_.
//   3. While we have invitations (PublishSlots) and staging buffers,
//      pull more tensor slices into SendPool (TryIssueSendTask).
//   4. Advance every active chunk through its state machine
//      (Copy-In -> Write Remote -> Completion write).
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

        // Pull as many invitations as we have free chunks
        while (tryIssueSendTask(op_ctx, lane)) {
            did_work = true;
        }

        // Advance every chunk through the sender state machine.
        // send op may fail due to wait invitation timeout in tryIssueSendTask.
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

// Poll the local CompletionLane for the head task and initiate Copy-Out.
//
// Returns true if a completion was processed (including stale-slot clearing).
bool P2PProxy::pollRecvCompletionSlot(RecvOpContext& op_ctx, RecvPeerLane& lane,
                                      RecvTransferTask& head_task) {
    // Check for timeout while waiting for the peer's CompletionSlot.
    if (isTimeout(head_task)) {
        LOG(ERROR) << "P2P recv completion timeout peer=" << op_ctx.peer_rank_
                   << " seq=" << head_task.sequence_;
        head_task.state_ = RecvTaskState::kFailed;
        return true;
    }

    auto* completion_lane = getLocalCompletionLane(op_ctx.peer_rank_);
    auto& slot = completion_lane[head_task.sequence_ % kP2PControlRingSize];

    // Step 1 -- Try load the slot
    uint32_t completed_len = 0;
    uint32_t slot_gen = 0;
    uint32_t slot_seq = 0;
    if (!slot.tryLoad(completed_len, slot_gen, slot_seq)) {
        // Slot is either empty or torn. Retry next poll.
        return false;
    }

    const uint32_t current_gen =
        peer_generation_[op_ctx.peer_rank_].load(std::memory_order_acquire);

    // Step 2 -- Stale packet: data from a previous epoch (before Reset).
    // Clear the slot so the fresh completion can land safely.  Do NOT pop
    // the task, do NOT free the chunk, and do NOT advance
    // completion_consume_seq_ -- the current task is still waiting for valid
    // completion.
    if (slot_gen != current_gen) {
        LOG(WARNING) << "[P2PProxy][Recv] pollRecvCompletionSlot peer="
                     << op_ctx.peer_rank_
                     << " front-seq=" << head_task.sequence_
                     << " GEN_MISMATCH slot.gen=" << slot_gen
                     << " current_gen=" << current_gen;
        slot.reset();
        return true;
    }

    // Step 3 -- Sequence check: make sure this is the exact completion
    // we are waiting for.
    if (slot_seq != head_task.sequence_) {
        return false;
    }

    void* src_ptr = head_task.local_addr_;
    auto* tensor_ptr = static_cast<uint8_t*>(op_ctx.tensor_.data_ptr());
    void* dst_ptr = tensor_ptr + head_task.tensor_offset_;

    if (is_cpu_) {
        std::memcpy(dst_ptr, src_ptr, head_task.chunk_len_);
        recv_pool_.release(head_task.local_addr_);
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
    ++lane.completion_consume_seq_;
    return true;
}

// Receiver state machine
//
// Pipeline per peer:
//   1. Drain the shared recv_queue_ into the peer's pending_recv_ops_.
//   2. Promote the first pending op to active_recv_op_.
//   3. While we have free RecvPool chunks, issue PublishSlots to the sender
//      (TryIssueRecvTask).
//   4. Poll the local CompletionLane in order.  When a CompletionSlot
//      arrives, initiate Copy-Out from RecvPool to the user tensor.
//   5. Advance every active chunk through its state machine
//      (Advertise -> Copy-Out -> erase).
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

        // In-order completion polling: we only look at the head of the queue
        // because the sender acknowledges chunks in the same order it
        // consumes invitations.
        if (!op_ctx.tasks_.empty()) {
            auto& head = op_ctx.tasks_.front();
            if (head.state_ == RecvTaskState::kWaitCompletion) {
                if (pollRecvCompletionSlot(op_ctx, lane, head)) {
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

void P2PProxy::setDeviceWorker(P2PDeviceWorker* worker) {
    device_worker_ = worker;
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

void P2PDeviceWorker::registerProxy(const std::shared_ptr<P2PProxy>& proxy) {
    proxy->setDeviceWorker(this);
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
    proxy->setDeviceWorker(nullptr);
    send_wakeup_cv_.notify_one();
    recv_wakeup_cv_.notify_one();
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

std::shared_ptr<P2PDeviceWorker> P2PDeviceWorkerManager::getCPUWorker() {
    std::lock_guard<std::mutex> lock(manager_mutex_);

    auto it = workers_.find(CPUWorkerID);
    if (it != workers_.end()) {
        if (auto ptr = it->second.lock()) return ptr;
    }

    auto worker =
        std::make_shared<P2PDeviceWorker>(/* is_cpu */ true, CPUWorkerID);
    workers_[CPUWorkerID] = worker;
    return worker;
}

std::shared_ptr<P2PDeviceWorker> P2PDeviceWorkerManager::getCUDAWorker(
    int cuda_device_index) {
    std::lock_guard<std::mutex> lock(manager_mutex_);

    auto it = workers_.find(cuda_device_index);
    if (it != workers_.end()) {
        if (auto ptr = it->second.lock()) return ptr;
    }

    auto worker = std::make_shared<P2PDeviceWorker>(/* is_cpu */ false,
                                                    cuda_device_index);
    workers_[cuda_device_index] = worker;
    return worker;
}

}  // namespace mooncake
