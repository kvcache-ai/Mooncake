#include "async_metadata_notifier.h"

#include <glog/logging.h>

#include <algorithm>

namespace mooncake {

AsyncMetadataNotifier::AsyncMetadataNotifier(P2PMasterClient& master_client,
                                             const UUID& client_id,
                                             size_t sender_thread_count,
                                             size_t max_batch_size,
                                             size_t queue_capacity,
                                             SyncFailureCallback failure_cb)
    : master_client_(master_client),
      client_id_(client_id),
      sender_thread_count_(sender_thread_count),
      max_batch_size_(max_batch_size),
      batch_buffers_(sender_thread_count),
      failure_cb_(std::move(failure_cb)) {
    const size_t min_queue_capacity = max_batch_size_ * sender_thread_count_;
    const size_t total_cap = std::max(queue_capacity, min_queue_capacity);
    const size_t per_shard = total_cap / sender_thread_count_;

    shards_.reserve(sender_thread_count_);
    for (size_t i = 0; i < sender_thread_count_; ++i) {
        auto shard = std::make_unique<SenderShard>();
        shard->capacity = per_shard;
        shard->slots.resize(per_shard);
        shard->free_stack.reserve(per_shard);
        for (size_t j = 0; j < per_shard; ++j) {
            shard->free_stack.push_back(j);
        }
        shards_.push_back(std::move(shard));
        batch_buffers_[i].resize(max_batch_size_);
    }
}

AsyncMetadataNotifier::~AsyncMetadataNotifier() { Stop(); }

void AsyncMetadataNotifier::Start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return;  // already running
    }

    for (auto& shard : shards_) {
        ResetShard(*shard);
    }

    LOG(INFO) << "AsyncMetadataNotifier starting with " << sender_thread_count_
              << " sender shards, max_batch_size=" << max_batch_size_;

    for (size_t i = 0; i < sender_thread_count_; ++i) {
        shards_[i]->sender_thread = std::thread([this, i]() { SenderLoop(i); });
    }
}

void AsyncMetadataNotifier::Stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return;  // not running
    }

    // Wake all sender threads (both shard CVs and stop CV for retry sleeps)
    {
        std::lock_guard<std::mutex> lk{stop_mutex_};
        stop_cv_.notify_all();
    }
    for (auto& shard : shards_) {
        shard->sender_cv.notify_all();
        shard->producer_cv.notify_all();
    }
    for (auto& shard : shards_) {
        if (shard->sender_thread.joinable()) {
            shard->sender_thread.join();
        }
    }

    // Clear all queues so next Start() begins clean.
    for (auto& shard : shards_) {
        ResetShard(*shard);
    }
    // Reset circuit breaker for next Start cycle
    consecutive_rpc_failures_.store(0, std::memory_order_release);

    LOG(INFO) << "AsyncMetadataNotifier stopped";
}

void AsyncMetadataNotifier::ResetShard(SenderShard& shard) {
    std::lock_guard<std::mutex> lock(shard.mutex);
    if (shard.count > 0) {
        LOG(WARNING) << "AsyncMetadataNotifier: discarding " << shard.count
                     << " pending ops on reset";
    }
    shard.list_head = InvalidIdx;
    shard.list_tail = InvalidIdx;
    shard.count = 0;
    shard.free_stack.clear();
    for (size_t i = 0; i < shard.capacity; ++i) {
        shard.slots[i] = Slot{};
        shard.free_stack.push_back(i);
    }
    shard.coalesce_index.clear();
}

tl::expected<void, ErrorCode> AsyncMetadataNotifier::EnqueueAdd(
    const std::string& key, const UUID& segment_id, size_t size) {
    PendingOp op;
    op.type = PendingOp::ADD;
    op.key = key;
    op.segment_id = segment_id;
    op.size = size;
    return DoEnqueue(std::move(op));
}

tl::expected<void, ErrorCode> AsyncMetadataNotifier::EnqueueRemove(
    const std::string& key, const UUID& segment_id) {
    PendingOp op;
    op.type = PendingOp::REMOVE;
    op.key = key;
    op.segment_id = segment_id;
    return DoEnqueue(std::move(op));
}

tl::expected<void, ErrorCode> AsyncMetadataNotifier::DoEnqueue(PendingOp&& op) {
    if (!running_.load(std::memory_order_acquire)) {
        LOG(WARNING)
            << "AsyncMetadataNotifier has stopped, fail to enqueue key="
            << op.key;
        return tl::unexpected(ErrorCode::ASYNC_ENQUEUE_FAILED);
    }

    const size_t shard_idx =
        std::hash<std::string>{}(op.key) % sender_thread_count_;
    auto& shard = *shards_[shard_idx];

    std::unique_lock<std::mutex> lock(shard.mutex);

    // 1. Wait if pool is full
    if (shard.IsFull()) {
        bool ok = shard.producer_cv.wait_for(lock, EnqueueTimeout, [&] {
            return !shard.IsFull() || !running_.load(std::memory_order_relaxed);
        });
        if (!ok || !running_.load(std::memory_order_relaxed)) {
            LOG(WARNING) << "AsyncMetadataNotifier: enqueue timeout/shutdown"
                         << ", key=" << op.key;
            return tl::unexpected(ErrorCode::ASYNC_ENQUEUE_FAILED);
        }
    }

    // 2. Coalescing: check for existing pending op
    CoalesceKey ck{op.key, op.segment_id};
    auto [ci, inserted] = shard.coalesce_index.emplace(ck, CoalesceEntry{});
    auto& entry = ci->second;

    // 2a. Duplicate same-type op already pending — treat as success, skip
    if (op.type == PendingOp::ADD && entry.add_idx != InvalidIdx) {
        return {};
    }
    if (op.type == PendingOp::REMOVE && entry.remove_idx != InvalidIdx) {
        return {};
    }

    // 2b. Opposite-type op pending — cancel it
    if (op.type == PendingOp::ADD && entry.remove_idx != InvalidIdx) {
        // Cancel the pending REMOVE, don't insert this ADD
        shard.Unlink(entry.remove_idx);
        shard.FreeSlot(entry.remove_idx);
        entry.remove_idx = InvalidIdx;
        shard.coalesce_index.erase(ci);
        lock.unlock();
        shard.producer_cv.notify_one();
        return {};
    }
    if (op.type == PendingOp::REMOVE && entry.add_idx != InvalidIdx) {
        // Cancel the pending ADD, don't insert this REMOVE
        shard.Unlink(entry.add_idx);
        shard.FreeSlot(entry.add_idx);
        entry.add_idx = InvalidIdx;
        shard.coalesce_index.erase(ci);
        lock.unlock();
        shard.producer_cv.notify_one();
        return {};
    }

    // 3. No coalescing: alloc slot, write data, link to tail
    size_t idx = shard.AllocSlot();
    shard.slots[idx].op = std::move(op);
    shard.LinkTail(idx);

    if (shard.slots[idx].op.type == PendingOp::ADD) {
        entry.add_idx = idx;
    } else {
        entry.remove_idx = idx;
    }

    lock.unlock();
    shard.sender_cv.notify_one();
    return {};
}

void AsyncMetadataNotifier::SenderLoop(size_t shard_idx) {
    auto& shard = *shards_[shard_idx];
    auto& batch = batch_buffers_[shard_idx];

    while (true) {
        // Circuit breaker: cool down then fall through to attempt send.
        // Must NOT continue — at least one thread must attempt SendBatch
        // so that RecordSuccess() can close the breaker.
        if (running_.load(std::memory_order_acquire) && IsPaused()) {
            std::unique_lock<std::mutex> lock(shard.mutex);
            shard.sender_cv.wait_for(lock, CircuitBreakerCooldown, [&] {
                return !running_.load(std::memory_order_relaxed) || !IsPaused();
            });
        }

        size_t n = CollectBatch(shard, batch);

        if (n == 0) {
            if (!running_.load(std::memory_order_acquire)) {
                // Draining: exit if queue is empty
                std::lock_guard<std::mutex> lock(shard.mutex);
                if (shard.IsEmpty()) {
                    break;
                }
            }
            continue;
        }

        SendBatch(batch, n);
    }
}

size_t AsyncMetadataNotifier::CollectBatch(SenderShard& shard,
                                           std::vector<PendingOp>& batch_out) {
    std::unique_lock<std::mutex> lock(shard.mutex);

    shard.sender_cv.wait_for(lock, BatchTimeout, [&] {
        return !shard.IsEmpty() || !running_.load(std::memory_order_relaxed);
    });

    size_t collected = 0;
    while (!shard.IsEmpty() && collected < max_batch_size_) {
        size_t idx = shard.list_head;
        auto& slot = shard.slots[idx];

        // Remove from coalesce index
        CoalesceKey ck{slot.op.key, slot.op.segment_id};
        auto ci = shard.coalesce_index.find(ck);
        if (ci != shard.coalesce_index.end()) {
            if (slot.op.type == PendingOp::ADD) {
                ci->second.add_idx = InvalidIdx;
            } else {
                ci->second.remove_idx = InvalidIdx;
            }
            if (ci->second.add_idx == InvalidIdx &&
                ci->second.remove_idx == InvalidIdx) {
                shard.coalesce_index.erase(ci);
            }
        }

        batch_out[collected++] = std::move(slot.op);
        shard.Unlink(idx);
        shard.FreeSlot(idx);
    }

    if (collected > 0) {
        lock.unlock();
        shard.producer_cv.notify_all();
    }

    return collected;
}

void AsyncMetadataNotifier::SendBatch(std::vector<PendingOp>& batch,
                                      size_t count) {
    BatchSyncReplicaRequest req;
    req.client_id = client_id_;

    req.add_keys.reserve(count);
    req.add_sizes.reserve(count);
    req.add_segment_ids.reserve(count);
    req.remove_keys.reserve(count);
    req.remove_segment_ids.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        auto& op = batch[i];
        if (op.type == PendingOp::ADD) {
            req.add_keys.push_back(std::move(op.key));
            req.add_sizes.push_back(op.size);
            req.add_segment_ids.push_back(op.segment_id);
        } else {
            req.remove_keys.push_back(std::move(op.key));
            req.remove_segment_ids.push_back(op.segment_id);
        }
    }

    for (int attempt = 0; attempt < MaxRetryCount; ++attempt) {
        auto result = master_client_.BatchSyncReplica(req);
        if (result.has_value()) {
            RecordSuccess();
            auto& resp = result.value();
            for (size_t i = 0; i < resp.add_results.size(); ++i) {
                auto ec = resp.add_results[i];
                if (ec != ErrorCode::OK &&
                    ec != ErrorCode::REPLICA_ALREADY_EXISTS) {
                    LOG(WARNING)
                        << "BatchSyncReplica ADD key=" << req.add_keys[i]
                        << " failed: " << toString(ec);
                    if (failure_cb_) {
                        failure_cb_(req.add_keys[i], req.add_segment_ids[i],
                                    ec);
                    }
                }
            }
            for (size_t i = 0; i < resp.remove_results.size(); ++i) {
                if (resp.remove_results[i] != ErrorCode::OK) {
                    LOG(WARNING)
                        << "BatchSyncReplica REMOVE key=" << req.remove_keys[i]
                        << " failed: " << toString(resp.remove_results[i]);
                }
            }
            return;
        }
        LOG(WARNING) << "BatchSyncReplica attempt " << attempt
                     << " failed: " << toString(result.error());
        if (attempt < MaxRetryCount - 1) {
            // Use CV wait instead of sleep so Stop() can interrupt promptly
            auto backoff = std::chrono::milliseconds(100 * (1 << attempt));
            std::unique_lock<std::mutex> lk{stop_mutex_};
            if (stop_cv_.wait_for(lk, backoff, [&] {
                    return !running_.load(std::memory_order_relaxed);
                })) {
                // Stop requested — abort retry, drop this batch
                break;
            }
        }
    }
    RecordFailure();
    LOG(ERROR) << "BatchSyncReplica failed after " << MaxRetryCount + 1
               << " attempts, dropping " << count << " ops";
    // Rollback: notify failure for all ADD ops so local replicas get cleaned up
    if (failure_cb_) {
        for (size_t i = 0; i < req.add_keys.size(); ++i) {
            failure_cb_(req.add_keys[i], req.add_segment_ids[i],
                        ErrorCode::INTERNAL_ERROR);
        }
    }
}

void AsyncMetadataNotifier::RecordSuccess() {
    int32_t prev =
        consecutive_rpc_failures_.exchange(0, std::memory_order_acq_rel);
    if (prev < 0) {
        LOG(INFO) << "AsyncMetadataNotifier: circuit breaker closed";
    }
}

void AsyncMetadataNotifier::RecordFailure() {
    int32_t old_val = consecutive_rpc_failures_.load(std::memory_order_acquire);
    while (true) {
        if (old_val < 0) return;  // already paused, nothing to do
        int32_t new_val = old_val + 1;
        if (new_val >= CircuitBreakerThreshold) {
            new_val = -new_val;  // flip to paused
        }
        if (consecutive_rpc_failures_.compare_exchange_weak(
                old_val, new_val, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            if (new_val < 0) {
                LOG(ERROR)
                    << "AsyncMetadataNotifier: circuit breaker OPEN after "
                    << -new_val << " consecutive failures";
            }
            return;
        }
        // old_val updated by CAS, retry
    }
}

}  // namespace mooncake
