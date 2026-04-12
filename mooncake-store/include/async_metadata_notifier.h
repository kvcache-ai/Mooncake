#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>

#include "p2p_master_client.h"
#include "types.h"

namespace mooncake {

// Called when an async ADD fails with a non-transient error.
// Caller should delete the local replica.
using SyncFailureCallback = std::function<void(
    const std::string& key, const UUID& segment_id, ErrorCode error)>;

class AsyncMetadataNotifier {
   public:
    AsyncMetadataNotifier(P2PMasterClient& master_client, const UUID& client_id,
                          size_t sender_thread_count, size_t max_batch_size,
                          size_t queue_capacity,
                          SyncFailureCallback failure_cb = nullptr);
    ~AsyncMetadataNotifier();

    AsyncMetadataNotifier(const AsyncMetadataNotifier&) = delete;
    AsyncMetadataNotifier& operator=(const AsyncMetadataNotifier&) = delete;

    // Start/Stop can be called alternately. Stop drains + clears queue.
    void Start();
    void Stop();

    tl::expected<void, ErrorCode> EnqueueAdd(const std::string& key,
                                             const UUID& segment_id,
                                             size_t size);

    tl::expected<void, ErrorCode> EnqueueRemove(const std::string& key,
                                                const UUID& segment_id);

    bool IsPaused() const {
        return consecutive_rpc_failures_.load(std::memory_order_acquire) < 0;
    }

   private:
    struct PendingOp {
        enum Type : int32_t { ADD = 0, REMOVE = 1 };

        Type type = ADD;
        std::string key;
        UUID segment_id{};
        size_t size = 0;
    };

    // Identifier of a Key. We assume that each key is unique in a segment.
    struct CoalesceKey {
        std::string key;
        UUID segment_id;

        bool operator==(const CoalesceKey& o) const {
            return key == o.key && segment_id == o.segment_id;
        }
    };

    struct CoalesceKeyHash {
        size_t operator()(const CoalesceKey& k) const {
            size_t h = std::hash<std::string>{}(k.key);
            boost::hash_combine(h, k.segment_id);
            return h;
        }
    };

    // Coalescing entry: tracks slot index of a pending op for a key.
    // InvalidIdx means no pending op of that type.
    static constexpr size_t InvalidIdx = SIZE_MAX;
    struct CoalesceEntry {
        size_t add_idx = InvalidIdx;
        size_t remove_idx = InvalidIdx;
    };

    // Pool node: PendingOp data + intrusive doubly-linked list pointers.
    struct Slot {
        PendingOp op;
        size_t prev = InvalidIdx;
        size_t next = InvalidIdx;
    };

    struct SenderShard {
        SenderShard() = default;
        SenderShard(const SenderShard&) = delete;
        SenderShard& operator=(const SenderShard&) = delete;
        SenderShard(SenderShard&&) = delete;
        SenderShard& operator=(SenderShard&&) = delete;

        std::mutex mutex;
        std::condition_variable sender_cv;    // sender waits when empty
        std::condition_variable producer_cv;  // producer waits when full

        // Pre-allocated slot pool — no dynamic alloc on enqueue/dequeue
        std::vector<Slot> slots;
        std::vector<size_t> free_stack;

        // Index-based intrusive doubly-linked list for FIFO ordering
        size_t list_head = InvalidIdx;
        size_t list_tail = InvalidIdx;
        size_t count = 0;
        size_t capacity = 0;

        std::unordered_map<CoalesceKey, CoalesceEntry, CoalesceKeyHash>
            coalesce_index;

        std::thread sender_thread;

        bool IsFull() const { return free_stack.empty(); }
        bool IsEmpty() const { return count == 0; }

        // Allocate a slot from the free stack. Caller must check !IsFull().
        size_t AllocSlot() {
            size_t idx = free_stack.back();
            free_stack.pop_back();
            return idx;
        }

        // Return a slot to the free stack.
        void FreeSlot(size_t idx) {
            slots[idx].prev = InvalidIdx;
            slots[idx].next = InvalidIdx;
            free_stack.push_back(idx);
        }

        // Append slot idx to the tail of the ordered list.
        void LinkTail(size_t idx) {
            slots[idx].prev = list_tail;
            slots[idx].next = InvalidIdx;
            if (list_tail != InvalidIdx) {
                slots[list_tail].next = idx;
            } else {
                list_head = idx;
            }
            list_tail = idx;
            count++;
        }

        // Remove slot idx from the ordered list.
        void Unlink(size_t idx) {
            auto& slot = slots[idx];
            if (slot.prev != InvalidIdx) {
                slots[slot.prev].next = slot.next;
            } else {
                list_head = slot.next;
            }
            if (slot.next != InvalidIdx) {
                slots[slot.next].prev = slot.prev;
            } else {
                list_tail = slot.prev;
            }
            count--;
        }
    };

   private:
    tl::expected<void, ErrorCode> DoEnqueue(PendingOp&& op);

    void SenderLoop(size_t shard_idx);
    size_t CollectBatch(SenderShard& shard, std::vector<PendingOp>& batch_out);
    void SendBatch(std::vector<PendingOp>& batch, size_t count);
    void RecordSuccess();
    void RecordFailure();
    void ResetShard(SenderShard& shard);

   private:
    static constexpr int MaxRetryCount = 3;
    static constexpr int CircuitBreakerThreshold = 5;
    static constexpr auto BatchTimeout = std::chrono::milliseconds(20);
    static constexpr auto EnqueueTimeout = std::chrono::milliseconds(1000);
    static constexpr auto CircuitBreakerCooldown = std::chrono::seconds(2);

    P2PMasterClient& master_client_;
    const UUID client_id_;
    const size_t sender_thread_count_;
    const size_t max_batch_size_;
    std::atomic<bool> running_{false};

    // Used to wake sender threads from retry sleep during Stop().
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;

    std::vector<std::unique_ptr<SenderShard>> shards_;
    std::vector<std::vector<PendingOp>> batch_buffers_;
    SyncFailureCallback failure_cb_;

    // Circuit breaker via consecutive failure count:
    //   >=0 : active, value = consecutive failures so far
    //    <0 : paused (breaker open), abs(value) = failure count at trip
    std::atomic<int32_t> consecutive_rpc_failures_{0};
};

}  // namespace mooncake
