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
    std::string_view key, const UUID& segment_id, ErrorCode error)>;

class AsyncMetadataNotifier {
   public:
    AsyncMetadataNotifier(P2PMasterClient& master_client, const UUID& client_id,
                          size_t sender_thread_count, size_t max_batch_size,
                          size_t queue_capacity,
                          SyncFailureCallback failure_cb = nullptr);
    ~AsyncMetadataNotifier();

    AsyncMetadataNotifier(const AsyncMetadataNotifier&) = delete;
    AsyncMetadataNotifier& operator=(const AsyncMetadataNotifier&) = delete;

    // Start/Stop can be called alternately.
    // drop_pending=false (default): sender drains remaining ops before exiting.
    // drop_pending=true: queued ops are discarded immediately without invoking
    // failure_cb.
    void Start();
    void Stop(bool drop_pending = false);

    // --- Normal priority ---
    tl::expected<void, ErrorCode> EnqueueAdd(std::string_view key,
                                             const UUID& segment_id,
                                             size_t size);

    tl::expected<void, ErrorCode> EnqueueRemove(std::string_view key,
                                                const UUID& segment_id);

    // --- Recovery priority (lower send priority) ---
    tl::expected<void, ErrorCode> EnqueueRecoveryAdd(std::string_view key,
                                                     const UUID& segment_id,
                                                     size_t size);

    /**
     * @brief Wait until all recovery ops have been sent.
     * Only valid after the caller has finished enqueueing all recovery ops.
     * @return true if drained, false if aborted or timed out.
     */
    bool WaitForRecoveryDrain(const std::function<bool()>& abort_fn,
                              std::chrono::milliseconds timeout);

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
        bool is_recovery = false;  // which list this slot belongs to
    };

    struct SenderShard {
       public:
        SenderShard() = default;
        SenderShard(const SenderShard&) = delete;
        SenderShard& operator=(const SenderShard&) = delete;
        SenderShard(SenderShard&&) = delete;
        SenderShard& operator=(SenderShard&&) = delete;

        bool IsFull() const { return free_stack.empty(); }
        bool IsFullForRecovery() const {
            return free_stack.size() <= normal_reserved;
        }
        bool IsEmpty() const {
            return normal_count == 0 && recovery_count == 0;
        }

        size_t AllocSlot() {
            size_t idx = free_stack.back();
            free_stack.pop_back();
            return idx;
        }

        void FreeSlot(size_t idx) {
            slots[idx].prev = InvalidIdx;
            slots[idx].next = InvalidIdx;
            slots[idx].is_recovery = false;
            free_stack.push_back(idx);
        }

        // Link to the tail of the appropriate list based on is_recovery flag.
        void LinkTail(size_t idx, bool is_recovery) {
            auto& head = is_recovery ? recovery_head : normal_head;
            auto& tail = is_recovery ? recovery_tail : normal_tail;
            auto& count = is_recovery ? recovery_count : normal_count;

            slots[idx].prev = tail;
            slots[idx].next = InvalidIdx;
            slots[idx].is_recovery = is_recovery;
            if (tail != InvalidIdx) {
                slots[tail].next = idx;
            } else {
                head = idx;
            }
            tail = idx;
            count++;
        }

        // Unlink from whichever list the slot belongs to.
        void Unlink(size_t idx) {
            auto& slot = slots[idx];
            auto& head = slot.is_recovery ? recovery_head : normal_head;
            auto& tail = slot.is_recovery ? recovery_tail : normal_tail;
            auto& count = slot.is_recovery ? recovery_count : normal_count;

            if (slot.prev != InvalidIdx) {
                slots[slot.prev].next = slot.next;
            } else {
                head = slot.next;
            }
            if (slot.next != InvalidIdx) {
                slots[slot.next].prev = slot.prev;
            } else {
                tail = slot.prev;
            }
            count--;
        }

       public:
        std::mutex mutex;
        std::condition_variable sender_cv;    // sender waits when both empty
        std::condition_variable producer_cv;  // normal producer waits when full
        std::condition_variable recovery_drain_cv;  // WaitForRecoveryDrain

        // Pre-allocated slot pool — shared by both queues
        std::vector<Slot> slots;
        std::vector<size_t> free_stack;
        size_t capacity = 0;
        size_t normal_reserved = 0;  // slots reserved for normal ops

        // Normal priority linked list
        size_t normal_head = InvalidIdx;
        size_t normal_tail = InvalidIdx;
        size_t normal_count = 0;

        // Recovery (low priority) linked list
        size_t recovery_head = InvalidIdx;
        size_t recovery_tail = InvalidIdx;
        size_t recovery_count = 0;

        // Shared coalesce index (covers both queues)
        std::unordered_map<CoalesceKey, CoalesceEntry, CoalesceKeyHash>
            coalesce_index;

        // Recovery ops that have been dequeued by CollectBatch but whose
        // SendBatch has not yet completed. WaitForRecoveryDrain waits for
        // both recovery_count and recovery_in_flight to reach zero.
        size_t recovery_in_flight = 0;

        std::thread sender_thread;
    };

   private:
    tl::expected<void, ErrorCode> DoEnqueue(PendingOp&& op, bool is_recovery);

    void SenderLoop(size_t shard_idx);
    // Returns (total_collected, recovery_collected)
    std::pair<size_t, size_t> CollectBatch(SenderShard& shard,
                                           std::vector<PendingOp>& batch_out);
    size_t CollectFromList(SenderShard& shard,
                           std::vector<PendingOp>& batch_out, size_t offset,
                           size_t max_count, bool from_recovery);
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

    // Set to true by Stop(drop_pending=true) before waking senders.
    // Tells CollectBatch to return 0 immediately so senders exit after their
    // current in-flight SendBatch without processing any further queued ops.
    std::atomic<bool> drop_on_stop_{false};

    // Circuit breaker via consecutive failure count:
    //   >=0 : active, value = consecutive failures so far
    //    <0 : paused (breaker open), abs(value) = failure count at trip
    std::atomic<int32_t> consecutive_rpc_failures_{0};
};

}  // namespace mooncake
