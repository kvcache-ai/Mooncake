#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <span>
#include <vector>
#include <memory>
#include <optional>
#include <utility>
#include "replica.h"
#include "offset_allocator/offset_allocator.hpp"
#include "mutex.h"

namespace mooncake {

/**
 * @brief Memory-efficient, flat storage for P2P replica routes.
 */
struct P2PRouteData {
    struct Item {
        UUID client_id;
        UUID segment_id;
        char ip_address[48];
        uint16_t rpc_port;
        uint64_t object_size;
    };
    size_t count;
    // items follow count in memory

    [[nodiscard]] const Item* data() const {
        return reinterpret_cast<const Item*>(
            reinterpret_cast<const char*>(this) + sizeof(size_t));
    }

    [[nodiscard]] Item* data() {
        return reinterpret_cast<Item*>(reinterpret_cast<char*>(this) +
                                       sizeof(size_t));
    }

    static size_t CalculateSize(size_t replica_count) {
        // Ensure 8-byte alignment for the entire record
        return (sizeof(size_t) + replica_count * sizeof(Item) + 7) & ~7;
    }

    /**
     * @brief Serialize replica data and key into a continuous memory block.
     * @return Total bytes written.
     */
    static size_t Serialize(void* dest, const std::string& key,
                            const std::vector<P2PProxyDescriptor>& replicas);
};

/**
 * @brief High-performance read-only handle for RouteCache entries.
 */
class P2PRouteHandle {
   public:
    P2PRouteHandle() : record_(nullptr) {}
    P2PRouteHandle(
        const P2PRouteData* record,
        std::shared_ptr<offset_allocator::OffsetAllocationHandle> handle)
        : record_(record), handle_(std::move(handle)) {}

    [[nodiscard]] std::span<const P2PRouteData::Item> items() const {
        if (!record_ || record_->count == 0) return {};
        return {record_->data(), record_->count};
    }

   private:
    const P2PRouteData* record_;
    std::shared_ptr<offset_allocator::OffsetAllocationHandle> handle_;
};

/**
 * @brief Client-side route cache for P2P read operations.
 *
 * This implementation uses an Atomic Bucket Array with Pointer RCU to achieve
 * lock-free reads while maintaining consistency under concurrent writes.
 * It uses a Clock algorithm (Second Chance) for O(1) eviction via Ring Buffers.
 */
class RouteCache {
   public:
    RouteCache(size_t max_memory_bytes, uint64_t ttl_ms);
    ~RouteCache();

    /**
     * @brief Lock-free lookup (RCU).
     */
    P2PRouteHandle Get(const std::string& key);

    /**
     * @brief overwrite
     */
    void Replace(const std::string& key,
                 const std::vector<P2PProxyDescriptor>& replicas);

    /**
     * @brief update if key exists, otherwise insert
     */
    void Upsert(const std::string& key,
                const std::vector<P2PProxyDescriptor>& replicas);

    void RemoveReplica(const std::string& key,
                       const std::vector<P2PProxyDescriptor>& remove_replicas);

    struct Metrics {
        size_t free_node_count;
        size_t total_node_count;
        size_t free_memory_bytes;
        size_t total_memory_bytes;
    };

    Metrics GetMetrics() const;

   private:
    struct alignas(64) Node {
        const char* key_;
        uint32_t key_len_;
        std::atomic<bool> accessed_{false};
        std::atomic<bool> is_deleted_{false};
        std::atomic<std::shared_ptr<offset_allocator::OffsetAllocationHandle>>
            handle_;
        std::atomic<int64_t> deadline_;
        std::atomic<Node*> next_;

        // Hash of the full key for fast bucket location during eviction
        uint32_t key_hash_{0};

        Node() : key_(nullptr), key_len_(0), next_(nullptr) {}

        void Init(
            const char* key, uint32_t key_len, size_t key_hash,
            std::shared_ptr<offset_allocator::OffsetAllocationHandle> handle,
            int64_t deadline_count) {
            key_ = key;
            key_len_ = key_len;
            key_hash_ = (uint32_t)key_hash;
            handle_.store(std::move(handle));
            deadline_.store(deadline_count, std::memory_order_relaxed);
            next_.store(nullptr, std::memory_order_relaxed);
            accessed_.store(false, std::memory_order_relaxed);
            is_deleted_.store(false, std::memory_order_release);
        }

        std::shared_ptr<offset_allocator::OffsetAllocationHandle> GetHandle() {
            return handle_.load();
        }

        void MarkDeleted() {
            is_deleted_.store(true, std::memory_order_release);
        }

        bool IsDeleted() const {
            return is_deleted_.load(std::memory_order_acquire);
        }

        bool IsExpired(int64_t now_count) const {
            return now_count >= deadline_.load(std::memory_order_relaxed);
        }

        bool IsActive(int64_t now) { return !IsDeleted() && !IsExpired(now); }
    };

    struct alignas(64) Shard {
        mutable Mutex mtx_;
        std::unique_ptr<std::atomic<Node*>[]> buckets_ GUARDED_BY(mtx_);

        // Clock Eviction scanning cursor
        std::atomic<size_t> evict_cursor_{0};

        // Shard-local Node pool manager
        struct NodeResourcePool {
            std::unique_ptr<Node[]> storage_;
            Node* free_head_{nullptr};
            size_t free_count_{0};

            void Init(size_t count) {
                storage_ = std::make_unique<Node[]>(count);
                Node* prev = nullptr;
                for (size_t i = 0; i < count; ++i) {
                    storage_[i].next_.store(prev, std::memory_order_relaxed);
                    prev = &storage_[i];
                }
                free_head_ = prev;
                free_count_ = count;
            }

            Node* Pop() {
                if (!free_head_) return nullptr;
                Node* node = free_head_;
                free_head_ = node->next_.load(std::memory_order_relaxed);
                free_count_--;
                return node;
            }

            void Push(Node* node) {
                if (!node) return;
                node->next_.store(free_head_, std::memory_order_relaxed);
                free_head_ = node;
                free_count_++;
            }
        } nodes_ GUARDED_BY(mtx_);

        // RCU Garbage Collection Queue
        struct PendingDelete {
            Node* node_;
            std::chrono::steady_clock::time_point timestamp_;
        };
        std::vector<PendingDelete> pending_deletes_ GUARDED_BY(mtx_);

        std::shared_ptr<offset_allocator::OffsetAllocator> allocator_;
        void* base_addr_ = nullptr;
        size_t gc_skip_count_ = 0;

        Shard() = default;
        // Non-copyable & Non-movable (due to mutex and arrays)
        Shard(const Shard&) = delete;
        Shard& operator=(const Shard&) = delete;
        Shard(Shard&&) = delete;
        Shard& operator=(Shard&&) = delete;
    };

   private:
    void InnerPut(Shard& shard, size_t bucket_idx, size_t hash_val,
                  const std::string& key,
                  const std::vector<P2PProxyDescriptor>& replicas, bool merge);

    void BuildReplicaList(
        Node* old_node,
        const std::vector<P2PProxyDescriptor>& increment_replicas,
        const std::vector<P2PProxyDescriptor>& remove_replicas,
        std::vector<P2PProxyDescriptor>& out);

    size_t Evict(Shard& shard, size_t goal_free_count) REQUIRES(shard.mtx_);
    void GCLoop();
    void SyncGC(Shard& shard) REQUIRES(shard.mtx_);

    // Internal Helpers

    // Returns {prev, node} for a given key in the bucket.
    std::pair<Node*, Node*> findNodeInBucket(Shard& shard, size_t bucket_idx,
                                             const char* key_ptr,
                                             uint32_t key_len) REQUIRES(shard.mtx_);

    void retireNode(Shard& shard, Node* prev, Node* node, size_t bucket_idx)
        REQUIRES(shard.mtx_);

    bool acquireResource(
        Shard& shard, size_t total_size, Node** out_node,
        std::optional<offset_allocator::OffsetAllocationHandle>& out_handle)
        REQUIRES(shard.mtx_);

   private:
    // Estimation of memory cost for dynamic resource allocation
    static constexpr size_t AVG_REPLICA_COUNT = 1;
    static constexpr size_t AVG_KEY_LEN = 64;
    static constexpr size_t ENTRY_METADATA_COST =
        sizeof(Node) + sizeof(std::atomic<Node*>);
    static constexpr size_t ENTRY_DATA_COST =
        sizeof(P2PRouteData) +
        (AVG_REPLICA_COUNT * sizeof(P2PRouteData::Item)) + AVG_KEY_LEN;
    static constexpr size_t TOTAL_AVG_COST_PER_ENTRY =
        ENTRY_METADATA_COST + ENTRY_DATA_COST;

    const size_t max_memory_bytes_;
    const uint64_t ttl_ms_;

    size_t shard_count_;
    size_t nodes_per_shard_;
    size_t buckets_per_shard_;

    static constexpr int MAX_TRY_LOCK_RETRIES = 5;
    // GC Control
    /**
     * @brief RCU Grace Period (200ms).
     *
     * This delay governs the lifecycle of the `Node` structure (metadata),
     * which is reused from a fixed per-shard pool.
     *
     * TWO-STAGE PROTECTION MODEL:
     * 1. [Function Execution]: Within Get(), the Reader is protected by this
     *    Grace Period rather than mutex. It must complete before the Node is
     *    recycled to avoid logical errors (e.g., requesting Key-A but returning
     *    Key-B data).
     * 2. [Return Handle]: After Get() returns, the Caller is protected by
     *    the shared_ptr within P2PRouteHandle. Even if the Node is recycled,
     *    the underlying physical memory (P2PRouteData) remains valid.
     */
    static constexpr auto GC_RETIRE_DELAY = std::chrono::milliseconds(200);
    static constexpr size_t MAX_GC_SKIP_COUNT = 5;
    static constexpr auto GC_IDLE_SLEEP = std::chrono::milliseconds(2000);
    static constexpr auto GC_LOW_PRESSURE_SLEEP =
        std::chrono::milliseconds(300);
    static constexpr auto GC_HIGH_PRESSURE_SLEEP =
        std::chrono::milliseconds(50);
    static constexpr double LOW_WATERMARK = 0.7;
    static constexpr double HIGH_WATERMARK = 0.9;
    static constexpr double ASYNC_EVICT_PROPORTION = 0.05;
    static constexpr size_t SYNC_EVICT_BATCH_SIZE = 5;

    std::atomic<bool> stop_gc_{false};
    std::thread gc_thread_;

    void* base_all_ = nullptr;
    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace mooncake
