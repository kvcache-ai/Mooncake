#include "route_cache.h"
#include <glog/logging.h>
#include <cstring>
#include <algorithm>

namespace mooncake {

size_t P2PRouteData::Serialize(
    void* dest, const std::string& key,
    const std::vector<P2PProxyDescriptor>& replicas) {
    size_t record_size = P2PRouteData::CalculateSize(replicas.size());
    char* base_ptr = reinterpret_cast<char*>(dest);
    auto* record = reinterpret_cast<P2PRouteData*>(base_ptr);
    char* key_ptr = base_ptr + record_size;

    record->count = replicas.size();
    for (size_t i = 0; i < replicas.size(); ++i) {
        const auto& src = replicas[i];
        auto& dst = record->data()[i];
        dst.client_id = src.client_id;
        dst.segment_id = src.segment_id;
        size_t copy_len =
            std::min(src.ip_address.length(), sizeof(dst.ip_address) - 1);
        std::memcpy(dst.ip_address, src.ip_address.c_str(), copy_len);
        dst.ip_address[copy_len] = '\0';
        dst.rpc_port = src.rpc_port;
        dst.object_size = src.object_size;
    }
    std::memcpy(key_ptr, key.data(), key.length());
    key_ptr[key.length()] = '\0';
    return record_size + key.length() + 1;
}

RouteCache::RouteCache(size_t max_memory_bytes, uint64_t ttl_ms)
    : max_memory_bytes_(max_memory_bytes), ttl_ms_(ttl_ms) {
    // 1. Determine shard_count (adaptive to concurrency and memory)
    size_t hw = std::thread::hardware_concurrency();
    shard_count_ = std::clamp((size_t)(hw * 4), (size_t)8, (size_t)1024);

    // If total memory is very small, reduce shard count to avoid metadata
    // overhead
    if (max_memory_bytes_ < 10 * 1024 * 1024) {
        shard_count_ = std::min(shard_count_, (size_t)8);
    }

    // 2. Determine per-shard resources based on projected entry count
    size_t total_estimated_nodes = max_memory_bytes_ / TOTAL_AVG_COST_PER_ENTRY;
    nodes_per_shard_ =
        std::max((size_t)1024, total_estimated_nodes / shard_count_);
    buckets_per_shard_ = nodes_per_shard_;  // Keep load factor = 1

    const size_t metadata_cost =
        (shard_count_ * (nodes_per_shard_ * sizeof(Node) +
                         buckets_per_shard_ * sizeof(void*)));
    LOG(INFO) << "RouteCache: Initializing with " << shard_count_ << " shards"
              << ", nodes in per shard:" << nodes_per_shard_
              << ", metadata cost:" << metadata_cost
              << ", cache data cost:" << max_memory_bytes_;

    for (size_t i = 0; i < shard_count_; ++i) {
        shards_.push_back(std::make_unique<Shard>());
    }

    base_all_ = std::malloc(max_memory_bytes_);
    if (!base_all_) {
        throw std::runtime_error(
            "Failed to allocate master memory for RouteCache");
    }

    size_t per_shard_bytes = max_memory_bytes_ / shard_count_;
    for (size_t i = 0; i < shard_count_; ++i) {
        auto& shard = *shards_[i];
        shard.base_addr_ = static_cast<char*>(base_all_) + i * per_shard_bytes;
        shard.allocator_ = offset_allocator::OffsetAllocator::create(
            reinterpret_cast<uintptr_t>(shard.base_addr_), per_shard_bytes);

        shard.buckets_ =
            std::make_unique<std::atomic<Node*>[]>(buckets_per_shard_);

        // Initialize Node memory pool
        shard.nodes_.Init(nodes_per_shard_);
    }

    // Initialize EBR reader slots
    reader_slots_ = std::make_unique<ReaderSlot[]>(MAX_READER_SLOTS);

    gc_thread_ = std::thread(&RouteCache::GCLoop, this);
}

RouteCache::~RouteCache() {
    stop_gc_.store(true);
    if (gc_thread_.joinable()) {
        gc_thread_.join();
    }

    // Mandatory cleanup of remaining pending deletes
    for (auto& shard_ptr : shards_) {
        auto& shard = *shard_ptr;
        for (auto& pd : shard.pending_deletes_) {
            if (pd.node_) pd.node_->handle_.reset();
        }
        shard.pending_deletes_.clear();
        shard.buckets_.reset();
    }

    if (base_all_) {
        std::free(base_all_);
    }
}

uint64_t RouteCache::ComputeSafeEpoch() const {
    uint64_t min_active = global_epoch_.load(std::memory_order_acquire);
    for (size_t i = 0; i < MAX_READER_SLOTS; ++i) {
        uint64_t e = reader_slots_[i].epoch.load(std::memory_order_acquire);
        if (e < min_active) {
            min_active = e;
        }
    }
    return (min_active > 0) ? min_active - 1 : 0;
}

// Lock-Free Read, EBR PROTECTION MODEL:
// 1. [Function Execution]: Within Get(), the Reader is protected by the
//    EpochGuard. Nodes will not be recycled until all readers that observed
//    them have exited their epoch.
// 2. [Return Handle]: After Get() returns, the Caller is protected by
//    the shared_ptr within P2PRouteHandle. Even if the Node is recycled, the
//    underlying P2PRouteData memory remains valid and unchanged.
P2PRouteHandle RouteCache::Get(const std::string& key)
    NO_THREAD_SAFETY_ANALYSIS {
    EpochGuard guard(this);

    size_t hash_val = std::hash<std::string>{}(key);
    auto& shard = *shards_[hash_val % shard_count_];
    size_t bucket_idx = hash_val % buckets_per_shard_;

    auto now_count =
        std::chrono::steady_clock::now().time_since_epoch().count();

    auto result =
        findNodeInBucket(shard, bucket_idx, key.data(), (uint32_t)key.length());
    Node* curr = result.second;
    if (curr && curr->IsActive(now_count)) {
        auto handle = curr->GetHandle();

        if (handle) {
            // Apply read-before-write strategy to avoid excessive cache-line
            // bouncing (false sharing) under heavy concurrent reads.
            if (!curr->accessed_.load(std::memory_order_relaxed)) {
                curr->accessed_.store(true, std::memory_order_relaxed);
            }

            if (ttl_ms_ > 0) {
                auto next_deadline_count =
                    now_count +
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::milliseconds(ttl_ms_))
                        .count();

                // Allow a drift threshold to avoid false sharing on reads (e.g.
                // 1% of TTL)
                auto drift_threshold =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::milliseconds(ttl_ms_ / 100))
                        .count();
                auto old_deadline =
                    curr->deadline_.load(std::memory_order_relaxed);
                if (next_deadline_count - old_deadline > drift_threshold) {
                    curr->deadline_.store(next_deadline_count,
                                          std::memory_order_relaxed);
                }
            }

            auto* ptr = handle->ptr();
            if (ptr) {
                return P2PRouteHandle(
                    reinterpret_cast<const P2PRouteData*>(ptr),
                    std::move(handle));
            }
        }
    }

    return P2PRouteHandle();
}

void RouteCache::Replace(const std::string& key,
                         const std::vector<P2PProxyDescriptor>& replicas)
    NO_THREAD_SAFETY_ANALYSIS {
    size_t hash_val = std::hash<std::string>{}(key);
    auto& shard = *shards_[hash_val % shard_count_];
    size_t bucket_idx = hash_val % buckets_per_shard_;

    MutexLocker lock(&shard.mtx_, false);
    int retries = 0;
    while (!lock.TryLock()) {
        if (++retries >= MAX_TRY_LOCK_RETRIES) {
            return;
        }
        MOONCAKE_CPU_RELAX();
    }

    InnerPut(shard, bucket_idx, hash_val, key, replicas, false);
}

void RouteCache::Upsert(const std::string& key,
                        const std::vector<P2PProxyDescriptor>& replicas)
    NO_THREAD_SAFETY_ANALYSIS {
    size_t hash_val = std::hash<std::string>{}(key);
    auto& shard = *shards_[hash_val % shard_count_];
    size_t bucket_idx = hash_val % buckets_per_shard_;

    MutexLocker lock(&shard.mtx_, false);
    int retries = 0;
    while (!lock.TryLock()) {
        if (++retries >= MAX_TRY_LOCK_RETRIES) {
            return;
        }
        MOONCAKE_CPU_RELAX();
    }

    InnerPut(shard, bucket_idx, hash_val, key, replicas, true);
}

void RouteCache::InnerPut(Shard& shard, size_t bucket_idx, size_t hash_val,
                          const std::string& key,
                          const std::vector<P2PProxyDescriptor>& replicas,
                          bool merge) REQUIRES(shard.mtx_) {
    // 1. Identify target node
    auto result =
        findNodeInBucket(shard, bucket_idx, key.data(), (uint32_t)key.length());
    Node* prev = result.first;
    Node* target_old_node = result.second;

    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    if (target_old_node && !target_old_node->IsActive(now)) {
        retireNode(shard, prev, target_old_node, bucket_idx);
        target_old_node = nullptr;
    }

    // 2. Prepare final replica list
    std::vector<P2PProxyDescriptor> final_replicas;
    if (merge && target_old_node) {
        BuildReplicaList(target_old_node, replicas, {}, final_replicas);
    } else {
        final_replicas = replicas;
    }

    size_t record_size = P2PRouteData::CalculateSize(final_replicas.size());
    size_t total_alloc_size = record_size + key.length() + 1;

    // 3. Acquire Resources
    Node* new_node = nullptr;
    std::optional<offset_allocator::OffsetAllocationHandle> handle_opt;
    if (!acquireResource(shard, total_alloc_size, &new_node, handle_opt)) {
        return;
    }

    // 4. Serialize Data
    P2PRouteData::Serialize(handle_opt->ptr(), key, final_replicas);
    char* key_ptr = reinterpret_cast<char*>(handle_opt->ptr()) + record_size;

    auto handle = std::make_shared<offset_allocator::OffsetAllocationHandle>(
        std::move(*handle_opt));
    auto deadline_count =
        now + std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::milliseconds(ttl_ms_))
                  .count();

    new_node->Init(key_ptr, (uint32_t)key.length(), hash_val, handle,
                   deadline_count);

    // 5. Commit Update and Retire Old Node
    if (target_old_node) {
        retireNode(shard, prev, target_old_node, bucket_idx);
    }

    Node* current_head =
        shard.buckets_[bucket_idx].load(std::memory_order_relaxed);
    new_node->next_.store(current_head, std::memory_order_relaxed);
    shard.buckets_[bucket_idx].store(new_node, std::memory_order_release);
}

void RouteCache::RemoveReplica(const std::string& key,
                               const std::vector<P2PProxyDescriptor>&
                                   remove_replicas) NO_THREAD_SAFETY_ANALYSIS {
    size_t hash_val = std::hash<std::string>{}(key);
    auto& shard = *shards_[hash_val % shard_count_];
    size_t bucket_idx = hash_val % buckets_per_shard_;
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();

    MutexLocker lock(&shard.mtx_, false);

    if (!lock.TryLock()) {
        // the EpochGuard ensures that the node's key_ and key_len_ won't be
        // cleared by SyncGC while we traverse the bucket chain.
        EpochGuard guard(this);

        auto result = findNodeInBucket(shard, bucket_idx, key.data(),
                                       (uint32_t)key.length());
        Node* curr = result.second;
        if (curr && curr->IsActive(now)) {
            curr->MarkDeleted();
        }
        return;
    }

    auto result =
        findNodeInBucket(shard, bucket_idx, key.data(), (uint32_t)key.length());
    Node* prev = result.first;
    Node* curr = result.second;

    if (!curr || !curr->IsActive(now)) {
        if (curr) retireNode(shard, prev, curr, bucket_idx);
        return;
    }

    std::vector<P2PProxyDescriptor> remaining;
    BuildReplicaList(curr, {}, remove_replicas, remaining);

    if (auto handle = curr->GetHandle()) {
        const auto* old_rec =
            reinterpret_cast<const P2PRouteData*>(handle->ptr());
        if (remaining.size() == old_rec->count) return;
    }

    if (remaining.empty()) {
        retireNode(shard, prev, curr, bucket_idx);
    } else {
        // Replace with new record containing remaining replicas
        InnerPut(shard, bucket_idx, hash_val, key, remaining, false);
    }
}

void RouteCache::BuildReplicaList(
    Node* old_node, const std::vector<P2PProxyDescriptor>& increment_replicas,
    const std::vector<P2PProxyDescriptor>& remove_replicas,
    std::vector<P2PProxyDescriptor>& out) {
    // Note: old_node might be marked as deleted soon after this method.
    // Ensure the data copy from old record is performed efficiently.
    out = increment_replicas;
    auto handle = old_node->handle_;
    if (!old_node || !handle) return;

    const auto* old_rec = reinterpret_cast<const P2PRouteData*>(handle->ptr());
    for (size_t i = 0; i < old_rec->count; ++i) {
        const auto& old_item = old_rec->data()[i];

        // 1. Check if in remove_replicas
        bool should_exclude = false;
        for (const auto& ex : remove_replicas) {
            if (old_item.client_id == ex.client_id &&
                old_item.segment_id == ex.segment_id) {
                should_exclude = true;
                break;
            }
        }
        if (should_exclude) continue;

        // 2. Check if already in increment_replicas (deduplication)
        bool exist = false;
        for (const auto& r : increment_replicas) {
            if (r.client_id == old_item.client_id &&
                r.segment_id == old_item.segment_id) {
                exist = true;
                break;
            }
        }
        if (!exist) {
            P2PProxyDescriptor p;
            p.client_id = old_item.client_id;
            p.segment_id = old_item.segment_id;
            p.ip_address = old_item.ip_address;
            p.rpc_port = old_item.rpc_port;
            p.object_size = old_item.object_size;
            out.push_back(p);
        }
    }
}

// Returns {prev, node} for a given key in the bucket.
// Performance-critical: Keep the chain short and avoid complex comparisons.
std::pair<RouteCache::Node*, RouteCache::Node*> RouteCache::findNodeInBucket(
    Shard& shard, size_t bucket_idx, const char* key_ptr, uint32_t key_len)
    REQUIRES(shard.mtx_) {
    Node* prev = nullptr;
    Node* curr = shard.buckets_[bucket_idx].load(std::memory_order_acquire);
    while (curr) {
        if (curr->key_len_ == key_len &&
            std::memcmp(curr->key_, key_ptr, key_len) == 0) {
            return {prev, curr};
        }
        prev = curr;
        curr = curr->next_.load(std::memory_order_acquire);
    }
    return {nullptr, nullptr};
}

void RouteCache::retireNode(Shard& shard, Node* prev, Node* node,
                            size_t bucket_idx) REQUIRES(shard.mtx_) {
    if (!node) return;

    // 1. Unlink from bucket chain
    if (bucket_idx != (size_t)-1) {
        if (prev) {
            if (prev->next_.load(std::memory_order_acquire) == node) {
                prev->next_.store(node->next_.load(std::memory_order_relaxed),
                                  std::memory_order_release);
            }
        } else {
            if (shard.buckets_[bucket_idx].load(std::memory_order_acquire) ==
                node) {
                shard.buckets_[bucket_idx].store(
                    node->next_.load(std::memory_order_relaxed),
                    std::memory_order_release);
            }
        }
    }

    // 2. Mark deleted and enqueue for EBR reclamation
    node->MarkDeleted();
    uint64_t current_epoch = global_epoch_.load(std::memory_order_acquire);
    shard.pending_deletes_.push_back({node, current_epoch});
}

bool RouteCache::acquireResource(
    Shard& shard, size_t total_size, Node** out_node,
    std::optional<offset_allocator::OffsetAllocationHandle>& out_handle)
    REQUIRES(shard.mtx_) {
    auto try_once = [&]() -> bool {
        Node* node = shard.nodes_.Pop();
        if (!node) return false;

        auto handle_opt = shard.allocator_->allocate(total_size);
        if (!handle_opt) {
            shard.nodes_.Push(node);
            return false;
        }
        *out_node = node;
        out_handle = std::move(handle_opt);
        return true;
    };

    if (try_once()) return true;
    Evict(shard, SYNC_EVICT_BATCH_SIZE);
    SyncGC(shard);
    return try_once();
}

size_t RouteCache::Evict(RouteCache::Shard& shard, size_t goal_free_count)
    REQUIRES(shard.mtx_) {
    size_t evicted_total = 0;
    const int max_passes = 2;
    const int checks_per_pass = (int)nodes_per_shard_;

    auto now = std::chrono::steady_clock::now().time_since_epoch().count();

    // Loop until we reach the goal or we exhaust search passes
    for (int pass = 0;
         pass < max_passes && shard.nodes_.free_count_ < goal_free_count;
         ++pass) {
        for (int i = 0;
             i < checks_per_pass && shard.nodes_.free_count_ < goal_free_count;
             ++i) {
            size_t pos =
                shard.evict_cursor_.fetch_add(1, std::memory_order_relaxed) %
                nodes_per_shard_;
            Node* node = &shard.nodes_.storage_[pos];

            // If it's not yet allocated (no key), skip
            if (!node->key_) continue;

            bool is_deleted = node->IsDeleted();

            if (!is_deleted) {
                if (pass == 0 && node->IsActive(now) &&
                    node->accessed_.load(std::memory_order_relaxed)) {
                    node->accessed_.store(false, std::memory_order_relaxed);
                    continue;  // Give second chance
                }
            }

            // At this point either we should evict it (LRU) or it was already
            // marked deleted (e.g. via lock-free fallback) but not unlinked.
            size_t b_idx = node->key_hash_ % buckets_per_shard_;
            auto result = findNodeInBucket(shard, b_idx, node->key_,
                                           (uint32_t)node->key_len_);

            // Only retire it if we successfully found it still in the bucket
            // chain. If it's missing, another thread already unlinked and
            // retired it.
            if (result.second == node) {
                // retireNode handles bucket unlinking and pending_deletes list.
                retireNode(shard, result.first, node, b_idx);
                if (!is_deleted) {
                    evicted_total++;
                }
            }
        }
    }
    return evicted_total;
}

// ============================================================================
// Epoch-Based Reclamation (EBR) — How the three roles interact
//
// [Reader] (Get / RemoveReplica lock-free path)
//   On entry, snapshots global_epoch_ into reader_slots_[slot] via EpochGuard.
//   On exit, resets slot to INACTIVE. While a slot holds epoch=E, the reader
//   may be traversing bucket chains and holding pointers to nodes that were
//   visible at epoch E.
//
// [Writer] (Replace / Upsert / RemoveReplica under shard mutex)
//   Unlinks the old node from the bucket chain, then calls retireNode() which
//   records {node, global_epoch_} into pending_deletes_. The retire_epoch
//   means: "this node left the bucket chain at epoch N."
//
// [GC thread] (this function → SyncGC → ComputeSafeEpoch)
//   Each iteration advances global_epoch_ by 1, then for each shard:
//   1. SyncGC: calls ComputeSafeEpoch() which scans all reader slots to find
//      min_active (the oldest epoch held by any active reader). Nodes with
//      retire_epoch < min_active are safe to reclaim — all active readers
//      entered at or after min_active, by which time these nodes were already
//      off the bucket chain and unreachable.
//   2. Watermark-based Clock eviction to keep memory usage healthy.
//
//   A freshly retired node needs at least 2 GC rounds to be reclaimed:
//   retired at epoch=N → GC advances to N+1 → next round confirms all
//   readers have left epoch N → reclaim.
// ============================================================================
void RouteCache::GCLoop() NO_THREAD_SAFETY_ANALYSIS {
    auto sleep_duration = GC_HIGH_PRESSURE_SLEEP;
    while (!stop_gc_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(sleep_duration);

        global_epoch_.fetch_add(1, std::memory_order_release);

        double max_usage = 0.0;
        for (auto& shard_ptr : shards_) {
            auto& shard = *shard_ptr;
            MutexLocker lock(&shard.mtx_, false);
            if (shard.gc_skip_count_ >= MAX_GC_SKIP_COUNT) {
                lock.lock();
            } else {
                if (!lock.TryLock()) {
                    shard.gc_skip_count_++;
                    continue;
                }
            }
            shard.gc_skip_count_ = 0;

            // 1. EBR-based cleanup of pending deletes
            SyncGC(shard);

            // 2. Double-watermark Check
            auto metrics = shard.allocator_->get_metrics();
            double node_usage =
                1.0 - (double)shard.nodes_.free_count_ / nodes_per_shard_;
            double mem_usage =
                1.0 - (double)metrics.total_free_space_ / metrics.capacity;
            double usage = std::max(node_usage, mem_usage);

            max_usage = std::max(max_usage, usage);

            if (usage >= HIGH_WATERMARK) {
                // Critical: Evict until back to healthy LOW_WATERMARK level
                size_t goal_free = nodes_per_shard_ * (1.0 - LOW_WATERMARK);
                Evict(shard, goal_free);
            } else if (usage >= LOW_WATERMARK) {
                // Preventive: Evict 5% to keep some buffer
                size_t goal_free =
                    shard.nodes_.free_count_ +
                    (size_t)(nodes_per_shard_ * ASYNC_EVICT_PROPORTION);
                Evict(shard, goal_free);
            }
        }

        if (max_usage >= HIGH_WATERMARK) {
            sleep_duration = GC_HIGH_PRESSURE_SLEEP;
        } else if (max_usage >= LOW_WATERMARK) {
            sleep_duration = GC_LOW_PRESSURE_SLEEP;
        } else {
            sleep_duration = GC_IDLE_SLEEP;
        }
    }
}

void RouteCache::SyncGC(Shard& shard) REQUIRES(shard.mtx_) {
    uint64_t safe_epoch = ComputeSafeEpoch();
    std::vector<Node*> to_retire;

    auto it = std::remove_if(shard.pending_deletes_.begin(),
                             shard.pending_deletes_.end(),
                             [&](const Shard::PendingDelete& pd) {
                                 if (pd.retire_epoch_ <= safe_epoch) {
                                     to_retire.push_back(pd.node_);
                                     return true;
                                 }
                                 return false;
                             });
    shard.pending_deletes_.erase(it, shard.pending_deletes_.end());

    for (auto* n : to_retire) {
        n->handle_.reset();
        n->key_ = nullptr;
        shard.nodes_.Push(n);
    }
}

RouteCache::Metrics RouteCache::GetMetrics() const NO_THREAD_SAFETY_ANALYSIS {
    Metrics m{0, 0, 0, 0};
    m.total_node_count = nodes_per_shard_ * shard_count_;
    m.total_memory_bytes = max_memory_bytes_;

    for (const auto& shard_ptr : shards_) {
        auto& shard = *shard_ptr;
        MutexLocker lock(&shard.mtx_);
        m.free_node_count += shard.nodes_.free_count_;
        auto alloc_metrics = shard.allocator_->get_metrics();
        m.free_memory_bytes += alloc_metrics.total_free_space_;
    }
    return m;
}

}  // namespace mooncake
