#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "offset_allocator/offset_allocator.h"
#include "replica.h"
#include "storage/distributed/fs_adapter.h"
#include "types.h"

namespace mooncake {

class DfsGlobalAllocator {
   public:
    struct EvictedKey {
        std::string key;
        int shard_idx;
        uint64_t offset;
    };

    DfsGlobalAllocator() = default;
    ~DfsGlobalAllocator();

    DfsGlobalAllocator(const DfsGlobalAllocator&) = delete;
    DfsGlobalAllocator& operator=(const DfsGlobalAllocator&) = delete;

    bool Init(const std::string& mount_path, int shard_count,
              uint64_t shard_capacity, uint64_t alignment);
    bool IsInitialized() const { return initialized_; }

    tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size);
    void Free(uint64_t offset, uint64_t aligned_size, int shard_idx,
              const std::string& key);
    void UpdateAccess(const std::string& key, int shard_idx, uint64_t offset);
    std::vector<EvictedKey> EvictIfNeeded();
    void SetEvictCallback(
        std::function<void(const std::string&, int, uint64_t)> cb);

    static std::string FormatShardIdx(int idx, int shard_count);

   private:
    using OffsetAllocator = offset_allocator::OffsetAllocator;
    using OffsetAllocationHandle = offset_allocator::OffsetAllocationHandle;

    struct ShardState {
        uint64_t capacity = 0;
        std::shared_ptr<OffsetAllocator> allocator;

        struct AllocationRecord {
            std::string key;
            std::shared_ptr<OffsetAllocationHandle> handle;
            uint64_t bytes = 0;
        };

        std::shared_mutex handle_mutex;
        std::unordered_map<uint64_t, AllocationRecord> offset_to_handle;

        std::mutex lru_mutex;
        std::list<std::pair<std::string, uint64_t>> lru_list;
        std::unordered_map<std::string, decltype(lru_list)::iterator> lru_index;

        struct PendingFree {
            std::shared_ptr<OffsetAllocationHandle> handle;
            uint64_t bytes = 0;
            std::chrono::steady_clock::time_point when;
        };
        std::mutex pending_mutex;
        std::deque<PendingFree> pending_free;
        uint64_t pending_free_bytes = 0;
    };

    static constexpr size_t kNumKeyStripes = 65536;

    std::unique_lock<std::mutex> LockKey(const std::string& key) {
        return std::unique_lock<std::mutex>(
            key_stripes_[std::hash<std::string>{}(key) % kNumKeyStripes]);
    }

    void ProcessPendingFrees(int shard_idx);
    void QueuePendingFree(ShardState& shard,
                          const std::shared_ptr<OffsetAllocationHandle>& handle,
                          uint64_t bytes,
                          std::chrono::steady_clock::time_point when);
    void CleanupExpiredPendingFrees(ShardState& shard,
                                    std::chrono::steady_clock::time_point now);
    double EffectiveUsage(ShardState& shard);
    std::vector<EvictedKey> EvictFromShard(int shard_idx);
    void EvictionMonitor();
    int SelectShard(const std::string& key) const;
    uint64_t AlignSize(uint64_t size) const;

    std::string mount_path_;
    int shard_count_ = 0;
    uint64_t alignment_ = 4096;
    std::vector<std::unique_ptr<ShardState>> shards_;
    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    bool eviction_enabled_ = true;
    double eviction_high_watermark_ = 0.9;
    double eviction_low_watermark_ = 0.7;
    std::chrono::seconds deferred_free_duration_{30};
    std::chrono::seconds eviction_check_interval_{5};
    std::thread eviction_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> initialized_{false};
    std::function<void(const std::string&, int, uint64_t)> on_evict_callback_;
    std::array<std::mutex, kNumKeyStripes> key_stripes_;
};

}  // namespace mooncake
