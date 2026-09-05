#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "offset_allocator/offset_allocator.h"
#include "replica.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/global_allocator_interface.h"
#include "types.h"

namespace mooncake {

struct DistributedStorageConfig;

class DfsGlobalAllocator final : public GlobalAllocatorInterface {
   public:
    struct EvictionCandidate {
        std::string key;
        int shard_idx;
        uint64_t offset;
    };

    // Keeps selected allocations pinned while the master decides which
    // candidates can be evicted. An unresolved transaction is aborted on
    // destruction so candidates cannot get stuck outside the LRU.
    class PendingEviction {
       public:
        PendingEviction() = default;
        ~PendingEviction();

        PendingEviction(const PendingEviction&) = delete;
        PendingEviction& operator=(const PendingEviction&) = delete;
        PendingEviction(PendingEviction&& other) noexcept;
        PendingEviction& operator=(PendingEviction&&) = delete;

        bool Empty() const { return candidates_.empty(); }
        const std::vector<EvictionCandidate>& Candidates() const {
            return candidates_;
        }

       private:
        friend class DfsGlobalAllocator;

        struct PreparedAllocation {
            EvictionCandidate candidate;
            std::shared_ptr<offset_allocator::OffsetAllocationHandle> handle;
            uint64_t bytes = 0;
        };

        explicit PendingEviction(DfsGlobalAllocator* owner) : owner_(owner) {}

        DfsGlobalAllocator* owner_ = nullptr;
        std::vector<EvictionCandidate> candidates_;
        std::vector<PreparedAllocation> prepared_;
    };

    DfsGlobalAllocator() = default;
    ~DfsGlobalAllocator() override;

    DfsGlobalAllocator(const DfsGlobalAllocator&) = delete;
    DfsGlobalAllocator& operator=(const DfsGlobalAllocator&) = delete;

    DfsAllocatorType Type() const override { return DfsAllocatorType::SHARD; }

    tl::expected<void, ErrorCode> Init(
        const DistributedStorageConfig& config) override;
    bool IsInitialized() const override {
        return initialized_.load(std::memory_order_acquire);
    }

    tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) override;
    std::vector<BatchAllocateResult> BatchAllocate(
        const std::vector<BatchAllocateRequest>& requests) override;

    void Free(const std::string& key,
              const DistributedFSDescriptor& descriptor) override;
    void UpdateAccess(
        const std::string& key,
        const DistributedFSDescriptor& descriptor) override;

    void Free(uint64_t offset, uint64_t aligned_size, int shard_idx,
              const std::string& key);
    void UpdateAccess(const std::string& key, int shard_idx, uint64_t offset);
    PendingEviction PrepareEviction();
    void CommitPreparedEviction(PendingEviction&& pending);
    void RestorePreparedEviction(PendingEviction&& pending);
    void ResolvePreparedEviction(PendingEviction&& pending,
                                 const std::vector<bool>& accepted);

    bool IsEvictionEnabled() const override { return eviction_enabled_; }
    std::chrono::seconds GetEvictionCheckInterval() const override {
        return eviction_check_interval_;
    }

    uint64_t GetTotalCapacity() const override;
    uint64_t GetUsedBytes() const override;

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
            bool eviction_prepared = false;
        };

        std::shared_mutex handle_mutex;
        std::unordered_map<uint64_t, AllocationRecord> offset_to_handle;

        std::mutex lru_mutex;
        std::list<std::pair<std::string, uint64_t>> lru_list;
        std::unordered_map<std::string, decltype(lru_list)::iterator> lru_index;
        // Once the high watermark is crossed, keep selecting candidates until
        // effective usage falls below the low watermark. Protected candidates
        // may make that span multiple prepare/resolve rounds.
        bool eviction_active = false;

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
    void PrepareEvictionFromShard(int shard_idx, PendingEviction& pending);
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
    std::atomic<bool> initialized_{false};
    std::array<std::mutex, kNumKeyStripes> key_stripes_;
};

}  // namespace mooncake
