#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "offset_allocator/offset_allocator.h"
#include "replica.h"
#include "storage/distributed/fs_adapter.h"
#include "types.h"

namespace mooncake {

struct DistributedStorageConfig;

class DfsGlobalAllocator {
   public:
    struct EvictionCandidate {
        std::string key;
        int shard_idx;
        uint64_t offset;
    };

    struct RecoveryReference {
        std::string key;
        DistributedFSDescriptor descriptor;
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
    ~DfsGlobalAllocator();

    DfsGlobalAllocator(const DfsGlobalAllocator&) = delete;
    DfsGlobalAllocator& operator=(const DfsGlobalAllocator&) = delete;

    // defer_recovery keeps allocations sealed until CompleteRecovery() has
    // reconciled the allocator sidecar with Master metadata.
    tl::expected<void, ErrorCode> Init(const DistributedStorageConfig& config,
                                       bool defer_recovery = false);
    bool IsInitialized() const {
        return initialized_.load(std::memory_order_acquire);
    }
    bool IsRecoveryReady() const {
        return recovery_ready_.load(std::memory_order_acquire);
    }

    tl::expected<int, ErrorCode> ExpandShards(int target_count);
    int GetShardCount() const;

    tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size);
    void Free(uint64_t offset, uint64_t aligned_size, int shard_idx,
              const std::string& key);
    void UpdateAccess(const std::string& key, int shard_idx, uint64_t offset);

    tl::expected<void, ErrorCode> ValidateAllocation(
        const std::string& key,
        const DistributedFSDescriptor& descriptor) const;
    tl::expected<void, ErrorCode> CompleteRecovery(
        const std::vector<RecoveryReference>& references);
    tl::expected<void, ErrorCode> RunMaintenance();
    tl::expected<void, ErrorCode> Checkpoint();

    PendingEviction PrepareEviction();
    void CommitPreparedEviction(PendingEviction&& pending);
    void RestorePreparedEviction(PendingEviction&& pending);
    void ResolvePreparedEviction(PendingEviction&& pending,
                                 const std::vector<bool>& accepted);

    bool IsEvictionEnabled() const { return eviction_enabled_; }
    std::chrono::seconds GetEvictionCheckInterval() const {
        return eviction_check_interval_;
    }

    static std::string FormatShardIdx(int idx, int shard_count);

   private:
    using OffsetAllocator = offset_allocator::OffsetAllocator;
    using OffsetAllocationHandle = offset_allocator::OffsetAllocationHandle;

    struct ShardState {
        std::string path;
        uint64_t capacity = 0;
        std::shared_ptr<OffsetAllocator> allocator;

        struct AllocationRecord {
            std::string key;
            std::shared_ptr<OffsetAllocationHandle> handle;
            uint64_t raw_offset = 0;
            uint64_t object_size = 0;
            uint64_t aligned_size = 0;
            uint64_t requested_size = 0;
            uint64_t bytes = 0;
            bool eviction_prepared = false;
        };

        struct PendingFree {
            std::shared_ptr<OffsetAllocationHandle> handle;
            uint64_t raw_offset = 0;
            uint64_t requested_size = 0;
            uint64_t bytes = 0;
            std::chrono::steady_clock::time_point when;
        };

        mutable std::mutex mutex;
        std::unordered_map<uint64_t, AllocationRecord> offset_to_handle;
        std::deque<PendingFree> pending_free;
        uint64_t pending_free_bytes = 0;

        std::list<std::pair<std::string, uint64_t>> lru_list;
        std::unordered_map<std::string, decltype(lru_list)::iterator> lru_index;
        bool eviction_active = false;

        uint64_t generation = 0;
        uint64_t base_sequence = 0;
        uint64_t next_sequence = 1;
        uint64_t wal_bytes = 0;
        bool poisoned = false;
        std::chrono::steady_clock::time_point last_checkpoint;
    };

    static constexpr size_t kNumKeyStripes = 65536;

    std::unique_lock<std::mutex> LockKey(const std::string& key) {
        return std::unique_lock<std::mutex>(
            key_stripes_[std::hash<std::string>{}(key) % kNumKeyStripes]);
    }

    tl::expected<void, ErrorCode> CreateFreshShard(ShardState& shard,
                                                   int shard_idx);
    tl::expected<void, ErrorCode> LoadShard(ShardState& shard, int shard_idx);
    tl::expected<void, ErrorCode> SaveCheckpointLocked(ShardState& shard,
                                                       int shard_idx,
                                                       uint64_t generation,
                                                       uint64_t base_sequence);
    tl::expected<void, ErrorCode> CompactShardLocked(ShardState& shard,
                                                     int shard_idx);
    tl::expected<void, ErrorCode> LoadCheckpointLocked(ShardState& shard,
                                                       int shard_idx);
    tl::expected<void, ErrorCode> LoadWalLocked(ShardState& shard,
                                                int shard_idx);
    tl::expected<void, ErrorCode> AppendAllocationLocked(
        ShardState& shard, int shard_idx,
        const ShardState::AllocationRecord& record, uint64_t exposed_offset);
    tl::expected<void, ErrorCode> AppendReleaseLocked(
        ShardState& shard, int shard_idx,
        const std::vector<uint64_t>& raw_offsets);

    void QueuePendingFreeLocked(
        ShardState& shard,
        const std::shared_ptr<OffsetAllocationHandle>& handle,
        uint64_t raw_offset, uint64_t requested_size, uint64_t bytes,
        std::chrono::steady_clock::time_point when);
    tl::expected<void, ErrorCode> CleanupExpiredPendingFreesLocked(
        ShardState& shard, int shard_idx,
        std::chrono::steady_clock::time_point now);
    double EffectiveUsageLocked(const ShardState& shard) const;
    void PrepareEvictionFromShard(ShardState& shard, int shard_idx,
                                  PendingEviction& pending);

    std::string ShardDataPath(const ShardState& shard) const;
    std::string ShardMetadataPath(const ShardState& shard) const;
    std::string ShardWalPath(const ShardState& shard) const;
    std::string ConfigurationFingerprint(const ShardState& shard) const;
    uint64_t AlignSize(uint64_t size) const;

    std::string mount_path_;
    std::string fs_adapter_type_;
    std::string metadata_namespace_;
    uint64_t shard_capacity_ = 0;
    uint64_t alignment_ = 4096;
    using ShardList = std::vector<std::shared_ptr<ShardState>>;
    std::shared_ptr<const ShardList> shards_;
    std::mutex expansion_mutex_;
    tl::expected<std::shared_ptr<ShardState>, ErrorCode> CreateShard(
        int shard_idx, const std::string& path,
        std::vector<std::string>& created_files);
    void CleanupCreatedFiles(const std::vector<std::string>& created_files);
    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    bool eviction_enabled_ = true;
    double eviction_high_watermark_ = 0.9;
    double eviction_low_watermark_ = 0.7;
    std::chrono::seconds deferred_free_duration_{30};
    std::chrono::seconds eviction_check_interval_{5};
    std::chrono::seconds metadata_checkpoint_interval_{300};
    uint64_t metadata_wal_compaction_threshold_bytes_ = 0;
    std::atomic<bool> initialized_{false};
    std::atomic<bool> recovery_ready_{false};
    std::array<std::mutex, kNumKeyStripes> key_stripes_;
};

}  // namespace mooncake
