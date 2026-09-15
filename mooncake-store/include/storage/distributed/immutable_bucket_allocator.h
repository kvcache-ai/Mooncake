#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/global_allocator_interface.h"

namespace mooncake {

struct DistributedStorageConfig;

enum class BucketEntryState : int32_t {
    PENDING = 0,    // Reserved while the client write is in progress.
    COMMITTED = 1,  // Finalized and eligible for whole-bucket eviction.
    TOMBSTONE = 2,  // Released without making its append range reusable.
};

enum class BucketLifecycle : int32_t {
    ACTIVE = 0,    // Current append target.
    SEALED = 1,    // Closed to appends and eligible for LRU selection.
    FROZEN = 2,    // Reserved by the validation phase of eviction.
    EVICTING = 3,  // Being deleted or waiting for deletion retry.
    RETIRED = 4,   // Deleted and ready to leave runtime state.
};

/**
 * Runtime-only append allocator for immutable DFS bucket files.
 *
 * No allocator metadata is persisted. Init therefore rejects a DFS root that
 * already contains a bucket data or metadata file instead of guessing whether
 * it belongs to this process. MarkCommitted records only runtime state.
 */
class ImmutableBucketAllocator final : public GlobalAllocatorInterface {
   public:
    ImmutableBucketAllocator() = default;
    ~ImmutableBucketAllocator() override;

    ImmutableBucketAllocator(const ImmutableBucketAllocator&) = delete;
    ImmutableBucketAllocator& operator=(const ImmutableBucketAllocator&) =
        delete;

    DfsAllocatorType Type() const override { return DfsAllocatorType::BUCKET; }
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
    void UpdateAccess(const std::string& key,
                      const DistributedFSDescriptor& descriptor) override;

    bool IsEvictionEnabled() const override { return eviction_enabled_; }
    std::chrono::seconds GetEvictionCheckInterval() const override {
        return eviction_check_interval_;
    }
    uint64_t GetUsedBytes() const override;
    uint64_t GetTotalCapacity() const override;

    bool MarkCommitted(const std::string& key,
                       const DistributedFSDescriptor& descriptor);

    class PendingEviction {
       public:
        PendingEviction() = default;
        ~PendingEviction();
        PendingEviction(const PendingEviction&) = delete;
        PendingEviction& operator=(const PendingEviction&) = delete;
        PendingEviction(PendingEviction&& other) noexcept;
        PendingEviction& operator=(PendingEviction&& other) noexcept;

        bool Empty() const { return owner_ == nullptr; }
        int64_t bucket_id() const { return bucket_id_; }
        const std::vector<EvictionCandidate>& Candidates() const {
            return candidates_;
        }

       private:
        friend class ImmutableBucketAllocator;
        ImmutableBucketAllocator* owner_ = nullptr;
        int64_t bucket_id_ = -1;
        std::shared_ptr<void> identity_;
        std::vector<EvictionCandidate> candidates_;
    };

    PendingEviction PrepareEviction();
    PendingEviction PrepareEvictionForAllocationFailure();
    tl::expected<void, ErrorCode> CommitEviction(PendingEviction&& pending);
    void AbortEviction(PendingEviction&& pending);
    size_t RetryFailedEvictions();

    size_t GetBucketCount() const;
    std::optional<int64_t> GetBucketIdForKey(const std::string& key) const;
    static std::string FormatBucketId(int64_t bucket_id);

   private:
    friend class PendingEviction;

    struct BucketEntry {
        BucketEntryLayout layout;
        BucketEntryState state = BucketEntryState::PENDING;
    };

    struct BucketState {
        int64_t id = -1;
        uint64_t capacity = 0;
        uint64_t append_offset = 0;
        uint64_t live_bytes = 0;
        uint64_t pending_entries = 0;
        BucketLifecycle lifecycle = BucketLifecycle::ACTIVE;
        std::unordered_map<std::string, BucketEntry> entries;
    };
    using BucketPtr = std::shared_ptr<BucketState>;

    std::string BucketDataPath(int64_t bucket_id) const;
    tl::expected<BucketPtr, ErrorCode> EnsureActiveBucketLocked(
        uint64_t required);
    tl::expected<DistributedFSDescriptor, ErrorCode> ReserveInBucketLocked(
        BucketState& bucket, const std::string& key, uint64_t size);
    BucketEntry* FindMatchingEntryLocked(
        const std::string& key, const DistributedFSDescriptor& descriptor,
        BucketPtr* bucket);
    void TombstoneLocked(const std::string& key, BucketState& bucket,
                         BucketEntry& entry);
    void TouchLruLocked(int64_t bucket_id);
    void AbortEvictionLocked(PendingEviction& pending, bool warm);
    PendingEviction PrepareEvictionLocked(bool force_one);
    uint64_t UsedBytesLocked() const;

    std::string fsdir_;
    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    uint64_t bucket_capacity_ = 0;
    uint64_t alignment_ = 4096;
    int64_t max_bucket_count_ = 0;
    bool eviction_enabled_ = true;
    double eviction_high_watermark_ = 0.9;
    double eviction_low_watermark_ = 0.7;
    std::chrono::seconds eviction_check_interval_{5};

    mutable std::mutex mutex_;
    std::unordered_map<int64_t, BucketPtr> buckets_;
    std::unordered_map<std::string, int64_t> key_index_;
    std::list<int64_t> lru_;
    std::unordered_map<int64_t, std::list<int64_t>::iterator> lru_index_;
    std::unordered_set<int64_t> failed_deletions_;
    int64_t active_bucket_id_ = -1;
    int64_t next_bucket_id_ = 0;
    bool eviction_active_ = false;
    std::atomic<bool> initialized_{false};
};

}  // namespace mooncake
