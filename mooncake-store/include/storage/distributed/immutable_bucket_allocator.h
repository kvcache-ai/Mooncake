#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/global_allocator_interface.h"
#include "types.h"

namespace mooncake {

struct DistributedStorageConfig;

/**
 * @brief Persisted state of one entry inside a bucket.
 *
 * `entry_offset` is the aligned start of the entry, not the value offset; the
 * value offset is always derived via BucketEntryLayout so there is a single
 * definition of the layout.
 */
struct PersistedBucketEntry {
    std::string key;
    uint64_t entry_offset = 0;
    uint64_t key_size = 0;
    uint64_t value_size = 0;
    uint64_t reserved_size = 0;
    uint64_t generation = 0;
    YLT_REFL(PersistedBucketEntry, key, entry_offset, key_size, value_size,
             reserved_size, generation);
};

/**
 * @brief On-disk `.meta` snapshot of one bucket.
 *
 * A CRC-32C over the serialized payload (with `checksum` zeroed) detects torn
 * or corrupt metadata; `version` rejects formats this build cannot parse.
 *
 * There is exactly one `.meta` file per bucket, holding its committed entries
 * at the moment it was written. It is written when the bucket stops being the
 * active one (it is sealed) and rewritten only when the sealed bucket's
 * committed contents change afterwards, never once per key.
 */
struct PersistedBucketMetadata {
    uint32_t version = 0;
    uint32_t checksum = 0;
    int64_t bucket_id = 0;
    uint64_t bucket_generation = 0;
    uint64_t capacity = 0;
    uint64_t alignment = 0;
    uint64_t append_offset = 0;
    // Set when the bucket has been chosen for eviction and its data file is
    // being deleted. Recovery treats such a bucket as gone rather than live.
    bool evicting = false;
    std::vector<PersistedBucketEntry> entries;
    YLT_REFL(PersistedBucketMetadata, version, checksum, bucket_id,
             bucket_generation, capacity, alignment, append_offset, evicting,
             entries);
};

// Bump when the layout of PersistedBucketMetadata changes incompatibly.
// Version 5 stores only committed entries; pending reservations and tombstones
// remain in memory but are intentionally absent from the on-disk snapshot.
inline constexpr uint32_t kBucketMetadataVersion = 5;

enum class BucketEntryState : int32_t {
    PENDING = 0,
    COMMITTED = 1,
    TOMBSTONE = 2,
};

/**
 * @brief Append-only, bucket-based DFS space allocator.
 *
 * Space is handed out by appending into the currently active bucket file. Each
 * bucket has a fixed capacity; when a request no longer fits, a new bucket is
 * created. Buckets carry persisted metadata so committed entries can be
 * recovered after a master restart, and eviction works at whole-bucket
 * granularity through a two-phase protocol driven by the master.
 *
 * Metadata durability
 * -------------------
 * Each bucket owns exactly two files:
 *
 *   bucket_NNNNNN.data     preallocated data file
 *   bucket_NNNNNN.meta     the bucket's single full metadata snapshot
 *
 * The active bucket keeps its metadata in memory only. The `.meta` file is
 * written when the bucket is sealed, i.e. when allocation moves on to a new
 * bucket, and rewritten afterwards only if the sealed bucket's state changed
 * (a tail reservation committing, or an entry being freed). Rewrites happen in
 * place: the hot path never writes metadata, never renames and never appends to
 * a log, so storing a key costs no metadata I/O at all.
 *
 * `FlushDirtyMetadata()` performs the deferred writes for every bucket marked
 * dirty. It runs on the master's DFS maintenance tick and at shutdown.
 *
 * The accepted failure mode: if the master dies before a bucket is sealed, that
 * bucket has no `.meta` file and recovery deletes its `.data` file, so at most
 * the newest bucket's contents are lost. Because an in-place rewrite that is
 * torn destroys both the old and the new content, a crash during a rewrite has
 * the same effect for that one bucket.
 *
 * Lock ownership
 * --------------
 * `mutex_` guards every mutable allocator member: `buckets_`, `key_index_`,
 * `active_bucket_id_`, `next_bucket_id_`, `lru_list_`/`lru_index_` and the
 * eviction bookkeeping. Bucket state lives behind shared_ptr and is only
 * mutated while `mutex_` is held, so no per-bucket mutex is needed.
 *
 * Slow DFS I/O (preallocation, metadata writes, deletes) is always performed
 * with `mutex_` released: the caller snapshots the state it needs under the
 * lock, does the I/O, then reacquires the lock and re-validates the bucket
 * generation before publishing the result. No RPC, callback or filesystem call
 * ever happens while `mutex_` is held.
 */
class ImmutableBucketAllocator final : public GlobalAllocatorInterface {
   public:
    ImmutableBucketAllocator() = default;
    ~ImmutableBucketAllocator() override;

    ImmutableBucketAllocator(const ImmutableBucketAllocator&) = delete;
    ImmutableBucketAllocator& operator=(const ImmutableBucketAllocator&) = delete;

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

    uint64_t GetTotalCapacity() const override;

    uint64_t GetUsedBytes() const override;

    /**
     * @brief Dynamically change the maximum number of buckets at runtime.
     *
     * The new positive limit takes effect on the next allocation: no new
     * bucket is created once the count is reached. It does not force eviction
     * of already-existing buckets, so it can be set below the current bucket
     * count without immediate effect.
     *
     * @return the previous value of max_bucket_count_.
     */
    int64_t SetMaxBucketCount(int64_t new_max_bucket_count);

    /**
     * @brief Mark a reservation as durable so restart recovery may revive it.
     *
     * Called by the master once the client reports the DFS data write finished
     * (PutEnd). A mismatching descriptor or generation is ignored, so a late
     * completion from a superseded operation cannot commit the allocation that
     * replaced it.
     *
     * @return true when the entry is COMMITTED and that fact is durable.
     */
    bool MarkCommitted(const std::string& key,
                       const DistributedFSDescriptor& descriptor);

    /**
     * @brief All committed entries recovered from disk at Init() time.
     *
     * The master re-registers these as COMPLETE DFS replicas so recovered
     * objects are queryable and readable, then clears the list.
     */
    struct RecoveredReplica {
        std::string key;
        DistributedFSDescriptor descriptor;
    };
    std::vector<RecoveredReplica> TakeRecoveredReplicas();

    /**
     * @brief Bucket-granular eviction transaction.
     *
     * Prepare() freezes one non-active bucket and lists its live entries. The
     * master validates every candidate without mutating metadata; only if all
     * are acceptable does it Commit(), which removes the replicas and then
     * deletes the bucket files. Any rejection leads to Abort(), which restores
     * the bucket untouched.
     *
     * Move-only, and Commit/Abort are each idempotent: whichever runs first
     * wins and the destructor aborts an unresolved transaction.
     */
    class PendingEviction {
       public:
        PendingEviction() = default;
        ~PendingEviction();

        PendingEviction(const PendingEviction&) = delete;
        PendingEviction& operator=(const PendingEviction&) = delete;
        PendingEviction(PendingEviction&& other) noexcept;
        PendingEviction& operator=(PendingEviction&& other) noexcept;

        bool Empty() const { return candidates_.empty(); }
        int64_t bucket_id() const { return bucket_id_; }
        const std::vector<EvictionCandidate>& Candidates() const {
            return candidates_;
        }

       private:
        friend class ImmutableBucketAllocator;

        ImmutableBucketAllocator* owner_ = nullptr;
        int64_t bucket_id_ = -1;
        uint64_t bucket_generation_ = 0;
        std::vector<EvictionCandidate> candidates_;
    };

    /**
     * @brief Freeze the coldest evictable bucket and return its live entries.
     * Returns an empty transaction when nothing can be evicted.
     */
    PendingEviction PrepareEviction();

    /**
     * @brief Prepare one cold bucket regardless of byte watermarks.
     *
     * Used only after allocation reports that the bucket-count limit has been
     * reached. It allows the master to reclaim one bucket and retry without
     * turning low-utilization bucket tails into a permanent allocation deadlock.
     */
    PendingEviction PrepareEvictionForAllocationFailure();

    /**
     * @brief Accept the eviction: drop the bucket and delete its files.
     * Must only be called once the master has removed every candidate replica.
     */
    void CommitEviction(PendingEviction&& pending);

    /**
     * @brief Reject the eviction and return the bucket to service unchanged.
     *
     * Used when the master declined the candidates. The bucket goes back at the
     * warm end of the LRU so the next round can reach a different candidate
     * instead of re-offering this one forever.
     */
    void AbortEviction(PendingEviction&& pending);

    /**
     * @brief Write the `.meta` file of every bucket whose state is dirty.
     *
     * The hot paths - BatchAllocate, MarkCommitted, Free - only update memory
     * and mark the bucket dirty, because the master calls `Free()` while holding
     * a metadata shard lock and DFS I/O must never happen under that lock. This
     * method performs the deferred writes, so it must be called with no master
     * lock held: the master's DFS maintenance tick drives it, and the destructor
     * runs it once more so a clean shutdown leaves no unwritten metadata.
     *
     * @return the number of buckets whose metadata was made durable.
     */
    size_t FlushDirtyMetadata();

    /**
     * @brief Number of buckets currently tracked (test/metrics helper).
     */
    size_t GetBucketCount() const;

    /**
     * @brief Bucket id an existing key currently lives in, if any.
     */
    std::optional<int64_t> GetBucketIdForKey(const std::string& key) const;

    static std::string FormatBucketId(int64_t bucket_id);

   private:
    friend class PendingEviction;

    // Shared implementation for watermark-driven and allocation-failure-driven
    // eviction. The latter bypasses only the watermark gate and still observes
    // active/frozen state plus the master's full validation protocol.
    PendingEviction PrepareEvictionInternal(bool force_one);

    // Shared implementation of AbortEviction. `demote` distinguishes an
    // explicit master rejection (return the bucket at the warm end so the scan
    // can move on) from a transaction dropped without a verdict by the
    // destructor or move assignment (restore its cold position).
    void AbortEviction(PendingEviction&& pending, bool demote);

    struct BucketEntry {
        uint64_t entry_offset = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;
        uint64_t reserved_size = 0;
        uint64_t generation = 0;
        BucketEntryState state = BucketEntryState::PENDING;
    };

    struct BucketState {
        int64_t bucket_id = 0;
        // Bumped whenever the bucket is (re)created so a stale transaction
        // cannot resolve against a different bucket that reused the id.
        uint64_t generation = 0;
        uint64_t capacity = 0;
        uint64_t append_offset = 0;
        // Bytes reserved by entries that are still live (PENDING or
        // COMMITTED). Drives the "is this bucket worth evicting" decision.
        uint64_t live_bytes = 0;
        int64_t last_access_ns = 0;
        // Set between PrepareEviction and Commit/Abort. A frozen bucket
        // accepts no new allocations and cannot be selected again.
        bool frozen = false;
        // True once the bucket has been sealed (it is no longer the active
        // bucket), which is when its `.meta` file starts to exist.
        bool sealed = false;
        // Set when the in-memory state differs from what the `.meta` file holds,
        // cleared once the file has been rewritten. Only meaningful for a sealed
        // bucket: an unsealed one is deliberately not persisted at all.
        bool meta_dirty = false;
        // Tombstoned entries, kept for eviction bookkeeping.
        uint64_t tombstones = 0;
        std::unordered_map<std::string, BucketEntry> entries;
    };

    using BucketPtr = std::shared_ptr<BucketState>;

    // --- helpers, called with mutex_ held only where the name says Locked ---

    std::string BucketDataPath(int64_t bucket_id) const;
    std::string BucketMetaPath(int64_t bucket_id) const;

    // Serializes `bucket` into a PersistedBucketMetadata snapshot. Taken under
    // `mutex_`; the file write happens outside it.
    PersistedBucketMetadata SnapshotLocked(BucketState& bucket,
                                           bool evicting);

    // Overwrites `bucket_id`'s single `.meta` file in place and syncs it.
    tl::expected<void, ErrorCode> PersistMetadata(
        const PersistedBucketMetadata& snapshot);

    // Marks `bucket` as needing its `.meta` file (re)written. A bucket that is
    // still active is never persisted, so the flag is only set once sealed.
    static void MarkMetaDirtyLocked(BucketState& bucket);

    // Seals the currently active bucket - it stops accepting allocations - and
    // marks it for persistence. Requires `mutex_`.
    void SealActiveBucketLocked();

    // Creates a fresh bucket: allocates the id under the lock, preallocates
    // the data file outside the lock, then publishes the bucket. Rolls back
    // id/state/files on any failure. The new bucket's `.meta` is written only
    // when it is later sealed.
    tl::expected<BucketPtr, ErrorCode> CreateBucketUnlocked(
        std::unique_lock<std::mutex>& lock);

    // Ensures an active bucket exists with at least `required` bytes free.
    // May temporarily release `lock` to create a bucket.
    tl::expected<BucketPtr, ErrorCode> EnsureActiveBucket(
        std::unique_lock<std::mutex>& lock, uint64_t required);

    void TouchLruLocked(int64_t bucket_id, int64_t now_ns);
    void RemoveFromLruLocked(int64_t bucket_id);

    // Applies one reservation to `bucket` and returns the descriptor.
    tl::expected<DistributedFSDescriptor, ErrorCode> ReserveInBucketLocked(
        BucketState& bucket, const std::string& key, uint64_t size);

    // Rolls a reservation back out of `bucket` (used when a later entry of the
    // same batch fails). Only valid while the reservation is the most recent
    // one, which BatchAllocate guarantees by unwinding in reverse order.
    void UnreserveInBucketLocked(BucketState& bucket, const std::string& key,
                                 const DistributedFSDescriptor& descriptor);

    // Matches `descriptor` against the recorded entry for `key`.
    // Returns nullptr when the descriptor is stale.
    BucketEntry* FindMatchingEntryLocked(const std::string& key,
                                         const DistributedFSDescriptor& desc,
                                         BucketPtr* out_bucket);

    tl::expected<void, ErrorCode> RecoverFromDisk();

    void DeleteBucketFiles(int64_t bucket_id);

    uint64_t UsedBytesLocked() const;

    std::string fsdir_;
    std::string fs_adapter_type_;
    std::unique_ptr<FileSystemAdapter> fs_adapter_;

    uint64_t bucket_capacity_ = 0;
    uint64_t alignment_ = 4096;
    int64_t max_bucket_count_ = 0;

    bool eviction_enabled_ = true;
    double eviction_high_watermark_ = 0.9;
    double eviction_low_watermark_ = 0.7;
    std::chrono::seconds eviction_check_interval_{5};

    mutable std::mutex mutex_;
    // Serializes bucket creation. Creating a bucket releases `mutex_` for the
    // file I/O, so without this flag several threads would each reserve a
    // distinct id and race to publish, orphaning all but one and letting a
    // loser's rollback delete a winner's files.
    bool bucket_creation_in_flight_ = false;
    std::condition_variable bucket_creation_cv_;
    std::unordered_map<int64_t, BucketPtr> buckets_;
    std::unordered_map<std::string, int64_t> key_index_;
    int64_t next_bucket_id_ = 0;
    int64_t active_bucket_id_ = -1;
    uint64_t next_generation_ = 1;
    // Once the high watermark is crossed, keep evicting until usage falls
    // below the low watermark; protected buckets may make that span several
    // prepare/resolve rounds.
    bool eviction_active_ = false;

    std::list<int64_t> lru_list_;
    std::unordered_map<int64_t, std::list<int64_t>::iterator> lru_index_;

    std::vector<RecoveredReplica> recovered_replicas_;

    std::atomic<bool> initialized_{false};
};

}  // namespace mooncake
