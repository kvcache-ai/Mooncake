#pragma once

// LeaseManager: the pending-write and pinned-read lease tables, plus the
// expiry scanner. V1 kept all of this inline in DataManager; here it is a
// component with an injected clock so expiry is testable without sleeping.
//
// Two rules shape the design:
//
//  1. Reservation precedes allocation. PreWrite/Put order is
//     ReservePendingWrite -> duplicate check -> allocate -> attach. The
//     reservation is taken under a shard lock, so N concurrent writers of the
//     same key produce exactly one allocation. Allocating first and
//     registering afterwards would make N-1 writers each waste an allocation,
//     and every wasted allocation can trigger a synchronous reclaim -- turning
//     write contention into an eviction storm.
//
//  2. A pinned lease is identified by (key, registration identity, BlockId),
//     because a key can be deleted and recreated, or its block replaced by a
//     migration, while an old lease is still alive. UnPinKey only knows
//     (key, token), so a token -> identity index is mandatory: without it
//     every unpin would scan a whole shard, where V1 was O(1).

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"
#include "utils.h"

namespace mooncake::v2 {

/**
 * @struct PendingWriteLease
 */
struct PendingWriteLease {
    std::string key;
    UUID write_token{0, 0};
    UUID tiler_id{0, 0};
    // Empty between Reserve and Attach. That gap is exactly the window in
    // which the key is claimed but nothing has been allocated yet.
    std::optional<std::variant<MutableBlock, CompletedBlock>> transaction;
    Clock::time_point deadline{};
};

/**
 * @struct PinnedLeaseId
 */
struct PinnedLeaseId {
    std::string key;
    RegistrationId registration_id;
    BlockId block_id;
    bool operator==(const PinnedLeaseId&) const = default;
};

struct PinnedLeaseIdHash {
    size_t operator()(const PinnedLeaseId& id) const noexcept;
};

/**
 * @struct PinnedReadLease
 * @brief The ImmutableBlock is the lease's only keepalive: releasing the
 *        record is all it takes to release the resource, and the BlockIndex is
 *        never called back.
 */
struct PinnedReadLease {
    PinnedLeaseId id;
    ImmutableBlock block;
    UUID read_token{0, 0};
    uint32_t ref_count = 1;
    Clock::time_point deadline{};
};

/**
 * @struct LeaseDeadline
 * @brief Min-heap element. Renewal pushes a new element rather than updating
 *        in place, so the scanner must re-check the record it names; stale
 *        elements are simply discarded. That avoids both mid-heap deletion and
 *        a full table scan per tick.
 */
template <typename Key>
struct LeaseDeadline {
    Clock::time_point deadline;
    Key key;
    UUID token{0, 0};
    bool operator>(const LeaseDeadline& rhs) const {
        return deadline > rhs.deadline;
    }
};

template <typename Key, typename Record, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
struct LeaseShard {
    mutable std::mutex mu;
    // Equal is std::equal_to<> for string keys so lookups take a string_view
    // without allocating.
    std::unordered_map<Key, Record, Hash, Equal> records;
    std::priority_queue<LeaseDeadline<Key>, std::vector<LeaseDeadline<Key>>,
                        std::greater<LeaseDeadline<Key>>>
        deadlines;
};

/**
 * @struct PinnedLeaseShard
 * @brief Sharded by key. `by_token` and `records` share one lock and one
 *        lifetime: every removal path (unpin, expiry, stop) must clear both.
 */
struct PinnedLeaseShard {
    mutable std::mutex mu;
    std::unordered_map<PinnedLeaseId, PinnedReadLease, PinnedLeaseIdHash>
        records;
    std::unordered_map<UUID, PinnedLeaseId, boost::hash<UUID>> by_token;
    // Live leases per key. A key can hold several at once (different
    // registrations or blocks), and Unpin needs to tell "this key is pinned
    // under some other token" (INVALID_READ) from "this key is not pinned at
    // all" (idempotent success) without scanning the shard.
    std::unordered_map<std::string, size_t, StringHash, std::equal_to<>>
        leases_per_key;
    std::priority_queue<LeaseDeadline<PinnedLeaseId>,
                        std::vector<LeaseDeadline<PinnedLeaseId>>,
                        std::greater<LeaseDeadline<PinnedLeaseId>>>
        deadlines;
};

/**
 * @struct PinLeaseResult
 */
struct PinLeaseResult {
    UUID read_token{0, 0};
    uint32_t ref_count = 0;
    Clock::time_point deadline{};
};

/**
 * @class LeaseManager
 */
class LeaseManager {
   public:
    LeaseManager(const KeyLeaseConfig& config, std::shared_ptr<Clock> clock,
                 size_t shard_count = 64);
    ~LeaseManager();

    LeaseManager(const LeaseManager&) = delete;
    LeaseManager& operator=(const LeaseManager&) = delete;

    std::chrono::milliseconds LeaseDuration() const { return lease_duration_; }

    // --- pending write: claim first, allocate second ---

    /**
     * @brief Atomically claim `key` for writing and mint a write token.
     * @return REPLICA_IS_PROCESSING if a live claim already exists. The caller
     *         has allocated nothing at that point, which is the whole purpose
     *         of separating claim from allocation.
     */
    tl::expected<UUID, ErrorCode> ReservePendingWrite(
        std::string_view key, Clock::time_point deadline);

    /** Attach the allocated transaction to an existing claim. */
    tl::expected<void, ErrorCode> AttachPendingWriteTransaction(
        std::string_view key, const UUID& token, const UUID& tiler_id,
        std::variant<MutableBlock, CompletedBlock> transaction);

    /**
     * @struct DetachedPendingWrite
     * @brief What a committing writer needs: the block, plus where PreWrite
     *        placed it.
     */
    struct DetachedPendingWrite {
        std::variant<MutableBlock, CompletedBlock> transaction;
        UUID tiler_id{0, 0};
    };

    /**
     * @brief Take the transaction out of a claim while keeping the claim.
     *
     * The write path needs the block long before it is safe to release the
     * key: the copy and the commit happen after this point, and a concurrent
     * writer that slipped in during that window would allocate a second
     * full-size block for a key that is about to exist. So the record stays,
     * and only the transaction moves out.
     *
     * @return the same error codes as TakePendingWrite, plus INVALID_WRITE if
     *         the claim has no transaction attached.
     */
    tl::expected<DetachedPendingWrite, ErrorCode> DetachPendingWriteTransaction(
        std::string_view key, const UUID& token);

    /**
     * @brief Remove the claim and hand back its record. Shared by commit,
     *        revoke and the allocation-failure rollback.
     * @return OBJECT_NOT_FOUND when absent, LEASE_EXPIRED when the deadline has
     *         passed, INVALID_WRITE on a token mismatch -- matching V1.
     */
    tl::expected<PendingWriteLease, ErrorCode> TakePendingWrite(
        std::string_view key, const UUID& token);

    /** True if a live (unexpired) claim exists for `key`. */
    bool HasPendingWrite(std::string_view key) const;

    // --- pinned read ---

    /**
     * @brief Pin `block`, or bump an existing lease on the exact same
     *        (key, registration, BlockId) triple.
     *
     * Reusing the token only for an exact triple match is what stops a lease
     * from surviving a delete-and-recreate of the same key.
     */
    tl::expected<PinLeaseResult, ErrorCode> Pin(ImmutableBlock block,
                                                Clock::time_point deadline);

    /**
     * @brief Release one reference. O(1) via the token index.
     * @return OK (idempotent) when `key` holds no lease at all, INVALID_READ
     *         when it holds one but not under this token, LEASE_EXPIRED when
     *         the lease has already timed out -- matching V1, which looks the
     *         record up by key and then compares the token.
     */
    tl::expected<void, ErrorCode> Unpin(std::string_view key,
                                        const UUID& token);

    /** Stop the scanner and drop every lease, destroying records unlocked. */
    void StopAndDrain();

    // --- introspection (tests and metrics) ---
    size_t PendingWriteCount() const;
    size_t PinnedLeaseCount() const;
    size_t PinnedTokenIndexSize() const;
    /** Force one expiry sweep instead of waiting for the scanner. */
    size_t ScanExpiredNow();

   private:
    void ScannerMain();
    size_t ScanPendingWrites(Clock::time_point now,
                             std::vector<PendingWriteLease>& detached);
    size_t ScanPinnedReads(Clock::time_point now,
                           std::vector<PinnedReadLease>& detached);

    size_t ShardFor(std::string_view key) const;

    std::chrono::milliseconds lease_duration_;
    std::chrono::milliseconds scan_interval_;
    std::shared_ptr<Clock> clock_;

    using PendingWriteShard =
        LeaseShard<std::string, PendingWriteLease, StringHash, std::equal_to<>>;
    std::vector<std::unique_ptr<PendingWriteShard>> pending_writes_;
    std::vector<std::unique_ptr<PinnedLeaseShard>> pinned_reads_;

    std::mutex scanner_mu_;
    std::condition_variable scanner_cv_;
    bool scanner_stop_ = false;
    std::thread scanner_;
};

}  // namespace mooncake::v2
