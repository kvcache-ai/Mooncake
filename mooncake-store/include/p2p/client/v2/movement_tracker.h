#pragma once

// MovementTracker: the per-key state of cross-tier movement.
//
// It exists so the event consumers can be stateless. A consumer that
// remembered "I already proposed this" or "this key moved recently" would be a
// second, silently diverging copy of state that already has an owner, and it
// would leak that state on every path that does not run through the consumer
// (a delete, a shutdown, a stale command dropped by the executor).
//
// It owns exactly three questions:
//
//   - is a movement for this key already in flight?  (dedup)
//   - did this key move too recently to move again?  (cooldown)
//   - has it lived here long enough to be moved at all?  (minimum residency)
//
// It answers none of these:
//
//   - does the block exist, and where?          -> BlockIndex
//   - how hot is it?                            -> FrequencyTracker
//   - which tier should it go to?               -> TierPlacementPolicy
//
// It therefore never stores a block address, and its answer is never a
// substitute for validating the token against the index.
//
// Every acquire must be settled. A lease that is never released blocks that
// key's dedup slot for the process lifetime, so the settle (the design doc
// calls it OnSettled; here it is MovementLease::Settle) runs on every exit
// path -- success, failure, stale, deadline, cancellation, shutdown -- and
// the RAII lease makes "every path" mean it.
//
// A settle frees exactly the slot its own acquire took, and nothing else ever
// frees that slot: not a delete of the key, not the record cap. That is what
// keeps "at most one movement in flight per dedup key" true across a delete
// that races a movement which is still running.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"
#include "utils.h"

namespace mooncake::v2 {

/**
 * @enum MovementDirection
 * @brief Which way across the tier graph. Cooldowns are per direction: a key
 *        that was just onboarded should not be immediately offloaded again,
 *        but the two decisions have different natural timescales.
 */
enum class MovementDirection : uint8_t {
    kOffload,  // towards a slower tier
    kOnboard,  // towards a faster tier
};

const char* ToString(MovementDirection direction);

/**
 * @struct MovementDedupKey
 * @brief What "the same movement" means.
 *
 * Registration identity rather than the key string, so a delete-and-recreate
 * of the same name is a different movement; source block id, so replacing the
 * block invalidates the proposal.
 */
struct MovementDedupKey {
    RegistrationId registration_id;
    BlockId source_block_id;
    /**
     * The tier the block is being moved off; the residency gate is asked
     * about this one. It repeats source_block_id.tiler_id, so leaving it zero
     * is a caller bug rather than a request to skip the gate, and the tracker
     * says so instead of quietly answering "nothing to check".
     */
    UUID source_tiler{0, 0};
    UUID destination_tiler{0, 0};
    bool operator==(const MovementDedupKey&) const = default;
};

struct MovementDedupKeyHash {
    size_t operator()(const MovementDedupKey& key) const noexcept;
};

/**
 * @enum MovementRejection
 * @brief Why TryAcquire said no. Reported separately because they mean very
 *        different things to an operator: cooldown and residency are the
 *        policy working, while a permanent stream of kInflight means commands
 *        are being proposed faster than they execute.
 */
enum class MovementRejection : uint8_t {
    kInflight,
    kCooldown,
    kMinimumResidency,
    kStopped,
    /**
     * The dedup key itself is unusable -- today, no source tier to ask the
     * residency question about. Kept apart from the policy reasons because it
     * is a caller bug: unlike a cooldown, waiting will not change the answer.
     */
    kInvalid,
};

const char* ToString(MovementRejection rejection);

class MovementTracker;

/**
 * @class MovementLease
 * @brief Move-only proof that a movement may proceed, released exactly once.
 *
 * The RAII form is the point: the executor has eight exit paths and a manual
 * OnSettled on each would eventually miss one, permanently wedging that key's
 * dedup slot.
 */
class MovementLease {
   public:
    MovementLease() = default;
    MovementLease(const MovementLease&) = delete;
    MovementLease& operator=(const MovementLease&) = delete;
    MovementLease(MovementLease&& other) noexcept;
    MovementLease& operator=(MovementLease&& other) noexcept;
    ~MovementLease();

    /** Release now, recording whether the movement actually happened. */
    void Settle(bool moved);

    explicit operator bool() const { return tracker_ != nullptr; }
    const MovementDedupKey& Key() const { return key_; }

   private:
    friend class MovementTracker;
    MovementLease(MovementTracker* tracker, MovementDedupKey key,
                  MovementDirection direction, std::string owner_key,
                  uint64_t lease_id);

    MovementTracker* tracker_ = nullptr;
    MovementDedupKey key_{};
    MovementDirection direction_ = MovementDirection::kOffload;
    std::string owner_key_;
    /**
     * Names this lease's dedup slot. The dedup key alone cannot: a slot left
     * behind by a delete and a slot granted afterwards can carry the same
     * key, and a settle must free only the slot it was handed.
     */
    uint64_t lease_id_ = 0;
};

/**
 * @struct MovementTrackerConfig
 */
struct MovementTrackerConfig {
    size_t shard_count = 16;
    /** After a successful move, how long before this key may move again. */
    std::chrono::milliseconds cooldown{2000};
    /** How long a block must have sat on a tier before it may be moved off. */
    std::chrono::milliseconds minimum_residency{1000};
    /**
     * Upper bound on remembered per-key records. Exceeding it drops the oldest
     * settled entries: forgetting a cooldown makes the policy slightly more
     * eager, which is survivable, whereas unbounded growth is not.
     */
    size_t max_tracked_keys = 65536;
};

tl::expected<void, ErrorCode> ValidateMovementTrackerConfig(
    const MovementTrackerConfig& config);

/**
 * @struct MovementTrackerStats
 */
struct MovementTrackerStats {
    /**
     * Outstanding leases, counted exactly: a dedup slot is born with its
     * lease and dies only with that lease's settle, so a delete of the key
     * cannot hide one from the operator (or from the destructor's check).
     */
    size_t inflight = 0;
    size_t tracked_keys = 0;
    uint64_t acquired = 0;
    uint64_t rejected_inflight = 0;
    uint64_t rejected_cooldown = 0;
    uint64_t rejected_residency = 0;
    uint64_t rejected_invalid = 0;
    uint64_t settled_moved = 0;
    uint64_t settled_unmoved = 0;
    uint64_t evicted_records = 0;
};

/**
 * @class MovementTracker
 */
class MovementTracker {
   public:
    MovementTracker(const MovementTrackerConfig& config,
                    std::shared_ptr<Clock> clock);
    ~MovementTracker();

    /**
     * @brief Claim the right to move this block, or say why not.
     *
     * Called by a consumer before it produces a command, never by the
     * executor: the point is to stop the command from being created twice.
     */
    tl::expected<MovementLease, MovementRejection> TryAcquire(
        std::string_view key, const MovementDedupKey& dedup,
        MovementDirection direction);

    /** A block became visible on a tier; starts its residency clock. */
    void OnCommitted(std::string_view key, const UUID& tiler_id);

    /** Forget everything about a key. Called when its last replica goes. */
    void OnDeleted(std::string_view key);

    /**
     * @brief Reject further acquisitions; outstanding leases still settle.
     *
     * Once it returns no further lease can be minted: it passes through every
     * shard lock, so an acquire that already read the flag cannot slip a
     * lease past a shutdown that has been reported as complete.
     */
    void Stop();

    MovementTrackerStats Stats() const;

   private:
    friend class MovementLease;

    struct Record {
        /** Per tiler, when the current block arrived. */
        std::unordered_map<UUID, Clock::time_point, boost::hash<UUID>> since;
        /** Per direction, when the last successful move settled. */
        Clock::time_point last_offload{};
        Clock::time_point last_onboard{};
        uint64_t inflight = 0;
        /** Bumped on every touch; used to pick a victim when capped. */
        uint64_t sequence = 0;
        /**
         * Fixed at creation. A delete followed by a Put gives the same name a
         * brand new record, and a movement still running against the old one
         * has to tell them apart -- otherwise its settle charges a cooldown
         * to a block it never touched.
         */
        uint64_t incarnation = 0;
    };

    /** What one granted lease holds while its movement runs. */
    struct InflightSlot {
        /** By value: the lease outlives the record when the key is deleted. */
        std::string key;
        uint64_t lease_id = 0;
        /** Incarnation of the record this lease pinned, 0 if none. */
        uint64_t record_incarnation = 0;
    };

    struct Shard {
        mutable std::mutex mu;
        std::unordered_map<std::string, Record, StringHash, std::equal_to<>>
            records;
        std::unordered_map<MovementDedupKey, InflightSlot, MovementDedupKeyHash>
            inflight;
    };

    Shard& ShardFor(std::string_view key);
    const Shard& ShardFor(std::string_view key) const;

    /** Called by ~MovementLease / MovementLease::Settle. */
    void Release(const MovementDedupKey& dedup, MovementDirection direction,
                 std::string_view key, uint64_t lease_id, bool moved);

    /** Monotone stamp for record births and record touches. */
    uint64_t NextStamp();

    /** Drop the least recently touched settled records. Caller holds `mu`. */
    void TrimLocked(Shard& shard);

    MovementTrackerConfig config_;
    std::shared_ptr<Clock> clock_;
    std::vector<std::unique_ptr<Shard>> shards_;
    std::atomic<bool> stopped_{false};
    std::atomic<uint64_t> sequence_{0};
    std::atomic<uint64_t> next_lease_id_{0};

    std::atomic<uint64_t> acquired_{0};
    std::atomic<uint64_t> rejected_inflight_{0};
    std::atomic<uint64_t> rejected_cooldown_{0};
    std::atomic<uint64_t> rejected_residency_{0};
    std::atomic<uint64_t> rejected_invalid_{0};
    std::atomic<uint64_t> settled_moved_{0};
    std::atomic<uint64_t> settled_unmoved_{0};
    std::atomic<uint64_t> evicted_records_{0};
};

}  // namespace mooncake::v2
