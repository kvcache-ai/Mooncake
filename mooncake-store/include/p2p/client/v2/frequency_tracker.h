#pragma once

// FrequencyTracker: the single owner of "how hot is this key".
//
// It answers two different questions from one record, and keeping them apart
// is the point:
//
//   heat       -- every touch, writes included. This is what GetHotKeyStats
//                 reports, and HARecoveryManager phase 1 recovers hot keys
//                 first from it, so a freshly written key must look warm.
//   read_heat  -- reads only. This is what the onboard decision reads. The
//                 previous design took its frequency from a sketch that was
//                 bumped on commit as well, so writing a key counted towards
//                 "this key is worth copying to a faster tier" -- which is
//                 backwards: a key nobody has read yet has shown no demand.
//
// Both are decaying counters, not running totals. A raw count that only ever
// grows makes a key that was hot an hour ago permanently outrank one that is
// hot now, and it never falls back below a threshold once it has crossed it.
// Each touch decays the stored value by the elapsed half-lives and then adds
// one, which is a sliding window without keeping a window of samples.
//
// Identity is (canonical registration, key), and the registration half is what
// makes delete-then-recreate unambiguous: a new registration for the same key
// name resets the record rather than inheriting the dead key's heat.
//
// Everything here is approximate by design. It decides what to *propose*; it
// is never consulted about whether a block exists.

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

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"
#include "utils.h"

namespace mooncake::v2 {

/**
 * @struct FrequencySnapshot
 * @brief One key's heat, already decayed to the moment it was taken.
 */
struct FrequencySnapshot {
    /** Decayed count of every touch. Drives hot-key reporting. */
    double heat = 0.0;
    /** Decayed count of reads only. Drives the onboard decision. */
    double read_heat = 0.0;
    /** Diagnostic: touches since this record was created. Never a decision. */
    uint64_t raw_touches = 0;
    /** True when the tracker had no record at all. */
    bool missing = true;
};

/**
 * @struct FrequencyTrackerConfig
 */
struct FrequencyTrackerConfig {
    size_t shard_count = 16;
    /** Upper bound on how many keys a snapshot may return. */
    size_t max_snapshot_keys = 4096;
    /** Used when the caller passes nullopt. */
    size_t default_hot_key_num = 64;

    /**
     * Time for an untouched score to halve. This is the "access frequency
     * window" of the design, expressed as a decay rather than as a bucket
     * boundary so a key does not lose its whole history at a tick.
     */
    std::chrono::milliseconds half_life{30000};

    /**
     * A record whose heat has decayed below `expiry_threshold` and which has
     * not been touched for this long is dropped. Without it the map keeps one
     * entry per key ever seen, for the process lifetime.
     */
    std::chrono::milliseconds entry_ttl{300000};
    double expiry_threshold = 0.05;

    /** Hard cap across all shards. Exceeding it drops the coldest records. */
    size_t max_tracked_keys = 1U << 20;
};

tl::expected<void, ErrorCode> ValidateFrequencyTrackerConfig(
    const FrequencyTrackerConfig& config);

/**
 * @class FrequencyTracker
 */
class FrequencyTracker {
   public:
    explicit FrequencyTracker(
        const FrequencyTrackerConfig& config = {},
        std::shared_ptr<Clock> clock = std::make_shared<SteadyClock>());

    /**
     * @brief A read happened. Returns the state *including* this access.
     *
     * The return value is what makes the ordering rule enforceable: the access
     * path records here and then publishes the Access event, so a consumer
     * that queries the tracker cannot see a frequency that excludes the very
     * access it is reacting to.
     */
    FrequencySnapshot RecordAccess(const RegistrationId& registration,
                                   std::string_view key);

    /** Point query. Does not count as a touch. */
    FrequencySnapshot Get(const RegistrationId& registration,
                          std::string_view key) const;

    /** A block became visible. Counts towards heat, not towards read_heat. */
    void OnCommit(const RegistrationId& registration, std::string_view key);

    /**
     * @brief The key's last replica is gone.
     *
     * Token-scoped: if the tracker now holds a record for a *newer*
     * registration of the same key name, that record survives. Otherwise a
     * delete arriving after a recreate would erase the live key's heat.
     */
    void OnDelete(const RegistrationId& registration, std::string_view key);

    /**
     * @brief Forget a key whatever its registration.
     *
     * For callers that only know the name -- the eviction path learns that a
     * key's last replica is gone without holding its identity.
     */
    void Remove(std::string_view key);

    void Clear();

    /**
     * @param hot_key_num nullopt = configured default, 0 = every tracked key
     *        (still bounded by max_snapshot_keys).
     */
    AccessStats Snapshot(std::optional<size_t> hot_key_num) const;

    size_t TrackedKeyCount() const;

    /** Snapshots that hit max_snapshot_keys and returned a partial answer. */
    uint64_t TruncatedSnapshotCount() const;

    /** Records dropped by the cap or by expiry. */
    uint64_t EvictedRecordCount() const;

   private:
    struct Entry {
        /** Which registration this heat belongs to. */
        RegistrationId registration;
        double heat = 0.0;
        double read_heat = 0.0;
        uint64_t raw_touches = 0;
        Clock::time_point last_touch{};
    };

    struct Shard {
        mutable std::mutex mu;
        std::unordered_map<std::string, Entry, StringHash, std::equal_to<>>
            entries;
    };

    Shard& ShardFor(std::string_view key);
    const Shard& ShardFor(std::string_view key) const;

    /** Decay `entry` forward to `now`. Caller holds the shard lock. */
    void DecayLocked(Entry& entry, Clock::time_point now) const;

    /** Common body of RecordAccess and OnCommit. */
    FrequencySnapshot Touch(const RegistrationId& registration,
                            std::string_view key, bool is_read);

    /** Drop expired and, if over the cap, coldest records in one shard. */
    void TrimLocked(Shard& shard, Clock::time_point now);

    FrequencyTrackerConfig config_;
    std::shared_ptr<Clock> clock_;
    std::vector<std::unique_ptr<Shard>> shards_;
    /** Per-shard share of max_tracked_keys, precomputed. */
    size_t max_per_shard_ = 0;

    mutable std::atomic<uint64_t> truncated_snapshots_{0};
    std::atomic<uint64_t> evicted_records_{0};
};

}  // namespace mooncake::v2
