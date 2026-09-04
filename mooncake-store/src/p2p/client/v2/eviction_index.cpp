#include "p2p/client/v2/eviction_index.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

// Reused rather than rewritten, and included only here: v2 headers may not
// reach into the tiered-cache tree, so the dependency stops at this .cpp.
#include "p2p/client/tiered_cache/event_driven_scheduler/frequency_sketch.h"
#include "p2p/client/tiered_cache/event_driven_scheduler/multi_lru.h"
#include "utils.h"

namespace mooncake::v2 {
namespace {

// The sketch is sized in *entries* (one 64-bit word per four counters), while
// `capacity` is a tier's byte budget. Using the byte count directly would ask
// a 64 GiB tier for a table larger than the tier, so it is clamped: the upper
// bound costs a couple of megabytes, the lower bound keeps a small tier's hot
// set resolvable. An operator who knows the block count sets sketch_capacity.
constexpr size_t kDefaultSketchCapacity = 1u << 16;
constexpr size_t kMinSketchCapacity = 1u << 10;
constexpr size_t kMaxSketchCapacity = 1u << 20;

/**
 * @enum Ordering
 * @brief The three configured orderings. They differ in one thing only --
 *        where the frequency handed to the banded chain comes from -- so they
 *        share one implementation instead of three copies of the consistency
 *        rules below.
 */
enum class Ordering {
    kLru,        // recency only: every block stays in the cold band
    kMultiLru,   // banded by this index's own exact access counts
    kTinyLfuLru  // banded by the shared TinyLFU sketch
};

/**
 * @enum Placement
 * @brief Where an installed record lands in its band's recency list.
 *
 * kObserved is a fact this index witnessed -- a commit or a migration
 * arrival -- and enters at the MRU. kRepaired is a record Reconcile put back
 * from the authoritative index: the block exists, but nobody here saw it
 * being used, and guessing "just now" would evict blocks that really were
 * touched ahead of one nothing can vouch for.
 */
enum class Placement { kObserved, kRepaired };

std::optional<Ordering> OrderingFromType(std::string_view type) {
    if (type == "lru") return Ordering::kLru;
    if (type == "multi_lru") return Ordering::kMultiLru;
    if (type == "tinylfu_lru") return Ordering::kTinyLfuLru;
    return std::nullopt;
}

BandThresholds ResolveThresholds(const EvictionIndexConfig& config) {
    BandThresholds thresholds;  // 0 in the config means "keep the default"
    if (config.band_warm_threshold > 0) {
        thresholds.warm = config.band_warm_threshold;
    }
    if (config.band_hot_threshold > 0) {
        thresholds.hot = config.band_hot_threshold;
    }
    if (config.band_veryhot_threshold > 0) {
        thresholds.very_hot = config.band_veryhot_threshold;
    }
    return thresholds;
}

BandThresholds ChainThresholds(const EvictionIndexConfig& config) {
    BandThresholds thresholds = ResolveThresholds(config);
    // MultiLRU asks every caller to run thresholds through this. After the
    // factory validation it has nothing left to repair, which is the point:
    // an operator's mistake is reported at configuration time rather than
    // silently clamped inside a hot path.
    ValidateBandThresholds(thresholds);
    return thresholds;
}

size_t ResolveSketchCapacity(const EvictionIndexConfig& config,
                             size_t capacity) {
    if (config.sketch_capacity > 0) return config.sketch_capacity;
    if (capacity == 0) return kDefaultSketchCapacity;
    return std::clamp(capacity, kMinSketchCapacity, kMaxSketchCapacity);
}

uint64_t KeyFingerprint(std::string_view key) {
    return static_cast<uint64_t>(StringHash{}(key));
}

// MultiLRU is keyed by string; this index is keyed by RegistrationId, the same
// choice BlockIndex makes. Encoding the id into the chain key -- rather than
// chaining on the key name -- keeps the two halves in one-to-one
// correspondence: a delete-then-recreate of the same key name gets its own
// chain node instead of inheriting the dead registration's recency and its
// pending removal. Twelve bytes fits in a std::string without allocating.
constexpr size_t kChainKeyBytes = sizeof(RegistrationId::registry_shard) +
                                  sizeof(RegistrationId::shard_sequence);

std::string ChainKeyOf(const RegistrationId& id) {
    std::string chain_key(kChainKeyBytes, '\0');
    std::memcpy(chain_key.data(), &id.registry_shard,
                sizeof(id.registry_shard));
    std::memcpy(chain_key.data() + sizeof(id.registry_shard),
                &id.shard_sequence, sizeof(id.shard_sequence));
    return chain_key;
}

std::optional<RegistrationId> RegistrationFromChainKey(
    std::string_view chain_key) {
    if (chain_key.size() != kChainKeyBytes) return std::nullopt;
    RegistrationId id;
    std::memcpy(&id.registry_shard, chain_key.data(),
                sizeof(id.registry_shard));
    std::memcpy(&id.shard_sequence,
                chain_key.data() + sizeof(id.registry_shard),
                sizeof(id.shard_sequence));
    return id;
}

/**
 * @class OrderedEvictionIndex
 * @brief One tiler's cold/hot ordering: an entry map keyed by registration
 *        plus a banded LRU chain, both moved under a single mutex.
 *
 * The single mutex is the whole design. The caller already holds the key's
 * registration mutation guard and holds no BlockIndex shard lock, so this lock
 * is the innermost one and never nests upward. Splitting the map from the
 * chain would let a concurrent commit and delete of one key interleave into a
 * permanent disagreement: a name in the chain with no entry behind it is
 * offered as a victim forever and can never be removed.
 */
class OrderedEvictionIndex final : public EvictionIndex {
   public:
    OrderedEvictionIndex(const EvictionIndexConfig& config, Ordering ordering,
                         const UUID& tiler_id, size_t capacity)
        : config_(config),
          ordering_(ordering),
          tiler_id_(tiler_id),
          sketch_(ResolveSketchCapacity(config, capacity)),
          chain_(ChainThresholds(config)) {}

    void OnCommit(const BlockToken& token) override {
        if (!Owns(token, "OnCommit")) return;
        const uint64_t frequency = RecordAccessFrequency(token);

        std::lock_guard<std::mutex> lock(mu_);
        if (!Indexable(token) ||
            !InsertLocked(token, frequency, Placement::kObserved)) {
            // A commit this index cannot file is a block nothing will ever
            // offer as a victim. The flag is what sends the evict engine to
            // the authoritative snapshot instead of concluding "no
            // candidates, nothing to reclaim" and leaking the bytes.
            MarkNeedsReconcileLocked();
        }
    }

    void OnAccess(const BlockToken& token) override {
        if (!Owns(token, "OnAccess")) return;
        // Frequency belongs to the key, so it is recorded even when this index
        // has never seen the block; only the reordering below needs an entry.
        const uint64_t frequency = RecordAccessFrequency(token);

        std::lock_guard<std::mutex> lock(mu_);
        auto entry = entries_.find(token.registration_id);
        // Accesses only reorder, so losing one is allowed by contract and is
        // deliberately not a reconcile trigger.
        if (entry == entries_.end()) return;
        chain_.Touch(ChainKeyOf(token.registration_id),
                     TouchLocked(entry->second, frequency));
    }

    void OnDelete(const BlockToken& token) override {
        if (!Owns(token, "OnDelete")) return;

        std::lock_guard<std::mutex> lock(mu_);
        if (!RemoveLocked(token)) {
            // Nothing here named that block: either the caller acted on a
            // candidate that was already gone, or it is deleting twice.
            ++stale_candidates_;
        }
    }

    void OnMigration(const BlockToken& old_token,
                     const BlockToken& new_token) override {
        const bool leaving = old_token.tiler_id == tiler_id_;
        const bool arriving = new_token.tiler_id == tiler_id_;
        if (!leaving && !arriving) {
            // Both halves are reported to both indexes, so exactly one
            // foreign side is the normal case and must stay silent. Neither
            // side naming this tiler is a routing bug.
            NoteForeignToken("OnMigration");
            return;
        }
        const uint64_t frequency =
            arriving ? RecordAccessFrequency(new_token) : 0;

        std::lock_guard<std::mutex> lock(mu_);
        if (leaving && !RemoveLocked(old_token)) ++stale_candidates_;
        if (!arriving) return;
        if (!Indexable(new_token) ||
            !InsertLocked(new_token, frequency, Placement::kObserved)) {
            MarkNeedsReconcileLocked();
        }
    }

    std::vector<BlockToken> SelectVictims(size_t target_bytes) override {
        std::vector<BlockToken> victims;
        // A zero target asks for nothing; evicting "just one to be safe" would
        // throw away a block no watermark asked for.
        if (target_bytes == 0) return victims;

        // Coldest first, and only as far as the target requires: an eviction
        // round must not walk the whole tier to reclaim a fraction of it. The
        // chain and the map are read under the same lock, so the join is a
        // consistent view of one record rather than two ages of it.
        std::lock_guard<std::mutex> lock(mu_);
        const auto candidates =
            chain_.CollectColdFirst(config_.max_victim_candidates);
        size_t collected = 0;
        for (const auto& candidate : candidates) {
            const auto id = RegistrationFromChainKey(candidate.key);
            auto entry = id.has_value() ? entries_.find(*id) : entries_.end();
            if (entry == entries_.end()) {
                // Unreachable while both halves move under mu_. If it ever
                // happens the chain holds a name nothing can remove, and only
                // a Reconcile can repair it.
                LOG_FIRST_N(ERROR, 1)
                    << "EvictionIndex chain and entry map disagree";
                MarkNeedsReconcileLocked();
                continue;
            }
            BlockToken token;
            token.key = entry->second.key;
            token.registration_id = entry->first;
            // Weak on purpose: a candidate must not keep the identity it names
            // alive, or the index would decide what still exists.
            token.registration = entry->second.registration;
            token.tiler_id = tiler_id_;
            token.block_id = entry->second.block_id;
            token.size_bytes = entry->second.size_bytes;
            victims.push_back(std::move(token));
            collected += entry->second.size_bytes;
            if (collected >= target_bytes) break;
        }
        return victims;
    }

    void Reconcile(const BlockIndexSnapshot& snapshot) override {
        if (!(snapshot.tiler_id == tiler_id_)) {
            NoteForeignToken("Reconcile");
            return;
        }

        std::lock_guard<std::mutex> lock(mu_);
        ++reconciles_;
        // Sampled before this pass files anything, because the repairs below
        // are this pass's own work and must not count as a race against it.
        const uint64_t mutations_at_entry = mutations_;
        bool filed_every_row = true;

        std::unordered_set<RegistrationId, RegistrationIdHash> authoritative;
        authoritative.reserve(snapshot.entries.size());
        for (const auto& token : snapshot.entries) {
            if (!(token.tiler_id == tiler_id_)) {
                // Not this index's block, so it can neither be filed nor
                // counted as proof of anything about this tiler.
                NoteForeignToken("Reconcile");
                filed_every_row = false;
                continue;
            }
            if (!Indexable(token)) {
                // The same class of update that raises the flag on the commit
                // path. Skipping it silently and then clearing the flag below
                // would report a gap this index really has as repaired.
                MarkNeedsReconcileLocked();
                filed_every_row = false;
                continue;
            }
            authoritative.insert(token.registration_id);
            auto known = entries_.find(token.registration_id);
            // Present *and* naming the same block is the only case that needs
            // nothing: a registration the authority says now holds a
            // different block would otherwise keep the dead one forever.
            if (known != entries_.end() &&
                known->second.block_id == token.block_id) {
                continue;
            }
            if (!InsertLocked(token, EstimatedFrequency(token),
                              Placement::kRepaired)) {
                MarkNeedsReconcileLocked();
                filed_every_row = false;
            }
        }

        DropAbsentLocked(snapshot, authoritative);

        // All three are needed to say "this index is whole": a snapshot of the
        // whole tiler, every row of it filed, and nothing changed here between
        // the capture and now. Clearing the flag on anything less switches off
        // the fallback scan while blocks are still missing from the ordering,
        // and nothing else offers victims.
        if (snapshot.complete && filed_every_row &&
            snapshot.observed_mutations == mutations_at_entry) {
            needs_reconcile_ = false;
        }
    }

    void Clear() override {
        std::lock_guard<std::mutex> lock(mu_);
        ++mutations_;
        // MultiLRU has no bulk clear, so the chain is emptied entry by entry
        // -- from the map, which is the set of names the chain can hold.
        for (const auto& [id, entry] : entries_) {
            chain_.Remove(ChainKeyOf(id));
        }
        entries_.clear();
        tracked_bytes_ = 0;
        // The flag is left exactly as it was: Clear is evidence about this
        // index, not about what the BlockIndex still holds, so teardown must
        // not clear a real gap and dropping state on purpose must not invent
        // one.
    }

    EvictionIndexStats Stats() const override {
        std::lock_guard<std::mutex> lock(mu_);
        EvictionIndexStats stats;
        stats.tracked_blocks = entries_.size();
        stats.tracked_bytes = tracked_bytes_;
        stats.stale_candidates = stale_candidates_;
        stats.reconciles = reconciles_;
        stats.needs_reconcile = needs_reconcile_count_;
        return stats;
    }

    bool NeedsReconcile() const override {
        std::lock_guard<std::mutex> lock(mu_);
        return needs_reconcile_;
    }

    uint64_t MutationCount() const override {
        std::lock_guard<std::mutex> lock(mu_);
        return mutations_;
    }

   private:
    /**
     * @struct Entry
     * @brief One tracked block. Holds names only -- the registration is weak,
     *        there is no allocation here and nothing this struct owns can free
     *        a byte of the tier.
     */
    struct Entry {
        std::string key;  // for reporting and for the frequency fingerprint
        BlockId block_id;
        WeakBlockRegistrationHandle registration;
        size_t size_bytes = 0;
        uint64_t access_count = 0;  // exact; kMultiLru bands on it
        // The mutation this record was installed at. A snapshot captured
        // before it cannot have seen it, so its absence from that snapshot
        // says nothing and must not be read as "the authority dropped it".
        uint64_t installed_at = 0;
    };

    /**
     * @brief Drop the entries `snapshot` is authoritative about and does not
     *        list.
     *
     * An entry the BlockIndex does not have sends the evict engine after a
     * block that can never validate, and it survives every future selection
     * because nothing can delete it -- so dropping is worth doing, but only
     * where the snapshot actually looked. See DroppableLocked.
     */
    void DropAbsentLocked(
        const BlockIndexSnapshot& snapshot,
        const std::unordered_set<RegistrationId, RegistrationIdHash>&
            authoritative) {
        for (auto it = entries_.begin(); it != entries_.end();) {
            if (authoritative.find(it->first) != authoritative.end() ||
                !DroppableLocked(snapshot, it->first, it->second)) {
                ++it;
                continue;
            }
            tracked_bytes_ -= std::min(tracked_bytes_, it->second.size_bytes);
            chain_.Remove(ChainKeyOf(it->first));
            it = entries_.erase(it);
            ++mutations_;
        }
    }

    /**
     * @brief Is `snapshot` proof that this entry is gone from the authority?
     *
     * Only for what it read. A newer record than the capture may simply be a
     * commit that landed in the window, and a per-shard snapshot says nothing
     * about the other shards -- walking the shards in turn and dropping on
     * each one's authority would leave only the last shard's blocks.
     */
    bool DroppableLocked(const BlockIndexSnapshot& snapshot,
                         const RegistrationId& id, const Entry& entry) const {
        if (entry.installed_at > snapshot.observed_mutations) return false;
        if (snapshot.complete) return true;
        if (snapshot.shard_count == 0) return false;
        return SnapshotShardOf(id, snapshot.shard_count) == snapshot.shard_id;
    }

    /**
     * @brief Reject a token that names another tiler.
     *
     * Absorbing it silently would let this index offer a victim whose
     * BlockIndex it cannot even name, so it is counted as the programming
     * error it is.
     */
    bool Owns(const BlockToken& token, const char* where) {
        if (token.tiler_id == tiler_id_) return true;
        NoteForeignToken(where);
        return false;
    }

    /**
     * @brief Count a rejected token and report the first one.
     *
     * Logged once at ERROR because a mis-routed caller produces one per
     * operation and would otherwise drown the log; the running count stays
     * visible at VLOG level for anyone chasing the rest.
     */
    void NoteForeignToken(const char* where) {
        const uint64_t count =
            foreign_tokens_.fetch_add(1, std::memory_order_relaxed) + 1;
        std::call_once(foreign_logged_, [this, where] {
            LOG(ERROR) << "EvictionIndex for tiler " << tiler_id_
                       << " was handed a token for another tiler in " << where
                       << "; the token is rejected, not indexed";
        });
        VLOG(1) << "EvictionIndex for tiler " << tiler_id_ << " rejected "
                << count << " foreign tokens, latest in " << where;
    }

    /**
     * @brief A token with no registration cannot be filed under one.
     *
     * BlockRegistry hands out sequences starting at 1, so zero means the
     * caller never registered the key -- an update this index cannot apply.
     */
    static bool Indexable(const BlockToken& token) {
        return token.registration_id.shard_sequence != 0;
    }

    /**
     * @brief Fold a write or read of `token` into the shared sketch.
     *
     * Deliberately outside mu_: the sketch has its own lock and nesting two
     * independent mutexes for no reason is how lock orders get invented.
     */
    uint64_t RecordAccessFrequency(const BlockToken& token) {
        if (ordering_ != Ordering::kTinyLfuLru) return 0;
        return sketch_.IncrementAndEstimate(KeyFingerprint(token.key));
    }

    /**
     * @brief The same estimate without counting the observation.
     *
     * The one call site (Reconcile) is under mu_, which is safe in the one
     * direction that matters: the sketch lock is never held while mu_ is
     * taken, so the two can never form a cycle.
     */
    uint64_t EstimatedFrequency(const BlockToken& token) const {
        if (ordering_ != Ordering::kTinyLfuLru) return 0;
        return sketch_.Estimate(KeyFingerprint(token.key));
    }

    /**
     * @brief The band input for `entry` as it stands, counting nothing.
     *
     * kLru returns 0 for everything, which parks the whole tier in the cold
     * band and leaves MultiLRU behaving as one plain recency list -- reusing
     * the chain rather than maintaining a second kind of it.
     */
    uint64_t BandInputLocked(const Entry& entry,
                             uint64_t sketch_frequency) const {
        switch (ordering_) {
            case Ordering::kLru:
                return 0;
            case Ordering::kMultiLru:
                return entry.access_count;
            case Ordering::kTinyLfuLru:
                break;
        }
        return sketch_frequency;
    }

    /** Count the touch on `entry` and return the band input for it. */
    uint64_t TouchLocked(Entry& entry, uint64_t sketch_frequency) {
        entry.access_count += 1;
        return BandInputLocked(entry, sketch_frequency);
    }

    /**
     * @brief Install `token`, replacing any older block for the same
     *        registration. Returns false if the record could not be made
     *        whole.
     */
    bool InsertLocked(const BlockToken& token, uint64_t frequency,
                      Placement placement) {
        // Whatever happens below changes what this index holds: on success a
        // record is installed, on failure any older one for the registration
        // is rolled back out. A snapshot captured before this call cannot be
        // used to judge either.
        ++mutations_;
        const std::string chain_key = ChainKeyOf(token.registration_id);
        const size_t size_bytes = token.size_bytes;
        // Read, and charged off, before anything below can throw: the
        // rollback erases this registration's record on every failing path,
        // so the bytes of the record being replaced have to leave the total
        // on that path too or tracked_bytes_ drifts up for good.
        auto existing = entries_.find(token.registration_id);
        // A re-commit of the same registration keeps its heat: the block
        // moved, the key did not become cold.
        const uint64_t inherited_count =
            existing == entries_.end() ? 0 : existing->second.access_count;
        if (existing != entries_.end()) {
            tracked_bytes_ -=
                std::min(tracked_bytes_, existing->second.size_bytes);
        }
        try {
            Entry entry;
            entry.key = token.key;
            entry.block_id = token.block_id;
            entry.registration = token.registration;
            entry.size_bytes = size_bytes;
            entry.installed_at = mutations_;
            entry.access_count = inherited_count;
            // A repair counts as neither an access nor a recency fact: the
            // band comes from what was already recorded for the key, and the
            // chain node enters at the cold end of that band.
            const uint64_t band = placement == Placement::kObserved
                                      ? TouchLocked(entry, frequency)
                                      : BandInputLocked(entry, frequency);
            entries_.insert_or_assign(token.registration_id, std::move(entry));
            if (placement == Placement::kObserved) {
                chain_.Insert(chain_key, size_bytes, band);
            } else {
                chain_.InsertColdest(chain_key, size_bytes, band);
            }
        } catch (const std::exception& error) {
            // Half a record is worse than none: an entry with no chain node is
            // invisible to SelectVictims and can never be offered again, and a
            // chain node with no entry is offered forever and can never be
            // removed. Undo both halves -- neither removal allocates, and the
            // chain insert is all-or-nothing -- and let Reconcile put the
            // block back.
            entries_.erase(token.registration_id);
            chain_.Remove(chain_key);
            LOG(ERROR) << "EvictionIndex could not index a committed block: "
                       << error.what();
            return false;
        }
        tracked_bytes_ += size_bytes;
        return true;
    }

    /**
     * @brief Token-scoped removal.
     * @return false when this index holds no such block, which is what makes
     *         the caller's candidate stale rather than this a lost entry.
     */
    bool RemoveLocked(const BlockToken& token) {
        auto entry = entries_.find(token.registration_id);
        if (entry == entries_.end()) return false;
        // The delete names one (registration, block). A newer commit for the
        // same registration may already have installed a different block, and
        // erasing on the old one's behalf would hide a live block from
        // eviction for good. An unset block id means "whatever it holds now",
        // which is what a plain key delete reports.
        if (!(token.block_id == BlockId{}) &&
            !(entry->second.block_id == token.block_id)) {
            return false;
        }
        tracked_bytes_ -= std::min(tracked_bytes_, entry->second.size_bytes);
        entries_.erase(entry);
        chain_.Remove(ChainKeyOf(token.registration_id));
        ++mutations_;
        return true;
    }

    void MarkNeedsReconcileLocked() {
        needs_reconcile_ = true;
        ++needs_reconcile_count_;
    }

    const EvictionIndexConfig config_;
    const Ordering ordering_;
    const UUID tiler_id_;

    FrequencySketch sketch_;  // has its own internal synchronization
    std::atomic<uint64_t> foreign_tokens_{0};
    std::once_flag foreign_logged_;

    mutable std::mutex mu_;
    MultiLRU chain_;  // has its own lock, always taken inside mu_
    std::unordered_map<RegistrationId, Entry, RegistrationIdHash> entries_;
    size_t tracked_bytes_ = 0;
    // Membership changes only. An access reorders but adds and removes
    // nothing, and counting it would keep a read-heavy tier from ever
    // proving a reconcile pass raced nothing.
    uint64_t mutations_ = 0;
    bool needs_reconcile_ = false;
    uint64_t stale_candidates_ = 0;
    uint64_t reconciles_ = 0;
    uint64_t needs_reconcile_count_ = 0;
};

}  // namespace

size_t SnapshotShardOf(const RegistrationId& id, size_t shard_count) {
    if (shard_count == 0) return 0;
    return RegistrationIdHash{}(id) % shard_count;
}

tl::expected<void, ErrorCode> ValidateEvictionIndexConfig(
    const EvictionIndexConfig& config) {
    if (!OrderingFromType(config.type).has_value()) {
        LOG(ERROR) << "Unknown eviction index type '" << config.type
                   << "'; expected lru, multi_lru or tinylfu_lru";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.max_victim_candidates == 0) {
        LOG(ERROR) << "eviction_index.max_victim_candidates must be greater "
                      "than zero; zero would turn every eviction round into a "
                      "no-op that still reports itself healthy";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Checked against the resolved values, not the raw ones: a config that
    // sets only `warm` above the default `hot` is just as inconsistent as one
    // that sets both the wrong way round.
    const BandThresholds thresholds = ResolveThresholds(config);
    if (thresholds.hot <= thresholds.warm ||
        thresholds.very_hot <= thresholds.hot) {
        LOG(ERROR) << "eviction_index band thresholds must be strictly "
                      "increasing, got warm="
                   << thresholds.warm << " hot=" << thresholds.hot
                   << " veryhot=" << thresholds.very_hot;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<std::unique_ptr<EvictionIndex>, ErrorCode> CreateEvictionIndex(
    const EvictionIndexConfig& config, const UUID& tiler_id, size_t capacity) {
    auto valid = ValidateEvictionIndexConfig(config);
    if (!valid) return tl::make_unexpected(valid.error());
    if (IsZeroUUID(tiler_id)) {
        // Every token is matched against this id; the all-zero sentinel would
        // make a default-constructed token look like it belonged here.
        LOG(ERROR) << "CreateEvictionIndex needs a real tiler id";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return std::make_unique<OrderedEvictionIndex>(
        config, *OrderingFromType(config.type), tiler_id, capacity);
}

}  // namespace mooncake::v2
