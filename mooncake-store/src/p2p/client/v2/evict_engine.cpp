#include "p2p/client/v2/evict_engine.h"

#include <algorithm>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace mooncake::v2 {

EvictEngine::EvictEngine(MultiTiler* tilers, BlockRegistry* registry,
                         MetadataCallbacks* callbacks,
                         std::shared_ptr<Clock> clock,
                         std::function<void(const std::string&)> on_evicted)
    : tilers_(tilers),
      registry_(registry),
      callbacks_(callbacks),
      clock_(std::move(clock)),
      on_evicted_(std::move(on_evicted)) {
    CHECK(tilers_ != nullptr && registry_ != nullptr && clock_ != nullptr)
        << "EvictEngine requires tilers, a registry and a clock";
}

EvictEngine::EvictOutcome EvictEngine::EvictVictim(TilerManager& tiler,
                                                   const BlockToken& victim) {
    auto registration = victim.registration.Lock();
    if (!registration.has_value() || registration->IsRetired()) return {};
    if (!registry_->IsCanonical(*registration)) return {};

    size_t size_bytes = 0;
    {
        // Under the key's mutation guard, so a concurrent Delete or migration
        // of the same key cannot interleave with this decision.
        auto guard = registration->LockMutation();
        if (guard.IsRetired() || !registry_->IsCanonical(*registration)) {
            return {};
        }

        auto current = tiler.Match(*registration);
        if (!current || !(current->Id() == victim.block_id)) {
            // The token named a block that has since been replaced; removing
            // the current one on its behalf would drop live data.
            //
            // Repair the ordering here, where both facts are in hand. Dropping
            // the dead token alone would lose the replacement's only record --
            // the index is keyed by registration -- and a block with no record
            // is never offered as a victim again, so its bytes are stranded
            // for as long as the tier lives.
            if (current) {
                BlockToken live = victim;
                live.block_id = current->Id();
                live.size_bytes = current->Size();
                tiler.Eviction()->OnCommit(live);
            }
            return {};
        }
        size_bytes = current->Size();
        // Released before the erase, so the physical bytes can actually come
        // back rather than being held by this very function.
        current.value() = ImmutableBlock();

        // Unconditional. No walk of the other tilers, no "is this the last
        // copy" question, no demotion: reclamation is tier-local, and
        // durability is the offload pipeline's job (section 4.1). Asking the
        // question here would read every other tier under this key's mutation
        // guard, and acting on the answer would copy a whole block on a path a
        // request thread is waiting on.
        auto removed = tiler.Delete(*registration, victim.block_id,
                                    /*defer_notify=*/true);
        if (!removed) return {};
    }

    // Outside the guard: a publish under the key's mutation guard can run a
    // whole inline fan-out while the key is locked. This is also what marks
    // the tier absent on the registration, so the presence read below sees
    // the world after this removal.
    tiler.NotifyDeleted(*registration, victim.block_id, size_bytes);

    // Observed, not consulted. The set of tiers holding this key lives on the
    // registration and is maintained by whoever inserts or deletes, so reading
    // it costs one lock on that registration and touches no other tier -- it
    // is not the cross-tier scan section 4.1 forbids, and it comes after a
    // removal it could not have prevented. Two things need it: the access
    // tracker should stop tracking a key that no longer exists anywhere, and
    // the trade-off this design accepts has to be countable.
    bool sole_replica = true;
    for (const UUID& holder : registration->PresenceHint()) {
        if (!(holder == tiler.Id())) {
            sole_replica = false;
            break;
        }
    }
    if (sole_replica) {
        LOG(WARNING) << "Evicted the last replica of key="
                     << registration->Key() << " from tier " << tiler.Id()
                     << "; the object is gone. Offload had not copied it to a "
                        "slower tier before this tier filled up.";
    }

    {
        std::lock_guard<std::mutex> lock(stats_mu_);
        ++stats_.victims_detached;
        if (sole_replica) ++stats_.victims_sole_replica;
    }
    return EvictOutcome{size_bytes, sole_replica};
}

tl::expected<ReclaimResult, ErrorCode> EvictEngine::ReclaimOneRound(
    const ReclaimRequest& request) {
    TilerManager* tiler = tilers_->Find(request.tiler_id);
    if (tiler == nullptr) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    {
        std::lock_guard<std::mutex> lock(stats_mu_);
        ++stats_.rounds;
    }

    ReclaimResult result;
    result.free_bytes_before = tiler->FreeBytes();

    // From the tier's own ordering, not from the placement policy. Victim
    // selection is a property of one tier's contents, and keeping it beside
    // that tier's BlockIndex is what lets both be updated in a single
    // mutation instead of drifting apart.
    const auto victims =
        tiler->Eviction()->SelectVictims(request.reclaim_target_bytes);
    // Every eviction is now a plain removal, so every one of them has to be
    // reported to Master. orphaned_keys is the subset whose last replica went
    // with it, which is what the access tracker needs to hear about.
    std::vector<std::string> removed_keys;
    std::vector<std::string> orphaned_keys;
    for (const auto& victim : victims) {
        if (clock_->Now() >= request.deadline) {
            // A request thread is waiting on this; overrunning the deadline
            // would turn a full tier into a latency cliff.
            result.deadline_reached = true;
            std::lock_guard<std::mutex> lock(stats_mu_);
            ++stats_.deadline_reached;
            break;
        }
        ++result.candidates_examined;
        const EvictOutcome outcome = EvictVictim(*tiler, victim);
        if (outcome.bytes == 0) {
            // The candidate named a block the index no longer has, so the
            // ordering has to forget it too -- otherwise the same dead token
            // is offered first on every subsequent round and the tier stops
            // making progress. Token-scoped, so a newer block for the same
            // registration keeps its record.
            tiler->Eviction()->OnDelete(victim);
            std::lock_guard<std::mutex> lock(stats_mu_);
            ++stats_.victims_stale;
            continue;
        }
        result.logically_detached_bytes += outcome.bytes;
        removed_keys.push_back(victim.key);
        if (outcome.was_sole_replica) orphaned_keys.push_back(victim.key);
        // No separate bookkeeping call: TilerManager::Delete already removed
        // the record from this tier's eviction index, in the same mutation
        // that removed it from the BlockIndex, so a later round in this same
        // call cannot be offered a victim this one already reclaimed.
        if (result.logically_detached_bytes >= request.reclaim_target_bytes) {
            break;
        }
    }

    // Callbacks outside every guard. Master tracks replicas per segment, so
    // every replica this engine deleted is reported -- including the ones that
    // were the object's last. Skipping the report for a destroyed object would
    // leave Master routing readers to a replica that no longer exists, which
    // turns accepted data loss into an unexplained read failure.
    if (callbacks_ != nullptr && callbacks_->remove_replica) {
        for (const auto& key : removed_keys) {
            auto notified = callbacks_->remove_replica(key, request.tiler_id);
            if (!notified) {
                LOG(WARNING)
                    << "remove-replica callback failed after evicting " << key;
            }
        }
    }
    // The access tracker is per key, not per replica, so it only forgets a key
    // once nothing holds it any more.
    if (on_evicted_) {
        for (const auto& key : orphaned_keys) on_evicted_(key);
    }

    result.free_bytes_after = tiler->FreeBytes();
    // Only what the pool actually gave back counts. A victim still held by a
    // reader or a pinned lease is detached but not yet free, and reporting it
    // as reclaimed would tell the caller to expect an allocation to succeed
    // when nothing was released.
    result.physically_reclaimed_bytes =
        result.free_bytes_after > result.free_bytes_before
            ? result.free_bytes_after - result.free_bytes_before
            : 0;
    return result;
}

tl::expected<void, ErrorCode> EvictEngine::EvictOne(const BlockToken& token) {
    TilerManager* tiler = tilers_->Find(token.tiler_id);
    if (tiler == nullptr) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    const EvictOutcome outcome = EvictVictim(*tiler, token);
    if (outcome.bytes == 0) {
        std::lock_guard<std::mutex> lock(stats_mu_);
        ++stats_.victims_stale;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (callbacks_ != nullptr && callbacks_->remove_replica) {
        auto notified = callbacks_->remove_replica(token.key, token.tiler_id);
        if (!notified) {
            LOG(WARNING) << "remove-replica callback failed after evicting "
                         << token.key;
        }
    }
    if (on_evicted_ && outcome.was_sole_replica) on_evicted_(token.key);
    return {};
}

EvictStats EvictEngine::Stats() const {
    std::lock_guard<std::mutex> lock(stats_mu_);
    return stats_;
}

}  // namespace mooncake::v2
