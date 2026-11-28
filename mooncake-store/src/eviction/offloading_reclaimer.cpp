#include "eviction/offloading_reclaimer.h"

#include <utility>

#include "metadata/object_state.h"
#include "object_runtime_state.h"

namespace mooncake {
namespace eviction {

tl::expected<Effect, ErrorCode> OffloadingReclaimer::Apply(
    metadata::Namespace& ns, const route::WriteGuard& hold, const Plan& plan,
    const Report& so_far) {
    ObjectMetadata& metadata = hold.metadata();
    // A local-disk replica whose removal awaits durability no longer keeps
    // the data, so it does not spare the object its offload.
    if (metadata.HasReplica([](const Replica& replica) {
            return replica.is_local_disk_replica() && replica.is_completed();
        })) {
        return inner_.Apply(ns, hold, plan, so_far);
    }
    // The replica an offload in flight pinned keeps the data until it lands.
    if (hold.state().offloading_task.has_value()) {
        return inner_.Apply(ns, hold, plan, so_far);
    }
    if (!options_.async && options_.force_evict &&
        static_cast<long>(so_far.Count(Outcome::kOffloadQueued)) >=
            options_.cap) {
        return WithoutOffload(ns, hold, plan, so_far, DropReason::kOffloadCap);
    }
    const bool pressure =
        options_.async && pressure_.load(std::memory_order_relaxed);
    // Drop what cannot be queued: always with force_evict, but async only
    // under pressure.
    const bool may_drop = options_.force_evict && (!options_.async || pressure);
    route::ObjectState& state = hold.state();
    const Clock::time_point now = Clock::now();
    // A copy of this object failed or could not be queued a moment ago, or
    // the run queues nothing new.
    if (options_.async && (!state.OffloadRetryDue(now) ||
                           !queueing_.load(std::memory_order_relaxed))) {
        if (may_drop) {
            return WithoutOffload(ns, hold, plan, so_far,
                                  DropReason::kOffloadRejected);
        }
        return Effect{};
    }

    bool owner_full = false;
    bool rejected = false;
    for (const ReplicaID id : plan.release) {
        Replica* source = metadata.GetReplicaByID(id);
        if (source == nullptr) {
            continue;
        }
        std::vector<UUID> mirror_clients;
        OffloadQueue::Pushed pushed =
            queue_.Push(ns.id(), hold.key(), *source, &mirror_clients);
        if (!pushed.queued()) {
            (pushed.result == OffloadQueue::Pushed::Result::kOwnerFull
                 ? owner_full
                 : rejected) = true;
            continue;
        }
        source->inc_refcnt();
        state.offload_retry_after = 0;
        state.offloading_task = OffloadingTask{
            id, now, std::move(mirror_clients), std::move(pushed.credit)};

        Plan rest;
        for (const ReplicaID other : plan.release) {
            if (other != id) {
                rest.release.push_back(other);
            }
        }
        auto effect = rest.release.empty()
                          ? tl::expected<Effect, ErrorCode>(Effect{})
                          : inner_.Apply(ns, hold, rest, so_far);
        if (effect) {
            effect->outcome = Outcome::kOffloadQueued;
            effect->offload_bytes = metadata.TotalLength();
        }
        return effect;
    }

    if (may_drop) {
        return WithoutOffload(ns, hold, plan, so_far,
                              DropReason::kOffloadRejected);
    }
    Effect kept;
    if (options_.async) {
        // A destination that refused for another reason than a full queue
        // (no disk, not serving) is not asked again for a while. A full one
        // is: the key keeps its place for the room that frees up next.
        if (rejected) {
            state.DelayOffloadRetry(now + kRetryBackoff);
        }
        // Every destination full and no pressure to free anything else:
        // nothing later in this run can be queued.
        kept.exhausted = owner_full && !pressure && queue_.AllFull();
    }
    return kept;
}

tl::expected<Effect, ErrorCode> OffloadingReclaimer::WithoutOffload(
    metadata::Namespace& ns, const route::WriteGuard& hold, const Plan& plan,
    const Report& so_far, DropReason reason) {
    auto effect = inner_.Apply(ns, hold, plan, so_far);
    if (effect) {
        effect->reason = reason;
    }
    return effect;
}

}  // namespace eviction
}  // namespace mooncake
