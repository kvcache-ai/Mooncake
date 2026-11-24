#include "eviction/offloading_reclaimer.h"

#include <utility>

namespace mooncake {
namespace eviction {

uint64_t OffloadingReclaimer::TryReserveOffloadSlot(const Report& so_far) {
    if (options_.cap <= 0) {
        return 0;
    }
    const uint64_t cap = static_cast<uint64_t>(options_.cap);
    if (so_far.Count(Outcome::kOffloadQueued) >= cap) {
        return 0;
    }
    uint64_t cur = run_offloads_.load(std::memory_order_relaxed);
    while (cur < cap) {
        if (run_offloads_.compare_exchange_weak(cur, cur + 1,
                                                std::memory_order_relaxed)) {
            return cur + 1;
        }
    }
    return 0;
}

tl::expected<Effect, ErrorCode> OffloadingReclaimer::Apply(
    metadata::Namespace& ns, const route::WriteGuard& hold, const Plan& plan,
    const Report& so_far) {
    ObjectMetadata& metadata = hold.metadata();
    // A local-disk replica whose removal awaits durability no longer keeps
    // the data, so it does not spare the object its offload.
    if (metadata.HasReplica([](const Replica& replica) {
            return replica.is_local_disk_replica() && replica.is_completed();
        })) {
        consecutive_rejections_.store(0, std::memory_order_relaxed);
        return inner_.Apply(ns, hold, plan, so_far);
    }
    // The replica an offload in flight pinned keeps the data until it lands.
    if (queue_.HasInFlight(ns.id(), hold.key())) {
        consecutive_rejections_.store(0, std::memory_order_relaxed);
        return inner_.Apply(ns, hold, plan, so_far);
    }

    const bool enforce_cap = options_.force_evict || options_.cap > 0;
    uint64_t reserved_slot = 0;
    if (enforce_cap) {
        reserved_slot = TryReserveOffloadSlot(so_far);
        if (reserved_slot == 0) {
            if (options_.force_evict) {
                return WithoutOffload(ns, hold, plan, so_far,
                                      DropReason::kOffloadCap);
            }
            Effect kept;
            kept.stop_reason = StopReason::kOffloadCap;
            return kept;
        }
    }

    for (const ReplicaID id : plan.release) {
        Replica* source = metadata.GetReplicaByID(id);
        if (source == nullptr) {
            continue;
        }
        // StartOffload pins `source` before publishing the task so any
        // concurrent completion/cancellation sees refcnt >= 1 once it acquires
        // the key lock.
        if (!queue_.StartOffload(ns.id(), hold.key(), *source)) {
            continue;
        }

        consecutive_rejections_.store(0, std::memory_order_relaxed);
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
            if (!options_.force_evict && options_.cap > 0 &&
                reserved_slot >= static_cast<uint64_t>(options_.cap)) {
                effect->stop_reason = StopReason::kOffloadCap;
            }
        }
        return effect;
    }

    if (reserved_slot != 0) {
        run_offloads_.fetch_sub(1, std::memory_order_relaxed);
    }
    if (options_.force_evict) {
        return WithoutOffload(ns, hold, plan, so_far,
                              DropReason::kOffloadRejected);
    }
    const uint64_t streak =
        consecutive_rejections_.fetch_add(1, std::memory_order_relaxed) + 1;
    Effect kept;
    if (streak >= kMaxConsecutiveRejections) {
        kept.stop_reason = StopReason::kOffloadSaturated;
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
