#include "eviction/offloading_reclaimer.h"

#include <utility>

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
    if (options_.force_evict && static_cast<long>(so_far.Count(
                                    Outcome::kOffloadQueued)) >= options_.cap) {
        return WithoutOffload(ns, hold, plan, so_far, DropReason::kOffloadCap);
    }

    for (const ReplicaID id : plan.release) {
        Replica* source = metadata.GetReplicaByID(id);
        std::vector<UUID> mirror_clients;
        if (source == nullptr ||
            !queue_.Push(ns.id(), hold.key(), *source, &mirror_clients)) {
            continue;
        }
        source->inc_refcnt();
        hold.state().offloading_task =
            OffloadingTask{id, Clock::now(), std::move(mirror_clients)};

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

    if (options_.force_evict) {
        return WithoutOffload(ns, hold, plan, so_far,
                              DropReason::kOffloadRejected);
    }
    return Effect{};
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
