#pragma once

// Evictor: frees memory by taking replicas off objects whose lease has run
// out, for the whole pool or for one namespace.
//
// Namespaces do not share their cache, so a run holds each to its own share of
// the goal: it walks one namespace's EvictionIndex cursor at a time, objects
// whose lease ran out first, until that namespace has given up its share, and
// then comes back to the objects the policy deferred while the namespace is
// short of its share of the minimum. What one namespace cannot give up is not
// made up by another, so each namespace's outcome is told by its own objects
// alone. With one namespace this is BatchEvict's order today.
//
// The evictor runs the loop and keeps its invariants; what varies is
// injected:
//
// - Each namespace's EvictionIndex decides the order objects are visited in.
// - The EvictionPolicy decides which objects qualify, which it defers, and
//   which of their replicas go.
// - The Reclaimer takes those replicas off and says what came of it.
// - EvictionObservers are told what happened and change nothing.
//
// What no policy overrides: a hard-pinned object, or one whose lease has not
// run out, is left alone; a group, which the index hands out as one entry,
// goes by its shared lease, its members judged one at a time under their own
// locks; an object reclaiming left with nothing valid is torn down under the
// lock it was reclaimed under. The fallback comes back to each deferred object
// on its own.
//
// Locks: the evictor holds at most one key lock at a time and no lock of its
// own, and frees the replicas it took only once that key lock is let go. What
// the caller holds across Run, it holds for the whole run.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "eviction/observer.h"
#include "eviction/policy.h"
#include "eviction/reclaimer.h"
#include "eviction/report.h"
#include "metadata/namespace_table.h"

namespace mooncake {
namespace eviction {

// How much a run frees, counted in objects that released memory, as shares
// of the objects each namespace's index filed when the run began. The caller
// sets it from the memory pressure it sees; the policy only says which objects
// may go. A ratio above 1 asks for every eligible object; one that is negative
// or not a number is logged and taken as 0.
struct ObjectGoal {
    // Each namespace frees the objects the policy takes outright up to this
    // share of what it filed, rounded up.
    double target_ratio = 0;
    // Each namespace frees the objects the policy deferred only while it is
    // short of this share, and only once nothing else qualifies. A minimum
    // above the target is lowered to it.
    double min_ratio = 0;
};

// How much a run in one namespace frees, in released bytes: from the objects
// the policy takes outright, then from those it deferred.
struct ByteGoal {
    uint64_t target_bytes = 0;
};

class Evictor {
   public:
    // Everything given outlives the evictor.
    Evictor(metadata::NamespaceTable& namespaces, const EvictionPolicy& policy,
            Reclaimer& reclaimer, std::vector<EvictionObserver*> observers);
    Evictor(const Evictor&) = delete;
    Evictor& operator=(const Evictor&) = delete;

    // Evicts within `scope`, or in `tenant_id`'s namespace, until `goal` is
    // met, nothing more qualifies, or an OpLog write fails. Namespaces created
    // during the run join the next one.
    Report Run(const Scope& scope, const ObjectGoal& goal);
    Report Run(const TenantId& tenant_id, const ByteGoal& goal);

   private:
    struct Aim;
    class Pass;

    Report RunFor(const Scope& scope, const Aim& aim);

    metadata::NamespaceTable& namespaces_;
    const EvictionPolicy& policy_;
    Reclaimer& reclaimer_;
    const std::vector<EvictionObserver*> observers_;
    // Where the next run starts among the namespaces it visits.
    std::atomic<size_t> next_start_{0};
};

}  // namespace eviction
}  // namespace mooncake
