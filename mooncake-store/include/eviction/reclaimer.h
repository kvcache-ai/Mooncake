#pragma once

// Reclaimer: takes the replicas a plan names off an object, and decides
// nothing about which. How the memory goes is the reclaimer's: freed on the
// spot, recorded in the OpLog and freed once durable, or copied to local disk
// first (OffloadingReclaimer, layered over one of the others). What came of it
// is the Effect it returns.

#include <ylt/util/tl/expected.hpp>

#include "eviction/policy.h"
#include "eviction/report.h"
#include "metadata/namespace.h"
#include "metadata/object_route.h"
#include "types.h"

namespace mooncake {
namespace eviction {

class Reclaimer {
   public:
    // Takes `plan`'s replicas off the object `hold` holds, under that write
    // lock, which it neither lets go nor uses to reach the route. `so_far` is
    // what the run has done before this object. An error, from an OpLog
    // write, means nothing was taken and the run stops.
    [[nodiscard]] virtual tl::expected<Effect, ErrorCode> Apply(
        metadata::Namespace& ns, const route::WriteGuard& hold,
        const Plan& plan, const Report& so_far) = 0;
    // Tears down the object `hold` holds, which reclaiming left with nothing
    // valid, under the lock Apply ran under.
    virtual void TearDown(metadata::Namespace& ns,
                          const route::WriteGuard& hold) = 0;

   protected:
    ~Reclaimer() = default;
};

}  // namespace eviction
}  // namespace mooncake
