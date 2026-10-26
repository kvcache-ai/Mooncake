#pragma once

// EvictionPolicy: which objects eviction may take memory from, and which of
// their replicas it takes. The evictor enforces what no policy may override,
// that a hard-pinned object or one whose lease has not run out is left alone,
// and asks the policy about the rest.
//
// A policy may also defer an object: take it only when the run falls short of
// its minimum without it (a soft pin, say). The evictor comes back to deferred
// objects once nothing else qualifies, and asks again.

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "object_metadata.h"
#include "replica.h"
#include "tenant_id.h"

namespace mooncake {
namespace eviction {

using Clock = std::chrono::system_clock;

// An object the evictor may take memory from, as the policy sees it under the
// object's write lock.
struct Candidate {
    const TenantId& tenant_id;
    const std::string& key;
    const ObjectMetadata& metadata;
    Clock::time_point now;
    // Set when the evictor comes back to an object the policy deferred.
    bool fallback;
};

// The replicas to take off the candidate.
struct Plan {
    std::vector<ReplicaID> release;
};

enum class SkipReason : uint8_t {
    // The index filed the key ahead of its real rank.
    kMoved,
    kHardPinned,
    kLeaseActive,
    // The policy takes it only to make up the minimum; see
    // Candidate::fallback.
    kDeferred,
    // The policy never takes it while soft pinned.
    kSoftPinned,
    // No replica the policy would release.
    kNothingToRelease,
};

class EvictionPolicy {
   public:
    // The replicas to release from `candidate`, or why it is skipped. It may
    // not change the object.
    [[nodiscard]] virtual tl::expected<Plan, SkipReason> Decide(
        const Candidate& candidate) const = 0;

   protected:
    ~EvictionPolicy() = default;
};

}  // namespace eviction
}  // namespace mooncake
