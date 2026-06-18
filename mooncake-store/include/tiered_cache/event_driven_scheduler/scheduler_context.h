#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include "types.h"  // UUID

namespace mooncake {

/**
 * Context objects passed to the scheduler hooks.
 *
 * Bundling hook parameters in a struct lets the hook payload grow (e.g. adding
 * `served_tier_id`) without churning every hook signature and every call site.
 */

struct AccessContext {
    std::string_view key;
    UUID served_tier_id;    // tier that actually served this access
    size_t size_bytes = 0;  // size of the served replica (0 if unknown)
};

struct CommitContext {
    std::string_view key;
    UUID tier_id;               // tier the replica was committed to
    size_t size_bytes;          // committed replica size
    uint64_t new_version = 0;   // metadata version after commit
    bool record_access = true;  // whether this commit counts as an access
};

struct DeleteContext {
    std::string_view key;
    std::optional<UUID> tier_id;  // nullopt => all replicas removed
};

struct AllocationFailureContext {
    UUID tier_id;          // tier whose allocation failed
    size_t required_bytes;  // bytes the failed allocation needed
};

}  // namespace mooncake
