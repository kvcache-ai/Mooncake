#pragma once

#include <string>
#include <string_view>

#include "lease.h"

namespace mooncake {

// The group name and deadline have the same lifetime, including when a
// reader of the eviction index still holds an old group incarnation.
struct NamedGroupLease : Lease {
    explicit NamedGroupLease(std::string_view name) : group_id(name) {}
    const std::string group_id;
};

}  // namespace mooncake
