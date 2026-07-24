#pragma once

#include <functional>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake {

// Published after the master has reclaimed the resources associated with an
// expired client lease. Async consumers must copy the event before returning
// from the callback.
struct ClientLeaseExpiredEvent {
    UUID client_id;
    std::vector<std::string> unmounted_memory_segment_names;
};

using ClientLeaseExpiredCallback =
    std::function<void(const ClientLeaseExpiredEvent&)>;

}  // namespace mooncake
