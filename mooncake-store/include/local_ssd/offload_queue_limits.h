#pragma once

#include <cstdint>

namespace mooncake {

inline constexpr bool DEFAULT_OFFLOADING_QUEUE_USE_BYTES = false;
inline constexpr uint64_t DEFAULT_OFFLOADING_QUEUE_LIMIT_BYTES =
    100ULL * 1000 * 1000 * 1000;

// Capacity mode is fixed at construction; count mode keeps existing behavior.
struct OffloadQueueLimits {
    uint64_t max_pending_bytes = DEFAULT_OFFLOADING_QUEUE_LIMIT_BYTES;
    bool use_bytes = DEFAULT_OFFLOADING_QUEUE_USE_BYTES;
};

}  // namespace mooncake
