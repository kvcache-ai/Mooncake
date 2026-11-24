#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_LL_PEER_SCOPE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_LL_PEER_SCOPE_H

#include "common_types.h"

namespace mooncake {

#if defined(__CUDACC__)
__host__ __device__
#endif
    inline constexpr bool
    llIncludesPeer(uint64_t peers, uint32_t index) noexcept {
    return index < kMaxNumRanks && (peers & (uint64_t{1} << index)) != 0;
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_LL_PEER_SCOPE_H
