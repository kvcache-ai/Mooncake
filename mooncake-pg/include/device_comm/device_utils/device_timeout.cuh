#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_DEVICE_TIMEOUT_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_DEVICE_TIMEOUT_CUH

#include <cstdint>

#include <cuda_alike.h>

namespace mooncake {

// A zero timeout permits an indefinite wait. Unsigned subtraction also handles
// a clock64() wrap between the start of the wait and this check.
__device__ __forceinline__ bool deviceTimedOut(uint64_t start,
                                               uint64_t timeout_ticks) {
    return timeout_ticks != 0 && clock64() - start >= timeout_ticks;
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_DEVICE_TIMEOUT_CUH
