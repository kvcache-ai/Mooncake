#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_DEVICE_UTILS_CUH
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_DEVICE_UTILS_CUH

#include <cstdint>

#include <cuda_alike.h>

namespace mooncake {

[[nodiscard]] __device__ __forceinline__ uint64_t minimum(uint64_t left,
                                                          uint64_t right) {
    return left < right ? left : right;
}

[[nodiscard]] __host__ __device__ __forceinline__ constexpr uint64_t ceilDiv(
    uint64_t value, uint64_t divisor) {
    return value / divisor + (value % divisor != 0 ? 1 : 0);
}

#if defined(__CUDACC__)
// A zero timeout permits an indefinite wait. Unsigned subtraction also handles
// a clock64() wrap between the start of the wait and this check.
__device__ __forceinline__ bool deviceTimedOut(uint64_t start,
                                               uint64_t timeout_ticks) {
    return timeout_ticks != 0 && clock64() - start >= timeout_ticks;
}
#endif

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_UTILS_DEVICE_UTILS_CUH
