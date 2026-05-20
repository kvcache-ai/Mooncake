#pragma once

#include <tent/device/ir/device_ops.cuh>
#include <tent/device/ir/ep_comm_ops.cuh>

namespace mooncake::tent::device {

template <typename NetworkBackend, typename PlatformBackend>
__device__ __forceinline__ EpCommOps buildEpCommOps(void* ctx) {
    return NetworkBackend::buildOps(ctx, PlatformBackend::getOps());
}

}  // namespace mooncake::tent::device
