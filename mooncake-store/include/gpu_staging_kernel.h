#pragma once

#include <cstddef>
#include <cstdint>
#include <cuda_runtime.h>

namespace mooncake {

// One device-to-device copy of `bytes` from `source` to `destination`.
struct GpuStagingCopy {
    uint64_t source;
    uint64_t destination;
    uint64_t bytes;
};

// Runs `count` copies (descriptors in device memory) with one kernel launch.
// Copies run in no particular order.
cudaError_t LaunchGpuStagingCopies(const GpuStagingCopy* copies, size_t count,
                                   cudaStream_t stream);

}  // namespace mooncake
