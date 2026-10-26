#pragma once

#include <cstddef>
#include <cstdint>
#include <cuda_runtime.h>

namespace mooncake {

struct GpuReadFragment {
    uint64_t destination;
    uint64_t offset;
    uint64_t bytes;
};

cudaError_t LaunchGpuReadScatter(const void* source,
                                 const GpuReadFragment* fragments, size_t count,
                                 cudaStream_t stream);

}  // namespace mooncake
