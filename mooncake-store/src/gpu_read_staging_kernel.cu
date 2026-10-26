#include "gpu_read_staging_kernel.h"

#include <cuda_runtime.h>

namespace mooncake {
namespace {

__global__ void Scatter(const unsigned char* source,
                        const GpuReadFragment* fragments, size_t count) {
    for (size_t index = blockIdx.x; index < count; index += gridDim.x) {
        const auto fragment = fragments[index];
        auto* dst = reinterpret_cast<unsigned char*>(fragment.destination);
        const auto* src = source + fragment.offset;
        if (((fragment.destination | fragment.offset | fragment.bytes) & 15) ==
            0) {
            auto* dst16 = reinterpret_cast<uint4*>(dst);
            const auto* src16 = reinterpret_cast<const uint4*>(src);
            for (size_t word = threadIdx.x; word < fragment.bytes / 16;
                 word += blockDim.x) {
                dst16[word] = src16[word];
            }
        } else {
            for (size_t byte = threadIdx.x; byte < fragment.bytes;
                 byte += blockDim.x) {
                dst[byte] = src[byte];
            }
        }
    }
}

}  // namespace

cudaError_t LaunchGpuReadScatter(const void* source,
                                 const GpuReadFragment* fragments, size_t count,
                                 cudaStream_t stream) {
    if (count == 0) return cudaSuccess;
    const unsigned blocks = count < 65535 ? count : 65535;
    Scatter<<<blocks, 256, 0, stream>>>(
        static_cast<const unsigned char*>(source), fragments, count);
    return cudaGetLastError();
}

}  // namespace mooncake
