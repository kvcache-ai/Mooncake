#include "gpu_staging_kernel.h"

#include <cuda_runtime.h>

namespace mooncake {
namespace {

// Few enough blocks to leave most SMs to the application's kernels; each block
// strides over the copies.
constexpr unsigned kMaxBlocks = 128;
constexpr unsigned kThreads = 256;

__global__ void Copy(const GpuStagingCopy* copies, size_t count) {
    for (size_t index = blockIdx.x; index < count; index += gridDim.x) {
        const auto copy = copies[index];
        auto* dst = reinterpret_cast<unsigned char*>(copy.destination);
        const auto* src = reinterpret_cast<const unsigned char*>(copy.source);
        if (((copy.destination | copy.source | copy.bytes) & 15) == 0) {
            auto* dst16 = reinterpret_cast<uint4*>(dst);
            const auto* src16 = reinterpret_cast<const uint4*>(src);
            for (size_t word = threadIdx.x; word < copy.bytes / 16;
                 word += blockDim.x) {
                dst16[word] = src16[word];
            }
        } else {
            for (size_t byte = threadIdx.x; byte < copy.bytes;
                 byte += blockDim.x) {
                dst[byte] = src[byte];
            }
        }
    }
}

}  // namespace

cudaError_t LaunchGpuStagingCopies(const GpuStagingCopy* copies, size_t count,
                                   cudaStream_t stream) {
    if (count == 0) return cudaSuccess;
    const unsigned blocks = count < kMaxBlocks ? count : kMaxBlocks;
    Copy<<<blocks, kThreads, 0, stream>>>(copies, count);
    return cudaGetLastError();
}

}  // namespace mooncake
