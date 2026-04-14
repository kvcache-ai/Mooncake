// mc_copy_kernel.cpp
#include <hip/hip_runtime.h>
#include <stdint.h>

extern "C" __global__ __launch_bounds__(1024) void MCCopyKernel(uint64_t dst_addr, uint64_t src_addr, uint64_t size) {
    /**
     * CRITICAL: Addresses are passed as uint64_t instead of pointers.
     * This bypasses the HIP Runtime (UMD) memory object validation, which would
     * otherwise crash when encountering addresses not allocated via hipMalloc.
     */
    uint4* dst_v = (uint4*)dst_addr;
    uint4* src_v = (uint4*)src_addr;
    uint64_t n_v4 = size / 16;

    uint64_t tid = (uint64_t)blockIdx.x * blockDim.x + threadIdx.x;
    uint64_t stride = (uint64_t)blockDim.x * gridDim.x;

    for (uint64_t i = tid; i < n_v4; i += stride) {
        dst_v[i] = src_v[i];
    }

    uint64_t remaining_bytes = size % 16;
    if (tid == 0 && remaining_bytes > 0) {
        uint64_t processed_bytes = n_v4 * 16;
        uint8_t* d_tail = (uint8_t*)dst_addr + processed_bytes;
        uint8_t* s_tail = (uint8_t*)src_addr + processed_bytes;
        for (uint64_t i = 0; i < remaining_bytes; ++i) {
            d_tail[i] = s_tail[i];
        }
    }
    return;
}