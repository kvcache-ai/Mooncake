#include "engram_local_cuda.h"

#include <cstdio>

namespace {
template <typename Id>
__global__ void gather(const EngramLocalTable* tables, const Id* ids,
                       uint8_t* output, size_t tokens, size_t width, size_t its,
                       size_t id_head_stride, size_t ots, size_t ohs) {
    const auto table = tables[blockIdx.y];
    for (size_t token = blockIdx.x; token < tokens; token += gridDim.x) {
        const int64_t id = ids[token * its + blockIdx.y * id_head_stride];
        if (id == -1) continue;
        if (id < table.offset || id - table.offset >= table.count) {
            if (threadIdx.x == 0)
                printf("Mooncake Engram row ID out of bounds\n");
            asm volatile("trap;");
            return;
        }
        const auto* source = table.rows + (id - table.offset) * width;
        auto* destination = output + token * ots + blockIdx.y * ohs;
        for (size_t byte = threadIdx.x; byte < width; byte += blockDim.x)
            destination[byte] = source[byte];
    }
}
}  // namespace

cudaError_t engram_local_gather(const EngramLocalTable* tables, const void* ids,
                                bool ids_int64, uint8_t* output, size_t tokens,
                                size_t heads, size_t width, size_t its,
                                size_t id_head_stride, size_t ots, size_t ohs,
                                cudaStream_t stream) {
    if (!tokens) return cudaSuccess;
    const dim3 grid(static_cast<unsigned>(tokens < 65535 ? tokens : 65535),
                    static_cast<unsigned>(heads));
    if (ids_int64)
        gather<<<grid, 256, 0, stream>>>(
            tables, static_cast<const int64_t*>(ids), output, tokens, width,
            its, id_head_stride, ots, ohs);
    else
        gather<<<grid, 256, 0, stream>>>(
            tables, static_cast<const int32_t*>(ids), output, tokens, width,
            its, id_head_stride, ots, ohs);
    return cudaGetLastError();
}
