#pragma once

#include <cuda_runtime_api.h>
#include <cstddef>
#include <cstdint>

struct EngramLocalTable {
    const uint8_t* rows;
    int64_t count;
    int64_t offset;
};

cudaError_t engram_local_gather(const EngramLocalTable* tables, const void* ids,
                                bool ids_int64, uint8_t* output, size_t tokens,
                                size_t heads, size_t width,
                                size_t id_token_stride, size_t id_head_stride,
                                size_t out_token_stride, size_t out_head_stride,
                                cudaStream_t stream);
