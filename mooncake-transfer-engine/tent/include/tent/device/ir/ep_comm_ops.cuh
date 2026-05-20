#pragma once

#include <stddef.h>
#include <stdint.h>

#include <tent/device/ir/device_ops.cuh>

namespace mooncake::tent::device {

struct EpCommOps {
    void (*put)(void* ctx, int ch, void* recv, const void* send, size_t n,
                int dst);
    void (*get)(void* ctx, int ch, const void* src, void* dst, size_t n,
                int src_rank);

    void (*signal)(void* ctx, int ch, int dst, uint64_t action);
    void (*wait_signal)(void* ctx, void* sig, uint64_t expected);

    void (*red_add)(void* ctx, int ch, void* sym, uint64_t val, int dst);

    void (*flush)(void* ctx, int ch, int dst);
    void (*wait)(void* ctx, int ch, uint16_t expected);

    void (*barrier)(void* ctx, int num_ranks);
    void (*put_value)(void* ctx, int ch, void* sym, uint64_t val, int dst);

    void* ctx;
};

template <typename NetworkBackend>
struct EpCommOpsOf {
    static __device__ __forceinline__ EpCommOps build(void* ctx,
                                                      DeviceOps* dops) {
        return NetworkBackend::buildOps(ctx, dops);
    }
};

}  // namespace mooncake::tent::device
