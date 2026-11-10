#pragma once

#include <cuda_runtime.h>

namespace mooncake {

struct CombineKernelArgs;

namespace jit {

void launch_combine_jit(int hidden, int num_max_topk, int num_warp_groups,
                        int num_warps_per_group, int num_sms,
                        const CombineKernelArgs& args, cudaStream_t stream);

}  // namespace jit
}  // namespace mooncake
