#pragma once

namespace mooncake {
struct CombineKernelArgs;
namespace jit {
void launch_combine_jit_musa(int hidden, int num_max_topk, int num_warp_groups,
                             int num_warps_per_group, int num_sms,
                             const CombineKernelArgs& args, void* stream);
}
}  // namespace mooncake
