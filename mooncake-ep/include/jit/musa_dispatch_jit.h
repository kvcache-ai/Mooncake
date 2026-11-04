#pragma once

namespace mooncake {
struct DispatchKernelArgs;
namespace jit {
void launch_dispatch_jit_musa(int hidden, bool use_fp8, int num_warp_groups,
                              int num_warps_per_group, int num_sms,
                              const DispatchKernelArgs& args, void* stream);
}
}  // namespace mooncake
