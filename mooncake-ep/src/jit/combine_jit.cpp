#include <jit/combine_jit.h>
#include <jit/jit_runtime.hpp>
#include <mooncake_ep_exception.cuh>

#include <cstdint>
#include <sstream>
#include <string>

namespace mooncake {
namespace jit {

static constexpr const char* kCombineJitEntryName =
    "mooncake_ep_jit_combine_kernel";

static std::string combine_jit_source(int hidden, int num_max_topk,
                                      int num_warp_groups,
                                      int num_warps_per_group) {
    std::ostringstream source;
    source << "#include <mooncake_ep_combine.cuh>\n"
           << "\n"
           << "extern \"C\" __global__ "
           << "EP_LAUNCH_BOUNDS(" << num_warp_groups << " * "
           << num_warps_per_group << " * 32, 1)\n"
           << "void " << kCombineJitEntryName << "(\n"
           << "    const mooncake::CombineKernelArgs p) {\n"
           << "    mooncake::combine_kernel_impl<" << num_warp_groups << ", "
           << num_warps_per_group << ", " << hidden << ", " << num_max_topk
           << ">(p);\n"
           << "}\n";
    return source.str();
}

void launch_combine_jit(int hidden, int num_max_topk, int num_warp_groups,
                        int num_warps_per_group, int num_sms,
                        const CombineKernelArgs& args, cudaStream_t stream) {
    static const int variant_identity = 0;

    std::uint64_t key = kRuntimeKeySeed;
    key = runtime_key_mix(key, static_cast<std::uint64_t>(hidden));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_max_topk));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_warp_groups));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_warps_per_group));

    JitKernelVariant variant;
    variant.kernel_family = "combine";
    variant.entry_name = kCombineJitEntryName;
    variant.identity = &variant_identity;
    variant.runtime_key = key;
    variant.num_blocks = num_sms;
    variant.block_dim = num_warp_groups * num_warps_per_group * 32;
    variant.dynamic_smem_bytes = 0;
    variant.min_sm = 80;
    variant.cooperative = true;

    std::string error;
    JitKernelStatus status = launch_jit_kernel_cached(
        variant, const_cast<CombineKernelArgs*>(&args), 0, stream, &error);

    if (status == JitKernelStatus::kLaunched) return;

    std::string variant_name;
    std::ostringstream name;
    name << "combine_h" << hidden << "_topk" << num_max_topk << "_wg"
         << num_warp_groups << "x" << num_warps_per_group;
    variant_name = name.str();

    if (status != JitKernelStatus::kLaunchFailed) {
        const std::string source = combine_jit_source(
            hidden, num_max_topk, num_warp_groups, num_warps_per_group);
        variant.variant_name = variant_name;
        variant.source = source;
        status = launch_jit_kernel(
            variant, const_cast<CombineKernelArgs*>(&args), stream, &error);
    }

    if (status != JitKernelStatus::kLaunched) {
        std::ostringstream message;
        message << "combine JIT launch failed for " << variant_name << ": "
                << jit_kernel_status_name(status);
        if (!error.empty()) message << ": " << error;
        throw EPException("JIT", __FILE__, __LINE__, message.str());
    }
}

}  // namespace jit
}  // namespace mooncake
