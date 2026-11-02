#include <jit/dispatch_jit.h>
#include <jit/jit_runtime.hpp>
#include <mooncake_ep_exception.cuh>

#include <cstdint>
#include <sstream>
#include <string>

namespace mooncake {
namespace jit {

static constexpr const char* kDispatchJitEntryName =
    "mooncake_ep_jit_dispatch_kernel";

static std::string dispatch_jit_source(bool use_fp8, int num_warp_groups,
                                       int num_warps_per_group, int hidden) {
    std::ostringstream source;
    source << "#include <mooncake_ep_dispatch.cuh>\n"
           << "\n"
           << "extern \"C\" __global__ "
           << "EP_LAUNCH_BOUNDS(" << num_warp_groups << " * "
           << num_warps_per_group << " * 32, 1)\n"
           << "void " << kDispatchJitEntryName << "(\n"
           << "    const mooncake::DispatchKernelArgs p) {\n"
           << "    mooncake::dispatch_kernel_impl<"
           << (use_fp8 ? "true" : "false") << ", " << num_warp_groups << ", "
           << num_warps_per_group << ", " << hidden << ">(p);\n"
           << "}\n";
    return source.str();
}

void launch_dispatch_jit(int hidden, bool use_fp8, int num_warp_groups,
                         int num_warps_per_group, int num_sms,
                         const DispatchKernelArgs& args, cudaStream_t stream) {
    static const int variant_identity = 0;

    std::uint64_t key = kRuntimeKeySeed;
    key = runtime_key_mix(key, static_cast<std::uint64_t>(hidden));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_warp_groups));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_warps_per_group));
    key = runtime_key_mix(key, use_fp8 ? 1u : 0u);

    JitKernelVariant variant;
    variant.kernel_family = "dispatch";
    variant.entry_name = kDispatchJitEntryName;
    variant.identity = &variant_identity;
    variant.runtime_key = key;
    variant.num_blocks = num_sms;
    variant.block_dim = num_warp_groups * num_warps_per_group * 32;
    variant.dynamic_smem_bytes = 0;
    variant.min_sm = 80;
    variant.cooperative = true;

    std::string error;
    JitKernelStatus status = launch_jit_kernel_cached(
        variant, const_cast<DispatchKernelArgs*>(&args), 0, stream, &error);

    if (status == JitKernelStatus::kLaunched) return;

    std::string variant_name;
    std::ostringstream name;
    name << "dispatch_h" << hidden << "_wg" << num_warp_groups << "x"
         << num_warps_per_group << (use_fp8 ? "_fp8" : "_bf16");
    variant_name = name.str();

    if (status != JitKernelStatus::kLaunchFailed) {
        const std::string source = dispatch_jit_source(
            use_fp8, num_warp_groups, num_warps_per_group, hidden);
        variant.variant_name = variant_name;
        variant.source = source;
        status = launch_jit_kernel(
            variant, const_cast<DispatchKernelArgs*>(&args), stream, &error);
    }

    if (status != JitKernelStatus::kLaunched) {
        std::ostringstream message;
        message << "dispatch JIT launch failed for " << variant_name << ": "
                << jit_kernel_status_name(status);
        if (!error.empty()) message << ": " << error;
        throw EPException("JIT", __FILE__, __LINE__, message.str());
    }
}

}  // namespace jit
}  // namespace mooncake
