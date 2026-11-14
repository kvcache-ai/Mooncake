#include <jit/elastic_combine_reduce_epilogue_jit.h>
#include <jit/jit_runtime.hpp>
#include <mooncake_ep_exception.cuh>

#include <cstdint>
#include <sstream>
#include <string>

namespace mooncake {
namespace jit {

static constexpr const char* kElasticCombineReduceEpilogueJitEntryName =
    "mooncake_ep_jit_elastic_combine_reduce_epilogue_kernel";

// Only the non-expanded, multiple-reduction mode is supported, and kNumSMs is
// fixed to 0 so the kernel reads gridDim.x, matching the instantiations of the
// prebuilt path; num_sms only sets the grid size.
static std::string elastic_combine_reduce_epilogue_jit_source(
    int num_warps, int num_scaleout_ranks, int num_scaleup_ranks, int hidden,
    int num_max_tokens_per_rank, int num_experts, int num_topk) {
    std::ostringstream source;
    source << "#include "
           << "<elastic/mooncake_ep_elastic_combine_reduce_epilogue.cuh>\n"
           << "\n"
           << "extern \"C\" __global__ "
           << "__launch_bounds__(" << num_warps << " * 32, 1)\n"
           << "void " << kElasticCombineReduceEpilogueJitEntryName << "(\n"
           << "    const mooncake::elastic::"
           << "CombineReduceEpilogueKernelArgs p) {\n"
           << "    mooncake::elastic::combine_reduce_epilogue_kernel_impl<"
           << "false, true, 0, " << num_warps << ", " << num_scaleout_ranks
           << ", " << num_scaleup_ranks << ", " << hidden << ", "
           << num_max_tokens_per_rank << ", " << num_experts << ", " << num_topk
           << ">(p);\n"
           << "}\n";
    return source.str();
}

void launch_elastic_combine_reduce_epilogue_jit(
    int num_warps, int num_sms, int num_scaleout_ranks, int num_scaleup_ranks,
    int hidden, int num_max_tokens_per_rank, int num_experts, int num_topk,
    int smem_bytes, const elastic::CombineReduceEpilogueKernelArgs& args,
    cudaStream_t stream) {
    static const int variant_identity = 0;

    std::uint64_t key = kRuntimeKeySeed;
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_warps));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_scaleout_ranks));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_scaleup_ranks));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(hidden));
    key = runtime_key_mix(key,
                          static_cast<std::uint64_t>(num_max_tokens_per_rank));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_experts));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_topk));

    JitKernelVariant variant;
    variant.kernel_family = "elastic_combine_reduce_epilogue";
    variant.entry_name = kElasticCombineReduceEpilogueJitEntryName;
    variant.identity = &variant_identity;
    variant.runtime_key = key;
    variant.num_blocks = num_sms;
    variant.block_dim = num_warps * 32;
    variant.dynamic_smem_bytes = smem_bytes;
    variant.min_sm = 80;
    variant.cooperative = true;

    std::string error;
    JitKernelStatus status = launch_jit_kernel_cached(
        variant, const_cast<elastic::CombineReduceEpilogueKernelArgs*>(&args),
        0, stream, &error);

    if (status == JitKernelStatus::kLaunched) return;

    std::string variant_name;
    std::ostringstream name;
    name << "elastic_combine_reduce_epilogue_w" << num_warps << "_so"
         << num_scaleout_ranks << "_su" << num_scaleup_ranks << "_h" << hidden
         << "_t" << num_max_tokens_per_rank << "_e" << num_experts << "_topk"
         << num_topk;
    variant_name = name.str();

    if (status != JitKernelStatus::kLaunchFailed) {
        const std::string source = elastic_combine_reduce_epilogue_jit_source(
            num_warps, num_scaleout_ranks, num_scaleup_ranks, hidden,
            num_max_tokens_per_rank, num_experts, num_topk);
        variant.variant_name = variant_name;
        variant.source = source;
        status = launch_jit_kernel(
            variant,
            const_cast<elastic::CombineReduceEpilogueKernelArgs*>(&args),
            stream, &error);
    }

    if (status != JitKernelStatus::kLaunched) {
        std::ostringstream message;
        message << "elastic combine reduce epilogue JIT launch failed for "
                << variant_name << ": " << jit_kernel_status_name(status);
        if (!error.empty()) message << ": " << error;
        throw EPException("JIT", __FILE__, __LINE__, message.str());
    }
}

}  // namespace jit
}  // namespace mooncake
