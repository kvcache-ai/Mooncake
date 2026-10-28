#include <jit/elastic_dispatch_copy_epilogue_jit.h>
#include <jit/jit_runtime.hpp>
#include <mooncake_ep_exception.cuh>

#include <cstdint>
#include <sstream>
#include <string>

namespace mooncake {
namespace jit {

static constexpr const char* kElasticDispatchCopyEpilogueJitEntryName =
    "mooncake_ep_jit_elastic_dispatch_copy_epilogue_kernel";

// kNumSMs is fixed to 0 so the kernel reads gridDim.x, matching the
// instantiations of the prebuilt path; num_sms only sets the grid size.
static std::string elastic_dispatch_copy_epilogue_jit_source(
    bool do_expand, bool cached_mode, int num_channels, int num_warps,
    int num_scaleout_ranks, int num_scaleup_ranks, int num_hidden_bytes,
    int num_sf_packs, int num_max_tokens_per_rank, int num_experts,
    int num_topk) {
    std::ostringstream source;
    source
        << "#include "
        << "<elastic/mooncake_ep_elastic_dispatch_copy_epilogue.cuh>\n"
        << "\n"
        << "extern \"C\" __global__ "
        << "__launch_bounds__(" << num_warps << " * 32, 1)\n"
        << "void " << kElasticDispatchCopyEpilogueJitEntryName << "(\n"
        << "    const mooncake::elastic::DispatchCopyEpilogueKernelArgs p) {\n"
        << "    mooncake::elastic::dispatch_copy_epilogue_kernel_impl<"
        << (do_expand ? "true" : "false") << ", "
        << (cached_mode ? "true" : "false") << ", 0, " << num_channels << ", "
        << num_warps << ", " << num_scaleout_ranks << ", " << num_scaleup_ranks
        << ", " << num_hidden_bytes << ", " << num_sf_packs << ", "
        << num_max_tokens_per_rank << ", " << num_experts << ", " << num_topk
        << ">(p);\n"
        << "}\n";
    return source.str();
}

void launch_elastic_dispatch_copy_epilogue_jit(
    bool do_expand, bool cached_mode, int num_channels, int num_warps,
    int num_sms, int num_scaleout_ranks, int num_scaleup_ranks,
    int num_hidden_bytes, int num_sf_packs, int num_max_tokens_per_rank,
    int num_experts, int num_topk, int smem_bytes,
    const elastic::DispatchCopyEpilogueKernelArgs& args, cudaStream_t stream) {
    static const int variant_identity = 0;

    std::uint64_t key = kRuntimeKeySeed;
    key = runtime_key_mix(key, static_cast<std::uint64_t>(do_expand));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(cached_mode));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_channels));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_warps));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_scaleout_ranks));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_scaleup_ranks));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_hidden_bytes));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_sf_packs));
    key = runtime_key_mix(key,
                          static_cast<std::uint64_t>(num_max_tokens_per_rank));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_experts));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_topk));

    JitKernelVariant variant;
    variant.kernel_family = "elastic_dispatch_copy_epilogue";
    variant.entry_name = kElasticDispatchCopyEpilogueJitEntryName;
    variant.identity = &variant_identity;
    variant.runtime_key = key;
    variant.num_blocks = num_sms;
    variant.block_dim = num_warps * 32;
    variant.dynamic_smem_bytes = smem_bytes;
    variant.min_sm = 80;
    variant.cooperative = true;

    std::string error;
    JitKernelStatus status = launch_jit_kernel_cached(
        variant, const_cast<elastic::DispatchCopyEpilogueKernelArgs*>(&args), 0,
        stream, &error);

    if (status == JitKernelStatus::kLaunched) return;

    std::string variant_name;
    std::ostringstream name;
    name << "elastic_dispatch_copy_epilogue_expand" << do_expand << "_cached"
         << cached_mode << "_c" << num_channels << "_w" << num_warps << "_so"
         << num_scaleout_ranks << "_su" << num_scaleup_ranks << "_hb"
         << num_hidden_bytes << "_sfp" << num_sf_packs << "_t"
         << num_max_tokens_per_rank << "_e" << num_experts << "_topk"
         << num_topk;
    variant_name = name.str();

    if (status != JitKernelStatus::kLaunchFailed) {
        const std::string source = elastic_dispatch_copy_epilogue_jit_source(
            do_expand, cached_mode, num_channels, num_warps, num_scaleout_ranks,
            num_scaleup_ranks, num_hidden_bytes, num_sf_packs,
            num_max_tokens_per_rank, num_experts, num_topk);
        variant.variant_name = variant_name;
        variant.source = source;
        status = launch_jit_kernel(
            variant,
            const_cast<elastic::DispatchCopyEpilogueKernelArgs*>(&args), stream,
            &error);
    }

    if (status != JitKernelStatus::kLaunched) {
        std::ostringstream message;
        message << "elastic dispatch copy epilogue JIT launch failed for "
                << variant_name << ": " << jit_kernel_status_name(status);
        if (!error.empty()) message << ": " << error;
        throw EPException("JIT", __FILE__, __LINE__, message.str());
    }
}

}  // namespace jit
}  // namespace mooncake
