#include <jit/elastic_dispatch_prologue_jit.h>
#include <jit/jit_runtime.hpp>
#include <mooncake_ep_exception.cuh>

#include <cstdint>
#include <sstream>
#include <string>

namespace mooncake {
namespace jit {

static constexpr const char* kElasticDispatchPrologueJitEntryName =
    "mooncake_ep_jit_elastic_dispatch_prologue_kernel";

static std::string elastic_dispatch_prologue_jit_source(
    int num_warps, int num_sms, int num_scaleup_ranks,
    int num_max_tokens_per_rank, int num_experts, int num_topk) {
    std::ostringstream source;
    source
        << "#include "
        << "<elastic/mooncake_ep_elastic_dispatch_deterministic_prologue.cuh>\n"
        << "\n"
        << "extern \"C\" __global__ "
        << "__launch_bounds__(" << num_warps << " * 32, 1)\n"
        << "void " << kElasticDispatchPrologueJitEntryName << "(\n"
        << "    const "
           "mooncake::elastic::DispatchDeterministicPrologueKernelArgs p) {\n"
        << "    mooncake::elastic::dispatch_deterministic_prologue_kernel_impl<"
        << num_sms << ", " << num_warps << ", " << num_scaleup_ranks << ", "
        << num_max_tokens_per_rank << ", " << num_experts << ", " << num_topk
        << ">(p);\n"
        << "}\n";
    return source.str();
}

void launch_elastic_dispatch_prologue_jit(
    int num_warps, int num_sms, int num_scaleup_ranks,
    int num_max_tokens_per_rank, int num_experts, int num_topk, int smem_bytes,
    const elastic::DispatchDeterministicPrologueKernelArgs& args,
    cudaStream_t stream) {
    static const int variant_identity = 0;

    std::uint64_t key = kRuntimeKeySeed;
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_warps));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_sms));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_scaleup_ranks));
    key = runtime_key_mix(key,
                          static_cast<std::uint64_t>(num_max_tokens_per_rank));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_experts));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_topk));

    JitKernelVariant variant;
    variant.kernel_family = "elastic_dispatch_prologue";
    variant.entry_name = kElasticDispatchPrologueJitEntryName;
    variant.identity = &variant_identity;
    variant.runtime_key = key;
    variant.num_blocks = num_sms;
    variant.block_dim = num_warps * 32;
    variant.dynamic_smem_bytes = smem_bytes;
    variant.min_sm = 80;
    variant.cooperative = true;

    std::string error;
    JitKernelStatus status = launch_jit_kernel_cached(
        variant,
        const_cast<elastic::DispatchDeterministicPrologueKernelArgs*>(&args), 0,
        stream, &error);

    if (status == JitKernelStatus::kLaunched) return;

    std::string variant_name;
    std::ostringstream name;
    name << "elastic_dispatch_prologue_w" << num_warps << "_sm" << num_sms
         << "_r" << num_scaleup_ranks << "_t" << num_max_tokens_per_rank << "_e"
         << num_experts << "_topk" << num_topk;
    variant_name = name.str();

    if (status != JitKernelStatus::kLaunchFailed) {
        const std::string source = elastic_dispatch_prologue_jit_source(
            num_warps, num_sms, num_scaleup_ranks, num_max_tokens_per_rank,
            num_experts, num_topk);
        variant.variant_name = variant_name;
        variant.source = source;
        status = launch_jit_kernel(
            variant,
            const_cast<elastic::DispatchDeterministicPrologueKernelArgs*>(
                &args),
            stream, &error);
    }

    if (status != JitKernelStatus::kLaunched) {
        std::ostringstream message;
        message << "elastic dispatch prologue JIT launch failed for "
                << variant_name << ": " << jit_kernel_status_name(status);
        if (!error.empty()) message << ": " << error;
        throw EPException("JIT", __FILE__, __LINE__, message.str());
    }
}

}  // namespace jit
}  // namespace mooncake
