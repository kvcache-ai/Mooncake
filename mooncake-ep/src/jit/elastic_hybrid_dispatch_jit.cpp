#include <elastic/mooncake_ep_elastic_launch.cuh>
#include <jit/elastic_hybrid_dispatch_jit.h>
#include <jit/jit_runtime.hpp>
#include <mooncake_ep_exception.cuh>

#include <cstdint>
#include <sstream>
#include <string>

namespace mooncake {
namespace jit {

static constexpr const char *kElasticHybridDispatchJitEntryName =
    "mooncake_ep_jit_elastic_hybrid_dispatch_kernel";

// Only the no-CPU-sync, alignment-1 modes are supported, matching the
// instantiations of the prebuilt path.
static std::string elastic_hybrid_dispatch_jit_source(
    const char *ops_name, bool reuse_slot_indices, int num_notify_warps,
    int num_scaleout_warps, int num_forward_warps, int num_sms,
    int num_scaleout_ranks, int num_scaleup_ranks, int num_hidden_bytes,
    int num_sf_packs, int num_max_tokens_per_rank, int num_experts,
    int num_topk) {
    std::ostringstream source;
    source
        << "#include <mooncake_ep_configs.cuh>\n"
        << "#include "
        << "<elastic/mooncake_ep_elastic_hybrid_dispatch_official.cuh>\n"
        << "\n"
        << "using Ops = mooncake::elastic::transport::" << ops_name << ";\n"
        << "\n"
        << "extern \"C\" __global__ "
        << "__launch_bounds__("
        << num_notify_warps + num_scaleout_warps + num_forward_warps
        << " * 32, 1)\n"
        << "void " << kElasticHybridDispatchJitEntryName << "(\n"
        << "    const mooncake::elastic::HybridDispatchKernelArgs<Ops> p) {\n"
        << "    mooncake::elastic::hybrid_dispatch_kernel_impl<Ops, false, "
        << (reuse_slot_indices ? "true" : "false") << ", " << num_sms << ", "
        << num_notify_warps << ", " << num_scaleout_warps << ", "
        << num_forward_warps << ", " << num_scaleout_ranks << ", "
        << num_scaleup_ranks << ", " << num_hidden_bytes << ", " << num_sf_packs
        << ", " << num_max_tokens_per_rank << ", " << num_experts << ", "
        << num_topk << ", 1, Ops::kNumQPs, NUM_TIMEOUT_CYCLES>(p);\n"
        << "}\n";
    return source.str();
}

void launch_elastic_hybrid_dispatch_jit(
    ElasticTransportBackend backend, bool reuse_slot_indices,
    int num_notify_warps, int num_scaleout_warps, int num_forward_warps,
    int num_sms, int num_scaleout_ranks, int num_scaleup_ranks,
    int num_hidden_bytes, int num_sf_packs, int num_max_tokens_per_rank,
    int num_experts, int num_topk, int smem_bytes, const void *args,
    cudaStream_t stream) {
    static const int variant_identity = 0;
    const char *ops_name =
        backend == ElasticTransportBackend::kNccl ? "NcclOps" : "IbgdaOps";

    std::uint64_t key = kRuntimeKeySeed;
    key = runtime_key_mix(key, ops_name);
    key = runtime_key_mix(key, static_cast<std::uint64_t>(reuse_slot_indices));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_notify_warps));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_scaleout_warps));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_forward_warps));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_sms));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_scaleout_ranks));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_scaleup_ranks));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_hidden_bytes));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_sf_packs));
    key = runtime_key_mix(key,
                          static_cast<std::uint64_t>(num_max_tokens_per_rank));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_experts));
    key = runtime_key_mix(key, static_cast<std::uint64_t>(num_topk));

    JitKernelVariant variant;
    variant.kernel_family = "elastic_hybrid_dispatch";
    variant.entry_name = kElasticHybridDispatchJitEntryName;
    variant.identity = &variant_identity;
    variant.runtime_key = key;
    variant.num_blocks = num_sms;
    variant.block_dim =
        (num_notify_warps + num_scaleout_warps + num_forward_warps) * 32;
    variant.dynamic_smem_bytes = smem_bytes;
    variant.min_sm = 80;
    variant.cooperative = true;

    std::string error;
    JitKernelStatus status = launch_jit_kernel_cached(
        variant, const_cast<void *>(args), 0, stream, &error);

    if (status == JitKernelStatus::kLaunched) return;

    std::string variant_name;
    std::ostringstream name;
    name << "elastic_hybrid_dispatch_" << ops_name << "_reuse"
         << reuse_slot_indices << "_nw" << num_notify_warps << "_sow"
         << num_scaleout_warps << "_fw" << num_forward_warps << "_sm" << num_sms
         << "_so" << num_scaleout_ranks << "_su" << num_scaleup_ranks << "_hb"
         << num_hidden_bytes << "_sfp" << num_sf_packs << "_t"
         << num_max_tokens_per_rank << "_e" << num_experts << "_topk"
         << num_topk;
    variant_name = name.str();

    if (status != JitKernelStatus::kLaunchFailed) {
        const std::string source = elastic_hybrid_dispatch_jit_source(
            ops_name, reuse_slot_indices, num_notify_warps, num_scaleout_warps,
            num_forward_warps, num_sms, num_scaleout_ranks, num_scaleup_ranks,
            num_hidden_bytes, num_sf_packs, num_max_tokens_per_rank,
            num_experts, num_topk);
        variant.variant_name = variant_name;
        variant.source = source;
        status = launch_jit_kernel(variant, const_cast<void *>(args), stream,
                                   &error);
    }

    if (status != JitKernelStatus::kLaunched) {
        std::ostringstream message;
        message << "elastic hybrid dispatch JIT launch failed for "
                << variant_name << ": " << jit_kernel_status_name(status);
        if (!error.empty()) message << ": " << error;
        throw EPException("JIT", __FILE__, __LINE__, message.str());
    }
}

}  // namespace jit
}  // namespace mooncake
