#pragma once

#include <cuda_runtime.h>

namespace mooncake {

namespace elastic {
struct DispatchCopyEpilogueKernelArgs;
}  // namespace elastic

namespace jit {

void launch_elastic_dispatch_copy_epilogue_jit(
    bool do_expand, bool cached_mode, int num_channels, int num_warps,
    int num_sms, int num_scaleout_ranks, int num_scaleup_ranks,
    int num_hidden_bytes, int num_sf_packs, int num_max_tokens_per_rank,
    int num_experts, int num_topk, int smem_bytes,
    const elastic::DispatchCopyEpilogueKernelArgs& args, cudaStream_t stream);

}  // namespace jit
}  // namespace mooncake
