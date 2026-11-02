#pragma once

#include <cuda_runtime.h>

namespace mooncake {

namespace elastic {
struct DispatchDeterministicPrologueKernelArgs;
}  // namespace elastic

namespace jit {

void launch_elastic_dispatch_prologue_jit(
    int num_warps, int num_sms, int num_scaleup_ranks,
    int num_max_tokens_per_rank, int num_experts, int num_topk, int smem_bytes,
    const elastic::DispatchDeterministicPrologueKernelArgs& args,
    cudaStream_t stream);

}  // namespace jit
}  // namespace mooncake
