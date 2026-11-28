#pragma once

#include <cuda_runtime.h>

#include <cstdint>

namespace mooncake {

enum class ElasticTransportBackend : uint8_t;

namespace jit {

// args points to the elastic::HybridCombineKernelArgs<Ops> of the given
// backend.
void launch_elastic_hybrid_combine_jit(
    ElasticTransportBackend backend, int num_scaleup_warps,
    int num_forward_warps, int num_sms, int num_scaleout_ranks,
    int num_scaleup_ranks, int hidden, int num_max_tokens_per_rank,
    int num_experts, int num_topk, int smem_bytes, const void* args,
    cudaStream_t stream);

}  // namespace jit
}  // namespace mooncake
