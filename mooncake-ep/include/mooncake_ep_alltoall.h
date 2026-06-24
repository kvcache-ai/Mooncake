#pragma once

#include <torch/torch.h>

namespace mooncake {

std::tuple<torch::Tensor, torch::Tensor> torch_alltoall_pack_dispatch_fused(
    const torch::Tensor& x, const torch::Tensor& topk_idx, int num_experts,
    int num_ranks);

std::tuple<torch::Tensor, torch::Tensor, torch::Tensor, torch::Tensor,
           torch::Tensor>
torch_alltoall_compact_dispatch_fused(const torch::Tensor& recv_payload,
                                      int num_local_experts,
                                      int num_max_dispatch_tokens_per_rank);

torch::Tensor torch_alltoall_pack_combine(const torch::Tensor& expert_buffers,
                                          const torch::Tensor& return_src_pos,
                                          int num_ranks,
                                          int max_messages_per_rank);

torch::Tensor torch_alltoall_reduce_combine(
    const torch::Tensor& recv_payload, const torch::Tensor& send_route,
    const torch::Tensor& topk_weights, const std::optional<torch::Tensor>& out);

}  // namespace mooncake
