#pragma once

#include <cstdint>

#include <cuda_runtime.h>
#include <torch/torch.h>

namespace mooncake {

struct ElasticLaunchContext {
    void* gdr_buffer = nullptr;
    const int32_t* nvlink_available = nullptr;
    void* const* ipc_peer_ptrs = nullptr;
    void* raddrs = nullptr;
    void* rkeys = nullptr;
    void* qp_devctxs = nullptr;
    const void* rdma_send_signal_buffer = nullptr;
    const void* rdma_recv_signal_buffer = nullptr;
    void* buffer = nullptr;
    void* workspace = nullptr;
    void* mapped_host_workspace = nullptr;
    int rank = 0;
    int num_ranks = 1;
    int scaleout_rank_idx = 0;
    int scaleup_rank_idx = 0;
    int num_scaleout_ranks = 1;
    int num_scaleup_ranks = 1;
    bool is_scaleup_nvlink = true;
    int num_qps = 1;
    int64_t timeout_cycles = -1;
};

void launch_elastic_dispatch_deterministic_prologue(
    const torch::Tensor& topk_idx, torch::Tensor& rank_count_buffer,
    torch::Tensor& dst_buffer_slot_idx, int num_tokens,
    int num_max_tokens_per_rank, int num_experts, int num_topk,
    int scaleup_rank_idx, int num_scaleup_ranks, int num_sms,
    int num_smem_bytes, cudaStream_t stream);

void launch_mooncake_elastic_dispatch(
    void* x, void* sf, int64_t* topk_idx, float* topk_weights,
    int64_t* copied_topk_idx, int* cumulative_local_expert_recv_stats,
    int* psum_num_recv_tokens_per_scaleup_rank,
    int* psum_num_recv_tokens_per_expert, int* dst_buffer_slot_idx,
    int* token_metadata_at_forward, int num_tokens, int num_max_tokens_per_rank,
    int hidden, int elem_size, int num_sf_packs, int sf_token_stride,
    int sf_hidden_stride, int num_experts, int num_topk, int expert_alignment,
    int num_sms, int num_channels_per_sm, int num_smem_bytes, bool cached_mode,
    bool deterministic, bool do_cpu_sync, const ElasticLaunchContext& ctx,
    cudaStream_t stream);

void launch_mooncake_elastic_dispatch_copy_epilogue(
    void* recv_x, void* recv_sf, int64_t* recv_topk_idx,
    float* recv_topk_weights, int* recv_src_metadata, int* channel_linked_list,
    int num_recv_tokens, int num_max_tokens_per_rank, int hidden, int elem_size,
    int num_sf_packs, int recv_sf_token_stride, int recv_sf_hidden_stride,
    int num_experts, int num_topk, int num_sms, int num_smem_bytes,
    int num_channels, bool do_expand, bool cached_mode,
    const ElasticLaunchContext& ctx, int* psum_num_recv_tokens_per_scaleup_rank,
    int* psum_num_recv_tokens_per_expert, cudaStream_t stream);

void* launch_mooncake_elastic_combine(
    void* x, float* topk_weights, int* src_metadata,
    int* psum_num_recv_tokens_per_scaleup_rank, int* token_metadata_at_forward,
    int* channel_linked_list, int num_reduced_tokens,
    int num_max_tokens_per_rank, int hidden, int num_experts, int num_topk,
    int num_sms, int num_smem_bytes, int num_channels, bool use_expanded_layout,
    bool allow_multiple_reduction, const ElasticLaunchContext& ctx,
    cudaStream_t stream);

void launch_mooncake_elastic_combine_reduce_epilogue(
    void* combined_x, float* combined_topk_weights, int64_t* combined_topk_idx,
    int num_combined_tokens, int num_max_tokens_per_rank, int hidden,
    int num_experts, int num_topk, void* reduce_buffer, void* bias_0,
    void* bias_1, int num_sms, int num_smem_bytes, bool use_expanded_layout,
    bool allow_multiple_reduction, const ElasticLaunchContext& ctx,
    cudaStream_t stream);

}  // namespace mooncake
