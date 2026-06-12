#pragma once

#include <cuda_runtime.h>

namespace mooncake {

// --- Existing kernels ---

void dispatch(void* packed_recv_x, float* packed_recv_x_scales,
              int* packed_recv_src_info, int64_t* packed_recv_layout_range,
              int* packed_recv_count, int32_t* active_ranks, void* mxa_buffer,
              int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
              void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
              void* cuda_counter_buffer, void* cuda_data_buffer, void* raddrs,
              void* rkeys, void* qp_devctxs, const int32_t* nvlink_available,
              void* const* ipc_peer_ptrs, const void* x,
              const int64_t* topk_idx, int* next_clean_buffer, int num_tokens,
              int hidden, int num_max_dispatch_tokens_per_rank, int num_topk,
              int num_experts, int rank, int num_ranks, bool use_fp8,
              void* workspace, cudaStream_t stream, int64_t timeout_ticks,
              int phases);

void launch_combine(void* combined_x, int32_t* active_ranks, void* mxa_buffer,
             int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
             void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
             void* cuda_counter_buffer, void* cuda_data_buffer, void* raddrs,
             void* rkeys, void* qp_devctxs, const int32_t* nvlink_available,
             void* const* ipc_peer_ptrs, const void* x, const int64_t* topk_idx,
             const float* topk_weights, const int* src_info,
             const int64_t* layout_range, int* next_clean_buffer,
             int num_combined_tokens, int hidden,
             int num_max_dispatch_tokens_per_rank, int num_topk,
             int num_experts, int rank, int num_ranks, void* workspace,
             cudaStream_t stream, int64_t timeout_ticks, int phases,
             bool zero_copy);

// --- DeepEP V2 ported kernels ---

// Standalone barrier kernel (adapted from DeepEP V2 barrier.cuh).
void launch_barrier(void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs,
             const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
             int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
             void* workspace, int rank, int num_ranks, int num_sms,
             cudaStream_t stream);

// Deterministic dispatch prologue: pre-computes slot indices.
void dispatch_deterministic_prologue(
    const int64_t* topk_idx, int* dst_buffer_slot_idx, int num_tokens,
    int hidden, int num_topk, int num_experts, int rank, int num_ranks,
    int num_max_dispatch_tokens_per_rank, void* workspace,
    cudaStream_t stream);

// Dispatch copy epilogue: copies received tokens from buffer to output tensors.
void dispatch_copy_epilogue(
    void* buffer, void* workspace, int* psum_num_recv_tokens_per_rank,
    int* psum_num_recv_tokens_per_expert, void* recv_x, void* recv_sf,
    int64_t* recv_topk_idx, float* recv_topk_weights, int* recv_src_metadata,
    int num_recv_tokens, int hidden, int num_sf_packs,
    int recv_sf_token_stride, int recv_sf_hidden_stride, int num_topk,
    int num_experts, int rank, int num_ranks,
    int num_max_dispatch_tokens_per_rank, bool use_fp8, cudaStream_t stream);

// Combine reduce epilogue: post-combine reduction from buffer to output.
void combine_reduce_epilogue(
    void* combined_x, float* combined_topk_weights,
    int64_t* combined_topk_idx, void* recv_buffer, void* bias_0, void* bias_1,
    int num_combined_tokens, int hidden, int num_topk, int num_experts,
    int rank, int num_ranks, int num_max_dispatch_tokens_per_rank,
    cudaStream_t stream);

// Elastic dispatch (DeepEP V2 style with notify warps + deterministic slots).
void launch_elastic_dispatch(
    void* x, const int64_t* topk_idx, float* topk_weights,
    int64_t* copied_topk_idx, int* cumulative_local_expert_recv_stats,
    int* psum_num_recv_tokens_per_rank, int* psum_num_recv_tokens_per_expert,
    int* dst_buffer_slot_idx, int num_tokens, int hidden, int num_topk,
    int num_experts, int rank, int num_ranks,
    int num_max_dispatch_tokens_per_rank, bool reuse_slot_indices,
    void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs,
    const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
    int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
    void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
    void* buffer, void* workspace, cudaStream_t stream,
    int64_t timeout_ticks);

// Elastic combine (DeepEP V2 style with barrier + warp-cooperative copy).
void launch_elastic_combine(
    void* x, float* topk_weights, int* src_metadata,
    int* psum_num_recv_tokens_per_rank,
    void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs,
    const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
    int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
    void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
    void* buffer, void* workspace,
    int num_reduced_tokens, int hidden, int num_topk, int num_experts,
    int rank, int num_ranks, int num_max_dispatch_tokens_per_rank,
    cudaStream_t stream, int64_t timeout_ticks);

}  // namespace mooncake
