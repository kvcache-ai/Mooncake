#pragma once

#include <cuda_alike.h>

namespace mooncake {

void dispatch(void* packed_recv_x, float* packed_recv_x_scales,
              int* packed_recv_src_info, int64_t* packed_recv_layout_range,
              int* packed_recv_count, int* cumulative_local_expert_recv_stats,
              int32_t* active_ranks, void* mxa_buffer,
              int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
              void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
              void* raddrs, void* rkeys, void* qp_devctxs,
              const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
              const void* x, const int64_t* topk_idx, int* next_clean_buffer,
              int num_tokens, int hidden, int num_max_dispatch_tokens_per_rank,
              int num_topk, int num_experts, int rank, int num_ranks,
              int qp_num_ranks, bool use_fp8, void* workspace,
              cudaStream_t stream, int64_t timeout_ticks, int phases,
              int active_qps_per_rank);

void combine(void* combined_x, int32_t* active_ranks, void* mxa_buffer,
             int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
             void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
             void* raddrs, void* rkeys, void* qp_devctxs,
             const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
             const void* x, const int64_t* topk_idx, const float* topk_weights,
             const int* src_info, const int64_t* layout_range,
             int* next_clean_buffer, int num_combined_tokens, int hidden,
             int num_max_dispatch_tokens_per_rank, int num_topk,
             int num_experts, int rank, int num_ranks, int qp_num_ranks,
             void* workspace, cudaStream_t stream, int64_t timeout_ticks,
             int phases, bool zero_copy, int active_qps_per_rank);

void update_mask_buffer(int32_t* active_ranks, int rank, bool masked,
                        cudaStream_t stream);
void query_mask_buffer(const int32_t* active_ranks, int num_ranks,
                       int32_t* output_mask, cudaStream_t stream);
void barrier(int32_t* active_ranks, int* send_count, int* arrivals,
             void* gdr_buffer, int32_t* nvlink_available,
             void* const* ipc_peer_ptrs, void* raddrs, void* rkeys,
             void* qp_devctxs, int rank, int rank_bound, int capacity_ranks,
             int num_qp_per_rank, cudaStream_t stream, int64_t timeout_ticks);

}  // namespace mooncake
