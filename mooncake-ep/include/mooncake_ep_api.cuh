#pragma once

#include <cuda_runtime.h>

namespace mooncake {

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

void combine(void* combined_x, int32_t* active_ranks, void* mxa_buffer,
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

}  // namespace mooncake
