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
              int force_rdma_data, int poll_rdma_put, int rdma_write_signal,
              int active_qps_per_peer,
              void* workspace, cudaStream_t stream, int64_t timeout_ticks,
              int phases);

void mark_phase_ack(void* mxa_buffer, const int32_t* nvlink_available,
                    void* const* ipc_peer_ptrs, int* ack_buffer, int rank,
                    int num_ranks, int epoch, cudaStream_t stream);

void wait_phase_ack(int* ack_buffer, int rank, int num_ranks, int epoch,
                    cudaStream_t stream, int64_t timeout_ticks);

void mark_and_wait_phase_ack(void* mxa_buffer, const int32_t* nvlink_available,
                             void* const* ipc_peer_ptrs, int* ack_buffer,
                             int rank, int num_ranks, int epoch,
                             cudaStream_t stream, int64_t timeout_ticks);

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
             int num_experts, int rank, int num_ranks, int force_rdma_data,
             int poll_rdma_put, int rdma_write_signal,
             int active_qps_per_peer, void* workspace, cudaStream_t stream,
             int64_t timeout_ticks, int phases, bool zero_copy);

void debug_rdma_put_probe(void* mxa_buffer, void* raddrs, void* rkeys,
                          void* qp_devctxs, int rank, int num_ranks,
                          int qps_per_rank, int dst_rank,
                          uint64_t dst_byte_offset, uint64_t src_byte_offset,
                          uint64_t value,
                          uint32_t nbytes, uint64_t* local_source,
                          int poll_completion, cudaStream_t stream);

void debug_rdma_multi_put_probe(
    void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs, int rank,
    int num_ranks, int qps_per_rank, int dst_rank, uint64_t dst_byte_offset,
    uint64_t dst_stride, uint64_t src_byte_offset, uint64_t src_stride,
    uint32_t nbytes, int nputs, int poll_completion, int delay_iters,
    int channel_offset, int warmup_puts, uint64_t warmup_dst_offset,
    uint64_t warmup_src_offset, int pre_post_delay_iters, int prewrite_source,
    cudaStream_t stream);

void debug_fill_multi_put_sources(void* mxa_buffer, int rank,
                                  uint64_t src_byte_offset,
                                  uint64_t src_stride, int nputs,
                                  cudaStream_t stream);

}  // namespace mooncake
