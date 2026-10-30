// clang-format off

#include <cstdio>

#include <mooncake_ep_combine.cuh>
#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_dispatch.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_launch.cuh>
#include <transport/device/comm_device.cuh>
#include <mooncake_ep_utils.cuh>

#if !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
#include <jit/combine_jit.h>
#include <jit/dispatch_jit.h>
#endif

namespace mooncake {

using mooncake::device::CommCtx;
using mooncake::device::make_comm_ctx;
using mooncake::device::mc_route_put;
using mooncake::device::mc_rdma_put;
using mooncake::device::mc_red_add;
using mooncake::device::mc_bar_sync;
using mooncake::device::mc_grid_sync;
using mooncake::device::mc_ld_nc;
using mooncake::device::mc_ld_nc_s32;
using mooncake::device::mc_ld_nc_f32;
using mooncake::device::mc_st_na;
using mooncake::device::mc_ld_acquire;
using mooncake::device::mc_st_release;
using mooncake::device::mc_atomic_add_release;
#ifdef MOONCAKE_EP_USE_MUSA
using mooncake::device::mc_atomic_add_relaxed;
using mooncake::device::mc_fence_noflush;
#endif
using mooncake::device::mc_fence;
using mooncake::device::mc_fence_barrier_fence;

__global__ void mark_phase_ack_kernel(void* mxa_buffer,
                                      const int32_t* nvlink_available,
                                      void* const* ipc_peer_ptrs,
                                      int* ack_buffer, int rank,
                                      int num_ranks, int epoch) {
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs, nullptr, nullptr, nullptr,
        ack_buffer, ack_buffer, rank, num_ranks, MAX_QP_COUNT);

    for (int peer = static_cast<int>(threadIdx.x); peer < num_ranks;
         peer += static_cast<int>(blockDim.x)) {
        if (peer == rank) {
            mc_st_release(ack_buffer + rank, epoch);
        } else {
            void* dst = mc_route_put(comm_ctx, peer, ack_buffer + rank);
            if (dst != nullptr)
                mc_st_release(reinterpret_cast<int*>(dst), epoch);
        }
    }
}

__global__ void wait_phase_ack_kernel(int* ack_buffer, int rank, int num_ranks,
                                      int epoch, int64_t timeout_ticks) {
    for (int peer = static_cast<int>(threadIdx.x); peer < num_ranks;
         peer += static_cast<int>(blockDim.x)) {
        if (peer == rank)
            continue;

        int64_t start_time = static_cast<int64_t>(clock64());
        while (mc_ld_acquire(ack_buffer + peer) < epoch) {
            int64_t end_time = static_cast<int64_t>(clock64());
            if (timeout_ticks != -1 && end_time - start_time > timeout_ticks)
                return;
        }
    }
}

__global__ void mark_and_wait_phase_ack_kernel(
        void* mxa_buffer, const int32_t* nvlink_available,
        void* const* ipc_peer_ptrs, int* ack_buffer, int rank, int num_ranks,
        int epoch, int64_t timeout_ticks) {
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs, nullptr, nullptr, nullptr,
        ack_buffer, ack_buffer, rank, num_ranks, MAX_QP_COUNT);

    for (int peer = static_cast<int>(threadIdx.x); peer < num_ranks;
         peer += static_cast<int>(blockDim.x)) {
        if (peer == rank) {
            mc_st_release(ack_buffer + rank, epoch);
        } else {
            void* dst = mc_route_put(comm_ctx, peer, ack_buffer + rank);
            if (dst != nullptr)
                mc_st_release(reinterpret_cast<int*>(dst), epoch);
        }
    }

    __syncthreads();

    for (int peer = static_cast<int>(threadIdx.x); peer < num_ranks;
         peer += static_cast<int>(blockDim.x)) {
        if (peer == rank)
            continue;

        int64_t start_time = static_cast<int64_t>(clock64());
        while (mc_ld_acquire(ack_buffer + peer) < epoch) {
            int64_t end_time = static_cast<int64_t>(clock64());
            if (timeout_ticks != -1 && end_time - start_time > timeout_ticks)
                return;
        }
    }
}

void mark_phase_ack(void* mxa_buffer, const int32_t* nvlink_available,
                    void* const* ipc_peer_ptrs, int* ack_buffer, int rank,
                    int num_ranks, int epoch, cudaStream_t stream) {
    SETUP_LAUNCH_CONFIG(1, 32, stream);
    LAUNCH_KERNEL(&cfg, mark_phase_ack_kernel, mxa_buffer, nvlink_available,
                  ipc_peer_ptrs, ack_buffer, rank, num_ranks, epoch);
}

void wait_phase_ack(int* ack_buffer, int rank, int num_ranks, int epoch,
                    cudaStream_t stream, int64_t timeout_ticks) {
    SETUP_LAUNCH_CONFIG(1, 32, stream);
    LAUNCH_KERNEL(&cfg, wait_phase_ack_kernel, ack_buffer, rank, num_ranks,
                  epoch, timeout_ticks);
}

void mark_and_wait_phase_ack(void* mxa_buffer,
                             const int32_t* nvlink_available,
                             void* const* ipc_peer_ptrs, int* ack_buffer,
                             int rank, int num_ranks, int epoch,
                             cudaStream_t stream, int64_t timeout_ticks) {
    SETUP_LAUNCH_CONFIG(1, 32, stream);
    LAUNCH_KERNEL(&cfg, mark_and_wait_phase_ack_kernel, mxa_buffer,
                  nvlink_available, ipc_peer_ptrs, ack_buffer, rank, num_ranks,
                  epoch, timeout_ticks);
}

void dispatch(void* packed_recv_x, float* packed_recv_x_scales,
              int* packed_recv_src_info, int64_t* packed_recv_layout_range,
              int* packed_recv_count,
              int* cumulative_local_expert_recv_stats,
              int32_t* active_ranks, void* mxa_buffer,
              int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
              void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
              void* raddrs, void* rkeys, void* qp_devctxs,
              const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
              const void* x, const int64_t* topk_idx,
              int* next_clean_buffer,
              int num_tokens, int hidden, int num_max_dispatch_tokens_per_rank,
              int num_topk, int num_experts, int rank, int num_ranks,
              int qp_num_ranks, bool use_fp8,
              void* workspace, cudaStream_t stream,
              int64_t timeout_ticks, int phases, int active_qps_per_rank) {
    constexpr int kNumMaxTopK = 17;
    constexpr int kNumWarpsPerGroup = 4;
    int num_warp_groups = 8;
#ifdef MOONCAKE_EP_USE_MUSA
    cudaDeviceProp device_prop{};
    int device = 0;
    CUDA_CHECK(cudaGetDevice(&device));
    CUDA_CHECK(cudaGetDeviceProperties(&device_prop, device));
    num_warp_groups = cell_div(num_experts, device_prop.multiProcessorCount);
    // Select the group count from the device shape. The S5000's 56 SMs select
    // six groups for 288 experts and five groups for the 256-expert shape.
    // Five is the minimum valid value for the kNumMaxTopK capacity check.
    num_warp_groups = max(5, min(8, num_warp_groups));
#endif
    EP_HOST_ASSERT(kNumMaxTopK + 1 <= num_warp_groups * kNumWarpsPerGroup &&
                   "Too many top-k selections");

    const auto num_sms = max(2, cell_div(num_experts, num_warp_groups));
    EP_HOST_ASSERT(num_topk <= kNumMaxTopK);

    // Workspace checks
    auto atomic_counter_per_expert = reinterpret_cast<int*>(workspace);
    auto atomic_finish_counter_per_expert = atomic_counter_per_expert + num_experts;
    EP_HOST_ASSERT(num_experts * sizeof(int) * 2 <= NUM_WORKSPACE_BYTES);

    const DispatchKernelArgs args {
        packed_recv_x,
        packed_recv_x_scales,
        packed_recv_src_info,
        packed_recv_layout_range,
        packed_recv_count,
        cumulative_local_expert_recv_stats,
        active_ranks,
        mxa_buffer,
        rdma_send_signal_buffer,
        rdma_recv_signal_buffer,
        rdma_send_data_buffer,
        rdma_recv_data_buffer,
        raddrs,
        rkeys,
        qp_devctxs,
        nvlink_available,
        ipc_peer_ptrs,
        x,
        topk_idx,
        atomic_counter_per_expert,
        atomic_finish_counter_per_expert,
        next_clean_buffer,
        num_tokens,
        num_max_dispatch_tokens_per_rank,
        num_topk,
        num_experts,
        rank,
        num_ranks,
        qp_num_ranks,
        timeout_ticks,
        phases,
        active_qps_per_rank,
    };

#if !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
    jit::launch_dispatch_jit(hidden, use_fp8, num_warp_groups,
                             kNumWarpsPerGroup, num_sms, args, stream);
#else // !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
#define DISPATCH_LAUNCH_GROUP(hidden, groups) case groups: { \
constexpr int kNumWarpGroups = groups; \
auto dispatch_func = use_fp8 ? dispatch<true, kNumWarpGroups, kNumWarpsPerGroup, hidden> : \
                               dispatch<false, kNumWarpGroups, kNumWarpsPerGroup, hidden>; \
LAUNCH_KERNEL(&cfg, dispatch_func, args); } break

#define DISPATCH_LAUNCH_CASE(hidden) { \
switch (num_warp_groups) { \
DISPATCH_LAUNCH_GROUP(hidden, 3); \
DISPATCH_LAUNCH_GROUP(hidden, 4); \
DISPATCH_LAUNCH_GROUP(hidden, 5); \
DISPATCH_LAUNCH_GROUP(hidden, 6); \
DISPATCH_LAUNCH_GROUP(hidden, 7); \
DISPATCH_LAUNCH_GROUP(hidden, 8); \
default: EP_HOST_ASSERT(false && "Unsupported dispatch warp-group count"); \
} } break

    SETUP_LAUNCH_CONFIG(num_sms, num_warp_groups * kNumWarpsPerGroup * 32, stream);
    SWITCH_HIDDEN(DISPATCH_LAUNCH_CASE);
#undef DISPATCH_LAUNCH_CASE
#undef DISPATCH_LAUNCH_GROUP
#endif // !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
}

void combine(void* combined_x, int32_t* active_ranks,
             void* mxa_buffer,
             int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
             void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
             void* raddrs, void* rkeys, void* qp_devctxs,
             const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
             const void* x, const int64_t* topk_idx, const float* topk_weights,
             const int* src_info, const int64_t* layout_range,
             int* next_clean_buffer,
             int num_combined_tokens, int hidden, int num_max_dispatch_tokens_per_rank,
             int num_topk, int num_experts, int rank, int num_ranks,
             int qp_num_ranks, void* workspace, cudaStream_t stream,
             int64_t timeout_ticks, int phases, bool zero_copy,
             int active_qps_per_rank) {
    constexpr int kNumWarpsPerGroup = 4;
    constexpr int kNumWarpGroups = 8;
    constexpr int kNumMaxTopk = 17;

    const auto num_sms = cell_div(num_experts, kNumWarpGroups);

    // Check workspace
    auto atomic_clean_flag = reinterpret_cast<int*>(workspace);
    EP_HOST_ASSERT(sizeof(int) <= NUM_WORKSPACE_BYTES);
    EP_HOST_ASSERT(num_topk <= kNumMaxTopk);

    const CombineKernelArgs args {
        combined_x,
        active_ranks,
        mxa_buffer,
        rdma_send_signal_buffer,
        rdma_recv_signal_buffer,
        rdma_send_data_buffer,
        rdma_recv_data_buffer,
        raddrs,
        rkeys,
        qp_devctxs,
        nvlink_available,
        ipc_peer_ptrs,
        x,
        topk_idx,
        topk_weights,
        src_info,
        layout_range,
        next_clean_buffer,
        atomic_clean_flag,
        num_combined_tokens,
        num_topk,
        num_max_dispatch_tokens_per_rank,
        num_experts,
        rank,
        num_ranks,
        qp_num_ranks,
        timeout_ticks,
        phases,
        zero_copy,
        active_qps_per_rank
    };

#if !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
    jit::launch_combine_jit(hidden, kNumMaxTopk,
                            kNumWarpGroups, kNumWarpsPerGroup, num_sms, args, stream);
#else // !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
#define COMBINE_LAUNCH_CASE(hidden) { \
auto combine_func = combine<kNumWarpGroups, kNumWarpsPerGroup, hidden, kNumMaxTopk>; \
LAUNCH_KERNEL(&cfg, combine_func, args); } break

    SETUP_LAUNCH_CONFIG(num_sms, kNumWarpGroups * kNumWarpsPerGroup * 32, stream);
    SWITCH_HIDDEN(COMBINE_LAUNCH_CASE);
#undef COMBINE_LAUNCH_CASE
#endif // !defined(MOONCAKE_EP_USE_MUSA) && !defined(MOONCAKE_EP_USE_MACA)
}

__global__ void update_active_mask_kernel(int32_t* active_ranks, int rank,
                                          int32_t active) {
    if (threadIdx.x == 0) active_ranks[rank] = active;
}

void update_mask_buffer(int32_t* active_ranks, int rank, bool masked,
                        cudaStream_t stream) {
    SETUP_LAUNCH_CONFIG(1, 32, stream);
    LAUNCH_KERNEL(&cfg, update_active_mask_kernel, active_ranks, rank,
                  masked ? 0 : 1);
}

__global__ void query_mask_buffer_kernel(const int32_t* active_ranks,
                                         int32_t* output_mask,
                                         int num_ranks) {
    int rank = blockIdx.x * blockDim.x + threadIdx.x;
    if (rank < num_ranks) output_mask[rank] = active_ranks[rank] ? 0 : 1;
}

void query_mask_buffer(const int32_t* active_ranks, int num_ranks,
                       int32_t* output_mask, cudaStream_t stream) {
    constexpr int kThreads = 256;
    SETUP_LAUNCH_CONFIG(cell_div(num_ranks, kThreads), kThreads, stream);
    LAUNCH_KERNEL(&cfg, query_mask_buffer_kernel, active_ranks, output_mask,
                  num_ranks);
}

__global__ void barrier_kernel(
    int32_t* active_ranks, int* send_count, int* arrivals, void* gdr_buffer,
    const int32_t* nvlink_available, void* const* ipc_peer_ptrs, void* raddrs,
    void* rkeys, void* qp_devctxs, int rank, int rank_bound,
    int capacity_ranks, int num_qp_per_rank, int64_t timeout_ticks) {
    const int dst_rank = threadIdx.x;
    if (dst_rank < rank_bound && dst_rank < capacity_ranks &&
        dst_rank != rank && active_ranks[dst_rank]) {
        const int sequence = atomicAdd(send_count + dst_rank, 1) + 1;
        auto* remote_arrival_local = arrivals + rank;
        const auto comm_ctx = make_comm_ctx(
            gdr_buffer, nvlink_available, ipc_peer_ptrs, raddrs, rkeys,
            qp_devctxs, send_count, arrivals, rank, capacity_ranks,
            MAX_QP_COUNT);
        void* p2p_dst = mc_route_put(comm_ctx, dst_rank, remote_arrival_local);
        if (p2p_dst != nullptr) {
            mc_st_release(reinterpret_cast<int*>(p2p_dst), sequence);
        } else {
            mc_fence();
            mc_rdma_put(comm_ctx, 0, dst_rank, num_qp_per_rank,
                        send_count + dst_rank, remote_arrival_local,
                        sizeof(int), 0);
        }

        const auto start = clock64();
        while (mc_ld_acquire(arrivals + dst_rank) != sequence) {
            if (!active_ranks[dst_rank]) break;
            if (timeout_ticks != -1 && clock64() - start > timeout_ticks) {
                active_ranks[dst_rank] = 0;
                break;
            }
        }
    }
    __syncthreads();
}

void barrier(int32_t* active_ranks, int* send_count, int* arrivals,
             void* gdr_buffer, int32_t* nvlink_available,
             void* const* ipc_peer_ptrs, void* raddrs, void* rkeys,
             void* qp_devctxs, int rank, int rank_bound, int capacity_ranks,
             int num_qp_per_rank, cudaStream_t stream,
             int64_t timeout_ticks) {
    constexpr int kThreads = MAX_QP_COUNT;
    SETUP_LAUNCH_CONFIG(1, kThreads, stream);
    LAUNCH_KERNEL(&cfg, barrier_kernel, active_ranks, send_count, arrivals,
                  gdr_buffer, nvlink_available, ipc_peer_ptrs, raddrs, rkeys,
                  qp_devctxs, rank, rank_bound, capacity_ranks,
                  num_qp_per_rank, timeout_ticks);
}

} // namespace mooncake
