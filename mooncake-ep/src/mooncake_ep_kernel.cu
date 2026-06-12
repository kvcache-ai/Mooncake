// clang-format off

#include <cstdio>

#include <mooncake_ep_barrier.cuh>
#include <mooncake_ep_combine_epilogue.cuh>
#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_dispatch_epilogue.cuh>
#include <mooncake_ep_dispatch_prologue.cuh>
#include <mooncake_ep_elastic_combine.cuh>
#include <mooncake_ep_elastic_dispatch.cuh>
#include <mooncake_ep_exception.cuh>
#include <mooncake_ep_launch.cuh>
#include <mooncake_ep_utils.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake {

using mooncake::device::CommCtx;
using mooncake::device::make_comm_ctx;
using mooncake::device::mc_route_put;
using mooncake::device::mc_rdma_put;
using mooncake::device::mc_signal;
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

template <bool kUseFP8, int kNumWarpGroups, int kNumWarpsPerGroup, int kHidden>
__global__ __launch_bounds__(kNumWarpGroups * kNumWarpsPerGroup * 32, 1) void
dispatch(void* packed_recv_x, float* packed_recv_x_scales,
         int* packed_recv_src_info, int64_t* packed_recv_layout_range,
         int* packed_recv_count, int32_t* active_ranks,
         void* mxa_buffer,
         int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
         void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
         void* cuda_counter_buffer, void* cuda_data_buffer,
         void* raddrs, void* rkeys, void* qp_devctxs,
         const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
         const void* x, const int64_t* topk_idx,
         int* atomic_counter_per_expert, int* atomic_finish_counter_per_expert,
         int* next_clean_buffer,
         int num_tokens, int num_max_dispatch_tokens_per_rank,
         int num_topk, int num_experts, int rank, int num_ranks,
         int64_t timeout_ticks,
         int phases) {
    const auto sm_id = static_cast<int>(blockIdx.x);
    const auto thread_id = static_cast<int>(threadIdx.x);
    const auto warp_id = thread_id / 32, lane_id = get_lane_id();
    const auto num_sms = static_cast<int>(gridDim.x);
    const auto num_warps = kNumWarpGroups * kNumWarpsPerGroup;
    const auto num_local_experts = num_experts / num_ranks;
    const auto warp_group_id = warp_id / kNumWarpsPerGroup;
    const auto sub_warp_id = warp_id % kNumWarpsPerGroup;
    const auto responsible_expert_idx = sm_id * kNumWarpGroups + warp_group_id;

    // FP8 staffs
    constexpr int kNumPerChannels = 128;
    constexpr float kFP8Margin = 1e-4, kFP8Amax = 448, kFP8AmaxInv = 1.0f / 448.0f;
    const int num_scales = kHidden / kNumPerChannels;
    const size_t hidden_bytes = kHidden * (kUseFP8 ? sizeof(__nv_fp8_storage_t) : sizeof(nv_bfloat16));
    const size_t hidden_int4 = hidden_bytes / sizeof(int4);

    // Message package: hidden data, FP8 scales, index at source
    // NOTES: currently we have 3 reserved int fields for future use
    using vec_t = typename std::conditional<kUseFP8, int2, int4>::type;
    const size_t num_bytes_per_msg = sizeof(int4) + (kUseFP8 ? (kHidden + num_scales * sizeof(float)) : (kHidden * sizeof(nv_bfloat16)));
    const size_t num_int4_per_msg = num_bytes_per_msg / sizeof(int4);
    EP_DEVICE_ASSERT(num_bytes_per_msg % sizeof(int4) == 0);

    // Communication context — platform dispatch is inside comm_device.cuh
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs,
        raddrs, rkeys, qp_devctxs,
        rdma_send_signal_buffer, rdma_recv_signal_buffer,
        rank, num_ranks, MAX_QP_COUNT);
    const size_t num_qp_per_rank = MAX_QP_COUNT / num_ranks;

    // Sending phase
    if ((phases & LOW_LATENCY_SEND_PHASE) == 0)
        goto LOW_LATENCY_DISPATCH_RECV;

    // Expert counts
    __shared__ int shared_num_tokens_sent_per_expert[kNumWarpGroups];

    // There are 2 kinds of warps in this part:
    // 1. The first-kind warps for FP8 cast and sending top-k tokens
    // 2. The last warp for reading `topk_idx` and count for per-expert information
    if (warp_id < num_warps - 1) {
        constexpr int kNumElemsPerRead = sizeof(int4) / sizeof(nv_bfloat16);
        EP_DEVICE_ASSERT(kHidden % kNumElemsPerRead == 0);
        EP_STATIC_ASSERT(kNumElemsPerRead * 32 % kNumPerChannels == 0, "Invalid vectorization");
        const auto num_threads = (num_warps - 1) * 32;
        const size_t hidden_bf16_int4 = kHidden / kNumElemsPerRead;

        for (int token_idx = sm_id; token_idx < num_tokens; token_idx += num_sms) {
            const auto x_int4 = reinterpret_cast<const int4*>(x) + token_idx * hidden_bf16_int4;
            const auto rdma_x_src_idx = reinterpret_cast<int*>(reinterpret_cast<uint8_t*>(rdma_send_data_buffer) + token_idx * num_bytes_per_msg);
            const auto rdma_x_vec = reinterpret_cast<vec_t*>(reinterpret_cast<uint8_t*>(rdma_x_src_idx) + sizeof(int4));
            const auto rdma_x_scales = reinterpret_cast<float*>(reinterpret_cast<uint8_t*>(rdma_x_vec) + hidden_bytes);

            // Overlap top-k index read and source token index write
            auto dst_expert_idx = warp_id < num_topk ? static_cast<int>(__ldg(topk_idx + token_idx * num_topk + warp_id)) : -1;
            thread_id == 0 ? (*rdma_x_src_idx = token_idx) : 0;

            // FP8 cast
            #pragma unroll
            for (int i = thread_id; i < hidden_bf16_int4; i += num_threads) {
                    // Read
                    auto int4_value = __ldg(x_int4 + i);

                    if (kUseFP8) {
                        // Calculate local amax
                        auto bf16_values = reinterpret_cast<nv_bfloat16*>(&int4_value);
                        float fp32_values[kNumElemsPerRead];
                        float amax = kFP8Margin, scale, scale_inv;
                        #pragma unroll
                        for (int j = 0; j < kNumElemsPerRead; ++ j) {
                            fp32_values[j] = __bfloat162float(bf16_values[j]);
                            amax = fmaxf(amax, fabsf(fp32_values[j]));
                        }

                        // Reduce amax and scale
                        EP_STATIC_ASSERT(kNumElemsPerRead * 32 / kNumPerChannels == 2, "Invalid vectorization");
                        amax = half_warp_reduce_max(amax), scale = kFP8Amax / amax, scale_inv = amax * kFP8AmaxInv;
                        if (lane_id == 0 or lane_id == 16)
                            rdma_x_scales[i * kNumElemsPerRead / 128] = scale_inv;

                        // Cast into send buffer
                        vec_t int2_value;
                        auto fp8x2_values = reinterpret_cast<__nv_fp8x2_storage_t*>(&int2_value);
                        #pragma unroll
                        for (int j = 0; j < kNumElemsPerRead; j += 2) {
                            float2 fp32x2 = {fp32_values[j] * scale, fp32_values[j + 1] * scale};
                            fp8x2_values[j / 2] = __nv_cvt_float2_to_fp8x2(fp32x2, __NV_SATFINITE, __NV_E4M3);
                        }
                        rdma_x_vec[i] = int2_value;
                    } else {
                        // Reinterpret-cast is for C++14 compatibility
                        rdma_x_vec[i] = *reinterpret_cast<vec_t*>(&int4_value);
                    }
                }
            mc_bar_sync(1, num_threads);

            // Issue sends
            if (dst_expert_idx >= 0) {
                int slot_idx = lane_id == 0 ? atomicAdd(atomic_counter_per_expert + dst_expert_idx, 1) : 0;
                slot_idx = __shfl_sync(0xffffffff, slot_idx, 0);
                const auto dst_rank = dst_expert_idx / num_local_experts;
                const auto dst_expert_local_idx = dst_expert_idx % num_local_experts;
                const auto src_ptr = reinterpret_cast<const void*>(rdma_x_src_idx);
                const auto dst_ptr = reinterpret_cast<void*>(
                    reinterpret_cast<uint64_t>(rdma_recv_data_buffer) +
                    dst_expert_local_idx * num_ranks * num_max_dispatch_tokens_per_rank * num_bytes_per_msg +
                    rank * num_max_dispatch_tokens_per_rank * num_bytes_per_msg +
                    slot_idx * num_bytes_per_msg);

                void* write_dst = mc_route_put(comm_ctx, dst_rank, dst_ptr);
                if (write_dst != nullptr) {
                    // Local or P2P path — warp-cooperative copy
                    const auto* src_int4_ptr = reinterpret_cast<const int4*>(src_ptr);
                    const auto* dst_int4_ptr = reinterpret_cast<int4*>(write_dst);
                    UNROLLED_WARP_COPY(8, lane_id, num_int4_per_msg, dst_int4_ptr, src_int4_ptr, mc_ld_nc, mc_st_na);
                } else {
                    // IBGDA path — send directly from source buffer
                    mc_rdma_put(comm_ctx, dst_expert_local_idx % num_qp_per_rank, dst_rank, num_qp_per_rank,
                                      src_ptr, dst_ptr, num_bytes_per_msg, lane_id);
                }

                // Increase counter after finishing
                __syncwarp();
                lane_id == 0 ? mc_atomic_add_release(atomic_finish_counter_per_expert + dst_expert_idx, 1) : 0;
            }
        }
    } else if (warp_id == num_warps - 1) {
        EP_DEVICE_ASSERT(num_sms > 1);
        if (sm_id == 0) {
            // The first SM is also responsible for cleaning the next buffer
            #pragma unroll
            for (int i = lane_id; i < num_experts; i += 32)
                next_clean_buffer[i] = 0;

            // Notify before executing `int_p`
            __syncwarp();
            #pragma unroll
            for (int i = lane_id; i < num_experts; i += 32)
                mc_atomic_add_release(atomic_finish_counter_per_expert + i, FINISHED_SUM_TAG);
        }

        // This SM should be responsible for some destination experts, read `topk_idx` for them
        int expert_count[kNumWarpGroups] = {0};
        const auto expert_begin_idx = sm_id * kNumWarpGroups;
        const auto expert_end_idx = min(expert_begin_idx + kNumWarpGroups, num_experts);

        // Per lane count
        #pragma unroll 8
        for (int i = lane_id; i < num_tokens * num_topk; i += 32) {
            auto idx = static_cast<int>(__ldg(topk_idx + i));
            if (idx >= expert_begin_idx and idx < expert_end_idx)
                expert_count[idx - expert_begin_idx] ++;
        }

        // Warp reduce
        #pragma unroll
        for (int i = expert_begin_idx; i < expert_end_idx; ++ i) {
            auto sum = warp_reduce_sum(expert_count[i - expert_begin_idx]);
            if (lane_id == 0) {
                shared_num_tokens_sent_per_expert[i - expert_begin_idx] = sum;
                mc_atomic_add_release(atomic_finish_counter_per_expert + i, FINISHED_SUM_TAG - sum);
            }
        }
    }
    __syncthreads();

    // Issue count sends
    if (responsible_expert_idx < num_experts and sub_warp_id == 0 and lane_id == 0) {
        const auto dst_rank = responsible_expert_idx / num_local_experts;
        const auto dst_expert_local_idx = responsible_expert_idx % num_local_experts;
        const auto num_tokens_sent = shared_num_tokens_sent_per_expert[responsible_expert_idx - sm_id * kNumWarpGroups];

        // Wait local sends issued and send expert counts
        while (mc_ld_acquire(atomic_finish_counter_per_expert + responsible_expert_idx) != FINISHED_SUM_TAG * 2);
        if (dst_rank != rank) {
            int* signal_ptr = rdma_recv_signal_buffer + dst_expert_local_idx * num_ranks + rank;
            mc_red_add(comm_ctx, dst_rank, dst_expert_local_idx % num_qp_per_rank, num_qp_per_rank,
                       signal_ptr, static_cast<int32_t>(-num_tokens_sent - 1));
        } else {
            mc_st_release(rdma_recv_signal_buffer + dst_expert_local_idx * num_ranks + rank, -num_tokens_sent - 1);
        }

        // Clean workspace for next use
        atomic_counter_per_expert[responsible_expert_idx] = 0;
        atomic_finish_counter_per_expert[responsible_expert_idx] = 0;

        // Clean `packed_recv_count`
        if (dst_rank == 0)
            packed_recv_count[dst_expert_local_idx] = 0;
    }
    __syncwarp();

    // Receiving phase
    LOW_LATENCY_DISPATCH_RECV:
    if ((phases & LOW_LATENCY_RECV_PHASE) == 0)
        return;

    // For send-and-recv kernels, we need a grid sync for making `packed_recv_count` visible
    if (phases & LOW_LATENCY_SEND_PHASE)
        mc_grid_sync();

    // Receiving and packing
    if (responsible_expert_idx < num_experts) {
        const auto src_rank = responsible_expert_idx / num_local_experts;
        const auto local_expert_idx = responsible_expert_idx % num_local_experts;
        const auto rdma_recv_x_uint8 = reinterpret_cast<uint8_t*>(rdma_recv_data_buffer) +
                local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * num_bytes_per_msg +
                src_rank * num_max_dispatch_tokens_per_rank * num_bytes_per_msg;
        const auto recv_x_int4 = reinterpret_cast<int4*>(packed_recv_x) +
                local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * hidden_int4;
        const auto recv_x_scales = packed_recv_x_scales + local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * num_scales;
        const auto recv_src_info = packed_recv_src_info + local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank;
        const auto recv_range = packed_recv_layout_range + local_expert_idx * num_ranks;

        // Shared between sub-warps in warp groups
        __shared__ int shared_num_recv_tokens[kNumWarpGroups], shared_recv_token_begin_idx[kNumWarpGroups];

        // Wait tokens to arrive
        // NOTES: using sub-warp 1 to overlap with sub-warp 0
        int num_recv_tokens, recv_token_begin_idx;
        EP_STATIC_ASSERT(kNumWarpsPerGroup > 1, "Requires more than one warp per group");
        if (sub_warp_id == 1 and lane_id == 0) {
            unsigned long long start_time = clock64();
            while ((num_recv_tokens = mc_ld_acquire(rdma_recv_signal_buffer + local_expert_idx * num_ranks + src_rank)) == 0) {
                unsigned long long end_time = clock64();
                if (timeout_ticks != -1 && end_time - start_time > timeout_ticks) {
                    active_ranks[src_rank] = 0;
                }
                if (!active_ranks[src_rank]) {
                    num_recv_tokens = -1;
                    break;
                }
            }
            num_recv_tokens = -num_recv_tokens - 1;
            recv_token_begin_idx = atomicAdd(packed_recv_count + local_expert_idx, num_recv_tokens);
            shared_num_recv_tokens[warp_group_id] = num_recv_tokens;
            shared_recv_token_begin_idx[warp_group_id] = recv_token_begin_idx;
            recv_range[src_rank] = pack2<int, int64_t>(num_recv_tokens, recv_token_begin_idx);
        }
        mc_bar_sync(warp_group_id + 2, kNumWarpsPerGroup * 32);
        num_recv_tokens = shared_num_recv_tokens[warp_group_id];
        recv_token_begin_idx = shared_recv_token_begin_idx[warp_group_id];

        // Copy tokens
        EP_DEVICE_ASSERT(num_scales <= 64);
        for (int i = sub_warp_id; i < num_recv_tokens; i += kNumWarpsPerGroup) {
            // Copy source info
            const auto src_src_idx = reinterpret_cast<int*>(rdma_recv_x_uint8 + i * num_bytes_per_msg);
            if (lane_id == 0)
                recv_src_info[recv_token_begin_idx + i] = mc_ld_nc_s32(src_src_idx);
            __syncwarp();

            // Copy data
            // NOTES: only 2 load iterations for 7K hidden with 7 unrolls
            const auto src_data = reinterpret_cast<int4*>(reinterpret_cast<uint8_t*>(src_src_idx) + sizeof(int4));
            const auto dst_data = recv_x_int4 + (recv_token_begin_idx + i) * hidden_int4;
            UNROLLED_WARP_COPY(7, lane_id, hidden_int4, dst_data, src_data, mc_ld_nc, mc_st_na);

            // Copy scales
            if (kUseFP8) {
                const auto src_scales = reinterpret_cast<float*>(reinterpret_cast<uint8_t*>(src_data) + hidden_bytes);
                const auto dst_scales = reinterpret_cast<float*>(recv_x_scales + recv_token_begin_idx + i);
                const auto scale_stride = num_ranks * num_max_dispatch_tokens_per_rank;
                auto scale_0 = lane_id < num_scales ? mc_ld_nc_f32(src_scales + lane_id) : 0;
                auto scale_1 = (lane_id + 32) < num_scales ? mc_ld_nc_f32(src_scales + lane_id + 32) : 0;
                lane_id < num_scales ? dst_scales[lane_id * scale_stride] = scale_0 : 0.0f;
                (lane_id + 32) < num_scales ? dst_scales[(lane_id + 32) * scale_stride] = scale_1 : 0.0f;
            }
        }
    }
}

void dispatch(void* packed_recv_x, float* packed_recv_x_scales,
              int* packed_recv_src_info, int64_t* packed_recv_layout_range,
              int* packed_recv_count, int32_t* active_ranks,
              void* mxa_buffer,
              int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
              void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
              void* cuda_counter_buffer, void* cuda_data_buffer,
              void* raddrs, void* rkeys, void* qp_devctxs,
              const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
              const void* x, const int64_t* topk_idx,
              int* next_clean_buffer,
              int num_tokens, int hidden, int num_max_dispatch_tokens_per_rank,
              int num_topk, int num_experts, int rank, int num_ranks, bool use_fp8,
              void* workspace, cudaStream_t stream, int64_t timeout_ticks, int phases) {
    constexpr int kNumMaxTopK = 11;
    constexpr int kNumWarpsPerGroup = 4;
    constexpr int kNumWarpGroups = 8;
    EP_STATIC_ASSERT(kNumMaxTopK + 1 <= kNumWarpGroups * kNumWarpsPerGroup, "Too many top-k selections");

    const auto num_warps = kNumWarpGroups * kNumWarpsPerGroup;
    const auto num_sms = cell_div(num_experts, kNumWarpGroups);
    EP_HOST_ASSERT(num_topk <= kNumMaxTopK);

    // Workspace checks
    auto atomic_counter_per_expert = reinterpret_cast<int*>(workspace);
    auto atomic_finish_counter_per_expert = atomic_counter_per_expert + num_experts;
    EP_HOST_ASSERT(num_experts * sizeof(int) * 2 <= NUM_WORKSPACE_BYTES);

#define DISPATCH_LAUNCH_CASE(hidden) { \
auto dispatch_func = use_fp8 ? dispatch<true, kNumWarpGroups, kNumWarpsPerGroup, hidden> : \
                               dispatch<false, kNumWarpGroups, kNumWarpsPerGroup, hidden>; \
LAUNCH_KERNEL(&cfg, dispatch_func, \
              packed_recv_x, packed_recv_x_scales, \
              packed_recv_src_info, packed_recv_layout_range, \
              packed_recv_count, active_ranks, \
              mxa_buffer, \
              rdma_send_signal_buffer, rdma_recv_signal_buffer, \
              rdma_send_data_buffer, rdma_recv_data_buffer, \
              cuda_counter_buffer, cuda_data_buffer, \
              raddrs, rkeys, qp_devctxs, \
              nvlink_available, ipc_peer_ptrs, \
              x, topk_idx, \
              atomic_counter_per_expert, atomic_finish_counter_per_expert, \
              next_clean_buffer, \
              num_tokens, num_max_dispatch_tokens_per_rank, \
              num_topk, num_experts, rank, num_ranks, timeout_ticks, phases); } break

    SETUP_LAUNCH_CONFIG(num_sms, num_warps * 32, stream);
    SWITCH_HIDDEN(DISPATCH_LAUNCH_CASE);
#undef DISPATCH_LAUNCH_CASE
}

template <int kNumWarpGroups, int kNumWarpsPerGroup, int kHidden, int kNumMaxTopk>
__global__ __launch_bounds__(kNumWarpGroups * kNumWarpsPerGroup * 32, 1) void
combine_kernel(void* combined_x, int32_t* active_ranks,
        void* mxa_buffer,
        int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
        void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
        void* cuda_counter_buffer, void* cuda_data_buffer,
        void* raddrs, void* rkeys, void* qp_devctxs,
        const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
        const void* x, const int64_t* topk_idx, const float* topk_weights,
        const int* src_info, const int64_t* layout_range,
        int* next_clean_buffer,
        int* atomic_clean_flag,
        int num_combined_tokens, int hidden, int num_topk,
        int num_max_dispatch_tokens_per_rank,
        int num_experts, int rank, int num_ranks,
        int64_t timeout_ticks,
        int phases, bool zero_copy) {
    const auto sm_id = static_cast<int>(blockIdx.x);
    const auto num_sms = static_cast<int>(gridDim.x);
    const auto thread_id = static_cast<int>(threadIdx.x);
    const auto num_threads = static_cast<int>(blockDim.x);
    const auto warp_id = thread_id / 32, lane_id = get_lane_id();
    const auto num_local_experts = num_experts / num_ranks;
    const auto warp_group_id = warp_id / kNumWarpsPerGroup;
    const auto sub_warp_id = warp_id % kNumWarpsPerGroup;
    const auto responsible_expert_idx = sm_id * kNumWarpGroups + warp_group_id;

    // Data type staffs
    constexpr int kNumElemsPerInt4 = sizeof(int4) / sizeof(nv_bfloat16);
    const size_t hidden_bf16_int4 = kHidden / kNumElemsPerInt4;

    // Message package
    constexpr size_t num_bytes_per_slot = kHidden * sizeof(nv_bfloat16);
    EP_STATIC_ASSERT(num_bytes_per_slot % sizeof(int4) == 0, "Invalid vectorization");

    // Communication context — platform dispatch is inside comm_device.cuh
    const CommCtx comm_ctx = make_comm_ctx(
        mxa_buffer, nvlink_available, ipc_peer_ptrs,
        raddrs, rkeys, qp_devctxs,
        rdma_send_signal_buffer, rdma_recv_signal_buffer,
        rank, num_ranks, MAX_QP_COUNT);
    const size_t num_qp_per_rank = MAX_QP_COUNT / num_ranks;

    // Sending phase
    if ((phases & LOW_LATENCY_SEND_PHASE) == 0)
        goto LOW_LATENCY_COMBINE_RECV;

    // Clean up next buffer
    if (sm_id == 0 and warp_group_id == 0 and sub_warp_id == 0) {
        #pragma unroll
        for (int i = lane_id; i < num_experts; i += 32)
            next_clean_buffer[i] = 0;

        // Notify before executing `int_p`
        __syncwarp();
        if (lane_id == 0)
            mc_atomic_add_release(atomic_clean_flag, num_experts);
    }

    // Issue IBGDA sends
    if (responsible_expert_idx < num_experts) {
        const auto dst_rank = responsible_expert_idx / num_local_experts;
        const auto local_expert_idx = responsible_expert_idx % num_local_experts;
        const auto global_expert_idx = rank * num_local_experts + local_expert_idx;
        const auto layout = __ldg(layout_range + local_expert_idx * num_ranks + dst_rank);
        const auto local_x = reinterpret_cast<const int4*>(x) +
                local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * hidden_bf16_int4;
        const auto local_src_info = src_info + local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank;
        const auto rdma_send_x_vec = reinterpret_cast<uint8_t*>(rdma_send_data_buffer) +
                local_expert_idx * num_ranks * num_max_dispatch_tokens_per_rank * num_bytes_per_slot;

        // Unpack layout
        int offset, num_tokens_to_send;
        unpack2(layout, num_tokens_to_send, offset);

        // Issue IBGDA send
        for (int token_idx = offset + sub_warp_id; token_idx < offset + num_tokens_to_send; token_idx += kNumWarpsPerGroup) {
            const auto x_int4 = local_x + token_idx * hidden_bf16_int4;
            const auto rdma_send_type_row = reinterpret_cast<int*>(rdma_send_x_vec + token_idx * num_bytes_per_slot);
            const auto rdma_send_x_vec_row = reinterpret_cast<uint8_t*>(rdma_send_type_row);

            // Copy directly to local rank, or copy to buffer and issue RDMA
            auto src_idx = __ldg(local_src_info + token_idx);
            const auto buf_ptr = reinterpret_cast<void*>(rdma_send_x_vec_row);
            const auto dst_ptr = reinterpret_cast<void*>(
                reinterpret_cast<uint64_t>(rdma_recv_data_buffer) +
                (global_expert_idx * num_max_dispatch_tokens_per_rank + src_idx) * num_bytes_per_slot);

            void* write_dst = mc_route_put(comm_ctx, dst_rank, dst_ptr);
            if (write_dst != nullptr) {
                // Local or P2P path — warp-cooperative copy
                const auto dst_int4_ptr = reinterpret_cast<int4*>(write_dst);
                UNROLLED_WARP_COPY(7, lane_id, hidden_bf16_int4, dst_int4_ptr, x_int4, mc_ld_nc, mc_st_na);
            } else {
                // IBGDA path — stage to send buffer then RDMA write
                const auto buf_int4_ptr = reinterpret_cast<int4*>(buf_ptr);
                if (not zero_copy)
                    UNROLLED_WARP_COPY(7, lane_id, hidden_bf16_int4, buf_int4_ptr, x_int4, mc_ld_nc, mc_st_na);
                __syncwarp();
                mc_rdma_put(comm_ctx, local_expert_idx % num_qp_per_rank, dst_rank, num_qp_per_rank,
                                  buf_ptr, dst_ptr, num_bytes_per_slot, lane_id);
            }
        }

        // Put finishing flag
        EP_STATIC_ASSERT(kNumWarpsPerGroup > 1, "Requires more than one warp per group");
        mc_bar_sync(warp_group_id + 1, kNumWarpsPerGroup * 32);
        if (sub_warp_id == 1 and lane_id == 0) {
            while (mc_ld_acquire(atomic_clean_flag) == 0);
            if (dst_rank != rank) {
                int* signal_ptr = rdma_recv_signal_buffer + global_expert_idx;
                mc_signal(comm_ctx, dst_rank, local_expert_idx % num_qp_per_rank, num_qp_per_rank, signal_ptr, 1);
            } else {
                mc_st_release(rdma_recv_signal_buffer + global_expert_idx, 1);
            }
            mc_atomic_add_release(atomic_clean_flag, -1);
        }
        __syncwarp();
    }

    // Receiving phase
    LOW_LATENCY_COMBINE_RECV:
    if ((phases & LOW_LATENCY_RECV_PHASE) == 0)
        return;

    // Wait all ranks to arrive
    if (responsible_expert_idx < num_experts) {
        const auto src_rank = responsible_expert_idx / num_local_experts;
        EP_STATIC_ASSERT(kNumWarpsPerGroup > 1, "Invalid number of warps per group");
        if (sub_warp_id == 0 and lane_id == 0) {
            unsigned long long start_time = clock64();
            while (mc_ld_acquire(rdma_recv_signal_buffer + responsible_expert_idx) == 0) {
                unsigned long long end_time = clock64();
                if (timeout_ticks != -1 && end_time - start_time > timeout_ticks) {
                    active_ranks[src_rank] = 0;
                }
                if (!active_ranks[src_rank]) {
                    break;
                }
            }
        }
    }
    mc_grid_sync();

    // Reduce tokens with FP8 cast
    EP_DEVICE_ASSERT(num_topk <= 32 and hidden_bf16_int4 <= num_threads);
    EP_STATIC_ASSERT(kHidden % (32 * kNumElemsPerInt4) == 0, "Invalid vectorization");
    if (thread_id < hidden_bf16_int4) {
        for (int token_idx = sm_id; token_idx < num_combined_tokens; token_idx += num_sms) {
            // Read top-k indices and weights
            int reg_topk_idx[kNumMaxTopk];
            float reg_topk_weights[kNumMaxTopk];
            #pragma unroll
            for (int i = 0; i < num_topk; ++ i) {
                reg_topk_idx[i] = static_cast<int>(__ldg(topk_idx + token_idx * num_topk + i));
                reg_topk_weights[i] = __ldg(topk_weights + token_idx * num_topk + i);
            }

            float combined_values[kNumElemsPerInt4] = {0.0f};
            #pragma unroll
            for (int i = 0; i < num_topk; ++ i) if (reg_topk_idx[i] >= 0) {
                // Read from sources
                auto rdma_buffer_type = reinterpret_cast<const int*>(reinterpret_cast<uint8_t*>(rdma_recv_data_buffer) + (reg_topk_idx[i] * num_max_dispatch_tokens_per_rank + token_idx) * num_bytes_per_slot);
                auto rdma_buffer_row = reinterpret_cast<const uint8_t*>(rdma_buffer_type);

                // Reduce
                auto x_vec = mc_ld_nc(reinterpret_cast<const int4*>(rdma_buffer_row) + thread_id);
                const auto x_bf16 = reinterpret_cast<nv_bfloat16*>(&x_vec);
                #pragma unroll
                for (int j = 0; j < kNumElemsPerInt4; ++ j)
                    combined_values[j] += __bfloat162float(x_bf16[j]) * reg_topk_weights[i];
            }

            // Write results
            int4& combined_int4 = *reinterpret_cast<int4*>(combined_values);
            auto combined_bf16 = reinterpret_cast<nv_bfloat16*>(&combined_values);
            #pragma unroll
            for (int j = 0; j < kNumElemsPerInt4; ++ j)
                combined_bf16[j] = __float2bfloat16(combined_values[j]);
            (reinterpret_cast<int4*>(combined_x) + token_idx * hidden_bf16_int4)[thread_id] = combined_int4;
        }
    }
}

void launch_combine(void* combined_x, int32_t* active_ranks,
             void* mxa_buffer,
             int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
             void* rdma_send_data_buffer, void* rdma_recv_data_buffer,
             void* cuda_counter_buffer, void* cuda_data_buffer,
             void* raddrs, void* rkeys, void* qp_devctxs,
             const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
             const void* x, const int64_t* topk_idx, const float* topk_weights,
             const int* src_info, const int64_t* layout_range,
             int* next_clean_buffer,
             int num_combined_tokens, int hidden, int num_max_dispatch_tokens_per_rank,
             int num_topk, int num_experts, int rank, int num_ranks,
             void* workspace, cudaStream_t stream,
             int64_t timeout_ticks, int phases, bool zero_copy) {
    constexpr int kNumWarpsPerGroup = 4;
    constexpr int kNumWarpGroups = 8;
    constexpr int kNumMaxTopk = 11;

    const auto num_warps = kNumWarpGroups * kNumWarpsPerGroup;
    const auto num_sms = cell_div(num_experts, kNumWarpGroups);

    // Check workspace
    auto atomic_clean_flag = reinterpret_cast<int*>(workspace);
    EP_HOST_ASSERT(sizeof(int) <= NUM_WORKSPACE_BYTES);
    EP_HOST_ASSERT(num_topk <= kNumMaxTopk);

#define COMBINE_LAUNCH_CASE(hidden) { \
auto combine_func = combine_kernel<kNumWarpGroups, kNumWarpsPerGroup, hidden, kNumMaxTopk>; \
LAUNCH_KERNEL(&cfg, combine_func, \
              combined_x, active_ranks, \
              mxa_buffer, \
              rdma_send_signal_buffer, rdma_recv_signal_buffer, \
              rdma_send_data_buffer, rdma_recv_data_buffer, \
              cuda_counter_buffer, cuda_data_buffer, \
              raddrs, rkeys, qp_devctxs, \
              nvlink_available, ipc_peer_ptrs, \
              x, topk_idx, topk_weights, src_info, layout_range, \
              next_clean_buffer, \
              atomic_clean_flag, \
              num_combined_tokens, hidden, num_topk, \
              num_max_dispatch_tokens_per_rank, \
              num_experts, rank, num_ranks, \
              timeout_ticks, phases, zero_copy); } break

    SETUP_LAUNCH_CONFIG(num_sms, num_warps * 32, stream);
    SWITCH_HIDDEN(COMBINE_LAUNCH_CASE);
#undef COMBINE_LAUNCH_CASE
}

// =========================================================================
// DeepEP V2 ported kernel launchers
// =========================================================================

void launch_barrier(void* mxa_buffer, void* raddrs, void* rkeys, void* qp_devctxs,
             const int32_t* nvlink_available, void* const* ipc_peer_ptrs,
             int* rdma_send_signal_buffer, int* rdma_recv_signal_buffer,
             void* workspace, int rank, int num_ranks, int num_sms,
             cudaStream_t stream) {
    constexpr int kNumThreads = 1024;
    constexpr int64_t kNumTimeoutCycles = NUM_TIMEOUT_CYCLES;

#define BARRIER_LAUNCH_CASE(nranks)                                        \
    {                                                                      \
        auto func = barrier::barrier_impl<kNumThreads, nranks,             \
                                          kNumTimeoutCycles>;              \
        LAUNCH_KERNEL(&cfg, func, mxa_buffer, raddrs, rkeys, qp_devctxs,  \
                      nvlink_available, ipc_peer_ptrs,                     \
                      rdma_send_signal_buffer, rdma_recv_signal_buffer,    \
                      workspace, rank, num_ranks, num_sms);                \
    }                                                                      \
    break

    SETUP_LAUNCH_CONFIG(num_sms, kNumThreads, stream);
    SWITCH_RANKS(BARRIER_LAUNCH_CASE);
#undef BARRIER_LAUNCH_CASE
}

void dispatch_deterministic_prologue(
    const int64_t* topk_idx, int* dst_buffer_slot_idx, int num_tokens,
    int hidden, int num_topk, int num_experts, int rank, int num_ranks,
    int num_max_dispatch_tokens_per_rank, void* workspace,
    cudaStream_t stream) {
    constexpr int kNumWarps = 8;
    constexpr int kNumThreads = kNumWarps * 32;
    const auto num_sms = cell_div(num_experts, 8);

    // Shared memory: (1 + 2 * kNumWarps) * kNumRanks * sizeof(int)
    const auto smem_bytes =
        (1 + 2 * kNumWarps) * num_ranks * static_cast<int>(sizeof(int));

#define PROLOGUE_LAUNCH_CASE(nranks)                                          \
    {                                                                         \
        auto func = dispatch_prologue::                                       \
            dispatch_deterministic_prologue_impl<                             \
                kNumWarps, nranks>;                                           \
        cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                                \
        cudaLaunchAttribute attr[1];                                          \
        attr[0].id = cudaLaunchAttributeCooperative;                          \
        attr[0].val.cooperative = 1;                                          \
        cfg.attrs = attr;                                                     \
        cfg.numAttrs = 1;                                                     \
        LAUNCH_KERNEL(&cfg, func, topk_idx,                                   \
                      reinterpret_cast<int*>(workspace), dst_buffer_slot_idx, \
                      num_tokens, num_topk, num_experts,                      \
                      num_max_dispatch_tokens_per_rank, rank, num_sms);       \
    }                                                                         \
    break

    SWITCH_RANKS(PROLOGUE_LAUNCH_CASE);
#undef PROLOGUE_LAUNCH_CASE
}

void dispatch_copy_epilogue(
    void* buffer, void* workspace, int* psum_num_recv_tokens_per_rank,
    int* psum_num_recv_tokens_per_expert, void* recv_x, void* recv_sf,
    int64_t* recv_topk_idx, float* recv_topk_weights, int* recv_src_metadata,
    int num_recv_tokens, int hidden, int num_sf_packs,
    int recv_sf_token_stride, int recv_sf_hidden_stride, int num_topk,
    int num_experts, int rank, int num_ranks,
    int num_max_dispatch_tokens_per_rank, bool use_fp8, cudaStream_t stream) {
    constexpr int kNumWarps = 8;
    constexpr int kNumThreads = kNumWarps * 32;
    const auto num_sms = cell_div(num_experts, 8);

    // Shared memory: one staging buffer per warp
    // Elastic dispatch writes BF16 hidden values and metadata into the
    // communication buffer.  FP8 scales are generated by this epilogue directly
    // into recv_sf; they are not present in the staged communication token.
    const auto token_bytes =
        align<int>(hidden * sizeof(nv_bfloat16), 16) +
        align<int>(num_topk * (sizeof(int) + sizeof(float)) +
                       (1 + num_topk) * static_cast<int>(sizeof(int)),
                   16);
    const auto smem_bytes = kNumWarps * token_bytes;

#define EPILOGUE_LAUNCH_CASE(hidden_val)                                        \
    {                                                                          \
        constexpr int kHiddenBytes = (hidden_val) * sizeof(nv_bfloat16);      \
        switch (num_ranks) {                                                   \
            case 1: {                                                          \
                auto func = use_fp8 ? dispatch_epilogue::                      \
                    dispatch_copy_epilogue_impl<                               \
                        false, true, kNumWarps, 1, kHiddenBytes> :             \
                    dispatch_epilogue::dispatch_copy_epilogue_impl<            \
                        false, false, kNumWarps, 1, kHiddenBytes>;             \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func, buffer, workspace,                   \
                              psum_num_recv_tokens_per_rank,                   \
                              psum_num_recv_tokens_per_expert, recv_x,         \
                              recv_sf, recv_topk_idx, recv_topk_weights,       \
                              recv_src_metadata, num_recv_tokens,              \
                              num_sf_packs,                                    \
                              recv_sf_token_stride, recv_sf_hidden_stride,     \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank,                \
                              rank, num_sms);                                  \
            } break;                                                           \
            case 2: {                                                          \
                auto func = use_fp8 ? dispatch_epilogue::                      \
                    dispatch_copy_epilogue_impl<                               \
                        false, true, kNumWarps, 2, kHiddenBytes> :             \
                    dispatch_epilogue::dispatch_copy_epilogue_impl<            \
                        false, false, kNumWarps, 2, kHiddenBytes>;             \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func, buffer, workspace,                   \
                              psum_num_recv_tokens_per_rank,                   \
                              psum_num_recv_tokens_per_expert, recv_x,         \
                              recv_sf, recv_topk_idx, recv_topk_weights,       \
                              recv_src_metadata, num_recv_tokens,              \
                              num_sf_packs,                                    \
                              recv_sf_token_stride, recv_sf_hidden_stride,     \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank,                \
                              rank, num_sms);                                  \
            } break;                                                           \
            case 4: {                                                          \
                auto func = use_fp8 ? dispatch_epilogue::                      \
                    dispatch_copy_epilogue_impl<                               \
                        false, true, kNumWarps, 4, kHiddenBytes> :             \
                    dispatch_epilogue::dispatch_copy_epilogue_impl<            \
                        false, false, kNumWarps, 4, kHiddenBytes>;             \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func, buffer, workspace,                   \
                              psum_num_recv_tokens_per_rank,                   \
                              psum_num_recv_tokens_per_expert, recv_x,         \
                              recv_sf, recv_topk_idx, recv_topk_weights,       \
                              recv_src_metadata, num_recv_tokens,              \
                              num_sf_packs,                                    \
                              recv_sf_token_stride, recv_sf_hidden_stride,     \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank,                \
                              rank, num_sms);                                  \
            } break;                                                           \
            case 8: {                                                          \
                auto func = use_fp8 ? dispatch_epilogue::                      \
                    dispatch_copy_epilogue_impl<                               \
                        false, true, kNumWarps, 8, kHiddenBytes> :             \
                    dispatch_epilogue::dispatch_copy_epilogue_impl<            \
                        false, false, kNumWarps, 8, kHiddenBytes>;             \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func, buffer, workspace,                   \
                              psum_num_recv_tokens_per_rank,                   \
                              psum_num_recv_tokens_per_expert, recv_x,         \
                              recv_sf, recv_topk_idx, recv_topk_weights,       \
                              recv_src_metadata, num_recv_tokens,              \
                              num_sf_packs,                                    \
                              recv_sf_token_stride, recv_sf_hidden_stride,     \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank,                \
                              rank, num_sms);                                  \
            } break;                                                           \
            default:                                                           \
                EP_HOST_ASSERT(false && "Unsupported ranks");                  \
        }                                                                      \
    }                                                                          \
    break

    SWITCH_HIDDEN(EPILOGUE_LAUNCH_CASE);
#undef EPILOGUE_LAUNCH_CASE
}

void combine_reduce_epilogue(
    void* combined_x, float* combined_topk_weights,
    int64_t* combined_topk_idx, void* recv_buffer, void* bias_0, void* bias_1,
    int num_combined_tokens, int hidden, int num_topk, int num_experts,
    int rank, int num_ranks, int num_max_dispatch_tokens_per_rank,
    cudaStream_t stream) {
    constexpr int kNumWarps = 8;
    constexpr int kNumThreads = kNumWarps * 32;
    const auto num_sms = cell_div(num_experts, 8);

    // Shared memory: one staging buffer per warp
    const auto smem_bytes = kNumWarps * align<int>(hidden * sizeof(nv_bfloat16), 16);

#define COMBINE_EPILOGUE_LAUNCH_CASE(hidden_val)                                \
    {                                                                          \
        switch (num_ranks) {                                                   \
            case 1: {                                                          \
                auto func = combine_epilogue::                                 \
                    combine_reduce_epilogue_impl<                              \
                        false, false, kNumWarps, 1, hidden_val>;               \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func,                                          \
                              static_cast<nv_bfloat16*>(combined_x),           \
                              combined_topk_weights,                           \
                              combined_topk_idx, recv_buffer, bias_0, bias_1,  \
                              num_combined_tokens, num_topk, num_experts,      \
                              num_max_dispatch_tokens_per_rank,                \
                              rank, num_sms);                                  \
            } break;                                                           \
            case 2: {                                                          \
                auto func = combine_epilogue::                                 \
                    combine_reduce_epilogue_impl<                              \
                        false, false, kNumWarps, 2, hidden_val>;               \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func,                                          \
                              static_cast<nv_bfloat16*>(combined_x),           \
                              combined_topk_weights,                           \
                              combined_topk_idx, recv_buffer, bias_0, bias_1,  \
                              num_combined_tokens, num_topk, num_experts,      \
                              num_max_dispatch_tokens_per_rank,                \
                              rank, num_sms);                                  \
            } break;                                                           \
            case 4: {                                                          \
                auto func = combine_epilogue::                                 \
                    combine_reduce_epilogue_impl<                              \
                        false, false, kNumWarps, 4, hidden_val>;               \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func,                                      \
                              static_cast<nv_bfloat16*>(combined_x),           \
                              combined_topk_weights,                           \
                              combined_topk_idx, recv_buffer, bias_0, bias_1,  \
                              num_combined_tokens, num_topk, num_experts,      \
                              num_max_dispatch_tokens_per_rank,                \
                              rank, num_sms);                                  \
            } break;                                                           \
            case 8: {                                                          \
                auto func = combine_epilogue::                                 \
                    combine_reduce_epilogue_impl<                              \
                        false, false, kNumWarps, 8, hidden_val>;               \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func,                                      \
                              static_cast<nv_bfloat16*>(combined_x),           \
                              combined_topk_weights,                           \
                              combined_topk_idx, recv_buffer, bias_0, bias_1,  \
                              num_combined_tokens, num_topk, num_experts,      \
                              num_max_dispatch_tokens_per_rank,                \
                              rank, num_sms);                                  \
            } break;                                                           \
            default:                                                           \
                EP_HOST_ASSERT(false && "Unsupported ranks");                  \
        }                                                                      \
    }                                                                          \
    break

    SWITCH_HIDDEN(COMBINE_EPILOGUE_LAUNCH_CASE);
#undef COMBINE_EPILOGUE_LAUNCH_CASE
}

// =========================================================================
// Elastic dispatch (DeepEP V2 style with notify warps)
// =========================================================================

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
    int64_t timeout_ticks) {
    constexpr int kNumNotifyWarps = EP_NUM_NOTIFY_WARPS;
    constexpr int kNumDispatchWarps = EP_NUM_DISPATCH_WARPS;
    constexpr int kNumWarps = kNumNotifyWarps + kNumDispatchWarps;
    constexpr int kNumThreads = kNumWarps * 32;
    constexpr int kExpertAlignment = 1;
    const auto num_sms = cell_div(num_experts, 8);

    // Shared memory: notify warps (rank+expert counts) + dispatch warps
    // (staging buffers for hidden data + metadata)
    const auto notify_smem_bytes =
        align<int>(num_ranks + num_experts, kNumNotifyWarps * 32) *
        static_cast<int>(sizeof(int));
    const auto dispatch_token_bytes =
        static_cast<int>(layout::TokenLayout(
            hidden * static_cast<int>(sizeof(nv_bfloat16)), 0, num_topk, true)
                             .get_num_bytes());
    const auto smem_bytes =
        notify_smem_bytes + kNumDispatchWarps * dispatch_token_bytes;

#define ELASTIC_DISPATCH_LAUNCH_CASE(hidden_val)                               \
    {                                                                          \
        constexpr int kHiddenBytes = (hidden_val) * sizeof(nv_bfloat16);       \
        switch (num_ranks) {                                                   \
            case 1: {                                                          \
                auto func =                                                    \
                    reuse_slot_indices                                         \
                        ? elastic_dispatch::dispatch_impl<                     \
                              true, kNumNotifyWarps, kNumDispatchWarps, 1,     \
                              kHiddenBytes, kExpertAlignment,                  \
                              NUM_TIMEOUT_CYCLES>                              \
                        : elastic_dispatch::dispatch_impl<                     \
                              false, kNumNotifyWarps, kNumDispatchWarps, 1,    \
                              kHiddenBytes, kExpertAlignment,                  \
                              NUM_TIMEOUT_CYCLES>;                             \
                CUDA_CHECK(cudaFuncSetAttribute(                                  \
                    func, cudaFuncAttributeMaxDynamicSharedMemorySize,          \
                    smem_bytes));                                              \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func, x,                                        \
                              const_cast<int64_t*>(topk_idx),                  \
                              topk_weights,                                    \
                              copied_topk_idx,                                 \
                              cumulative_local_expert_recv_stats,              \
                              psum_num_recv_tokens_per_rank,                   \
                              psum_num_recv_tokens_per_expert,                 \
                              dst_buffer_slot_idx, num_tokens,                 \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank,                \
                              mxa_buffer, raddrs, rkeys, qp_devctxs,           \
                              nvlink_available, ipc_peer_ptrs,                 \
                              rdma_send_signal_buffer,                         \
                              rdma_recv_signal_buffer, rdma_send_data_buffer,  \
                              rdma_recv_data_buffer, buffer, workspace, rank,  \
                              num_ranks, num_sms);                             \
            } break;                                                           \
            case 2: {                                                          \
                auto func =                                                    \
                    reuse_slot_indices                                         \
                        ? elastic_dispatch::dispatch_impl<                     \
                              true, kNumNotifyWarps, kNumDispatchWarps, 2,     \
                              kHiddenBytes, kExpertAlignment,                  \
                              NUM_TIMEOUT_CYCLES>                              \
                        : elastic_dispatch::dispatch_impl<                     \
                              false, kNumNotifyWarps, kNumDispatchWarps, 2,    \
                              kHiddenBytes, kExpertAlignment,                  \
                              NUM_TIMEOUT_CYCLES>;                             \
                CUDA_CHECK(cudaFuncSetAttribute(                                  \
                    func, cudaFuncAttributeMaxDynamicSharedMemorySize,          \
                    smem_bytes));                                              \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func, x,                                        \
                              const_cast<int64_t*>(topk_idx),                  \
                              topk_weights,                                    \
                              copied_topk_idx,                                 \
                              cumulative_local_expert_recv_stats,              \
                              psum_num_recv_tokens_per_rank,                   \
                              psum_num_recv_tokens_per_expert,                 \
                              dst_buffer_slot_idx, num_tokens,                 \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank,                \
                              mxa_buffer, raddrs, rkeys, qp_devctxs,           \
                              nvlink_available, ipc_peer_ptrs,                 \
                              rdma_send_signal_buffer,                         \
                              rdma_recv_signal_buffer, rdma_send_data_buffer,  \
                              rdma_recv_data_buffer, buffer, workspace, rank,  \
                              num_ranks, num_sms);                             \
            } break;                                                           \
            case 4: {                                                          \
                auto func =                                                    \
                    reuse_slot_indices                                         \
                        ? elastic_dispatch::dispatch_impl<                     \
                              true, kNumNotifyWarps, kNumDispatchWarps, 4,     \
                              kHiddenBytes, kExpertAlignment,                  \
                              NUM_TIMEOUT_CYCLES>                              \
                        : elastic_dispatch::dispatch_impl<                     \
                              false, kNumNotifyWarps, kNumDispatchWarps, 4,    \
                              kHiddenBytes, kExpertAlignment,                  \
                              NUM_TIMEOUT_CYCLES>;                             \
                CUDA_CHECK(cudaFuncSetAttribute(                                  \
                    func, cudaFuncAttributeMaxDynamicSharedMemorySize,          \
                    smem_bytes));                                              \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func, x,                                    \
                              const_cast<int64_t*>(topk_idx),                  \
                              topk_weights,                                    \
                              copied_topk_idx,                                 \
                              cumulative_local_expert_recv_stats,              \
                              psum_num_recv_tokens_per_rank,                   \
                              psum_num_recv_tokens_per_expert,                 \
                              dst_buffer_slot_idx, num_tokens,                 \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank,                \
                              mxa_buffer, raddrs, rkeys, qp_devctxs,           \
                              nvlink_available, ipc_peer_ptrs,                 \
                              rdma_send_signal_buffer,                         \
                              rdma_recv_signal_buffer, rdma_send_data_buffer,  \
                              rdma_recv_data_buffer, buffer, workspace, rank,  \
                              num_ranks, num_sms);                             \
            } break;                                                           \
            case 8: {                                                          \
                auto func =                                                    \
                    reuse_slot_indices                                         \
                        ? elastic_dispatch::dispatch_impl<                     \
                              true, kNumNotifyWarps, kNumDispatchWarps, 8,     \
                              kHiddenBytes, kExpertAlignment,                  \
                              NUM_TIMEOUT_CYCLES>                              \
                        : elastic_dispatch::dispatch_impl<                     \
                              false, kNumNotifyWarps, kNumDispatchWarps, 8,    \
                              kHiddenBytes, kExpertAlignment,                  \
                              NUM_TIMEOUT_CYCLES>;                             \
                CUDA_CHECK(cudaFuncSetAttribute(                                  \
                    func, cudaFuncAttributeMaxDynamicSharedMemorySize,          \
                    smem_bytes));                                              \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func, x,                                    \
                              const_cast<int64_t*>(topk_idx),                  \
                              topk_weights,                                    \
                              copied_topk_idx,                                 \
                              cumulative_local_expert_recv_stats,              \
                              psum_num_recv_tokens_per_rank,                   \
                              psum_num_recv_tokens_per_expert,                 \
                              dst_buffer_slot_idx, num_tokens,                 \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank,                \
                              mxa_buffer, raddrs, rkeys, qp_devctxs,           \
                              nvlink_available, ipc_peer_ptrs,                 \
                              rdma_send_signal_buffer,                         \
                              rdma_recv_signal_buffer, rdma_send_data_buffer,  \
                              rdma_recv_data_buffer, buffer, workspace, rank,  \
                              num_ranks, num_sms);                             \
            } break;                                                           \
            default:                                                           \
                EP_HOST_ASSERT(false && "Unsupported ranks");                  \
        }                                                                      \
    }                                                                          \
    break

    SWITCH_HIDDEN(ELASTIC_DISPATCH_LAUNCH_CASE);
#undef ELASTIC_DISPATCH_LAUNCH_CASE
}

// =========================================================================
// Elastic combine (DeepEP V2 style with barrier + local reduction)
// =========================================================================

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
    cudaStream_t stream, int64_t timeout_ticks) {
    constexpr int kNumWarps = 8;
    constexpr int kNumThreads = kNumWarps * 32;
    const auto num_sms = cell_div(num_experts, 8);

    // Shared memory: one staging buffer per warp (hidden + metadata)
    const auto combine_token_bytes =
        static_cast<int>(layout::TokenLayout(
            hidden * static_cast<int>(sizeof(nv_bfloat16)), 0, num_topk, false)
                             .get_num_bytes());
    const auto smem_bytes = kNumWarps * combine_token_bytes;

#define ELASTIC_COMBINE_LAUNCH_CASE(hidden_val)                                \
    {                                                                          \
        switch (num_ranks) {                                                   \
            case 1: {                                                          \
                auto func = elastic_combine::combine_impl<                     \
                    kNumWarps, 1, hidden_val,                                  \
                    NUM_TIMEOUT_CYCLES>;                                       \
                CUDA_CHECK(cudaFuncSetAttribute(                                  \
                    func, cudaFuncAttributeMaxDynamicSharedMemorySize,          \
                    smem_bytes));                                              \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func,                                          \
                              static_cast<nv_bfloat16*>(x),                    \
                              topk_weights, src_metadata,                      \
                              psum_num_recv_tokens_per_rank, mxa_buffer,       \
                              raddrs, rkeys, qp_devctxs, nvlink_available,     \
                              ipc_peer_ptrs, rdma_send_signal_buffer,          \
                              rdma_recv_signal_buffer, rdma_send_data_buffer,  \
                              rdma_recv_data_buffer, buffer, workspace, rank,  \
                              num_ranks, num_sms, num_reduced_tokens,          \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank);               \
            } break;                                                           \
            case 2: {                                                          \
                auto func = elastic_combine::combine_impl<                     \
                    kNumWarps, 2, hidden_val,                                  \
                    NUM_TIMEOUT_CYCLES>;                                       \
                CUDA_CHECK(cudaFuncSetAttribute(                                  \
                    func, cudaFuncAttributeMaxDynamicSharedMemorySize,          \
                    smem_bytes));                                              \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func,                                          \
                              static_cast<nv_bfloat16*>(x),                    \
                              topk_weights, src_metadata,                      \
                              psum_num_recv_tokens_per_rank, mxa_buffer,       \
                              raddrs, rkeys, qp_devctxs, nvlink_available,     \
                              ipc_peer_ptrs, rdma_send_signal_buffer,          \
                              rdma_recv_signal_buffer, rdma_send_data_buffer,  \
                              rdma_recv_data_buffer, buffer, workspace, rank,  \
                              num_ranks, num_sms, num_reduced_tokens,          \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank);               \
            } break;                                                           \
            case 4: {                                                          \
                auto func = elastic_combine::combine_impl<                     \
                    kNumWarps, 4, hidden_val,                                  \
                    NUM_TIMEOUT_CYCLES>;                                       \
                CUDA_CHECK(cudaFuncSetAttribute(                                  \
                    func, cudaFuncAttributeMaxDynamicSharedMemorySize,          \
                    smem_bytes));                                              \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func,                                      \
                              static_cast<nv_bfloat16*>(x),                    \
                              topk_weights, src_metadata,                      \
                              psum_num_recv_tokens_per_rank, mxa_buffer,       \
                              raddrs, rkeys, qp_devctxs, nvlink_available,     \
                              ipc_peer_ptrs, rdma_send_signal_buffer,          \
                              rdma_recv_signal_buffer, rdma_send_data_buffer,  \
                              rdma_recv_data_buffer, buffer, workspace, rank,  \
                              num_ranks, num_sms, num_reduced_tokens,          \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank);               \
            } break;                                                           \
            case 8: {                                                          \
                auto func = elastic_combine::combine_impl<                     \
                    kNumWarps, 8, hidden_val,                                  \
                    NUM_TIMEOUT_CYCLES>;                                       \
                CUDA_CHECK(cudaFuncSetAttribute(                                  \
                    func, cudaFuncAttributeMaxDynamicSharedMemorySize,          \
                    smem_bytes));                                              \
                cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {kNumThreads, 1, 1}, smem_bytes, stream, nullptr, 0};                 \
                cudaLaunchAttribute attr[1];                                   \
                attr[0].id = cudaLaunchAttributeCooperative;                   \
                attr[0].val.cooperative = 1;                                   \
                cfg.attrs = attr;                                              \
                cfg.numAttrs = 1;                                              \
                LAUNCH_KERNEL(&cfg, func,                                      \
                              static_cast<nv_bfloat16*>(x),                    \
                              topk_weights, src_metadata,                      \
                              psum_num_recv_tokens_per_rank, mxa_buffer,       \
                              raddrs, rkeys, qp_devctxs, nvlink_available,     \
                              ipc_peer_ptrs, rdma_send_signal_buffer,          \
                              rdma_recv_signal_buffer, rdma_send_data_buffer,  \
                              rdma_recv_data_buffer, buffer, workspace, rank,  \
                              num_ranks, num_sms, num_reduced_tokens,          \
                              num_topk, num_experts,                           \
                              num_max_dispatch_tokens_per_rank);               \
            } break;                                                           \
            default:                                                           \
                EP_HOST_ASSERT(false && "Unsupported ranks");                  \
        }                                                                      \
    }                                                                          \
    break

    SWITCH_HIDDEN(ELASTIC_COMBINE_LAUNCH_CASE);
#undef ELASTIC_COMBINE_LAUNCH_CASE
}

} // namespace mooncake
