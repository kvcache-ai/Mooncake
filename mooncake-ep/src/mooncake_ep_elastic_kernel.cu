// clang-format off

#include <algorithm>
#include <stdexcept>
#include <string>

#include <mooncake_ep_configs.cuh>
#include <elastic/mooncake_ep_elastic_api.cuh>
#include <elastic/mooncake_ep_elastic_exception.cuh>
#include <elastic/mooncake_ep_elastic_launch.cuh>
#include <transport/device/comm_device.cuh>

namespace mooncake {
namespace {

constexpr int kElasticNumNotifyWarps = 4;
#ifdef MOONCAKE_EP_USE_MUSA
constexpr int kElasticNumDispatchWarps = 4;
constexpr int kElasticNumEpilogueWarps = 4;
#else
constexpr int kElasticNumDispatchWarps = 8;
constexpr int kElasticNumEpilogueWarps = 8;
#endif
constexpr int kElasticNumHybridScaleoutWarps = 4;
constexpr int kElasticNumHybridForwardWarps = 4;
constexpr int kElasticNumHybridScaleupWarps = 4;
constexpr int kElasticNumQPs = MAX_QP_COUNT;
constexpr int64_t kElasticTimeoutCycles = NUM_TIMEOUT_CYCLES;

inline int ceil_div(int x, int y) { return (x + y - 1) / y; }

inline int hybrid_num_channels(int num_sms) {
    return num_sms * kElasticNumHybridForwardWarps;
}

inline void* hybrid_combine_reduce_buffer_ptr(void* buffer, int hidden,
                                             int num_topk,
                                             int num_max_tokens_per_rank,
                                             int num_scaleout_ranks,
                                             int num_scaleup_ranks,
                                             bool allow_multiple_reduction) {
    const int num_tokens_in_scaleup_layout =
        allow_multiple_reduction && num_scaleup_ranks <= num_topk
            ? num_scaleup_ranks
            : num_topk;
    const auto token_layout = elastic::layout::TokenLayout(
        hidden * static_cast<int>(sizeof(nv_bfloat16)), 0, num_topk, false);
    const auto scaleup_buffer = elastic::layout::BufferLayout<false>(
        token_layout, num_tokens_in_scaleup_layout,
        num_scaleout_ranks * num_max_tokens_per_rank, buffer);
    return scaleup_buffer.get_buffer_end_ptr();
}

inline int dispatch_smem_bytes(int hidden, int elem_size, int num_sf_packs,
                               int num_topk, int num_ranks,
                               int num_experts, int num_notify_warps,
                               int num_dispatch_warps) {
    const int notify_smem_bytes = num_notify_warps == 0
        ? 0
        : elastic::math::align(num_ranks + num_experts, num_notify_warps * 32) *
              static_cast<int>(sizeof(int));
    const auto token_layout =
        elastic::layout::TokenLayout(hidden * elem_size,
                                     num_sf_packs * sizeof(sf_pack_t),
                                     num_topk, true);
    return notify_smem_bytes +
           num_dispatch_warps * static_cast<int>(token_layout.get_num_bytes<true>());
}

inline int dispatch_epilogue_smem_bytes(int hidden, int elem_size,
                                        int num_sf_packs, int num_topk,
                                        int num_warps) {
    const auto token_layout =
        elastic::layout::TokenLayout(hidden * elem_size,
                                     num_sf_packs * sizeof(sf_pack_t),
                                     num_topk, true);
    return num_warps * static_cast<int>(token_layout.get_num_bytes<true>());
}

inline int combine_smem_bytes(int hidden, int num_topk, int num_warps) {
    const auto token_layout = elastic::layout::TokenLayout(
        hidden * static_cast<int>(sizeof(nv_bfloat16)), 0, num_topk, false);
    return num_warps * static_cast<int>(token_layout.get_num_bytes<true>());
}

inline int combine_epilogue_smem_bytes(int hidden, int num_warps) {
    const auto token_layout = elastic::layout::TokenLayout(
        hidden * static_cast<int>(sizeof(nv_bfloat16)), 0, 0, false);
    return num_warps * static_cast<int>(token_layout.get_num_bytes<false>());
}

inline device::CommCtx make_comm_ctx(const ElasticLaunchContext& ctx) {
    device::CommCtx comm_ctx{};
    comm_ctx.rank = ctx.rank;
    comm_ctx.p2p.available = ctx.nvlink_available;
    comm_ctx.p2p.peer_ptrs = ctx.ipc_peer_ptrs;
    comm_ctx.p2p.local_base = ctx.gdr_buffer;
    comm_ctx.ibgda.qp_devctxs =
        reinterpret_cast<mlx5gda_qp_devctx*>(ctx.qp_devctxs);
    comm_ctx.ibgda.raddrs = reinterpret_cast<const uint64_t*>(ctx.raddrs);
    comm_ctx.ibgda.rkeys = reinterpret_cast<const uint32_t*>(ctx.rkeys);
    comm_ctx.ibgda.local_atomic_base = ctx.rdma_send_signal_buffer;
    comm_ctx.ibgda.remote_atomic_base = ctx.rdma_recv_signal_buffer;
    return comm_ctx;
}

#ifdef MOONCAKE_EP_USE_MUSA

// MUSA currently cannot rely on CUDA-style cooperative grid synchronization for
// the official elastic dispatch notify/prologue.  These prepare kernels keep the
// dispatch algorithm semantics unchanged (slot assignment, count publication and
// prefix-sum generation), but split that prologue into ordinary launches with an
// explicit scale-up barrier so peer count writes cannot race with local clears.
// Payload movement still goes through the common dispatch kernel and Mooncake
// Device API transport primitives; this is only a no-cooperative-grid-sync
// metadata preparation fallback.

__global__ void musa_elastic_prepare_init_kernel(void* workspace,
                                                 int num_scaleup_ranks,
                                                 int num_experts) {
    const auto layout = elastic::layout::WorkspaceLayout(
        workspace, 1, num_scaleup_ranks, num_experts);
    const int tid = blockIdx.x * blockDim.x + threadIdx.x;
    const int stride = blockDim.x * gridDim.x;
    for (int i = tid; i < num_scaleup_ranks + num_experts; i += stride) {
        layout.get_scaleup_rank_expert_count_ptr<true>()[i] = 0;
        layout.get_scaleup_rank_expert_count_ptr<false>()[i] = 0;
    }
    for (int i = tid; i < num_scaleup_ranks; i += stride) {
        layout.get_scaleup_atomic_sender_counter()[i] = 0;
    }
}

__global__ void musa_elastic_prepare_clear_barrier_kernel(
    device::CommCtx comm_ctx, void* workspace, int rank_idx,
    int num_scaleup_ranks, int num_experts, int64_t timeout_cycles) {
    const auto layout = elastic::layout::WorkspaceLayout(
        workspace, 1, num_scaleup_ranks, num_experts);
    const auto gin = elastic::transport::MooncakeGin(
        comm_ctx, 0, 0, 1, 0, rank_idx, num_scaleup_ranks, num_scaleup_ranks);
    constexpr int kTag = elastic::comm::kDeviceBarrierTag;
    const int status =
        static_cast<int>((*layout.get_nvl_barrier_counter_ptr(kTag)) & 3);
    const int phase = status & 1;
    const int sign = status >> 1;
    const int* base_signal = layout.get_nvl_barrier_signal_ptr(kTag, phase);

    if (threadIdx.x < num_scaleup_ranks) {
        auto* dst_ptr = const_cast<int*>(base_signal) + rank_idx;
        gin.red_add_rel<elastic::transport::ScaleupTeam>(
            dst_ptr, sign ? -1 : 1, threadIdx.x);
    }
    __syncthreads();

    if (threadIdx.x == 0) {
        atomicAdd(layout.get_nvl_barrier_counter_ptr(kTag), 1ULL);
        const int target = sign ? 0 : num_scaleup_ranks;
        const auto start_clock = clock64();
        while (true) {
            int sum = 0;
            for (int i = 0; i < num_scaleup_ranks; ++i) {
                sum += elastic::ptx::ld_acquire_sys<int>(
                    const_cast<int*>(base_signal) + i);
            }
            if (sum == target) break;
            if (timeout_cycles >= 0 &&
                clock64() - start_clock >= timeout_cycles) {
                printf(
                    "MUSA prepare clear barrier timeout, rank=%d sum=%d "
                    "target=%d\n",
                    rank_idx, sum, target);
                break;
            }
        }
    }
}

__global__ void musa_elastic_assign_slots_kernel(
    const int64_t* topk_idx, int* dst_buffer_slot_idx, void* workspace,
    int num_tokens, int num_max_tokens_per_rank, int num_experts, int num_topk,
    int num_scaleup_ranks, int rank_idx) {
    const auto layout = elastic::layout::WorkspaceLayout(
        workspace, 1, num_scaleup_ranks, num_experts);
    const int num_experts_per_rank = num_experts / num_scaleup_ranks;
    const int token_stride = blockDim.x * gridDim.x;
    for (int token_idx = blockIdx.x * blockDim.x + threadIdx.x;
         token_idx < num_tokens; token_idx += token_stride) {
        int seen_ranks[32];
        int num_seen = 0;
        for (int k = 0; k < num_topk; ++k) {
            dst_buffer_slot_idx[token_idx * num_topk + k] = -1;
        }
        for (int k = 0; k < num_topk; ++k) {
            const int expert_idx =
                static_cast<int>(topk_idx[token_idx * num_topk + k]);
            if (expert_idx < 0) continue;
            const int dst_rank = expert_idx / num_experts_per_rank;
            bool duplicate_rank = false;
            for (int i = 0; i < num_seen; ++i)
                duplicate_rank |= (seen_ranks[i] == dst_rank);
            if (!duplicate_rank) {
                seen_ranks[num_seen++] = dst_rank;
                const int slot = atomicAdd(
                    layout.get_scaleup_atomic_sender_counter() + dst_rank, 1);
                dst_buffer_slot_idx[token_idx * num_topk + k] =
                    rank_idx * num_max_tokens_per_rank + slot;
                atomicAdd(reinterpret_cast<unsigned long long*>(
                              layout.get_scaleup_rank_count_ptr<true>() +
                              dst_rank),
                          1ULL);
            }
            atomicAdd(reinterpret_cast<unsigned long long*>(
                          layout.get_scaleup_expert_count_ptr<true>() +
                          expert_idx),
                      1ULL);
        }
    }
}

__global__ void musa_elastic_publish_counts_kernel(
    device::CommCtx comm_ctx, void* workspace, int rank_idx,
    int num_scaleup_ranks, int num_experts) {
    const auto layout = elastic::layout::WorkspaceLayout(
        workspace, 1, num_scaleup_ranks, num_experts);
    const int num_experts_per_rank = num_experts / num_scaleup_ranks;
    const auto gin = elastic::transport::MooncakeGin(
        comm_ctx, 0, 0, 1, 0, rank_idx, num_scaleup_ranks, num_scaleup_ranks);
    const int tid = blockIdx.x * blockDim.x + threadIdx.x;
    const int stride = blockDim.x * gridDim.x;
    for (int dst_rank = tid; dst_rank < num_scaleup_ranks; dst_rank += stride) {
        const auto count = static_cast<int>(
            layout.get_scaleup_rank_count_ptr<true>()[dst_rank]);
        auto* dst = reinterpret_cast<int*>(
            layout.get_scaleup_rank_count_ptr<false>() + rank_idx);
        const auto encoded_count =
            elastic::math::encode_decode_positive(count);
        if (dst_rank == rank_idx) {
            elastic::ptx::st_release_sys<int>(dst, encoded_count);
        } else {
            gin.put_value<elastic::transport::ScaleupTeam>(
                dst, encoded_count, dst_rank, 0);
        }
    }
    for (int expert_idx = tid; expert_idx < num_experts; expert_idx += stride) {
        const int dst_rank = expert_idx / num_experts_per_rank;
        const int local_expert_idx = expert_idx % num_experts_per_rank;
        const auto count = static_cast<int>(
            layout.get_scaleup_expert_count_ptr<true>()[expert_idx]);
        auto* dst = reinterpret_cast<int*>(
            layout.get_scaleup_expert_count_ptr<false>() +
            rank_idx * num_experts_per_rank + local_expert_idx);
        const auto encoded_count =
            elastic::math::encode_decode_positive(count);
        if (dst_rank == rank_idx) {
            elastic::ptx::st_release_sys<int>(dst, encoded_count);
        } else {
            gin.put_value<elastic::transport::ScaleupTeam>(
                dst, encoded_count, dst_rank, 0);
        }
    }
}

__global__ void musa_elastic_wait_prefix_counts_kernel(
    void* workspace, int* psum_num_recv_tokens_per_scaleup_rank,
    int* psum_num_recv_tokens_per_expert, int rank_idx, int num_scaleup_ranks,
    int num_experts, int expert_alignment, int64_t timeout_cycles) {
    (void)rank_idx;
    const auto layout = elastic::layout::WorkspaceLayout(
        workspace, 1, num_scaleup_ranks, num_experts);
    const int num_experts_per_rank = num_experts / num_scaleup_ranks;
    if (threadIdx.x == 0 && blockIdx.x == 0) {
        int psum = 0;
        for (int src_rank = 0; src_rank < num_scaleup_ranks; ++src_rank) {
            auto* ptr = layout.get_scaleup_rank_count_ptr<false>() + src_rank;
            const auto start_clock = clock64();
            auto* word_ptr = reinterpret_cast<int*>(ptr);
            int count = elastic::math::encode_decode_positive(
                elastic::ptx::ld_acquire_sys<int>(word_ptr));
            while (!elastic::math::is_decoded_positive_ready(count)) {
                if (timeout_cycles >= 0 &&
                    clock64() - start_clock >= timeout_cycles) {
                    printf("MUSA prepare rank-count timeout, self=%d src=%d decoded=%d\n",
                           rank_idx, src_rank, count);
                    count = 0;
                    break;
                }
                count = elastic::math::encode_decode_positive(
                    elastic::ptx::ld_acquire_sys<int>(word_ptr));
            }
            *ptr = 0;
            psum += count;
            psum_num_recv_tokens_per_scaleup_rank[src_rank] = psum;
        }
        psum = 0;
        psum_num_recv_tokens_per_expert[0] = 0;
        for (int expert_idx = 0; expert_idx < num_experts_per_rank;
             ++expert_idx) {
            int count = 0;
            for (int src_rank = 0; src_rank < num_scaleup_ranks; ++src_rank) {
                auto* ptr = layout.get_scaleup_expert_count_ptr<false>() +
                            src_rank * num_experts_per_rank + expert_idx;
                const auto start_clock = clock64();
                auto* word_ptr = reinterpret_cast<int*>(ptr);
                int encoded_count = elastic::math::encode_decode_positive(
                    elastic::ptx::ld_acquire_sys<int>(word_ptr));
                while (!elastic::math::is_decoded_positive_ready(encoded_count)) {
                    if (timeout_cycles >= 0 &&
                        clock64() - start_clock >= timeout_cycles) {
                        printf("MUSA prepare expert-count timeout, self=%d src=%d expert=%d decoded=%d\n",
                               rank_idx, src_rank, expert_idx, encoded_count);
                        encoded_count = 0;
                        break;
                    }
                    encoded_count = elastic::math::encode_decode_positive(
                        elastic::ptx::ld_acquire_sys<int>(word_ptr));
                }
                count += encoded_count;
                *ptr = 0;
            }
            psum += elastic::math::align(count, expert_alignment);
            psum_num_recv_tokens_per_expert[expert_idx + 1] = psum;
        }
    }
}

void launch_musa_elastic_prepare_dispatch(
    const int64_t* topk_idx, int* psum_num_recv_tokens_per_scaleup_rank,
    int* psum_num_recv_tokens_per_expert, int* dst_buffer_slot_idx,
    int num_tokens, int num_max_tokens_per_rank, int num_experts, int num_topk,
    int expert_alignment, const device::CommCtx& comm_ctx,
    const ElasticLaunchContext& ctx, cudaStream_t stream) {
    constexpr int kThreads = 256;
    const int blocks = std::max(1, std::min(128, ceil_div(num_tokens, kThreads)));
    musa_elastic_prepare_init_kernel<<<blocks, kThreads, 0, stream>>>(
        ctx.workspace, ctx.num_scaleup_ranks, num_experts);
    CUDA_RUNTIME_CHECK(cudaGetLastError());
    // Each rank clears its local receive-count slots in the init kernel above.
    // Without a cross-rank phase boundary, a fast peer may publish counts into
    // this rank while its init kernel is still clearing the same slots, losing
    // the peer write.  CUDA's cooperative prologue gets this ordering from grid
    // synchronization; MUSA needs an explicit scale-up barrier before publish.
    musa_elastic_prepare_clear_barrier_kernel<<<1, kThreads, 0, stream>>>(
        comm_ctx, ctx.workspace, ctx.scaleup_rank_idx, ctx.num_scaleup_ranks,
        num_experts, ctx.timeout_cycles);
    CUDA_RUNTIME_CHECK(cudaGetLastError());
    musa_elastic_assign_slots_kernel<<<blocks, kThreads, 0, stream>>>(
        topk_idx, dst_buffer_slot_idx, ctx.workspace, num_tokens,
        num_max_tokens_per_rank, num_experts, num_topk, ctx.num_scaleup_ranks,
        ctx.scaleup_rank_idx);
    CUDA_RUNTIME_CHECK(cudaGetLastError());
    musa_elastic_publish_counts_kernel<<<blocks, kThreads, 0, stream>>>(
        comm_ctx, ctx.workspace, ctx.scaleup_rank_idx, ctx.num_scaleup_ranks,
        num_experts);
    CUDA_RUNTIME_CHECK(cudaGetLastError());
    musa_elastic_wait_prefix_counts_kernel<<<1, 1, 0, stream>>>(
        ctx.workspace, psum_num_recv_tokens_per_scaleup_rank,
        psum_num_recv_tokens_per_expert, ctx.scaleup_rank_idx,
        ctx.num_scaleup_ranks, num_experts, expert_alignment,
        ctx.timeout_cycles);
    CUDA_RUNTIME_CHECK(cudaGetLastError());
}

#endif

template <typename Kernel, typename... Args>
void launch_cooperative(Kernel kernel, int num_sms, int num_threads,
                        int smem_bytes, cudaStream_t stream, Args... args) {
#ifndef MOONCAKE_EP_USE_MUSA
    CUDA_CHECK(cudaFuncSetAttribute(
        kernel, cudaFuncAttributeMaxDynamicSharedMemorySize, smem_bytes));
#endif
#ifdef MOONCAKE_EP_USE_MUSA
    kernel<<<num_sms, num_threads, smem_bytes, stream>>>(args...);
    CUDA_RUNTIME_CHECK(cudaGetLastError());
#else
    cudaLaunchConfig_t cfg = {{num_sms, 1, 1}, {num_threads, 1, 1},
                              static_cast<unsigned int>(smem_bytes), stream,
                              nullptr, 0};
    cudaLaunchAttribute attr[1];
    attr[0].id = cudaLaunchAttributeCooperative;
    attr[0].val.cooperative = 1;
    cfg.attrs = attr;
    cfg.numAttrs = 1;
    CUDA_RUNTIME_CHECK(cudaLaunchKernelEx(&cfg, kernel, args...));
#endif
}

[[noreturn]] void unsupported_elastic_config(const char* op, int hidden,
                                             int num_experts, int num_topk,
                                             int num_max_tokens_per_rank,
                                             int num_sms, int ranks) {
    throw std::runtime_error(
        std::string("Unsupported Mooncake elastic ") + op +
        " static-template config: hidden=" + std::to_string(hidden) +
        ", experts=" + std::to_string(num_experts) +
        ", topk=" + std::to_string(num_topk) +
        ", max_tokens=" + std::to_string(num_max_tokens_per_rank) +
        ", num_sms=" + std::to_string(num_sms) +
        ", ranks=" + std::to_string(ranks));
}

}  // namespace

void launch_elastic_dispatch_deterministic_prologue(
    const int64_t* topk_idx, int* rank_count_buffer, int* dst_buffer_slot_idx,
    int num_tokens,
    int num_max_tokens_per_rank, int num_experts, int num_topk,
    int scaleup_rank_idx, int num_scaleup_ranks, int num_sms,
    int num_smem_bytes, cudaStream_t stream) {
    constexpr int kNumWarps = kElasticNumEpilogueWarps;
    constexpr int kNumThreads = kNumWarps * 32;
    const int smem_bytes = (1 + 2 * kNumWarps) * num_scaleup_ranks * sizeof(int);
    (void)num_smem_bytes;

#define LAUNCH_PROLOGUE(HIDDEN, EXPERTS, TOPK, MAXTOK, SMS, RANKS)             \
    do {                                                                       \
        auto kernel = elastic::dispatch_deterministic_prologue_impl<           \
            SMS, kNumWarps, RANKS, MAXTOK, EXPERTS, TOPK>;                    \
        launch_cooperative(kernel, SMS, kNumThreads, smem_bytes, stream,       \
                           const_cast<int64_t*>(topk_idx), rank_count_buffer,  \
                           dst_buffer_slot_idx, num_tokens,                    \
                           scaleup_rank_idx);                                  \
    } while (false)

#define TRY_PROLOGUE(H, E, K, M, S, R)                                         \
    if (hidden == H && num_experts == E && num_topk == K &&                    \
        num_max_tokens_per_rank == M && num_sms == S &&                       \
        num_scaleup_ranks == R) {                                             \
        LAUNCH_PROLOGUE(H, E, K, M, S, R);                                     \
        return;                                                               \
    }

    const int hidden = 0;
    (void)hidden;
#ifdef MOONCAKE_EP_USE_MUSA
    // Keep the MUSA compile set intentionally small while validating the
    // native elastic scale-up path; MUSA non-hybrid dispatch prepares slots in
    // a separate kernel and does not call this CUDA cooperative prologue.
    TRY_PROLOGUE(0, 256, 8, 128, 24, 2);
    TRY_PROLOGUE(0, 256, 8, 256, 24, 2);
    TRY_PROLOGUE(0, 256, 8, 512, 24, 2);
    TRY_PROLOGUE(0, 256, 8, 1024, 24, 2);
#else
    // Common production MoE shapes; hidden is irrelevant for this prologue.
    TRY_PROLOGUE(0, 128, 8, 128, 16, 8);
    TRY_PROLOGUE(0, 256, 8, 128, 16, 8);
    TRY_PROLOGUE(0, 256, 8, 256, 16, 8);
    TRY_PROLOGUE(0, 256, 8, 512, 16, 8);
    TRY_PROLOGUE(0, 256, 8, 128, 24, 8);
    TRY_PROLOGUE(0, 256, 8, 128, 24, 2);
    TRY_PROLOGUE(0, 256, 8, 256, 24, 8);
    TRY_PROLOGUE(0, 256, 8, 256, 24, 2);
    TRY_PROLOGUE(0, 256, 8, 512, 24, 8);
    TRY_PROLOGUE(0, 256, 8, 512, 24, 2);
    TRY_PROLOGUE(0, 256, 8, 1024, 24, 8);
    TRY_PROLOGUE(0, 256, 8, 1024, 24, 2);
    TRY_PROLOGUE(0, 384, 8, 128, 24, 8);
    TRY_PROLOGUE(0, 384, 8, 256, 24, 8);
    TRY_PROLOGUE(0, 384, 8, 512, 24, 8);
    TRY_PROLOGUE(0, 384, 8, 1024, 24, 8);
#endif

#undef TRY_PROLOGUE
#undef LAUNCH_PROLOGUE
    unsupported_elastic_config("deterministic_prologue", 0, num_experts,
                               num_topk, num_max_tokens_per_rank, num_sms,
                               num_scaleup_ranks);
}

void launch_mooncake_elastic_dispatch(
    void* x, void* sf, int64_t* topk_idx, float* topk_weights,
    int64_t* copied_topk_idx, int* cumulative_local_expert_recv_stats,
    int* psum_num_recv_tokens_per_scaleup_rank,
    int* psum_num_recv_tokens_per_expert, int* dst_buffer_slot_idx,
    int* token_metadata_at_forward, int num_tokens,
    int num_max_tokens_per_rank, int hidden, int elem_size, int num_sf_packs,
    int sf_token_stride, int sf_hidden_stride, int num_experts, int num_topk,
    int expert_alignment, int num_sms, int num_channels_per_sm,
    int num_smem_bytes, bool cached_mode, bool deterministic,
    bool do_cpu_sync, const ElasticLaunchContext& ctx, cudaStream_t stream) {
#ifdef MOONCAKE_EP_USE_MUSA
    const bool musa_use_prepared_slots = !cached_mode && ctx.num_scaleout_ranks == 1;
#else
    const bool musa_use_prepared_slots = false;
#endif
    const bool effective_cached_mode = cached_mode || musa_use_prepared_slots;
    const int num_notify_warps = effective_cached_mode ? 0 : kElasticNumNotifyWarps;
    const int num_dispatch_warps = kElasticNumDispatchWarps;
    const int num_threads = (num_notify_warps + num_dispatch_warps) * 32;
    const int smem_bytes = std::max(
        num_smem_bytes,
        dispatch_smem_bytes(hidden, elem_size, num_sf_packs, num_topk,
                            ctx.num_scaleup_ranks, num_experts,
                            num_notify_warps, num_dispatch_warps));
    const bool reuse_slot_indices = effective_cached_mode || deterministic;
    const auto comm_ctx = make_comm_ctx(ctx);
    (void)num_channels_per_sm;

#ifdef MOONCAKE_EP_USE_MUSA
    if (musa_use_prepared_slots) {
        launch_musa_elastic_prepare_dispatch(
            topk_idx, psum_num_recv_tokens_per_scaleup_rank,
            psum_num_recv_tokens_per_expert, dst_buffer_slot_idx, num_tokens,
            num_max_tokens_per_rank, num_experts, num_topk, expert_alignment,
            comm_ctx, ctx, stream);
    }
#endif

#ifndef MOONCAKE_EP_USE_MUSA
    if (ctx.num_scaleout_ranks != 1) {
        const bool hybrid_reuse_slot_indices = cached_mode;
        const int hybrid_dispatch_warps =
            kElasticNumHybridScaleoutWarps + kElasticNumHybridForwardWarps;
        const int hybrid_threads =
            (num_notify_warps + hybrid_dispatch_warps) * 32;
        const int hybrid_smem_bytes = std::max(
            num_smem_bytes,
            dispatch_smem_bytes(hidden, elem_size, num_sf_packs, num_topk,
                                ctx.num_scaleout_ranks * ctx.num_scaleup_ranks,
                                num_experts, num_notify_warps,
                                hybrid_dispatch_warps));

#define LAUNCH_HYBRID_DISPATCH(HB, SFP, E, K, M, S, SO, SU)                    \
        do {                                                                   \
            constexpr int kHiddenBytes = (HB);                                 \
            constexpr int kNumSFPacks = (SFP);                                 \
            if (cached_mode) {                                                 \
                auto kernel = elastic::hybrid_dispatch_impl<                   \
                    false, true, S, 0, kElasticNumHybridScaleoutWarps,         \
                    kElasticNumHybridForwardWarps, SO, SU, kHiddenBytes,       \
                    kNumSFPacks, M, E, K, 1, kElasticNumQPs,                   \
                    kElasticTimeoutCycles>;                                    \
                launch_cooperative(kernel, S, hybrid_threads,                  \
                                   hybrid_smem_bytes, stream, x,               \
                                   static_cast<sf_pack_t*>(sf), topk_idx,      \
                                   topk_weights, copied_topk_idx,              \
                                   cumulative_local_expert_recv_stats,         \
                                   psum_num_recv_tokens_per_scaleup_rank,      \
                                   psum_num_recv_tokens_per_expert,            \
                                   dst_buffer_slot_idx,                        \
                                   token_metadata_at_forward, num_tokens,      \
                                   sf_token_stride, sf_hidden_stride,          \
                                   comm_ctx, ctx.buffer, ctx.workspace,        \
                                   ctx.mapped_host_workspace,                  \
                                   ctx.scaleout_rank_idx,                      \
                                   ctx.scaleup_rank_idx);                      \
            } else if (hybrid_reuse_slot_indices) {                            \
                auto kernel = elastic::hybrid_dispatch_impl<                   \
                    false, true, S, kElasticNumNotifyWarps,                    \
                    kElasticNumHybridScaleoutWarps,                            \
                    kElasticNumHybridForwardWarps, SO, SU, kHiddenBytes,       \
                    kNumSFPacks, M, E, K, 1, kElasticNumQPs,                   \
                    kElasticTimeoutCycles>;                                    \
                launch_cooperative(kernel, S, hybrid_threads,                  \
                                   hybrid_smem_bytes, stream, x,               \
                                   static_cast<sf_pack_t*>(sf), topk_idx,      \
                                   topk_weights, copied_topk_idx,              \
                                   cumulative_local_expert_recv_stats,         \
                                   psum_num_recv_tokens_per_scaleup_rank,      \
                                   psum_num_recv_tokens_per_expert,            \
                                   dst_buffer_slot_idx,                        \
                                   token_metadata_at_forward, num_tokens,      \
                                   sf_token_stride, sf_hidden_stride,          \
                                   comm_ctx, ctx.buffer, ctx.workspace,        \
                                   ctx.mapped_host_workspace,                  \
                                   ctx.scaleout_rank_idx,                      \
                                   ctx.scaleup_rank_idx);                      \
            } else {                                                           \
                auto kernel = elastic::hybrid_dispatch_impl<                   \
                    false, false, S, kElasticNumNotifyWarps,                   \
                    kElasticNumHybridScaleoutWarps,                            \
                    kElasticNumHybridForwardWarps, SO, SU, kHiddenBytes,       \
                    kNumSFPacks, M, E, K, 1, kElasticNumQPs,                   \
                    kElasticTimeoutCycles>;                                    \
                launch_cooperative(kernel, S, hybrid_threads,                  \
                                   hybrid_smem_bytes, stream, x,               \
                                   static_cast<sf_pack_t*>(sf), topk_idx,      \
                                   topk_weights, copied_topk_idx,              \
                                   cumulative_local_expert_recv_stats,         \
                                   psum_num_recv_tokens_per_scaleup_rank,      \
                                   psum_num_recv_tokens_per_expert,            \
                                   dst_buffer_slot_idx,                        \
                                   token_metadata_at_forward, num_tokens,      \
                                   sf_token_stride, sf_hidden_stride,          \
                                   comm_ctx, ctx.buffer, ctx.workspace,        \
                                   ctx.mapped_host_workspace,                  \
                                   ctx.scaleout_rank_idx,                      \
                                   ctx.scaleup_rank_idx);                      \
            }                                                                  \
        } while (false)

#define TRY_HYBRID_DISPATCH_TYPED(H, E, K, M, S, SO, SU, EL, SFP)              \
        if (hidden == H && num_experts == E && num_topk == K &&                \
            num_max_tokens_per_rank == M && num_sms == S &&                   \
            ctx.num_scaleout_ranks == SO && ctx.num_scaleup_ranks == SU &&     \
            elem_size == EL && num_sf_packs == SFP && expert_alignment == 1 && \
            !do_cpu_sync) {                                                    \
            LAUNCH_HYBRID_DISPATCH((H) * (EL), SFP, E, K, M, S, SO, SU);       \
            return;                                                            \
        }

#define TRY_HYBRID_DISPATCH(H, E, K, M, S, SO, SU)                             \
        TRY_HYBRID_DISPATCH_TYPED(H, E, K, M, S, SO, SU,                       \
                                  static_cast<int>(sizeof(nv_bfloat16)), 0);   \
        TRY_HYBRID_DISPATCH_TYPED(H, E, K, M, S, SO, SU, 1, (H) / 128)

#define TRY_HYBRID_DISPATCH_SHAPE(H, E, K, M, S)                               \
        TRY_HYBRID_DISPATCH(H, E, K, M, S, 2, 4);                              \
        TRY_HYBRID_DISPATCH(H, E, K, M, S, 2, 8)

        TRY_HYBRID_DISPATCH_SHAPE(4096, 256, 8, 128, 24);
        TRY_HYBRID_DISPATCH_SHAPE(4096, 256, 8, 256, 24);
        TRY_HYBRID_DISPATCH_SHAPE(4096, 256, 8, 512, 24);
        TRY_HYBRID_DISPATCH_SHAPE(4096, 256, 8, 1024, 24);
        TRY_HYBRID_DISPATCH_SHAPE(7168, 256, 8, 128, 24);
        TRY_HYBRID_DISPATCH_SHAPE(7168, 256, 8, 256, 24);
        TRY_HYBRID_DISPATCH_SHAPE(7168, 256, 8, 512, 24);
        TRY_HYBRID_DISPATCH_SHAPE(7168, 256, 8, 1024, 24);
        TRY_HYBRID_DISPATCH_SHAPE(7168, 384, 8, 128, 24);
        TRY_HYBRID_DISPATCH_SHAPE(7168, 384, 8, 256, 24);
        TRY_HYBRID_DISPATCH_SHAPE(7168, 384, 8, 512, 24);
        TRY_HYBRID_DISPATCH_SHAPE(7168, 384, 8, 1024, 24);

#undef TRY_HYBRID_DISPATCH_SHAPE
#undef TRY_HYBRID_DISPATCH
#undef TRY_HYBRID_DISPATCH_TYPED
#undef LAUNCH_HYBRID_DISPATCH
    }
#endif

#define LAUNCH_DISPATCH(HB, SFP, E, K, M, S, R)                                \
    do {                                                                       \
        constexpr int kHiddenBytes = (HB);                                     \
        constexpr int kNumSFPacks = (SFP);                                     \
        if (effective_cached_mode) {                                           \
            auto kernel = elastic::dispatch_impl<                              \
                true, false, true, S, 0, kElasticNumDispatchWarps, R,          \
                kHiddenBytes, kNumSFPacks, M, E, K, 1, kElasticNumQPs,         \
                kElasticTimeoutCycles>;                                        \
            launch_cooperative(kernel, S, num_threads, smem_bytes, stream, x,  \
                               static_cast<sf_pack_t*>(sf), topk_idx,          \
                               topk_weights, copied_topk_idx,                  \
                               cumulative_local_expert_recv_stats,             \
                               psum_num_recv_tokens_per_scaleup_rank,          \
                               psum_num_recv_tokens_per_expert,                \
                               dst_buffer_slot_idx, num_tokens, sf_token_stride,\
                               sf_hidden_stride, comm_ctx, ctx.buffer,         \
                               ctx.workspace, ctx.mapped_host_workspace,       \
                               ctx.scaleup_rank_idx);                          \
        } else if (reuse_slot_indices) {                                       \
            auto kernel = elastic::dispatch_impl<                              \
                true, false, true, S, kElasticNumNotifyWarps,                  \
                kElasticNumDispatchWarps, R, kHiddenBytes, kNumSFPacks, M, E, K, 1, \
                kElasticNumQPs, kElasticTimeoutCycles>;                       \
            launch_cooperative(kernel, S, num_threads, smem_bytes, stream, x,  \
                               static_cast<sf_pack_t*>(sf), topk_idx,          \
                               topk_weights, copied_topk_idx,                  \
                               cumulative_local_expert_recv_stats,             \
                               psum_num_recv_tokens_per_scaleup_rank,          \
                               psum_num_recv_tokens_per_expert,                \
                               dst_buffer_slot_idx, num_tokens, sf_token_stride,\
                               sf_hidden_stride, comm_ctx, ctx.buffer,         \
                               ctx.workspace, ctx.mapped_host_workspace,       \
                               ctx.scaleup_rank_idx);                          \
        } else {                                                               \
            auto kernel = elastic::dispatch_impl<                              \
                true, false, false, S, kElasticNumNotifyWarps,                 \
                kElasticNumDispatchWarps, R, kHiddenBytes, kNumSFPacks, M, E, K, 1, \
                kElasticNumQPs, kElasticTimeoutCycles>;                       \
            launch_cooperative(kernel, S, num_threads, smem_bytes, stream, x,  \
                               static_cast<sf_pack_t*>(sf), topk_idx,          \
                               topk_weights, copied_topk_idx,                  \
                               cumulative_local_expert_recv_stats,             \
                               psum_num_recv_tokens_per_scaleup_rank,          \
                               psum_num_recv_tokens_per_expert,                \
                               dst_buffer_slot_idx, num_tokens, sf_token_stride,\
                               sf_hidden_stride, comm_ctx, ctx.buffer,         \
                               ctx.workspace, ctx.mapped_host_workspace,       \
                               ctx.scaleup_rank_idx);                          \
        }                                                                      \
    } while (false)

#define TRY_DISPATCH_TYPED(H, E, K, M, S, R, EL, SFP)                          \
    if (hidden == H && num_experts == E && num_topk == K &&                    \
        num_max_tokens_per_rank == M && num_sms == S &&                       \
        ctx.num_scaleup_ranks == R && elem_size == EL &&                       \
        num_sf_packs == SFP && expert_alignment == 1 && !do_cpu_sync) {        \
        LAUNCH_DISPATCH((H) * (EL), SFP, E, K, M, S, R);                       \
        return;                                                               \
    }

#define TRY_DISPATCH(H, E, K, M, S, R)                                         \
    TRY_DISPATCH_TYPED(H, E, K, M, S, R, static_cast<int>(sizeof(nv_bfloat16)), 0); \
    TRY_DISPATCH_TYPED(H, E, K, M, S, R, 1, (H) / 128)

#ifdef MOONCAKE_EP_USE_MUSA
    TRY_DISPATCH_TYPED(4096, 256, 8, 128, 24, 2,
                       static_cast<int>(sizeof(nv_bfloat16)), 0);
    TRY_DISPATCH_TYPED(4096, 256, 8, 256, 24, 2,
                       static_cast<int>(sizeof(nv_bfloat16)), 0);
    TRY_DISPATCH_TYPED(4096, 256, 8, 512, 24, 2,
                       static_cast<int>(sizeof(nv_bfloat16)), 0);
    TRY_DISPATCH_TYPED(4096, 256, 8, 1024, 24, 2,
                       static_cast<int>(sizeof(nv_bfloat16)), 0);
#else
    TRY_DISPATCH(4096, 256, 8, 128, 24, 8);
    TRY_DISPATCH(4096, 256, 8, 128, 24, 2);
    TRY_DISPATCH(4096, 256, 8, 256, 24, 8);
    TRY_DISPATCH(4096, 256, 8, 256, 24, 2);
    TRY_DISPATCH(4096, 256, 8, 512, 24, 8);
    TRY_DISPATCH(4096, 256, 8, 512, 24, 2);
    TRY_DISPATCH(4096, 256, 8, 1024, 24, 8);
    TRY_DISPATCH(4096, 256, 8, 1024, 24, 2);
    TRY_DISPATCH(7168, 256, 8, 128, 24, 8);
    TRY_DISPATCH(7168, 256, 8, 256, 24, 8);
    TRY_DISPATCH(7168, 256, 8, 512, 24, 8);
    TRY_DISPATCH(7168, 256, 8, 1024, 24, 8);
    TRY_DISPATCH(7168, 384, 8, 128, 24, 8);
    TRY_DISPATCH(7168, 384, 8, 256, 24, 8);
    TRY_DISPATCH(7168, 384, 8, 512, 24, 8);
    TRY_DISPATCH(7168, 384, 8, 1024, 24, 8);
#endif

#undef TRY_DISPATCH
#undef TRY_DISPATCH_TYPED
#undef LAUNCH_DISPATCH
    unsupported_elastic_config("dispatch", hidden, num_experts, num_topk,
                               num_max_tokens_per_rank, num_sms,
                               ctx.num_scaleup_ranks);
}

void launch_mooncake_elastic_dispatch_copy_epilogue(
    void* recv_x, void* recv_sf, int64_t* recv_topk_idx,
    float* recv_topk_weights, int* recv_src_metadata,
    int* channel_linked_list, int num_recv_tokens, int num_max_tokens_per_rank,
    int hidden, int elem_size, int num_sf_packs, int recv_sf_token_stride,
    int recv_sf_hidden_stride, int num_experts, int num_topk, int num_sms,
    int num_smem_bytes, int num_channels, bool do_expand, bool cached_mode,
    const ElasticLaunchContext& ctx, int* psum_num_recv_tokens_per_scaleup_rank,
    int* psum_num_recv_tokens_per_expert, cudaStream_t stream) {
    const int num_threads = kElasticNumEpilogueWarps * 32;
    const int smem_bytes = std::max(
        num_smem_bytes,
        dispatch_epilogue_smem_bytes(hidden, elem_size, num_sf_packs, num_topk,
                                     kElasticNumEpilogueWarps));

#ifndef MOONCAKE_EP_USE_MUSA
    if (ctx.num_scaleout_ranks != 1) {
#define LAUNCH_HYBRID_DISPATCH_EPILOGUE(HB, SFP, E, K, M, S, SO, SU, C)         \
        do {                                                                   \
            constexpr int kHiddenBytes = (HB);                                 \
            constexpr int kNumSFPacks = (SFP);                                 \
            auto kernel = do_expand ?                                          \
                elastic::dispatch_copy_epilogue_impl<                          \
                    true, false, S, C, kElasticNumEpilogueWarps, SO, SU,       \
                    kHiddenBytes, kNumSFPacks, M, E, K> :                      \
                (cached_mode ?                                                 \
                    elastic::dispatch_copy_epilogue_impl<                      \
                        false, true, S, C, kElasticNumEpilogueWarps, SO, SU,   \
                        kHiddenBytes, kNumSFPacks, M, E, K> :                  \
                    elastic::dispatch_copy_epilogue_impl<                      \
                        false, false, S, C, kElasticNumEpilogueWarps, SO, SU,  \
                        kHiddenBytes, kNumSFPacks, M, E, K>);                  \
            launch_cooperative(kernel, S, num_threads, smem_bytes, stream,     \
                               ctx.buffer, ctx.workspace,                      \
                               psum_num_recv_tokens_per_scaleup_rank,          \
                               psum_num_recv_tokens_per_expert, recv_x,        \
                               static_cast<sf_pack_t*>(recv_sf),               \
                               recv_topk_idx, recv_topk_weights,               \
                               recv_src_metadata, channel_linked_list,         \
                               num_recv_tokens, recv_sf_token_stride,          \
                               recv_sf_hidden_stride, ctx.scaleout_rank_idx,   \
                               ctx.scaleup_rank_idx);                          \
        } while (false)

#define TRY_HYBRID_DISPATCH_EPILOGUE_TYPED(H, E, K, M, S, SO, SU, EL, SFP)     \
        if (hidden == H && num_experts == E && num_topk == K &&                \
            num_max_tokens_per_rank == M && num_sms == S &&                   \
            ctx.num_scaleout_ranks == SO && ctx.num_scaleup_ranks == SU &&     \
            elem_size == EL && num_sf_packs == SFP &&                          \
            num_channels == hybrid_num_channels(S)) {                          \
            LAUNCH_HYBRID_DISPATCH_EPILOGUE((H) * (EL), SFP, E, K, M, S, SO, SU, \
                                            (S) * kElasticNumHybridForwardWarps); \
            return;                                                            \
        }

#define TRY_HYBRID_DISPATCH_EPILOGUE(H, E, K, M, S, SO, SU)                    \
        TRY_HYBRID_DISPATCH_EPILOGUE_TYPED(H, E, K, M, S, SO, SU,              \
                                           static_cast<int>(sizeof(nv_bfloat16)), 0); \
        TRY_HYBRID_DISPATCH_EPILOGUE_TYPED(H, E, K, M, S, SO, SU, 1, (H) / 128)

#define TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(H, E, K, M, S)                      \
        TRY_HYBRID_DISPATCH_EPILOGUE(H, E, K, M, S, 2, 4);                     \
        TRY_HYBRID_DISPATCH_EPILOGUE(H, E, K, M, S, 2, 8)

        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(4096, 256, 8, 128, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(4096, 256, 8, 256, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(4096, 256, 8, 512, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(4096, 256, 8, 1024, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(7168, 256, 8, 128, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(7168, 256, 8, 256, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(7168, 256, 8, 512, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(7168, 256, 8, 1024, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(7168, 384, 8, 128, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(7168, 384, 8, 256, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(7168, 384, 8, 512, 24);
        TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE(7168, 384, 8, 1024, 24);

#undef TRY_HYBRID_DISPATCH_EPILOGUE_SHAPE
#undef TRY_HYBRID_DISPATCH_EPILOGUE
#undef TRY_HYBRID_DISPATCH_EPILOGUE_TYPED
#undef LAUNCH_HYBRID_DISPATCH_EPILOGUE
    }
#endif

#define LAUNCH_DISPATCH_EPILOGUE(HB, SFP, E, K, M, S, R)                       \
    do {                                                                       \
        constexpr int kHiddenBytes = (HB);                                     \
        constexpr int kNumSFPacks = (SFP);                                     \
        auto kernel = do_expand ?                                              \
            elastic::dispatch_copy_epilogue_impl<                              \
                true, false, S, 1, kElasticNumEpilogueWarps, 1, R,             \
                kHiddenBytes, kNumSFPacks, M, E, K> :                          \
            (cached_mode ?                                                     \
                elastic::dispatch_copy_epilogue_impl<                          \
                    false, true, S, 1, kElasticNumEpilogueWarps, 1, R,         \
                    kHiddenBytes, kNumSFPacks, M, E, K> :                      \
                elastic::dispatch_copy_epilogue_impl<                          \
                    false, false, S, 1, kElasticNumEpilogueWarps, 1, R,        \
                    kHiddenBytes, kNumSFPacks, M, E, K>);                      \
        launch_cooperative(kernel, S, num_threads, smem_bytes, stream,         \
                           ctx.buffer, ctx.workspace,                          \
                           psum_num_recv_tokens_per_scaleup_rank,              \
                           psum_num_recv_tokens_per_expert, recv_x,            \
                           static_cast<sf_pack_t*>(recv_sf), recv_topk_idx,    \
                           recv_topk_weights, recv_src_metadata,               \
                           channel_linked_list, num_recv_tokens,               \
                           recv_sf_token_stride, recv_sf_hidden_stride,        \
                           ctx.scaleout_rank_idx, ctx.scaleup_rank_idx);       \
    } while (false)

#define TRY_DISPATCH_EPILOGUE_TYPED(H, E, K, M, S, R, EL, SFP)                 \
    if (hidden == H && num_experts == E && num_topk == K &&                    \
        num_max_tokens_per_rank == M && num_sms == S &&                       \
        ctx.num_scaleup_ranks == R && elem_size == EL &&                       \
        num_sf_packs == SFP) {                                                 \
        LAUNCH_DISPATCH_EPILOGUE((H) * (EL), SFP, E, K, M, S, R);              \
        return;                                                               \
    }

#define TRY_DISPATCH_EPILOGUE(H, E, K, M, S, R)                                \
    TRY_DISPATCH_EPILOGUE_TYPED(H, E, K, M, S, R, static_cast<int>(sizeof(nv_bfloat16)), 0); \
    TRY_DISPATCH_EPILOGUE_TYPED(H, E, K, M, S, R, 1, (H) / 128)

#ifdef MOONCAKE_EP_USE_MUSA
    TRY_DISPATCH_EPILOGUE_TYPED(4096, 256, 8, 128, 24, 2,
                                static_cast<int>(sizeof(nv_bfloat16)), 0);
    TRY_DISPATCH_EPILOGUE_TYPED(4096, 256, 8, 256, 24, 2,
                                static_cast<int>(sizeof(nv_bfloat16)), 0);
    TRY_DISPATCH_EPILOGUE_TYPED(4096, 256, 8, 512, 24, 2,
                                static_cast<int>(sizeof(nv_bfloat16)), 0);
    TRY_DISPATCH_EPILOGUE_TYPED(4096, 256, 8, 1024, 24, 2,
                                static_cast<int>(sizeof(nv_bfloat16)), 0);
#else
    TRY_DISPATCH_EPILOGUE(4096, 256, 8, 128, 24, 8);
    TRY_DISPATCH_EPILOGUE(4096, 256, 8, 128, 24, 2);
    TRY_DISPATCH_EPILOGUE(4096, 256, 8, 256, 24, 8);
    TRY_DISPATCH_EPILOGUE(4096, 256, 8, 256, 24, 2);
    TRY_DISPATCH_EPILOGUE(4096, 256, 8, 512, 24, 8);
    TRY_DISPATCH_EPILOGUE(4096, 256, 8, 512, 24, 2);
    TRY_DISPATCH_EPILOGUE(4096, 256, 8, 1024, 24, 8);
    TRY_DISPATCH_EPILOGUE(4096, 256, 8, 1024, 24, 2);
    TRY_DISPATCH_EPILOGUE(7168, 256, 8, 128, 24, 8);
    TRY_DISPATCH_EPILOGUE(7168, 256, 8, 256, 24, 8);
    TRY_DISPATCH_EPILOGUE(7168, 256, 8, 512, 24, 8);
    TRY_DISPATCH_EPILOGUE(7168, 256, 8, 1024, 24, 8);
    TRY_DISPATCH_EPILOGUE(7168, 384, 8, 128, 24, 8);
    TRY_DISPATCH_EPILOGUE(7168, 384, 8, 256, 24, 8);
    TRY_DISPATCH_EPILOGUE(7168, 384, 8, 512, 24, 8);
    TRY_DISPATCH_EPILOGUE(7168, 384, 8, 1024, 24, 8);
#endif

#undef TRY_DISPATCH_EPILOGUE
#undef TRY_DISPATCH_EPILOGUE_TYPED
#undef LAUNCH_DISPATCH_EPILOGUE
    unsupported_elastic_config("dispatch_copy_epilogue", hidden, num_experts,
                               num_topk, num_max_tokens_per_rank, num_sms,
                               ctx.num_scaleup_ranks);
}

void* launch_mooncake_elastic_combine(
    void* x, float* topk_weights, int* src_metadata,
    int* psum_num_recv_tokens_per_scaleup_rank,
    int* token_metadata_at_forward, int* channel_linked_list,
    int num_reduced_tokens, int num_max_tokens_per_rank, int hidden,
    int num_experts, int num_topk, int num_sms, int num_smem_bytes,
    int num_channels, bool use_expanded_layout, bool allow_multiple_reduction,
    const ElasticLaunchContext& ctx, cudaStream_t stream) {
    const int num_threads = kElasticNumEpilogueWarps * 32;
    const int smem_bytes = std::max(
        num_smem_bytes, combine_smem_bytes(hidden, num_topk, kElasticNumEpilogueWarps));
    const auto comm_ctx = make_comm_ctx(ctx);
    (void)token_metadata_at_forward;
    (void)channel_linked_list;

#ifndef MOONCAKE_EP_USE_MUSA
    if (ctx.num_scaleout_ranks != 1) {
        const int hybrid_combine_warps =
            kElasticNumHybridScaleupWarps + kElasticNumHybridForwardWarps;
        const int hybrid_threads = hybrid_combine_warps * 32;
        const int hybrid_smem_bytes = std::max(
            num_smem_bytes,
            combine_smem_bytes(hidden, num_topk, hybrid_combine_warps));

#define LAUNCH_HYBRID_COMBINE(H, E, K, M, S, SO, SU)                           \
        do {                                                                   \
            auto kernel = elastic::hybrid_combine_impl<                        \
                false, true, S, kElasticNumHybridScaleupWarps,                 \
                kElasticNumHybridForwardWarps, SO, SU, H, M, E, K,             \
                kElasticNumQPs, kElasticTimeoutCycles>;                        \
            launch_cooperative(kernel, S, hybrid_threads, hybrid_smem_bytes,   \
                               stream, static_cast<nv_bfloat16*>(x),           \
                               topk_weights, src_metadata,                     \
                               psum_num_recv_tokens_per_scaleup_rank,          \
                               token_metadata_at_forward, channel_linked_list, \
                               comm_ctx, ctx.buffer, ctx.workspace,            \
                               ctx.scaleout_rank_idx, ctx.scaleup_rank_idx,    \
                               num_reduced_tokens);                            \
        } while (false)

#define TRY_HYBRID_COMBINE(H, E, K, M, S, SO, SU)                              \
        if (hidden == H && num_experts == E && num_topk == K &&                \
            num_max_tokens_per_rank == M && num_sms == S &&                   \
            ctx.num_scaleout_ranks == SO && ctx.num_scaleup_ranks == SU &&     \
            allow_multiple_reduction && !use_expanded_layout &&                \
            num_channels == hybrid_num_channels(S) &&                          \
            token_metadata_at_forward != nullptr && channel_linked_list != nullptr) { \
            LAUNCH_HYBRID_COMBINE(H, E, K, M, S, SO, SU);                      \
            return hybrid_combine_reduce_buffer_ptr(                            \
                ctx.buffer, H, K, M, SO, SU, allow_multiple_reduction);         \
        }

#define TRY_HYBRID_COMBINE_SHAPE(H, E, K, M, S)                                \
        TRY_HYBRID_COMBINE(H, E, K, M, S, 2, 4);                               \
        TRY_HYBRID_COMBINE(H, E, K, M, S, 2, 8)

        TRY_HYBRID_COMBINE_SHAPE(4096, 256, 8, 128, 24);
        TRY_HYBRID_COMBINE_SHAPE(4096, 256, 8, 256, 24);
        TRY_HYBRID_COMBINE_SHAPE(4096, 256, 8, 512, 24);
        TRY_HYBRID_COMBINE_SHAPE(4096, 256, 8, 1024, 24);
        TRY_HYBRID_COMBINE_SHAPE(7168, 256, 8, 128, 24);
        TRY_HYBRID_COMBINE_SHAPE(7168, 256, 8, 256, 24);
        TRY_HYBRID_COMBINE_SHAPE(7168, 256, 8, 512, 24);
        TRY_HYBRID_COMBINE_SHAPE(7168, 256, 8, 1024, 24);
        TRY_HYBRID_COMBINE_SHAPE(7168, 384, 8, 128, 24);
        TRY_HYBRID_COMBINE_SHAPE(7168, 384, 8, 256, 24);
        TRY_HYBRID_COMBINE_SHAPE(7168, 384, 8, 512, 24);
        TRY_HYBRID_COMBINE_SHAPE(7168, 384, 8, 1024, 24);

#undef TRY_HYBRID_COMBINE_SHAPE
#undef TRY_HYBRID_COMBINE
#undef LAUNCH_HYBRID_COMBINE
    }
#endif

    (void)num_channels;

#define LAUNCH_COMBINE(H, E, K, M, S, R)                                       \
    do {                                                                       \
        auto kernel = elastic::combine_impl<true, false, true, S,              \
            kElasticNumEpilogueWarps, R, H, M, E, K, kElasticNumQPs,           \
            kElasticTimeoutCycles>;                                           \
        launch_cooperative(kernel, S, num_threads, smem_bytes, stream,         \
                           static_cast<nv_bfloat16*>(x), topk_weights,         \
                           src_metadata, psum_num_recv_tokens_per_scaleup_rank,\
                           comm_ctx, ctx.buffer, ctx.workspace,                \
                           ctx.scaleup_rank_idx, num_reduced_tokens);          \
    } while (false)

#define TRY_COMBINE(H, E, K, M, S, R)                                          \
    if (hidden == H && num_experts == E && num_topk == K &&                    \
        num_max_tokens_per_rank == M && num_sms == S &&                       \
        ctx.num_scaleup_ranks == R && !use_expanded_layout &&                  \
        allow_multiple_reduction) {                                            \
        LAUNCH_COMBINE(H, E, K, M, S, R);                                      \
        return ctx.buffer;                                                    \
    }

#ifdef MOONCAKE_EP_USE_MUSA
    TRY_COMBINE(4096, 256, 8, 128, 24, 2);
    TRY_COMBINE(4096, 256, 8, 256, 24, 2);
    TRY_COMBINE(4096, 256, 8, 512, 24, 2);
    TRY_COMBINE(4096, 256, 8, 1024, 24, 2);
#else
    TRY_COMBINE(4096, 256, 8, 128, 24, 8);
    TRY_COMBINE(4096, 256, 8, 128, 24, 2);
    TRY_COMBINE(4096, 256, 8, 256, 24, 8);
    TRY_COMBINE(4096, 256, 8, 256, 24, 2);
    TRY_COMBINE(4096, 256, 8, 512, 24, 8);
    TRY_COMBINE(4096, 256, 8, 512, 24, 2);
    TRY_COMBINE(4096, 256, 8, 1024, 24, 8);
    TRY_COMBINE(4096, 256, 8, 1024, 24, 2);
    TRY_COMBINE(7168, 256, 8, 128, 24, 8);
    TRY_COMBINE(7168, 256, 8, 256, 24, 8);
    TRY_COMBINE(7168, 256, 8, 512, 24, 8);
    TRY_COMBINE(7168, 256, 8, 1024, 24, 8);
    TRY_COMBINE(7168, 384, 8, 128, 24, 8);
    TRY_COMBINE(7168, 384, 8, 256, 24, 8);
    TRY_COMBINE(7168, 384, 8, 512, 24, 8);
    TRY_COMBINE(7168, 384, 8, 1024, 24, 8);
#endif

#undef TRY_COMBINE
#undef LAUNCH_COMBINE
    unsupported_elastic_config("combine", hidden, num_experts, num_topk,
                               num_max_tokens_per_rank, num_sms,
                               ctx.num_scaleup_ranks);
}

void launch_mooncake_elastic_combine_reduce_epilogue(
    void* combined_x, float* combined_topk_weights, int64_t* combined_topk_idx,
    int num_combined_tokens, int num_max_tokens_per_rank, int hidden,
    int num_experts, int num_topk, void* reduce_buffer, void* bias_0,
    void* bias_1, int num_sms, int num_smem_bytes, bool use_expanded_layout,
    bool allow_multiple_reduction, const ElasticLaunchContext& ctx,
    cudaStream_t stream) {
    const int num_threads = kElasticNumEpilogueWarps * 32;
    const int smem_bytes = std::max(
        num_smem_bytes, combine_epilogue_smem_bytes(hidden, kElasticNumEpilogueWarps));

#define LAUNCH_COMBINE_EPILOGUE(H, E, K, M, S, SO, SU)                         \
    do {                                                                       \
        auto kernel = elastic::combine_reduce_epilogue_impl<                   \
            false, true, S, kElasticNumEpilogueWarps, SO, SU, H, M, E, K>;     \
        launch_cooperative(kernel, S, num_threads, smem_bytes, stream,         \
                           static_cast<nv_bfloat16*>(combined_x),              \
                           combined_topk_weights, combined_topk_idx,           \
                           reduce_buffer, bias_0, bias_1, num_combined_tokens, \
                           ctx.scaleout_rank_idx, ctx.scaleup_rank_idx);       \
    } while (false)

#define TRY_COMBINE_EPILOGUE(H, E, K, M, S, SO, SU)                            \
    if (hidden == H && num_experts == E && num_topk == K &&                    \
        num_max_tokens_per_rank == M && num_sms == S &&                       \
        ctx.num_scaleout_ranks == SO && ctx.num_scaleup_ranks == SU &&         \
        !use_expanded_layout && allow_multiple_reduction) {                    \
        LAUNCH_COMBINE_EPILOGUE(H, E, K, M, S, SO, SU);                        \
        return;                                                               \
    }

#ifdef MOONCAKE_EP_USE_MUSA
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 128, 24, 1, 2);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 256, 24, 1, 2);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 512, 24, 1, 2);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 1024, 24, 1, 2);
#else
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 128, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 128, 24, 1, 2);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 256, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 256, 24, 1, 2);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 512, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 512, 24, 1, 2);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 1024, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(4096, 256, 8, 1024, 24, 1, 2);
    TRY_COMBINE_EPILOGUE(7168, 256, 8, 128, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(7168, 256, 8, 256, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(7168, 256, 8, 512, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(7168, 256, 8, 1024, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(7168, 384, 8, 128, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(7168, 384, 8, 256, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(7168, 384, 8, 512, 24, 1, 8);
    TRY_COMBINE_EPILOGUE(7168, 384, 8, 1024, 24, 1, 8);

#define TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(H, E, K, M, S)                       \
    TRY_COMBINE_EPILOGUE(H, E, K, M, S, 2, 4);                                 \
    TRY_COMBINE_EPILOGUE(H, E, K, M, S, 2, 8)

    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(4096, 256, 8, 128, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(4096, 256, 8, 256, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(4096, 256, 8, 512, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(4096, 256, 8, 1024, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(7168, 256, 8, 128, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(7168, 256, 8, 256, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(7168, 256, 8, 512, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(7168, 256, 8, 1024, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(7168, 384, 8, 128, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(7168, 384, 8, 256, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(7168, 384, 8, 512, 24);
    TRY_HYBRID_COMBINE_EPILOGUE_SHAPE(7168, 384, 8, 1024, 24);
#endif

#undef TRY_HYBRID_COMBINE_EPILOGUE_SHAPE

#undef TRY_COMBINE_EPILOGUE
#undef LAUNCH_COMBINE_EPILOGUE
    unsupported_elastic_config("combine_reduce_epilogue", hidden, num_experts,
                               num_topk, num_max_tokens_per_rank, num_sms,
                               ctx.num_scaleup_ranks);
}

}  // namespace mooncake
