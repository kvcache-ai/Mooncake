#include <elastic/mooncake_ep_elastic_buffer.h>
#include <elastic/mooncake_ep_elastic_launch.cuh>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include <cuda_runtime.h>

namespace mooncake {
namespace {

int64_t ceil_div_i64(int64_t x, int64_t y) { return (x + y - 1) / y; }

constexpr int kElasticHybridChannelsPerSm = 4;

int64_t align_i64(int64_t x, int64_t alignment) {
    return ceil_div_i64(x, alignment) * alignment;
}

int getenv_int(const char* name, int default_value) {
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') return default_value;
    return std::max(1, std::atoi(value));
}

int hybrid_num_channels(int num_sms) {
    return std::max(1, num_sms) * kElasticHybridChannelsPerSm;
}

int hybrid_num_max_tokens_per_channel(int num_max_tokens_per_rank,
                                      int num_sms) {
    return static_cast<int>(
        ceil_div_i64(num_max_tokens_per_rank, hybrid_num_channels(num_sms)));
}

int64_t elastic_workspace_num_bytes() {
    constexpr int64_t kNumMaxRanks = 1024;
    constexpr int64_t kNumMaxExperts = 2048;
    constexpr int64_t kNumMaxChannels = 8 * 160;
    constexpr int64_t kNumMaxInflightAGRS = 32;
    constexpr int64_t kNumBarrierTags = 16;

    int64_t num_bytes = 0;
    num_bytes += kNumBarrierTags *
                 (sizeof(unsigned long long) + 2 * kNumMaxRanks * sizeof(int));
    num_bytes += (kNumMaxRanks + kNumMaxExperts) * sizeof(int64_t);
    num_bytes += kNumMaxRanks * sizeof(int64_t) * 2;
    num_bytes += kNumMaxExperts * sizeof(int64_t) * 2;
    num_bytes += kNumMaxRanks * sizeof(int);
    num_bytes += kNumMaxRanks * sizeof(int) * 2;
    num_bytes += kNumMaxExperts * sizeof(int) * 2;
    num_bytes += kNumMaxRanks * kNumMaxChannels * sizeof(int64_t);
    num_bytes += kNumMaxRanks * kNumMaxChannels * sizeof(int);
    num_bytes += 2 * 2 * sizeof(int64_t);
    num_bytes += (kNumMaxInflightAGRS + 1) * kNumMaxRanks * sizeof(int);
    return align_i64(num_bytes, 32);
}

int64_t elastic_atomic_scratch_num_bytes() {
    return elastic_workspace_num_bytes();
}

int device_smem_bytes() {
#ifdef MOONCAKE_EP_USE_MUSA
    return 0;
#else
    int device = 0;
    cudaGetDevice(&device);
    int value = 0;
    cudaDeviceGetAttribute(&value, cudaDevAttrMaxSharedMemoryPerBlockOptin,
                           device);
    return value > 0 ? value : 98304;
#endif
}

}  // namespace

ElasticLaunchContext MooncakeElasticBuffer::make_launch_context(
    MooncakeEpBuffer& buffer, const ElasticTopology& topology,
    void* mapped_host_workspace, int64_t timeout_cycles) {
    ElasticLaunchContext ctx;
    auto* rdma = buffer.rdma_transport_;
    auto* gdr_base = static_cast<char*>(buffer.gdr_buffer);
    // Mooncake P2P/RDMA Device API translates remote pointers as offsets from
    // the registered GDR buffer base.  DeepEP elastic writes both `buffer` and
    // `workspace` pointers to peer ranks through GIN, so both regions must live
    // inside the same peer-visible registered allocation.  The elastic buffer
    // size reserves `elastic_workspace_num_bytes()` first; use that prefix as
    // the workspace.  RDMA atomics also need a separate local response area:
    // mlx5 atomics write the fetched old value to the WQE local address, so
    // reusing the remote signal workspace as `local_atomic_base` can corrupt
    // the barrier/signal slots.  Reserve an equal-sized scratch prefix after
    // the workspace, then place the communication buffer after both prefixes.
    const auto workspace_bytes = elastic_workspace_num_bytes();
    const auto atomic_scratch_bytes = elastic_atomic_scratch_num_bytes();
    ctx.gdr_buffer = gdr_base;
    ctx.nvlink_available = buffer.p2p_transport_->availableTablePtr();
    ctx.ipc_peer_ptrs = buffer.p2p_transport_->peerPtrsTablePtr();
    ctx.raddrs = rdma ? rdma->raddrsPtr() : nullptr;
    ctx.rkeys = rdma ? rdma->rkeysPtr() : nullptr;
    ctx.qp_devctxs = rdma ? rdma->qpDevCtxsPtr() : nullptr;
    ctx.rdma_send_signal_buffer = gdr_base + workspace_bytes;
    ctx.rdma_recv_signal_buffer = gdr_base;
    ctx.workspace = gdr_base;
    ctx.buffer = gdr_base + workspace_bytes + atomic_scratch_bytes;
    ctx.mapped_host_workspace = mapped_host_workspace;
    ctx.rank = topology.rank_idx;
    ctx.num_ranks = topology.num_ranks;
    ctx.scaleout_rank_idx = topology.scaleout_rank_idx;
    ctx.scaleup_rank_idx = topology.scaleup_rank_idx;
    ctx.num_scaleout_ranks = topology.num_scaleout_ranks;
    ctx.num_scaleup_ranks = topology.num_scaleup_ranks;
    ctx.is_scaleup_nvlink = true;
    ctx.num_qps = buffer.USE_QP_COUNT;
    ctx.timeout_cycles = timeout_cycles;
    return ctx;
}

MooncakeElasticBuffer::MooncakeElasticBuffer(
    int rank, int num_ranks, int64_t num_buffer_bytes,
    int64_t num_max_tokens_per_rank, int64_t hidden, int64_t num_topk,
    bool use_fp8_dispatch, bool deterministic, bool allow_hybrid_mode,
    bool allow_multiple_reduction, bool prefer_overlap_with_compute, int sl_idx,
    int num_allocated_qps, int num_cpu_timeout_secs, int num_gpu_timeout_secs) {
    config_.num_max_tokens_per_rank = num_max_tokens_per_rank;
    config_.hidden = hidden;
    config_.num_topk = num_topk;
    config_.use_fp8_dispatch = use_fp8_dispatch;
    config_.deterministic = deterministic;
    config_.allow_hybrid_mode = allow_hybrid_mode;
    config_.allow_multiple_reduction = allow_multiple_reduction;
    config_.prefer_overlap_with_compute = prefer_overlap_with_compute;
    config_.sl_idx = sl_idx;
    config_.num_allocated_qps = num_allocated_qps;
    config_.num_cpu_timeout_secs = num_cpu_timeout_secs;
    config_.num_gpu_timeout_secs = num_gpu_timeout_secs;

    topology_ = discover_topology(rank, num_ranks, allow_hybrid_mode);
    if (!allow_multiple_reduction) {
        throw std::runtime_error(
            "Mooncake ElasticBuffer currently supports only "
            "allow_multiple_reduction=true");
    }
    if (num_buffer_bytes == 0) {
        num_buffer_bytes = calculate_buffer_size(
            num_ranks, num_max_tokens_per_rank, hidden, num_topk,
            use_fp8_dispatch, allow_hybrid_mode, allow_multiple_reduction);
    }
    native_buffer_ =
        std::make_unique<MooncakeEpBuffer>(rank, num_ranks, num_buffer_bytes);
    host_workspace_bytes_ = elastic_workspace_num_bytes();
    CUDA_CHECK(cudaHostAlloc(&host_workspace_, host_workspace_bytes_,
                             cudaHostAllocMapped));
    CUDA_CHECK(
        cudaHostGetDevicePointer(&mapped_host_workspace_, host_workspace_, 0));
    std::memset(host_workspace_, 0, host_workspace_bytes_);
}

MooncakeElasticBuffer::~MooncakeElasticBuffer() {
    if (host_workspace_ != nullptr) {
        cudaFreeHost(host_workspace_);
        host_workspace_ = nullptr;
        mapped_host_workspace_ = nullptr;
    }
}

int64_t MooncakeElasticBuffer::calculate_buffer_size(
    int num_ranks, int64_t num_max_tokens_per_rank, int64_t hidden,
    int64_t num_topk, bool use_fp8_dispatch, bool allow_hybrid_mode,
    bool allow_multiple_reduction) {
    num_topk = std::max<int64_t>(1, num_topk);
    const int64_t dtype_bytes = use_fp8_dispatch ? 1 : 2;
    const int64_t scale_bytes =
        use_fp8_dispatch ? ceil_div_i64(hidden, 128) * 4 : 0;
    const int64_t token_bytes =
        align_i64(hidden * dtype_bytes, 32) + align_i64(scale_bytes, 32);
    const int64_t metadata_bytes = align_i64(
        num_topk * (sizeof(int) + sizeof(float)) + (1 + num_topk) * sizeof(int),
        32);
    const int64_t per_slot_bytes = token_bytes + metadata_bytes;
    const int64_t dispatch_bytes =
        num_ranks * num_max_tokens_per_rank * num_topk * per_slot_bytes * 2;
    const int64_t combine_factor = allow_multiple_reduction ? 3 : 4;
    const int64_t combine_bytes = dispatch_bytes * combine_factor;
    const int64_t hybrid_factor = allow_hybrid_mode && num_ranks > 1 ? 2 : 1;
    return elastic_workspace_num_bytes() + elastic_atomic_scratch_num_bytes() +
           hybrid_factor * (dispatch_bytes + combine_bytes);
}

std::tuple<int, int> MooncakeElasticBuffer::get_physical_domain_size() const {
    return {topology_.num_rdma_ranks, topology_.num_nvlink_ranks};
}

std::tuple<int, int> MooncakeElasticBuffer::get_logical_domain_size() const {
    return {topology_.num_scaleout_ranks, topology_.num_scaleup_ranks};
}

std::shared_ptr<void>
MooncakeElasticBuffer::ensure_deterministic_rank_count_buffer(int num_sms) {
    const int64_t required_bytes = static_cast<int64_t>(sizeof(int)) * num_sms *
                                   topology_.num_scaleup_ranks;
    if (deterministic_rank_count_buffer_ != nullptr &&
        deterministic_rank_count_buffer_bytes_ >= required_bytes) {
        return deterministic_rank_count_buffer_;
    }

    void* buffer_ptr = nullptr;
    CUDA_CHECK(cudaMalloc(&buffer_ptr, required_bytes));
    deterministic_rank_count_buffer_ =
        std::shared_ptr<void>(buffer_ptr, [](void* p) { cudaFree(p); });
    deterministic_rank_count_buffer_bytes_ = required_bytes;
    return deterministic_rank_count_buffer_;
}

int MooncakeElasticBuffer::get_theoretical_num_sms(int num_experts,
                                                   int num_topk) const {
    int device = 0;
    cudaGetDevice(&device);
    cudaDeviceProp prop{};
    cudaGetDeviceProperties(&prop, device);
    if (config_.prefer_overlap_with_compute) {
        return std::max(1, std::min(24, prop.multiProcessorCount / 4));
    }
    return std::max(1, std::min({40, prop.multiProcessorCount / 2,
                                 std::max(1, num_experts * num_topk)}));
}

std::optional<EventHandle> MooncakeElasticBuffer::dispatch(
    uint64_t x_ptr, int x_element_size, uint64_t sf_ptr, int num_tokens,
    int hidden, int num_sf_packs, int sf_token_stride, int sf_hidden_stride,
    uint64_t topk_idx_ptr, int num_topk, uint64_t topk_weights_ptr,
    uint64_t active_ranks_ptr, int num_experts, int num_max_tokens_per_rank,
    int expert_alignment, int num_sms, bool do_expand,
    bool async_with_compute_stream, uint64_t compute_stream_ptr,
    bool cached_mode, uint64_t psum_num_recv_tokens_per_scaleup_rank_ptr,
    uint64_t psum_num_recv_tokens_per_expert_ptr,
    uint64_t dst_buffer_slot_idx_ptr, uint64_t token_metadata_at_forward_ptr,
    uint64_t channel_linked_list_ptr, uint64_t recv_x_ptr,
    uint64_t recv_x_scales_ptr, uint64_t recv_topk_idx_ptr,
    uint64_t recv_topk_weights_ptr, uint64_t recv_src_metadata_ptr) {
    const bool use_sf = sf_ptr != 0;
    EP_HOST_ASSERT(num_experts % topology_.num_ranks == 0);

    const int num_local_experts = num_experts / topology_.num_ranks;
    // The copy epilogue uses `kNumMaxTokensPerRank * kNumRanks` as the
    // no-CPU-sync sentinel and then reads the real local receive count from the
    // GPU prefix-sum tensor. In hybrid mode each scale-up peer may receive
    // tokens forwarded from every scale-out rank, so the conservative output
    // capacity and sentinel must cover the full logical world, not just the
    // intra-node scale-up domain.
    const int num_recv_tokens = num_max_tokens_per_rank * topology_.num_ranks;
    const int num_smem_bytes = device_smem_bytes();
    const int num_channels_per_sm = 1;
    const int num_channels = num_sms * num_channels_per_sm;
    const bool use_hybrid = topology_.num_scaleout_ranks != 1;
    const int hybrid_channels = use_hybrid ? hybrid_num_channels(num_sms) : 0;
    const int hybrid_max_tokens_per_channel =
        use_hybrid ? hybrid_num_max_tokens_per_channel(num_max_tokens_per_rank,
                                                       num_sms)
                   : 0;

    EP_HOST_ASSERT(x_ptr != 0 && topk_idx_ptr != 0 && active_ranks_ptr != 0);
    EP_HOST_ASSERT(psum_num_recv_tokens_per_scaleup_rank_ptr != 0);
    EP_HOST_ASSERT(psum_num_recv_tokens_per_expert_ptr != 0);
    EP_HOST_ASSERT(dst_buffer_slot_idx_ptr != 0);
    EP_HOST_ASSERT(recv_x_ptr != 0 && recv_topk_idx_ptr != 0 &&
                   recv_src_metadata_ptr != 0);
    if (use_hybrid) {
        EP_HOST_ASSERT(token_metadata_at_forward_ptr != 0);
        EP_HOST_ASSERT(channel_linked_list_ptr != 0);
    }

    void* x = reinterpret_cast<void*>(x_ptr);
    void* sf = reinterpret_cast<void*>(sf_ptr);
    auto* topk_idx = reinterpret_cast<int64_t*>(topk_idx_ptr);
    auto* topk_weights = reinterpret_cast<float*>(topk_weights_ptr);
    auto* active_ranks = reinterpret_cast<int*>(active_ranks_ptr);
    auto* psum_num_recv_tokens_per_scaleup_rank =
        reinterpret_cast<int*>(psum_num_recv_tokens_per_scaleup_rank_ptr);
    auto* psum_num_recv_tokens_per_expert =
        reinterpret_cast<int*>(psum_num_recv_tokens_per_expert_ptr);
    auto* dst_buffer_slot_idx = reinterpret_cast<int*>(dst_buffer_slot_idx_ptr);
    auto* token_metadata_at_forward =
        reinterpret_cast<int*>(token_metadata_at_forward_ptr);
    auto* channel_linked_list = reinterpret_cast<int*>(channel_linked_list_ptr);
    void* recv_x = reinterpret_cast<void*>(recv_x_ptr);
    void* recv_x_scales = reinterpret_cast<void*>(recv_x_scales_ptr);
    auto* recv_topk_idx = reinterpret_cast<int64_t*>(recv_topk_idx_ptr);
    auto* recv_topk_weights = reinterpret_cast<float*>(recv_topk_weights_ptr);
    auto* recv_src_metadata = reinterpret_cast<int*>(recv_src_metadata_ptr);

    auto compute_stream_raw =
        reinterpret_cast<cudaStream_t>(compute_stream_ptr);
    auto launch_stream = native_buffer_->comm_stream;
    stream_wait(launch_stream, compute_stream_raw);

    const int64_t timeout_cycles =
        config_.num_gpu_timeout_secs < 0
            ? -1
            : static_cast<int64_t>(native_buffer_->clock_rate_khz) *
                  static_cast<int64_t>(config_.num_gpu_timeout_secs) * 1000;
    auto launch_ctx = make_launch_context(
        *native_buffer_, topology_, mapped_host_workspace_, timeout_cycles);

    std::shared_ptr<void> deterministic_rank_count_buffer;
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA non-hybrid dispatch always runs
    // launch_musa_elastic_prepare_dispatch(), which assigns slots and publishes
    // counts without cooperative grid sync.
    const bool run_deterministic_prologue = false;
#else
    const bool run_deterministic_prologue =
        config_.deterministic && !cached_mode && !use_hybrid;
#endif
    if (run_deterministic_prologue) {
        deterministic_rank_count_buffer =
            ensure_deterministic_rank_count_buffer(num_sms);
        launch_elastic_dispatch_deterministic_prologue(
            topk_idx, static_cast<int*>(deterministic_rank_count_buffer.get()),
            dst_buffer_slot_idx, num_tokens, num_max_tokens_per_rank,
            num_experts, num_topk, topology_.scaleup_rank_idx,
            topology_.num_scaleup_ranks, num_sms, num_smem_bytes,
            launch_stream);
    }

    launch_mooncake_elastic_dispatch(
        x, sf, topk_idx, topk_weights, nullptr, nullptr,
        psum_num_recv_tokens_per_scaleup_rank, psum_num_recv_tokens_per_expert,
        dst_buffer_slot_idx, token_metadata_at_forward, num_tokens,
        num_max_tokens_per_rank, hidden, x_element_size, num_sf_packs,
        sf_token_stride, sf_hidden_stride, num_experts, num_topk,
        expert_alignment, num_sms,
        use_hybrid ? kElasticHybridChannelsPerSm : num_channels_per_sm,
        num_smem_bytes, cached_mode, config_.deterministic, false, launch_ctx,
        launch_stream);

    const int recv_sf_token_stride = num_sf_packs;
    const int recv_sf_hidden_stride = 1;
    auto* epilogue_psum_num_recv_tokens_per_expert =
        do_expand ? psum_num_recv_tokens_per_expert
                  : psum_num_recv_tokens_per_expert + 1;

    launch_mooncake_elastic_dispatch_copy_epilogue(
        recv_x, recv_x_scales, recv_topk_idx, recv_topk_weights,
        recv_src_metadata, channel_linked_list, num_recv_tokens,
        num_max_tokens_per_rank, hidden, x_element_size, num_sf_packs,
        recv_sf_token_stride, recv_sf_hidden_stride, num_experts, num_topk,
        num_sms, num_smem_bytes, use_hybrid ? hybrid_channels : num_channels,
        do_expand, cached_mode, launch_ctx,
        psum_num_recv_tokens_per_scaleup_rank,
        epilogue_psum_num_recv_tokens_per_expert, launch_stream);

    (void)active_ranks;
    (void)num_local_experts;
    (void)hybrid_max_tokens_per_channel;
    if (!async_with_compute_stream) {
        stream_wait(compute_stream_raw, launch_stream);
        return std::nullopt;
    }
    return EventHandle(reinterpret_cast<uint64_t>(launch_stream),
                       deterministic_rank_count_buffer);
}

std::optional<EventHandle> MooncakeElasticBuffer::combine(
    uint64_t x_ptr, int num_input_tokens, int hidden, uint64_t topk_idx_ptr,
    int num_combined_tokens, int num_topk, uint64_t topk_weights_ptr,
    uint64_t psum_num_recv_tokens_per_scaleup_rank_ptr,
    uint64_t recv_src_metadata_ptr, uint64_t token_metadata_at_forward_ptr,
    uint64_t channel_linked_list_ptr, uint64_t active_ranks_ptr,
    int num_experts, int num_max_tokens_per_rank, bool do_expand, int num_sms,
    bool async_with_compute_stream, uint64_t compute_stream_ptr,
    uint64_t combined_x_ptr) {
    EP_HOST_ASSERT(x_ptr != 0 && topk_idx_ptr != 0 && topk_weights_ptr != 0);
    EP_HOST_ASSERT(psum_num_recv_tokens_per_scaleup_rank_ptr != 0);
    EP_HOST_ASSERT(recv_src_metadata_ptr != 0 && active_ranks_ptr != 0);
    EP_HOST_ASSERT(combined_x_ptr != 0);
    void* x = reinterpret_cast<void*>(x_ptr);
    auto* topk_idx = reinterpret_cast<int64_t*>(topk_idx_ptr);
    auto* topk_weights = reinterpret_cast<float*>(topk_weights_ptr);
    auto* psum_num_recv_tokens_per_scaleup_rank =
        reinterpret_cast<int*>(psum_num_recv_tokens_per_scaleup_rank_ptr);
    auto* recv_src_metadata = reinterpret_cast<int*>(recv_src_metadata_ptr);
    auto* token_metadata_at_forward =
        reinterpret_cast<int*>(token_metadata_at_forward_ptr);
    auto* channel_linked_list = reinterpret_cast<int*>(channel_linked_list_ptr);
    auto* active_ranks = reinterpret_cast<int*>(active_ranks_ptr);
    void* combined_x = reinterpret_cast<void*>(combined_x_ptr);

    const int num_smem_bytes = device_smem_bytes();
    const int num_channels = std::max(1, num_sms);
    const bool use_hybrid = topology_.num_scaleout_ranks != 1;
    const int hybrid_channels = use_hybrid ? hybrid_num_channels(num_sms) : 0;
    auto compute_stream_raw =
        reinterpret_cast<cudaStream_t>(compute_stream_ptr);
    auto launch_stream = native_buffer_->comm_stream;
    stream_wait(launch_stream, compute_stream_raw);
    const int64_t timeout_cycles =
        config_.num_gpu_timeout_secs < 0
            ? -1
            : static_cast<int64_t>(native_buffer_->clock_rate_khz) *
                  static_cast<int64_t>(config_.num_gpu_timeout_secs) * 1000;
    auto launch_ctx = make_launch_context(
        *native_buffer_, topology_, mapped_host_workspace_, timeout_cycles);
    void* reduce_buffer = launch_mooncake_elastic_combine(
        x, topk_weights, recv_src_metadata,
        psum_num_recv_tokens_per_scaleup_rank, token_metadata_at_forward,
        channel_linked_list, num_input_tokens, num_max_tokens_per_rank, hidden,
        num_experts, num_topk, num_sms, num_smem_bytes,
        use_hybrid ? hybrid_channels : num_channels, do_expand,
        config_.allow_multiple_reduction, launch_ctx, launch_stream);

    launch_mooncake_elastic_combine_reduce_epilogue(
        combined_x, topk_weights, topk_idx, num_combined_tokens,
        num_max_tokens_per_rank, hidden, num_experts, num_topk, reduce_buffer,
        nullptr, nullptr, num_sms, num_smem_bytes, do_expand,
        config_.allow_multiple_reduction, launch_ctx, launch_stream);

    (void)active_ranks;
    if (!async_with_compute_stream) {
        stream_wait(compute_stream_raw, launch_stream);
        return std::nullopt;
    }
    return EventHandle(reinterpret_cast<uint64_t>(launch_stream));
}

ElasticTopology MooncakeElasticBuffer::discover_topology(
    int rank, int num_ranks, bool allow_hybrid_mode) {
    int device_count = 1;
    cudaGetDeviceCount(&device_count);
    int num_local_ranks =
        getenv_int("MOONCAKE_EP_NUM_LOCAL_RANKS",
                   std::max(1, std::min(num_ranks, device_count)));
    num_local_ranks = std::max(1, std::min(num_local_ranks, num_ranks));

    ElasticTopology topology;
    topology.rank_idx = rank;
    topology.num_ranks = num_ranks;
    topology.num_rdma_ranks =
        static_cast<int>(ceil_div_i64(num_ranks, num_local_ranks));
    topology.num_nvlink_ranks = num_local_ranks;
    if (allow_hybrid_mode && topology.num_rdma_ranks > 1) {
        topology.num_scaleout_ranks = topology.num_rdma_ranks;
        topology.num_scaleup_ranks = topology.num_nvlink_ranks;
        topology.hybrid_enabled = true;
    } else {
        topology.num_scaleout_ranks = 1;
        topology.num_scaleup_ranks = num_ranks;
        topology.hybrid_enabled = false;
    }
    topology.scaleout_rank_idx = rank / topology.num_scaleup_ranks;
    topology.scaleup_rank_idx = rank % topology.num_scaleup_ranks;
    return topology;
}

}  // namespace mooncake
