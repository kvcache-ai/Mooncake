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
    int device = 0;
    cudaGetDevice(&device);
    int value = 0;
    cudaDeviceGetAttribute(&value, cudaDevAttrMaxSharedMemoryPerBlockOptin,
                           device);
    return value > 0 ? value : 98304;
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

ElasticDispatchOutput MooncakeElasticBuffer::dispatch(
    const torch::Tensor& x, const std::optional<torch::Tensor>& sf,
    const torch::Tensor& topk_idx,
    const std::optional<torch::Tensor>& topk_weights,
    torch::Tensor& active_ranks, int num_experts, int num_max_tokens_per_rank,
    int expert_alignment, int num_sms, bool do_expand, bool do_cpu_sync,
    bool async_with_compute_stream,
    const std::optional<ElasticNativeHandle>& cached_handle) {
    EP_HOST_ASSERT(x.dim() == 2 && x.is_contiguous());
    const bool use_sf = sf.has_value();
    if (use_sf) {
        EP_HOST_ASSERT(x.element_size() == 1);
        EP_HOST_ASSERT(sf->dim() == 2 && sf->is_cuda());
        EP_HOST_ASSERT(sf->scalar_type() == torch::kFloat32 ||
                       sf->scalar_type() == torch::kInt32);
        EP_HOST_ASSERT(sf->size(0) == x.size(0));
    } else {
        EP_HOST_ASSERT(!config_.use_fp8_dispatch);
        EP_HOST_ASSERT(x.scalar_type() == torch::kBFloat16);
    }
    EP_HOST_ASSERT(topk_idx.dim() == 2 && topk_idx.is_contiguous());
    EP_HOST_ASSERT(topk_idx.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(x.size(0) == topk_idx.size(0));
    EP_HOST_ASSERT(num_experts % topology_.num_ranks == 0);

    const int num_tokens = static_cast<int>(x.size(0));
    const int hidden = static_cast<int>(x.size(1));
    const int num_topk = static_cast<int>(topk_idx.size(1));
    const int num_sf_packs = use_sf ? static_cast<int>(sf->size(1)) : 0;
    const int sf_token_stride = use_sf ? static_cast<int>(sf->stride(0)) : 0;
    const int sf_hidden_stride = use_sf ? static_cast<int>(sf->stride(1)) : 0;
    const int num_local_experts = num_experts / topology_.num_ranks;
    // The copy epilogue uses `kNumMaxTokensPerRank * kNumRanks` as the
    // no-CPU-sync sentinel and then reads the real local receive count from the
    // GPU prefix-sum tensor.  In hybrid mode each scale-up peer may receive
    // tokens forwarded from every scale-out rank, so the conservative output
    // capacity and sentinel must cover the full logical world, not just the
    // intra-node scale-up domain.
    const int num_recv_tokens = num_max_tokens_per_rank * topology_.num_ranks;
    const int num_smem_bytes = device_smem_bytes();
    const int num_channels_per_sm = 1;
    const int num_channels = num_sms * num_channels_per_sm;
    const bool cached_mode = cached_handle.has_value();
    const bool use_hybrid = topology_.num_scaleout_ranks != 1;
    const int hybrid_channels = use_hybrid ? hybrid_num_channels(num_sms) : 0;
    const int hybrid_max_tokens_per_channel =
        use_hybrid ? hybrid_num_max_tokens_per_channel(num_max_tokens_per_rank,
                                                       num_sms)
                   : 0;
    if (cached_mode) {
        const auto& handle = cached_handle.value();
        EP_HOST_ASSERT(!handle.do_expand && !do_expand);
        EP_HOST_ASSERT(handle.num_experts == num_experts);
        EP_HOST_ASSERT(handle.expert_alignment == expert_alignment);
        EP_HOST_ASSERT(handle.num_max_tokens_per_rank ==
                       num_max_tokens_per_rank);
        EP_HOST_ASSERT(handle.num_sms == num_sms);
        if (use_hybrid) {
            EP_HOST_ASSERT(handle.dst_buffer_slot_idx.dim() == 4);
            EP_HOST_ASSERT(handle.dst_buffer_slot_idx.size(0) ==
                           hybrid_channels);
            EP_HOST_ASSERT(handle.dst_buffer_slot_idx.size(1) ==
                           topology_.num_scaleout_ranks);
            EP_HOST_ASSERT(handle.dst_buffer_slot_idx.size(2) ==
                           hybrid_max_tokens_per_channel);
            EP_HOST_ASSERT(handle.dst_buffer_slot_idx.size(3) == num_topk);
            EP_HOST_ASSERT(handle.token_metadata_at_forward.has_value());
            EP_HOST_ASSERT(handle.channel_linked_list.has_value());
        } else {
            EP_HOST_ASSERT(handle.dst_buffer_slot_idx.dim() == 2);
            EP_HOST_ASSERT(handle.dst_buffer_slot_idx.size(0) == num_tokens);
            EP_HOST_ASSERT(handle.dst_buffer_slot_idx.size(1) == num_topk);
        }
    }

    auto compute_stream = at::cuda::getCurrentCUDAStream();
    auto launch_stream = native_buffer_->comm_stream;
    stream_wait(launch_stream, compute_stream);

    const int64_t timeout_cycles =
        config_.num_gpu_timeout_secs < 0
            ? -1
            : static_cast<int64_t>(native_buffer_->clock_rate_khz) *
                  static_cast<int64_t>(config_.num_gpu_timeout_secs) * 1000;
    auto launch_ctx = make_launch_context(
        *native_buffer_, topology_, mapped_host_workspace_, timeout_cycles);

    auto psum_num_recv_tokens_per_scaleup_rank =
        cached_mode ? cached_handle->psum_num_recv_tokens_per_scaleup_rank
                    : torch::empty({topology_.num_scaleup_ranks},
                                   torch::TensorOptions()
                                       .dtype(torch::kInt32)
                                       .device(x.device()));
    auto psum_num_recv_tokens_per_expert =
        cached_mode
            ? cached_handle->psum_num_recv_tokens_per_expert
            : torch::empty({num_local_experts + 1}, torch::TensorOptions()
                                                        .dtype(torch::kInt32)
                                                        .device(x.device()));
    auto dst_buffer_slot_idx =
        cached_mode
            ? cached_handle->dst_buffer_slot_idx
            : (use_hybrid ? torch::empty(
                                {hybrid_channels, topology_.num_scaleout_ranks,
                                 hybrid_max_tokens_per_channel, num_topk},
                                torch::TensorOptions()
                                    .dtype(torch::kInt32)
                                    .device(x.device()))
                          : torch::empty({num_tokens, num_topk},
                                         torch::TensorOptions()
                                             .dtype(torch::kInt32)
                                             .device(x.device())));
    std::optional<torch::Tensor> token_metadata_at_forward = std::nullopt;
    std::optional<torch::Tensor> channel_linked_list = std::nullopt;
    if (use_hybrid) {
        if (cached_mode) {
            token_metadata_at_forward =
                cached_handle->token_metadata_at_forward;
            channel_linked_list = cached_handle->channel_linked_list;
        } else {
            const int forward_metadata_dims = 2 + num_topk * 2;
            token_metadata_at_forward = torch::empty(
                {hybrid_channels,
                 topology_.num_scaleout_ranks * hybrid_max_tokens_per_channel +
                     1,
                 forward_metadata_dims},
                torch::TensorOptions().dtype(torch::kInt32).device(x.device()));
            channel_linked_list = torch::empty(
                {hybrid_channels,
                 topology_.num_scaleout_ranks * hybrid_max_tokens_per_channel +
                     1,
                 topology_.num_scaleup_ranks},
                torch::TensorOptions().dtype(torch::kInt32).device(x.device()));
        }
    }
    std::optional<torch::Tensor> deterministic_rank_count_buffer = std::nullopt;
    if (config_.deterministic && !cached_mode && !use_hybrid) {
        deterministic_rank_count_buffer = torch::empty(
            {num_sms, topology_.num_scaleup_ranks},
            torch::TensorOptions().dtype(torch::kInt32).device(x.device()));
        launch_elastic_dispatch_deterministic_prologue(
            topk_idx, deterministic_rank_count_buffer.value(),
            dst_buffer_slot_idx, num_tokens, num_max_tokens_per_rank,
            num_experts, num_topk, topology_.scaleup_rank_idx,
            topology_.num_scaleup_ranks, num_sms, num_smem_bytes,
            launch_stream.stream());
    }

    launch_mooncake_elastic_dispatch(
        x.data_ptr(), use_sf ? const_cast<void*>(sf->data_ptr()) : nullptr,
        const_cast<int64_t*>(topk_idx.data_ptr<int64_t>()),
        topk_weights.has_value()
            ? const_cast<float*>(topk_weights->data_ptr<float>())
            : nullptr,
        nullptr, nullptr, psum_num_recv_tokens_per_scaleup_rank.data_ptr<int>(),
        psum_num_recv_tokens_per_expert.data_ptr<int>(),
        dst_buffer_slot_idx.data_ptr<int>(),
        token_metadata_at_forward.has_value()
            ? token_metadata_at_forward->data_ptr<int>()
            : nullptr,
        num_tokens, num_max_tokens_per_rank, hidden,
        static_cast<int>(x.element_size()), num_sf_packs, sf_token_stride,
        sf_hidden_stride, num_experts, num_topk, expert_alignment, num_sms,
        use_hybrid ? kElasticHybridChannelsPerSm : num_channels_per_sm,
        num_smem_bytes, cached_mode, config_.deterministic, false, launch_ctx,
        launch_stream.stream());

    const int num_recv_output_capacity =
        do_expand ? num_recv_tokens * num_topk : num_recv_tokens;
    auto recv_x = torch::empty({num_recv_output_capacity, hidden}, x.options());
    auto recv_x_scales = std::optional<torch::Tensor>();
    void* recv_x_scales_ptr = nullptr;
    int recv_sf_token_stride = 0;
    int recv_sf_hidden_stride = 0;
    if (use_sf) {
        recv_x_scales = torch::empty({num_recv_output_capacity, num_sf_packs},
                                     sf->options());
        recv_x_scales_ptr = recv_x_scales->data_ptr();
        recv_sf_token_stride = static_cast<int>(recv_x_scales->stride(0));
        recv_sf_hidden_stride = static_cast<int>(recv_x_scales->stride(1));
    }
    auto recv_topk_idx =
        torch::empty({num_recv_tokens, num_topk}, topk_idx.options());
    auto recv_topk_weights = std::optional<torch::Tensor>();
    float* recv_topk_weights_ptr = nullptr;
    if (topk_weights.has_value()) {
        recv_topk_weights = do_expand
                                ? torch::empty({num_recv_output_capacity},
                                               topk_weights->options())
                                : torch::empty({num_recv_tokens, num_topk},
                                               topk_weights->options());
        recv_topk_weights_ptr = recv_topk_weights->data_ptr<float>();
    }
    auto recv_src_metadata = torch::empty(
        {num_recv_tokens, num_topk + 2},
        torch::TensorOptions().dtype(torch::kInt32).device(x.device()));
    auto handle_psum_num_recv_tokens_per_expert =
        do_expand
            ? psum_num_recv_tokens_per_expert.slice(0, 0, num_local_experts)
            : psum_num_recv_tokens_per_expert.slice(0, 1,
                                                    num_local_experts + 1);
    auto epilogue_psum_num_recv_tokens_per_expert =
        do_expand ? psum_num_recv_tokens_per_expert
                  : handle_psum_num_recv_tokens_per_expert;

    launch_mooncake_elastic_dispatch_copy_epilogue(
        recv_x.data_ptr(), recv_x_scales_ptr, recv_topk_idx.data_ptr<int64_t>(),
        recv_topk_weights_ptr, recv_src_metadata.data_ptr<int>(),
        channel_linked_list.has_value() ? channel_linked_list->data_ptr<int>()
                                        : nullptr,
        num_recv_tokens, num_max_tokens_per_rank, hidden,
        static_cast<int>(x.element_size()), num_sf_packs, recv_sf_token_stride,
        recv_sf_hidden_stride, num_experts, num_topk, num_sms, num_smem_bytes,
        use_hybrid ? hybrid_channels : num_channels, do_expand, cached_mode,
        launch_ctx, psum_num_recv_tokens_per_scaleup_rank.data_ptr<int>(),
        epilogue_psum_num_recv_tokens_per_expert.data_ptr<int>(),
        launch_stream.stream());

    if (do_cpu_sync || !async_with_compute_stream) {
        stream_wait(compute_stream, launch_stream);
    }
    std::optional<EventHandle> event = std::nullopt;
    if (async_with_compute_stream) {
        event = EventHandle(launch_stream);
    }

    std::vector<int> num_recv_tokens_per_expert_list;
    int actual_num_recv_tokens = num_recv_tokens;
    int actual_num_output_tokens = num_recv_tokens;
    if (do_cpu_sync) {
        auto scaleup_psum_cpu = psum_num_recv_tokens_per_scaleup_rank.cpu();
        auto expert_psum_cpu = psum_num_recv_tokens_per_expert.cpu();
        const auto* scaleup_psum = scaleup_psum_cpu.data_ptr<int>();
        const auto* expert_psum = expert_psum_cpu.data_ptr<int>();
        actual_num_recv_tokens = scaleup_psum[topology_.num_scaleup_ranks - 1];
        EP_HOST_ASSERT(actual_num_recv_tokens >= 0 &&
                       actual_num_recv_tokens <= num_recv_tokens);
        actual_num_output_tokens = actual_num_recv_tokens;

        num_recv_tokens_per_expert_list.reserve(num_local_experts);
        const auto align_count = [expert_alignment](int value) {
            return ((value + expert_alignment - 1) / expert_alignment) *
                   expert_alignment;
        };
        if (do_expand) {
            int previous_psum = 0;
            for (int i = 0; i < num_local_experts; ++i) {
                const int count = expert_psum[i] - align_count(previous_psum);
                EP_HOST_ASSERT(count >= 0);
                num_recv_tokens_per_expert_list.push_back(count);
                previous_psum = expert_psum[i];
            }
            actual_num_output_tokens =
                num_local_experts == 0 ? 0 : expert_psum[num_local_experts - 1];
        } else {
            for (int i = 0; i < num_local_experts; ++i) {
                const int count = expert_psum[i + 1] - expert_psum[i];
                EP_HOST_ASSERT(count >= 0);
                num_recv_tokens_per_expert_list.push_back(count);
            }
        }
        EP_HOST_ASSERT(actual_num_output_tokens >= 0 &&
                       actual_num_output_tokens <= recv_x.size(0));

        recv_x = recv_x.slice(0, 0, actual_num_output_tokens);
        if (recv_x_scales.has_value()) {
            recv_x_scales =
                recv_x_scales->slice(0, 0, actual_num_output_tokens);
        }
        recv_topk_idx = recv_topk_idx.slice(0, 0, actual_num_recv_tokens);
        if (recv_topk_weights.has_value()) {
            recv_topk_weights =
                recv_topk_weights->slice(0, 0, actual_num_output_tokens);
        }
        recv_src_metadata =
            recv_src_metadata.slice(0, 0, actual_num_recv_tokens);
    }

    ElasticNativeHandle handle;
    handle.do_expand = do_expand;
    handle.num_experts = num_experts;
    handle.expert_alignment = expert_alignment;
    handle.num_max_tokens_per_rank = num_max_tokens_per_rank;
    handle.num_sms = num_sms;
    handle.topk_idx = cached_mode ? cached_handle->topk_idx : topk_idx.clone();
    handle.psum_num_recv_tokens_per_expert =
        handle_psum_num_recv_tokens_per_expert;
    handle.psum_num_recv_tokens_per_scaleup_rank =
        psum_num_recv_tokens_per_scaleup_rank;
    handle.recv_src_metadata = recv_src_metadata;
    handle.recv_layout_range = torch::empty(
        {0}, torch::TensorOptions().dtype(torch::kInt64).device(x.device()));
    handle.dst_buffer_slot_idx = dst_buffer_slot_idx;
    handle.token_metadata_at_forward = token_metadata_at_forward;
    handle.channel_linked_list = channel_linked_list;
    handle.num_recv_tokens_per_expert_list = num_recv_tokens_per_expert_list;

    ElasticDispatchOutput output;
    output.recv_x = recv_x;
    output.recv_x_scales = recv_x_scales;
    output.recv_topk_idx = recv_topk_idx;
    output.recv_topk_weights = recv_topk_weights;
    output.handle = handle;
    output.event = event;
    return output;
}

ElasticCombineOutput MooncakeElasticBuffer::combine(
    const torch::Tensor& x, const ElasticNativeHandle& handle,
    const std::optional<torch::Tensor>& topk_weights,
    torch::Tensor& active_ranks, int num_sms, bool async_with_compute_stream,
    const std::optional<torch::Tensor>& out) {
    EP_HOST_ASSERT(x.dim() == 2 && x.is_contiguous());
    EP_HOST_ASSERT(x.scalar_type() == torch::kBFloat16);
    torch::Tensor weights = topk_weights.value_or(torch::Tensor());
    if (!weights.defined()) {
        weights = torch::ones(
            handle.topk_idx.sizes(),
            torch::TensorOptions().dtype(torch::kFloat32).device(x.device()));
    }
    const int hidden = static_cast<int>(x.size(1));
    const int num_topk = static_cast<int>(handle.topk_idx.size(1));
    const int num_combined_tokens = static_cast<int>(handle.topk_idx.size(0));
    const int num_smem_bytes = device_smem_bytes();
    const int num_channels = std::max(1, num_sms);
    const bool use_hybrid = topology_.num_scaleout_ranks != 1;
    const int hybrid_channels = use_hybrid ? hybrid_num_channels(num_sms) : 0;
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    auto launch_stream = native_buffer_->comm_stream;
    stream_wait(launch_stream, compute_stream);
    const int64_t timeout_cycles =
        config_.num_gpu_timeout_secs < 0
            ? -1
            : static_cast<int64_t>(native_buffer_->clock_rate_khz) *
                  static_cast<int64_t>(config_.num_gpu_timeout_secs) * 1000;
    auto launch_ctx = make_launch_context(
        *native_buffer_, topology_, mapped_host_workspace_, timeout_cycles);
    auto psum_num_recv_tokens_per_scaleup_rank =
        handle.psum_num_recv_tokens_per_scaleup_rank;
    void* reduce_buffer = launch_mooncake_elastic_combine(
        x.data_ptr(), weights.data_ptr<float>(),
        const_cast<int*>(handle.recv_src_metadata.data_ptr<int>()),
        psum_num_recv_tokens_per_scaleup_rank.data_ptr<int>(),
        handle.token_metadata_at_forward.has_value()
            ? handle.token_metadata_at_forward->data_ptr<int>()
            : nullptr,
        handle.channel_linked_list.has_value()
            ? handle.channel_linked_list->data_ptr<int>()
            : nullptr,
        static_cast<int>(x.size(0)), handle.num_max_tokens_per_rank, hidden,
        handle.num_experts, num_topk, num_sms, num_smem_bytes,
        use_hybrid ? hybrid_channels : num_channels, handle.do_expand,
        config_.allow_multiple_reduction, launch_ctx, launch_stream.stream());

    torch::Tensor combined_x =
        out.has_value()
            ? out.value()
            : torch::empty({num_combined_tokens, hidden}, x.options());
    launch_mooncake_elastic_combine_reduce_epilogue(
        combined_x.data_ptr(), weights.data_ptr<float>(),
        const_cast<int64_t*>(handle.topk_idx.data_ptr<int64_t>()),
        num_combined_tokens, handle.num_max_tokens_per_rank, hidden,
        handle.num_experts, num_topk, reduce_buffer, nullptr, nullptr, num_sms,
        num_smem_bytes, handle.do_expand, config_.allow_multiple_reduction,
        launch_ctx, launch_stream.stream());

    if (!async_with_compute_stream) {
        stream_wait(compute_stream, launch_stream);
    }
    std::optional<EventHandle> event = std::nullopt;
    if (async_with_compute_stream) event = EventHandle(launch_stream);
    (void)active_ranks;

    ElasticCombineOutput output;
    output.combined_x = combined_x;
    output.combined_topk_weights = std::nullopt;
    output.event = event;
    return output;
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
