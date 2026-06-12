#include <mooncake_ep_buffer.h>
#include <glog/logging.h>
#include <sstream>
#include <mooncake_ep_layout.cuh>
#include <transfer_engine.h>

namespace mooncake {

// Initialize an RDMA transport: register memory, allocate control buffer,
// create QPs.  Returns true on success, false if IBGDA is unavailable.
static bool initRdmaTransport(device::RdmaTransport* t, void* gdr_buffer,
                              int64_t num_ep_buffer_bytes, int num_ranks,
                              int use_qp_count, void* stream) {
    int ret = t->initialize("", num_ranks, use_qp_count);
    if (ret == 0) {
        ret = t->registerMemory(gdr_buffer, num_ep_buffer_bytes);
    }
    if (ret == 0) {
        ret = t->allocateControlBuffer();
    }
    if (ret == 0) {
        ret = t->createQueuePairs(stream);
    }
    return ret == 0;
}

MooncakeEpBuffer::MooncakeEpBuffer(int rank, int num_ranks,
                                   int64_t num_ep_buffer_bytes,
                                   TransferEngine* engine)
    : rank(rank),
      num_ranks(num_ranks),
      num_ep_buffer_bytes(num_ep_buffer_bytes),
      comm_stream(at::cuda::getStreamFromPool(true)) {
    USE_QP_COUNT = MAX_QP_COUNT / num_ranks * num_ranks;
    // Get ranks
    CUDA_CHECK(cudaGetDevice(&device_id));
    CUDA_CHECK(cudaDeviceGetAttribute(&clock_rate_khz, cudaDevAttrClockRate,
                                      device_id));

    // P2P transport — owns GDR buffer allocation and IPC handle exchange.
    if (engine) {
        p2p_transport_ = engine->getOrCreateP2pTransport(num_ranks);
    } else {
        owned_p2p_transport_ = device::createP2pDeviceTransport(num_ranks);
        p2p_transport_ = owned_p2p_transport_.get();
    }

    gdr_buffer = p2p_transport_->allocateBuffer(num_ep_buffer_bytes);
    if (!gdr_buffer) {
        throw std::runtime_error("[EP] Failed to allocate GDR buffer");
    }
    CUDA_CHECK(cudaMemset(gdr_buffer, 0, num_ep_buffer_bytes));

    // RDMA transport — optional; disabled if init fails.
    if (engine) {
        rdma_transport_ = engine->getOrCreateRdmaTransport();
        if (rdma_transport_) {
            if (!initRdmaTransport(rdma_transport_, gdr_buffer,
                                   num_ep_buffer_bytes, num_ranks, USE_QP_COUNT,
                                   comm_stream.stream())) {
                rdma_transport_ = nullptr;
                ibgda_disabled_ = true;
                LOG(INFO) << "[EP] IBGDA unavailable, using P2P-only path";
            }
        } else {
            ibgda_disabled_ = true;
        }
    } else {
        // Allow explicit IBGDA disable via env var (P2P-only mode).
        if (std::getenv("MOONCAKE_EP_DISABLE_IBGDA")) {
            ibgda_disabled_ = true;
            LOG(INFO) << "[EP] IBGDA disabled by MOONCAKE_EP_DISABLE_IBGDA, "
                         "using P2P-only path";
        } else {
            // Read optional NIC whitelist from env var (same convention as PG
            // tests).
            std::vector<std::string> device_filter;
            if (const char* env = std::getenv("MOONCAKE_EP_DEVICE_FILTER")) {
                std::string s(env);
                std::istringstream ss(s);
                std::string tok;
                while (std::getline(ss, tok, ',')) {
                    if (!tok.empty()) device_filter.push_back(tok);
                }
            }
            auto t = device::createIbgdaDeviceTransport(device_filter);
            if (initRdmaTransport(t.get(), gdr_buffer, num_ep_buffer_bytes,
                                  num_ranks, USE_QP_COUNT,
                                  comm_stream.stream())) {
                owned_rdma_transport_ = std::move(t);
                rdma_transport_ = owned_rdma_transport_.get();
            } else {
                ibgda_disabled_ = true;
                LOG(INFO) << "[EP] IBGDA unavailable, using P2P-only path";
            }
        }
    }

    // Create 32 MiB workspace
    CUDA_CHECK(cudaMalloc(&workspace, NUM_WORKSPACE_BYTES));
    CUDA_CHECK(cudaMemsetAsync(workspace, 0, NUM_WORKSPACE_BYTES, comm_stream));

    // Elastic workspace (DeepEP V2 port) — must be inside GDR buffer
    // because the dispatch kernel uses mc_route_put/mc_signal which
    // compute peer pointers relative to ctx.local_base (GDR buffer base).
    auto elastic_bytes = layout::WorkspaceLayout::get_num_bytes();
    elastic_workspace = static_cast<char*>(gdr_buffer) +
                        num_ep_buffer_bytes - elastic_bytes;
    CUDA_CHECK(
        cudaMemsetAsync(elastic_workspace, 0, elastic_bytes, comm_stream));

    // Feature flags
    if (const char* env = std::getenv("MC_EP_ELASTIC_DISPATCH")) {
        use_elastic_dispatch_ = (std::string(env) == "1");
    }
    if (const char* env = std::getenv("MC_EP_ELASTIC_COMBINE")) {
        use_elastic_combine_ = (std::string(env) == "1");
    }
}

MooncakeEpBuffer::~MooncakeEpBuffer() noexcept(false) {
    // When EP owns the rdma transport, destructor handles QP/MR/ctrl_buf
    // teardown. When engine owns it, just clear the pointer.
    owned_rdma_transport_.reset();
    rdma_transport_ = nullptr;

    // When EP owns the p2p transport, free the buffer and reset.
    if (owned_p2p_transport_) {
        if (gdr_buffer) {
            owned_p2p_transport_->freeBuffer(gdr_buffer);
            gdr_buffer = nullptr;
        }
        owned_p2p_transport_.reset();
    } else {
        // Engine-owned: just free the buffer via the engine's transport.
        if (gdr_buffer && p2p_transport_) {
            p2p_transport_->freeBuffer(gdr_buffer);
            gdr_buffer = nullptr;
        }
    }
    p2p_transport_ = nullptr;

    if (workspace) cudaFree(workspace);
    // elastic_workspace is inside gdr_buffer, freed with the buffer
}

std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor,
           torch::Tensor, torch::Tensor, std::optional<EventHandle>,
           std::optional<std::function<void()>>>
MooncakeEpBuffer::dispatch(const torch::Tensor& x,
                           const torch::Tensor& topk_idx,
                           torch::Tensor& active_ranks,
                           int num_max_dispatch_tokens_per_rank,
                           int num_experts, int timeout_us, bool use_fp8,
                           bool async, bool return_recv_hook) {
    // Tensor checks
    // By default using `ptp128c` FP8 cast
    EP_HOST_ASSERT(x.dim() == 2 and x.is_contiguous() and
                   x.scalar_type() == torch::kBFloat16);
    EP_HOST_ASSERT(x.size(1) % sizeof(int4) == 0 and x.size(1) % 128 == 0);
    EP_HOST_ASSERT(topk_idx.dim() == 2 and topk_idx.is_contiguous());
    EP_HOST_ASSERT(x.size(0) == topk_idx.size(0) and
                   x.size(0) <= num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(topk_idx.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(num_experts % num_ranks == 0);
    EP_HOST_ASSERT(USE_QP_COUNT % num_ranks == 0);

    auto num_tokens = static_cast<int>(x.size(0)),
         hidden = static_cast<int>(x.size(1));
    auto num_scales = hidden / 128,
         num_topk = static_cast<int>(topk_idx.size(1));
    int num_local_experts = num_experts / num_ranks;

    // Buffer control
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_ep_buffer_bytes);
    auto buffer = layout.buffers[buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) {
        stream_wait(launch_stream, compute_stream);
    } else if (use_elastic_dispatch_) {
        // Elastic dispatch is eager even in hook mode, but hook mode launches on
        // the caller stream.  The constructor initializes the peer-visible GDR
        // workspace on comm_stream, so order the caller stream after it before
        // the first cooperative barrier touches elastic_workspace.
        stream_wait(launch_stream, comm_stream);
    }

    // Allocate packed tensors
    auto packed_recv_x = torch::empty(
        {num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank,
         hidden},
        x.options().dtype(use_fp8 ? torch::kFloat8_e4m3fn : torch::kBFloat16));
    auto packed_recv_src_info = torch::full(
        {num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank * 2},
        -1,
        torch::dtype(torch::kInt32).device(x.device()));
    auto packed_recv_layout_range =
        torch::empty({num_local_experts, num_ranks},
                     torch::dtype(torch::kInt64).device(x.device()));
    auto packed_recv_count = torch::zeros(
        {num_local_experts}, torch::dtype(torch::kInt32).device(x.device()));

    // Allocate column-majored scales
    auto packed_recv_x_scales = std::optional<torch::Tensor>();
    float* packed_recv_x_scales_ptr = nullptr;
    if (use_fp8) {
        EP_HOST_ASSERT((num_ranks * num_max_dispatch_tokens_per_rank) % 4 ==
                           0 and
                       "TMA requires the number of tokens to be multiple of 4");
        packed_recv_x_scales =
            torch::empty({num_local_experts, num_scales,
                          num_ranks * num_max_dispatch_tokens_per_rank},
                         torch::dtype(torch::kFloat32).device(x.device()));
        packed_recv_x_scales =
            torch::transpose(packed_recv_x_scales.value(), 1, 2);
        packed_recv_x_scales_ptr = packed_recv_x_scales->data_ptr<float>();
    }

    int64_t timeout_ticks =
        timeout_us == -1 ? -1
                         : (int64_t)clock_rate_khz * (int64_t)timeout_us / 1000;

    void* raddrs_ptr = rdma_transport_ ? rdma_transport_->raddrsPtr() : nullptr;
    void* rkeys_ptr = rdma_transport_ ? rdma_transport_->rkeysPtr() : nullptr;
    void* qp_devctxs_ptr =
        rdma_transport_ ? rdma_transport_->qpDevCtxsPtr() : nullptr;
    int32_t* nvlink_avail = p2p_transport_->availableTablePtr();
    void** ipc_ptrs = p2p_transport_->peerPtrsTablePtr();

    auto launcher = [=](int phases) {
        mooncake::dispatch(
            packed_recv_x.data_ptr(), packed_recv_x_scales_ptr,
            packed_recv_src_info.data_ptr<int>(),
            packed_recv_layout_range.data_ptr<int64_t>(),
            packed_recv_count.data_ptr<int>(), active_ranks.data_ptr<int32_t>(),
            gdr_buffer, buffer.rdma_send_signal_buffer,
            buffer.rdma_recv_signal_buffer, buffer.rdma_send_data_buffer,
            buffer.rdma_recv_data_buffer, nullptr, nullptr, raddrs_ptr,
            rkeys_ptr, qp_devctxs_ptr, nvlink_avail, ipc_ptrs, x.data_ptr(),
            topk_idx.data_ptr<int64_t>(), next_buffer.rdma_recv_signal_buffer,
            num_tokens, hidden, num_max_dispatch_tokens_per_rank, num_topk,
            num_experts, rank, num_ranks, use_fp8, workspace, launch_stream,
            timeout_ticks, phases);
    };

    // Elastic dispatch path (DeepEP V2 port)
    if (use_elastic_dispatch_) {
        // Allocate slot index buffer
        auto dst_buffer_slot_idx = torch::empty(
            {num_tokens, num_topk},
            torch::dtype(torch::kInt32).device(x.device()));
        auto psum_num_recv_tokens_per_rank = torch::zeros(
            {num_ranks}, torch::dtype(torch::kInt32).device(x.device()));
        auto psum_num_recv_tokens_per_expert = torch::zeros(
            {num_local_experts},
            torch::dtype(torch::kInt32).device(x.device()));
        const int num_elastic_sms = cell_div(num_experts, 8);
        auto prologue_rank_count_buffer = torch::empty(
            {num_elastic_sms, num_ranks},
            torch::dtype(torch::kInt32).device(x.device()));

        // Run deterministic prologue to pre-compute slot indices
        mooncake::dispatch_deterministic_prologue(
            topk_idx.data_ptr<int64_t>(),
            dst_buffer_slot_idx.data_ptr<int>(),
            num_tokens, hidden, num_topk, num_experts, rank, num_ranks,
            num_max_dispatch_tokens_per_rank,
            prologue_rank_count_buffer.data_ptr<int>(),
            launch_stream);

        // Run elastic dispatch kernel
        mooncake::launch_elastic_dispatch(
            const_cast<void*>(x.data_ptr()),
            topk_idx.data_ptr<int64_t>(),
            nullptr,  // topk_weights (not needed for dispatch)
            nullptr,  // copied_topk_idx
            nullptr,  // cumulative_local_expert_recv_stats
            psum_num_recv_tokens_per_rank.data_ptr<int>(),
            psum_num_recv_tokens_per_expert.data_ptr<int>(),
            dst_buffer_slot_idx.data_ptr<int>(),
            num_tokens, hidden, num_topk, num_experts, rank, num_ranks,
            num_max_dispatch_tokens_per_rank,
            true,  // reuse_slot_indices
            gdr_buffer, raddrs_ptr, rkeys_ptr, qp_devctxs_ptr,
            nvlink_avail, ipc_ptrs,
            buffer.rdma_send_signal_buffer, buffer.rdma_recv_signal_buffer,
            buffer.rdma_send_data_buffer, buffer.rdma_recv_data_buffer,
            gdr_buffer,  // buffer (communication buffer)
            elastic_workspace, launch_stream, timeout_ticks);

        // Sync to catch elastic dispatch errors before epilogue
        CUDA_CHECK(cudaStreamSynchronize(launch_stream));

        // Run copy epilogue to unpack received tokens
        mooncake::dispatch_copy_epilogue(
            gdr_buffer, elastic_workspace,
            psum_num_recv_tokens_per_rank.data_ptr<int>(),
            // Use the returned per-expert count tensor as the epilogue's
            // atomic slot allocator.  The dispatch kernel fills
            // psum_num_recv_tokens_per_expert with prefix sums; those are not
            // valid per-expert local offsets for the packed output layout.
            packed_recv_count.data_ptr<int>(),
            packed_recv_x.data_ptr(),
            packed_recv_x_scales_ptr,
            nullptr,  // recv_topk_idx
            nullptr,  // recv_topk_weights
            packed_recv_src_info.data_ptr<int>(),
            num_max_dispatch_tokens_per_rank * num_ranks, hidden,
            use_fp8 ? num_scales : 0,
            // Scale tensor layout is [local_expert, token, hidden/128]
            // with strides [total_tokens * num_scales, 1, total_tokens].
            1, num_ranks * num_max_dispatch_tokens_per_rank,
            num_topk, num_experts, rank, num_ranks,
            num_max_dispatch_tokens_per_rank, use_fp8, launch_stream);
    } else {
        launcher(return_recv_hook
                     ? LOW_LATENCY_SEND_PHASE
                     : (LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE));
    }

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        // NOTES: we must ensure the all tensors will not be deallocated
        // before the stream-wait happens, so in Python API, we must wrap
        // all tensors into the event handle.
        event = EventHandle(launch_stream);
    } else if (not return_recv_hook) {
        stream_wait(compute_stream, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook) {
        if (use_elastic_dispatch_) {
            // Elastic dispatch is not split into SEND/RECV phases in this
            // port: prologue, main dispatch, and copy epilogue have already
            // completed above.  Return a no-op hook to preserve Python API
            // semantics without launching the legacy recv kernel.
            recv_hook = []() {};
        } else {
            recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };
        }
    }

    // Return values
    return {packed_recv_x,
            packed_recv_x_scales,
            packed_recv_count,
            packed_recv_src_info,
            packed_recv_layout_range,
            event,
            recv_hook};
}

std::tuple<torch::Tensor, std::optional<EventHandle>,
           std::optional<std::function<void()>>>
MooncakeEpBuffer::combine(const torch::Tensor& x, const torch::Tensor& topk_idx,
                          const torch::Tensor& topk_weights,
                          const torch::Tensor& src_info,
                          const torch::Tensor& layout_range,
                          torch::Tensor& active_ranks,
                          int num_max_dispatch_tokens_per_rank, int num_experts,
                          int timeout_us, bool zero_copy, bool async,
                          bool return_recv_hook,
                          const std::optional<torch::Tensor>& out) {
    // Tensor checks
    EP_HOST_ASSERT(x.dim() == 3 and x.is_contiguous() and
                   x.scalar_type() == torch::kBFloat16);
    EP_HOST_ASSERT(x.size(0) == num_experts / num_ranks);
    EP_HOST_ASSERT(x.size(1) == num_ranks * num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(x.size(2) % sizeof(int4) == 0 and x.size(2) % 128 == 0);
    EP_HOST_ASSERT(topk_idx.dim() == 2 and topk_idx.is_contiguous());
    EP_HOST_ASSERT(topk_idx.size(0) == topk_weights.size(0) and
                   topk_idx.size(1) == topk_weights.size(1));
    EP_HOST_ASSERT(topk_idx.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(topk_weights.dim() == 2 and topk_weights.is_contiguous());
    EP_HOST_ASSERT(topk_weights.size(0) <= num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(topk_weights.scalar_type() == torch::kFloat32);
    EP_HOST_ASSERT(src_info.dim() == 2 and src_info.is_contiguous());
    EP_HOST_ASSERT(src_info.scalar_type() == torch::kInt32 and
                   x.size(0) == src_info.size(0));
    EP_HOST_ASSERT(layout_range.dim() == 2 and layout_range.is_contiguous());
    EP_HOST_ASSERT(layout_range.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(layout_range.size(0) == num_experts / num_ranks and
                   layout_range.size(1) == num_ranks);
    auto hidden = static_cast<int>(x.size(2));
    auto num_local_experts = num_experts / num_ranks,
         num_topk = static_cast<int>(topk_weights.size(1));
    auto num_combined_tokens = static_cast<int>(topk_weights.size(0));

    // Buffer control
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_ep_buffer_bytes);
    auto buffer = layout.buffers[buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) {
        stream_wait(launch_stream, compute_stream);
    } else if (use_elastic_combine_) {
        // See dispatch hook-mode ordering note above.
        stream_wait(launch_stream, comm_stream);
    }

    // Allocate output tensor
    torch::Tensor combined_x;
    if (out.has_value()) {
        EP_HOST_ASSERT(out->dim() == 2 and out->is_contiguous());
        EP_HOST_ASSERT(out->size(0) == num_combined_tokens and
                       out->size(1) == hidden);
        EP_HOST_ASSERT(out->scalar_type() == x.scalar_type());
        combined_x = out.value();
    } else {
        combined_x = torch::empty({num_combined_tokens, hidden}, x.options());
    }

    int64_t timeout_ticks =
        timeout_us == -1 ? -1
                         : (int64_t)clock_rate_khz * (int64_t)timeout_us / 1000;

    void* raddrs_ptr = rdma_transport_ ? rdma_transport_->raddrsPtr() : nullptr;
    void* rkeys_ptr = rdma_transport_ ? rdma_transport_->rkeysPtr() : nullptr;
    void* qp_devctxs_ptr =
        rdma_transport_ ? rdma_transport_->qpDevCtxsPtr() : nullptr;
    int32_t* nvlink_avail = p2p_transport_->availableTablePtr();
    void** ipc_ptrs = p2p_transport_->peerPtrsTablePtr();

    // Kernel launch
    auto launcher = [=](int phases) {
        mooncake::launch_combine(
            combined_x.data_ptr(), active_ranks.data_ptr<int32_t>(), gdr_buffer,
            buffer.rdma_send_signal_buffer, buffer.rdma_recv_signal_buffer,
            buffer.rdma_send_data_buffer, buffer.rdma_recv_data_buffer, nullptr,
            nullptr, raddrs_ptr, rkeys_ptr, qp_devctxs_ptr, nvlink_avail,
            ipc_ptrs, x.data_ptr(), topk_idx.data_ptr<int64_t>(),
            topk_weights.data_ptr<float>(), src_info.data_ptr<int>(),
            layout_range.data_ptr<int64_t>(),
            next_buffer.rdma_recv_signal_buffer, num_combined_tokens, hidden,
            num_max_dispatch_tokens_per_rank, num_topk, num_experts, rank,
            num_ranks, workspace, launch_stream, timeout_ticks, phases,
            zero_copy);
    };

    // Elastic combine path (DeepEP V2 port)
    if (use_elastic_combine_) {
        auto psum_num_recv_tokens_per_rank = torch::zeros(
            {num_ranks}, torch::dtype(torch::kInt32).device(x.device()));

        // Run elastic combine kernel
        mooncake::launch_elastic_combine(
            const_cast<void*>(x.data_ptr()),
            const_cast<float*>(topk_weights.data_ptr<float>()),
            const_cast<int*>(src_info.data_ptr<int>()),
            psum_num_recv_tokens_per_rank.data_ptr<int>(),
            gdr_buffer, raddrs_ptr, rkeys_ptr, qp_devctxs_ptr,
            nvlink_avail, ipc_ptrs,
            buffer.rdma_send_signal_buffer, buffer.rdma_recv_signal_buffer,
            buffer.rdma_send_data_buffer, buffer.rdma_recv_data_buffer,
            gdr_buffer,  // buffer (communication buffer)
            elastic_workspace,
            num_combined_tokens, hidden, num_topk, num_experts, rank,
            num_ranks, num_max_dispatch_tokens_per_rank,
            launch_stream, timeout_ticks);

        // Run reduce epilogue
        // Pass topk_idx as combined_topk_idx so the epilogue can determine
        // which rank's buffer to read from for each token.
        mooncake::combine_reduce_epilogue(
            combined_x.data_ptr(),
            const_cast<float*>(topk_weights.data_ptr<float>()),  // input weights
            topk_idx.data_ptr<int64_t>(),  // combined_topk_idx
            gdr_buffer,  // recv_buffer
            nullptr, nullptr,  // bias_0, bias_1
            num_combined_tokens, hidden, num_topk, num_experts, rank,
            num_ranks, num_max_dispatch_tokens_per_rank, launch_stream);
    } else {
        launcher(return_recv_hook
                     ? LOW_LATENCY_SEND_PHASE
                     : (LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE));
    }

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        // NOTES: we must ensure the all tensors will not be deallocated
        // before the stream-wait happens, so in Python API, we must wrap
        // all tensors into the event handle.
        event = EventHandle(launch_stream);
    } else if (not return_recv_hook) {
        stream_wait(compute_stream, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook) {
        if (use_elastic_combine_) {
            // Elastic combine is executed eagerly above; do not launch the
            // legacy recv phase from the hook path.
            recv_hook = []() {};
        } else {
            recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };
        }
    }

    // Return values
    return {combined_x, event, recv_hook};
}

torch::Tensor MooncakeEpBuffer::get_next_combine_buffer(
    int num_max_dispatch_tokens_per_rank, int hidden, int num_experts) {
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);

    auto buffer = layout.buffers[buffer_idx];
    auto dtype = torch::kBFloat16;
    size_t num_bytes_per_combine_msg = hidden * sizeof(nv_bfloat16);
    auto num_msg_elems =
        static_cast<int>(num_bytes_per_combine_msg / elementSize(dtype));

    EP_HOST_ASSERT(num_bytes_per_combine_msg % elementSize(dtype) == 0);
    return torch::from_blob(
        buffer.rdma_send_data_buffer,
        {num_experts / num_ranks, num_ranks * num_max_dispatch_tokens_per_rank,
         hidden},
        {num_ranks * num_max_dispatch_tokens_per_rank * num_msg_elems,
         num_msg_elems, 1},
        torch::TensorOptions().dtype(dtype).device(torch::kCUDA));
}

void MooncakeEpBuffer::update_local_qpns() {
    if (!rdma_transport_) return;
    int ret = rdma_transport_->recreateQueuePairs(comm_stream.stream());
    if (ret != 0) {
        ibgda_disabled_ = true;
        LOG(ERROR) << "[EP] Failed to recreate QPs";
    }
}

void MooncakeEpBuffer::sync_ibgda_peers(
    const std::vector<int64_t>& remote_addrs,
    const std::vector<int32_t>& remote_keys,
    const std::vector<std::vector<int32_t>>& peer_qpns,
    const std::vector<std::vector<int32_t>>& peer_lids,
    const std::vector<int64_t>& subnet_prefixes,
    const std::vector<int64_t>& interface_ids,
    const std::vector<int>& active_ranks_mask) {
    if (!rdma_transport_) return;

    // Flatten per-rank QPN/LID vectors into per-QP arrays expected by
    // connectPeers.  Each rank owns USE_QP_COUNT/num_ranks QPs.
    int qps_per_rank = USE_QP_COUNT / num_ranks;
    std::vector<int32_t> flat_qpns, flat_lids;
    flat_qpns.reserve(USE_QP_COUNT);
    flat_lids.reserve(USE_QP_COUNT);
    for (int r = 0; r < num_ranks; ++r) {
        for (int q = 0; q < qps_per_rank; ++q) {
            flat_qpns.push_back(peer_qpns[r][q]);
            flat_lids.push_back(peer_lids[r][q]);
        }
    }

    int ret = rdma_transport_->connectPeers(
        rank, rdma_transport_->isRoce(), remote_addrs, remote_keys, flat_qpns,
        flat_lids, subnet_prefixes, interface_ids, active_ranks_mask);
    if (ret != 0) {
        ibgda_disabled_ = true;
        LOG(WARNING)
            << "[EP] IBGDA connectPeers failed, falling back to P2P-only path";
    }
}

std::vector<int32_t> MooncakeEpBuffer::get_ipc_handle() {
    return p2p_transport_->exportIpcHandle(gdr_buffer);
}

void MooncakeEpBuffer::sync_nvlink_ipc_handles(
    const std::vector<std::vector<int32_t>>& remote_handles,
    const std::vector<int>& active_ranks_mask) {
    p2p_transport_->importPeerHandles(gdr_buffer, rank, num_ranks,
                                      remote_handles, active_ranks_mask);
}

}  // namespace mooncake
