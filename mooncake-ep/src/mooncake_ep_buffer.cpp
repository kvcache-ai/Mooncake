#include <mooncake_ep_buffer.h>
#include <glog/logging.h>
#include <sstream>
#include <chrono>
#include <transfer_engine.h>

namespace mooncake {

static bool epTimingEnabled() {
    const char* env = std::getenv("MOONCAKE_EP_TIMING");
    return env && (env[0] == '1' || env[0] == 'Y' || env[0] == 'y');
}

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

    // RDMA transport — optional; disabled if init fails or env var set.
    if (const char* env = std::getenv("MOONCAKE_EP_DISABLE_IBGDA")) {
        if (env[0] == '1' || env[0] == 'Y' || env[0] == 'y') {
            ibgda_disabled_ = true;
            LOG(INFO) << "[EP] IBGDA disabled by MOONCAKE_EP_DISABLE_IBGDA";
        }
    }
    if (!ibgda_disabled_ && engine) {
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
    } else if (!ibgda_disabled_) {
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
                              num_ranks, USE_QP_COUNT, comm_stream.stream())) {
            owned_rdma_transport_ = std::move(t);
            rdma_transport_ = owned_rdma_transport_.get();
        } else {
            ibgda_disabled_ = true;
            LOG(INFO) << "[EP] IBGDA unavailable, using P2P-only path";
        }
    }

    // Create 32 MiB workspace
    CUDA_CHECK(cudaMalloc(&workspace, NUM_WORKSPACE_BYTES));
    CUDA_CHECK(cudaMemsetAsync(workspace, 0, NUM_WORKSPACE_BYTES, comm_stream));
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
    int current_buffer_idx = buffer_idx;
    auto buffer = layout.buffers[current_buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];
    int phase_epoch = ++phase_epochs[current_buffer_idx];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) stream_wait(launch_stream, compute_stream);

    // Allocate packed tensors
    auto packed_recv_x = torch::empty(
        {num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank,
         hidden},
        x.options().dtype(use_fp8 ? torch::kFloat8_e4m3fn : torch::kBFloat16));
    auto packed_recv_src_info = torch::empty(
        {num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank},
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

    auto mark_send_done = [=]() {
#ifdef MOONCAKE_EP_USE_MUSA
        mooncake::mark_phase_ack(gdr_buffer, nvlink_avail, ipc_ptrs,
                                 buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch,
                                 launch_stream);
#endif
    };

    auto wait_peer_send_done = [=]() {
#ifdef MOONCAKE_EP_USE_MUSA
        mooncake::wait_phase_ack(buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch,
                                 launch_stream, timeout_ticks);
#endif
    };

    auto mark_and_wait_peer_send_done = [=]() {
#ifdef MOONCAKE_EP_USE_MUSA
        mooncake::mark_and_wait_phase_ack(
            gdr_buffer, nvlink_avail, ipc_ptrs, buffer.rdma_send_signal_buffer,
            rank, num_ranks, phase_epoch, launch_stream, timeout_ticks);
#endif
    };

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
            num_experts, rank, num_ranks, use_fp8, workspace,
            launch_stream, timeout_ticks, phases);
    };
    if (return_recv_hook) {
        launcher(LOW_LATENCY_SEND_PHASE);
        mark_send_done();
    } else {
#ifdef MOONCAKE_EP_USE_MUSA
        if (epTimingEnabled()) {
            using hrc = std::chrono::high_resolution_clock;
            auto t0 = hrc::now();
            launcher(LOW_LATENCY_SEND_PHASE);
            cudaStreamSynchronize(launch_stream);
            auto t1 = hrc::now();
            mark_send_done();
            cudaStreamSynchronize(launch_stream);
            auto t2 = hrc::now();
            wait_peer_send_done();
            cudaStreamSynchronize(launch_stream);
            auto t3 = hrc::now();
            launcher(LOW_LATENCY_RECV_PHASE);
            cudaStreamSynchronize(launch_stream);
            auto t4 = hrc::now();
            auto send_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            auto ack_us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
            auto wait_us = std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count();
            auto recv_us = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count();
            LOG(INFO) << "[EP_TIMING] dispatch rank=" << rank
                      << " tokens=" << num_tokens
                      << " send=" << send_us << "us"
                      << " mark_ack=" << ack_us << "us"
                      << " wait_ack=" << wait_us << "us"
                      << " recv=" << recv_us << "us"
                      << " total=" << (send_us + ack_us + wait_us + recv_us) << "us";
        } else {
            launcher(LOW_LATENCY_SEND_PHASE);
            mark_and_wait_peer_send_done();
            launcher(LOW_LATENCY_RECV_PHASE);
        }
#else
        launcher(LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE);
#endif
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
    if (return_recv_hook)
        recv_hook = [=]() {
            wait_peer_send_done();
            launcher(LOW_LATENCY_RECV_PHASE);
        };

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
    int current_buffer_idx = buffer_idx;
    auto buffer = layout.buffers[current_buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];
    int phase_epoch = ++phase_epochs[current_buffer_idx];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
    auto compute_stream = at::cuda::getCurrentCUDAStream();
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) stream_wait(launch_stream, compute_stream);

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

    auto mark_send_done = [=]() {
#ifdef MOONCAKE_EP_USE_MUSA
        mooncake::mark_phase_ack(gdr_buffer, nvlink_avail, ipc_ptrs,
                                 buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch,
                                 launch_stream);
#endif
    };

    auto wait_peer_send_done = [=]() {
#ifdef MOONCAKE_EP_USE_MUSA
        mooncake::wait_phase_ack(buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch,
                                 launch_stream, timeout_ticks);
#endif
    };

    auto mark_and_wait_peer_send_done = [=]() {
#ifdef MOONCAKE_EP_USE_MUSA
        mooncake::mark_and_wait_phase_ack(
            gdr_buffer, nvlink_avail, ipc_ptrs, buffer.rdma_send_signal_buffer,
            rank, num_ranks, phase_epoch, launch_stream, timeout_ticks);
#endif
    };

    // Kernel launch
    auto launcher = [=](int phases) {
        mooncake::combine(
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
    if (return_recv_hook) {
        launcher(LOW_LATENCY_SEND_PHASE);
        mark_send_done();
    } else {
#ifdef MOONCAKE_EP_USE_MUSA
        if (epTimingEnabled()) {
            using hrc = std::chrono::high_resolution_clock;
            auto t0 = hrc::now();
            launcher(LOW_LATENCY_SEND_PHASE);
            cudaStreamSynchronize(launch_stream);
            auto t1 = hrc::now();
            mark_send_done();
            cudaStreamSynchronize(launch_stream);
            auto t2 = hrc::now();
            wait_peer_send_done();
            cudaStreamSynchronize(launch_stream);
            auto t3 = hrc::now();
            launcher(LOW_LATENCY_RECV_PHASE);
            cudaStreamSynchronize(launch_stream);
            auto t4 = hrc::now();
            auto send_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            auto ack_us = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
            auto wait_us = std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count();
            auto recv_us = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count();
            LOG(INFO) << "[EP_TIMING] combine rank=" << rank
                      << " tokens=" << num_combined_tokens
                      << " send=" << send_us << "us"
                      << " mark_ack=" << ack_us << "us"
                      << " wait_ack=" << wait_us << "us"
                      << " recv=" << recv_us << "us"
                      << " total=" << (send_us + ack_us + wait_us + recv_us) << "us";
        } else {
            launcher(LOW_LATENCY_SEND_PHASE);
            mark_and_wait_peer_send_done();
            launcher(LOW_LATENCY_RECV_PHASE);
        }
#else
        launcher(LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE);
#endif
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
    if (return_recv_hook)
        recv_hook = [=]() {
            wait_peer_send_done();
            launcher(LOW_LATENCY_RECV_PHASE);
        };

    // Return values
    return {combined_x, event, recv_hook};
}

torch::Tensor MooncakeEpBuffer::get_next_combine_buffer(
    int num_max_dispatch_tokens_per_rank, int hidden, int num_experts) {
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);

    auto buffer = layout.buffers[buffer_idx];
    auto dtype = torch::kBFloat16;
    size_t num_bytes_per_combine_msg = hidden * EP_BF16_SIZE;
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
