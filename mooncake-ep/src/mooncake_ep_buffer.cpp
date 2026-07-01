#include <mooncake_ep_buffer.h>
#include <glog/logging.h>
#include <algorithm>
#include <cstdlib>
#include <sstream>
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

static bool envEnabled(const char* name) {
    const char* env = std::getenv(name);
    if (!env) return false;
    std::string value(env);
    for (char& c : value) c = static_cast<char>(std::toupper(c));
    return value == "1" || value == "ON" || value == "TRUE" ||
           value == "YES";
}

static bool macaHostOnlyPhaseSyncEnabled() {
#ifdef MOONCAKE_EP_USE_MACA
    const char* env = std::getenv("MOONCAKE_EP_MACA_HOST_ONLY_PHASE_SYNC");
    if (!env) return false;
    std::string value(env);
    for (char& c : value) c = static_cast<char>(std::toupper(c));
    return value == "1" || value == "ON" || value == "TRUE" ||
           value == "YES";
#else
    return false;
#endif
}

static int envInt(const char* name, int default_value) {
    const char* env = std::getenv(name);
    if (!env || !*env) return default_value;
    char* end = nullptr;
    long value = std::strtol(env, &end, 10);
    if (end == env || value <= 0) return default_value;
    return static_cast<int>(value);
}

MooncakeEpBuffer::MooncakeEpBuffer(int rank, int num_ranks,
                                   int64_t num_ep_buffer_bytes,
                                   TransferEngine* engine)
    : rank(rank),
      num_ranks(num_ranks),
      num_ep_buffer_bytes(num_ep_buffer_bytes),
      comm_stream(at::cuda::getStreamFromPool(true)) {
    USE_QP_COUNT = MAX_QP_COUNT / num_ranks * num_ranks;
    if (const char* env = std::getenv("MOONCAKE_EP_ACTIVE_QPS_PER_PEER")) {
        int active_qps_per_peer =
            envInt("MOONCAKE_EP_ACTIVE_QPS_PER_PEER", USE_QP_COUNT / num_ranks);
        active_qps_per_peer =
            std::min(active_qps_per_peer, USE_QP_COUNT / num_ranks);
        USE_QP_COUNT = active_qps_per_peer * num_ranks;
        LOG(INFO) << "[EP] Limiting IBGDA QPs to " << USE_QP_COUNT
                  << " total (" << active_qps_per_peer << " per peer) via "
                  << "MOONCAKE_EP_ACTIVE_QPS_PER_PEER";
    }
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

std::tuple<torch::Tensor, torch::Tensor> MooncakeEpBuffer::debug_signal_buffers(
    int num_max_dispatch_tokens_per_rank, int hidden, int num_experts,
    int buffer_slot) {
    EP_HOST_ASSERT(buffer_slot == 0 || buffer_slot == 1);
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_ep_buffer_bytes);
    auto buffer = layout.buffers[buffer_slot];
    auto options = torch::dtype(torch::kInt32).device(torch::kCUDA);
    auto send = torch::empty({num_experts}, options);
    auto recv = torch::empty({num_experts}, options);
    CUDA_CHECK(cudaMemcpyAsync(send.data_ptr<int>(),
                               buffer.rdma_send_signal_buffer,
                               num_experts * sizeof(int),
                               cudaMemcpyDeviceToDevice,
                               at::cuda::getCurrentCUDAStream()));
    CUDA_CHECK(cudaMemcpyAsync(recv.data_ptr<int>(),
                               buffer.rdma_recv_signal_buffer,
                               num_experts * sizeof(int),
                               cudaMemcpyDeviceToDevice,
                               at::cuda::getCurrentCUDAStream()));
    return {send, recv};
}

torch::Tensor MooncakeEpBuffer::debug_dispatch_recv_src_slots(
    int num_max_dispatch_tokens_per_rank, int hidden, int num_experts,
    int buffer_slot) {
    EP_HOST_ASSERT(buffer_slot == 0 || buffer_slot == 1);
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_ep_buffer_bytes);
    auto buffer = layout.buffers[buffer_slot];
    int num_local_experts = num_experts / num_ranks;
    size_t hidden_bytes = static_cast<size_t>(hidden) * EP_BF16_SIZE;
    size_t msg_bytes = sizeof(int4) + hidden_bytes;
    auto options = torch::dtype(torch::kInt32).device(torch::kCPU);
    auto out = torch::empty(
        {num_local_experts, num_ranks, num_max_dispatch_tokens_per_rank},
        options);
    auto out_acc = out.accessor<int, 3>();
    for (int e = 0; e < num_local_experts; ++e) {
        for (int r = 0; r < num_ranks; ++r) {
            for (int s = 0; s < num_max_dispatch_tokens_per_rank; ++s) {
                int value = 0;
                auto* src = reinterpret_cast<char*>(buffer.rdma_recv_data_buffer) +
                            (static_cast<size_t>(e) * num_ranks *
                                 num_max_dispatch_tokens_per_rank +
                             static_cast<size_t>(r) *
                                 num_max_dispatch_tokens_per_rank +
                             s) *
                                msg_bytes;
                CUDA_CHECK(cudaMemcpy(&value, src, sizeof(int),
                                      cudaMemcpyDeviceToHost));
                out_acc[e][r][s] = value;
            }
        }
    }
    return out;
}

torch::Tensor MooncakeEpBuffer::debug_rdma_put_probe(
    int dst_rank, int64_t dst_byte_offset, uint64_t value) {
    EP_HOST_ASSERT(rdma_transport_ != nullptr);
    EP_HOST_ASSERT(dst_rank >= 0 && dst_rank < num_ranks);
    EP_HOST_ASSERT(dst_byte_offset >= 0);
    EP_HOST_ASSERT(dst_byte_offset + static_cast<int64_t>(sizeof(uint64_t)) <=
                   num_ep_buffer_bytes);

    int64_t src_byte_offset =
        envInt("MACA_EP_PROBE_SRC_OFFSET", 0);
    EP_HOST_ASSERT(src_byte_offset >= 0);
    EP_HOST_ASSERT(src_byte_offset + static_cast<int64_t>(sizeof(uint64_t)) <=
                   num_ep_buffer_bytes);

    auto options = torch::dtype(torch::kUInt64).device(torch::kCUDA);
    auto* local_source = reinterpret_cast<uint64_t*>(
        static_cast<char*>(gdr_buffer) + src_byte_offset);
    CUDA_CHECK(cudaMemsetAsync(
        static_cast<char*>(gdr_buffer) + dst_byte_offset, 0, sizeof(uint64_t),
        comm_stream.stream()));
    CUDA_CHECK(cudaMemsetAsync(local_source, 0, sizeof(uint64_t),
                               comm_stream.stream()));

    int qps_per_rank = USE_QP_COUNT / num_ranks;
    int probe_bytes = envInt("MACA_EP_PROBE_BYTES", 8);
    EP_HOST_ASSERT(probe_bytes > 0);
    EP_HOST_ASSERT(src_byte_offset + probe_bytes <= num_ep_buffer_bytes);
    EP_HOST_ASSERT(dst_byte_offset + probe_bytes <= num_ep_buffer_bytes);
    int poll_completion = envEnabled("MACA_EP_PROBE_POLL") ? 1 : 0;
    mooncake::debug_rdma_put_probe(
        gdr_buffer, rdma_transport_->raddrsPtr(), rdma_transport_->rkeysPtr(),
        rdma_transport_->qpDevCtxsPtr(), rank, num_ranks, qps_per_rank,
        dst_rank, static_cast<uint64_t>(dst_byte_offset),
        static_cast<uint64_t>(src_byte_offset), value,
        static_cast<uint32_t>(probe_bytes), local_source, poll_completion,
        comm_stream.stream());
    CUDA_CHECK(cudaStreamSynchronize(comm_stream.stream()));

    auto out = torch::empty({2}, options);
    CUDA_CHECK(cudaMemcpyAsync(
        out.data_ptr(), static_cast<char*>(gdr_buffer) + dst_byte_offset,
        sizeof(uint64_t), cudaMemcpyDeviceToDevice,
        at::cuda::getCurrentCUDAStream()));
    CUDA_CHECK(cudaMemcpyAsync(
        out.data_ptr<uint64_t>() + 1, local_source, sizeof(uint64_t),
        cudaMemcpyDeviceToDevice, at::cuda::getCurrentCUDAStream()));
    return out;
}

torch::Tensor MooncakeEpBuffer::debug_rdma_multi_put_probe(
    int dst_rank, int64_t dst_byte_offset, int64_t dst_stride,
    int64_t src_byte_offset, int64_t src_stride, int nbytes, int nputs) {
    EP_HOST_ASSERT(rdma_transport_ != nullptr);
    EP_HOST_ASSERT(dst_rank >= 0 && dst_rank < num_ranks);
    EP_HOST_ASSERT(dst_byte_offset >= 0);
    EP_HOST_ASSERT(dst_stride >= nbytes);
    EP_HOST_ASSERT(src_byte_offset >= 0);
    EP_HOST_ASSERT(src_stride >= nbytes);
    EP_HOST_ASSERT(nbytes > 0);
    EP_HOST_ASSERT(nputs > 0);
    EP_HOST_ASSERT(src_byte_offset + (nputs - 1) * src_stride + nbytes <=
                   num_ep_buffer_bytes);
    EP_HOST_ASSERT(dst_byte_offset + (nputs - 1) * dst_stride + nbytes <=
                   num_ep_buffer_bytes);

    for (int i = 0; i < nputs; ++i) {
        CUDA_CHECK(cudaMemsetAsync(
            static_cast<char*>(gdr_buffer) + dst_byte_offset + i * dst_stride,
            0, nbytes, comm_stream.stream()));
        CUDA_CHECK(cudaMemsetAsync(
            static_cast<char*>(gdr_buffer) + src_byte_offset + i * src_stride,
            0, nbytes, comm_stream.stream()));
    }

    int qps_per_rank = USE_QP_COUNT / num_ranks;
    int poll_completion = envEnabled("MACA_EP_PROBE_POLL") ? 1 : 0;
    int delay_iters = envInt("MACA_EP_MULTI_PUT_DELAY_ITERS", 0);
    int pre_post_delay_iters =
        envInt("MACA_EP_MULTI_PUT_PRE_POST_DELAY_ITERS", 0);
    int channel_offset = envInt("MACA_EP_MULTI_PUT_CHANNEL_OFFSET", 0);
    int warmup_puts = envInt("MACA_EP_MULTI_PUT_WARMUP", 0);
    int prewrite_source = envEnabled("MACA_EP_MULTI_PUT_PREWRITE") ? 1 : 0;
    int64_t warmup_dst_offset =
        envInt("MACA_EP_MULTI_PUT_WARMUP_DST_OFFSET", 262144);
    int64_t warmup_src_offset =
        envInt("MACA_EP_MULTI_PUT_WARMUP_SRC_OFFSET", 131072);
    EP_HOST_ASSERT(warmup_puts >= 0);
    if (warmup_puts > 0) {
        EP_HOST_ASSERT(warmup_src_offset >= 0);
        EP_HOST_ASSERT(warmup_dst_offset >= 0);
        EP_HOST_ASSERT(warmup_src_offset + (warmup_puts - 1) * src_stride +
                           nbytes <=
                       num_ep_buffer_bytes);
        EP_HOST_ASSERT(warmup_dst_offset + (warmup_puts - 1) * dst_stride +
                           nbytes <=
                       num_ep_buffer_bytes);
    }
    if (prewrite_source) {
        for (int i = 0; i < nputs; ++i) {
            uint64_t value = (rank == 0 ? 0xabc0000000000000ULL
                                        : 0xdef0000000000000ULL) |
                             static_cast<uint64_t>(i);
            CUDA_CHECK(cudaMemcpyAsync(
                static_cast<char*>(gdr_buffer) + src_byte_offset +
                    i * src_stride,
                &value, sizeof(value), cudaMemcpyHostToDevice,
                comm_stream.stream()));
        }
        for (int i = 0; i < warmup_puts; ++i) {
            uint64_t value = (rank == 0 ? 0xabc0000000000000ULL
                                        : 0xdef0000000000000ULL) |
                             static_cast<uint64_t>(i);
            CUDA_CHECK(cudaMemcpyAsync(
                static_cast<char*>(gdr_buffer) + warmup_src_offset +
                    i * src_stride,
                &value, sizeof(value), cudaMemcpyHostToDevice,
                comm_stream.stream()));
        }
        CUDA_CHECK(cudaStreamSynchronize(comm_stream.stream()));
    }
    mooncake::debug_rdma_multi_put_probe(
        gdr_buffer, rdma_transport_->raddrsPtr(), rdma_transport_->rkeysPtr(),
        rdma_transport_->qpDevCtxsPtr(), rank, num_ranks, qps_per_rank,
        dst_rank, static_cast<uint64_t>(dst_byte_offset),
        static_cast<uint64_t>(dst_stride),
        static_cast<uint64_t>(src_byte_offset),
        static_cast<uint64_t>(src_stride), static_cast<uint32_t>(nbytes),
        nputs, poll_completion, delay_iters, channel_offset,
        warmup_puts, static_cast<uint64_t>(warmup_dst_offset),
        static_cast<uint64_t>(warmup_src_offset), pre_post_delay_iters,
        prewrite_source, comm_stream.stream());
    CUDA_CHECK(cudaStreamSynchronize(comm_stream.stream()));

    auto options = torch::dtype(torch::kUInt64).device(torch::kCUDA);
    auto out = torch::empty({nputs, 2}, options);
    for (int i = 0; i < nputs; ++i) {
        CUDA_CHECK(cudaMemcpyAsync(
            out.data_ptr<uint64_t>() + i * 2,
            static_cast<char*>(gdr_buffer) + dst_byte_offset + i * dst_stride,
            sizeof(uint64_t), cudaMemcpyDeviceToDevice,
            at::cuda::getCurrentCUDAStream()));
        CUDA_CHECK(cudaMemcpyAsync(
            out.data_ptr<uint64_t>() + i * 2 + 1,
            static_cast<char*>(gdr_buffer) + src_byte_offset + i * src_stride,
            sizeof(uint64_t), cudaMemcpyDeviceToDevice,
            at::cuda::getCurrentCUDAStream()));
    }
    return out;
}

torch::Tensor MooncakeEpBuffer::debug_read_u64(int64_t byte_offset) {
    EP_HOST_ASSERT(byte_offset >= 0);
    EP_HOST_ASSERT(byte_offset + static_cast<int64_t>(sizeof(uint64_t)) <=
                   num_ep_buffer_bytes);
    auto options = torch::dtype(torch::kUInt64).device(torch::kCUDA);
    auto out = torch::empty({1}, options);
    CUDA_CHECK(cudaMemcpyAsync(
        out.data_ptr(), static_cast<char*>(gdr_buffer) + byte_offset,
        sizeof(uint64_t), cudaMemcpyDeviceToDevice,
        at::cuda::getCurrentCUDAStream()));
    return out;
}

void MooncakeEpBuffer::debug_clear_u64(int64_t byte_offset) {
    EP_HOST_ASSERT(byte_offset >= 0);
    EP_HOST_ASSERT(byte_offset + static_cast<int64_t>(sizeof(uint64_t)) <=
                   num_ep_buffer_bytes);
    CUDA_CHECK(cudaMemsetAsync(static_cast<char*>(gdr_buffer) + byte_offset, 0,
                               sizeof(uint64_t), comm_stream.stream()));
    CUDA_CHECK(cudaStreamSynchronize(comm_stream.stream()));
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
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
        if (macaHostOnlyPhaseSyncEnabled()) return;
        mooncake::mark_phase_ack(gdr_buffer, nvlink_avail, ipc_ptrs,
                                 buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream);
#endif
    };

    auto wait_peer_send_done = [=]() {
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
        if (macaHostOnlyPhaseSyncEnabled()) return;
        mooncake::wait_phase_ack(buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream,
                                 timeout_ticks);
#endif
    };

    auto mark_and_wait_peer_send_done = [=]() {
#if defined(MOONCAKE_EP_USE_MACA)
        if (macaHostOnlyPhaseSyncEnabled()) return;
        mooncake::mark_phase_ack(gdr_buffer, nvlink_avail, ipc_ptrs,
                                 buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream);
        CUDA_CHECK(cudaStreamSynchronize(launch_stream));
        mooncake::wait_phase_ack(buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream,
                                 timeout_ticks);
        CUDA_CHECK(cudaStreamSynchronize(launch_stream));
#elif defined(MOONCAKE_EP_USE_MUSA)
        mooncake::mark_and_wait_phase_ack(
            gdr_buffer, nvlink_avail, ipc_ptrs, buffer.rdma_send_signal_buffer,
            rank, num_ranks, phase_epoch, launch_stream, timeout_ticks);
#endif
    };

    const int force_rdma_data =
        envEnabled("MOONCAKE_EP_FORCE_RDMA_DATA_PATH") ? 1 : 0;
    const int poll_rdma_put =
        envEnabled("MOONCAKE_EP_RDMA_POLL_PUT") ? 1 : 0;
    const int rdma_write_signal =
        envEnabled("MOONCAKE_EP_RDMA_WRITE_SIGNAL") ? 1 : 0;
    const int active_qps_per_peer = std::min(
        USE_QP_COUNT / num_ranks,
        envInt("MOONCAKE_EP_ACTIVE_QPS_PER_PEER", USE_QP_COUNT / num_ranks));

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
            num_experts, rank, num_ranks, use_fp8, force_rdma_data,
            poll_rdma_put, rdma_write_signal, active_qps_per_peer, workspace,
            launch_stream, timeout_ticks, phases);
    };
    if (return_recv_hook) {
        launcher(LOW_LATENCY_SEND_PHASE);
        mark_send_done();
    } else {
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
        launcher(LOW_LATENCY_SEND_PHASE);
        mark_and_wait_peer_send_done();
        launcher(LOW_LATENCY_RECV_PHASE);
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
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
        if (macaHostOnlyPhaseSyncEnabled()) return;
        mooncake::mark_phase_ack(gdr_buffer, nvlink_avail, ipc_ptrs,
                                 buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream);
#endif
    };

    auto wait_peer_send_done = [=]() {
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
        if (macaHostOnlyPhaseSyncEnabled()) return;
        mooncake::wait_phase_ack(buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream,
                                 timeout_ticks);
#endif
    };

    auto mark_and_wait_peer_send_done = [=]() {
#if defined(MOONCAKE_EP_USE_MACA)
        if (macaHostOnlyPhaseSyncEnabled()) return;
        mooncake::mark_phase_ack(gdr_buffer, nvlink_avail, ipc_ptrs,
                                 buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream);
        CUDA_CHECK(cudaStreamSynchronize(launch_stream));
        mooncake::wait_phase_ack(buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream,
                                 timeout_ticks);
        CUDA_CHECK(cudaStreamSynchronize(launch_stream));
#elif defined(MOONCAKE_EP_USE_MUSA)
        mooncake::mark_and_wait_phase_ack(
            gdr_buffer, nvlink_avail, ipc_ptrs, buffer.rdma_send_signal_buffer,
            rank, num_ranks, phase_epoch, launch_stream, timeout_ticks);
#endif
    };

    // Kernel launch
    const int force_rdma_data =
        envEnabled("MOONCAKE_EP_FORCE_RDMA_DATA_PATH") ? 1 : 0;
    const int poll_rdma_put =
        envEnabled("MOONCAKE_EP_RDMA_POLL_PUT") ? 1 : 0;
    const int rdma_write_signal =
        envEnabled("MOONCAKE_EP_RDMA_WRITE_SIGNAL") ? 1 : 0;
    const int active_qps_per_peer = std::min(
        USE_QP_COUNT / num_ranks,
        envInt("MOONCAKE_EP_ACTIVE_QPS_PER_PEER", USE_QP_COUNT / num_ranks));

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
            num_ranks, force_rdma_data, poll_rdma_put, rdma_write_signal,
            active_qps_per_peer, workspace, launch_stream, timeout_ticks,
            phases, zero_copy);
    };
    if (return_recv_hook) {
        launcher(LOW_LATENCY_SEND_PHASE);
        mark_send_done();
    } else {
#if defined(MOONCAKE_EP_USE_MUSA) || defined(MOONCAKE_EP_USE_MACA)
        launcher(LOW_LATENCY_SEND_PHASE);
        mark_and_wait_peer_send_done();
        launcher(LOW_LATENCY_RECV_PHASE);
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
    if (envEnabled("MOONCAKE_EP_DEBUG_BOOTSTRAP")) {
        auto local_meta = rdma_transport_->localMetadata();
        std::ostringstream oss;
        oss << "[EP IBGDA connect layout] rank=" << rank
            << " qps_per_rank=" << qps_per_rank
            << " local_raddr=0x" << std::hex
            << static_cast<uint64_t>(local_meta.raddr)
            << " local_rkey=0x" << static_cast<uint32_t>(local_meta.rkey)
            << std::dec << " active=";
        for (int r = 0; r < num_ranks; ++r) {
            oss << active_ranks_mask[r];
        }
        oss << " remote_meta=";
        for (int r = 0; r < num_ranks; ++r) {
            oss << "{r=" << r << ",addr=0x" << std::hex
                << static_cast<uint64_t>(remote_addrs[r])
                << ",key=0x" << static_cast<uint32_t>(remote_keys[r])
                << std::dec << "}";
        }
        oss << " flat_qpns=";
        int limit = std::min<int>(static_cast<int>(flat_qpns.size()), 32);
        for (int i = 0; i < limit; ++i) {
            int peer_rank = i * num_ranks / USE_QP_COUNT;
            int channel = i % qps_per_rank;
            oss << "{i=" << i << ",peer=" << peer_rank
                << ",ch=" << channel << ",qpn=" << flat_qpns[i] << "}";
        }
        if (static_cast<int>(flat_qpns.size()) > limit) oss << "...";
        LOG(INFO) << oss.str();
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
