#include <mooncake_ep_buffer.h>
#include <glog/logging.h>
#include <algorithm>
#include <cstdlib>
#include <sstream>
#include <transfer_engine.h>

namespace mooncake {

namespace {

int active_qps_per_rank_for_ep(int qps_per_rank, bool is_roce, int cap) {
    if (!is_roce) return qps_per_rank;
    return std::min(qps_per_rank, cap);
}

cudaStream_t create_comm_stream() {
    int least_priority = 0;
    int greatest_priority = 0;
    auto status =
        cudaDeviceGetStreamPriorityRange(&least_priority, &greatest_priority);
    if (status != cudaSuccess) {
        cudaGetLastError();
        least_priority = 0;
        greatest_priority = 0;
    }

    cudaStream_t stream = nullptr;
    CUDA_CHECK(cudaStreamCreateWithPriority(&stream, cudaStreamNonBlocking,
                                            greatest_priority));
    return stream;
}

}  // namespace

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

static bool macaHostPhaseFenceCoversPeers() {
#ifdef MOONCAKE_EP_USE_MACA
    return true;
#else
    return false;
#endif
}

MooncakeEpBuffer::MooncakeEpBuffer(int rank, int num_ranks,
                                   int64_t num_ep_buffer_bytes,
                                   TransferEngine* engine)
    : rank(rank),
      num_ranks(num_ranks),
      num_ep_buffer_bytes(num_ep_buffer_bytes),
      comm_stream(create_comm_stream()) {
    USE_QP_COUNT = MAX_QP_COUNT / num_ranks * num_ranks;

    // Optional runtime override for the RoCE active-QP cap (default 8).
    // Set MOONCAKE_EP_ACTIVE_QPS_PER_RANK to a value >= the per-rank QP count
    // (e.g. 256) to effectively disable the cap.
    if (const char* env = std::getenv("MOONCAKE_EP_ACTIVE_QPS_PER_RANK")) {
        char* end = nullptr;
        long v = std::strtol(env, &end, 10);
        if (end != env && *end == '\0' && v > 0) {
            active_qps_cap_ = static_cast<int>(v);
        } else {
            LOG(WARNING) << "[EP] ignoring invalid "
                            "MOONCAKE_EP_ACTIVE_QPS_PER_RANK='"
                         << env << "'";
        }
    }
    LOG(INFO) << "[EP] RoCE active QPs/rank cap = " << active_qps_cap_;

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
                                   comm_stream)) {
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
                              num_ranks, USE_QP_COUNT, comm_stream)) {
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
    if (comm_stream) {
        cudaStreamDestroy(comm_stream);
        comm_stream = nullptr;
    }
}

std::tuple<std::optional<EventHandle>, std::optional<std::function<void()>>>
MooncakeEpBuffer::dispatch(
    uint64_t x_ptr, uint64_t topk_idx_ptr, uint64_t active_ranks_ptr,
    int num_tokens, int hidden, int num_topk,
    int num_max_dispatch_tokens_per_rank, int num_experts, int timeout_us,
    bool use_fp8, uint64_t packed_recv_x_ptr, uint64_t packed_recv_x_scales_ptr,
    uint64_t packed_recv_count_ptr, uint64_t packed_recv_src_info_ptr,
    uint64_t packed_recv_layout_range_ptr, bool async, bool return_recv_hook,
    uint64_t compute_stream_ptr) {
    EP_HOST_ASSERT(num_experts % num_ranks == 0);
    EP_HOST_ASSERT(USE_QP_COUNT % num_ranks == 0);
    EP_HOST_ASSERT(hidden % static_cast<int>(sizeof(int4)) == 0 &&
                   hidden % 128 == 0);
    EP_HOST_ASSERT(num_tokens <= num_max_dispatch_tokens_per_rank);

    auto num_scales = hidden / 128;
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
    // NOTES: the hook mode will always use the default stream, whose native
    // handle is allowed to be nullptr in CUDA/PyTorch.
    auto compute_stream_raw =
        reinterpret_cast<cudaStream_t>(compute_stream_ptr);
    auto launch_stream = return_recv_hook ? compute_stream_raw : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) stream_wait(launch_stream, compute_stream_raw);

    // Allocate packed tensors
    void* x = reinterpret_cast<void*>(x_ptr);
    auto* topk_idx = reinterpret_cast<int64_t*>(topk_idx_ptr);
    auto* active_ranks = reinterpret_cast<int32_t*>(active_ranks_ptr);
    void* packed_recv_x = reinterpret_cast<void*>(packed_recv_x_ptr);
    auto* packed_recv_x_scales =
        reinterpret_cast<float*>(packed_recv_x_scales_ptr);
    auto* packed_recv_count = reinterpret_cast<int*>(packed_recv_count_ptr);
    auto* packed_recv_src_info =
        reinterpret_cast<int*>(packed_recv_src_info_ptr);
    auto* packed_recv_layout_range =
        reinterpret_cast<int64_t*>(packed_recv_layout_range_ptr);
    EP_HOST_ASSERT(active_ranks != nullptr);
    EP_HOST_ASSERT(num_tokens == 0 || (x != nullptr && topk_idx != nullptr));
    EP_HOST_ASSERT(packed_recv_x != nullptr && packed_recv_count != nullptr);
    EP_HOST_ASSERT(packed_recv_src_info != nullptr &&
                   packed_recv_layout_range != nullptr);
    if (use_fp8) {
        EP_HOST_ASSERT((num_ranks * num_max_dispatch_tokens_per_rank) % 4 ==
                           0 and
                       "TMA requires the number of tokens to be multiple of 4");
        EP_HOST_ASSERT(packed_recv_x_scales != nullptr);
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
    int active_qps_per_rank = active_qps_per_rank_for_ep(
        USE_QP_COUNT / num_ranks, rdma_transport_ && rdma_transport_->isRoce(),
        active_qps_cap_);

    auto mark_send_done = [=]() {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
        mooncake::mark_phase_ack(gdr_buffer, nvlink_avail, ipc_ptrs,
                                 buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream);
#endif
    };

    auto wait_peer_send_done = [=]() {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
        mooncake::wait_phase_ack(buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream,
                                 timeout_ticks);
#endif
    };

    auto mark_and_wait_peer_send_done = [=]() {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
        mooncake::mark_and_wait_phase_ack(
            gdr_buffer, nvlink_avail, ipc_ptrs, buffer.rdma_send_signal_buffer,
            rank, num_ranks, phase_epoch, launch_stream, timeout_ticks);
#endif
    };

    auto launcher = [=](int phases) {
        mooncake::dispatch(
            packed_recv_x, packed_recv_x_scales, packed_recv_src_info,
            packed_recv_layout_range, packed_recv_count, active_ranks,
            gdr_buffer, buffer.rdma_send_signal_buffer,
            buffer.rdma_recv_signal_buffer, buffer.rdma_send_data_buffer,
            buffer.rdma_recv_data_buffer, nullptr, nullptr, raddrs_ptr,
            rkeys_ptr, qp_devctxs_ptr, nvlink_avail, ipc_ptrs, x, topk_idx,
            next_buffer.rdma_recv_signal_buffer, num_tokens, hidden,
            num_max_dispatch_tokens_per_rank, num_topk, num_experts, rank,
            num_ranks, use_fp8, workspace, launch_stream, timeout_ticks, phases,
            active_qps_per_rank);
    };
    if (return_recv_hook) {
        launcher(LOW_LATENCY_SEND_PHASE);
        mark_send_done();
    } else {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
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
        event = EventHandle(reinterpret_cast<uint64_t>(launch_stream));
    } else if (return_recv_hook && macaHostPhaseFenceCoversPeers()) {
        event = EventHandle(reinterpret_cast<uint64_t>(launch_stream));
    } else if (not return_recv_hook) {
        stream_wait(compute_stream_raw, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook)
        recv_hook = [=]() {
            if (!macaHostPhaseFenceCoversPeers()) wait_peer_send_done();
            launcher(LOW_LATENCY_RECV_PHASE);
        };

    // Return values
    return {event, recv_hook};
}

std::tuple<std::optional<EventHandle>, std::optional<std::function<void()>>>
MooncakeEpBuffer::combine(uint64_t x_ptr, uint64_t topk_idx_ptr,
                          uint64_t topk_weights_ptr, uint64_t src_info_ptr,
                          uint64_t layout_range_ptr, uint64_t active_ranks_ptr,
                          int num_local_experts, int num_combined_tokens,
                          int hidden, int num_topk,
                          int num_max_dispatch_tokens_per_rank, int num_experts,
                          int timeout_us, bool zero_copy,
                          uint64_t combined_x_ptr, bool async,
                          bool return_recv_hook, uint64_t compute_stream_ptr) {
    EP_HOST_ASSERT(num_local_experts == num_experts / num_ranks);
    EP_HOST_ASSERT(hidden % static_cast<int>(sizeof(int4)) == 0 &&
                   hidden % 128 == 0);
    EP_HOST_ASSERT(num_combined_tokens <= num_max_dispatch_tokens_per_rank);
    void* x = reinterpret_cast<void*>(x_ptr);
    auto* topk_idx = reinterpret_cast<int64_t*>(topk_idx_ptr);
    auto* topk_weights = reinterpret_cast<float*>(topk_weights_ptr);
    auto* src_info = reinterpret_cast<int*>(src_info_ptr);
    auto* layout_range = reinterpret_cast<int64_t*>(layout_range_ptr);
    auto* active_ranks = reinterpret_cast<int32_t*>(active_ranks_ptr);
    void* combined_x = reinterpret_cast<void*>(combined_x_ptr);
    EP_HOST_ASSERT(
        num_combined_tokens == 0 ||
        (x != nullptr && topk_idx != nullptr && topk_weights != nullptr));
    EP_HOST_ASSERT(src_info != nullptr && layout_range != nullptr);
    EP_HOST_ASSERT(active_ranks != nullptr);
    EP_HOST_ASSERT(num_combined_tokens == 0 || combined_x != nullptr);

    // Buffer control
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_ep_buffer_bytes);
    int current_buffer_idx = buffer_idx;
    auto buffer = layout.buffers[current_buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];
    int phase_epoch = ++phase_epochs[current_buffer_idx];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream, whose native
    // handle is allowed to be nullptr in CUDA/PyTorch.
    auto compute_stream_raw =
        reinterpret_cast<cudaStream_t>(compute_stream_ptr);
    auto launch_stream = return_recv_hook ? compute_stream_raw : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) stream_wait(launch_stream, compute_stream_raw);

    int64_t timeout_ticks =
        timeout_us == -1 ? -1
                         : (int64_t)clock_rate_khz * (int64_t)timeout_us / 1000;

    void* raddrs_ptr = rdma_transport_ ? rdma_transport_->raddrsPtr() : nullptr;
    void* rkeys_ptr = rdma_transport_ ? rdma_transport_->rkeysPtr() : nullptr;
    void* qp_devctxs_ptr =
        rdma_transport_ ? rdma_transport_->qpDevCtxsPtr() : nullptr;
    int32_t* nvlink_avail = p2p_transport_->availableTablePtr();
    void** ipc_ptrs = p2p_transport_->peerPtrsTablePtr();
    int active_qps_per_rank = active_qps_per_rank_for_ep(
        USE_QP_COUNT / num_ranks, rdma_transport_ && rdma_transport_->isRoce(),
        active_qps_cap_);

    auto mark_send_done = [=]() {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
        mooncake::mark_phase_ack(gdr_buffer, nvlink_avail, ipc_ptrs,
                                 buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream);
#endif
    };

    auto wait_peer_send_done = [=]() {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
        mooncake::wait_phase_ack(buffer.rdma_send_signal_buffer, rank,
                                 num_ranks, phase_epoch, launch_stream,
                                 timeout_ticks);
#endif
    };

    auto mark_and_wait_peer_send_done = [=]() {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
        mooncake::mark_and_wait_phase_ack(
            gdr_buffer, nvlink_avail, ipc_ptrs, buffer.rdma_send_signal_buffer,
            rank, num_ranks, phase_epoch, launch_stream, timeout_ticks);
#endif
    };

    // Kernel launch
    auto launcher = [=](int phases) {
        mooncake::combine(
            combined_x, active_ranks, gdr_buffer,
            buffer.rdma_send_signal_buffer, buffer.rdma_recv_signal_buffer,
            buffer.rdma_send_data_buffer, buffer.rdma_recv_data_buffer, nullptr,
            nullptr, raddrs_ptr, rkeys_ptr, qp_devctxs_ptr, nvlink_avail,
            ipc_ptrs, x, topk_idx, topk_weights, src_info, layout_range,
            next_buffer.rdma_recv_signal_buffer, num_combined_tokens, hidden,
            num_max_dispatch_tokens_per_rank, num_topk, num_experts, rank,
            num_ranks, workspace, launch_stream, timeout_ticks, phases,
            zero_copy, active_qps_per_rank);
    };
    if (return_recv_hook) {
        launcher(LOW_LATENCY_SEND_PHASE);
        mark_send_done();
    } else {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
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
        event = EventHandle(reinterpret_cast<uint64_t>(launch_stream));
    } else if (return_recv_hook && macaHostPhaseFenceCoversPeers()) {
        event = EventHandle(reinterpret_cast<uint64_t>(launch_stream));
    } else if (not return_recv_hook) {
        stream_wait(compute_stream_raw, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook)
        recv_hook = [=]() {
            if (!macaHostPhaseFenceCoversPeers()) wait_peer_send_done();
            launcher(LOW_LATENCY_RECV_PHASE);
        };

    // Return values
    return {event, recv_hook};
}

void MooncakeEpBuffer::update_local_qpns() {
    if (!rdma_transport_) return;
    int ret = rdma_transport_->recreateQueuePairs(comm_stream);
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
