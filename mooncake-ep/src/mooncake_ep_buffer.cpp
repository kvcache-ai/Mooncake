#include <mooncake_ep_buffer.h>
#include <glog/logging.h>
#include <algorithm>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <transfer_engine.h>

namespace mooncake {

namespace {

int active_qps_per_rank_for_ep(int qps_per_rank, bool is_roce, int cap,
                               int num_local_experts) {
#ifdef MOONCAKE_EP_USE_MUSA
    // MUSA's verified protocol uses one active QP until each QP owns an
    // independent source row.
    return 1;
#else
    if (!is_roce) return qps_per_rank;
    // CUDA RoCE shares HCAs across local GPUs; spreading small EP messages
    // over all expert QPs adds doorbell/progress overhead. MACA keeps the
    // existing expert-scaled policy until its high-QP path is fully tuned.
    int target = cap;
    if (target <= 0) {
#ifdef MOONCAKE_EP_USE_MACA
        target = std::max(8, num_local_experts);
#else
        target = 8;
#endif
    }
    return std::min(qps_per_rank, target);
#endif
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

bool stream_is_capturing(cudaStream_t stream) {
    cudaStreamCaptureStatus status = cudaStreamCaptureStatusNone;
    auto error = cudaStreamIsCapturing(stream, &status);
    if (error != cudaSuccess) {
        cudaGetLastError();
        return false;
    }
    return status != cudaStreamCaptureStatusNone;
}

constexpr int kTopkShadowMaxTokens = 1024;
constexpr int kTopkShadowMaxTopk = 32;
constexpr int kTopkShadowSlots = kEpDispatchSlots;
constexpr size_t kTopkShadowBytes = static_cast<size_t>(kTopkShadowMaxTokens) *
                                    kTopkShadowMaxTopk * sizeof(int64_t);

int validate_rank(int rank, int num_ranks) {
    if (num_ranks <= 0 || num_ranks > MAX_QP_COUNT || rank < 0 ||
        rank >= num_ranks) {
        throw std::invalid_argument("invalid EP rank or world size");
    }
    return rank;
}

int64_t validate_buffer_size(int64_t num_ep_buffer_bytes) {
    if (num_ep_buffer_bytes <= 0) {
        throw std::invalid_argument("EP buffer size must be positive");
    }
    return num_ep_buffer_bytes;
}

int64_t* topk_shadow(void* workspace, int shadow_slot, int capacity_experts) {
    const size_t workspace_bytes = static_cast<size_t>(NUM_WORKSPACE_BYTES);
    const size_t counter_bytes =
        2 * static_cast<size_t>(capacity_experts) * sizeof(int);
    const size_t shadow_pool_bytes =
        static_cast<size_t>(kTopkShadowSlots) * kTopkShadowBytes;
    EP_HOST_ASSERT(shadow_pool_bytes <= workspace_bytes &&
                   counter_bytes <= workspace_bytes - shadow_pool_bytes);
    auto* base = reinterpret_cast<uint8_t*>(workspace) + counter_bytes;
    return reinterpret_cast<int64_t*>(base + shadow_slot * kTopkShadowBytes);
}

}  // namespace

// Initialize an RDMA transport: register memory, allocate control buffer,
// create QPs.  Returns true on success, false if IBGDA is unavailable.
static bool initRdmaTransport(device::RdmaTransport* t, void* gdr_buffer,
                              int64_t num_ep_buffer_bytes, int num_ranks,
                              int use_qp_count, void* stream,
                              device::RdmaMemoryRegion& memory_region) {
    int ret = t->initialize("", num_ranks, use_qp_count);
    if (ret == 0) {
        ret = t->registerMemory(gdr_buffer, num_ep_buffer_bytes, memory_region);
    }
    if (ret == 0) {
        ret = t->allocateControlBuffer();
    }
    if (ret == 0) {
        ret = t->createQueuePairs(stream);
    }
    if (ret != 0 && memory_region.addr) {
        t->unregisterMemory(memory_region);
        memory_region = {};
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
                                   bool disable_p2p, TransferEngine* engine)
    : rank(validate_rank(rank, num_ranks)),
      num_ranks(num_ranks),
      num_ep_buffer_bytes(validate_buffer_size(num_ep_buffer_bytes)),
      p2p_enabled_(!disable_p2p),
      comm_stream(create_comm_stream()) {
    USE_QP_COUNT = MAX_QP_COUNT / num_ranks * num_ranks;

    // Optional runtime override for the CUDA/MACA RoCE active-QP count.
    active_qps_cap_ = 0;
#ifndef MOONCAKE_EP_USE_MUSA
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
    LOG(INFO) << "[EP] RoCE active QPs/rank override = "
              << (active_qps_cap_ > 0 ? std::to_string(active_qps_cap_)
                                      : "auto");
#else
    LOG(INFO) << "[EP] MUSA active QPs/rank = 1";
#endif

    // Get ranks
    CUDA_CHECK(cudaGetDevice(&device_id));
    CUDA_CHECK(cudaDeviceGetAttribute(&clock_rate_khz, cudaDevAttrClockRate,
                                      device_id));

    // P2P transport owns GDR buffer allocation. Peer mappings remain disabled
    // when the EP caller selects RDMA-only operation.
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
                                   comm_stream, rdma_memory_region_)) {
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
                              num_ranks, USE_QP_COUNT, comm_stream,
                              rdma_memory_region_)) {
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
    if (explicitly_destroy_ && !destroyed_) {
        owned_p2p_transport_.release();
        owned_rdma_transport_.release();
        return;
    }
    destroy();
}

void MooncakeEpBuffer::set_explicit_destroy(bool explicitly_destroy) {
    if (destroyed_) throw std::runtime_error("EP buffer is already destroyed");
    explicitly_destroy_ = explicitly_destroy;
}

void MooncakeEpBuffer::destroy() {
    if (destroyed_) return;
    if (elastic_enabled_) {
        CUDA_CHECK(cudaDeviceSynchronize());
        std::lock_guard<std::mutex> lock(elastic_mutex_);
        destroyed_ = true;
        if (p2p_transport_) {
            for (int peer = 0; peer < num_ranks; ++peer)
                p2p_transport_->discardStagedPeer(peer);
        }
    }

    // When EP owns the rdma transport, its destructor handles QP/MR/control
    // teardown. When the engine owns it, unregister this buffer's MR.
    if (!owned_rdma_transport_ && rdma_transport_ && rdma_memory_region_.addr) {
        const int ret = rdma_transport_->unregisterMemory(rdma_memory_region_);
        if (ret != 0) {
            LOG(ERROR) << "[EP] Failed to unregister the IBGDA memory region";
        }
    }
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

    if (workspace) {
        cudaFree(workspace);
        workspace = nullptr;
    }
    if (comm_stream) {
        cudaStreamDestroy(comm_stream);
        comm_stream = nullptr;
    }
    destroyed_ = true;
}

void MooncakeEpBuffer::configure_elastic(int num_experts_per_rank,
                                         uint64_t active_ranks_ptr) {
    std::lock_guard<std::mutex> lock(elastic_mutex_);
    if (elastic_enabled_ || destroyed_ || num_experts_per_rank <= 0 ||
        active_ranks_ptr == 0 || num_ranks <= 0 || rank < 0 ||
        rank >= num_ranks) {
        throw std::invalid_argument("invalid elastic EP buffer configuration");
    }
    const size_t control_bytes =
        2 * static_cast<size_t>(num_ranks) * sizeof(int32_t);
    if (num_ep_buffer_bytes < static_cast<int64_t>(control_bytes)) {
        throw std::invalid_argument(
            "elastic EP buffer is too small for rank control slots; use "
            "get_ep_elastic_buffer_size_hint");
    }

    elastic_enabled_ = true;
    num_experts_per_rank_ = num_experts_per_rank;
    elastic_active_ranks_ = reinterpret_cast<int32_t*>(active_ranks_ptr);
    elastic_connected_ranks_.assign(num_ranks, 0);
    elastic_committed_ranks_.assign(num_ranks, 0);
    elastic_staged_ranks_.assign(num_ranks, 0);
    elastic_active_ranks_host_.assign(num_ranks, 0);
    elastic_connected_ranks_[rank] = 1;
    elastic_committed_ranks_[rank] = 1;
    elastic_active_ranks_host_[rank] = 1;
    active_rank_bound_ = rank + 1;

    auto* control = reinterpret_cast<uint8_t*>(gdr_buffer) +
                    num_ep_buffer_bytes - control_bytes;
    barrier_send_count_ = reinterpret_cast<int*>(control);
    barrier_arrivals_ = barrier_send_count_ + num_ranks;
    CUDA_CHECK(cudaMemsetAsync(elastic_active_ranks_, 0,
                               num_ranks * sizeof(int32_t), comm_stream));
    const int32_t self_active = 1;
    CUDA_CHECK(cudaMemcpyAsync(elastic_active_ranks_ + rank, &self_active,
                               sizeof(self_active), cudaMemcpyHostToDevice,
                               comm_stream));
    if (rdma_transport_) {
        const int64_t local_addr =
            reinterpret_cast<int64_t>(rdma_memory_region_.addr);
        const int32_t local_key =
            static_cast<int32_t>(rdma_memory_region_.lkey);
        CUDA_CHECK(
            cudaMemcpyAsync(static_cast<char*>(rdma_transport_->raddrsPtr()) +
                                rank * sizeof(uint64_t),
                            &local_addr, sizeof(local_addr),
                            cudaMemcpyHostToDevice, comm_stream));
        CUDA_CHECK(
            cudaMemcpyAsync(static_cast<char*>(rdma_transport_->rkeysPtr()) +
                                rank * sizeof(uint32_t),
                            &local_key, sizeof(local_key),
                            cudaMemcpyHostToDevice, comm_stream));
    }
    CUDA_CHECK(cudaStreamSynchronize(comm_stream));
}

void MooncakeEpBuffer::validate_connect_ranks(
    const std::vector<int>& peer_ranks) {
    std::lock_guard<std::mutex> lock(elastic_mutex_);
    if (!elastic_enabled_ || destroyed_) {
        throw std::runtime_error("elastic EP buffer is not configured");
    }
    for (int peer_rank : peer_ranks) {
        if (peer_rank < 0 || peer_rank >= num_ranks || peer_rank == rank) {
            std::ostringstream message;
            message << "invalid elastic EP connect rank slot " << peer_rank;
            throw std::invalid_argument(message.str());
        }
        if (elastic_connected_ranks_[peer_rank]) {
            std::ostringstream message;
            message << "elastic EP rank slot " << peer_rank
                    << " is already connected; call disconnect_ranks() first";
            throw std::invalid_argument(message.str());
        }
    }
}

void MooncakeEpBuffer::stage_peer(int peer_rank, int64_t raddr, int32_t rkey,
                                  int64_t subnet_prefix, int64_t interface_id,
                                  const std::vector<int32_t>& qpns,
                                  const std::vector<int32_t>& lids,
                                  const std::vector<int32_t>& ipc_handle) {
    std::lock_guard<std::mutex> lock(elastic_mutex_);
    if (!elastic_enabled_ || destroyed_ || peer_rank < 0 ||
        peer_rank >= num_ranks || peer_rank == rank) {
        throw std::invalid_argument("invalid elastic EP peer rank");
    }
    if (elastic_connected_ranks_[peer_rank]) {
        std::ostringstream message;
        message << "elastic EP rank slot " << peer_rank
                << " is already connected; call disconnect_ranks() first";
        throw std::invalid_argument(message.str());
    }
    if (elastic_active_ranks_host_[peer_rank]) {
        throw std::invalid_argument(
            "cannot prepare an active elastic EP rank slot");
    }
    if (rdma_transport_) {
        const int qps_per_rank = USE_QP_COUNT / num_ranks;
        if (static_cast<int>(qpns.size()) != qps_per_rank ||
            static_cast<int>(lids.size()) != qps_per_rank) {
            throw std::invalid_argument(
                "elastic peer metadata must contain the fixed-capacity QP row");
        }
    }

    if (ipc_handle.empty() && !rdma_transport_) {
        throw std::runtime_error(
            "elastic EP peer has no P2P handle and IBGDA is unavailable");
    }
    if (!p2p_enabled_ && !rdma_transport_) {
        throw std::runtime_error(
            "elastic EP has neither P2P nor IBGDA for this peer");
    }

    try {
        if (p2p_enabled_) {
            const int ret = p2p_transport_->stagePeerHandle(
                gdr_buffer, rank, peer_rank, ipc_handle);
            if (ret != 0) {
                throw std::runtime_error(
                    "failed to stage elastic P2P peer mapping");
            }
        }

        if (rdma_transport_) {
            const int qps_per_rank = USE_QP_COUNT / num_ranks;
            const int qp_offset = peer_rank * qps_per_rank;
            std::vector<int64_t> remote_addrs(num_ranks, 0);
            std::vector<int32_t> remote_keys(num_ranks, 0);
            std::vector<int32_t> remote_qpns(USE_QP_COUNT, 0);
            std::vector<int32_t> remote_lids(USE_QP_COUNT, 0);
            std::vector<int64_t> subnet_prefixes(num_ranks, 0);
            std::vector<int64_t> interface_ids(num_ranks, 0);
            std::vector<int> active_peer(num_ranks, 0);
            remote_addrs[peer_rank] = raddr;
            remote_keys[peer_rank] = rkey;
            subnet_prefixes[peer_rank] = subnet_prefix;
            interface_ids[peer_rank] = interface_id;
            std::copy(qpns.begin(), qpns.end(),
                      remote_qpns.begin() + qp_offset);
            std::copy(lids.begin(), lids.end(),
                      remote_lids.begin() + qp_offset);
            active_peer[peer_rank] = 1;

            auto prepare_stream = create_comm_stream();
            try {
                if (rdma_transport_->resetQueuePairs(qp_offset, qps_per_rank,
                                                     prepare_stream) != 0) {
                    throw std::runtime_error(
                        "failed to reset elastic peer QPs");
                }
                const int ret = rdma_transport_->connectPeers(
                    rank, rdma_transport_->isRoce(), rdma_memory_region_.lkey,
                    remote_addrs, remote_keys, remote_qpns, remote_lids,
                    subnet_prefixes, interface_ids, active_peer);
                if (ret != 0) {
                    throw std::runtime_error(
                        "failed to connect elastic peer QPs");
                }
            } catch (...) {
                const auto status = cudaStreamDestroy(prepare_stream);
                if (status != cudaSuccess) {
                    LOG(ERROR) << "failed to destroy elastic prepare stream: "
                               << cudaGetErrorString(status);
                }
                throw;
            }
            CUDA_CHECK(cudaStreamDestroy(prepare_stream));
            LOG(INFO) << "[EP] prepared RDMA peer rank=" << peer_rank
                      << " (masked until commit)";
        }
    } catch (...) {
        if (p2p_enabled_) p2p_transport_->discardStagedPeer(peer_rank);
        throw;
    }

    elastic_connected_ranks_[peer_rank] = 1;
    elastic_staged_ranks_[peer_rank] = 1;
}

void MooncakeEpBuffer::commit_staged_peers() {
    if (!elastic_enabled_) return;
    bool has_staged = false;
    for (int peer = 0; peer < num_ranks; ++peer)
        has_staged |= elastic_staged_ranks_[peer] != 0;
    if (!has_staged) return;

    CUDA_CHECK(cudaDeviceSynchronize());
    for (int peer = 0; peer < num_ranks; ++peer) {
        if (!elastic_staged_ranks_[peer]) continue;
        if (p2p_enabled_ && p2p_transport_->publishStagedPeer(peer) != 0) {
            throw std::runtime_error("failed to publish elastic P2P peer slot");
        }
        CUDA_CHECK(cudaMemsetAsync(barrier_send_count_ + peer, 0,
                                   sizeof(int32_t), comm_stream));
        CUDA_CHECK(cudaMemsetAsync(barrier_arrivals_ + peer, 0, sizeof(int32_t),
                                   comm_stream));
        elastic_staged_ranks_[peer] = 0;
        elastic_committed_ranks_[peer] = 1;
    }
    CUDA_CHECK(cudaStreamSynchronize(comm_stream));
}

void MooncakeEpBuffer::refresh_active_rank_bound() {
    active_rank_bound_ = 0;
    for (int peer = 0; peer < num_ranks; ++peer) {
        if (elastic_active_ranks_host_[peer]) active_rank_bound_ = peer + 1;
    }
}

void MooncakeEpBuffer::update_mask_buffer(int peer_rank, bool masked,
                                          uint64_t stream_ptr) {
    std::lock_guard<std::mutex> lock(elastic_mutex_);
    if (!elastic_enabled_ || destroyed_ || peer_rank < 0 ||
        peer_rank >= num_ranks || (peer_rank == rank && masked)) {
        throw std::invalid_argument("invalid elastic EP mask update");
    }
    if (!masked && !elastic_connected_ranks_[peer_rank]) {
        throw std::invalid_argument("cannot unmask an unconnected EP rank");
    }
    commit_staged_peers();
    elastic_active_ranks_host_[peer_rank] = masked ? 0 : 1;
    refresh_active_rank_bound();
    mooncake::update_mask_buffer(elastic_active_ranks_, peer_rank, masked,
                                 reinterpret_cast<cudaStream_t>(stream_ptr));
}

void MooncakeEpBuffer::query_mask_buffer(uint64_t output_ptr,
                                         uint64_t stream_ptr) const {
    if (!elastic_enabled_ || destroyed_ || output_ptr == 0) {
        throw std::invalid_argument("invalid elastic EP mask query");
    }
    mooncake::query_mask_buffer(elastic_active_ranks_, num_ranks,
                                reinterpret_cast<int32_t*>(output_ptr),
                                reinterpret_cast<cudaStream_t>(stream_ptr));
}

void MooncakeEpBuffer::clean_mask_buffer(uint64_t stream_ptr) {
    std::lock_guard<std::mutex> lock(elastic_mutex_);
    commit_staged_peers();
    if (!elastic_enabled_ || destroyed_) {
        throw std::runtime_error("elastic EP buffer is not configured");
    }
    for (int peer = 0; peer < num_ranks; ++peer) {
        const bool active = elastic_connected_ranks_[peer] != 0;
        elastic_active_ranks_host_[peer] = active ? 1 : 0;
    }
    refresh_active_rank_bound();
    CUDA_CHECK(cudaMemcpyAsync(
        elastic_active_ranks_, elastic_active_ranks_host_.data(),
        num_ranks * sizeof(int32_t), cudaMemcpyHostToDevice,
        reinterpret_cast<cudaStream_t>(stream_ptr)));
}

void MooncakeEpBuffer::barrier(int timeout_us, uint64_t stream_ptr) {
    if (!elastic_enabled_ || destroyed_) {
        throw std::runtime_error("elastic EP buffer is not configured");
    }
    const int64_t timeout_ticks =
        timeout_us == -1 ? -1 : (int64_t)clock_rate_khz * timeout_us / 1000;
    const int qps_per_rank = USE_QP_COUNT / num_ranks;
    mooncake::barrier(
        elastic_active_ranks_, barrier_send_count_, barrier_arrivals_,
        gdr_buffer,
        p2p_transport_ ? p2p_transport_->availableTablePtr() : nullptr,
        p2p_transport_ ? p2p_transport_->peerPtrsTablePtr() : nullptr,
        rdma_transport_ ? rdma_transport_->raddrsPtr() : nullptr,
        rdma_transport_ ? rdma_transport_->rkeysPtr() : nullptr,
        rdma_transport_ ? rdma_transport_->qpDevCtxsPtr() : nullptr, rank,
        active_rank_bound_, num_ranks, qps_per_rank,
        reinterpret_cast<cudaStream_t>(stream_ptr), timeout_ticks);
}

void MooncakeEpBuffer::disconnect_ranks(const std::vector<int>& peer_ranks) {
    if (!elastic_enabled_ || destroyed_) {
        throw std::runtime_error("elastic EP buffer is not configured");
    }
    if (peer_ranks.empty()) return;
    std::lock_guard<std::mutex> lock(elastic_mutex_);
    for (int peer : peer_ranks) {
        if (peer < 0 || peer >= num_ranks || peer == rank ||
            !elastic_connected_ranks_[peer]) {
            throw std::invalid_argument("invalid elastic EP disconnect rank");
        }
    }
    CUDA_CHECK(cudaDeviceSynchronize());
    for (int peer : peer_ranks) {
        elastic_active_ranks_host_[peer] = 0;
        mooncake::update_mask_buffer(elastic_active_ranks_, peer, true,
                                     comm_stream);
    }
    refresh_active_rank_bound();
    CUDA_CHECK(cudaStreamSynchronize(comm_stream));

    const int qps_per_rank = USE_QP_COUNT / num_ranks;
    for (int peer : peer_ranks) {
        if (elastic_staged_ranks_[peer] && p2p_transport_)
            p2p_transport_->discardStagedPeer(peer);
        if (rdma_transport_) {
            const int qp_offset = peer * qps_per_rank;
            if (rdma_transport_->resetQueuePairs(qp_offset, qps_per_rank,
                                                 comm_stream) != 0) {
                throw std::runtime_error("failed to reset disconnected EP QPs");
            }
            CUDA_CHECK(cudaMemsetAsync(
                static_cast<char*>(rdma_transport_->raddrsPtr()) +
                    peer * sizeof(uint64_t),
                0, sizeof(uint64_t), comm_stream));
            CUDA_CHECK(cudaMemsetAsync(
                static_cast<char*>(rdma_transport_->rkeysPtr()) +
                    peer * sizeof(uint32_t),
                0, sizeof(uint32_t), comm_stream));
        }
        if (elastic_committed_ranks_[peer] && p2p_enabled_ &&
            p2p_transport_->disconnectPeer(peer) != 0) {
            throw std::runtime_error("failed to disconnect elastic P2P slot");
        }
        CUDA_CHECK(cudaMemsetAsync(barrier_send_count_ + peer, 0,
                                   sizeof(int32_t), comm_stream));
        CUDA_CHECK(cudaMemsetAsync(barrier_arrivals_ + peer, 0, sizeof(int32_t),
                                   comm_stream));
        elastic_staged_ranks_[peer] = 0;
        elastic_committed_ranks_[peer] = 0;
        elastic_connected_ranks_[peer] = 0;
    }
    CUDA_CHECK(cudaStreamSynchronize(comm_stream));
}

std::tuple<uint64_t, size_t> MooncakeEpBuffer::get_local_buffer_descriptor()
    const {
    if (destroyed_ || gdr_buffer == nullptr) {
        throw std::runtime_error("EP buffer is not available");
    }
    const size_t control_bytes =
        elastic_enabled_ ? 2 * static_cast<size_t>(num_ranks) * sizeof(int32_t)
                         : 0;
    if (static_cast<size_t>(num_ep_buffer_bytes) < control_bytes) {
        throw std::runtime_error("invalid elastic EP buffer size");
    }
    return {reinterpret_cast<uint64_t>(gdr_buffer),
            static_cast<size_t>(num_ep_buffer_bytes) - control_bytes};
}

std::tuple<std::optional<EventHandle>, std::optional<std::function<void()>>>
MooncakeEpBuffer::dispatch(
    uint64_t x_ptr, uint64_t topk_idx_ptr, uint64_t active_ranks_ptr,
    int num_tokens, int hidden, int num_topk,
    int num_max_dispatch_tokens_per_rank, int num_experts, int timeout_us,
    bool use_fp8, uint64_t packed_recv_x_ptr, uint64_t packed_recv_x_scales_ptr,
    uint64_t packed_recv_count_ptr, uint64_t packed_recv_src_info_ptr,
    uint64_t packed_recv_layout_range_ptr, bool async, bool return_recv_hook,
    uint64_t compute_stream_ptr,
    uint64_t cumulative_local_expert_recv_stats_ptr) {
    const int capacity_ranks = this->num_ranks;
    int num_ranks = capacity_ranks;
    int qp_num_ranks = capacity_ranks;
    int capacity_experts = num_experts;
    if (elastic_enabled_) {
        EP_HOST_ASSERT(active_ranks_ptr ==
                       reinterpret_cast<uint64_t>(elastic_active_ranks_));
        EP_HOST_ASSERT(num_experts % num_experts_per_rank_ == 0);
        const int rank_bound = num_experts / num_experts_per_rank_;
        EP_HOST_ASSERT(rank_bound >= active_rank_bound_ &&
                       rank_bound <= capacity_ranks);
        num_ranks = rank_bound;
        capacity_experts = capacity_ranks * num_experts_per_rank_;
    }
    EP_HOST_ASSERT(num_experts % num_ranks == 0);
    EP_HOST_ASSERT(USE_QP_COUNT % qp_num_ranks == 0);
    EP_HOST_ASSERT(hidden % static_cast<int>(sizeof(int4)) == 0 &&
                   hidden % 128 == 0);
    EP_HOST_ASSERT(num_tokens <= num_max_dispatch_tokens_per_rank);

    auto num_scales = hidden / 128;
    int num_local_experts = num_experts / num_ranks;

    auto compute_stream_raw =
        reinterpret_cast<cudaStream_t>(compute_stream_ptr);
    const bool graph_capture = stream_is_capturing(compute_stream_raw);

    // Buffer control
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      qp_num_ranks, capacity_experts);
    const size_t required_bytes =
        layout.total_bytes +
        (elastic_enabled_
             ? 2 * static_cast<size_t>(capacity_ranks) * sizeof(int32_t)
             : 0);
    EP_HOST_ASSERT(required_bytes <= static_cast<size_t>(num_ep_buffer_bytes));
    int current_buffer_idx = buffer_idx;
    auto buffer = layout.buffers[current_buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];
    int shadow_slot;
    {
        std::lock_guard<std::mutex> lock(buffer_mutex);
        shadow_slot = dispatch_slots.take(packed_recv_src_info_ptr);
    }

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream, whose native
    // handle is allowed to be nullptr in CUDA/PyTorch.
    auto launch_stream =
        (return_recv_hook || graph_capture) ? compute_stream_raw : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook and not graph_capture)
        stream_wait(launch_stream, compute_stream_raw);

    // Allocate packed tensors
    void* x = reinterpret_cast<void*>(x_ptr);
    auto* topk_idx = reinterpret_cast<int64_t*>(topk_idx_ptr);
    auto* active_ranks = reinterpret_cast<int32_t*>(active_ranks_ptr);
    void* packed_recv_x = reinterpret_cast<void*>(packed_recv_x_ptr);
    auto* packed_recv_x_scales =
        reinterpret_cast<float*>(packed_recv_x_scales_ptr);
    auto* packed_recv_count = reinterpret_cast<int*>(packed_recv_count_ptr);
    auto* cumulative_local_expert_recv_stats =
        reinterpret_cast<int*>(cumulative_local_expert_recv_stats_ptr);
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
    EP_HOST_ASSERT(num_topk <= kTopkShadowMaxTopk);
    if (num_tokens > 0) {
        CUDA_CHECK(cudaMemcpyAsync(
            topk_shadow(workspace, shadow_slot, capacity_experts), topk_idx,
            static_cast<size_t>(num_tokens) * num_topk * sizeof(int64_t),
            cudaMemcpyDeviceToDevice, launch_stream));
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
    int active_qps_per_rank =
        active_qps_per_rank_for_ep(USE_QP_COUNT / qp_num_ranks,
                                   rdma_transport_ && rdma_transport_->isRoce(),
                                   active_qps_cap_, num_experts / num_ranks);
    auto launcher = [=](int phases) {
        mooncake::dispatch(
            packed_recv_x, packed_recv_x_scales, packed_recv_src_info,
            packed_recv_layout_range, packed_recv_count,
            cumulative_local_expert_recv_stats, active_ranks, gdr_buffer,
            buffer.rdma_send_signal_buffer, buffer.rdma_recv_signal_buffer,
            buffer.rdma_send_data_buffer, buffer.rdma_recv_data_buffer,
            raddrs_ptr, rkeys_ptr, qp_devctxs_ptr, nvlink_avail, ipc_ptrs, x,
            topk_idx, next_buffer.rdma_recv_signal_buffer, num_tokens, hidden,
            num_max_dispatch_tokens_per_rank, num_topk, num_experts, rank,
            num_ranks, qp_num_ranks, use_fp8, workspace, launch_stream,
            timeout_ticks, phases, active_qps_per_rank);
    };
    if (return_recv_hook &&
        (!graph_capture || !macaHostPhaseFenceCoversPeers())) {
        launcher(LOW_LATENCY_SEND_PHASE);
    } else if (graph_capture) {
        launcher(LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE);
    } else {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
        launcher(LOW_LATENCY_SEND_PHASE);
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
    } else if (not return_recv_hook and not graph_capture) {
        stream_wait(compute_stream_raw, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook)
        recv_hook = [=]() {
            if (graph_capture && macaHostPhaseFenceCoversPeers()) return;
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
    const int capacity_ranks = this->num_ranks;
    int num_ranks = capacity_ranks;
    const int qp_num_ranks = capacity_ranks;
    int capacity_experts = num_experts;
    if (elastic_enabled_) {
        EP_HOST_ASSERT(active_ranks_ptr ==
                       reinterpret_cast<uint64_t>(elastic_active_ranks_));
        EP_HOST_ASSERT(num_experts % num_experts_per_rank_ == 0);
        const int rank_bound = num_experts / num_experts_per_rank_;
        EP_HOST_ASSERT(rank_bound >= active_rank_bound_ &&
                       rank_bound <= capacity_ranks);
        num_ranks = rank_bound;
        capacity_experts = capacity_ranks * num_experts_per_rank_;
    }
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

    auto compute_stream_raw =
        reinterpret_cast<cudaStream_t>(compute_stream_ptr);
    const bool graph_capture = stream_is_capturing(compute_stream_raw);

    // Buffer control.  The metadata tensor is the opaque handle returned by
    // dispatch through the Python wrapper, so use it to pair this combine
    // with the dispatch that produced it.  A process-wide flip is incorrect
    // when TBO interleaves its two logical dispatchers on one native Buffer.
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      qp_num_ranks, capacity_experts);
    const size_t required_bytes =
        layout.total_bytes +
        (elastic_enabled_
             ? 2 * static_cast<size_t>(capacity_ranks) * sizeof(int32_t)
             : 0);
    EP_HOST_ASSERT(required_bytes <= static_cast<size_t>(num_ep_buffer_bytes));
    int shadow_slot;
    {
        std::lock_guard<std::mutex> lock(buffer_mutex);
        shadow_slot = dispatch_slots.find(src_info_ptr);
    }
    EP_HOST_ASSERT(shadow_slot >= 0 &&
                   "the combine handle must come from one of the last "
                   "kEpDispatchSlots dispatches on this buffer");
    int current_buffer_idx = buffer_idx;
    auto buffer = layout.buffers[current_buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream, whose native
    // handle is allowed to be nullptr in CUDA/PyTorch.
    auto launch_stream =
        (return_recv_hook || graph_capture) ? compute_stream_raw : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook and not graph_capture)
        stream_wait(launch_stream, compute_stream_raw);

    int64_t timeout_ticks =
        timeout_us == -1 ? -1
                         : (int64_t)clock_rate_khz * (int64_t)timeout_us / 1000;

    void* raddrs_ptr = rdma_transport_ ? rdma_transport_->raddrsPtr() : nullptr;
    void* rkeys_ptr = rdma_transport_ ? rdma_transport_->rkeysPtr() : nullptr;
    void* qp_devctxs_ptr =
        rdma_transport_ ? rdma_transport_->qpDevCtxsPtr() : nullptr;
    int32_t* nvlink_avail = p2p_transport_->availableTablePtr();
    void** ipc_ptrs = p2p_transport_->peerPtrsTablePtr();
    int active_qps_per_rank =
        active_qps_per_rank_for_ep(USE_QP_COUNT / qp_num_ranks,
                                   rdma_transport_ && rdma_transport_->isRoce(),
                                   active_qps_cap_, num_experts / num_ranks);

    // Kernel launch
    auto launcher = [=](int phases) {
        auto* kernel_topk_idx =
            topk_shadow(workspace, shadow_slot, capacity_experts);
        mooncake::combine(
            combined_x, active_ranks, gdr_buffer,
            buffer.rdma_send_signal_buffer, buffer.rdma_recv_signal_buffer,
            buffer.rdma_send_data_buffer, buffer.rdma_recv_data_buffer,
            raddrs_ptr, rkeys_ptr, qp_devctxs_ptr, nvlink_avail, ipc_ptrs, x,
            kernel_topk_idx, topk_weights, src_info, layout_range,
            next_buffer.rdma_recv_signal_buffer, num_combined_tokens, hidden,
            num_max_dispatch_tokens_per_rank, num_topk, num_experts, rank,
            num_ranks, qp_num_ranks, workspace, launch_stream, timeout_ticks,
            phases, zero_copy, active_qps_per_rank);
    };
    if (return_recv_hook &&
        (!graph_capture || !macaHostPhaseFenceCoversPeers())) {
        launcher(LOW_LATENCY_SEND_PHASE);
    } else if (graph_capture) {
        launcher(LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE);
    } else {
#ifdef MOONCAKE_EP_SPLIT_SEND_RECV
        launcher(LOW_LATENCY_SEND_PHASE);
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
    } else if (not return_recv_hook and not graph_capture) {
        stream_wait(compute_stream_raw, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook)
        recv_hook = [=]() {
            if (graph_capture && macaHostPhaseFenceCoversPeers()) return;
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
        rank, rdma_transport_->isRoce(), rdma_memory_region_.lkey, remote_addrs,
        remote_keys, flat_qpns, flat_lids, subnet_prefixes, interface_ids,
        active_ranks_mask);
    if (ret != 0) {
        ibgda_disabled_ = true;
        LOG(WARNING)
            << "[EP] IBGDA connectPeers failed, falling back to P2P-only path";
    }
}

std::vector<int32_t> MooncakeEpBuffer::get_ipc_handle() {
    if (!p2p_enabled_) return {};
    return p2p_transport_->exportIpcHandle(gdr_buffer);
}

void MooncakeEpBuffer::sync_nvlink_ipc_handles(
    const std::vector<std::vector<int32_t>>& remote_handles,
    const std::vector<int>& active_ranks_mask) {
    if (!p2p_enabled_) return;
    p2p_transport_->importPeerHandles(gdr_buffer, rank, num_ranks,
                                      remote_handles, active_ranks_mask);
}

}  // namespace mooncake
