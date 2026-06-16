#ifndef MOONCAKE_EP_BUFFER_H
#define MOONCAKE_EP_BUFFER_H

#include <ATen/cuda/CUDAContext.h>
#include <cuda_bf16.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <memory>
#include <mooncake_ep_api.cuh>
#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_event.h>
#include <mooncake_ep_exception.cuh>
#include <torch/torch.h>
#include <transport/device/device_transport.h>

namespace mooncake {

class TransferEngine;
class MooncakeElasticBuffer;

// MAX_QP_COUNT is defined in mooncake_ep_configs.cuh (shared with kernel code).

struct BufferLayout {
    int* rdma_send_signal_buffer;
    int* rdma_recv_signal_buffer;
    void* rdma_send_data_buffer;
    void* rdma_recv_data_buffer;
};

struct BufferPair {
    size_t total_bytes = 0;
    BufferLayout buffers[2];

    template <typename out_ptr_t = void*, typename count_ptr_t = uint8_t*,
              typename in_ptr_t = void*>
    static out_ptr_t advance(const in_ptr_t& ptr, size_t count) {
        return reinterpret_cast<out_ptr_t>(reinterpret_cast<count_ptr_t>(ptr) +
                                           count);
    }

    BufferPair(void* rdma_buffer, int num_max_dispatch_tokens_per_rank,
               int hidden, int num_ranks, int num_experts) {
        size_t signaling_buffer_bytes = num_experts * sizeof(int);
        size_t send_recv_buffer_bytes =
            num_experts * num_max_dispatch_tokens_per_rank *
            (2 * sizeof(int4) + hidden * sizeof(nv_bfloat16));
        for (int i = 0; i < 2; ++i) {
            size_t rdma_base_offset = total_bytes +
                                      2 * i * signaling_buffer_bytes +
                                      2 * i * send_recv_buffer_bytes;
            buffers[i] = {
                advance<int*>(rdma_buffer, rdma_base_offset),
                advance<int*>(rdma_buffer,
                              rdma_base_offset + signaling_buffer_bytes),
                advance<int*>(rdma_buffer,
                              rdma_base_offset + 2 * signaling_buffer_bytes),
                advance<int*>(rdma_buffer, rdma_base_offset +
                                               2 * signaling_buffer_bytes +
                                               send_recv_buffer_bytes),
            };
        }
        total_bytes += 4 * signaling_buffer_bytes + 4 * send_recv_buffer_bytes;
    }
};

struct MooncakeEpBuffer {
   private:
    friend class MooncakeElasticBuffer;

    // Device info and communication
    int device_id;
    int rank, num_ranks;
    int clock_rate_khz;

    // GDR buffer — owned by p2p_transport_
    int buffer_idx{};
    int64_t num_ep_buffer_bytes;
    void* gdr_buffer = nullptr;

    // Device transports — own all platform-specific state.
    // p2p_transport_: NVLink intra-node P2P.
    // rdma_transport_: IBGDA inter-node RDMA.  nullptr when IBGDA unavailable.
    device::P2pTransport* p2p_transport_ = nullptr;
    device::RdmaTransport* rdma_transport_ = nullptr;
    // When EP creates transports itself (no engine provided), ownership lives
    // in these unique_ptrs.  When an engine is provided, the engine owns them
    // and these remain null.
    std::unique_ptr<device::P2pTransport> owned_p2p_transport_;
    std::unique_ptr<device::RdmaTransport> owned_rdma_transport_;

    bool ibgda_disabled_ = false;

    int USE_QP_COUNT = MAX_QP_COUNT;

    // Stream for communication
    at::cuda::CUDAStream comm_stream;

    // Workspace
    void* workspace = nullptr;

   public:
    // If engine is provided, EP gets P2pTransport/RdmaTransport from it
    // (engine owns the transports).  Otherwise EP creates its own via the
    // factory functions (EP owns them via owned_p2p_transport_ etc.).
    MooncakeEpBuffer(int rank, int num_ranks, int64_t num_ep_buffer_bytes,
                     TransferEngine* engine = nullptr);

    ~MooncakeEpBuffer() noexcept(false);

    std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor,
               torch::Tensor, torch::Tensor, std::optional<EventHandle>,
               std::optional<std::function<void()>>>
    dispatch(const torch::Tensor& x, const torch::Tensor& topk_idx,
             torch::Tensor& active_ranks, int num_max_dispatch_tokens_per_rank,
             int num_experts, int timeout_us, bool use_fp8, bool async,
             bool return_recv_hook);

    std::tuple<torch::Tensor, std::optional<EventHandle>,
               std::optional<std::function<void()>>>
    combine(const torch::Tensor& x, const torch::Tensor& topk_idx,
            const torch::Tensor& topk_weights, const torch::Tensor& src_info,
            const torch::Tensor& layout_range, torch::Tensor& active_ranks,
            int num_max_dispatch_tokens_per_rank, int num_experts,
            int timeout_us, bool zero_copy, bool async, bool return_recv_hook,
            const std::optional<torch::Tensor>& out);

    torch::Tensor get_next_combine_buffer(int num_max_dispatch_tokens_per_rank,
                                          int hidden, int num_experts);

    bool ibgda_disabled() const { return ibgda_disabled_; }

    bool is_roce() const {
        return rdma_transport_ && rdma_transport_->isRoce();
    }

    // Fast-path: IBGDA available, or all peers accessible via P2P.
    bool use_fast_path() {
        if (!ibgda_disabled_) return true;
        bool p2p_all = p2p_transport_ && p2p_transport_->allPeersAccessible();
        if (!p2p_all) {
            LOG(WARNING) << "IBGDA unavailable and P2P not fully accessible. "
                         << "Using fallback (degraded performance).";
        }
        return p2p_all;
    }

    // Recreate QPs (called when active_ranks changes).
    void update_local_qpns();

    // Connect IBGDA QPs to peers.  Unified entry point — handles both IB and
    // RoCE based on is_roce().
    void sync_ibgda_peers(const std::vector<int64_t>& remote_addrs,
                          const std::vector<int32_t>& remote_keys,
                          const std::vector<std::vector<int32_t>>& peer_qpns,
                          const std::vector<std::vector<int32_t>>& peer_lids,
                          const std::vector<int64_t>& subnet_prefixes,
                          const std::vector<int64_t>& interface_ids,
                          const std::vector<int>& active_ranks_mask);

    // Metadata accessors for Python-level bootstrap exchange.
    std::tuple<int64_t, int32_t> get_mr_info() {
        if (!rdma_transport_) return {0, 0};
        auto m = rdma_transport_->localMetadata();
        return {m.raddr, m.rkey};
    }

    std::tuple<int64_t, int64_t> get_gid() {
        if (!rdma_transport_) return {0, 0};
        auto m = rdma_transport_->localMetadata();
        return {m.subnet_prefix, m.interface_id};
    }

    std::vector<int32_t> get_local_qpns() {
        if (!rdma_transport_) return {};
        return rdma_transport_->localMetadata().qpns;
    }

    std::vector<int32_t> get_local_lids() {
        if (!rdma_transport_) return {};
        return rdma_transport_->localMetadata().lids;
    }

    // IPC handle for P2P (NVLink).
    std::vector<int32_t> get_ipc_handle();

    void sync_nvlink_ipc_handles(
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask);
};

inline size_t get_ep_buffer_size_hint(int num_max_dispatch_tokens_per_rank,
                                      int hidden, int num_ranks,
                                      int num_experts) {
    return BufferPair(nullptr, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts)
        .total_bytes;
}

}  // namespace mooncake

#endif  // MOONCAKE_EP_BUFFER_H
