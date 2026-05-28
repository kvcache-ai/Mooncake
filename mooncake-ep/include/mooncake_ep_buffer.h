#ifndef MOONCAKE_EP_BUFFER_H
#define MOONCAKE_EP_BUFFER_H

#ifdef MOONCAKE_EP_USE_MUSA
#include <ATen/musa/MUSAContext.h>
#include <musa_runtime.h>
#else
#include <ATen/cuda/CUDAContext.h>
#include <cuda_bf16.h>
#include <cuda.h>
#include <cuda_runtime.h>
#endif
#include <fstream>
#include <memory>
#include <tent/runtime/device_transport.h>
#include <mooncake_ep_api.cuh>
#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_event.h>
#include <mooncake_ep_exception.cuh>
#include <torch/torch.h>

namespace mooncake {

#ifdef MOONCAKE_EP_USE_TENT
inline constexpr int MAX_QP_COUNT = tent::kIbGdaMaxQueuePairs;
#endif

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
    // Device info and communication
    int device_id;
    int rank, num_ranks;
    int clock_rate_khz;

    // MXA Buffer
    int buffer_idx{};
    int64_t num_ep_buffer_bytes;
    void* gdr_buffer = nullptr;

    // IBGDA control buffer
    static constexpr size_t CTRL_BUF_SIZE = 1024ULL * 1024 * 1024;  // 1024 MiB
    void* ctrl_buf = nullptr;
    void* raddrs = nullptr;
    void* rkeys = nullptr;
    void* qp_devctxs = nullptr;
    std::string device_name;
    bool is_roce_ = false;
    bool ibgda_disabled_ = false;
    int gid_index_ = -1;  // Dynamically discovered GID index
#ifdef MOONCAKE_EP_USE_MUSA
    int USE_QP_COUNT = 1;  // MUSA: no QPs, but need a non-zero value
#else
    int USE_QP_COUNT = MAX_QP_COUNT;
#endif

    // Unified device transport — owns all transport state (IBGDA, NVLink, or
    // MTLink).  EP never directly accesses transport-specific types; all
    // operations go through the DeviceTransport interface.
    //
    // On CUDA: transport_ is the NVLink transport (P2P); ibgda_transport_ is
    // the IBGDA transport (RDMA).  Both are needed because the kernel uses
    // both paths simultaneously.
    // On MUSA: transport_ is the MTLink transport (P2P); no ibgda_transport_.
    std::unique_ptr<tent::DeviceTransport> transport_;
#ifndef MOONCAKE_EP_USE_MUSA
    std::unique_ptr<tent::DeviceTransport> ibgda_transport_;
#endif

    // Fabric memory (MNNVL)
    bool use_fabric_mem_ = false;
#ifndef MOONCAKE_EP_USE_MUSA
    CUmemGenericAllocationHandle fabric_mem_handle_{};
#endif
    size_t fabric_alloc_size_ = 0;

    // Stream for communication
#ifdef MOONCAKE_EP_USE_MUSA
    at::musa::MUSAStream comm_stream;
#else
    at::cuda::CUDAStream comm_stream;
#endif

    // Workspace
    void* workspace = nullptr;

   public:
    MooncakeEpBuffer(int rank, int num_ranks, int64_t num_ep_buffer_bytes,
                     std::string device_name);

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

    int init_ibgda();

    void refresh_tent_ibgda_context();

    bool ibgda_disabled() { return ibgda_disabled_; }

    bool is_roce() { return is_roce_; }

    // Decide whether EP can safely run CUDA kernels ("fast-path").
    //
    // There are two independent ways EP kernels can work:
    // - IBGDA RDMA path: requires successful IBGDA init (qps/mr/etc).
    // - NVLink P2P+IPC path: requires full P2P+IPC across ranks on the same
    // node.
    //
    // IMPORTANT INVARIANT:
    // If `allPeersAccessible() == true`, `sync_nvlink_ipc_handles()` guarantees
    // `availableTablePtr()[dst_rank] == 1` for every rank pair, so the CUDA
    // kernels will never take the IBGDA branch and therefore do NOT require
    // `qps`.
    bool use_fast_path() {
        if (!ibgda_disabled_) {
            return true;  // IBGDA available
        }
        // IBGDA disabled: only allow fast-path if we can rely on NVLink
        // P2P+IPC.
        bool p2p_all = transport_ && transport_->allPeersAccessible();
        if (!p2p_all) {
            LOG(WARNING) << "Failed to initialize IBGDA. "
                         << "Using fallback implementation. "
                         << "Performance will be degraded.";
        }
        return p2p_all;
    }

    bool update_local_qpns();

    void sync_ib(const std::vector<int64_t>& remote_addrs,
                 const std::vector<int32_t>& remote_keys,
                 const std::vector<int32_t>& remote_qpns,
                 const std::vector<int32_t>& remote_lids,
                 const std::vector<int>& active_ranks_mask);

    void sync_roce(const std::vector<int64_t>& remote_addrs,
                   const std::vector<int32_t>& remote_keys,
                   const std::vector<int32_t>& remote_qpns,
                   const std::vector<int64_t>& subnet_prefixes,
                   const std::vector<int64_t>& interface_ids,
                   const std::vector<int>& active_ranks_mask);

    void sync_ibgda_peers(
        const std::vector<int64_t>& remote_addrs,
        const std::vector<int32_t>& remote_keys,
        const std::vector<std::vector<int32_t>>& peer_qpns,
        const std::vector<std::vector<int32_t>>& peer_lids,
        const std::vector<int64_t>& subnet_prefixes,
        const std::vector<int64_t>& interface_ids,
        const std::vector<int>& active_ranks_mask);

    std::tuple<int64_t, int32_t> get_mr_info() {
#ifndef MOONCAKE_EP_USE_MUSA
        if (ibgda_transport_) {
            auto metadata = ibgda_transport_->localMetadata();
            return {metadata.raddr, metadata.rkey};
        }
#endif
        return {(int64_t)0, (int32_t)0};
    }

    std::tuple<int64_t, int64_t> get_gid() {
#ifndef MOONCAKE_EP_USE_MUSA
        if (ibgda_transport_) {
            auto metadata = ibgda_transport_->localMetadata();
            return {metadata.subnet_prefix, metadata.interface_id};
        }
#endif
        return {(int64_t)0, (int64_t)0};
    }

    std::vector<int32_t> get_local_qpns() {
#ifndef MOONCAKE_EP_USE_MUSA
        if (ibgda_transport_) {
            return ibgda_transport_->localMetadata().qpns;
        }
#endif
        return {};
    }

    std::vector<int32_t> get_local_lids() {
#ifndef MOONCAKE_EP_USE_MUSA
        if (ibgda_transport_) {
            return ibgda_transport_->localMetadata().lids;
        }
#endif
        return {};
    }

    std::tuple<int32_t, int32_t, int32_t, int64_t, int64_t, int64_t>
    get_tent_ibgda_context_info() {
#ifndef MOONCAKE_EP_USE_MUSA
        if (!ibgda_transport_) return {0, 0, 0, 0, 0, 0};
        auto* ctx = ibgda_transport_->deviceContextPtr();
        auto abi = ibgda_transport_->deviceContextAbi();
        if (abi == tent::DeviceTransport::DeviceContextAbi::kIbGda) {
            auto* ibgda_ctx = static_cast<const tent::IbGdaDeviceContext*>(ctx);
            return {static_cast<int32_t>(ibgda_ctx->abi_version),
                    static_cast<int32_t>(ibgda_ctx->num_ranks),
                    static_cast<int32_t>(ibgda_ctx->num_qps),
                    reinterpret_cast<int64_t>(ibgda_ctx->raddrs),
                    reinterpret_cast<int64_t>(ibgda_ctx->rkeys),
                    reinterpret_cast<int64_t>(ibgda_ctx->qp_devctxs)};
        }
#endif
        return {0, 0, 0, 0, 0, 0};
    }

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
