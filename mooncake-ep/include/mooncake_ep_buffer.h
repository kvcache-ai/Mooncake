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
#ifdef MOONCAKE_EP_USE_TENT
#ifdef MOONCAKE_EP_USE_MUSA
#include <tent/device/mtlink_musa.cuh>
#include <tent/device/mtlink.h>
#else
#include <tent/device/ibgda.h>
#include <tent/device/nvlink.h>
#endif
#else
#include <tent/transport/ibgda/detail/memheap.h>
#include <tent/transport/ibgda/detail/mlx5gda.h>
#endif
#include <mooncake_ep_api.cuh>
#include <mooncake_ep_configs.cuh>
#include <mooncake_ep_event.h>
#include <mooncake_ep_exception.cuh>
#include <tent/runtime/device_resources.h>
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

    // IBGDA (not available on MUSA)
    static constexpr size_t CTRL_BUF_SIZE = 1024ULL * 1024 * 1024;  // 1024 MiB
    void* ctrl_buf = nullptr;
#ifndef MOONCAKE_EP_USE_TENT
    // RDMA memory region for `gdr_buffer`. Must be nullptr when IBGDA init
    // fails.
    ibv_mr* mr = nullptr;
    std::vector<mlx5gda_qp*> qps;
    ibv_gid gid;
#endif
    void* raddrs = nullptr;
    void* rkeys = nullptr;
    void* qp_devctxs = nullptr;
#ifndef MOONCAKE_EP_USE_MUSA
    tent::IbGdaDeviceContext tent_ibgda_ctx_;
#endif
    std::string device_name;
    bool is_roce_ = false;
    bool ibgda_disabled_ = false;
    int gid_index_ = -1;  // Dynamically discovered GID index
#ifdef MOONCAKE_EP_USE_MUSA
    int USE_QP_COUNT = 1;  // MUSA: no QPs, but need a non-zero value
#else
    int USE_QP_COUNT = MAX_QP_COUNT;
#endif

#ifndef MOONCAKE_EP_USE_TENT
    mlx5dv_devx_umem* ctrl_buf_umem = nullptr;
    ibv_pd* pd = nullptr;
    mlx5dv_pd mpd = {};
    memheap* ctrl_buf_heap = nullptr;
#endif
#ifdef MOONCAKE_EP_USE_TENT
#ifndef MOONCAKE_EP_USE_MUSA
    std::unique_ptr<tent::IbGdaDeviceTransport> tent_ibgda_transport_;
#endif
#endif

    // Fabric memory (MNNVL)
    bool use_fabric_mem_ = false;
#ifndef MOONCAKE_EP_USE_MUSA
    CUmemGenericAllocationHandle fabric_mem_handle_{};
#endif
    size_t fabric_alloc_size_ = 0;

    // NVLink/MTLink P2P
    int32_t* nvlink_available = nullptr;
#ifndef MOONCAKE_EP_USE_TENT
    void** ipc_peer_ptrs_host = nullptr;
#endif
    void** ipc_peer_ptrs = nullptr;
    bool p2p_ipc_all_enabled_ = false;
#if defined(MOONCAKE_EP_USE_TENT) && defined(MOONCAKE_EP_USE_MUSA)
    tent::MtLinkDeviceContext tent_mtlink_ctx_;
    std::unique_ptr<tent::MtLinkDeviceTransport> tent_mtlink_transport_;
#elif defined(MOONCAKE_EP_USE_TENT)
    tent::NvLinkDeviceContext tent_nvlink_ctx_;
    std::unique_ptr<tent::NvLinkDeviceTransport> tent_nvlink_transport_;
#endif

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

    // Decide whether EP can safely run CUDA kernels (\"fast-path\").
    //
    // There are two independent ways EP kernels can work:
    // - IBGDA RDMA path: requires successful IBGDA init (qps/mr/etc).
    // - NVLink P2P+IPC path: requires full P2P+IPC across ranks on the same
    // node.
    //
    // IMPORTANT INVARIANT:
    // If `p2p_ipc_all_enabled_ == true`, `sync_nvlink_ipc_handles()` guarantees
    // `nvlink_available[dst_rank] == 1` for every rank pair, so the CUDA
    // kernels will never take the IBGDA branch and therefore do NOT require
    // `qps`.
    bool use_fast_path() {
        if (!ibgda_disabled_) {
            return true;  // IBGDA available
        }
        // IBGDA disabled: only allow fast-path if we can rely on NVLink
        // P2P+IPC.
        if (!p2p_ipc_all_enabled_) {
            LOG(WARNING) << "Failed to initialize IBGDA. "
                         << "Using fallback implementation. "
                         << "Performance will be degraded.";
        }
        return p2p_ipc_all_enabled_;
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
#if defined(MOONCAKE_EP_USE_TENT) && !defined(MOONCAKE_EP_USE_MUSA)
        auto metadata = tent_ibgda_transport_->localMetadata();
        return {metadata.raddr, metadata.rkey};
#else
        return {(int64_t)0, (int32_t)0};
#endif
    }

    std::tuple<int64_t, int64_t> get_gid() {
#if defined(MOONCAKE_EP_USE_TENT) && !defined(MOONCAKE_EP_USE_MUSA)
        auto metadata = tent_ibgda_transport_->localMetadata();
        return {metadata.subnet_prefix, metadata.interface_id};
#else
        return {(int64_t)0, (int64_t)0};
#endif
    }

    std::vector<int32_t> get_local_qpns() {
#if defined(MOONCAKE_EP_USE_TENT) && !defined(MOONCAKE_EP_USE_MUSA)
        return tent_ibgda_transport_->localMetadata().qpns;
#else
        return {};
#endif
    }

    std::vector<int32_t> get_local_lids() {
#if defined(MOONCAKE_EP_USE_TENT) && !defined(MOONCAKE_EP_USE_MUSA)
        return tent_ibgda_transport_->localMetadata().lids;
#else
        return {};
#endif
    }

    std::tuple<int32_t, int32_t, int32_t, int64_t, int64_t, int64_t>
    get_tent_ibgda_context_info() {
#ifndef MOONCAKE_EP_USE_MUSA
        return {static_cast<int32_t>(tent_ibgda_ctx_.abi_version),
                static_cast<int32_t>(tent_ibgda_ctx_.num_ranks),
                static_cast<int32_t>(tent_ibgda_ctx_.num_qps),
                reinterpret_cast<int64_t>(tent_ibgda_ctx_.raddrs),
                reinterpret_cast<int64_t>(tent_ibgda_ctx_.rkeys),
                reinterpret_cast<int64_t>(tent_ibgda_ctx_.qp_devctxs)};
#else
        return {0, 0, 0, 0, 0, 0};
#endif
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
