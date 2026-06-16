#ifndef MOONCAKE_EP_ELASTIC_BUFFER_H
#define MOONCAKE_EP_ELASTIC_BUFFER_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <tuple>
#include <vector>

#include <mooncake_ep_buffer.h>

namespace mooncake {

struct ElasticLaunchContext;

struct ElasticTopology {
    int rank_idx = 0;
    int num_ranks = 1;
    int num_rdma_ranks = 1;
    int num_nvlink_ranks = 1;
    int num_scaleout_ranks = 1;
    int num_scaleup_ranks = 1;
    int scaleout_rank_idx = 0;
    int scaleup_rank_idx = 0;
    bool hybrid_enabled = false;
};

struct ElasticConfig {
    int64_t num_max_tokens_per_rank = 0;
    int64_t hidden = 0;
    int64_t num_topk = 0;
    bool use_fp8_dispatch = false;
    bool deterministic = false;
    bool allow_hybrid_mode = true;
    bool allow_multiple_reduction = true;
    bool prefer_overlap_with_compute = true;
    int sl_idx = 3;
    int num_allocated_qps = 0;
    int num_cpu_timeout_secs = 300;
    int num_gpu_timeout_secs = 100;
};

struct ElasticNativeHandle {
    bool do_expand = false;
    int num_experts = 0;
    int expert_alignment = 1;
    int num_max_tokens_per_rank = 0;
    int num_sms = 0;
    torch::Tensor topk_idx;
    torch::Tensor psum_num_recv_tokens_per_scaleup_rank;
    torch::Tensor psum_num_recv_tokens_per_expert;
    torch::Tensor recv_src_metadata;
    torch::Tensor recv_layout_range;
    torch::Tensor dst_buffer_slot_idx;
    std::optional<torch::Tensor> token_metadata_at_forward;
    std::optional<torch::Tensor> channel_linked_list;
    std::vector<int> num_recv_tokens_per_expert_list;
};

struct ElasticDispatchOutput {
    torch::Tensor recv_x;
    std::optional<torch::Tensor> recv_x_scales;
    std::optional<torch::Tensor> recv_topk_idx;
    std::optional<torch::Tensor> recv_topk_weights;
    ElasticNativeHandle handle;
    std::optional<EventHandle> event;
};

struct ElasticCombineOutput {
    torch::Tensor combined_x;
    std::optional<torch::Tensor> combined_topk_weights;
    std::optional<EventHandle> event;
};

class MooncakeElasticBuffer {
   public:
    MooncakeElasticBuffer(int rank, int num_ranks, int64_t num_buffer_bytes,
                          int64_t num_max_tokens_per_rank, int64_t hidden,
                          int64_t num_topk, bool use_fp8_dispatch,
                          bool deterministic, bool allow_hybrid_mode,
                          bool allow_multiple_reduction,
                          bool prefer_overlap_with_compute, int sl_idx,
                          int num_allocated_qps, int num_cpu_timeout_secs,
                          int num_gpu_timeout_secs);

    ~MooncakeElasticBuffer();

    static int64_t calculate_buffer_size(int num_ranks,
                                         int64_t num_max_tokens_per_rank,
                                         int64_t hidden, int64_t num_topk,
                                         bool use_fp8_dispatch,
                                         bool allow_hybrid_mode,
                                         bool allow_multiple_reduction);

    std::tuple<int, int> get_physical_domain_size() const;
    std::tuple<int, int> get_logical_domain_size() const;
    int get_theoretical_num_sms(int num_experts, int num_topk) const;

    ElasticDispatchOutput dispatch(
        const torch::Tensor& x, const std::optional<torch::Tensor>& sf,
        const torch::Tensor& topk_idx,
        const std::optional<torch::Tensor>& topk_weights,
        torch::Tensor& active_ranks, int num_experts,
        int num_max_tokens_per_rank, int expert_alignment, int num_sms,
        bool do_expand, bool do_cpu_sync, bool async_with_compute_stream,
        const std::optional<ElasticNativeHandle>& cached_handle = std::nullopt);

    ElasticCombineOutput combine(
        const torch::Tensor& x, const ElasticNativeHandle& handle,
        const std::optional<torch::Tensor>& topk_weights,
        torch::Tensor& active_ranks, int num_sms, bool async_with_compute_stream,
        const std::optional<torch::Tensor>& out);

    MooncakeEpBuffer& native_buffer() { return *native_buffer_; }

    bool ibgda_disabled() const { return native_buffer_->ibgda_disabled(); }
    bool use_fast_path() { return native_buffer_->use_fast_path(); }
    void update_local_qpns() { native_buffer_->update_local_qpns(); }
    bool is_roce() const { return native_buffer_->is_roce(); }
    void sync_ibgda_peers(
        const std::vector<int64_t>& remote_addrs,
        const std::vector<int32_t>& remote_keys,
        const std::vector<std::vector<int32_t>>& peer_qpns,
        const std::vector<std::vector<int32_t>>& peer_lids,
        const std::vector<int64_t>& subnet_prefixes,
        const std::vector<int64_t>& interface_ids,
        const std::vector<int>& active_ranks_mask) {
        native_buffer_->sync_ibgda_peers(remote_addrs, remote_keys, peer_qpns,
                                         peer_lids, subnet_prefixes,
                                         interface_ids, active_ranks_mask);
    }
    std::tuple<int64_t, int32_t> get_mr_info() {
        return native_buffer_->get_mr_info();
    }
    std::tuple<int64_t, int64_t> get_gid() { return native_buffer_->get_gid(); }
    std::vector<int32_t> get_local_qpns() {
        return native_buffer_->get_local_qpns();
    }
    std::vector<int32_t> get_local_lids() {
        return native_buffer_->get_local_lids();
    }
    std::vector<int32_t> get_ipc_handle() {
        return native_buffer_->get_ipc_handle();
    }
    void sync_nvlink_ipc_handles(
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask) {
        native_buffer_->sync_nvlink_ipc_handles(remote_handles,
                                                active_ranks_mask);
    }

   private:
    ElasticConfig config_;
    ElasticTopology topology_;
    std::unique_ptr<MooncakeEpBuffer> native_buffer_;
    int64_t host_workspace_bytes_ = 0;
    void* host_workspace_ = nullptr;
    void* mapped_host_workspace_ = nullptr;

    static ElasticLaunchContext make_launch_context(
        MooncakeEpBuffer& buffer, const ElasticTopology& topology,
        void* mapped_host_workspace, int64_t timeout_cycles);
    static ElasticTopology discover_topology(int rank, int num_ranks,
                                             bool allow_hybrid_mode);
};

}  // namespace mooncake

#endif  // MOONCAKE_EP_ELASTIC_BUFFER_H
