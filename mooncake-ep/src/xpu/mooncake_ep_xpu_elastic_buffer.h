// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <sycl/sycl.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "mooncake_ep_xpu_buffer.h"
#include "xpu_ep_kernels.h"
#include "xpu_ipc.h"

namespace mooncake {
namespace xpu_ep {

// Intel XPU implementation of the native ElasticBuffer (DeepEP V2 API) that
// mooncake_elastic_buffer.ElasticBuffer drives, with the CUDA methods' names
// and arguments. Peers on the node are reached over Level Zero IPC; ranks on
// other nodes, when built with the verbs path, through a host proxy's RDMA
// writes, as in MooncakeEpXpuBuffer. Either way dispatch and combine are
// flat, over all ranks: there is no IBGDA, NCCL or hierarchical (scale-out)
// mode, and those arguments are rejected rather than ignored. The constructor
// additionally takes the torch.xpu stream (`sycl_queue`) to allocate on.
//
// The kernels run on the caller's stream. Setting
// MOONCAKE_EP_XPU_ASYNC_WITH_COMPUTE_STREAM_ENABLE (experimental, off by
// default) runs them on a queue of the buffer's own instead, as CUDA runs
// them on a communication stream; only launchQueue, finish and maxWgs differ
// between the two.
class MooncakeEpXpuElasticBuffer {
   public:
    MooncakeEpXpuElasticBuffer(
        int rank, int num_ranks, int64_t num_buffer_bytes,
        int64_t num_max_tokens_per_rank, int64_t hidden, int64_t num_topk,
        bool use_fp8_dispatch, bool deterministic, bool allow_hybrid_mode,
        bool allow_multiple_reduction, bool prefer_overlap_with_compute,
        int sl_idx, int num_allocated_qps, int num_cpu_timeout_secs,
        int num_gpu_timeout_secs, const std::string& transport,
        const std::vector<int32_t>& nccl_unique_id, uint64_t stream_ptr);
    ~MooncakeEpXpuElasticBuffer();
    MooncakeEpXpuElasticBuffer(const MooncakeEpXpuElasticBuffer&) = delete;
    MooncakeEpXpuElasticBuffer& operator=(const MooncakeEpXpuElasticBuffer&) =
        delete;

    void destroy();
    bool using_nccl() const { return false; }
    bool ibgda_disabled() const { return true; }
    bool use_fast_path() const;
    // (nodes, ranks per node), as init_rdma found them; (1, num_ranks)
    // without it.
    std::tuple<int, int> get_physical_domain_size() const;
    // Always flat: (scale-out, scale-up) = (1, num_ranks).
    std::tuple<int, int> get_logical_domain_size() const {
        return {1, num_ranks_};
    }

    // Two buffer halves, each sized for the larger of a dispatch and a
    // combine of this shape (see ElasticHalfLayout), with the regions for
    // ranks on other nodes if allow_hybrid_mode is set and num_ranks > 1.
    static int64_t calculate_buffer_size(int num_ranks,
                                         int64_t num_max_tokens_per_rank,
                                         int64_t hidden, int64_t num_topk,
                                         bool use_fp8_dispatch,
                                         bool allow_hybrid_mode,
                                         bool allow_multiple_reduction);

    // Ranks on other nodes, as in MooncakeEpXpuBuffer: init_rdma registers
    // the buffer with the nearest NIC and creates a queue pair for each rank
    // r with remote[r] set, returning what peers need to connect to it;
    // connect_rdma connects to the active ones from every rank's init_rdma
    // result. Both come before sync_nvlink_ipc_handles (the Level Zero IPC
    // import), which then leaves remote ranks unmapped and requires every
    // active one to be connected.
    static bool rdma_available();
    std::vector<int32_t> init_rdma(const std::vector<int>& remote);
    void connect_rdma(const std::vector<std::vector<int32_t>>& infos,
                      const std::vector<int>& active_ranks_mask);

    std::vector<int32_t> get_ipc_handle();
    void sync_nvlink_ipc_handles(
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask);

    // num_sms is accepted for API compatibility and ignored: the kernels size
    // their grids from the device's compute units. Both launch on the
    // compute stream; with async_with_compute_stream they return an event
    // the caller can wait on, else nothing.
    //
    // With the communication queue they launch there once the work queued on
    // the compute stream is done, and num_sms caps the work-groups of the
    // kernels whose grid is not fixed by the number of ranks or experts.
    // Without async_with_compute_stream the compute stream then waits for
    // them; with it, compute queued meanwhile can overlap them.
    std::optional<XpuEventHandle> dispatch(
        uint64_t x_ptr, int x_element_size, uint64_t sf_ptr, int num_tokens,
        int hidden, int num_sf_packs, int sf_token_stride, int sf_hidden_stride,
        uint64_t topk_idx_ptr, int num_topk, uint64_t topk_weights_ptr,
        uint64_t active_ranks_ptr, int num_experts, int num_max_tokens_per_rank,
        int expert_alignment, int num_sms, bool do_expand,
        bool async_with_compute_stream, uint64_t compute_stream_ptr,
        bool cached_mode, int num_recv_tokens,
        uint64_t psum_num_recv_tokens_per_scaleup_rank_ptr,
        uint64_t psum_num_recv_tokens_per_expert_ptr,
        uint64_t dst_buffer_slot_idx_ptr,
        uint64_t token_metadata_at_forward_ptr,
        uint64_t channel_linked_list_ptr, uint64_t recv_x_ptr,
        uint64_t recv_x_scales_ptr, uint64_t recv_topk_idx_ptr,
        uint64_t recv_topk_weights_ptr, uint64_t recv_src_metadata_ptr);

    // Only allow_multiple_reduction=True semantics: each destination rank's
    // row is summed unweighted, and topk_weights is not read.
    std::optional<XpuEventHandle> combine(
        uint64_t x_ptr, int num_input_tokens, int hidden, uint64_t topk_idx_ptr,
        int num_combined_tokens, int num_topk, uint64_t topk_weights_ptr,
        uint64_t psum_num_recv_tokens_per_scaleup_rank_ptr,
        uint64_t recv_src_metadata_ptr, uint64_t token_metadata_at_forward_ptr,
        uint64_t channel_linked_list_ptr, uint64_t active_ranks_ptr,
        int num_experts, int num_max_tokens_per_rank, bool do_expand,
        int num_sms, bool async_with_compute_stream,
        uint64_t compute_stream_ptr, uint64_t combined_x_ptr);

   private:
    struct Half {
        int idx;
        int* ready;
        int* next_ready;
        uint8_t* staging;
    };
    void checkAlive() const;
    sycl::queue& queueFor(uint64_t stream_ptr);
    // All need mu_ held. launchQueue orders the call after the previous ones
    // and returns the queue to launch on; finish records the launch and
    // returns what dispatch and combine return.
    void orderAfterLast(sycl::queue& q);
    Half takeHalf(size_t need_bytes);
    sycl::queue& launchQueue(sycl::queue& q);
    std::optional<XpuEventHandle> finish(sycl::queue& q, sycl::event done,
                                         bool async_with_compute_stream);
    int maxWgs(int num_sms) const;
    PeerView peerView() const;
    // Has the proxy write the puts `plan` returns once `after` completes.
    void submitPuts(sycl::event after,
                    std::function<std::vector<ProxyPut>()> plan);
    // Waits for the proxy's writes (with mu_ not held).
    void drainProxy();

    int rank_, num_ranks_;
    size_t half_stride_;
    int64_t timeout_ticks_;
    int max_wgs_;
    sycl::queue queue_;
    // The communication queue, set only with
    // MOONCAKE_EP_XPU_ASYNC_WITH_COMPUTE_STREAM_ENABLE. In order, so calls run
    // in issue order: they share the arrival counter and the buffer halves.
    std::optional<sycl::queue> comm_queue_;
    std::unique_ptr<XpuIpcBuffer> ipc_;
    uint8_t* workspace_ = nullptr;

    std::mutex mu_;
    int buffer_idx_ = 0;
    // The last kernel launched and its queue: every launch is ordered after
    // it, so calls run in issue order whichever streams they come from.
    sycl::event last_event_;
    std::optional<sycl::queue> last_queue_;

    // Ranks on other nodes, and those connected over RDMA.
    std::vector<int> remote_;
    std::vector<int> connected_;
    std::vector<int> rdma_ranks_;  // connected, in rank order
    bool hybrid_ = false;          // some rank is connected
    int* d_remote_ = nullptr;      // device copy of connected_
    // For each buffer half, what the send kernels sent to each rank: the
    // proxy reads it after the call has moved on to the other half.
    int* host_counts_ = nullptr;
#if MOONCAKE_EP_XPU_RDMA
    std::unique_ptr<XpuEpProxy> proxy_;
#endif
};

}  // namespace xpu_ep
}  // namespace mooncake
