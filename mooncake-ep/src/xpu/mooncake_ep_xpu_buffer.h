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

#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <tuple>
#include <vector>

#include "xpu_ep_common.h"
#include "xpu_ep_kernels.h"
#include "xpu_ipc.h"
// Set by the build when rdma-core has ibv_reg_dmabuf_mr.
#ifndef MOONCAKE_EP_XPU_RDMA
#define MOONCAKE_EP_XPU_RDMA 0
#endif
#if MOONCAKE_EP_XPU_RDMA
#include "xpu_ep_proxy.h"
#endif

namespace mooncake {
namespace xpu_ep {

// Stream pointers from Python are the `sycl::queue*` behind a torch.xpu
// stream (`torch.xpu.Stream.sycl_queue`).
class XpuEventHandle {
   public:
    explicit XpuEventHandle(uint64_t stream_ptr = 0);
    explicit XpuEventHandle(sycl::event event) : event_(std::move(event)) {}
    void current_stream_wait(uint64_t stream_ptr) const;
    void synchronize() { event_.wait(); }

   private:
    sycl::event event_;
};

// Intel XPU implementation of the native Mooncake EP Buffer. It exposes the
// same methods as the CUDA MooncakeEpBuffer so that mooncake_ep_buffer.Buffer
// drives both. Peers on the node are reached over Level Zero IPC; ranks on
// other nodes, when built with the verbs path, through a host proxy's RDMA
// writes (the hybrid path). There is no IBGDA, so ibgda_disabled() is always
// true.
class MooncakeEpXpuBuffer {
   public:
    using Hook = std::optional<std::function<void()>>;
    using Result = std::tuple<std::optional<XpuEventHandle>, Hook>;

    MooncakeEpXpuBuffer(int rank, int num_ranks, int64_t num_ep_buffer_bytes,
                        bool disable_p2p, uint64_t stream_ptr);
    ~MooncakeEpXpuBuffer();

    bool ibgda_disabled() const { return true; }
    bool p2p_enabled() const { return p2p_enabled_; }
    bool is_roce() const { return false; }
    bool use_fast_path() const;

    // The hybrid path. init_rdma registers the buffer with the nearest NIC
    // and creates a queue pair for each rank r with remote[r] set (ranks on
    // other nodes), returning what peers need to connect to it; connect_rdma
    // connects to the active ones from every rank's init_rdma result. Both
    // come before sync_nvlink_ipc_handles, which then leaves remote ranks
    // unmapped. Every rank must end up with the same ranks connected or none.
    static bool rdma_available();
    std::vector<int32_t> init_rdma(const std::vector<int>& remote);
    void connect_rdma(const std::vector<std::vector<int32_t>>& infos,
                      const std::vector<int>& active_ranks_mask);

    std::vector<int32_t> get_ipc_handle();
    void sync_nvlink_ipc_handles(
        const std::vector<std::vector<int32_t>>& remote_handles,
        const std::vector<int>& active_ranks_mask);

    Result dispatch(uint64_t x_ptr, uint64_t topk_idx_ptr,
                    uint64_t active_ranks_ptr, int num_tokens, int hidden,
                    int num_topk, int num_max_dispatch_tokens_per_rank,
                    int num_experts, int timeout_us, bool use_fp8,
                    uint64_t packed_recv_x_ptr,
                    uint64_t packed_recv_x_scales_ptr,
                    uint64_t packed_recv_count_ptr,
                    uint64_t packed_recv_src_info_ptr,
                    uint64_t packed_recv_layout_range_ptr, bool async,
                    bool return_recv_hook, uint64_t compute_stream_ptr,
                    uint64_t topk_weights_ptr, bool with_topk_weights);

    Result combine(uint64_t x_ptr, uint64_t topk_idx_ptr,
                   uint64_t topk_weights_ptr, uint64_t src_info_ptr,
                   uint64_t layout_range_ptr, uint64_t active_ranks_ptr,
                   int num_local_experts, int num_combined_tokens, int hidden,
                   int num_topk, int num_max_dispatch_tokens_per_rank,
                   int num_experts, int timeout_us, bool zero_copy,
                   uint64_t combined_x_ptr, bool async, bool return_recv_hook,
                   uint64_t compute_stream_ptr);

   private:
    sycl::queue& queueFor(uint64_t stream_ptr);
    // Both need mu_ held.
    void orderAfterLast(sycl::queue& q);
    RecvTable recvTable(int slot);
    int64_t timeoutTicks(int timeout_us) const;
    int64_t* topkShadow(int slot) const;
    int32_t* slotShadow(int slot) const;
    void checkShape(int hidden, int num_topk,
                    int num_max_dispatch_tokens_per_rank,
                    int num_experts) const;
    PeerView peerView() const;
    // Waits for the proxy's writes (with mu_ not held).
    void drainProxy();

    int rank_, num_ranks_;
    int64_t num_ep_buffer_bytes_;
    bool p2p_enabled_;
    sycl::queue queue_;
    std::unique_ptr<XpuIpcBuffer> ipc_;
    uint8_t* workspace_ = nullptr;
    int64_t clock_mhz_;
    int max_wgs_;

    std::mutex mu_;
    int buffer_idx_ = 0;
    // The last kernel launched and its queue: every launch is ordered after
    // it, so calls run in issue order whichever streams they come from.
    sycl::event last_event_;
    std::optional<sycl::queue> last_queue_;
    // Each dispatch's slot for its top-k snapshot, shared with the CUDA
    // buffer, and whether that dispatch had top-k weights, so its combine
    // reads pre-reduced rows.
    DispatchSlots dispatch_slots_;
    std::array<bool, kTopkShadowSlots> slot_reduced_{};
    // recv_table_ints(num_ranks) per slot, allocated by the first dispatch
    // given top-k weights.
    int32_t* recv_tables_ = nullptr;

    // The hybrid path: ranks on other nodes, and whether any is connected.
    std::vector<int> remote_;
    std::vector<int> rdma_ranks_;  // connected, in rank order
    bool rdma_ok_ = false;         // connect_rdma succeeded
    bool hybrid_ = false;          // some rank is connected
    int* d_remote_ = nullptr;      // device copy of the connected ranks
    int* host_counts_ = nullptr;   // two halves of kHostCountInts
#if MOONCAKE_EP_XPU_RDMA
    std::unique_ptr<XpuEpProxy> proxy_;
#endif
};

}  // namespace xpu_ep
}  // namespace mooncake
