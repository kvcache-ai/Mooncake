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

#include "mooncake_ep_xpu_buffer.h"

#include <glog/logging.h>

#include <string>

#include "xpu_ep_common.h"
#include "xpu_ep_kernels.h"

namespace mooncake {
namespace xpu_ep {

namespace {

// Workspace (not shared with peers): per-expert slot counters, the two
// send-kernel arrival counters and the per-rank counters, then per dispatch
// slot the top-k snapshot and the packed entry of every pick.
constexpr size_t kCounterBytes = 64 * 1024;
constexpr int kMaxExperts = kCounterBytes / sizeof(int);
constexpr size_t kDoneOffset = kCounterBytes;
constexpr size_t kRankCounterOffset = kDoneOffset + kLineBytes;
constexpr int kMaxRanks = (4096 - kLineBytes) / sizeof(int);
constexpr size_t kShadowOffset = kCounterBytes + 4096;
constexpr size_t kSlotShadowOffset =
    kShadowOffset + kTopkShadowSlots * kTopkShadowBytes;
constexpr size_t kWorkspaceBytes =
    kSlotShadowOffset + kTopkShadowSlots * kSlotShadowBytes;

sycl::queue* asQueue(uint64_t stream_ptr) {
    XPU_EP_CHECK(stream_ptr != 0,
                 "a torch.xpu stream (sycl_queue) is required");
    return reinterpret_cast<sycl::queue*>(stream_ptr);
}

}  // namespace

XpuEventHandle::XpuEventHandle(uint64_t stream_ptr) {
    if (stream_ptr) event_ = asQueue(stream_ptr)->ext_oneapi_submit_barrier();
}

void XpuEventHandle::current_stream_wait(uint64_t stream_ptr) const {
    asQueue(stream_ptr)->ext_oneapi_submit_barrier({event_});
}

MooncakeEpXpuBuffer::MooncakeEpXpuBuffer(int rank, int num_ranks,
                                         int64_t num_ep_buffer_bytes,
                                         bool disable_p2p, uint64_t stream_ptr)
    : rank_(rank),
      num_ranks_(num_ranks),
      num_ep_buffer_bytes_(num_ep_buffer_bytes),
      p2p_enabled_(!disable_p2p),
      queue_(*asQueue(stream_ptr)) {
    XPU_EP_CHECK(num_ep_buffer_bytes > 0, "EP buffer size must be positive");
    XPU_EP_CHECK(num_ranks > 0 && num_ranks <= kMaxRanks,
                 "too many ranks for the XPU EP buffer");
    XPU_EP_CHECK(queue_.is_in_order(), "the torch.xpu stream must be in-order");
    ipc_ = std::make_unique<XpuIpcBuffer>(
        queue_, static_cast<size_t>(num_ep_buffer_bytes), rank, num_ranks);
    workspace_ = sycl::malloc_device<uint8_t>(kWorkspaceBytes, queue_);
    XPU_EP_CHECK(workspace_ != nullptr, "workspace allocation failed");
    queue_.memset(workspace_, 0, kWorkspaceBytes).wait();

    const auto dev = queue_.get_device();
    // __spirv_ReadClockKHR counts GPU cycles (~2.4 GHz on B60, measured), so
    // a timeout in microseconds is clock_mhz ticks per microsecond, as the
    // CUDA path does with clock64() and the SM clock rate.
    clock_mhz_ = dev.get_info<sycl::info::device::max_clock_frequency>();
    max_wgs_ =
        static_cast<int>(dev.get_info<sycl::info::device::max_compute_units>());
}

MooncakeEpXpuBuffer::~MooncakeEpXpuBuffer() {
    // Calls may have run on other streams than queue_; the last one launched
    // completes after all of them.
    try {
        last_event_.wait();
        queue_.wait();
    } catch (...) {
    }
    if (workspace_) sycl::free(workspace_, queue_);
    if (recv_tables_) sycl::free(recv_tables_, queue_);
    ipc_.reset();
}

bool MooncakeEpXpuBuffer::use_fast_path() const {
    if (!p2p_enabled_) return false;
    if (!ipc_->allPeersAccessible()) {
        LOG(WARNING) << "[EP XPU] not every peer is reachable over Level Zero "
                        "IPC; using the fallback path (degraded performance)";
        return false;
    }
    return true;
}

std::vector<int32_t> MooncakeEpXpuBuffer::get_ipc_handle() {
    return ipc_->exportHandle();
}

void MooncakeEpXpuBuffer::sync_nvlink_ipc_handles(
    const std::vector<std::vector<int32_t>>& remote_handles,
    const std::vector<int>& active_ranks_mask) {
    // Remapping peers closes the old mappings, which launched kernels may
    // still be reading.
    {
        std::lock_guard<std::mutex> lock(mu_);
        last_event_.wait();
    }
    queue_.wait();
    ipc_->importPeers(remote_handles, active_ranks_mask);
}

sycl::queue& MooncakeEpXpuBuffer::queueFor(uint64_t stream_ptr) {
    sycl::queue& q = *asQueue(stream_ptr);
    XPU_EP_CHECK(q.get_context() == queue_.get_context() &&
                     q.get_device() == queue_.get_device(),
                 "the stream belongs to a different device than the buffer");
    XPU_EP_CHECK(q.is_in_order(), "the torch.xpu stream must be in-order");
    return q;
}

void MooncakeEpXpuBuffer::orderAfterLast(sycl::queue& q) {
    // Calls share the slot and arrival counters and the buffer halves, so
    // they must run in issue order, as on CUDA's single comm stream. On one
    // in-order stream that holds already; across streams, wait for the last
    // launch.
    if (last_queue_ && *last_queue_ != q)
        q.ext_oneapi_submit_barrier({last_event_});
    last_queue_ = q;
}

RecvTable MooncakeEpXpuBuffer::recvTable(int slot) {
    if (!recv_tables_) {
        recv_tables_ = sycl::malloc_device<int32_t>(
            kTopkShadowSlots * recv_table_ints(num_ranks_), queue_);
        XPU_EP_CHECK(recv_tables_ != nullptr,
                     "receive table allocation failed");
    }
    int32_t* base = recv_tables_ + slot * recv_table_ints(num_ranks_);
    return {base, base + num_ranks_,
            base + num_ranks_ +
                static_cast<size_t>(num_ranks_) * kTopkShadowMaxTokens};
}

int64_t MooncakeEpXpuBuffer::timeoutTicks(int timeout_us) const {
    return timeout_us < 0 ? -1 : clock_mhz_ * static_cast<int64_t>(timeout_us);
}

int64_t* MooncakeEpXpuBuffer::topkShadow(int slot) const {
    return reinterpret_cast<int64_t*>(workspace_ + kShadowOffset +
                                      slot * kTopkShadowBytes);
}

int32_t* MooncakeEpXpuBuffer::slotShadow(int slot) const {
    return reinterpret_cast<int32_t*>(workspace_ + kSlotShadowOffset +
                                      slot * kSlotShadowBytes);
}

void MooncakeEpXpuBuffer::checkShape(int hidden, int num_topk,
                                     int num_max_dispatch_tokens_per_rank,
                                     int num_experts) const {
    XPU_EP_CHECK(hidden > 0 && hidden % kNumPerChannels == 0,
                 "hidden must be a multiple of 128");
    XPU_EP_CHECK(num_topk > 0 && num_topk <= kTopkShadowMaxTopk,
                 "num_topk must be in [1, 32]");
    XPU_EP_CHECK(num_max_dispatch_tokens_per_rank > 0 &&
                     num_max_dispatch_tokens_per_rank <= kTopkShadowMaxTokens,
                 "num_max_dispatch_tokens_per_rank must be in [1, 1024]");
    XPU_EP_CHECK(num_experts > 0 && num_experts % num_ranks_ == 0 &&
                     num_experts <= kMaxExperts,
                 "num_experts must be a positive multiple of the EP size");
    const size_t need = get_ep_buffer_size_hint(
        num_max_dispatch_tokens_per_rank, hidden, num_ranks_, num_experts);
    XPU_EP_CHECK(need <= static_cast<size_t>(num_ep_buffer_bytes_),
                 "EP buffer too small for this shape; size it with "
                 "Buffer.get_ep_buffer_size_hint()");
}

MooncakeEpXpuBuffer::Result MooncakeEpXpuBuffer::dispatch(
    uint64_t x_ptr, uint64_t topk_idx_ptr, uint64_t active_ranks_ptr,
    int num_tokens, int hidden, int num_topk,
    int num_max_dispatch_tokens_per_rank, int num_experts, int timeout_us,
    bool use_fp8, uint64_t packed_recv_x_ptr, uint64_t packed_recv_x_scales_ptr,
    uint64_t packed_recv_count_ptr, uint64_t packed_recv_src_info_ptr,
    uint64_t packed_recv_layout_range_ptr, bool async, bool return_recv_hook,
    uint64_t compute_stream_ptr, uint64_t topk_weights_ptr,
    bool with_topk_weights) {
    XPU_EP_CHECK(!(async && return_recv_hook),
                 "async and return_recv_hook are mutually exclusive");
    checkShape(hidden, num_topk, num_max_dispatch_tokens_per_rank, num_experts);
    XPU_EP_CHECK(
        num_tokens >= 0 && num_tokens <= num_max_dispatch_tokens_per_rank,
        "num_tokens exceeds num_max_dispatch_tokens_per_rank");
    XPU_EP_CHECK(active_ranks_ptr != 0, "active_ranks is required");
    XPU_EP_CHECK(!use_fp8 || packed_recv_x_scales_ptr != 0,
                 "FP8 dispatch requires packed_recv_x_scales");
    // The mode is a flag of its own: a rank with no tokens passes an empty
    // weights tensor, whose pointer is null, yet must reduce like its peers.
    XPU_EP_CHECK(!with_topk_weights || num_tokens == 0 || topk_weights_ptr != 0,
                 "topk_weights is required for its tokens");
    sycl::queue& q = queueFor(compute_stream_ptr);

    BufferPair layout(ipc_->base(), num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks_, num_experts);
    std::lock_guard<std::mutex> lock(mu_);
    const int cur = buffer_idx_;
    buffer_idx_ ^= 1;
    const int shadow_slot = dispatch_slots_.take(packed_recv_src_info_ptr);
    slot_reduced_[shadow_slot] = with_topk_weights;
    orderAfterLast(q);
    if (num_tokens > 0)
        q.memcpy(topkShadow(shadow_slot), reinterpret_cast<void*>(topk_idx_ptr),
                 static_cast<size_t>(num_tokens) * num_topk * sizeof(int64_t));

    DispatchArgs a{};
    a.x = reinterpret_cast<const void*>(x_ptr);
    a.topk_idx = reinterpret_cast<const int64_t*>(topk_idx_ptr);
    a.topk_weights = reinterpret_cast<const float*>(topk_weights_ptr);
    a.active_ranks = reinterpret_cast<int32_t*>(active_ranks_ptr);
    a.packed_recv_x = reinterpret_cast<void*>(packed_recv_x_ptr);
    a.packed_recv_x_scales = reinterpret_cast<float*>(packed_recv_x_scales_ptr);
    a.packed_recv_src_info = reinterpret_cast<int*>(packed_recv_src_info_ptr);
    a.packed_recv_layout_range =
        reinterpret_cast<int64_t*>(packed_recv_layout_range_ptr);
    a.packed_recv_count = reinterpret_cast<int*>(packed_recv_count_ptr);
    a.ready = layout.halves[cur].ready;
    a.next_ready = layout.halves[cur ^ 1].ready;
    a.staging = layout.halves[cur].staging;
    a.slot_shadow = slotShadow(shadow_slot);
    a.counter_per_expert = reinterpret_cast<int*>(workspace_);
    a.counter_per_rank =
        reinterpret_cast<int*>(workspace_ + kRankCounterOffset);
    a.done_counter = reinterpret_cast<int*>(workspace_ + kDoneOffset);
    if (with_topk_weights) a.recv = recvTable(shadow_slot);
    a.peers = PeerView{ipc_->base(), ipc_->peerBases()};
    a.num_tokens = num_tokens;
    a.hidden = hidden;
    a.num_topk = num_topk;
    a.num_max_dispatch_tokens_per_rank = num_max_dispatch_tokens_per_rank;
    a.num_experts = num_experts;
    a.rank = rank_;
    a.num_ranks = num_ranks_;
    a.use_fp8 = use_fp8;
    a.timeout_ticks = timeoutTicks(timeout_us);
    a.max_wgs = max_wgs_;

    last_event_ = dispatch_send(q, a);
    Hook hook;
    if (return_recv_hook) {
        sycl::queue hook_queue = q;
        hook = [this, hook_queue, a]() mutable {
            std::lock_guard<std::mutex> lock(mu_);
            orderAfterLast(hook_queue);
            last_event_ = dispatch_recv(hook_queue, a);
        };
    } else {
        last_event_ = dispatch_recv(q, a);
    }
    std::optional<XpuEventHandle> event;
    if (async) event.emplace(q.ext_oneapi_submit_barrier());
    return {event, hook};
}

MooncakeEpXpuBuffer::Result MooncakeEpXpuBuffer::combine(
    uint64_t x_ptr, uint64_t topk_idx_ptr, uint64_t topk_weights_ptr,
    uint64_t src_info_ptr, uint64_t layout_range_ptr, uint64_t active_ranks_ptr,
    int num_local_experts, int num_combined_tokens, int hidden, int num_topk,
    int num_max_dispatch_tokens_per_rank, int num_experts, int timeout_us,
    bool zero_copy, uint64_t combined_x_ptr, bool async, bool return_recv_hook,
    uint64_t compute_stream_ptr) {
    // Combine reduces with the top-k snapshot its dispatch took (found by
    // src_info), so topk_idx_ptr is not read.
    (void)topk_idx_ptr;
    XPU_EP_CHECK(!zero_copy, "zero-copy combine is not supported");
    XPU_EP_CHECK(!(async && return_recv_hook),
                 "async and return_recv_hook are mutually exclusive");
    checkShape(hidden, num_topk, num_max_dispatch_tokens_per_rank, num_experts);
    XPU_EP_CHECK(num_local_experts * num_ranks_ == num_experts,
                 "num_local_experts does not match num_experts");
    XPU_EP_CHECK(
        num_combined_tokens >= 0 &&
            num_combined_tokens <= num_max_dispatch_tokens_per_rank,
        "num_combined_tokens exceeds num_max_dispatch_tokens_per_rank");
    XPU_EP_CHECK(active_ranks_ptr != 0, "active_ranks is required");
    sycl::queue& q = queueFor(compute_stream_ptr);

    BufferPair layout(ipc_->base(), num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks_, num_experts);
    std::lock_guard<std::mutex> lock(mu_);
    const int shadow_slot = dispatch_slots_.find(src_info_ptr);
    XPU_EP_CHECK(shadow_slot >= 0,
                 "the combine handle must come from one of the last " +
                     std::to_string(kEpDispatchSlots) +
                     " dispatches on this buffer (it is found by its "
                     "src_info tensor)");
    const int cur = buffer_idx_;
    buffer_idx_ ^= 1;
    orderAfterLast(q);

    CombineArgs a{};
    a.x = reinterpret_cast<const void*>(x_ptr);
    a.topk_idx = topkShadow(shadow_slot);
    a.slot_shadow = slotShadow(shadow_slot);
    a.topk_weights = reinterpret_cast<const float*>(topk_weights_ptr);
    a.layout_range = reinterpret_cast<const int64_t*>(layout_range_ptr);
    a.active_ranks = reinterpret_cast<int32_t*>(active_ranks_ptr);
    a.combined_x = reinterpret_cast<void*>(combined_x_ptr);
    a.ready = layout.halves[cur].ready;
    a.next_ready = layout.halves[cur ^ 1].ready;
    a.staging = layout.halves[cur].staging;
    a.done_counter = reinterpret_cast<int*>(workspace_ + kDoneOffset) + 1;
    if (slot_reduced_[shadow_slot]) a.recv = recvTable(shadow_slot);
    a.peers = PeerView{ipc_->base(), ipc_->peerBases()};
    a.num_combined_tokens = num_combined_tokens;
    a.hidden = hidden;
    a.num_topk = num_topk;
    a.num_max_dispatch_tokens_per_rank = num_max_dispatch_tokens_per_rank;
    a.num_experts = num_experts;
    a.rank = rank_;
    a.num_ranks = num_ranks_;
    a.timeout_ticks = timeoutTicks(timeout_us);
    a.max_wgs = max_wgs_;

    last_event_ = combine_send(q, a);
    Hook hook;
    if (return_recv_hook) {
        sycl::queue hook_queue = q;
        hook = [this, hook_queue, a]() mutable {
            std::lock_guard<std::mutex> lock(mu_);
            orderAfterLast(hook_queue);
            last_event_ = combine_recv(hook_queue, a);
        };
    } else {
        last_event_ = combine_recv(q, a);
    }
    std::optional<XpuEventHandle> event;
    if (async) event.emplace(q.ext_oneapi_submit_barrier());
    return {event, hook};
}

}  // namespace xpu_ep
}  // namespace mooncake
