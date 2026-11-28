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

#include <algorithm>
#include <stdexcept>
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
// A half of the hybrid path's host counts (see kHostCellsOffset).
static_assert(kMaxRanks <= static_cast<int>(kHostCellsOffset));
constexpr size_t kHostCountInts = kHostCellsOffset + kMaxExperts;

sycl::queue* asQueue(uint64_t stream_ptr) {
    XPU_EP_CHECK(stream_ptr != 0,
                 "a torch.xpu stream (sycl_queue) is required");
    return reinterpret_cast<sycl::queue*>(stream_ptr);
}

// Checked in the initializer list, before the stream: bad arguments raise
// ValueError (std::invalid_argument) with the same messages as CUDA.
int validate_rank(int rank, int num_ranks) {
    if (num_ranks <= 0 || num_ranks > kMaxRanks || rank < 0 ||
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
    : rank_(validate_rank(rank, num_ranks)),
      num_ranks_(num_ranks),
      num_ep_buffer_bytes_(validate_buffer_size(num_ep_buffer_bytes)),
      p2p_enabled_(!disable_p2p),
      queue_(*asQueue(stream_ptr)) {
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
    // As on CUDA, a buffer left to destroy() keeps its memory, which peers
    // may still map, when it is collected without one.
    if (explicitly_destroy_ && !destroyed_) {
        (void)ipc_.release();
#if MOONCAKE_EP_XPU_RDMA
        (void)proxy_.release();
#endif
        return;
    }
    destroy();
}

void MooncakeEpXpuBuffer::destroy() {
    if (destroyed_) return;
    // Calls may have run on other streams than queue_; the last one launched
    // completes after all of them.
    try {
        quiesce();
    } catch (...) {
    }
    std::lock_guard<std::mutex> lock(elastic_mu_);
    destroyed_ = true;
#if MOONCAKE_EP_XPU_RDMA
    proxy_.reset();  // deregisters the buffer before it is freed
#endif
    if (workspace_) sycl::free(workspace_, queue_);
    if (recv_tables_) sycl::free(recv_tables_, queue_);
    if (d_remote_) sycl::free(d_remote_, queue_);
    if (host_counts_) sycl::free(host_counts_, queue_);
    workspace_ = nullptr;
    recv_tables_ = nullptr;
    d_remote_ = nullptr;
    host_counts_ = nullptr;
    ipc_.reset();
}

void MooncakeEpXpuBuffer::quiesce() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        last_event_.wait();
    }
    queue_.wait();
    drainProxy();
}

bool MooncakeEpXpuBuffer::use_fast_path() const {
    if (!p2p_enabled_) return false;
    if (!ipc_->allPeersAccessible()) {
        LOG(WARNING) << "[EP XPU] not every peer is reachable over Level Zero "
                        "IPC; using the fallback path (degraded performance)";
        return false;
    }
    if (std::any_of(remote_.begin(), remote_.end(),
                    [](int r) { return r != 0; }) &&
        !rdma_ok_) {
        LOG(WARNING) << "[EP XPU] ranks on other nodes are not connected over "
                        "RDMA; using the fallback path (degraded performance)";
        return false;
    }
    return true;
}

bool MooncakeEpXpuBuffer::rdma_available() { return MOONCAKE_EP_XPU_RDMA != 0; }

std::vector<int32_t> MooncakeEpXpuBuffer::init_rdma(
    const std::vector<int>& remote) {
    XPU_EP_CHECK(static_cast<int>(remote.size()) == num_ranks_,
                 "remote must have one entry per rank");
    XPU_EP_CHECK(!elastic_ && !destroyed_,
                 "an elastic buffer reaches peers over Level Zero IPC only");
    quiesce();
    remote_ = remote;
    rdma_ok_ = false;
    hybrid_ = false;
    rdma_ranks_.clear();
#if MOONCAKE_EP_XPU_RDMA
    proxy_.reset();
    proxy_ = std::make_unique<XpuEpProxy>(
        queue_.get_device(), ipc_->zeContext(), ipc_->base(),
        static_cast<size_t>(num_ep_buffer_bytes_), rank_, remote);
    return proxy_->localInfo();
#else
    XPU_EP_CHECK(false,
                 "built without the verbs path (no rdma-core with "
                 "ibv_reg_dmabuf_mr)");
    return {};
#endif
}

void MooncakeEpXpuBuffer::connect_rdma(
    const std::vector<std::vector<int32_t>>& infos,
    const std::vector<int>& active_ranks_mask) {
#if MOONCAKE_EP_XPU_RDMA
    XPU_EP_CHECK(proxy_ != nullptr && !destroyed_,
                 "connect_rdma needs init_rdma first");
    rdma_ranks_.clear();
    proxy_->connect(infos, active_ranks_mask);
    std::vector<int> connected(num_ranks_, 0);
    for (int r = 0; r < num_ranks_; ++r) {
        if (!proxy_->connected(r)) continue;
        connected[r] = 1;
        rdma_ranks_.push_back(r);
    }
    if (!d_remote_) {
        d_remote_ = sycl::malloc_device<int>(num_ranks_, queue_);
        host_counts_ = sycl::malloc_host<int>(2 * kHostCountInts, queue_);
        XPU_EP_CHECK(d_remote_ && host_counts_,
                     "hybrid path allocation failed");
    }
    queue_.memcpy(d_remote_, connected.data(), num_ranks_ * sizeof(int)).wait();
    hybrid_ = !rdma_ranks_.empty();
    rdma_ok_ = true;
#else
    (void)infos;
    (void)active_ranks_mask;
    XPU_EP_CHECK(false,
                 "built without the verbs path (no rdma-core with "
                 "ibv_reg_dmabuf_mr)");
#endif
}

std::vector<int32_t> MooncakeEpXpuBuffer::get_ipc_handle() {
    XPU_EP_CHECK(!destroyed_, "EP buffer has been destroyed");
    return ipc_->exportHandle();
}

void MooncakeEpXpuBuffer::sync_nvlink_ipc_handles(
    const std::vector<std::vector<int32_t>>& remote_handles,
    const std::vector<int>& active_ranks_mask) {
    XPU_EP_CHECK(!elastic_ && !destroyed_,
                 "an elastic buffer connects peers with connect_ranks");
    // Remapping peers closes the old mappings, which launched kernels may
    // still be reading.
    quiesce();
    // Ranks on other nodes are reached over RDMA, if at all.
    std::vector<int> active = active_ranks_mask;
    for (size_t r = 0; r < remote_.size() && r < active.size(); ++r)
        if (remote_[r]) active[r] = 0;
    ipc_->importPeers(remote_handles, active);
}

void MooncakeEpXpuBuffer::drainProxy() {
#if MOONCAKE_EP_XPU_RDMA
    if (proxy_) proxy_->drain();
#endif
}

PeerView MooncakeEpXpuBuffer::peerView() const {
    return PeerView{ipc_->base(), ipc_->peerBases(),
                    hybrid_ ? d_remote_ : nullptr};
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
                                     int num_experts, int rank_bound,
                                     int layout_experts) const {
    XPU_EP_CHECK(hidden > 0 && hidden % kNumPerChannels == 0,
                 "hidden must be a multiple of 128");
    XPU_EP_CHECK(num_topk > 0 && num_topk <= kTopkShadowMaxTopk,
                 "num_topk must be in [1, 32]");
    XPU_EP_CHECK(num_max_dispatch_tokens_per_rank > 0 &&
                     num_max_dispatch_tokens_per_rank <= kTopkShadowMaxTokens,
                 "num_max_dispatch_tokens_per_rank must be in [1, 1024]");
    XPU_EP_CHECK(num_experts > 0 && num_experts % rank_bound == 0 &&
                     layout_experts <= kMaxExperts,
                 "num_experts must be a positive multiple of the EP size");
    const size_t need = BufferPair(nullptr, num_max_dispatch_tokens_per_rank,
                                   hidden, num_ranks_, layout_experts, hybrid_)
                            .total_bytes;
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
    XPU_EP_CHECK(!destroyed_, "EP buffer has been destroyed");
    const int rank_bound = rankBound(num_experts, active_ranks_ptr);
    const int layout_experts =
        elastic_ ? num_ranks_ * num_experts_per_rank_ : num_experts;
    checkShape(hidden, num_topk, num_max_dispatch_tokens_per_rank, num_experts,
               rank_bound, layout_experts);
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
                      num_ranks_, layout_experts, hybrid_);
    std::lock_guard<std::mutex> lock(mu_);
    last_half_bytes_ = layout.half_bytes;
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
    if (hybrid_) a.host_counts = host_counts_ + cur * kHostCountInts;
    if (with_topk_weights) a.recv = recvTable(shadow_slot);
    a.peers = peerView();
    a.num_tokens = num_tokens;
    a.hidden = hidden;
    a.num_topk = num_topk;
    a.num_max_dispatch_tokens_per_rank = num_max_dispatch_tokens_per_rank;
    a.num_experts = num_experts;
    a.rank = rank_;
    a.num_ranks = rank_bound;
    a.use_fp8 = use_fp8;
    a.timeout_ticks = timeoutTicks(timeout_us);
    a.max_wgs = max_wgs_;

    last_event_ = dispatch_send(q, a);
#if MOONCAKE_EP_XPU_RDMA
    if (hybrid_) {
        proxy_->submit(last_event_,
                       [a, base = ipc_->base(), ranks = rdma_ranks_] {
                           return dispatch_puts(a, base, ranks);
                       });
    }
#endif
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
    XPU_EP_CHECK(!destroyed_, "EP buffer has been destroyed");
    const int rank_bound = rankBound(num_experts, active_ranks_ptr);
    const int layout_experts =
        elastic_ ? num_ranks_ * num_experts_per_rank_ : num_experts;
    checkShape(hidden, num_topk, num_max_dispatch_tokens_per_rank, num_experts,
               rank_bound, layout_experts);
    XPU_EP_CHECK(num_local_experts * rank_bound == num_experts,
                 "num_local_experts does not match num_experts");
    XPU_EP_CHECK(
        num_combined_tokens >= 0 &&
            num_combined_tokens <= num_max_dispatch_tokens_per_rank,
        "num_combined_tokens exceeds num_max_dispatch_tokens_per_rank");
    XPU_EP_CHECK(active_ranks_ptr != 0, "active_ranks is required");
    sycl::queue& q = queueFor(compute_stream_ptr);

    BufferPair layout(ipc_->base(), num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks_, layout_experts, hybrid_);
    std::lock_guard<std::mutex> lock(mu_);
    last_half_bytes_ = layout.half_bytes;
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
    if (hybrid_) {
        a.inbox = a.staging + layout.combine_bytes;
        a.host_counts = host_counts_ + cur * kHostCountInts;
    }
    if (slot_reduced_[shadow_slot]) a.recv = recvTable(shadow_slot);
    a.peers = peerView();
    a.num_combined_tokens = num_combined_tokens;
    a.hidden = hidden;
    a.num_topk = num_topk;
    a.num_max_dispatch_tokens_per_rank = num_max_dispatch_tokens_per_rank;
    a.num_experts = num_experts;
    a.rank = rank_;
    a.num_ranks = rank_bound;
    a.timeout_ticks = timeoutTicks(timeout_us);
    a.max_wgs = max_wgs_;

    last_event_ = combine_send(q, a);
#if MOONCAKE_EP_XPU_RDMA
    if (hybrid_) {
        proxy_->submit(last_event_,
                       [a, base = ipc_->base(), ranks = rdma_ranks_] {
                           return combine_puts(a, base, ranks);
                       });
    }
#endif
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

int MooncakeEpXpuBuffer::rankBound(int num_experts, uint64_t active_ranks_ptr) {
    if (!elastic_) return num_ranks_;
    std::lock_guard<std::mutex> lock(elastic_mu_);
    XPU_EP_CHECK(
        active_ranks_ptr == reinterpret_cast<uint64_t>(elastic_active_ranks_),
        "an elastic buffer takes its own active-rank mask");
    XPU_EP_CHECK(num_experts > 0 && num_experts % num_experts_per_rank_ == 0,
                 "num_experts must be a multiple of num_experts_per_rank");
    const int bound = num_experts / num_experts_per_rank_;
    XPU_EP_CHECK(bound >= active_rank_bound_ && bound <= num_ranks_,
                 "num_experts must cover every active rank slot and fit the "
                 "rank capacity");
    return bound;
}

void MooncakeEpXpuBuffer::set_explicit_destroy(bool explicitly_destroy) {
    if (destroyed_) throw std::runtime_error("EP buffer is already destroyed");
    explicitly_destroy_ = explicitly_destroy;
}

void MooncakeEpXpuBuffer::configure_elastic(int num_experts_per_rank,
                                            uint64_t active_ranks_ptr) {
    std::lock_guard<std::mutex> lock(elastic_mu_);
    if (elastic_ || destroyed_ || num_experts_per_rank <= 0 ||
        active_ranks_ptr == 0) {
        throw std::invalid_argument("invalid elastic EP buffer configuration");
    }
    if (static_cast<int64_t>(num_ranks_) * num_experts_per_rank > kMaxExperts) {
        throw std::invalid_argument("elastic EP capacity exceeds " +
                                    std::to_string(kMaxExperts) +
                                    " experts on XPU");
    }
    XPU_EP_CHECK(p2p_enabled_,
                 "XPU elastic EP reaches peers over Level Zero IPC only, so "
                 "P2P must be enabled");
    XPU_EP_CHECK(remote_.empty(),
                 "XPU elastic EP reaches peers over Level Zero IPC only");

    elastic_ = true;
    num_experts_per_rank_ = num_experts_per_rank;
    elastic_active_ranks_ = reinterpret_cast<int32_t*>(active_ranks_ptr);
    elastic_connected_.assign(num_ranks_, 0);
    elastic_committed_.assign(num_ranks_, 0);
    elastic_staged_.assign(num_ranks_, 0);
    elastic_active_host_.assign(num_ranks_, 0);
    elastic_connected_[rank_] = 1;
    elastic_committed_[rank_] = 1;
    elastic_active_host_[rank_] = 1;
    active_rank_bound_ = rank_ + 1;
    queue_.memset(elastic_active_ranks_, 0, num_ranks_ * sizeof(int32_t));
    queue_.fill(elastic_active_ranks_ + rank_, int32_t{1}, 1);
    queue_.wait();
}

void MooncakeEpXpuBuffer::validate_connect_ranks(
    const std::vector<int>& peer_ranks) {
    std::lock_guard<std::mutex> lock(elastic_mu_);
    if (!elastic_ || destroyed_) {
        throw std::runtime_error("elastic EP buffer is not configured");
    }
    for (int peer_rank : peer_ranks) {
        if (peer_rank < 0 || peer_rank >= num_ranks_ || peer_rank == rank_) {
            throw std::invalid_argument(
                "invalid elastic EP connect rank slot " +
                std::to_string(peer_rank));
        }
        if (elastic_connected_[peer_rank]) {
            throw std::invalid_argument(
                "elastic EP rank slot " + std::to_string(peer_rank) +
                " is already connected; call disconnect_ranks() first");
        }
    }
}

void MooncakeEpXpuBuffer::stage_peer(int peer_rank, int64_t raddr, int32_t rkey,
                                     int64_t subnet_prefix,
                                     int64_t interface_id,
                                     const std::vector<int32_t>& qpns,
                                     const std::vector<int32_t>& lids,
                                     const std::vector<int32_t>& ipc_handle) {
    (void)raddr;
    (void)rkey;
    (void)subnet_prefix;
    (void)interface_id;
    (void)qpns;
    (void)lids;
    std::lock_guard<std::mutex> lock(elastic_mu_);
    if (!elastic_ || destroyed_ || peer_rank < 0 || peer_rank >= num_ranks_ ||
        peer_rank == rank_) {
        throw std::invalid_argument("invalid elastic EP peer rank");
    }
    if (elastic_connected_[peer_rank]) {
        throw std::invalid_argument(
            "elastic EP rank slot " + std::to_string(peer_rank) +
            " is already connected; call disconnect_ranks() first");
    }
    if (elastic_active_host_[peer_rank]) {
        throw std::invalid_argument(
            "cannot prepare an active elastic EP rank slot");
    }
    XPU_EP_CHECK(!ipc_handle.empty(),
                 "elastic EP rank " + std::to_string(peer_rank) +
                     " has no IPC handle (it is on another host?); XPU "
                     "elastic EP reaches peers over Level Zero IPC only");
    ipc_->stagePeer(peer_rank, ipc_handle);
    elastic_connected_[peer_rank] = 1;
    elastic_staged_[peer_rank] = 1;
}

void MooncakeEpXpuBuffer::commitStagedPeers() {
    if (std::none_of(elastic_staged_.begin(), elastic_staged_.end(),
                     [](uint8_t v) { return v != 0; }))
        return;
    // Kernels read the peer table, so it changes only between calls.
    quiesce();
    for (int peer = 0; peer < num_ranks_; ++peer) {
        if (!elastic_staged_[peer]) continue;
        ipc_->publishPeer(peer);
        elastic_staged_[peer] = 0;
        elastic_committed_[peer] = 1;
    }
}

void MooncakeEpXpuBuffer::refreshActiveRankBound() {
    active_rank_bound_ = 0;
    for (int peer = 0; peer < num_ranks_; ++peer)
        if (elastic_active_host_[peer]) active_rank_bound_ = peer + 1;
}

void MooncakeEpXpuBuffer::disconnect_ranks(const std::vector<int>& peer_ranks) {
    std::lock_guard<std::mutex> lock(elastic_mu_);
    if (!elastic_ || destroyed_) {
        throw std::runtime_error("elastic EP buffer is not configured");
    }
    if (peer_ranks.empty()) return;
    for (int peer : peer_ranks) {
        if (peer < 0 || peer >= num_ranks_ || peer == rank_ ||
            !elastic_connected_[peer]) {
            throw std::invalid_argument("invalid elastic EP disconnect rank");
        }
    }
    quiesce();
    for (int peer : peer_ranks) {
        elastic_active_host_[peer] = 0;
        queue_.fill(elastic_active_ranks_ + peer, int32_t{0}, 1);
    }
    refreshActiveRankBound();
    size_t half_bytes;
    {
        std::lock_guard<std::mutex> mu_lock(mu_);
        half_bytes = last_half_bytes_;
    }
    // The peer's last ready flags in this rank's buffer would otherwise
    // greet the slot's next occupant as already sent. Half 0's flags are at
    // the base; half 1's, if any call has run, half_bytes past it.
    int* ready0 = static_cast<int*>(ipc_->base());
    const size_t half_ints = half_bytes / sizeof(int);
    for (int peer : peer_ranks) {
        if (elastic_staged_[peer]) ipc_->discardStagedPeer(peer);
        if (elastic_committed_[peer]) ipc_->disconnectPeer(peer);
        int* flag = ready0 + static_cast<size_t>(peer) * kReadyStride;
        queue_.single_task([=] {
            uc_store(flag, 0);
            if (half_ints) uc_store(flag + half_ints, 0);
        });
        elastic_staged_[peer] = 0;
        elastic_committed_[peer] = 0;
        elastic_connected_[peer] = 0;
    }
    queue_.wait();
}

void MooncakeEpXpuBuffer::update_mask_buffer(int peer_rank, bool masked,
                                             uint64_t stream_ptr) {
    std::lock_guard<std::mutex> lock(elastic_mu_);
    if (!elastic_ || destroyed_ || peer_rank < 0 || peer_rank >= num_ranks_ ||
        (peer_rank == rank_ && masked)) {
        throw std::invalid_argument("invalid elastic EP mask update");
    }
    if (!masked && !elastic_connected_[peer_rank]) {
        throw std::invalid_argument("cannot unmask an unconnected EP rank");
    }
    sycl::queue& q = queueFor(stream_ptr);
    commitStagedPeers();
    elastic_active_host_[peer_rank] = masked ? 0 : 1;
    refreshActiveRankBound();
    q.fill(elastic_active_ranks_ + peer_rank, int32_t{masked ? 0 : 1}, 1);
}

void MooncakeEpXpuBuffer::query_mask_buffer(uint64_t output_ptr,
                                            uint64_t stream_ptr) {
    if (!elastic_ || destroyed_ || output_ptr == 0) {
        throw std::invalid_argument("invalid elastic EP mask query");
    }
    // As on CUDA, the output is the mask: 1 for an inactive rank slot.
    int32_t* out = reinterpret_cast<int32_t*>(output_ptr);
    const int32_t* active = elastic_active_ranks_;
    queueFor(stream_ptr)
        .parallel_for(sycl::range<1>(num_ranks_),
                      [=](sycl::id<1> r) { out[r] = active[r] ? 0 : 1; });
}

void MooncakeEpXpuBuffer::clean_mask_buffer(uint64_t stream_ptr) {
    std::lock_guard<std::mutex> lock(elastic_mu_);
    if (!elastic_ || destroyed_) {
        throw std::runtime_error("elastic EP buffer is not configured");
    }
    sycl::queue& q = queueFor(stream_ptr);
    commitStagedPeers();
    for (int peer = 0; peer < num_ranks_; ++peer)
        elastic_active_host_[peer] = elastic_connected_[peer] ? 1 : 0;
    refreshActiveRankBound();
    // Waits: the source is elastic_active_host_, which later calls change.
    q.memcpy(elastic_active_ranks_, elastic_active_host_.data(),
             num_ranks_ * sizeof(int32_t))
        .wait();
}

int MooncakeEpXpuBuffer::get_active_rank_bound() {
    std::lock_guard<std::mutex> lock(elastic_mu_);
    return active_rank_bound_;
}

std::tuple<uint64_t, size_t> MooncakeEpXpuBuffer::get_local_buffer_descriptor()
    const {
    if (destroyed_ || !ipc_) {
        throw std::runtime_error("EP buffer is not available");
    }
    return {reinterpret_cast<uint64_t>(ipc_->base()),
            static_cast<size_t>(num_ep_buffer_bytes_)};
}

}  // namespace xpu_ep
}  // namespace mooncake
