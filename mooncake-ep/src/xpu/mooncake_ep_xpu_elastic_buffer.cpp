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

#include "mooncake_ep_xpu_elastic_buffer.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>

#include "xpu_ep_elastic_kernels.h"
#include "xpu_ep_common.h"

namespace mooncake {
namespace xpu_ep {

namespace {

// Workspace (not shared with peers): the per-call counts the kernels pass to
// one another and the send-kernel arrival counter.
constexpr int kMaxRanks = 1024;
constexpr int kMaxExperts = 2048;
constexpr size_t kRecvCountOffset = 0;
constexpr size_t kExpertCountOffset = kMaxRanks * sizeof(int);
constexpr size_t kDoneOffset = kExpertCountOffset + kMaxExperts * sizeof(int);
constexpr size_t kWorkspaceBytes = kDoneOffset + kLineBytes;

// As _env_enabled in mooncake_ep_buffer.py.
bool envEnabled(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr) return false;
    std::string s(value);
    for (char& ch : s)
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    return s == "1" || s == "ON" || s == "TRUE" || s == "YES";
}

sycl::queue* asQueue(uint64_t stream_ptr) {
    XPU_EP_CHECK(stream_ptr != 0,
                 "a torch.xpu stream (sycl_queue) is required");
    return reinterpret_cast<sycl::queue*>(stream_ptr);
}

ElasticMsgLayout default_msg(int64_t hidden, int64_t num_topk, bool use_fp8) {
    const int h = static_cast<int>(hidden);
    return ElasticMsgLayout(
        h, use_fp8 ? 1 : kBf16Size,
        use_fp8 ? (h + kNumPerChannels - 1) / kNumPerChannels : 0,
        static_cast<int>(std::max<int64_t>(1, num_topk)));
}

}  // namespace

MooncakeEpXpuElasticBuffer::MooncakeEpXpuElasticBuffer(
    int rank, int num_ranks, int64_t num_buffer_bytes,
    int64_t num_max_tokens_per_rank, int64_t hidden, int64_t num_topk,
    bool use_fp8_dispatch, bool deterministic, bool allow_hybrid_mode,
    bool allow_multiple_reduction, bool prefer_overlap_with_compute, int sl_idx,
    int num_allocated_qps, int num_cpu_timeout_secs, int num_gpu_timeout_secs,
    const std::string& transport, const std::vector<int32_t>& nccl_unique_id,
    uint64_t stream_ptr)
    : rank_(rank), num_ranks_(num_ranks), queue_(*asQueue(stream_ptr)) {
    // Shape hints, the hybrid switch (a single node never needs it), and the
    // RDMA and SM knobs have nothing to act on here. Every slot is assigned in
    // token order, so dispatch is deterministic either way.
    (void)num_max_tokens_per_rank;
    (void)hidden;
    (void)num_topk;
    (void)use_fp8_dispatch;
    (void)deterministic;
    (void)allow_hybrid_mode;
    (void)prefer_overlap_with_compute;
    (void)sl_idx;
    (void)num_allocated_qps;
    (void)num_cpu_timeout_secs;
    XPU_EP_CHECK(transport == "auto" || transport == "ipc",
                 "the XPU ElasticBuffer supports only transport='ipc' (one "
                 "node over Level Zero IPC), got '" +
                     transport + "'");
    XPU_EP_CHECK(nccl_unique_id.empty(),
                 "the XPU ElasticBuffer has no NCCL transport");
    XPU_EP_CHECK(allow_multiple_reduction,
                 "the XPU ElasticBuffer supports only "
                 "allow_multiple_reduction=True");
    XPU_EP_CHECK(num_ranks > 0 && num_ranks <= kMaxRanks && rank >= 0 &&
                     rank < num_ranks,
                 "invalid rank or number of ranks");
    XPU_EP_CHECK(num_buffer_bytes > 0, "buffer size must be positive");
    XPU_EP_CHECK(queue_.is_in_order(), "the torch.xpu stream must be in-order");
    if (envEnabled("MOONCAKE_EP_XPU_ASYNC_WITH_COMPUTE_STREAM_ENABLE"))
        comm_queue_.emplace(queue_.get_context(), queue_.get_device(),
                            sycl::property::queue::in_order{});
    half_stride_ = static_cast<size_t>(num_buffer_bytes) / 2 / 4096 * 4096;
    XPU_EP_CHECK(half_stride_ > 0, "buffer too small");

    ipc_ = std::make_unique<XpuIpcBuffer>(
        queue_, static_cast<size_t>(num_buffer_bytes), rank, num_ranks);
    workspace_ = sycl::malloc_device<uint8_t>(kWorkspaceBytes, queue_);
    XPU_EP_CHECK(workspace_ != nullptr, "workspace allocation failed");
    queue_.memset(workspace_, 0, kWorkspaceBytes).wait();

    const auto dev = queue_.get_device();
    // See MooncakeEpXpuBuffer::timeoutTicks.
    const int64_t clock_mhz =
        dev.get_info<sycl::info::device::max_clock_frequency>();
    timeout_ticks_ =
        num_gpu_timeout_secs <= 0
            ? -1
            : clock_mhz * 1000000 * static_cast<int64_t>(num_gpu_timeout_secs);
    max_wgs_ =
        static_cast<int>(dev.get_info<sycl::info::device::max_compute_units>());
}

MooncakeEpXpuElasticBuffer::~MooncakeEpXpuElasticBuffer() {
    try {
        destroy();
    } catch (...) {
    }
}

void MooncakeEpXpuElasticBuffer::destroy() {
    if (!ipc_) return;
    // Calls may have run on other streams than queue_; the last one launched
    // completes after all of them.
    {
        std::lock_guard<std::mutex> lock(mu_);
        last_event_.wait();
    }
    queue_.wait();
    if (workspace_) sycl::free(workspace_, queue_);
    workspace_ = nullptr;
    ipc_.reset();
}

void MooncakeEpXpuElasticBuffer::checkAlive() const {
    XPU_EP_CHECK(ipc_ != nullptr, "ElasticBuffer has been destroyed");
}

bool MooncakeEpXpuElasticBuffer::use_fast_path() const {
    return ipc_ && ipc_->allPeersAccessible();
}

int64_t MooncakeEpXpuElasticBuffer::calculate_buffer_size(
    int num_ranks, int64_t num_max_tokens_per_rank, int64_t hidden,
    int64_t num_topk, bool use_fp8_dispatch, bool allow_hybrid_mode,
    bool allow_multiple_reduction) {
    (void)allow_hybrid_mode;
    (void)allow_multiple_reduction;
    const ElasticHalfLayout hl(num_ranks,
                               static_cast<int>(num_max_tokens_per_rank),
                               default_msg(hidden, num_topk, use_fp8_dispatch),
                               static_cast<int>(hidden));
    return static_cast<int64_t>(2 * hl.bytes);
}

std::vector<int32_t> MooncakeEpXpuElasticBuffer::get_ipc_handle() {
    checkAlive();
    return ipc_->exportHandle();
}

void MooncakeEpXpuElasticBuffer::sync_nvlink_ipc_handles(
    const std::vector<std::vector<int32_t>>& remote_handles,
    const std::vector<int>& active_ranks_mask) {
    checkAlive();
    // Remapping peers closes the old mappings, which launched kernels may
    // still be reading.
    {
        std::lock_guard<std::mutex> lock(mu_);
        last_event_.wait();
    }
    queue_.wait();
    ipc_->importPeers(remote_handles, active_ranks_mask);
    XPU_EP_CHECK(ipc_->allPeersAccessible(),
                 "the XPU ElasticBuffer needs every active rank on this node "
                 "and reachable over Level Zero IPC; multi-node (hybrid) "
                 "mode is not supported");
}

sycl::queue& MooncakeEpXpuElasticBuffer::queueFor(uint64_t stream_ptr) {
    sycl::queue& q = *asQueue(stream_ptr);
    XPU_EP_CHECK(q.get_context() == queue_.get_context() &&
                     q.get_device() == queue_.get_device(),
                 "the stream belongs to a different device than the buffer");
    XPU_EP_CHECK(q.is_in_order(), "the torch.xpu stream must be in-order");
    return q;
}

void MooncakeEpXpuElasticBuffer::orderAfterLast(sycl::queue& q) {
    // Calls share the arrival counter and the buffer halves, so they must run
    // in issue order; across streams, wait for the last launch.
    if (last_queue_ && *last_queue_ != q)
        q.ext_oneapi_submit_barrier({last_event_});
    last_queue_ = q;
}

sycl::queue& MooncakeEpXpuElasticBuffer::launchQueue(sycl::queue& q) {
    if (comm_queue_) {
        // In order, so after every earlier call; and after the compute
        // stream's queued work, which produces the inputs.
        comm_queue_->ext_oneapi_submit_barrier({q.ext_oneapi_submit_barrier()});
        return *comm_queue_;
    }
    orderAfterLast(q);
    return q;
}

std::optional<XpuEventHandle> MooncakeEpXpuElasticBuffer::finish(
    sycl::queue& q, sycl::event done, bool async_with_compute_stream) {
    last_event_ = done;
    if (comm_queue_) {
        if (async_with_compute_stream) return XpuEventHandle(std::move(done));
        q.ext_oneapi_submit_barrier({done});
        return std::nullopt;
    }
    if (!async_with_compute_stream) return std::nullopt;
    return XpuEventHandle(q.ext_oneapi_submit_barrier());
}

int MooncakeEpXpuElasticBuffer::maxWgs(int num_sms) const {
    if (!comm_queue_ || num_sms <= 0) return max_wgs_;
    return std::min(num_sms, max_wgs_);
}

MooncakeEpXpuElasticBuffer::Half MooncakeEpXpuElasticBuffer::takeHalf(
    size_t need_bytes) {
    XPU_EP_CHECK(need_bytes <= half_stride_,
                 "ElasticBuffer too small for this shape; size it with "
                 "ElasticBuffer.get_buffer_size_hint()");
    auto* base = static_cast<uint8_t*>(ipc_->base());
    const int cur = buffer_idx_;
    buffer_idx_ ^= 1;
    const size_t ready_bytes = round_up(num_ranks_ * kLineBytes, 4096);
    return {reinterpret_cast<int*>(base + cur * half_stride_),
            reinterpret_cast<int*>(base + (cur ^ 1) * half_stride_),
            base + cur * half_stride_ + ready_bytes};
}

std::optional<XpuEventHandle> MooncakeEpXpuElasticBuffer::dispatch(
    uint64_t x_ptr, int x_element_size, uint64_t sf_ptr, int num_tokens,
    int hidden, int num_sf_packs, int sf_token_stride, int sf_hidden_stride,
    uint64_t topk_idx_ptr, int num_topk, uint64_t topk_weights_ptr,
    uint64_t active_ranks_ptr, int num_experts, int num_max_tokens_per_rank,
    int expert_alignment, int num_sms, bool do_expand,
    bool async_with_compute_stream, uint64_t compute_stream_ptr,
    bool cached_mode, int num_recv_tokens,
    uint64_t psum_num_recv_tokens_per_scaleup_rank_ptr,
    uint64_t psum_num_recv_tokens_per_expert_ptr,
    uint64_t dst_buffer_slot_idx_ptr, uint64_t token_metadata_at_forward_ptr,
    uint64_t channel_linked_list_ptr, uint64_t recv_x_ptr,
    uint64_t recv_x_scales_ptr, uint64_t recv_topk_idx_ptr,
    uint64_t recv_topk_weights_ptr, uint64_t recv_src_metadata_ptr) {
    checkAlive();
    XPU_EP_CHECK(
        token_metadata_at_forward_ptr == 0 && channel_linked_list_ptr == 0,
        "hybrid (scale-out) dispatch is not supported on XPU");
    XPU_EP_CHECK(x_element_size == 1 || x_element_size == kBf16Size,
                 "x must be BF16 or FP8");
    XPU_EP_CHECK(
        hidden > 0 && static_cast<size_t>(hidden) * x_element_size % 16 == 0,
        "a token's bytes (hidden * element size) must be a multiple "
        "of 16");
    XPU_EP_CHECK(num_topk >= 1 && num_topk <= kWarpSize,
                 "num_topk must be in [1, 32]");
    XPU_EP_CHECK(num_experts > 0 && num_experts % num_ranks_ == 0 &&
                     num_experts <= kMaxExperts,
                 "num_experts must be a positive multiple of the EP size, at "
                 "most 2048");
    XPU_EP_CHECK(num_max_tokens_per_rank > 0 && num_tokens >= 0 &&
                     num_tokens <= num_max_tokens_per_rank,
                 "num_tokens exceeds num_max_tokens_per_rank");
    XPU_EP_CHECK(expert_alignment >= 1, "expert_alignment must be positive");
    const int max_num_recv_tokens = num_ranks_ * num_max_tokens_per_rank;
    XPU_EP_CHECK(num_recv_tokens >= 0 &&
                     num_recv_tokens <= max_num_recv_tokens &&
                     (cached_mode || num_recv_tokens == max_num_recv_tokens),
                 "invalid num_recv_tokens");
    XPU_EP_CHECK(num_sf_packs >= 0, "invalid num_sf_packs");
    // Like recv_x below, recv_x_scales is empty (null) when a cached handle
    // received no tokens.
    XPU_EP_CHECK(
        num_sf_packs == 0 || ((num_tokens == 0 || sf_ptr != 0) &&
                              (num_recv_tokens == 0 || recv_x_scales_ptr != 0)),
        "FP8 dispatch needs both the scales and recv_x_scales");
    XPU_EP_CHECK(num_tokens == 0 || (x_ptr != 0 && topk_idx_ptr != 0 &&
                                     dst_buffer_slot_idx_ptr != 0),
                 "x, topk_idx and dst_buffer_slot_idx are required");
    XPU_EP_CHECK(active_ranks_ptr != 0 &&
                     psum_num_recv_tokens_per_scaleup_rank_ptr != 0 &&
                     psum_num_recv_tokens_per_expert_ptr != 0,
                 "active_ranks and the prefix-sum outputs are required");
    XPU_EP_CHECK(
        num_recv_tokens == 0 || (recv_x_ptr != 0 && recv_topk_idx_ptr != 0 &&
                                 recv_src_metadata_ptr != 0),
        "recv_x, recv_topk_idx and recv_src_metadata are required");
    sycl::queue& q = queueFor(compute_stream_ptr);

    const ElasticMsgLayout m(hidden, x_element_size, num_sf_packs, num_topk);
    const ElasticHalfLayout hl(num_ranks_, num_max_tokens_per_rank, m, hidden);

    std::lock_guard<std::mutex> lock(mu_);
    const Half half = takeHalf(hl.ready_bytes + hl.dispatch_end);
    sycl::queue& launch_q = launchQueue(q);

    ElasticDispatchArgs a{};
    a.x = reinterpret_cast<const void*>(x_ptr);
    a.sf = reinterpret_cast<const int32_t*>(sf_ptr);
    a.topk_idx = reinterpret_cast<const int64_t*>(topk_idx_ptr);
    a.topk_weights = reinterpret_cast<const float*>(topk_weights_ptr);
    a.active_ranks = reinterpret_cast<int32_t*>(active_ranks_ptr);
    a.psum_num_recv_tokens_per_rank =
        reinterpret_cast<int*>(psum_num_recv_tokens_per_scaleup_rank_ptr);
    a.psum_num_recv_tokens_per_expert =
        reinterpret_cast<int*>(psum_num_recv_tokens_per_expert_ptr);
    a.dst_buffer_slot_idx = reinterpret_cast<int*>(dst_buffer_slot_idx_ptr);
    a.recv_x = reinterpret_cast<void*>(recv_x_ptr);
    a.recv_sf = reinterpret_cast<int32_t*>(recv_x_scales_ptr);
    a.recv_topk_idx = reinterpret_cast<int64_t*>(recv_topk_idx_ptr);
    a.recv_topk_weights = reinterpret_cast<float*>(recv_topk_weights_ptr);
    a.recv_src_metadata = reinterpret_cast<int*>(recv_src_metadata_ptr);
    a.ready = half.ready;
    a.next_ready = half.next_ready;
    a.staging = half.staging;
    a.scratch = {reinterpret_cast<int*>(workspace_ + kRecvCountOffset),
                 reinterpret_cast<int*>(workspace_ + kExpertCountOffset),
                 reinterpret_cast<int*>(workspace_ + kDoneOffset)};
    a.peers = PeerView{ipc_->base(), ipc_->peerBases(), nullptr};
    a.num_tokens = num_tokens;
    a.hidden = hidden;
    a.elem_size = x_element_size;
    a.num_sf_packs = num_sf_packs;
    a.sf_token_stride = sf_token_stride;
    a.sf_hidden_stride = sf_hidden_stride;
    a.num_topk = num_topk;
    a.num_experts = num_experts;
    a.num_max_tokens_per_rank = num_max_tokens_per_rank;
    a.num_recv_tokens = num_recv_tokens;
    a.expert_alignment = expert_alignment;
    a.rank = rank_;
    a.num_ranks = num_ranks_;
    a.do_expand = do_expand;
    a.timeout_ticks = timeout_ticks_;
    a.max_wgs = maxWgs(num_sms);

    return finish(q, elastic_dispatch(launch_q, a), async_with_compute_stream);
}

std::optional<XpuEventHandle> MooncakeEpXpuElasticBuffer::combine(
    uint64_t x_ptr, int num_input_tokens, int hidden, uint64_t topk_idx_ptr,
    int num_combined_tokens, int num_topk, uint64_t topk_weights_ptr,
    uint64_t psum_num_recv_tokens_per_scaleup_rank_ptr,
    uint64_t recv_src_metadata_ptr, uint64_t token_metadata_at_forward_ptr,
    uint64_t channel_linked_list_ptr, uint64_t active_ranks_ptr,
    int num_experts, int num_max_tokens_per_rank, bool do_expand, int num_sms,
    bool async_with_compute_stream, uint64_t compute_stream_ptr,
    uint64_t combined_x_ptr) {
    (void)topk_weights_ptr;
    checkAlive();
    XPU_EP_CHECK(
        token_metadata_at_forward_ptr == 0 && channel_linked_list_ptr == 0,
        "hybrid (scale-out) combine is not supported on XPU");
    XPU_EP_CHECK(hidden > 0 && hidden % 8 == 0,
                 "hidden must be a multiple of 8");
    XPU_EP_CHECK(num_topk >= 1 && num_topk <= kWarpSize,
                 "num_topk must be in [1, 32]");
    XPU_EP_CHECK(num_experts > 0 && num_experts % num_ranks_ == 0 &&
                     num_experts <= kMaxExperts,
                 "num_experts must be a positive multiple of the EP size, at "
                 "most 2048");
    XPU_EP_CHECK(num_max_tokens_per_rank > 0 && num_combined_tokens >= 0 &&
                     num_combined_tokens <= num_max_tokens_per_rank,
                 "num_combined_tokens exceeds num_max_tokens_per_rank");
    XPU_EP_CHECK(num_input_tokens == 0 || x_ptr != 0, "x is required");
    XPU_EP_CHECK(
        num_combined_tokens == 0 || (topk_idx_ptr != 0 && combined_x_ptr != 0),
        "topk_idx and combined_x are required");
    // A rank that received no tokens has an empty (null) recv_src_metadata,
    // but must still take part in the combine.
    XPU_EP_CHECK(active_ranks_ptr != 0 &&
                     psum_num_recv_tokens_per_scaleup_rank_ptr != 0 &&
                     (num_input_tokens == 0 || recv_src_metadata_ptr != 0),
                 "active_ranks and the dispatch handle are required");
    sycl::queue& q = queueFor(compute_stream_ptr);

    const ElasticHalfLayout hl(num_ranks_, num_max_tokens_per_rank,
                               ElasticMsgLayout(hidden, kBf16Size, 0, num_topk),
                               hidden);

    std::lock_guard<std::mutex> lock(mu_);
    const Half half = takeHalf(hl.ready_bytes + hl.combine_end);
    sycl::queue& launch_q = launchQueue(q);

    ElasticCombineArgs a{};
    a.x = reinterpret_cast<const void*>(x_ptr);
    a.topk_idx = reinterpret_cast<const int64_t*>(topk_idx_ptr);
    a.psum_num_recv_tokens_per_rank =
        reinterpret_cast<const int*>(psum_num_recv_tokens_per_scaleup_rank_ptr);
    a.recv_src_metadata = reinterpret_cast<const int*>(recv_src_metadata_ptr);
    a.active_ranks = reinterpret_cast<int32_t*>(active_ranks_ptr);
    a.combined_x = reinterpret_cast<void*>(combined_x_ptr);
    a.ready = half.ready;
    a.next_ready = half.next_ready;
    a.staging = half.staging;
    a.done_counter = reinterpret_cast<int*>(workspace_ + kDoneOffset);
    a.peers = PeerView{ipc_->base(), ipc_->peerBases(), nullptr};
    a.num_input_tokens = num_input_tokens;
    a.num_combined_tokens = num_combined_tokens;
    a.hidden = hidden;
    a.num_topk = num_topk;
    a.num_experts = num_experts;
    a.num_max_tokens_per_rank = num_max_tokens_per_rank;
    a.rank = rank_;
    a.num_ranks = num_ranks_;
    a.do_expand = do_expand;
    a.timeout_ticks = timeout_ticks_;
    a.max_wgs = maxWgs(num_sms);

    return finish(q, elastic_combine(launch_q, a), async_with_compute_stream);
}

}  // namespace xpu_ep
}  // namespace mooncake
