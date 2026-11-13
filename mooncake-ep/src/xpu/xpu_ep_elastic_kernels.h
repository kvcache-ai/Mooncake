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

// SYCL kernels of the XPU ElasticBuffer (DeepEP V2 API). Peers on this node
// are reached over Level Zero IPC and, as in the low-latency Buffer, data is
// pulled: each rank stages what it sends in its own buffer half and flags the
// peers, which read it (see BufferPair in xpu_ep_common.h for why).
//
// Peers on other nodes (PeerView::remote) cannot be read. As in the Buffer's
// hybrid path, the send kernels pack what such a peer would have pulled, and
// the host proxy writes it into that peer's buffer half over RDMA, then sets
// its flag; the receive kernels read it from their own half.

#pragma once

#include <sycl/sycl.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "xpu_ep_common.h"
#include "xpu_ep_kernels.h"

namespace mooncake {
namespace xpu_ep {

// A dispatch message, one per source token, padded to whole cache lines:
//   data[hidden * elem_size]   BF16 or FP8 values (16-byte aligned)
//   sf[num_sf_packs]           4-byte scale packs, FP8 only (16-byte aligned)
//   src_global_idx             source rank * T + source token
//   topk_idx[num_topk]         global expert ids as int32
//   topk_weights[num_topk]     float
struct ElasticMsgLayout {
    size_t data_bytes, sf_off, sf_bytes, src_off, topk_off, weights_off, bytes;
    ElasticMsgLayout(int hidden, int elem_size, int num_sf_packs,
                     int num_topk) {
        data_bytes = static_cast<size_t>(hidden) * elem_size;
        sf_off = round_up(data_bytes, 16);
        sf_bytes = static_cast<size_t>(num_sf_packs) * 4;
        src_off = sf_off + round_up(sf_bytes, 16);
        topk_off = src_off + sizeof(int32_t);
        weights_off = topk_off + static_cast<size_t>(num_topk) * 4;
        bytes = round_up(weights_off + static_cast<size_t>(num_topk) * 4,
                         kLineBytes);
    }
};

// One buffer half of the ElasticBuffer, by offset from its base:
//   ready[num_ranks]        one cache line per source rank, written by peers
//   staging, either
//     dispatch:
//       msgs[T]             this rank's token messages (read by peers)
//       rank_counts[R]      tokens this rank sends to each rank (read by peers)
//       rank_lists[R][T]    per destination rank, the tokens in slot order
//       local_stage[R][T]   messages pulled from each source, in slot order
//                           (a source on another node writes them here)
//       outbox[R][T]        hybrid: this rank's messages for each rank on
//                           another node, in slot order
//     combine:
//       rows[R][T]          per source rank and source token, the BF16 row
//                           this rank sends back (read by that source); for a
//                           source on another node, packed in its slot order
//       inbox[R][T]         hybrid: the rows each rank on another node wrote
//                           here, in this rank's slot order there
//       slots[R][T]         hybrid: each token's slot at each rank on another
//                           node (this rank's own scratch)
// Consecutive calls (dispatch or combine) alternate halves. The hybrid
// regions follow the others, so the offsets do not depend on `hybrid`.
struct ElasticHalfLayout {
    size_t ready_bytes, msgs, rank_counts, rank_lists, local_stage, outbox,
        dispatch_end, inbox, slots, combine_end, bytes;
    ElasticHalfLayout(int num_ranks, int T, const ElasticMsgLayout& msg,
                      int hidden, bool hybrid = false) {
        const size_t r = num_ranks, t = T;
        const size_t rows = r * t * static_cast<size_t>(hidden) * kBf16Size;
        ready_bytes = round_up(r * kLineBytes, 4096);
        msgs = 0;
        rank_counts = msgs + t * msg.bytes;
        rank_lists = rank_counts + round_up(r * sizeof(int), kLineBytes);
        local_stage = rank_lists + round_up(r * t * sizeof(int), kLineBytes);
        outbox = local_stage + r * t * msg.bytes;
        dispatch_end = hybrid ? outbox + r * t * msg.bytes : outbox;
        inbox = rows;
        slots = inbox + rows;
        combine_end =
            hybrid ? slots + round_up(r * t * sizeof(int), kLineBytes) : inbox;
        bytes =
            round_up(ready_bytes + std::max(dispatch_end, combine_end), 4096);
    }
};

// Local (not shared) scratch the kernels of one call pass results through.
struct ElasticScratch {
    int* recv_count;    // [num_ranks] tokens received from each source
    int* expert_count;  // [num_local_experts] picks of each local expert
    int* done_counter;  // send-kernel arrival counter (self-resetting)
};

struct ElasticDispatchArgs {
    const void* x;
    const int32_t* sf;  // 4-byte packs, element (t, p) at t * sf_token_stride
                        // + p * sf_hidden_stride
    const int64_t* topk_idx;
    const float* topk_weights;  // may be null
    int32_t* active_ranks;
    int* psum_num_recv_tokens_per_rank;    // [R] inclusive
    int* psum_num_recv_tokens_per_expert;  // [L + 1], see dispatch
    int* dst_buffer_slot_idx;              // [num_tokens][num_topk]
    void* recv_x;
    int32_t* recv_sf;  // [capacity][num_sf_packs]
    int64_t* recv_topk_idx;
    float* recv_topk_weights;  // may be null
    int* recv_src_metadata;    // [num_recv_tokens][num_topk + 2]
    int* host_counts;  // hybrid: [R] tokens sent to each rank (host memory)
    int* ready;
    int* next_ready;
    uint8_t* staging;
    ElasticScratch scratch;
    PeerView peers;
    int num_tokens, hidden, elem_size, num_sf_packs, sf_token_stride,
        sf_hidden_stride, num_topk, num_experts, num_max_tokens_per_rank,
        expert_alignment, rank, num_ranks;
    // Rows of recv_src_metadata and recv_topk_idx; recv_x and recv_sf have
    // num_topk times as many when expanded.
    int num_recv_tokens;
    bool do_expand;
    int64_t timeout_ticks;
    int max_wgs;
};

struct ElasticCombineArgs {
    const void* x;            // BF16 [num_input_tokens][hidden]
    const int64_t* topk_idx;  // [num_combined_tokens][num_topk], global ids
    const int* psum_num_recv_tokens_per_rank;  // from the dispatch handle
    const int* recv_src_metadata;              // from the dispatch handle
    int32_t* active_ranks;
    void* combined_x;  // BF16 [num_combined_tokens][hidden]
    int* host_counts;  // hybrid: [R] rows sent back to each rank (host memory)
    int* ready;
    int* next_ready;
    uint8_t* staging;
    int* done_counter;
    PeerView peers;
    int num_input_tokens, num_combined_tokens, hidden, num_topk, num_experts,
        num_max_tokens_per_rank, rank, num_ranks;
    bool do_expand;
    int64_t timeout_ticks;
    int max_wgs;
};

// Dispatch: send stages messages and posts flags carrying the token counts;
// recv pulls; the epilogue writes the outputs. Returns the last event.
sycl::event elastic_dispatch(sycl::queue& q, const ElasticDispatchArgs& a);
// Combine: send stages one row per received token (expanded: the sum of its
// local experts' rows); recv waits for every peer and sums the pulled rows.
sycl::event elastic_combine(sycl::queue& q, const ElasticCombineArgs& a);

// The same, in the two parts the hybrid path runs the proxy between: after
// the send part completes, the puts below go out; the receive part waits for
// the flags they end with. `q` must be in-order.
sycl::event elastic_dispatch_send(sycl::queue& q, const ElasticDispatchArgs& a);
sycl::event elastic_dispatch_recv(sycl::queue& q, const ElasticDispatchArgs& a);
sycl::event elastic_combine_send(sycl::queue& q, const ElasticCombineArgs& a);
sycl::event elastic_combine_recv(sycl::queue& q, const ElasticCombineArgs& a);

// After the send part with host_counts set, the RDMA writes that take what
// it packed for each rank in `ranks` (on other nodes) to where that rank's
// receive part reads it, then set its flag. `base` is peers.local_base.
std::vector<ProxyPut> elastic_dispatch_puts(const ElasticDispatchArgs& a,
                                            const void* base,
                                            const std::vector<int>& ranks);
std::vector<ProxyPut> elastic_combine_puts(const ElasticCombineArgs& a,
                                           const void* base,
                                           const std::vector<int>& ranks);

}  // namespace xpu_ep
}  // namespace mooncake
