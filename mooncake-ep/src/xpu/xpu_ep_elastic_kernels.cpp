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

#include "xpu_ep_elastic_kernels.h"

#include <algorithm>
#include <climits>

#include "xpu_ep_device.h"

namespace mooncake {
namespace xpu_ep {

namespace {

using sycl::group_barrier;

template <typename T>
XPU_EP_INLINE T* at(uint8_t* base, size_t off) {
    return reinterpret_cast<T*>(base + off);
}
template <typename T>
XPU_EP_INLINE const T* at(const uint8_t* base, size_t off) {
    return reinterpret_cast<const T*>(base + off);
}

// Work-item 0 fills s_rank_start[0..R] with the exclusive prefix sum of the
// tokens received from each source rank (so token i came from the rank s with
// s_rank_start[s] <= i < s_rank_start[s + 1]) and, if s_expert_start is given,
// s_expert_start[0..L] with that of each local expert's picks, each count
// rounded up to expert_alignment.
XPU_EP_INLINE void load_starts(sycl::nd_item<1> it, const ElasticScratch& sc,
                               int num_ranks, int num_local, int alignment,
                               int* s_rank_start, int* s_expert_start) {
    if (it.get_local_id(0) == 0) {
        int acc = 0;
        s_rank_start[0] = 0;
        for (int r = 0; r < num_ranks; ++r) {
            acc += sc.recv_count[r];
            s_rank_start[r + 1] = acc;
        }
        if (s_expert_start) {
            acc = 0;
            s_expert_start[0] = 0;
            for (int le = 0; le < num_local; ++le) {
                const int c = sc.expert_count[le];
                acc += (c + alignment - 1) / alignment * alignment;
                s_expert_start[le + 1] = acc;
            }
        }
    }
    group_barrier(it.get_group());
}

// Plans and stages this rank's tokens, then flags every rank with the number
// of tokens it gets.
//
// Work-group r < num_ranks plans destination rank r: in token order, each
// token with an expert on r gets the next slot of r's token list, which makes
// the slots deterministic. dst_buffer_slot_idx[t][k] is rank * T + slot for
// the first pick naming r and -1 for the others, as on CUDA.
// Every work-group then stages whole tokens into msgs.
sycl::event dispatch_send(sycl::queue& q, const ElasticDispatchArgs a,
                          const ElasticMsgLayout m,
                          const ElasticHalfLayout hl) {
    const int num_wgs =
        std::max(a.num_ranks, std::min(a.num_tokens, a.max_wgs));
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<int, 1> s_last(1, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(num_wgs) * kWGSize, kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int wg = static_cast<int>(it.get_group(0));
                const auto grp = it.get_group();
                const int T = a.num_max_tokens_per_rank;
                const int K = a.num_topk;
                const int N = a.num_tokens;
                const int num_local = a.num_experts / a.num_ranks;
                auto* rank_counts = at<int>(a.staging, hl.rank_counts);
                auto* rank_lists = at<int>(a.staging, hl.rank_lists);

                if (wg == 0) clean_ready(tid, a.next_ready, a.num_ranks);

                if (wg < a.num_ranks) {
                    const int r = wg;
                    int* list = rank_lists + static_cast<size_t>(r) * T;
                    int base = 0;
                    for (int t0 = 0; t0 < N; t0 += kWGSize) {
                        const int t = t0 + tid;
                        const int64_t* row =
                            a.topk_idx + static_cast<size_t>(t) * K;
                        int first_k = -1;
                        if (t < N)
                            for (int k = 0; k < K; ++k) {
                                const int64_t e = row[k];
                                if (e >= 0 && e / num_local == r) {
                                    first_k = k;
                                    break;
                                }
                            }
                        const int sel = first_k >= 0 ? 1 : 0;
                        const int slot =
                            base + sycl::exclusive_scan_over_group(
                                       grp, sel, sycl::plus<int>());
                        if (sel) list[slot] = t;
                        if (t < N && a.dst_buffer_slot_idx) {
                            int* dst = a.dst_buffer_slot_idx +
                                       static_cast<size_t>(t) * K;
                            for (int k = 0; k < K; ++k) {
                                const int64_t e = row[k];
                                if (e >= 0 && e / num_local == r)
                                    dst[k] =
                                        k == first_k ? a.rank * T + slot : -1;
                                else if (e < 0 && r == 0)
                                    dst[k] = -1;
                            }
                        }
                        base += sycl::reduce_over_group(grp, sel,
                                                        sycl::plus<int>());
                    }
                    if (tid == 0) rank_counts[r] = base;
                }

                const auto* x = static_cast<const uint8_t*>(a.x);
                for (int t = wg; t < N; t += num_wgs) {
                    uint8_t* msg =
                        a.staging + hl.msgs + static_cast<size_t>(t) * m.bytes;
                    coop_copy(msg, x + static_cast<size_t>(t) * m.data_bytes,
                              m.data_bytes, tid, kWGSize);
                    auto* sf = at<int32_t>(msg, m.sf_off);
                    for (int p = tid; p < a.num_sf_packs; p += kWGSize)
                        sf[p] =
                            a.sf[static_cast<size_t>(t) * a.sf_token_stride +
                                 static_cast<size_t>(p) * a.sf_hidden_stride];
                    if (tid == 0) *at<int32_t>(msg, m.src_off) = a.rank * T + t;
                    auto* topk = at<int32_t>(msg, m.topk_off);
                    auto* w = at<float>(msg, m.weights_off);
                    for (int k = tid; k < K; k += kWGSize) {
                        const size_t idx = static_cast<size_t>(t) * K + k;
                        topk[k] = static_cast<int32_t>(a.topk_idx[idx]);
                        w[k] = a.topk_weights ? a.topk_weights[idx] : 0.0f;
                    }
                }

                if (!arrive_last(it, a.scratch.done_counter, &s_last[0]))
                    return;
                post_ready(tid, a.peers, a.ready, a.rank, a.num_ranks,
                           rank_counts);
            });
    });
}

// A work-group per source rank s: waits for its flag, then pulls the tokens
// s listed for this rank into local_stage[s], in slot order.
sycl::event dispatch_recv(sycl::queue& q, const ElasticDispatchArgs a,
                          const ElasticMsgLayout m,
                          const ElasticHalfLayout hl) {
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<int, 1> s_flag(1, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(a.num_ranks) * kWGSize,
                              kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int warp = tid / kWarpSize;
                const int lane = tid % kWarpSize;
                const int T = a.num_max_tokens_per_rank;
                const int s = static_cast<int>(it.get_group(0));
                const uint8_t* src = rebase(a.peers, a.staging, s);

                // An unmapped peer (inactive at connect) never posts a flag;
                // an inactive rank contributes no tokens.
                if (tid == 0)
                    s_flag[0] =
                        src ? wait_flag(a.ready + s * kReadyStride,
                                        a.active_ranks, s, a.timeout_ticks)
                            : 0;
                group_acquire(it);
                const int n = s_flag[0] ? s_flag[0] - 1 : 0;
                if (tid == 0) a.scratch.recv_count[s] = n;
                if (n == 0) return;

                const int* list = at<int>(src, hl.rank_lists) +
                                  static_cast<size_t>(a.rank) * T;
                uint8_t* stage = a.staging + hl.local_stage +
                                 static_cast<size_t>(s) * T * m.bytes;
                for (int j = warp; j < n; j += kNumWarps) {
                    const int t = list[j];
                    coop_pull(stage + static_cast<size_t>(j) * m.bytes,
                              src + hl.msgs + static_cast<size_t>(t) * m.bytes,
                              m.bytes, lane, kWarpSize);
                }
            });
    });
}

// A work-group per local expert counts its picks among the received tokens.
// Expanded, it also gives each pick its position within the expert's rows, in
// received-token order, as recv_src_metadata[i][2 + k] (dispatch_copy turns
// it into a row index).
sycl::event dispatch_count(sycl::queue& q, const ElasticDispatchArgs a,
                           const ElasticMsgLayout m,
                           const ElasticHalfLayout hl) {
    const int num_local = a.num_experts / a.num_ranks;
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<int, 1> s_rank_start(a.num_ranks + 1, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(num_local) * kWGSize,
                              kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const auto grp = it.get_group();
                const int T = a.num_max_tokens_per_rank;
                const int K = a.num_topk;
                const int le = static_cast<int>(it.get_group(0));
                const int expert = a.rank * num_local + le;
                load_starts(it, a.scratch, a.num_ranks, num_local, 1,
                            &s_rank_start[0], nullptr);

                int total = 0;
                for (int s = 0; s < a.num_ranks; ++s) {
                    const int n = s_rank_start[s + 1] - s_rank_start[s];
                    const uint8_t* stage = a.staging + hl.local_stage +
                                           static_cast<size_t>(s) * T * m.bytes;
                    for (int j0 = 0; j0 < n; j0 += kWGSize) {
                        const int j = j0 + tid;
                        const int32_t* topk =
                            j < n ? at<int32_t>(stage + static_cast<size_t>(j) *
                                                            m.bytes,
                                                m.topk_off)
                                  : nullptr;
                        int cnt = 0;
                        if (topk)
                            for (int k = 0; k < K; ++k)
                                cnt += topk[k] == expert;
                        const int pos =
                            total + sycl::exclusive_scan_over_group(
                                        grp, cnt, sycl::plus<int>());
                        if (a.do_expand && cnt &&
                            s_rank_start[s] + j < a.num_recv_tokens) {
                            int* meta =
                                a.recv_src_metadata +
                                static_cast<size_t>(s_rank_start[s] + j) *
                                    (K + 2);
                            int c = 0;
                            for (int k = 0; k < K; ++k)
                                if (topk[k] == expert) meta[2 + k] = pos + c++;
                        }
                        total += sycl::reduce_over_group(grp, cnt,
                                                         sycl::plus<int>());
                    }
                }
                if (tid == 0) a.scratch.expert_count[le] = total;
            });
    });
}

// The epilogue: received token i (in source rank, then slot order) goes to
// row i, or expanded, to one row per local expert it picked, at the expert's
// aligned start plus the position dispatch_count gave it. A sub-group per
// token, lane k on pick k.
sycl::event dispatch_copy(sycl::queue& q, const ElasticDispatchArgs a,
                          const ElasticMsgLayout m,
                          const ElasticHalfLayout hl) {
    const int num_local = a.num_experts / a.num_ranks;
    const int num_wgs = std::max(
        1, std::min(a.max_wgs,
                    a.num_ranks * a.num_max_tokens_per_rank / kNumWarps));
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<int, 1> s_rank_start(a.num_ranks + 1, h);
        sycl::local_accessor<int, 1> s_expert_start(num_local + 1, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(num_wgs) * kWGSize, kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int warp = tid / kWarpSize;
                const int lane = tid % kWarpSize;
                auto sg = it.get_sub_group();
                const int wg = static_cast<int>(it.get_group(0));
                const int T = a.num_max_tokens_per_rank;
                const int K = a.num_topk;
                const int R = a.num_ranks;
                const int P = a.num_sf_packs;
                load_starts(it, a.scratch, R, num_local, a.expert_alignment,
                            &s_rank_start[0], &s_expert_start[0]);
                // A cached handle sized for fewer tokens than now arrive
                // (a different routing) must not write past its outputs.
                const int N = sycl::min(s_rank_start[R], a.num_recv_tokens);
                // The capacity mooncake_elastic_buffer.py allocates: expanded,
                // K rows per token plus alignment padding for each expert.
                const size_t out_rows =
                    a.do_expand ? static_cast<size_t>(a.num_recv_tokens) * K +
                                      static_cast<size_t>(num_local) *
                                          (a.expert_alignment - 1)
                                : static_cast<size_t>(a.num_recv_tokens);

                if (wg == 0 && tid == 0) {
                    for (int r = 0; r < R; ++r)
                        a.psum_num_recv_tokens_per_rank[r] =
                            s_rank_start[r + 1];
                    // Not expanded: the exclusive aligned prefix sum. Expanded
                    // (as CUDA leaves it after its epilogue): each expert's
                    // end, then the aligned total.
                    int* psum = a.psum_num_recv_tokens_per_expert;
                    for (int le = 0; le < num_local; ++le)
                        psum[le] =
                            s_expert_start[le] +
                            (a.do_expand ? a.scratch.expert_count[le] : 0);
                    psum[num_local] = s_expert_start[num_local];
                }

                auto* recv_x = static_cast<uint8_t*>(a.recv_x);
                for (int i = wg * kNumWarps + warp; i < N;
                     i += num_wgs * kNumWarps) {
                    int s = 0;
                    while (s_rank_start[s + 1] <= i) ++s;
                    const int j = i - s_rank_start[s];
                    const uint8_t* msg =
                        a.staging + hl.local_stage +
                        (static_cast<size_t>(s) * T + j) * m.bytes;
                    const auto* sf = at<int32_t>(msg, m.sf_off);
                    int* meta =
                        a.recv_src_metadata + static_cast<size_t>(i) * (K + 2);

                    int local = -1;
                    float w = 0.0f;
                    if (lane < K) {
                        const int g = at<int32_t>(msg, m.topk_off)[lane];
                        if (g >= a.rank * num_local &&
                            g < (a.rank + 1) * num_local)
                            local = g - a.rank * num_local;
                        w = at<float>(msg, m.weights_off)[lane];
                    }
                    const int master = sycl::reduce_over_group(
                        sg, local >= 0 ? lane : INT_MAX, sycl::minimum<int>());
                    if (lane == 0) {
                        meta[0] = *at<int32_t>(msg, m.src_off);
                        meta[1] = s * K + (master == INT_MAX ? 0 : master);
                    }

                    if (!a.do_expand) {
                        // Only here, as on CUDA: expanded, the row layout
                        // already says which expert each row is for.
                        if (lane < K)
                            a.recv_topk_idx[static_cast<size_t>(i) * K + lane] =
                                local;
                        if (lane < K && a.recv_topk_weights)
                            a.recv_topk_weights[static_cast<size_t>(i) * K +
                                                lane] = w;
                        coop_copy(
                            recv_x + static_cast<size_t>(i) * m.data_bytes, msg,
                            m.data_bytes, lane, kWarpSize);
                        for (int p = lane; p < P; p += kWarpSize)
                            a.recv_sf[static_cast<size_t>(i) * P + p] = sf[p];
                        continue;
                    }

                    int dst = -1;
                    if (lane < K) {
                        if (local >= 0) {
                            dst = s_expert_start[local] + meta[2 + lane];
                            if (static_cast<size_t>(dst) >= out_rows) dst = -1;
                        }
                        if (dst >= 0) {
                            if (a.recv_topk_weights)
                                a.recv_topk_weights[dst] = w;
                        }
                        meta[2 + lane] = dst;
                    }
                    for (int k = 0; k < K; ++k) {
                        const int d = sycl::select_from_group(sg, dst, k);
                        if (d < 0) continue;
                        coop_copy(
                            recv_x + static_cast<size_t>(d) * m.data_bytes, msg,
                            m.data_bytes, lane, kWarpSize);
                        for (int p = lane; p < P; p += kWarpSize)
                            a.recv_sf[static_cast<size_t>(d) * P + p] = sf[p];
                    }
                }
            });
    });
}

// Expert side: one BF16 row per received token i -- x[i], or expanded, the
// unweighted sum of the rows dispatch put its picks in -- staged at
// rows[source rank][source token], then flags every rank.
sycl::event combine_send(sycl::queue& q, const ElasticCombineArgs a) {
    const int num_wgs = std::max(1, a.max_wgs);
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<int, 1> s_last(1, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(num_wgs) * kWGSize, kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int warp = tid / kWarpSize;
                const int lane = tid % kWarpSize;
                const int wg = static_cast<int>(it.get_group(0));
                const int T = a.num_max_tokens_per_rank;
                const int K = a.num_topk;
                const size_t row_bytes =
                    static_cast<size_t>(a.hidden) * kBf16Size;
                const int n_vec = a.hidden / 8;  // 16-byte BF16 chunks
                const int N =
                    sycl::min(a.psum_num_recv_tokens_per_rank[a.num_ranks - 1],
                              a.num_ranks * T);
                const auto* x = static_cast<const uint8_t*>(a.x);

                if (wg == 0) clean_ready(tid, a.next_ready, a.num_ranks);

                for (int i = wg * kNumWarps + warp; i < N;
                     i += num_wgs * kNumWarps) {
                    const int* meta =
                        a.recv_src_metadata + static_cast<size_t>(i) * (K + 2);
                    uint8_t* row =
                        a.staging + static_cast<size_t>(meta[0]) * row_bytes;
                    if (!a.do_expand) {
                        coop_copy(row, x + static_cast<size_t>(i) * row_bytes,
                                  row_bytes, lane, kWarpSize);
                        continue;
                    }
                    const auto* xv = reinterpret_cast<const vec4*>(x);
                    for (int vi = lane; vi < n_vec; vi += kWarpSize) {
                        float acc[8] = {};
                        for (int k = 0; k < K; ++k) {
                            const int p = meta[2 + k];
                            if (p < 0) continue;
                            const vec4 v =
                                xv[static_cast<size_t>(p) * n_vec + vi];
                            for (int e = 0; e < 8; ++e)
                                acc[e] +=
                                    bf16_bits_to_float(static_cast<uint16_t>(
                                        (e & 1) ? (v[e / 2] >> 16)
                                                : (v[e / 2] & 0xffffu)));
                        }
                        vec4 out;
                        for (int e = 0; e < 4; ++e)
                            out[e] = static_cast<uint32_t>(
                                         float_to_bf16_bits(acc[2 * e])) |
                                     (static_cast<uint32_t>(
                                          float_to_bf16_bits(acc[2 * e + 1]))
                                      << 16);
                        reinterpret_cast<vec4*>(row)[vi] = out;
                    }
                }

                if (!arrive_last(it, a.done_counter, &s_last[0])) return;
                post_ready(tid, a.peers, a.ready, a.rank, a.num_ranks);
            });
    });
}

// Source side: a work-group per token sums, unweighted, the row each distinct
// destination rank of the token staged for it (zeros if it has none).
sycl::event combine_recv(sycl::queue& q, const ElasticCombineArgs a) {
    const int num_wgs = std::max(1, std::min(a.num_combined_tokens, a.max_wgs));
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<uint64_t, 1> s_row(kWarpSize, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(num_wgs) * kWGSize, kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int warp = tid / kWarpSize;
                const int lane = tid % kWarpSize;
                auto sg = it.get_sub_group();
                const int wg = static_cast<int>(it.get_group(0));
                const int T = a.num_max_tokens_per_rank;
                const int K = a.num_topk;
                const int num_local = a.num_experts / a.num_ranks;
                const size_t row_bytes =
                    static_cast<size_t>(a.hidden) * kBf16Size;
                const int n_vec = a.hidden / 8;

                // Wait for every rank's rows (a timed-out rank is dropped
                // from active_ranks and skipped below).
                for (int r = tid; r < a.num_ranks; r += kWGSize)
                    if (a.peers.peer_bases[r])
                        wait_flag(a.ready + r * kReadyStride, a.active_ranks, r,
                                  a.timeout_ticks);
                group_acquire(it);

                for (int t = wg; t < a.num_combined_tokens; t += num_wgs) {
                    if (warp == 0) {
                        int r = -1;
                        if (lane < K) {
                            const int64_t e =
                                a.topk_idx[static_cast<size_t>(t) * K + lane];
                            if (e >= 0) r = static_cast<int>(e / num_local);
                        }
                        bool first = r >= 0;
                        for (int j = 0; j < K; ++j) {
                            const int rj = sycl::select_from_group(sg, r, j);
                            if (j < lane && rj == r) first = false;
                        }
                        const uint8_t* src = nullptr;
                        if (first &&
                            device_atomic<int32_t>(a.active_ranks[r]).load())
                            src = rebase(
                                a.peers,
                                a.staging +
                                    (static_cast<size_t>(a.rank) * T + t) *
                                        row_bytes,
                                r);
                        s_row[lane] = reinterpret_cast<uint64_t>(src);
                    }
                    group_barrier(it.get_group());
                    for (int vi = tid; vi < n_vec; vi += kWGSize) {
                        float acc[8] = {};
                        // Rows mostly live on peers: issue a batch of loads
                        // before consuming any, as coop_pull does.
                        for (int k0 = 0; k0 < K; k0 += kPullUnroll) {
                            vec4 v[kPullUnroll];
#pragma unroll
                            for (int u = 0; u < kPullUnroll; ++u) {
                                const int k = k0 + u;
                                const uint64_t row = k < K ? s_row[k] : 0;
                                v[u] =
                                    row ? reinterpret_cast<const vec4*>(row)[vi]
                                        : vec4(0);
                            }
#pragma unroll
                            for (int u = 0; u < kPullUnroll; ++u) {
                                const int k = k0 + u;
                                if (k >= K || !s_row[k]) continue;
                                for (int e = 0; e < 8; ++e)
                                    acc[e] += bf16_bits_to_float(
                                        static_cast<uint16_t>(
                                            (e & 1) ? (v[u][e / 2] >> 16)
                                                    : (v[u][e / 2] & 0xffffu)));
                            }
                        }
                        vec4 out;
                        for (int e = 0; e < 4; ++e)
                            out[e] = static_cast<uint32_t>(
                                         float_to_bf16_bits(acc[2 * e])) |
                                     (static_cast<uint32_t>(
                                          float_to_bf16_bits(acc[2 * e + 1]))
                                      << 16);
                        reinterpret_cast<vec4*>(
                            a.combined_x)[static_cast<size_t>(t) * n_vec + vi] =
                            out;
                    }
                    group_barrier(it.get_group());
                }
            });
    });
}

}  // namespace

sycl::event elastic_dispatch(sycl::queue& q, const ElasticDispatchArgs& a) {
    const ElasticMsgLayout m(a.hidden, a.elem_size, a.num_sf_packs, a.num_topk);
    const ElasticHalfLayout hl(a.num_ranks, a.num_max_tokens_per_rank, m,
                               a.hidden);
    dispatch_send(q, a, m, hl);
    dispatch_recv(q, a, m, hl);
    dispatch_count(q, a, m, hl);
    return dispatch_copy(q, a, m, hl);
}

sycl::event elastic_combine(sycl::queue& q, const ElasticCombineArgs& a) {
    combine_send(q, a);
    return combine_recv(q, a);
}

}  // namespace xpu_ep
}  // namespace mooncake
