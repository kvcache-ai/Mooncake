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

// SYCL implementation of the intra-node Mooncake EP low-latency dispatch and
// combine. It produces exactly the CUDA kernels' outputs (masked layout,
// packed_recv_count, src_info, layout_range, the handle) and keeps their
// protocol shape -- two buffer halves used alternately, the next half cleaned
// before any flag of the current call is posted, receive-side timeouts that
// clear active_ranks -- but moves data by pulling rather than pushing (see
// BufferPair in xpu_ep_common.h for why), and differs in two ways forced by
// SYCL having no cooperative grid barrier:
//
//  - A collective is a send kernel plus a receive kernel; the kernel boundary
//    is the grid sync, and the receive kernel is what a recv hook defers.
//  - The send kernel's flags are posted by whichever work-group finishes last
//    (a device-scope arrival counter), not by per-expert finish counters that
//    work-groups spin on, so nothing assumes co-resident work-groups.
//
// PCIe traffic is cut where a token picks several experts of one rank:
// dispatch moves such a token once and copies it locally, and, given the
// top-k weights at dispatch, combine sums its rows on the expert side and
// moves one row back. Results are those of the CUDA kernels up to the
// rounding of that partial sum to BF16.

#include "xpu_ep_kernels.h"

#include <algorithm>

#include "xpu_ep_common.h"

namespace mooncake {
namespace xpu_ep {

namespace {

using vec4 = sycl::vec<uint32_t, 4>;

constexpr int kWGSize = 512;
constexpr int kNumWarps = kWGSize / kWarpSize;

constexpr float kFP8Margin = 1e-4f;
constexpr float kFP8Amax = 448.0f;
constexpr float kFP8AmaxInv = 1.0f / 448.0f;

template <typename T>
using device_atomic =
    sycl::atomic_ref<T, sycl::memory_order::relaxed, sycl::memory_scope::device,
                     sycl::access::address_space::global_space>;

XPU_EP_INLINE uint8_t* rebase(const PeerView& pv, const void* local, int rank) {
    void* b = pv.peer_bases[rank];
    if (!b) return nullptr;
    return static_cast<uint8_t*>(b) +
           (static_cast<const uint8_t*>(local) -
            static_cast<const uint8_t*>(pv.local_base));
}

// Copies `bytes` (a multiple of 16) with `lanes` cooperating work-items.
XPU_EP_INLINE void coop_copy(uint8_t* dst, const uint8_t* src, size_t bytes,
                             int lane, int lanes) {
    auto* d = reinterpret_cast<vec4*>(dst);
    auto* s = reinterpret_cast<const vec4*>(src);
    for (size_t j = lane; j < bytes / sizeof(vec4); j += lanes) d[j] = s[j];
}

// coop_copy from a peer: a remote load takes a PCIe round trip, so each
// work-item issues kPullUnroll independent loads before storing any.
constexpr int kPullUnroll = 8;
XPU_EP_INLINE void coop_pull(uint8_t* dst, const uint8_t* src, size_t bytes,
                             int lane, int lanes) {
    auto* d = reinterpret_cast<vec4*>(dst);
    auto* s = reinterpret_cast<const vec4*>(src);
    const size_t n = bytes / sizeof(vec4);
    for (size_t base = lane; base < n;
         base += static_cast<size_t>(lanes) * kPullUnroll) {
        vec4 r[kPullUnroll];
#pragma unroll
        for (int u = 0; u < kPullUnroll; ++u) {
            const size_t i = base + static_cast<size_t>(u) * lanes;
            if (i < n) r[u] = s[i];
        }
#pragma unroll
        for (int u = 0; u < kPullUnroll; ++u) {
            const size_t i = base + static_cast<size_t>(u) * lanes;
            if (i < n) d[i] = r[u];
        }
    }
}

// A poll iteration takes a few thousand cycles; anything longer between two
// clock reads is the thread having been preempted and resumed.
constexpr uint64_t kMaxClockStep = 1ull << 20;

// Spins until *flag is non-zero. Returns 0 when `src_rank` is (or becomes,
// after timeout_ticks) inactive, matching the CUDA receive loop.
//
// The cycle counter is per execution unit, so a thread resumed on another
// one sees it jump in either direction; summing only short steps keeps such a
// jump from firing the timeout.
XPU_EP_INLINE int wait_flag(const int* flag, int32_t* active_ranks,
                            int src_rank, int64_t timeout_ticks) {
    device_atomic<int32_t> active(active_ranks[src_rank]);
    uint64_t prev = device_clock(), waited = 0;
    int v;
    while ((v = uc_load(flag)) == 0) {
        if (timeout_ticks >= 0) {
            const uint64_t now = device_clock();
            const uint64_t step = now - prev;
            if (step < kMaxClockStep) waited += step;
            prev = now;
            if (waited > static_cast<uint64_t>(timeout_ticks)) active.store(0);
        }
        if (active.load() == 0) return 0;
    }
    return v;
}

// Every work-group calls this once at the end of a send kernel. Returns true
// in exactly one work-group -- the last to arrive -- once every other
// work-group's staging stores have been made visible system-wide.
XPU_EP_INLINE bool arrive_last(sycl::nd_item<1> it, int* done_counter,
                               int* is_last) {
    sycl::atomic_fence(sycl::memory_order::release, sycl::memory_scope::system);
    sycl::group_barrier(it.get_group());
    if (it.get_local_id(0) == 0) {
        sycl::atomic_ref<int, sycl::memory_order::acq_rel,
                         sycl::memory_scope::device,
                         sycl::access::address_space::global_space>
            done(*done_counter);
        const int prev = done.fetch_add(1);
        const bool last = prev == static_cast<int>(it.get_group_range(0)) - 1;
        if (last) done.store(0);
        *is_last = last;
    }
    sycl::group_barrier(it.get_group());
    if (!*is_last) return false;
    sycl::atomic_fence(sycl::memory_order::acquire, sycl::memory_scope::system);
    return true;
}

// The last work-group of a send kernel tells every rank this one's staging is
// ready, by setting ready[this rank] in that rank's copy of the current half.
// The flag is 1, or with `values`, values[r] + 1 for rank r, which saves the
// receiver a remote read.
XPU_EP_INLINE void post_ready(int tid, const PeerView& pv, int* ready, int rank,
                              int num_ranks, const int* values = nullptr) {
    sycl::atomic_fence(sycl::memory_order::release, sycl::memory_scope::system);
    for (int r = tid; r < num_ranks; r += kWGSize) {
        int* flag =
            reinterpret_cast<int*>(rebase(pv, ready + rank * kReadyStride, r));
        if (flag) uc_store(flag, values ? values[r] + 1 : 1);
    }
}

XPU_EP_INLINE void clean_ready(int tid, int* next_ready, int num_ranks) {
    for (int r = tid; r < num_ranks; r += kWGSize)
        uc_store(next_ready + r * kReadyStride, 0);
}

// A pick's entry in slot_shadow: list slot | rank list position << 16.
constexpr int kNoSlot = 0xffff;

// Lists token t's picks, run by one sub-group with lane k on pick k
// (num_topk <= kWarpSize). Each destination rank gets the token once in its
// token list, and each pick goes under its expert with that list position, so
// the receiver pulls the token once whichever of its experts picked it.
XPU_EP_INLINE void list_picks(const DispatchArgs& a, sycl::sub_group sg,
                              int lane, int t, int T, int num_local, int* lists,
                              int* rank_lists, int32_t* rank_picks) {
    const bool live = lane < a.num_topk;
    const size_t k = static_cast<size_t>(t) * a.num_topk + lane;
    const int e = live ? static_cast<int>(a.topk_idx[k]) : -1;
    const int dst = e >= 0 ? e / num_local : -1;
    int first = lane;  // the first lane naming the same rank
    for (int j = a.num_topk - 1; j >= 0; --j)
        if (sycl::select_from_group(sg, dst, j) == dst && j < lane) first = j;
    int pos = 0;
    if (dst >= 0 && first == lane)
        pos = device_atomic<int>(a.counter_per_rank[dst]).fetch_add(1);
    pos = sycl::select_from_group(sg, pos, first);

    int slot = -1;
    if (e >= 0) {
        slot = device_atomic<int>(a.counter_per_expert[e]).fetch_add(1);
        // Only a token naming one expert twice can pass T picks per expert;
        // drop such extra picks rather than overrun the expert's list.
        if (slot < T)
            lists[static_cast<size_t>(e) * T + slot] = t | (pos << 16);
        else
            slot = -1;
    }
    if (live) a.slot_shadow[k] = (pos << 16) | (slot < 0 ? kNoSlot : slot);

    // This pick's index among the kept picks of the same rank, and their count.
    int index = 0, count = 0;
    for (int j = 0; j < a.num_topk; ++j) {
        const bool same = sycl::select_from_group(sg, dst, j) == dst &&
                          sycl::select_from_group(sg, slot, j) >= 0;
        count += same;
        index += same && j < lane;
    }
    if (dst < 0) return;
    const size_t entry = static_cast<size_t>(dst) * T + pos;
    if (first == lane) rank_lists[entry] = t | (count << 16);
    if (a.recv.count && slot >= 0) {
        int32_t* pick = rank_picks + (entry * kTopkShadowMaxTopk + index) * 2;
        pick[0] = (e % num_local) | (slot << 16);
        pick[1] = sycl::bit_cast<int32_t>(a.topk_weights[k]);
    }
}

template <bool kUseFP8>
sycl::event dispatch_send_impl(sycl::queue& q, const DispatchArgs a) {
    const int num_wgs = std::max(1, std::min(a.num_tokens, a.max_wgs));
    const size_t msg_bytes = dispatch_msg_bytes(a.hidden, kUseFP8);
    const DispatchStaging st(a.hidden, a.num_max_dispatch_tokens_per_rank,
                             a.num_experts, a.num_ranks);
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<int, 1> s_last(1, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(num_wgs) * kWGSize, kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int wg = static_cast<int>(it.get_group(0));
                const int warp = tid / kWarpSize;
                const int lane = tid % kWarpSize;
                auto sg = it.get_sub_group();
                const int T = a.num_max_dispatch_tokens_per_rank;
                const int num_local = a.num_experts / a.num_ranks;
                const int hidden = a.hidden;
                const int n_vec = hidden / 8;  // 16-byte BF16 chunks
                auto* counts = reinterpret_cast<int*>(a.staging + st.counts);
                auto* lists = reinterpret_cast<int*>(a.staging + st.lists);
                auto* rank_counts =
                    reinterpret_cast<int*>(a.staging + st.rank_counts);
                auto* rank_lists =
                    reinterpret_cast<int*>(a.staging + st.rank_lists);
                auto* rank_picks =
                    reinterpret_cast<int32_t*>(a.staging + st.rank_picks);

                // Clean the next half before any flag of this call is posted
                // (the last work-group posts only after this one arrives).
                if (wg == 0) {
                    clean_ready(tid, a.next_ready, a.num_ranks);
                    for (int i = tid; i < num_local; i += kWGSize)
                        a.packed_recv_count[i] = 0;
                }

                for (int t = wg; t < a.num_tokens; t += num_wgs) {
                    if (warp == 0)
                        list_picks(a, sg, lane, t, T, num_local, lists,
                                   rank_lists, rank_picks);

                    uint8_t* msg = a.staging + t * msg_bytes;
                    const auto* xv = reinterpret_cast<const vec4*>(
                        static_cast<const uint8_t*>(a.x) +
                        static_cast<size_t>(t) * hidden * kBf16Size);
                    // Sub-group-uniform trip count: the FP8 amax shuffles
                    // need every lane of the sub-group.
                    for (int base = warp * kWarpSize; base < n_vec;
                         base += kWGSize) {
                        const int i = base + lane;
                        const bool live = i < n_vec;
                        const vec4 v = live ? xv[i] : vec4(0);
                        if constexpr (!kUseFP8) {
                            if (live) reinterpret_cast<vec4*>(msg)[i] = v;
                        } else {
                            float f[8];
                            float amax = kFP8Margin;
                            for (int j = 0; j < 8; ++j) {
                                const uint32_t w = v[j / 2];
                                f[j] = bf16_bits_to_float(static_cast<uint16_t>(
                                    (j & 1) ? (w >> 16) : (w & 0xffffu)));
                                amax = sycl::fmax(amax, sycl::fabs(f[j]));
                            }
                            // 16 lanes x 8 values = one 128-channel block.
                            for (int m = 8; m >= 1; m >>= 1)
                                amax = sycl::fmax(
                                    amax,
                                    sycl::permute_group_by_xor(sg, amax, m));
                            const float scale = kFP8Amax / amax;
                            if (live) {
                                uint64_t packed = 0;
                                for (int j = 0; j < 8; ++j)
                                    packed |= static_cast<uint64_t>(
                                                  float_to_e4m3(f[j] * scale))
                                              << (8 * j);
                                reinterpret_cast<uint64_t*>(msg)[i] = packed;
                                if ((i & 15) == 0)
                                    reinterpret_cast<float*>(msg +
                                                             hidden)[i / 16] =
                                        amax * kFP8AmaxInv;
                            }
                        }
                    }
                }

                if (!arrive_last(it, a.done_counter, &s_last[0])) return;
                // The slot counters are now the number of tokens this rank
                // sends each expert and rank. Publish and reset them.
                for (int e = tid; e < a.num_experts; e += kWGSize) {
                    counts[e] = std::min(
                        device_atomic<int>(a.counter_per_expert[e]).load(), T);
                    a.counter_per_expert[e] = 0;
                }
                for (int r = tid; r < a.num_ranks; r += kWGSize) {
                    rank_counts[r] =
                        device_atomic<int>(a.counter_per_rank[r]).load();
                    a.counter_per_rank[r] = 0;
                }
                sycl::group_barrier(it.get_group());
                // Each rank's flag carries how many tokens this rank sends it.
                post_ready(tid, a.peers, a.ready, a.rank, a.num_ranks,
                           rank_counts);
            });
    });
}

// A system-scope acquire invalidates the caches this work-group reads
// through (its Xe-core's L1 and the shared L3), so the leader issuing it
// before a barrier covers every work-item -- not one per work-item, which at
// 512 lanes per work-group dominates a decode-sized receive.
XPU_EP_INLINE void group_acquire(sycl::nd_item<1> it) {
    sycl::group_barrier(it.get_group());
    if (it.get_local_id(0) == 0)
        sycl::atomic_fence(sycl::memory_order::acquire,
                           sycl::memory_scope::system);
    sycl::group_barrier(it.get_group());
}

template <bool kUseFP8>
sycl::event dispatch_recv_impl(sycl::queue& q, const DispatchArgs a) {
    // A work-group per source rank. It pulls each token that rank sent here
    // once, into this rank's local stage, then copies it to every local
    // expert row that picked it (and, given top-k weights, keeps the picks for
    // combine). Remote reads are latency-bound, so each warp prefetches what
    // it reads later -- list entries, its expert's count and list -- before
    // it pulls, and the source's flag carries the token count.
    const int num_local = a.num_experts / a.num_ranks;
    const size_t msg_bytes = dispatch_msg_bytes(a.hidden, kUseFP8);
    const size_t stage_bytes = dispatch_msg_bytes(a.hidden, false);
    const size_t hidden_bytes =
        kUseFP8 ? a.hidden : static_cast<size_t>(a.hidden) * kBf16Size;
    const DispatchStaging st(a.hidden, a.num_max_dispatch_tokens_per_rank,
                             a.num_experts, a.num_ranks);
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<int, 1> s_flag(1, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(a.num_ranks) * kWGSize,
                              kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int warp = tid / kWarpSize;
                const int lane = tid % kWarpSize;
                auto sg = it.get_sub_group();
                const int T = a.num_max_dispatch_tokens_per_rank;
                const int slots = a.num_ranks * T;
                const int src_rank = static_cast<int>(it.get_group(0));
                const uint8_t* src = rebase(a.peers, a.staging, src_rank);

                // An unmapped peer (inactive at connect) never posts a flag;
                // an inactive rank contributes no tokens.
                if (tid == 0)
                    s_flag[0] =
                        src == nullptr
                            ? 0
                            : wait_flag(a.ready + src_rank * kReadyStride,
                                        a.active_ranks, src_rank,
                                        a.timeout_ticks);
                group_acquire(it);
                const bool ok = s_flag[0] != 0;
                const int n = ok ? s_flag[0] - 1 : 0;
                if (a.recv.count && tid == 0) a.recv.count[src_rank] = n;
                if (!ok) src = nullptr;

                // Prefetch: lane i holds this warp's i-th token entry, and
                // the count and list head of the warp's first local expert.
                const int* rank_list =
                    src ? reinterpret_cast<const int*>(src + st.rank_lists) +
                              static_cast<size_t>(a.rank) * T
                        : nullptr;
                const int p_lane = warp + lane * kNumWarps;
                const int ent_lane =
                    rank_list && p_lane < n ? rank_list[p_lane] : 0;
                const int le0 = warp;
                const int* list0 =
                    src && le0 < num_local
                        ? reinterpret_cast<const int*>(src + st.lists) +
                              static_cast<size_t>(a.rank * num_local + le0) * T
                        : nullptr;
                const int cnt0 = sycl::group_broadcast(
                    sg,
                    list0 && lane == 0
                        ? reinterpret_cast<const int*>(
                              src + st.counts)[a.rank * num_local + le0]
                        : 0,
                    0);
                const int list0_lane = lane < cnt0 ? list0[lane] : 0;

                uint8_t* stage =
                    a.staging + st.local_stage +
                    static_cast<size_t>(src_rank) * T * stage_bytes;
                const int pick_ints = 2 * a.num_topk;
                for (int i = 0, p = warp; p < n; ++i, p += kNumWarps) {
                    const size_t entry = static_cast<size_t>(a.rank) * T + p;
                    const int tok =
                        i < kWarpSize
                            ? sycl::select_from_group(sg, ent_lane, i)
                            : sycl::group_broadcast(
                                  sg, lane == 0 ? rank_list[p] : 0, 0);
                    // Load the picks with the pull, store them after it.
                    int pick_a = 0, pick_b = 0;
                    const int32_t* picks = nullptr;
                    if (a.recv.count) {
                        picks = reinterpret_cast<const int32_t*>(
                                    src + st.rank_picks) +
                                entry * kTopkShadowMaxTopk * 2;
                        if (lane < pick_ints) pick_a = picks[lane];
                        if (lane + kWarpSize < pick_ints)
                            pick_b = picks[lane + kWarpSize];
                    }
                    coop_pull(stage + p * stage_bytes,
                              src + (tok & 0xffff) * msg_bytes, msg_bytes, lane,
                              kWarpSize);
                    if (a.recv.count) {
                        const int np = tok >> 16;
                        const size_t idx = static_cast<size_t>(src_rank) *
                                               kTopkShadowMaxTokens +
                                           p;
                        int32_t* dst =
                            a.recv.picks + idx * kTopkShadowMaxTopk * 2;
                        if (lane == 0) a.recv.npicks[idx] = np;
                        if (lane < 2 * np) dst[lane] = pick_a;
                        if (lane + kWarpSize < 2 * np)
                            dst[lane + kWarpSize] = pick_b;
                    }
                }
                sycl::group_barrier(it.get_group());

                const int num_scales = a.hidden / kNumPerChannels;
                for (int le = warp; le < num_local; le += kNumWarps) {
                    const bool first = le == le0;
                    const int expert = a.rank * num_local + le;
                    const int* list =
                        src ? reinterpret_cast<const int*>(src + st.lists) +
                                  static_cast<size_t>(expert) * T
                            : nullptr;
                    int cnt = 0, begin = 0;
                    if (lane == 0) {
                        if (src)
                            cnt = first ? cnt0
                                        : reinterpret_cast<const int*>(
                                              src + st.counts)[expert];
                        begin = device_atomic<int>(a.packed_recv_count[le])
                                    .fetch_add(cnt);
                        a.packed_recv_layout_range[le * a.num_ranks +
                                                   src_rank] =
                            pack_range(cnt, begin);
                    }
                    cnt = sycl::group_broadcast(sg, cnt, 0);
                    begin = sycl::group_broadcast(sg, begin, 0);

                    int lane_e = first ? list0_lane : 0;
                    for (int j = 0; j < cnt; ++j) {
                        // Fetch the list kWarpSize entries at a time.
                        if (j % kWarpSize == 0 && !(first && j == 0))
                            lane_e = j + lane < cnt ? list[j + lane] : 0;
                        const int e =
                            sycl::select_from_group(sg, lane_e, j % kWarpSize);
                        const uint8_t* msg = stage + (e >> 16) * stage_bytes;
                        const size_t row =
                            static_cast<size_t>(le) * slots + begin + j;
                        if (lane == 0) a.packed_recv_src_info[row] = e & 0xffff;
                        coop_copy(static_cast<uint8_t*>(a.packed_recv_x) +
                                      row * hidden_bytes,
                                  msg, hidden_bytes, lane, kWarpSize);
                        if constexpr (kUseFP8) {
                            const auto* src_scales =
                                reinterpret_cast<const float*>(msg + a.hidden);
                            float* dst_scales =
                                a.packed_recv_x_scales +
                                static_cast<size_t>(le) * slots * num_scales +
                                begin + j;
                            for (int s = lane; s < num_scales; s += kWarpSize)
                                dst_scales[static_cast<size_t>(s) * slots] =
                                    src_scales[s];
                        }
                    }
                }
            });
    });
}

// Per source rank and token it sent here, the weighted sum of this rank's
// expert rows for that token: the source then pulls one row per rank rather
// than one per pick. Rows are staged as row[src][position in its rank list].
// Like combine_recv, a work-group splits one row and issues every pick's
// loads before using any, so a token costs about one memory round trip.
sycl::event combine_send_reduced(sycl::queue& q, const CombineArgs a) {
    const int T = a.num_max_dispatch_tokens_per_rank;
    const int groups = std::max(1, std::min(T, a.max_wgs / a.num_ranks));
    const int num_wgs = a.num_ranks * groups;
    const size_t row_bytes = static_cast<size_t>(a.hidden) * kBf16Size;
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<int, 1> s_last(1, h);
        sycl::local_accessor<uint64_t, 1> s_row(kTopkShadowMaxTopk, h);
        sycl::local_accessor<float, 1> s_w(kTopkShadowMaxTopk, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(num_wgs) * kWGSize, kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int wg = static_cast<int>(it.get_group(0));
                const int src_rank = wg / groups;
                const int slots = a.num_ranks * T;
                const int n_vec = a.hidden / 8;

                if (wg == 0) clean_ready(tid, a.next_ready, a.num_ranks);

                const int n = a.recv.count[src_rank];
                for (int p = wg % groups; p < n; p += groups) {
                    const size_t idx =
                        static_cast<size_t>(src_rank) * kTopkShadowMaxTokens +
                        p;
                    const int np = a.recv.npicks[idx];
                    if (tid < np) {
                        const int32_t* pick =
                            a.recv.picks + (idx * kTopkShadowMaxTopk + tid) * 2;
                        const int le = pick[0] & 0xffff;
                        const int slot = static_cast<uint32_t>(pick[0]) >> 16;
                        int cnt, begin;
                        unpack_range(
                            a.layout_range[le * a.num_ranks + src_rank], cnt,
                            begin);
                        s_row[tid] = reinterpret_cast<uint64_t>(
                            static_cast<const uint8_t*>(a.x) +
                            (static_cast<size_t>(le) * slots + begin + slot) *
                                row_bytes);
                        s_w[tid] = sycl::bit_cast<float>(pick[1]);
                    }
                    sycl::group_barrier(it.get_group());
                    auto* out = reinterpret_cast<vec4*>(
                        a.staging +
                        (static_cast<size_t>(src_rank) * T + p) * row_bytes);
                    for (int vi = tid; vi < n_vec; vi += kWGSize) {
                        float acc[8] = {};
                        for (int k0 = 0; k0 < np; k0 += kPullUnroll) {
                            vec4 v[kPullUnroll];
#pragma unroll
                            for (int u = 0; u < kPullUnroll; ++u)
                                if (k0 + u < np)
                                    v[u] = reinterpret_cast<const vec4*>(
                                        s_row[k0 + u])[vi];
#pragma unroll
                            for (int u = 0; u < kPullUnroll; ++u) {
                                if (k0 + u >= np) continue;
                                const float w = s_w[k0 + u];
                                for (int j = 0; j < 8; ++j)
                                    acc[j] +=
                                        w *
                                        bf16_bits_to_float(
                                            static_cast<uint16_t>(
                                                (j & 1)
                                                    ? (v[u][j / 2] >> 16)
                                                    : (v[u][j / 2] & 0xffffu)));
                            }
                        }
                        vec4 o;
                        for (int j = 0; j < 4; ++j)
                            o[j] = static_cast<uint32_t>(
                                       float_to_bf16_bits(acc[2 * j])) |
                                   (static_cast<uint32_t>(
                                        float_to_bf16_bits(acc[2 * j + 1]))
                                    << 16);
                        out[vi] = o;
                    }
                    sycl::group_barrier(it.get_group());
                }

                if (!arrive_last(it, a.done_counter, &s_last[0])) return;
                post_ready(tid, a.peers, a.ready, a.rank, a.num_ranks);
            });
    });
}

// Where combine reads pick k of token t back from, and with what weight, into
// s_row[k] and s_w[k] (row 0: nothing to add). Run by one sub-group with lane
// k on pick k. Pre-reduced rows already carry the weights and hold every pick
// of one rank, so each rank is read once, at the token's first pick of it.
XPU_EP_INLINE void locate_rows(const CombineArgs& a, sycl::sub_group sg,
                               int lane, int t, int T, int num_local,
                               size_t row_bytes, uint64_t* s_row, float* s_w) {
    const bool live = lane < a.num_topk;
    const size_t k = static_cast<size_t>(t) * a.num_topk + lane;
    const int e = live ? static_cast<int>(a.topk_idx[k]) : -1;
    const int packed = live ? a.slot_shadow[k] : 0;
    const int dst = e >= 0 ? e / num_local : -1;
    bool add = e >= 0;
    size_t offset;
    if (a.recv.count) {
        for (int j = 0; j < a.num_topk; ++j)
            if (sycl::select_from_group(sg, dst, j) == dst && j < lane)
                add = false;
        offset = (static_cast<size_t>(a.rank) * T +
                  (static_cast<uint32_t>(packed) >> 16)) *
                 row_bytes;
    } else {
        const int slot = packed & 0xffff;
        add = add && slot != kNoSlot;
        offset =
            ((static_cast<size_t>(e % num_local) * a.num_ranks + a.rank) * T +
             slot) *
            row_bytes;
    }
    uint64_t row = 0;
    if (add) {
        const uint8_t* base = rebase(a.peers, a.staging, dst);
        if (base && device_atomic<int32_t>(a.active_ranks[dst]).load())
            row = reinterpret_cast<uint64_t>(base + offset);
    }
    if (live) {
        s_row[lane] = row;
        s_w[lane] = a.recv.count ? 1.0f : a.topk_weights[k];
    }
}

}  // namespace

sycl::event dispatch_send(sycl::queue& q, const DispatchArgs& args) {
    return args.use_fp8 ? dispatch_send_impl<true>(q, args)
                        : dispatch_send_impl<false>(q, args);
}

sycl::event dispatch_recv(sycl::queue& q, const DispatchArgs& args) {
    return args.use_fp8 ? dispatch_recv_impl<true>(q, args)
                        : dispatch_recv_impl<false>(q, args);
}

sycl::event combine_send(sycl::queue& q, const CombineArgs& args) {
    if (args.recv.count) return combine_send_reduced(q, args);
    const CombineArgs a = args;
    // One work-group per (source rank, local expert): stage the rows that
    // source will pull, in the order its dispatch listed them.
    const int num_wgs = a.num_experts;
    const size_t row_bytes = static_cast<size_t>(a.hidden) * kBf16Size;
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<int, 1> s_last(1, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(num_wgs) * kWGSize, kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int warp = tid / kWarpSize;
                const int lane = tid % kWarpSize;
                const int T = a.num_max_dispatch_tokens_per_rank;
                const int num_local = a.num_experts / a.num_ranks;
                const int slots = a.num_ranks * T;
                const int p = static_cast<int>(it.get_group(0));
                const int src_rank = p / num_local;
                const int le = p % num_local;

                if (p == 0) clean_ready(tid, a.next_ready, a.num_ranks);

                int cnt, begin;
                unpack_range(a.layout_range[le * a.num_ranks + src_rank], cnt,
                             begin);
                uint8_t* rows =
                    a.staging +
                    (static_cast<size_t>(le) * a.num_ranks + src_rank) * T *
                        row_bytes;
                const uint8_t* x =
                    static_cast<const uint8_t*>(a.x) +
                    (static_cast<size_t>(le) * slots + begin) * row_bytes;
                for (int j = warp; j < cnt; j += kNumWarps)
                    coop_copy(rows + j * row_bytes, x + j * row_bytes,
                              row_bytes, lane, kWarpSize);

                if (!arrive_last(it, a.done_counter, &s_last[0])) return;
                post_ready(tid, a.peers, a.ready, a.rank, a.num_ranks);
            });
    });
}

sycl::event combine_recv(sycl::queue& q, const CombineArgs& args) {
    const CombineArgs a = args;
    const int num_wgs = std::max(1, std::min(a.num_combined_tokens, a.max_wgs));
    const size_t row_bytes = static_cast<size_t>(a.hidden) * kBf16Size;
    return q.submit([&](sycl::handler& h) {
        sycl::local_accessor<uint64_t, 1> s_row(kTopkShadowMaxTopk, h);
        sycl::local_accessor<float, 1> s_w(kTopkShadowMaxTopk, h);
        h.parallel_for(
            sycl::nd_range<1>(static_cast<size_t>(num_wgs) * kWGSize, kWGSize),
            [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
                const int tid = static_cast<int>(it.get_local_id(0));
                const int warp = tid / kWarpSize;
                const int lane = tid % kWarpSize;
                auto sg = it.get_sub_group();
                const int wg = static_cast<int>(it.get_group(0));
                const int T = a.num_max_dispatch_tokens_per_rank;
                const int num_local = a.num_experts / a.num_ranks;
                const int n_vec = a.hidden / 8;

                // Wait for every rank's rows (a timed-out rank is dropped
                // from active_ranks and skipped below).
                for (int r = tid; r < a.num_ranks; r += kWGSize)
                    if (a.peers.peer_bases[r])
                        wait_flag(a.ready + r * kReadyStride, a.active_ranks, r,
                                  a.timeout_ticks);
                group_acquire(it);

                for (int t = wg; t < a.num_combined_tokens; t += num_wgs) {
                    if (warp == 0)
                        locate_rows(a, sg, lane, t, T, num_local, row_bytes,
                                    &s_row[0], &s_w[0]);
                    sycl::group_barrier(it.get_group());
                    for (int vi = tid; vi < n_vec; vi += kWGSize) {
                        float acc[8] = {};
                        // Rows mostly live on peers: issue a batch of loads
                        // before consuming any, as coop_pull does.
                        for (int k0 = 0; k0 < a.num_topk; k0 += kPullUnroll) {
                            vec4 v[kPullUnroll];
#pragma unroll
                            for (int u = 0; u < kPullUnroll; ++u) {
                                const int k = k0 + u;
                                const uint64_t row =
                                    k < a.num_topk ? s_row[k] : 0;
                                v[u] =
                                    row ? reinterpret_cast<const vec4*>(row)[vi]
                                        : vec4(0);
                            }
#pragma unroll
                            for (int u = 0; u < kPullUnroll; ++u) {
                                const int k = k0 + u;
                                if (k >= a.num_topk || !s_row[k]) continue;
                                const float w = s_w[k];
                                for (int j = 0; j < 8; ++j) {
                                    const uint32_t word = v[u][j / 2];
                                    acc[j] +=
                                        w *
                                        bf16_bits_to_float(
                                            static_cast<uint16_t>(
                                                (j & 1) ? (word >> 16)
                                                        : (word & 0xffffu)));
                                }
                            }
                        }
                        vec4 out;
                        for (int j = 0; j < 4; ++j)
                            out[j] = static_cast<uint32_t>(
                                         float_to_bf16_bits(acc[2 * j])) |
                                     (static_cast<uint32_t>(
                                          float_to_bf16_bits(acc[2 * j + 1]))
                                      << 16);
                        reinterpret_cast<vec4*>(
                            static_cast<uint8_t*>(a.combined_x) +
                            static_cast<size_t>(t) * a.hidden * kBf16Size)[vi] =
                            out;
                    }
                    sycl::group_barrier(it.get_group());
                }
            });
    });
}

}  // namespace xpu_ep
}  // namespace mooncake
