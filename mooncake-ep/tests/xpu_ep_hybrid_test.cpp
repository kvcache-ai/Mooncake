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

// Checks the hybrid path's layout on one GPU, without a NIC: every rank of a
// simulated EP group gets its own buffer on this device, ranks on the same
// simulated node map each other directly, and the proxy's RDMA writes
// (dispatch_puts, combine_puts) are replayed as device copies. Each rank's
// kernels run one after another -- every send kernel, the writes, then every
// receive kernel -- so a receive never waits on a flag not yet set.
//
// Dispatch is checked against a host reference (exactly for BF16), and both
// dispatch and combine against the same group with every rank on one node.

#include <sycl/sycl.hpp>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "xpu_ep_common.h"
#include "xpu_ep_kernels.h"

using namespace mooncake::xpu_ep;

namespace {

struct Config {
    int num_ranks, num_experts, hidden, num_topk, T;
    bool fp8, weights;
    int nodes;        // ranks are split into this many simulated nodes
    bool interleave;  // node of rank r: r % nodes (else contiguous blocks)
};

struct Inputs {
    std::vector<std::vector<uint16_t>> x;  // [rank][T][hidden] BF16
    std::vector<std::vector<int64_t>> topk;
    std::vector<std::vector<float>> w;
    std::vector<int> tokens;
};

struct Outputs {
    std::vector<std::vector<int>> count, src_info;
    std::vector<std::vector<int64_t>> range;
    std::vector<std::vector<uint8_t>> rx;
    std::vector<std::vector<float>> scales;
    std::vector<std::vector<uint16_t>> combined;
    std::vector<int> active;
};

uint16_t to_bf16(float f) {
    uint32_t u;
    std::memcpy(&u, &f, 4);
    return static_cast<uint16_t>((u + 0x7fff + ((u >> 16) & 1)) >> 16);
}

float from_bf16(uint16_t b) {
    const uint32_t u = static_cast<uint32_t>(b) << 16;
    float f;
    std::memcpy(&f, &u, 4);
    return f;
}

Inputs make_inputs(const Config& c, std::mt19937& rng) {
    Inputs in;
    std::uniform_real_distribution<float> val(-2.f, 2.f), unit(0.f, 1.f);
    for (int r = 0; r < c.num_ranks; ++r) {
        const int n = r == 1 ? 0 : c.T - static_cast<int>(rng() % (c.T / 4));
        in.tokens.push_back(n);
        std::vector<uint16_t> x(static_cast<size_t>(c.T) * c.hidden);
        for (auto& v : x) v = to_bf16(val(rng));
        std::vector<int64_t> topk(static_cast<size_t>(c.T) * c.num_topk, -1);
        std::vector<float> w(topk.size(), 0.f);
        for (int t = 0; t < n; ++t) {
            std::vector<int> e(c.num_experts);
            for (int i = 0; i < c.num_experts; ++i) e[i] = i;
            std::shuffle(e.begin(), e.end(), rng);
            for (int k = 0; k < c.num_topk; ++k) {
                const size_t i = static_cast<size_t>(t) * c.num_topk + k;
                topk[i] = unit(rng) < 0.1f ? -1 : e[k];
                w[i] = unit(rng);
            }
        }
        in.x.push_back(std::move(x));
        in.topk.push_back(std::move(topk));
        in.w.push_back(std::move(w));
    }
    return in;
}

int node_of(const Config& c, int r) {
    return c.interleave ? r % c.nodes
                        : r / ((c.num_ranks + c.nodes - 1) / c.nodes);
}

void apply(sycl::queue& q, const std::vector<ProxyPut>& puts,
           const std::vector<uint8_t*>& bufs, int src) {
    for (const ProxyPut& p : puts) {
        if (p.bytes)
            q.memcpy(bufs[p.rank] + p.remote_offset, bufs[src] + p.offset,
                     p.bytes);
        else
            q.fill(reinterpret_cast<int*>(bufs[p.rank] + p.remote_offset),
                   static_cast<int>(p.value), 1);
    }
    q.wait();
}

template <typename T>
std::vector<T> to_host(sycl::queue& q, const T* p, size_t n) {
    std::vector<T> v(n);
    if (n) q.memcpy(v.data(), p, n * sizeof(T)).wait();
    return v;
}

// One dispatch and one combine, twice (both buffer halves), with
// `hybrid` ranks on other simulated nodes reached through replayed puts.
Outputs run(sycl::queue& q, const Config& c, const Inputs& in, bool hybrid) {
    const int R = c.num_ranks, E = c.num_experts, T = c.T, H = c.hidden;
    const int L = E / R;
    const size_t slots = static_cast<size_t>(R) * T;
    const size_t msg_hidden = c.fp8 ? H : static_cast<size_t>(H) * 2;
    const int max_wgs = static_cast<int>(
        q.get_device().get_info<sycl::info::device::max_compute_units>());
    const size_t bytes = get_ep_buffer_size_hint(T, H, R, E);

    std::vector<uint8_t*> bufs(R);
    std::vector<void**> peer_tables(R);
    std::vector<int*> remote_tables(R);
    std::vector<std::vector<int>> remote_ranks(R);
    for (int r = 0; r < R; ++r) {
        bufs[r] = sycl::malloc_device<uint8_t>(bytes, q);
        q.memset(bufs[r], 0, bytes);
    }
    for (int r = 0; r < R; ++r) {
        std::vector<void*> peers(R, nullptr);
        std::vector<int> remote(R, 0);
        for (int s = 0; s < R; ++s) {
            if (!hybrid || node_of(c, s) == node_of(c, r)) {
                peers[s] = bufs[s];
            } else {
                remote[s] = 1;
                remote_ranks[r].push_back(s);
            }
        }
        peer_tables[r] = sycl::malloc_device<void*>(R, q);
        remote_tables[r] = sycl::malloc_device<int>(R, q);
        q.memcpy(peer_tables[r], peers.data(), R * sizeof(void*));
        q.memcpy(remote_tables[r], remote.data(), R * sizeof(int));
        q.wait();  // before the host tables go out of scope
    }

    struct Rank {
        uint16_t* x;
        int64_t* topk;
        float* w;
        int* counters;  // [E] experts, then [R] ranks, then 2 done counters
        int32_t* slot_shadow;
        int32_t* table;
        int* host;
        int32_t* active;
        uint8_t* rx;
        float* scales;
        int *count, *src_info;
        int64_t* range;
        uint16_t* combined;
    };
    std::vector<Rank> ranks(R);
    const size_t host_ints = kHostCellsOffset + E;
    for (int r = 0; r < R; ++r) {
        Rank& k = ranks[r];
        k.x = sycl::malloc_device<uint16_t>(in.x[r].size(), q);
        k.topk = sycl::malloc_device<int64_t>(in.topk[r].size(), q);
        k.w = sycl::malloc_device<float>(in.w[r].size(), q);
        q.memcpy(k.x, in.x[r].data(), in.x[r].size() * 2);
        q.memcpy(k.topk, in.topk[r].data(), in.topk[r].size() * 8);
        q.memcpy(k.w, in.w[r].data(), in.w[r].size() * 4);
        k.counters = sycl::malloc_device<int>(E + R + 2, q);
        q.memset(k.counters, 0, (E + R + 2) * sizeof(int));
        k.slot_shadow = sycl::malloc_device<int32_t>(
            static_cast<size_t>(T) * c.num_topk, q);
        k.table = sycl::malloc_device<int32_t>(recv_table_ints(R), q);
        k.host = sycl::malloc_host<int>(2 * host_ints, q);
        k.active = sycl::malloc_device<int32_t>(R, q);
        q.fill(k.active, 1, R);
        k.rx = sycl::malloc_device<uint8_t>(L * slots * msg_hidden, q);
        k.scales = sycl::malloc_device<float>(L * slots * (H / 128), q);
        k.count = sycl::malloc_device<int>(L, q);
        k.src_info = sycl::malloc_device<int>(L * slots, q);
        k.range = sycl::malloc_device<int64_t>(L * R, q);
        k.combined =
            sycl::malloc_device<uint16_t>(static_cast<size_t>(T) * H, q);
    }
    q.wait();

    Outputs out;
    int half = 0;
    const int64_t timeout = 4'000'000'000;  // ~2 s: a bug drops a rank
    for (int iter = 0; iter < 2; ++iter) {
        std::vector<DispatchArgs> d(R);
        for (int r = 0; r < R; ++r) {
            Rank& k = ranks[r];
            BufferPair bp(bufs[r], T, H, R, E, hybrid);
            DispatchArgs& a = d[r];
            a = DispatchArgs{};
            a.x = k.x;
            a.topk_idx = k.topk;
            a.topk_weights = k.w;
            a.active_ranks = k.active;
            a.packed_recv_x = k.rx;
            a.packed_recv_x_scales = k.scales;
            a.packed_recv_src_info = k.src_info;
            a.packed_recv_layout_range = k.range;
            a.packed_recv_count = k.count;
            a.ready = bp.halves[half].ready;
            a.next_ready = bp.halves[half ^ 1].ready;
            a.staging = bp.halves[half].staging;
            a.slot_shadow = k.slot_shadow;
            a.counter_per_expert = k.counters;
            a.counter_per_rank = k.counters + E;
            a.done_counter = k.counters + E + R;
            if (hybrid) a.host_counts = k.host + half * host_ints;
            if (c.weights)
                a.recv = {k.table, k.table + R,
                          k.table + R +
                              static_cast<size_t>(R) * kTopkShadowMaxTokens};
            a.peers = {bufs[r], peer_tables[r],
                       hybrid ? remote_tables[r] : nullptr};
            a.num_tokens = in.tokens[r];
            a.hidden = H;
            a.num_topk = c.num_topk;
            a.num_max_dispatch_tokens_per_rank = T;
            a.num_experts = E;
            a.rank = r;
            a.num_ranks = R;
            a.use_fp8 = c.fp8;
            a.timeout_ticks = timeout;
            a.max_wgs = max_wgs;
            dispatch_send(q, a).wait();
        }
        if (hybrid)
            for (int r = 0; r < R; ++r)
                apply(q, dispatch_puts(d[r], bufs[r], remote_ranks[r]), bufs,
                      r);
        for (int r = 0; r < R; ++r) dispatch_recv(q, d[r]).wait();
        half ^= 1;
        if (c.fp8) continue;  // combine reads BF16 expert rows

        std::vector<CombineArgs> cb(R);
        for (int r = 0; r < R; ++r) {
            Rank& k = ranks[r];
            BufferPair bp(bufs[r], T, H, R, E, hybrid);
            CombineArgs& a = cb[r];
            a = CombineArgs{};
            a.x = k.rx;  // identity experts
            a.topk_idx = k.topk;
            a.slot_shadow = k.slot_shadow;
            a.topk_weights = k.w;
            a.layout_range = k.range;
            a.active_ranks = k.active;
            a.combined_x = k.combined;
            a.ready = bp.halves[half].ready;
            a.next_ready = bp.halves[half ^ 1].ready;
            a.staging = bp.halves[half].staging;
            if (hybrid) {
                a.inbox = a.staging + bp.combine_bytes;
                a.host_counts = k.host + half * host_ints;
            }
            a.done_counter = k.counters + E + R + 1;
            a.recv = d[r].recv;
            a.peers = d[r].peers;
            a.num_combined_tokens = in.tokens[r];
            a.hidden = H;
            a.num_topk = c.num_topk;
            a.num_max_dispatch_tokens_per_rank = T;
            a.num_experts = E;
            a.rank = r;
            a.num_ranks = R;
            a.timeout_ticks = timeout;
            a.max_wgs = max_wgs;
            combine_send(q, a).wait();
        }
        if (hybrid)
            for (int r = 0; r < R; ++r)
                apply(q, combine_puts(cb[r], bufs[r], remote_ranks[r]), bufs,
                      r);
        for (int r = 0; r < R; ++r) combine_recv(q, cb[r]).wait();
        half ^= 1;
    }

    for (int r = 0; r < R; ++r) {
        Rank& k = ranks[r];
        out.count.push_back(to_host(q, k.count, L));
        out.src_info.push_back(to_host(q, k.src_info, L * slots));
        out.range.push_back(to_host(q, k.range, L * R));
        out.rx.push_back(to_host(q, k.rx, L * slots * msg_hidden));
        out.scales.push_back(to_host(q, k.scales, L * slots * (H / 128)));
        out.combined.push_back(
            to_host(q, k.combined, static_cast<size_t>(in.tokens[r]) * H));
        const auto act = to_host(q, k.active, R);
        out.active.push_back(std::count(act.begin(), act.end(), 1));
        for (void* p :
             {static_cast<void*>(k.x), static_cast<void*>(k.topk),
              static_cast<void*>(k.w), static_cast<void*>(k.counters),
              static_cast<void*>(k.slot_shadow), static_cast<void*>(k.table),
              static_cast<void*>(k.host), static_cast<void*>(k.active),
              static_cast<void*>(k.rx), static_cast<void*>(k.scales),
              static_cast<void*>(k.count), static_cast<void*>(k.src_info),
              static_cast<void*>(k.range), static_cast<void*>(k.combined)})
            sycl::free(p, q);
        sycl::free(bufs[r], q);
        sycl::free(peer_tables[r], q);
        sycl::free(remote_tables[r], q);
    }
    return out;
}

// Rows of (local expert, source) as (token, bytes, scales), sorted by token:
// their order within the segment depends on work-group timing.
struct Row {
    int token;
    std::vector<uint8_t> data;
    std::vector<float> scales;
    bool operator<(const Row& o) const { return token < o.token; }
    bool operator==(const Row& o) const {
        return token == o.token && data == o.data && scales == o.scales;
    }
};

std::vector<Row> segment(const Config& c, const Outputs& o, int r, int le,
                         int src) {
    const size_t slots = static_cast<size_t>(c.num_ranks) * c.T;
    const size_t mh = c.fp8 ? c.hidden : static_cast<size_t>(c.hidden) * 2;
    const int ns = c.hidden / 128;
    int cnt, begin;
    unpack_range(o.range[r][le * c.num_ranks + src], cnt, begin);
    std::vector<Row> rows;
    for (int j = 0; j < cnt; ++j) {
        const size_t row = le * slots + begin + j;
        Row x{o.src_info[r][row],
              std::vector<uint8_t>(o.rx[r].begin() + row * mh,
                                   o.rx[r].begin() + (row + 1) * mh),
              {}};
        if (c.fp8)
            for (int s = 0; s < ns; ++s)
                x.scales.push_back(
                    o.scales[r][le * slots * ns + s * slots + begin + j]);
        rows.push_back(std::move(x));
    }
    std::sort(rows.begin(), rows.end());
    return rows;
}

int check(const Config& c, const Inputs& in, const Outputs& a,
          const Outputs& b) {
    const int R = c.num_ranks, L = c.num_experts / R;
    int errors = 0;
    auto fail = [&](const std::string& what) {
        if (errors++ < 10) std::printf("    FAIL %s\n", what.c_str());
    };
    for (int r = 0; r < R; ++r) {
        if (a.active[r] != R || b.active[r] != R)
            fail("rank " + std::to_string(r) + " dropped a peer");
        for (int le = 0; le < L; ++le) {
            if (a.count[r][le] != b.count[r][le])
                fail("count r" + std::to_string(r) + " le" +
                     std::to_string(le));
            const int expert = r * L + le;
            for (int s = 0; s < R; ++s) {
                const auto sa = segment(c, a, r, le, s);
                const auto sb = segment(c, b, r, le, s);
                const std::string where = " r" + std::to_string(r) + " le" +
                                          std::to_string(le) + " src" +
                                          std::to_string(s);
                if (!(sa == sb))
                    fail("hybrid differs from one node at" + where);
                // Host reference: which tokens, and for BF16 their bytes.
                std::vector<int> want;
                for (int t = 0; t < in.tokens[s]; ++t)
                    for (int k = 0; k < c.num_topk; ++k)
                        if (in.topk[s][t * c.num_topk + k] == expert)
                            want.push_back(t);
                if (sb.size() != want.size()) {
                    fail("token count at" + where);
                    continue;
                }
                for (size_t i = 0; i < want.size(); ++i) {
                    if (sb[i].token != want[i]) fail("token at" + where);
                    if (!c.fp8 &&
                        std::memcmp(sb[i].data.data(),
                                    in.x[s].data() +
                                        static_cast<size_t>(want[i]) * c.hidden,
                                    c.hidden * 2) != 0)
                        fail("row bytes at" + where);
                }
            }
        }
        if (c.fp8) continue;
        if (a.combined[r] != b.combined[r])
            fail("combined differs from one node, rank " + std::to_string(r));
        // Identity experts: combined = sum of weights (or picks) times x.
        for (int t = 0; t < in.tokens[r]; ++t) {
            float wsum = 0.f;
            for (int k = 0; k < c.num_topk; ++k)
                if (in.topk[r][t * c.num_topk + k] >= 0)
                    wsum += in.w[r][t * c.num_topk + k];
            for (int h = 0; h < c.hidden; h += 61) {
                const float want =
                    wsum *
                    from_bf16(in.x[r][static_cast<size_t>(t) * c.hidden + h]);
                const float got = from_bf16(
                    b.combined[r][static_cast<size_t>(t) * c.hidden + h]);
                if (std::fabs(got - want) > 0.05f + 0.02f * std::fabs(want)) {
                    fail("combined value r" + std::to_string(r) + " t" +
                         std::to_string(t));
                    break;
                }
            }
        }
    }
    return errors;
}

}  // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    sycl::queue q{sycl::gpu_selector_v, sycl::property::queue::in_order()};
    std::printf("device: %s\n",
                q.get_device().get_info<sycl::info::device::name>().c_str());
    std::mt19937 rng(1234);
    const std::vector<Config> shapes = {
        {4, 16, 256, 4, 32, false, false, 2, false},
        {8, 64, 2048, 8, 128, false, false, 2, false},
        {6, 24, 512, 6, 64, false, false, 3, true},
    };
    int failures = 0;
    for (Config c : shapes) {
        for (int mode = 0; mode < 3; ++mode) {
            c.fp8 = mode == 2;
            c.weights = mode == 1;
            const Inputs in = make_inputs(c, rng);
            std::printf("R=%d mode %d: one node\n", c.num_ranks, mode);
            const Outputs one = run(q, c, in, false);
            std::printf("R=%d mode %d: hybrid\n", c.num_ranks, mode);
            const Outputs hyb = run(q, c, in, true);
            const int errors = check(c, in, one, hyb);
            std::printf(
                "R=%d E=%d hidden=%d topk=%d T=%d nodes=%d%s %s%s: %s\n",
                c.num_ranks, c.num_experts, c.hidden, c.num_topk, c.T, c.nodes,
                c.interleave ? " (interleaved)" : "", c.fp8 ? "fp8" : "bf16",
                c.weights ? " weights" : "", errors ? "FAIL" : "ok");
            failures += errors != 0;
        }
    }
    std::printf(failures ? "FAILED\n" : "PASSED\n");
    return failures ? 1 : 0;
}
