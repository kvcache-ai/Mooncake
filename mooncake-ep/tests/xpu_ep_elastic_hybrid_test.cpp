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

// Checks the ElasticBuffer's hybrid path on one GPU, without a NIC, as
// xpu_ep_hybrid_test does the Buffer's: every rank of a simulated EP group
// gets its own buffer on this device, ranks on the same simulated node map
// each other directly, and the proxy's RDMA writes (elastic_dispatch_puts,
// elastic_combine_puts) are replayed as device copies. Every rank's send part
// runs, then the writes, then every rank's receive part, so a receive never
// waits on a flag not yet set.
//
// The kernels are deterministic, so every output must match the same group
// with every rank on one node byte for byte. Dispatch is also checked
// against a host reference, and combine against identity experts.

#include <sycl/sycl.hpp>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "xpu_ep_common.h"
#include "xpu_ep_elastic_kernels.h"

using namespace mooncake::xpu_ep;

namespace {

struct Config {
    int num_ranks, num_experts, hidden, num_topk, T;
    int nodes;        // ranks are split into this many simulated nodes
    bool interleave;  // node of rank r: r % nodes (else contiguous blocks)
    bool fp8, expand;
};

struct Inputs {
    std::vector<std::vector<uint8_t>> x;   // [rank][T][hidden * elem_size]
    std::vector<std::vector<int32_t>> sf;  // [rank][T][num_sf_packs]
    std::vector<std::vector<int64_t>> topk;
    std::vector<std::vector<float>> w;
    std::vector<int> tokens;
};

// Per rank, every output of the last dispatch and combine.
struct Outputs {
    std::vector<std::vector<int>> psum_rank, psum_expert, slot_idx, meta;
    std::vector<std::vector<uint8_t>> recv_x;
    std::vector<std::vector<int32_t>> recv_sf;
    std::vector<std::vector<int64_t>> recv_topk;
    std::vector<std::vector<float>> recv_w;
    std::vector<std::vector<uint16_t>> combined;
    std::vector<int> active;
};

constexpr int kAlignment = 4;  // expanded only

int elem_size(const Config& c) { return c.fp8 ? 1 : kBf16Size; }
int sf_packs(const Config& c) {
    return c.fp8 ? (c.hidden + kNumPerChannels - 1) / kNumPerChannels : 0;
}
int alignment(const Config& c) { return c.expand ? kAlignment : 1; }
// Rows of recv_x when expanded, as the Python wrapper sizes it.
size_t out_rows(const Config& c) {
    const size_t n = static_cast<size_t>(c.num_ranks) * c.T;
    return c.expand ? n * c.num_topk +
                          (c.num_experts / c.num_ranks) * (alignment(c) - 1)
                    : n;
}

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
    const size_t row = static_cast<size_t>(c.hidden) * elem_size(c);
    for (int r = 0; r < c.num_ranks; ++r) {
        const int n = r == 1 ? 0 : c.T - static_cast<int>(rng() % (c.T / 4));
        in.tokens.push_back(n);
        std::vector<uint8_t> x(c.T * row);
        if (c.fp8) {
            for (auto& v : x) v = static_cast<uint8_t>(rng());
        } else {
            for (size_t i = 0; i < x.size(); i += 2) {
                const uint16_t b = to_bf16(val(rng));
                std::memcpy(&x[i], &b, 2);
            }
        }
        std::vector<int32_t> sf(static_cast<size_t>(c.T) * sf_packs(c));
        for (auto& v : sf) v = static_cast<int32_t>(rng());
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
        in.sf.push_back(std::move(sf));
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

template <typename T>
T* to_device(sycl::queue& q, const std::vector<T>& v) {
    T* p = sycl::malloc_device<T>(std::max<size_t>(v.size(), 1), q);
    if (!v.empty()) q.memcpy(p, v.data(), v.size() * sizeof(T));
    return p;
}

template <typename T>
T* zeros(sycl::queue& q, size_t n) {
    T* p = sycl::malloc_device<T>(std::max<size_t>(n, 1), q);
    q.memset(p, 0, std::max<size_t>(n, 1) * sizeof(T));
    return p;
}

// One dispatch and one combine, twice (both buffer halves), with `hybrid`
// ranks on other simulated nodes reached through replayed puts.
Outputs run(sycl::queue& q, const Config& c, const Inputs& in, bool hybrid) {
    const int R = c.num_ranks, E = c.num_experts, T = c.T, H = c.hidden;
    const int K = c.num_topk, L = E / R, P = sf_packs(c);
    const size_t N = static_cast<size_t>(R) * T;
    const size_t rows = out_rows(c);
    const size_t row_bytes = static_cast<size_t>(H) * elem_size(c);
    const int max_wgs = static_cast<int>(
        q.get_device().get_info<sycl::info::device::max_compute_units>());
    const int64_t timeout = 4'000'000'000;  // ~2 s: a bug drops a rank
    // As the buffer sizes a half: the larger of dispatch and combine.
    const size_t half_bytes =
        ElasticHalfLayout(R, T, ElasticMsgLayout(H, elem_size(c), P, K), H,
                          true)
            .bytes;
    const size_t ready_bytes = round_up(R * kLineBytes, 4096);

    std::vector<uint8_t*> bufs(R);
    std::vector<void**> peer_tables(R);
    std::vector<int*> remote_tables(R);
    std::vector<std::vector<int>> remote_ranks(R);
    for (int r = 0; r < R; ++r) bufs[r] = zeros<uint8_t>(q, 2 * half_bytes);
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
        peer_tables[r] = to_device(q, peers);
        remote_tables[r] = to_device(q, remote);
        q.wait();  // before the host tables go out of scope
    }

    struct Rank {
        uint8_t* x;
        int32_t* sf;
        int64_t* topk;
        float* w;
        int32_t* active;
        int *psum_rank, *psum_expert, *slot_idx, *meta;
        uint8_t* recv_x;
        int32_t* recv_sf;
        int64_t* recv_topk;
        float* recv_w;
        uint16_t* combined;
        int* scratch;  // recv_count[R], expert_count[L], done_counter
        int* host;     // host_counts of each half
    };
    std::vector<Rank> ranks(R);
    for (int r = 0; r < R; ++r) {
        Rank& k = ranks[r];
        k.x = to_device(q, in.x[r]);
        k.sf = to_device(q, in.sf[r]);
        k.topk = to_device(q, in.topk[r]);
        k.w = to_device(q, in.w[r]);
        k.active = sycl::malloc_device<int32_t>(R, q);
        q.fill(k.active, 1, R);
        k.psum_rank = zeros<int>(q, R);
        k.psum_expert = zeros<int>(q, L + 1);
        k.slot_idx = zeros<int>(q, static_cast<size_t>(T) * K);
        k.meta = zeros<int>(q, N * (K + 2));
        k.recv_x = zeros<uint8_t>(q, rows * row_bytes);
        k.recv_sf = zeros<int32_t>(q, rows * P);
        k.recv_topk = zeros<int64_t>(q, N * K);
        k.recv_w = zeros<float>(q, rows);
        k.combined = zeros<uint16_t>(q, static_cast<size_t>(T) * H);
        k.scratch = zeros<int>(q, R + L + 1);
        k.host = sycl::malloc_host<int>(2 * R, q);
        std::fill(k.host, k.host + 2 * R, 0);
    }
    q.wait();

    int half = 0;
    for (int iter = 0; iter < 2; ++iter) {
        auto half_of = [&](int r, int h) { return bufs[r] + h * half_bytes; };
        std::vector<ElasticDispatchArgs> d(R);
        for (int r = 0; r < R; ++r) {
            Rank& k = ranks[r];
            ElasticDispatchArgs& a = d[r];
            a = ElasticDispatchArgs{};
            a.x = k.x;
            a.sf = c.fp8 ? k.sf : nullptr;
            a.topk_idx = k.topk;
            a.topk_weights = c.expand ? k.w : nullptr;
            a.active_ranks = k.active;
            a.psum_num_recv_tokens_per_rank = k.psum_rank;
            a.psum_num_recv_tokens_per_expert = k.psum_expert;
            a.dst_buffer_slot_idx = k.slot_idx;
            a.recv_x = k.recv_x;
            a.recv_sf = c.fp8 ? k.recv_sf : nullptr;
            a.recv_topk_idx = k.recv_topk;
            a.recv_topk_weights = c.expand ? k.recv_w : nullptr;
            a.recv_src_metadata = k.meta;
            if (hybrid) a.host_counts = k.host + half * R;
            a.ready = reinterpret_cast<int*>(half_of(r, half));
            a.next_ready = reinterpret_cast<int*>(half_of(r, half ^ 1));
            a.staging = half_of(r, half) + ready_bytes;
            a.scratch = {k.scratch, k.scratch + R, k.scratch + R + L};
            a.peers = {bufs[r], peer_tables[r],
                       hybrid ? remote_tables[r] : nullptr};
            a.num_tokens = in.tokens[r];
            a.hidden = H;
            a.elem_size = elem_size(c);
            a.num_sf_packs = P;
            a.sf_token_stride = P;
            a.sf_hidden_stride = 1;
            a.num_topk = K;
            a.num_experts = E;
            a.num_max_tokens_per_rank = T;
            a.expert_alignment = alignment(c);
            a.rank = r;
            a.num_ranks = R;
            a.num_recv_tokens = static_cast<int>(N);
            a.do_expand = c.expand;
            a.timeout_ticks = timeout;
            a.max_wgs = max_wgs;
            elastic_dispatch_send(q, a).wait();
        }
        if (hybrid)
            for (int r = 0; r < R; ++r)
                apply(q, elastic_dispatch_puts(d[r], bufs[r], remote_ranks[r]),
                      bufs, r);
        for (int r = 0; r < R; ++r) elastic_dispatch_recv(q, d[r]).wait();
        half ^= 1;
        if (c.fp8) continue;  // combine reads BF16 expert rows

        std::vector<ElasticCombineArgs> cb(R);
        for (int r = 0; r < R; ++r) {
            Rank& k = ranks[r];
            const int received = to_host(q, k.psum_rank, R)[R - 1];
            ElasticCombineArgs& a = cb[r];
            a = ElasticCombineArgs{};
            a.x = k.recv_x;  // identity experts
            a.topk_idx = k.topk;
            a.psum_num_recv_tokens_per_rank = k.psum_rank;
            a.recv_src_metadata = k.meta;
            a.active_ranks = k.active;
            a.combined_x = k.combined;
            if (hybrid) a.host_counts = k.host + half * R;
            a.ready = reinterpret_cast<int*>(half_of(r, half));
            a.next_ready = reinterpret_cast<int*>(half_of(r, half ^ 1));
            a.staging = half_of(r, half) + ready_bytes;
            a.done_counter = k.scratch + R + L;
            a.peers = d[r].peers;
            a.num_input_tokens = c.expand ? static_cast<int>(rows) : received;
            a.num_combined_tokens = in.tokens[r];
            a.hidden = H;
            a.num_topk = K;
            a.num_experts = E;
            a.num_max_tokens_per_rank = T;
            a.rank = r;
            a.num_ranks = R;
            a.do_expand = c.expand;
            a.timeout_ticks = timeout;
            a.max_wgs = max_wgs;
            elastic_combine_send(q, a).wait();
        }
        if (hybrid)
            for (int r = 0; r < R; ++r)
                apply(q, elastic_combine_puts(cb[r], bufs[r], remote_ranks[r]),
                      bufs, r);
        for (int r = 0; r < R; ++r) elastic_combine_recv(q, cb[r]).wait();
        half ^= 1;
    }

    Outputs out;
    for (int r = 0; r < R; ++r) {
        Rank& k = ranks[r];
        out.psum_rank.push_back(to_host(q, k.psum_rank, R));
        out.psum_expert.push_back(to_host(q, k.psum_expert, L + 1));
        out.slot_idx.push_back(
            to_host(q, k.slot_idx, static_cast<size_t>(T) * K));
        out.meta.push_back(to_host(q, k.meta, N * (K + 2)));
        out.recv_x.push_back(to_host(q, k.recv_x, rows * row_bytes));
        out.recv_sf.push_back(to_host(q, k.recv_sf, rows * P));
        out.recv_topk.push_back(to_host(q, k.recv_topk, N * K));
        out.recv_w.push_back(to_host(q, k.recv_w, rows));
        out.combined.push_back(
            to_host(q, k.combined, static_cast<size_t>(in.tokens[r]) * H));
        const auto act = to_host(q, k.active, R);
        out.active.push_back(std::count(act.begin(), act.end(), 1));
        for (void* p :
             {static_cast<void*>(k.x), static_cast<void*>(k.sf),
              static_cast<void*>(k.topk), static_cast<void*>(k.w),
              static_cast<void*>(k.active), static_cast<void*>(k.psum_rank),
              static_cast<void*>(k.psum_expert), static_cast<void*>(k.slot_idx),
              static_cast<void*>(k.meta), static_cast<void*>(k.recv_x),
              static_cast<void*>(k.recv_sf), static_cast<void*>(k.recv_topk),
              static_cast<void*>(k.recv_w), static_cast<void*>(k.combined),
              static_cast<void*>(k.scratch), static_cast<void*>(k.host)})
            sycl::free(p, q);
        sycl::free(bufs[r], q);
        sycl::free(peer_tables[r], q);
        sycl::free(remote_tables[r], q);
    }
    return out;
}

int check(const Config& c, const Inputs& in, const Outputs& a,
          const Outputs& b) {
    const int R = c.num_ranks, T = c.T, K = c.num_topk, L = c.num_experts / R;
    const size_t row_bytes = static_cast<size_t>(c.hidden) * elem_size(c);
    int errors = 0;
    auto fail = [&](const std::string& what) {
        if (errors++ < 10) std::printf("    FAIL %s\n", what.c_str());
    };
    for (int r = 0; r < R; ++r) {
        const std::string at = " at rank " + std::to_string(r);
        if (a.active[r] != R || b.active[r] != R) fail("dropped a peer" + at);
        if (a.psum_rank[r] != b.psum_rank[r]) fail("psum per rank" + at);
        if (a.psum_expert[r] != b.psum_expert[r]) fail("psum per expert" + at);
        if (a.slot_idx[r] != b.slot_idx[r]) fail("dst_buffer_slot_idx" + at);
        if (a.meta[r] != b.meta[r]) fail("recv_src_metadata" + at);
        if (a.recv_x[r] != b.recv_x[r]) fail("recv_x" + at);
        if (a.recv_sf[r] != b.recv_sf[r]) fail("recv_sf" + at);
        if (a.recv_topk[r] != b.recv_topk[r]) fail("recv_topk_idx" + at);
        if (a.recv_w[r] != b.recv_w[r]) fail("recv_topk_weights" + at);
        if (a.combined[r] != b.combined[r]) fail("combined_x" + at);

        // Host reference, not expanded: from each source in rank order, its
        // tokens with an expert on this rank, in token order.
        if (!c.expand) {
            int j = 0;
            for (int s = 0; s < R; ++s) {
                for (int t = 0; t < in.tokens[s]; ++t) {
                    bool hit = false;
                    for (int k = 0; k < K; ++k) {
                        const int64_t e = in.topk[s][t * K + k];
                        hit |= e >= 0 && e / L == r;
                    }
                    if (!hit) continue;
                    const std::string tok = " (src " + std::to_string(s) +
                                            " token " + std::to_string(t) +
                                            ")" + at;
                    if (b.meta[r][static_cast<size_t>(j) * (K + 2)] !=
                        s * T + t)
                        fail("source of slot " + std::to_string(j) + tok);
                    else if (std::memcmp(b.recv_x[r].data() + j * row_bytes,
                                         in.x[s].data() + t * row_bytes,
                                         row_bytes) != 0)
                        fail("recv_x row" + tok);
                    ++j;
                }
                if (b.psum_rank[r][s] != j)
                    fail("psum of source " + std::to_string(s) + at);
            }
        }

        if (c.fp8) continue;
        // Identity experts: each rank sends back the token once (expanded:
        // once per local pick), so combined = that count times x.
        for (int t = 0; t < in.tokens[r]; ++t) {
            int n = 0;
            for (int k = 0; k < K; ++k) {
                const int64_t e = in.topk[r][t * K + k];
                if (e < 0) continue;
                bool first = true;
                for (int j = 0; j < k && !c.expand; ++j) {
                    const int64_t f = in.topk[r][t * K + j];
                    first &= !(f >= 0 && f / L == e / L);
                }
                n += first;
            }
            for (int h = 0; h < c.hidden; h += 61) {
                uint16_t xb;
                std::memcpy(&xb, in.x[r].data() + t * row_bytes + h * kBf16Size,
                            2);
                const float want = n * from_bf16(xb);
                const float got = from_bf16(
                    b.combined[r][static_cast<size_t>(t) * c.hidden + h]);
                if (std::fabs(got - want) > 0.05f + 0.02f * std::fabs(want)) {
                    fail("combined value t" + std::to_string(t) + at);
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
    // fp8 and expand are set for each mode below.
    const std::vector<Config> shapes = {
        {4, 16, 256, 4, 32, 2, false, false, false},
        {4, 16, 256, 4, 32, 4, false, false, false},  // each rank its own node
        {8, 64, 2048, 8, 128, 2, false, false, false},
        {6, 24, 512, 6, 64, 3, true, false, false},
    };
    int failures = 0;
    for (Config c : shapes) {
        for (int mode = 0; mode < 4; ++mode) {
            c.fp8 = mode >= 2;
            c.expand = mode % 2 == 1;
            const Inputs in = make_inputs(c, rng);
            const Outputs one = run(q, c, in, false);
            const Outputs hyb = run(q, c, in, true);
            const int errors = check(c, in, one, hyb);
            std::printf(
                "R=%d E=%d hidden=%d topk=%d T=%d nodes=%d%s %s%s: %s\n",
                c.num_ranks, c.num_experts, c.hidden, c.num_topk, c.T, c.nodes,
                c.interleave ? " (interleaved)" : "", c.fp8 ? "fp8" : "bf16",
                c.expand ? " expanded" : "", errors ? "FAIL" : "ok");
            failures += errors != 0;
        }
    }
    std::printf(failures ? "FAILED\n" : "PASSED\n");
    return failures ? 1 : 0;
}
