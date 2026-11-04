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

// Peer-to-peer bandwidth over the GPU interconnect of one Intel GPU node.
//
// The XPU EP pulls peer data within a node: each GPU stages what it sends in
// its own memory and peers read it (xpu_ep_common.h). That choice rests on
// measurements of one GPU and topology. This tool repeats them on any Intel
// GPU node, with the EP's own IPC mapping and traffic pattern, to show
// whether peer writes there are as fast as reads, in which case pushing may
// serve better. It varies what the EP could change:
//
//   --op write-kernel  a kernel on each source GPU stores into peer memory
//        read-kernel   a kernel on each GPU loads from peer memory
//        write-copy    queue.memcpy to each peer, one in-order queue per peer
//        read-copy     queue.memcpy from each peer
//   --vec 4|16         bytes per load/store
//   --uc               uncached hint on the peer access (store hint for
//                      writes, load hint for reads; hint 0x7 as in the EP)
//   --unroll 1|8       loads each work-item issues before storing any
//                      (kernel ops; the EP's pull uses 8, kPullUnroll)
//   --pattern all2all  every GPU moves data at once (default; what EP does)
//             fanout   only GPU 0 does, so the peers are otherwise idle
//   --peers N          peers per source GPU (default: all others)
//
// Peers are reached as in the EP: one process per GPU, each mapping the
// others' memory with the EP's XpuIpcBuffer (xpu/xpu_ipc.cpp, Level Zero
// IPC).
//
// Each process owns one buffer: what it sends (`bytes`), then one `bytes`
// slot per GPU it receives from. An iteration starts on a barrier across the
// processes and ends when the slowest source GPU finishes.
//
// Build and run (not built by default; needs -DWITH_EP=ON -DUSE_XPU=ON):
//   cmake --build build --target xpu_peer_bw
//   for op in write-kernel read-kernel write-copy read-copy; do
//     for p in 1 3; do
//       build/mooncake-ep/src/xpu_peer_bw --op $op --peers $p
//     done
//   done

#include <pthread.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <sycl/sycl.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>
#include <vector>

#include "xpu_ep_common.h"
#include "xpu_ipc.h"

namespace {

using mooncake::xpu_ep::XpuIpcBuffer;

constexpr int kMaxGpus = 16;  // pattern_word keeps the rank in 4 bits
constexpr int kMaxPeers = kMaxGpus - 1;
constexpr int kMaxIters = 10000;
constexpr int kHandleSlotWords = 64;
using vec4 = sycl::vec<uint32_t, 4>;

struct Options {
    std::string op = "write-kernel";
    std::string pattern = "all2all";
    std::vector<int> gpus;  // empty: every GPU
    int peers = 0;          // 0: every other GPU
    size_t bytes = size_t(32) << 20;
    int vec = 16;
    bool uc = false;
    int wg = 256;
    int groups_per_peer = 64;
    int unroll = 1;
    int warmup = 5;
    int iters = 20;
};

[[noreturn]] void usage(const char* argv0) {
    std::fprintf(stderr,
                 "usage: %s [--op write-kernel|read-kernel|write-copy|"
                 "read-copy] [--pattern all2all|fanout] [--gpus 0,1,..] "
                 "[--peers N] [--bytes N] [--vec 4|16] [--uc] [--unroll 1|8] "
                 "[--wg N] "
                 "[--groups-per-peer N] [--warmup N] [--iters N]\n",
                 argv0);
    std::exit(2);
}

Options parse(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) usage(argv[0]);
            return argv[++i];
        };
        if (a == "--op") {
            o.op = next();
        } else if (a == "--pattern") {
            o.pattern = next();
        } else if (a == "--gpus") {
            const std::string s = next();
            size_t pos = 0;
            while (pos <= s.size()) {
                const size_t comma = std::min(s.find(',', pos), s.size());
                o.gpus.push_back(std::stoi(s.substr(pos, comma - pos)));
                pos = comma + 1;
            }
        } else if (a == "--peers") {
            o.peers = std::stoi(next());
        } else if (a == "--bytes") {
            o.bytes = std::stoull(next());
        } else if (a == "--vec") {
            o.vec = std::stoi(next());
        } else if (a == "--uc") {
            o.uc = true;
        } else if (a == "--unroll") {
            o.unroll = std::stoi(next());
        } else if (a == "--wg") {
            o.wg = std::stoi(next());
        } else if (a == "--groups-per-peer") {
            o.groups_per_peer = std::stoi(next());
        } else if (a == "--warmup") {
            o.warmup = std::stoi(next());
        } else if (a == "--iters") {
            o.iters = std::stoi(next());
        } else {
            usage(argv[0]);
        }
    }
    const bool op_ok = o.op == "write-kernel" || o.op == "read-kernel" ||
                       o.op == "write-copy" || o.op == "read-copy";
    if (!op_ok || (o.pattern != "all2all" && o.pattern != "fanout") ||
        (o.vec != 4 && o.vec != 16) || (o.unroll != 1 && o.unroll != 8) ||
        o.bytes == 0 || o.bytes % 16 != 0 || o.wg <= 0 ||
        o.groups_per_peer <= 0 || o.iters <= 0 || o.iters > kMaxIters ||
        o.warmup < 0 || o.peers < 0)
        usage(argv[0]);
    return o;
}

template <typename V, bool kUc>
inline __attribute__((always_inline)) V load(const V* p) {
#ifdef __SYCL_DEVICE_ONLY__
    if constexpr (kUc)
        return *__builtin_intel_sycl_ptr_annotation(
            const_cast<V*>(p), "sycl-cache-read-hint", 0x7);
#endif
    return *p;
}

template <typename V, bool kUc>
inline __attribute__((always_inline)) void store(V* p, const V& v) {
#ifdef __SYCL_DEVICE_ONLY__
    if constexpr (kUc) {
        *__builtin_intel_sycl_ptr_annotation(p, "sycl-cache-write-hint", 0x7) =
            v;
        return;
    }
#endif
    *p = v;
}

// One kernel per source GPU moves `bytes` for each of its peers at once;
// work-group g serves peer g / groups_per_peer.
struct Plan {
    void* dst[kMaxPeers];
    const void* src[kMaxPeers];
    int n;
};

// Each work-item issues kUnroll loads before storing any, as the EP's
// coop_pull does, so that many peer reads are in flight per work-item.
template <typename V, bool kUcLoad, bool kUcStore, int kUnroll>
sycl::event launch(sycl::queue& q, const Plan& plan, size_t bytes, int wg,
                   int gpp) {
    const size_t nvec = bytes / sizeof(V);
    const size_t stride = static_cast<size_t>(gpp) * wg;
    const size_t groups = static_cast<size_t>(plan.n) * gpp;
    return q.parallel_for(
        sycl::nd_range<1>(groups * wg, wg), [=](sycl::nd_item<1> it) {
            const size_t g = it.get_group(0);
            const int k = static_cast<int>(g / gpp);
            V* dst = static_cast<V*>(plan.dst[k]);
            const V* src = static_cast<const V*>(plan.src[k]);
            for (size_t base = (g % gpp) * wg + it.get_local_id(0); base < nvec;
                 base += stride * kUnroll) {
                V r[kUnroll];
#pragma unroll
                for (int u = 0; u < kUnroll; ++u) {
                    const size_t i = base + static_cast<size_t>(u) * stride;
                    if (i < nvec) r[u] = load<V, kUcLoad>(src + i);
                }
#pragma unroll
                for (int u = 0; u < kUnroll; ++u) {
                    const size_t i = base + static_cast<size_t>(u) * stride;
                    if (i < nvec) store<V, kUcStore>(dst + i, r[u]);
                }
            }
        });
}

using LaunchFn = sycl::event (*)(sycl::queue&, const Plan&, size_t, int, int);

template <int kUnroll>
LaunchFn pick_kernel(int vec, bool uc_load, bool uc_store) {
    if (vec == 4) {
        if (uc_load) return launch<uint32_t, true, false, kUnroll>;
        if (uc_store) return launch<uint32_t, false, true, kUnroll>;
        return launch<uint32_t, false, false, kUnroll>;
    }
    if (uc_load) return launch<vec4, true, false, kUnroll>;
    if (uc_store) return launch<vec4, false, true, kUnroll>;
    return launch<vec4, false, false, kUnroll>;
}

uint32_t pattern_word(int rank, size_t i) {
    return (static_cast<uint32_t>(rank) << 28) ^ static_cast<uint32_t>(i);
}

// What the processes share, mapped before they fork.
struct Shared {
    pthread_barrier_t barrier;
    int num_devices;
    int handle_words[kMaxGpus];
    int32_t handle[kMaxGpus][kHandleSlotWords];
    double ms[kMaxGpus][kMaxIters];  // per rank and timed iteration
};

std::vector<sycl::device> level_zero_gpus() {
    for (const auto& p : sycl::platform::get_platforms()) {
        if (p.get_backend() != sycl::backend::ext_oneapi_level_zero) continue;
        auto devs = p.get_devices(sycl::info::device_type::gpu);
        if (!devs.empty()) return devs;
    }
    return {};
}

bool is_source(const Options& o, int rank) {
    return o.pattern == "all2all" || rank == 0;
}

// The process of rank `rank`, driving GPU gpus[rank]. Returns its exit code.
int run_rank(const Options& o, const std::vector<int>& gpus, int rank,
             int peers, Shared* sh) {
    const int n = static_cast<int>(gpus.size());
    const sycl::device dev = level_zero_gpus().at(gpus[rank]);
    std::printf("rank %d: gpu %d: %s\n", rank, gpus[rank],
                dev.get_info<sycl::info::device::name>().c_str());
    std::fflush(stdout);
    sycl::queue q(dev, sycl::property::queue::in_order());

    XpuIpcBuffer buf(q, o.bytes * (n + 1), rank, n);
    const std::vector<int32_t> mine = buf.exportHandle();
    if (mine.size() > static_cast<size_t>(kHandleSlotWords)) {
        std::fprintf(stderr, "rank %d: IPC handle too large\n", rank);
        return 1;
    }
    sh->handle_words[rank] = static_cast<int>(mine.size());
    std::copy(mine.begin(), mine.end(), sh->handle[rank]);
    pthread_barrier_wait(&sh->barrier);
    std::vector<std::vector<int32_t>> handles(n);
    for (int r = 0; r < n; ++r)
        handles[r].assign(sh->handle[r], sh->handle[r] + sh->handle_words[r]);
    buf.importPeers(handles, {});
    if (!buf.allPeersAccessible()) {
        std::fprintf(stderr, "rank %d: a peer could not be mapped\n", rank);
        return 1;
    }
    std::vector<uint8_t*> base(n);
    q.memcpy(base.data(), buf.peerBases(), n * sizeof(void*)).wait();

    // Uncached, as the EP zeroes its buffer: peers read this over PCIe, so
    // none of it may be left only in this GPU's caches.
    auto* w = reinterpret_cast<uint32_t*>(base[rank]);
    const uint32_t tag = static_cast<uint32_t>(rank) << 28;
    q.parallel_for(sycl::range<1>(o.bytes / 4), [=](sycl::id<1> i) {
         mooncake::xpu_ep::uc_store(w + i[0],
                                    tag ^ static_cast<uint32_t>(i[0]));
     }).wait();
    pthread_barrier_wait(&sh->barrier);

    // write: this rank's send area -> peer p's slot for this rank.
    // read:  peer p's send area -> this rank's slot for p.
    const bool is_write = o.op.rfind("write", 0) == 0;
    const bool is_copy = o.op.find("copy") != std::string::npos;
    const bool source = is_source(o, rank);
    Plan plan{};
    plan.n = peers;
    for (int k = 0; k < peers; ++k) {
        const int p = (rank + 1 + k) % n;
        plan.dst[k] = is_write ? base[p] + (1 + rank) * o.bytes
                               : base[rank] + (1 + p) * o.bytes;
        plan.src[k] = is_write ? base[rank] : base[p];
    }
    // memcpy: one in-order queue per peer so the copies overlap.
    std::vector<sycl::queue> copy_q;
    if (is_copy && source) {
        for (int k = 0; k < peers; ++k)
            copy_q.emplace_back(q.get_context(), dev,
                                sycl::property::queue::in_order());
    }
    const LaunchFn kernel =
        (o.unroll == 8 ? pick_kernel<8> : pick_kernel<1>)(o.vec,
                                                          o.uc && !is_write,
                                                          o.uc && is_write);

    std::vector<sycl::event> events;
    for (int it = 0; it < o.warmup + o.iters; ++it) {
        pthread_barrier_wait(&sh->barrier);
        const auto t0 = std::chrono::steady_clock::now();
        if (source) {
            events.clear();
            if (is_copy) {
                for (int k = 0; k < peers; ++k)
                    events.push_back(
                        copy_q[k].memcpy(plan.dst[k], plan.src[k], o.bytes));
            } else {
                events.push_back(
                    kernel(q, plan, o.bytes, o.wg, o.groups_per_peer));
            }
            for (auto& e : events) e.wait_and_throw();
        }
        const auto t1 = std::chrono::steady_clock::now();
        if (it >= o.warmup)
            sh->ms[rank][it - o.warmup] =
                source
                    ? std::chrono::duration<double, std::milli>(t1 - t0).count()
                    : 0.0;
    }
    pthread_barrier_wait(&sh->barrier);

    // Each rank checks every word of the slots it received.
    const size_t words = o.bytes / 4;
    std::vector<uint32_t> host(words);
    int rc = 0;
    for (int s = 0; s < n; ++s) {
        if (!is_source(o, s)) continue;
        for (int k = 0; k < peers; ++k) {
            const int p = (s + 1 + k) % n;
            const int from = is_write ? s : p;
            const int owner = is_write ? p : s;
            if (owner != rank) continue;
            q.memcpy(host.data(), base[rank] + (1 + from) * o.bytes, o.bytes)
                .wait();
            size_t bad = 0, first = 0;
            for (size_t i = 0; i < words; ++i) {
                if (host[i] != pattern_word(from, i) && bad++ == 0) first = i;
            }
            if (bad) {
                std::fprintf(stderr,
                             "mismatch: rank %d -> rank %d: %zu of %zu words "
                             "wrong, first at word %zu: got 0x%08x want "
                             "0x%08x\n",
                             from, owner, bad, words, first, host[first],
                             pattern_word(from, first));
                rc = 1;
            }
        }
    }
    // No peer may still use this buffer when it is freed.
    pthread_barrier_wait(&sh->barrier);
    return rc;
}

}  // namespace

int main(int argc, char** argv) {
    const Options o = parse(argc, argv);

    auto* sh = static_cast<Shared*>(mmap(nullptr, sizeof(Shared),
                                         PROT_READ | PROT_WRITE,
                                         MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    if (sh == MAP_FAILED) {
        std::perror("mmap");
        return 1;
    }

    // This process never initializes Level Zero, whose state a forked child
    // cannot use; a short-lived child counts the GPUs instead.
    std::fflush(nullptr);
    const pid_t counter = fork();
    if (counter == 0) {
        sh->num_devices = static_cast<int>(level_zero_gpus().size());
        std::exit(0);
    }
    int status = 0;
    if (counter < 0 || waitpid(counter, &status, 0) < 0 || !WIFEXITED(status) ||
        WEXITSTATUS(status) != 0) {
        std::fprintf(stderr, "could not list the Level Zero GPUs\n");
        return 1;
    }

    std::vector<int> gpus = o.gpus;
    if (gpus.empty()) {
        for (int i = 0; i < sh->num_devices; ++i) gpus.push_back(i);
    }
    for (int g : gpus) {
        if (g < 0 || g >= sh->num_devices) usage(argv[0]);
    }
    const int n = static_cast<int>(gpus.size());
    const int peers = o.peers ? o.peers : n - 1;
    if (n < 2 || n > kMaxGpus || peers > n - 1) {
        std::fprintf(stderr,
                     "found %d Level Zero GPUs; need 2..%d of them and "
                     "--peers in 1..%d\n",
                     sh->num_devices, kMaxGpus, std::max(1, n - 1));
        return 1;
    }

    pthread_barrierattr_t attr;
    pthread_barrierattr_init(&attr);
    pthread_barrierattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_barrier_init(&sh->barrier, &attr, n);

    std::vector<pid_t> pids(n, -1);
    std::vector<bool> alive(n, false);
    auto stop_all = [&] {
        for (int r = 0; r < n; ++r)
            if (alive[r]) kill(pids[r], SIGKILL);
    };
    for (int r = 0; r < n; ++r) {
        std::fflush(nullptr);
        pids[r] = fork();
        if (pids[r] < 0) {
            std::perror("fork");
            stop_all();
            return 1;
        }
        if (pids[r] == 0) {
            int rc = 1;
            try {
                rc = run_rank(o, gpus, r, peers, sh);
            } catch (const std::exception& e) {
                std::fprintf(stderr, "rank %d: %s\n", r, e.what());
            }
            std::exit(rc);
        }
        alive[r] = true;
    }

    // A rank that fails before a barrier leaves the others waiting at it.
    bool failed = false;
    for (int left = n; left > 0; --left) {
        const pid_t pid = wait(&status);
        if (pid < 0) break;
        const int r = static_cast<int>(
            std::find(pids.begin(), pids.end(), pid) - pids.begin());
        if (r < n) alive[r] = false;
        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) continue;
        if (!failed) {
            std::fprintf(stderr, "rank %d (gpu %d) failed\n", r,
                         r < n ? gpus[r] : -1);
            stop_all();
        }
        failed = true;
    }
    if (failed) return 1;

    const int num_sources = o.pattern == "all2all" ? n : 1;
    std::vector<double> ms(o.iters);
    for (int it = 0; it < o.iters; ++it) {
        ms[it] = 0;
        for (int r = 0; r < n; ++r) ms[it] = std::max(ms[it], sh->ms[r][it]);
    }
    std::sort(ms.begin(), ms.end());
    const double med = ms[ms.size() / 2];
    const double best = ms.front();
    const double per_peer = o.bytes / (med * 1e6);
    std::printf(
        "op=%s pattern=%s gpus=%d peers=%d bytes=%zu vec=%d uc=%d "
        "unroll=%d wg=%d groups_per_peer=%d\n"
        "median %.3f ms (best %.3f) | per peer %.2f GB/s | per source GPU "
        "%.2f GB/s | total %.2f GB/s\n",
        o.op.c_str(), o.pattern.c_str(), n, peers, o.bytes, o.vec, o.uc ? 1 : 0,
        o.unroll, o.wg, o.groups_per_peer, med, best, per_peer,
        per_peer * peers, per_peer * peers * num_sources);
    return 0;
}
