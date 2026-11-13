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

// Hardware check for the XPU EP's inter-node signalling: one host proxy
// RDMA-writes a payload from one GPU into another GPU's memory, then writes a
// sequence number into a flag next to it, on one RC queue pair. A kernel on
// the receiving GPU is already polling the flag, as the EP's receive kernel
// does; once it sees the sequence number it checks the payload twice:
//
//   uncached loads   memory itself: a mismatch means the flag became visible
//                    before the data (the proxy would need a flush READ)
//   plain loads      after a system-scope acquire, as the EP reads: a
//                    mismatch means stale cache lines
//
// The payload changes every round, so stale data from an earlier round shows.
// The two GPUs default to the NICs nearest to them, so the traffic crosses
// the network instead of looping back inside one HCA.
//
// Build: cmake --build <build dir> --target xpu_rdma_test
// Run:   xpu_rdma_test [--src-gpu 0] [--dst-gpu 1] [--src-nic mlx5_0]
//                      [--dst-nic mlx5_4] [--bytes 8388608] [--rounds 100]
//                      [--src-mem device|host] [--dst-mem device|host]
//                      [--check kernel|none] [--pollers 1024]
//                      [--flags-per-poller 1] [--poll-delay 0]
//
// For bandwidth only: --src-mem / --dst-mem host put that end in host memory
// instead of GPU memory (--dst-mem host needs --check none), and --check none
// runs no kernel during the write; the host then checks the payload after the
// last round. --pollers caps the checker's work-groups, each of which polls
// the flag. --flags-per-poller K makes K lanes of each poller's first
// sub-group load a cache line each per poll, as the EP's combine_recv polls
// one ready flag per source rank; only lane 0's line is the real flag.
// --poll-delay spins that many clock cycles between polls without touching
// memory.

#include <sycl/sycl.hpp>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "xpu_ep_common.h"
#include "xpu_rdma.h"

using namespace mooncake::xpu_ep;

namespace {

constexpr int kWG = 256;
constexpr uint64_t kMaxClockStep = 1ull << 20;
constexpr uint64_t kTimeoutTicks = 1ull << 34;  // ~7 s at B60's 2.4 GHz

struct Args {
    int src_gpu = 0;
    int dst_gpu = -1;
    std::string src_nic, dst_nic;
    size_t bytes = 8 << 20;
    int rounds = 100;
    bool src_host = false, dst_host = false;
    bool check = true;
    int pollers = 1024;
    int flags_per_poller = 1;
    long poll_delay = 0;
};

bool parseChoice(const std::string& k, const std::string& v, const char* yes,
                 const char* no) {
    if (v == yes) return true;
    if (v == no) return false;
    throw std::invalid_argument(k + " must be " + yes + " or " + no);
}

Args parse(int argc, char** argv) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        const std::string k = argv[i];
        if (i + 1 >= argc) throw std::invalid_argument("missing value: " + k);
        const char* v = argv[++i];
        if (k == "--src-gpu")
            a.src_gpu = std::atoi(v);
        else if (k == "--dst-gpu")
            a.dst_gpu = std::atoi(v);
        else if (k == "--src-nic")
            a.src_nic = v;
        else if (k == "--dst-nic")
            a.dst_nic = v;
        else if (k == "--bytes")
            a.bytes = std::strtoull(v, nullptr, 0);
        else if (k == "--rounds")
            a.rounds = std::atoi(v);
        else if (k == "--src-mem")
            a.src_host = parseChoice(k, v, "host", "device");
        else if (k == "--dst-mem")
            a.dst_host = parseChoice(k, v, "host", "device");
        else if (k == "--check")
            a.check = parseChoice(k, v, "kernel", "none");
        else if (k == "--pollers")
            a.pollers = std::atoi(v);
        else if (k == "--flags-per-poller")
            a.flags_per_poller = std::atoi(v);
        else if (k == "--poll-delay")
            a.poll_delay = std::atol(v);
        else
            throw std::invalid_argument("unknown option: " + k);
    }
    a.bytes = a.bytes / sizeof(uint32_t) * sizeof(uint32_t);
    if (a.bytes == 0 || a.bytes > (1ull << 31) || a.rounds < 1 || a.pollers < 1)
        throw std::invalid_argument(
            "--bytes must be in [4, 2^31], --rounds and --pollers >= 1");
    if (a.flags_per_poller < 1 || a.flags_per_poller > kWarpSize ||
        a.poll_delay < 0)
        throw std::invalid_argument("--flags-per-poller must be in [1, " +
                                    std::to_string(kWarpSize) +
                                    "], --poll-delay >= 0");
    if (a.dst_host && a.check)
        throw std::invalid_argument("--dst-mem host needs --check none");
    return a;
}

inline uint32_t pattern(uint32_t round, size_t i) {
    return static_cast<uint32_t>(i) * 2654435761u ^ round * 0x9e3779b9u;
}

// A whole, uncompressed Level Zero allocation that exports as a dma-buf.
void* allocExportable(sycl::queue& q, size_t bytes) {
    auto ctx =
        sycl::get_native<sycl::backend::ext_oneapi_level_zero>(q.get_context());
    auto dev =
        sycl::get_native<sycl::backend::ext_oneapi_level_zero>(q.get_device());
    ze_external_memory_export_desc_t export_desc{
        ZE_STRUCTURE_TYPE_EXTERNAL_MEMORY_EXPORT_DESC, nullptr,
        ZE_EXTERNAL_MEMORY_TYPE_FLAG_DMA_BUF};
    ze_memory_compression_hints_ext_desc_t uncompressed{
        ZE_STRUCTURE_TYPE_MEMORY_COMPRESSION_HINTS_EXT_DESC, &export_desc,
        ZE_MEMORY_COMPRESSION_HINTS_EXT_FLAG_UNCOMPRESSED};
    ze_device_mem_alloc_desc_t desc{ZE_STRUCTURE_TYPE_DEVICE_MEM_ALLOC_DESC,
                                    &uncompressed, 0, 0};
    void* p = nullptr;
    if (zeMemAllocDevice(ctx, &desc, bytes, 4096, dev, &p) !=
            ZE_RESULT_SUCCESS ||
        !p)
        throw std::runtime_error("zeMemAllocDevice failed");
    return p;
}

void freeExportable(sycl::queue& q, void* p) {
    zeMemFree(
        sycl::get_native<sycl::backend::ext_oneapi_level_zero>(q.get_context()),
        p);
}

// Host buffers are plain pages: ibv_reg_mr cannot pin sycl::malloc_host
// memory (EFAULT).
void* allocBuffer(sycl::queue& q, size_t bytes, bool host) {
    if (!host) return allocExportable(q, bytes);
    constexpr size_t kPage = 4096;
    void* p = std::aligned_alloc(kPage, (bytes + kPage - 1) / kPage * kPage);
    if (!p) throw std::runtime_error("aligned_alloc failed");
    std::memset(p, 0, bytes);
    return p;
}

void freeBuffer(sycl::queue& q, void* p, bool host) {
    if (host)
        std::free(p);
    else
        freeExportable(q, p);
}

RdmaRegion registerBuffer(RdmaDevice& nic, sycl::queue& q, void* p,
                          size_t bytes, bool host) {
    if (host) return nic.registerHost(p, bytes);
    int fd = -1;
    uint64_t off = 0;
    exportDmabuf(
        sycl::get_native<sycl::backend::ext_oneapi_level_zero>(q.get_context()),
        p, &fd, &off);
    return nic.registerDmabuf(p, bytes, fd, off);
}

// Results per round: [0] uncached mismatches, [1] plain mismatches,
// [2] work-groups that timed out waiting for the flag.
sycl::event launchChecker(sycl::queue& q, const uint32_t* payload,
                          const uint32_t* flag, size_t words, uint32_t round,
                          const uint32_t* extra_lines, const Args& a,
                          int* started, int* results) {
    const size_t groups = std::min<size_t>((words + kWG - 1) / kWG,
                                           static_cast<size_t>(a.pollers));
    const uint32_t lines = static_cast<uint32_t>(a.flags_per_poller);
    const uint64_t delay = static_cast<uint64_t>(a.poll_delay);
    return q.parallel_for(
        sycl::nd_range<1>(groups * kWG, kWG),
        [=](sycl::nd_item<1> it) [[sycl::reqd_sub_group_size(kWarpSize)]] {
            const auto g = it.get_group();
            const auto sg = it.get_sub_group();
            const size_t lid = it.get_local_id(0);
            if (it.get_global_id(0) == 0)
                sycl::atomic_ref<int, sycl::memory_order::relaxed,
                                 sycl::memory_scope::system,
                                 sycl::access::address_space::global_space>(
                    *started)
                    .store(1);
            uint32_t seen = 0;
            if (sg.get_group_linear_id() == 0) {
                const uint32_t lane = sg.get_local_linear_id();
                const uint32_t* line =
                    lane == 0 ? flag : extra_lines + lane * kReadyStride;
                uint64_t prev = device_clock(), waited = 0;
                for (;;) {
                    const uint32_t v = sycl::group_broadcast(
                        sg, lane < lines ? uc_load(line) : 0u, 0);
                    const uint64_t now = device_clock();
                    if (now - prev < kMaxClockStep + delay)
                        waited += now - prev;
                    prev = now;
                    if (v == round ||
                        sycl::group_broadcast(sg, waited > kTimeoutTicks, 0)) {
                        seen = v;
                        break;
                    }
                    for (const uint64_t t0 = device_clock();
                         device_clock() - t0 < delay;) {
                    }
                }
            }
            seen = sycl::group_broadcast(g, seen, 0);
            sycl::atomic_ref<int, sycl::memory_order::relaxed,
                             sycl::memory_scope::device,
                             sycl::access::address_space::global_space>
                timeouts(results[2]), bad_uc(results[0]), bad_plain(results[1]);
            if (seen != round) {
                if (lid == 0) timeouts.fetch_add(1);
                return;
            }
            // Uncached first, right after the flag: that is the ordering
            // check. Then a system acquire, as the EP's group_acquire does,
            // and plain loads.
            int uc = 0, plain = 0;
            const size_t stride = it.get_global_range(0);
            for (size_t i = it.get_global_id(0); i < words; i += stride)
                uc += uc_load(payload + i) != pattern(round, i);
            sycl::group_barrier(g);
            if (lid == 0)
                sycl::atomic_fence(sycl::memory_order::acquire,
                                   sycl::memory_scope::system);
            sycl::group_barrier(g);
            for (size_t i = it.get_global_id(0); i < words; i += stride)
                plain += payload[i] != pattern(round, i);
            uc = sycl::reduce_over_group(g, uc, sycl::plus<int>());
            plain = sycl::reduce_over_group(g, plain, sycl::plus<int>());
            if (lid == 0) {
                if (uc) bad_uc.fetch_add(uc);
                if (plain) bad_plain.fetch_add(plain);
            }
        });
}

int run(const Args& a) {
    std::vector<sycl::device> gpus;
    for (const auto& d :
         sycl::device::get_devices(sycl::info::device_type::gpu))
        if (d.get_backend() == sycl::backend::ext_oneapi_level_zero)
            gpus.push_back(d);
    const int dst_gpu = a.dst_gpu >= 0 ? a.dst_gpu : (gpus.size() > 1 ? 1 : 0);
    if (a.src_gpu >= static_cast<int>(gpus.size()) ||
        dst_gpu >= static_cast<int>(gpus.size()))
        throw std::invalid_argument("only " + std::to_string(gpus.size()) +
                                    " Level Zero GPUs");
    sycl::queue qs(gpus[a.src_gpu]), qd(gpus[dst_gpu]);
    const std::string src_nic =
        a.src_nic.empty() ? nearestRdmaDevice(gpus[a.src_gpu]) : a.src_nic;
    const std::string dst_nic =
        a.dst_nic.empty() ? nearestRdmaDevice(gpus[dst_gpu]) : a.dst_nic;

    const size_t words = a.bytes / sizeof(uint32_t);
    auto* src = static_cast<uint32_t*>(allocBuffer(qs, a.bytes, a.src_host));
    // Payload, then the flag on its own cache line.
    auto* dst = static_cast<uint32_t*>(
        allocBuffer(qd, a.bytes + kLineBytes, a.dst_host));
    uint32_t* flag = dst + words;
    if (!a.dst_host)
        qd.parallel_for(sycl::range<1>(words + kLineBytes / sizeof(uint32_t)),
                        [=](sycl::id<1> i) { uc_store(dst + i, 0u); })
            .wait();
    int* results = sycl::malloc_device<int>(3, qd);
    int* started = sycl::malloc_host<int>(1, qd);
    // Lines the extra polling lanes load; nothing writes them.
    auto* extra_lines = sycl::malloc_device<uint32_t>(
        static_cast<size_t>(kWarpSize) * kReadyStride, qd);
    qd.memset(extra_lines, 0, sizeof(uint32_t) * kWarpSize * kReadyStride)
        .wait();

    int ret = 1;
    {
        RdmaDevice nic_s(src_nic), nic_d(dst_nic);
        const RdmaRegion rs =
            registerBuffer(nic_s, qs, src, a.bytes, a.src_host);
        const RdmaRegion rd =
            registerBuffer(nic_d, qd, dst, a.bytes + kLineBytes, a.dst_host);
        RdmaQueuePair qp_s(nic_s), qp_d(nic_d);
        qp_s.connect(qp_d.localInfo());
        qp_d.connect(qp_s.localInfo());
        std::printf(
            "GPU %d %s (%s, GID %d) -> GPU %d %s (%s, GID %d), %zu bytes, "
            "%d rounds, check %s, %d pollers x %d flags, poll delay %ld\n",
            a.src_gpu, a.src_host ? "host" : "device", src_nic.c_str(),
            nic_s.gidIndex(), dst_gpu, a.dst_host ? "host" : "device",
            dst_nic.c_str(), nic_d.gidIndex(), a.bytes, a.rounds,
            a.check ? "kernel" : "none", a.check ? a.pollers : 0,
            a.flags_per_poller, a.poll_delay);

        long bad_uc = 0, bad_plain = 0, timeouts = 0;
        int first_bad = 0;
        double write_us = 0;
        for (uint32_t r = 1; r <= static_cast<uint32_t>(a.rounds); ++r) {
            if (a.src_host)
                for (size_t i = 0; i < words; ++i) src[i] = pattern(r, i);
            else
                qs.parallel_for(sycl::range<1>(words), [=](sycl::id<1> i) {
                      src[i] = pattern(r, i);
                  }).wait();
            sycl::event ev;
            if (a.check) {
                qd.memset(results, 0, 3 * sizeof(int)).wait();
                __atomic_store_n(started, 0, __ATOMIC_RELAXED);
                ev = launchChecker(qd, dst, flag, words, r, extra_lines, a,
                                   started, results);
                // Post only once the checker is polling, so the writes land
                // while it reads.
                const auto launch_deadline =
                    std::chrono::steady_clock::now() + std::chrono::seconds(10);
                while (!__atomic_load_n(started, __ATOMIC_ACQUIRE)) {
                    if (std::chrono::steady_clock::now() > launch_deadline)
                        throw std::runtime_error(
                            "the checker kernel never started");
                    std::this_thread::yield();
                }
            }
            const auto t0 = std::chrono::steady_clock::now();
            qp_s.write(rs.addr, rs.lkey, rd.addr, rd.rkey, a.bytes, false);
            qp_s.writeValue(r, rd.addr + a.bytes, rd.rkey, true);
            qp_s.poll(1, 10000);
            write_us += std::chrono::duration<double, std::micro>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
            if (!a.check) continue;
            ev.wait();
            int res[3];
            qd.memcpy(res, results, sizeof(res)).wait();
            bad_uc += res[0];
            bad_plain += res[1];
            timeouts += res[2];
            if ((res[0] || res[1] || res[2]) && !first_bad) first_bad = r;
        }
        if (!a.check) {
            // No kernel watched the rounds; check what the last one left.
            std::vector<uint32_t> host(words + 1);
            if (a.dst_host) {
                std::memcpy(host.data(), dst, a.bytes + sizeof(uint32_t));
            } else {
                qd.memcpy(host.data(), dst, a.bytes).wait();
                qd.memcpy(&host[words], flag, sizeof(uint32_t)).wait();
            }
            const uint32_t last = static_cast<uint32_t>(a.rounds);
            for (size_t i = 0; i < words; ++i)
                bad_plain += host[i] != pattern(last, i);
            if (bad_plain || host[words] != last) first_bad = a.rounds;
        }
        const double avg_us = write_us / a.rounds;
        std::printf("write + flag: %.1f us avg, %.2f GB/s\n", avg_us,
                    a.bytes / avg_us / 1e3);
        std::printf(
            "mismatched words: %ld uncached, %ld plain; flag timeouts: %ld "
            "work-groups\n",
            bad_uc, bad_plain, timeouts);
        if (first_bad)
            std::printf("FAIL (first bad round %d)\n", first_bad);
        else
            std::printf("PASS\n");
        ret = first_bad ? 1 : 0;
    }  // queue pairs and MRs go before the memory they cover

    sycl::free(extra_lines, qd);
    sycl::free(started, qd);
    sycl::free(results, qd);
    freeBuffer(qd, dst, a.dst_host);
    freeBuffer(qs, src, a.src_host);
    return ret;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        return run(parse(argc, argv));
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 2;
    }
}
