// Copyright 2024 KVCache.AI
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

// CXI multi-NIC transfer test
//
// Target node: allocate buffers, register on all NICs, wait.
// Initiator node: allocate receive buffer, register, pull from target.
//
// Usage:
//   # Target (holds KV cache):
//   ./cxi_transfer_test --mode target --server <target_ip>:12345 \
//       --num_bufs 4 --buf_size_gb 1
//
//   # Initiator (pulls data):
//   ./cxi_transfer_test --mode initiator --server <initiator_ip>:12346 \
//       --target <target_ip>:12345 --num_bufs 4 --buf_size_gb 1 \
//       --transfer_mb 368

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/mman.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <string>
#include <thread>
#include <vector>
#include <numaif.h>

#include "transfer_engine.h"
#include "transport/cxi_transport/cxi_transport.h"

#if defined(USE_CUDA) || defined(USE_HIP)
#include <cuda_alike.h>
#endif

using namespace mooncake;

DEFINE_string(mode, "target", "Running mode: target or initiator");
DEFINE_string(server, "", "Local server name, e.g. 172.31.6.162:12345");
DEFINE_string(target, "", "Target server name (initiator mode only)");
DEFINE_string(metadata, "P2PHANDSHAKE", "Metadata server");
DEFINE_int32(num_bufs, 200, "Number of buffers to allocate and register");
DEFINE_double(buf_size_gb, 2.0, "Size of each buffer in GB");
DEFINE_double(transfer_mb, 368.0,
              "Transfer size per iteration in MB (initiator)");
DEFINE_int32(iterations, 50, "Number of benchmark iterations");
DEFINE_int32(warmup, 5, "Number of warmup iterations");
DEFINE_int32(batch_size, 1, "Batch size for each transfer submission");
DEFINE_int32(threads, 1, "Number of initiator worker threads");
DEFINE_uint64(block_size, 65536, "Block size for transfer requests (64KB)");
DEFINE_bool(use_device, false, "Use CUDA device memory for transfer");

static std::atomic<bool> g_running(true);

static void signalHandler(int) {
    g_running.store(false, std::memory_order_relaxed);
}

static void setupSignalHandler() {
    struct sigaction sa;
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

static void* allocateHugepage(size_t size, int node_id) {
    void* buf = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (buf == MAP_FAILED) {
        LOG(WARNING) << "Hugepage mmap failed (" << strerror(errno)
                     << "), falling back to regular pages";
        buf = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (buf == MAP_FAILED) return nullptr;
    }
    unsigned long nodemask = 1UL << node_id;

    mbind(buf, size, MPOL_BIND, &nodemask,
          /* maxnode */ sizeof(nodemask) * 8, 0);
    return buf;
}

#if defined(USE_CUDA) || defined(USE_HIP)
static constexpr bool can_use_device = true;

static void* allocateDevice(size_t size, int device) {
    cudaSetDevice(device);
    void* buf = nullptr;
    cudaError_t err = cudaMalloc(&buf, size);
    if (err != cudaSuccess) {
        return nullptr;
    }
    return buf;
}

static int setDeviceMem(void* buf, size_t size, int device, int val) {
    cudaSetDevice(device);
    cudaError_t err = cudaMemset(buf, val, size);
    if (err != cudaSuccess) {
        return -1;
    }
    return 0;
}

static bool checkDeviceMem(void* buf, int device, uint8_t expected) {
    cudaSetDevice(device);
    uint8_t actual;
    size_t offset = 42;
    cudaError_t err =
        cudaMemcpy(&actual, buf + offset, 1, cudaMemcpyDeviceToHost);
    if (err != cudaSuccess) return false;
    return expected == actual;
}

static int freeDeviceMem(void* buf, int device) {
    cudaSetDevice(device);
    cudaError_t err = cudaFree(buf);
    if (err != cudaSuccess) return -1;
    return 0;
}

#else
static constexpr bool can_use_device = false;
#endif

static int runTarget(TransferEngine* engine) {
    size_t buf_bytes =
        static_cast<size_t>(FLAGS_buf_size_gb * 1024 * 1024 * 1024);
    int num_bufs = FLAGS_num_bufs;

    LOG(INFO) << "=== Target Node ===";
    LOG(INFO) << "Registering " << num_bufs << " x " << FLAGS_buf_size_gb
              << " GB = " << num_bufs * FLAGS_buf_size_gb << " GB";

    // Allocate buffers
    std::vector<void*> bufs;
    bufs.reserve(num_bufs);
    LOG(INFO) << "Allocating " << num_bufs << " buffers...";
    for (int i = 0; i < num_bufs; ++i) {
        void* buf;
        if (!FLAGS_use_device) {
            buf = allocateHugepage(buf_bytes, 0);
            if (!buf) {
                LOG(ERROR) << "Allocation failed at buffer " << i;
                for (auto* p : bufs) munmap(p, buf_bytes);
                return 1;
            }
            buf = memset(buf, (uint8_t)42, buf_bytes);
            if (!buf) {
                LOG(ERROR) << "Memset failed at buffer " << i;
                for (auto* p : bufs) munmap(p, buf_bytes);
                return 1;
            }
        } else {
#if defined(USE_CUDA) || defined(USE_HIP)
            int buf_device = i % 4;
            buf = allocateDevice(buf_bytes, buf_device);
            if (!buf) {
                LOG(ERROR) << "allocation failed at buffer " << i;
                for (int j = 0; j < i; j++) {
                    freeDeviceMem(bufs[j], j % 4);
                }
                return 1;
            }
            if (setDeviceMem(buf, buf_bytes, buf_device, 42) != 0) {
                LOG(ERROR) << "failed to set memory for buffer " << i;
                for (int j = 0; j < i; j++) {
                    freeDeviceMem(bufs[j], j % 4);
                }
                return 1;
            }
#endif
        }

        bufs.push_back(buf);
        if ((i + 1) % 50 == 0 || i == num_bufs - 1)
            LOG(INFO) << "  allocated " << (i + 1) << "/" << num_bufs;
    }

    // Register all buffers
    LOG(INFO) << "Registering " << num_bufs << " buffers on all NICs...";
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < num_bufs; ++i) {
        int ret = engine->registerLocalMemory(bufs[i], buf_bytes, "*", true);
        if (ret != 0) {
            LOG(ERROR) << "registerLocalMemory failed at buffer " << i
                       << ": ret=" << ret;
            for (int j = 0; j < i; ++j) engine->unregisterLocalMemory(bufs[j]);
            for (auto* p : bufs) munmap(p, buf_bytes);
            return 1;
        }
        if ((i + 1) % 50 == 0 || i == num_bufs - 1) {
            auto now = std::chrono::steady_clock::now();
            double elapsed = std::chrono::duration<double>(now - t0).count();
            LOG(INFO) << "  registered " << (i + 1) << "/" << num_bufs << " ("
                      << elapsed << "s)";
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "Registration complete: "
              << std::chrono::duration<double>(t1 - t0).count() << "s";

    LOG(INFO) << "Target ready. First buffer at " << bufs[0]
              << ". Waiting for initiator (Ctrl+C to stop)...";

    if (FLAGS_use_device) cudaSetDevice(0);
    while (g_running) sleep(1);

    LOG(INFO) << "Shutting down target...";
    for (auto* p : bufs) {
        engine->unregisterLocalMemory(p);
        munmap(p, buf_bytes);
    }
    return 0;
}

struct LatencyStats {
    double avg_ms;
    double p50_ms;
    double p99_ms;
    double throughput_gbs;
};

static LatencyStats computeStats(std::vector<double>& latencies_ms,
                                 size_t transfer_bytes) {
    std::sort(latencies_ms.begin(), latencies_ms.end());
    double sum = std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0);
    size_t n = latencies_ms.size();
    LatencyStats stats;
    stats.avg_ms = sum / n;
    stats.p50_ms = latencies_ms[n / 2];
    stats.p99_ms = latencies_ms[std::min(n - 1, (size_t)(n * 0.99))];
    stats.throughput_gbs = (transfer_bytes / 1e9) / (stats.p50_ms / 1000.0);
    return stats;
}

static int runInitiator(TransferEngine* engine) {
    size_t transfer_bytes =
        static_cast<size_t>(FLAGS_transfer_mb * 1024 * 1024);

    LOG(INFO) << "=== Initiator Node ===";
    LOG(INFO) << "Target: " << FLAGS_target;
    LOG(INFO) << "Transfer size: " << FLAGS_transfer_mb << " MB";
    LOG(INFO) << "Threads: " << FLAGS_threads;

    if (FLAGS_target.empty()) {
        LOG(ERROR) << "--target required in initiator mode";
        return 1;
    }

    // Allocate local receive buffer (one per thread)
    size_t recv_bytes = transfer_bytes;
    std::vector<void*> recv_bufs(FLAGS_threads);
    for (int t = 0; t < FLAGS_threads; ++t) {
        if (!FLAGS_use_device) {
            recv_bufs[t] = allocateHugepage(recv_bytes, 0);
            if (!recv_bufs[t]) {
                LOG(ERROR) << "Failed to allocate receive buffer for thread "
                           << t;
                return 1;
            }
            recv_bufs[t] = memset(recv_bufs[t], 66, recv_bytes);
            if (!recv_bufs[t]) {
                LOG(ERROR) << "Failed to memset receive buffer for thread "
                           << t;
                return 1;
            }
        } else {
#if defined(USE_CUDA) || defined(USE_HIP)
            int tid_device = t % 4;
            recv_bufs[t] = allocateDevice(recv_bytes, tid_device);
            if (!recv_bufs[t]) {
                LOG(ERROR) << "Failed to allocate receive buffer for thread "
                           << t;
                return 1;
            }
            if (setDeviceMem(recv_bufs[t], recv_bytes, tid_device, 66) != 0) {
                LOG(ERROR) << "Failed to set data on receive buffer for thread "
                           << t;
            }
#endif
        }

        int ret =
            engine->registerLocalMemory(recv_bufs[t], recv_bytes, "*", true);
        if (ret != 0) {
            LOG(ERROR) << "Failed to register receive buffer: " << ret;
            return 1;
        }
    }
    LOG(INFO) << "Allocated and registered " << FLAGS_threads
              << " receive buffers of " << recv_bytes / 1e6 << " MB each";

    // Open remote segment
    auto segment_id = engine->openSegment(FLAGS_target);
    if (segment_id < 0) {
        LOG(ERROR) << "openSegment failed for " << FLAGS_target;
        return 1;
    }

    // Get remote buffer info
    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    if (!segment_desc || segment_desc->buffers.empty()) {
        LOG(ERROR) << "No remote buffers found";
        return 1;
    }
    size_t num_remote_bufs = segment_desc->buffers.size();
    LOG(INFO) << "Remote has " << num_remote_bufs << " buffers";
    LOG(INFO) << "First buffer: addr=0x" << std::hex
              << segment_desc->buffers[0].addr << std::dec
              << " size=" << segment_desc->buffers[0].length;

    for (auto buffer : segment_desc->buffers) {
        for (int i = 0; i <= 4; i++) {
            LOG(INFO) << "retry " << i << " selected device: "
                      << segment_desc->topology.selectDevice(buffer.name, i);
        }
        for (int i = 0; i < buffer.rkey.size(); i++) {
            LOG(INFO) << "remote device " << i << " has rkey " << buffer.rkey[i]
                      << "\n";
        }
    }

    // Connection warmup: small transfer to establish endpoints
    LOG(INFO) << "Warming up connection...";
    {
        size_t warmup_size = std::min(transfer_bytes, (size_t)(64 * 1024));
        auto batch_id = engine->allocateBatchID(1);
        TransferRequest req;
        req.opcode = TransferRequest::READ;
        req.source = (uint8_t*)recv_bufs[0];
        req.target_id = segment_id;
        req.target_offset = segment_desc->buffers[0].addr;
        req.length = warmup_size;
        auto s = engine->submitTransfer(batch_id, {req});
        if (!s.ok()) {
            LOG(ERROR) << "Warmup transfer failed: " << s.ToString();
            return 1;
        }
        while (true) {
            TransferStatus status;
            engine->getTransferStatus(batch_id, 0, status);
            if (status.s == TransferStatusEnum::COMPLETED) break;
            if (status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "Warmup transfer FAILED";
                return 1;
            }
        }
        if (recv_bufs[0]) engine->freeBatchID(batch_id);
    }
    LOG(INFO) << "Connection ready.";

    // Refresh segment desc after connection warmup (endpoints are now up)
    engine->syncSegmentCache(FLAGS_target);
    segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);

    // Worker function: each thread runs its own transfer loop
    struct ThreadResult {
        std::vector<double> latencies;
        int errors = 0;
    };

    auto workerFn = [&](int tid, int warmup_iters, int bench_iters,
                        ThreadResult* result) {
        void* my_recv = recv_bufs[tid];
        size_t buf_idx =
            (tid + 1) % num_remote_bufs;  // one thread <-> one buffer
        for (int w = 0; w < warmup_iters; ++w) {
            uint64_t raddr = segment_desc->buffers[buf_idx].addr;
            size_t rlen = segment_desc->buffers[buf_idx].length;
            size_t xfer = std::min(transfer_bytes, rlen);

            auto bid = engine->allocateBatchID(1);
            TransferRequest req;
            req.opcode = TransferRequest::READ;
            req.source = (uint8_t*)my_recv;
            req.target_id = segment_id;
            req.target_offset = raddr;
            req.length = xfer;

            engine->submitTransfer(bid, {req});
            while (true) {
                TransferStatus st;
                engine->getTransferStatus(bid, 0, st);
                if (st.s == TransferStatusEnum::COMPLETED ||
                    st.s == TransferStatusEnum::FAILED)
                    break;
            }
            engine->freeBatchID(bid);
        }

        for (int i = 0; i < bench_iters; ++i) {
            uint64_t raddr = segment_desc->buffers[buf_idx].addr;
            size_t rlen = segment_desc->buffers[buf_idx].length;
            size_t xfer = std::min(transfer_bytes, rlen);

            auto t0 = std::chrono::steady_clock::now();
            auto bid = engine->allocateBatchID(1);
            TransferRequest req;
            req.opcode = TransferRequest::READ;
            req.source = (uint8_t*)my_recv;
            req.target_id = segment_id;
            req.target_offset = raddr;
            req.length = xfer;
            auto s = engine->submitTransfer(bid, {req});
            if (!s.ok()) {
                result->errors++;
                engine->freeBatchID(bid);
                continue;
            }
            bool ok = false;
            while (true) {
                TransferStatus st;
                engine->getTransferStatus(bid, 0, st);
                if (st.s == TransferStatusEnum::COMPLETED) {
                    ok = true;
                    break;
                }
                if (st.s == TransferStatusEnum::FAILED) {
                    LOG(ERROR)
                        << "thread " << tid << " has failed the transfer\n";
                    result->errors++;
                    break;
                }
            }
            engine->freeBatchID(bid);
            if (ok) {
                auto t1 = std::chrono::steady_clock::now();
                result->latencies.push_back(
                    std::chrono::duration<double, std::milli>(t1 - t0).count());
            }
        }
    };

    // Run warmup + benchmark with threads
    int num_threads = FLAGS_threads;
    LOG(INFO) << "Running with " << num_threads << " threads, " << FLAGS_warmup
              << " warmup + " << FLAGS_iterations
              << " bench iterations per thread...";

    std::vector<ThreadResult> results(num_threads);
    std::vector<std::thread> threads;

    auto wall_t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back(workerFn, t, FLAGS_warmup, FLAGS_iterations,
                             &results[t]);
    }
    for (auto& th : threads) th.join();
    auto wall_t1 = std::chrono::steady_clock::now();
    double wall_ms =
        std::chrono::duration<double, std::milli>(wall_t1 - wall_t0).count();

    // Aggregate results
    std::vector<double> all_latencies;
    int total_errors = 0;
    for (auto& r : results) {
        all_latencies.insert(all_latencies.end(), r.latencies.begin(),
                             r.latencies.end());
        total_errors += r.errors;
    }

    if (all_latencies.empty()) {
        LOG(ERROR) << "All transfers failed";
        return 1;
    }

    auto stats = computeStats(all_latencies, transfer_bytes);
    size_t total_xfers = all_latencies.size();
    double total_bytes = (double)total_xfers * transfer_bytes;
    double agg_throughput = total_bytes / 1e9 / (wall_ms / 1000.0);

    LOG(INFO) << "=== Results (" << num_threads << " threads) ===";
    LOG(INFO) << "Transfer: " << FLAGS_transfer_mb << " MB x " << total_xfers
              << " transfers";
    LOG(INFO) << "Wall time: " << wall_ms << " ms";
    LOG(INFO) << "Per-transfer p50: " << stats.p50_ms << " ms"
              << "  p99: " << stats.p99_ms << " ms";
    LOG(INFO) << "Per-transfer throughput: " << stats.throughput_gbs << " GB/s";
    LOG(INFO) << "Aggregate throughput: " << agg_throughput << " GB/s";
    LOG(INFO) << "Errors: " << total_errors;

    // Per-thread stats
    for (int t = 0; t < num_threads; ++t) {
        if (results[t].latencies.empty()) continue;
        auto ts = computeStats(results[t].latencies, transfer_bytes);
        LOG(INFO) << "  Thread " << t << ": p50=" << ts.p50_ms
                  << "ms  tput=" << ts.throughput_gbs << " GB/s"
                  << "  iters=" << results[t].latencies.size()
                  << "  errors=" << results[t].errors;
    }

    // Cleanup
    for (int t = 0; t < FLAGS_threads; ++t) {
        if (!FLAGS_use_device) {
            if (((uint8_t*)recv_bufs[t])[66] != 42) {
                LOG(ERROR) << "transfer has corrupted data, expected to find "
                              "42 but found "
                           << ((int)((uint8_t*)recv_bufs[t])[66]);
            } else {
                LOG(INFO) << "transfer was successful";
            }
            engine->unregisterLocalMemory(recv_bufs[t]);
            munmap(recv_bufs[t], recv_bytes);
        } else {
#if defined(USE_CUDA) || defined(USE_HIP)
            bool result = checkDeviceMem(recv_bufs[t], t % 4, 42);
            if (!result) {
                LOG(ERROR)
                    << "transfer has corrupted data, expected to find 42";
            } else {
                LOG(INFO) << "transfer was successful\n";
            }
            freeDeviceMem(recv_bufs[t], t % 4);
#endif
        }
    }
    return total_errors > 0 ? 1 : 0;
}

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;

    setupSignalHandler();

    if (FLAGS_use_device && !can_use_device) {
        LOG(ERROR)
            << "Cannot use device memory, recompile with cmake -DUSE_CUDA!";
        return 1;
    }

    if (FLAGS_use_device) {
        LOG(INFO) << "Using device memory";
    }

    if (FLAGS_server.empty()) {
        LOG(ERROR) << "--server is required";
        return 1;
    }

    // Parse host:port
    auto colon = FLAGS_server.rfind(':');
    std::string host = FLAGS_server.substr(0, colon);
    uint64_t port = 12345;
    if (colon != std::string::npos)
        port = std::stoull(FLAGS_server.substr(colon + 1));

    auto engine = std::make_unique<TransferEngine>(false);
    int ret = engine->init(FLAGS_metadata, FLAGS_server, host, port);
    if (ret != 0) {
        LOG(ERROR) << "Engine init failed: " << ret;
        return 1;
    }

    // Discover topology and install EFA transport (all NICs)
    engine->getLocalTopology()->discover({});
    auto* xport = engine->installTransport("cxi", nullptr);
    if (!xport) {
        LOG(ERROR) << "installTransport(cxi) failed";
        return 1;
    }

    std::string actual_server = engine->getLocalIpAndPort();
    LOG(INFO) << "Actual server name (use this for --target): "
              << actual_server;

    if (FLAGS_mode == "target") {
        ret = runTarget(engine.get());
    } else if (FLAGS_mode == "initiator") {
        ret = runInitiator(engine.get());
    } else {
        LOG(ERROR) << "Unknown mode: " << FLAGS_mode;
        ret = 1;
    }

    return ret;
}
