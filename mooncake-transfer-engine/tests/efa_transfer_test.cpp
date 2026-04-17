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

// EFA multi-NIC transfer test: register 200x2GB on all NICs, then transfer.
//
// Target node: allocate 200x2GB buffers, register on all NICs, wait.
// Initiator node: allocate receive buffer, register, pull from target.
//
// Usage:
//   # Target (holds KV cache):
//   ./efa_transfer_test --mode target --server <target_ip>:12345 \
//       --num_bufs 200 --buf_size_gb 2
//
//   # Initiator (pulls data):
//   ./efa_transfer_test --mode initiator --server <initiator_ip>:12346 \
//       --target <target_ip>:12345 --num_bufs 200 --buf_size_gb 2 \
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

#include "transfer_engine.h"

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

static void* allocateHugepage(size_t size) {
    void* buf = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (buf == MAP_FAILED) {
        LOG(WARNING) << "Hugepage mmap failed (" << strerror(errno)
                     << "), falling back to regular pages";
        buf = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (buf == MAP_FAILED) return nullptr;
    }
    return buf;
}

static int runTarget(TransferEngine* engine) {
    size_t buf_bytes = static_cast<size_t>(FLAGS_buf_size_gb * 1024 * 1024 * 1024);
    int num_bufs = FLAGS_num_bufs;

    LOG(INFO) << "=== Target Node ===";
    LOG(INFO) << "Registering " << num_bufs << " x "
              << FLAGS_buf_size_gb << " GB = "
              << num_bufs * FLAGS_buf_size_gb << " GB";

    // Allocate buffers
    std::vector<void*> bufs;
    bufs.reserve(num_bufs);
    LOG(INFO) << "Allocating " << num_bufs << " buffers...";
    for (int i = 0; i < num_bufs; ++i) {
        void* buf = allocateHugepage(buf_bytes);
        if (!buf) {
            LOG(ERROR) << "Allocation failed at buffer " << i;
            for (auto* p : bufs) munmap(p, buf_bytes);
            return 1;
        }
        bufs.push_back(buf);
        if ((i + 1) % 50 == 0 || i == num_bufs - 1)
            LOG(INFO) << "  allocated " << (i + 1) << "/" << num_bufs;
    }

    // Register all buffers
    LOG(INFO) << "Registering " << num_bufs << " buffers on all NICs...";
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < num_bufs; ++i) {
        int ret = engine->registerLocalMemory(bufs[i], buf_bytes, "cpu:0", true);
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
            LOG(INFO) << "  registered " << (i + 1) << "/" << num_bufs
                      << " (" << elapsed << "s)";
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "Registration complete: "
              << std::chrono::duration<double>(t1 - t0).count() << "s";

    LOG(INFO) << "Target ready. First buffer at " << bufs[0]
              << ". Waiting for initiator (Ctrl+C to stop)...";

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
        recv_bufs[t] = allocateHugepage(recv_bytes);
        if (!recv_bufs[t]) {
            LOG(ERROR) << "Failed to allocate receive buffer for thread " << t;
            return 1;
        }
        int ret = engine->registerLocalMemory(recv_bufs[t], recv_bytes,
                                              "cpu:0", true);
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
        engine->freeBatchID(batch_id);
    }
    LOG(INFO) << "Connection ready.";

    // Refresh segment desc after connection warmup (endpoints are now up)
    engine->syncSegmentCache(FLAGS_target);
    segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);

    // Warmup iterations
    LOG(INFO) << "Running " << FLAGS_warmup << " warmup rounds...";
    for (int w = 0; w < FLAGS_warmup; ++w) {
        for (int i = 0; i < FLAGS_iterations; ++i) {
            size_t buf_idx = i % num_remote_bufs;
            uint64_t remote_addr = segment_desc->buffers[buf_idx].addr;
            size_t remote_len = segment_desc->buffers[buf_idx].length;
            size_t actual_xfer = std::min(transfer_bytes, remote_len);

            auto batch_id = engine->allocateBatchID(1);
            TransferRequest req;
            req.opcode = TransferRequest::READ;
            req.source = (uint8_t*)recv_bufs[0];
            req.target_id = segment_id;
            req.target_offset = remote_addr;
            req.length = actual_xfer;
            engine->submitTransfer(batch_id, {req});
            while (true) {
                TransferStatus status;
                engine->getTransferStatus(batch_id, 0, status);
                if (status.s == TransferStatusEnum::COMPLETED ||
                    status.s == TransferStatusEnum::FAILED)
                    break;
            }
            engine->freeBatchID(batch_id);
        }
    }

    // Benchmark: single-buffer reads from different remote buffers
    LOG(INFO) << "Benchmarking " << FLAGS_iterations
              << " iterations across " << num_remote_bufs << " remote buffers...";

    std::vector<double> latencies;
    int errors = 0;

    for (int i = 0; i < FLAGS_iterations; ++i) {
        size_t buf_idx = i % num_remote_bufs;
        uint64_t remote_addr = segment_desc->buffers[buf_idx].addr;
        size_t remote_len = segment_desc->buffers[buf_idx].length;
        size_t actual_xfer = std::min(transfer_bytes, remote_len);

        auto t0 = std::chrono::steady_clock::now();

        auto batch_id = engine->allocateBatchID(1);
        TransferRequest req;
        req.opcode = TransferRequest::READ;
        req.source = (uint8_t*)recv_bufs[0];
        req.target_id = segment_id;
        req.target_offset = remote_addr;
        req.length = actual_xfer;
        auto s = engine->submitTransfer(batch_id, {req});
        if (!s.ok()) {
            LOG(ERROR) << "submitTransfer failed at iter " << i;
            errors++;
            engine->freeBatchID(batch_id);
            continue;
        }

        while (true) {
            TransferStatus status;
            engine->getTransferStatus(batch_id, 0, status);
            if (status.s == TransferStatusEnum::COMPLETED) break;
            if (status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "Transfer FAILED at iter " << i;
                errors++;
                break;
            }
        }
        engine->freeBatchID(batch_id);

        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        latencies.push_back(ms);
    }

    if (latencies.empty()) {
        LOG(ERROR) << "All transfers failed";
        return 1;
    }

    auto stats = computeStats(latencies, transfer_bytes);
    LOG(INFO) << "=== Results ===";
    LOG(INFO) << "Transfer: " << FLAGS_transfer_mb << " MB";
    LOG(INFO) << "Iterations: " << latencies.size()
              << " (errors: " << errors << ")";
    LOG(INFO) << "Avg latency:  " << stats.avg_ms << " ms";
    LOG(INFO) << "p50 latency:  " << stats.p50_ms << " ms";
    LOG(INFO) << "p99 latency:  " << stats.p99_ms << " ms";
    LOG(INFO) << "Throughput:   " << stats.throughput_gbs << " GB/s";

    // Multi-block batch test: read from multiple remote buffers in one batch
    if (num_remote_bufs >= 4) {
        LOG(INFO) << "\n=== Multi-block batch transfer test ===";
        int batch_n = std::min((int)num_remote_bufs, 16);
        size_t per_block = std::min(transfer_bytes / batch_n, (size_t)(64 << 20));

        // Allocate a larger recv buffer for batch
        size_t batch_recv_bytes = per_block * batch_n;
        void* batch_recv = allocateHugepage(batch_recv_bytes);
        if (batch_recv) {
            int ret = engine->registerLocalMemory(batch_recv, batch_recv_bytes,
                                                  "cpu:0", true);
            if (ret == 0) {
                std::vector<double> batch_latencies;
                for (int i = 0; i < FLAGS_iterations; ++i) {
                    auto batch_id = engine->allocateBatchID(batch_n);
                    std::vector<TransferRequest> reqs;
                    for (int b = 0; b < batch_n; ++b) {
                        TransferRequest req;
                        req.opcode = TransferRequest::READ;
                        req.source = (uint8_t*)batch_recv + b * per_block;
                        req.target_id = segment_id;
                        req.target_offset =
                            segment_desc->buffers[b % num_remote_bufs].addr;
                        req.length = per_block;
                        reqs.push_back(req);
                    }

                    auto t0 = std::chrono::steady_clock::now();
                    engine->submitTransfer(batch_id, reqs);
                    for (int b = 0; b < batch_n; ++b) {
                        while (true) {
                            TransferStatus status;
                            engine->getTransferStatus(batch_id, b, status);
                            if (status.s == TransferStatusEnum::COMPLETED ||
                                status.s == TransferStatusEnum::FAILED)
                                break;
                        }
                    }
                    auto t1 = std::chrono::steady_clock::now();
                    engine->freeBatchID(batch_id);
                    batch_latencies.push_back(
                        std::chrono::duration<double, std::milli>(t1 - t0)
                            .count());
                }

                auto bstats =
                    computeStats(batch_latencies, per_block * batch_n);
                LOG(INFO) << "Batch " << batch_n << " x "
                          << per_block / (1024 * 1024) << " MB:";
                LOG(INFO) << "  Avg: " << bstats.avg_ms << " ms"
                          << "  p50: " << bstats.p50_ms << " ms"
                          << "  Throughput: " << bstats.throughput_gbs
                          << " GB/s";

                engine->unregisterLocalMemory(batch_recv);
            }
            munmap(batch_recv, batch_recv_bytes);
        }
    }

    // Cleanup
    for (int t = 0; t < FLAGS_threads; ++t) {
        engine->unregisterLocalMemory(recv_bufs[t]);
        munmap(recv_bufs[t], recv_bytes);
    }
    return errors > 0 ? 1 : 0;
}

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;

    setupSignalHandler();

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
    auto* xport = engine->installTransport("efa", nullptr);
    if (!xport) {
        LOG(ERROR) << "installTransport(efa) failed";
        return 1;
    }

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
