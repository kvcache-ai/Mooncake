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

// Reproduce: single-NIC + 200GB 4KB-page buffer registration.
// Expected: auto-split into chunks and register all on the one NIC.
// Current bug: "Buffer requires N chunks but only 1 NICs available."
//
// Usage:
//   ./efa_single_nic_large_mr_test [--nic rdmap85s0] [--size_gb 200]
//
// The buffer is allocated via mmap (no MAP_HUGETLB) to get 4KB pages,
// matching the customer's /dev/shm mmap pattern.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/mman.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "transfer_engine_c.h"

DEFINE_string(nic, "",
              "EFA device name (e.g. rdmap85s0). "
              "If empty, auto-detects the first rdmap* device.");
DEFINE_double(size_gb, 200.0, "Total memory in GB to register");
DEFINE_double(chunk_gb, 0, "Per-buffer chunk size in GB (0 = single buffer)");
DEFINE_string(server, "127.0.0.1:12345", "Local server name");

static std::string detectFirstEfaDevice() {
    FILE* fp = popen(
        "ls /sys/class/infiniband/ 2>/dev/null | grep rdmap | head -1", "r");
    if (!fp) return "";
    char buf[256] = {};
    if (fgets(buf, sizeof(buf), fp)) {
        size_t len = strlen(buf);
        if (len > 0 && buf[len - 1] == '\n') buf[len - 1] = '\0';
    }
    pclose(fp);
    return std::string(buf);
}

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;

    std::string nic = FLAGS_nic;

    size_t size_bytes = static_cast<size_t>(FLAGS_size_gb * 1024 * 1024 * 1024);
    LOG(INFO) << "Buffer size: " << FLAGS_size_gb << " GB (" << size_bytes
              << " bytes)";

    // Create engine
    transfer_engine_t engine = createTransferEngine(
        "P2PHANDSHAKE", FLAGS_server.c_str(), "127.0.0.1", 12345, 0);
    if (!engine) {
        LOG(ERROR) << "createTransferEngine failed";
        return 1;
    }

    // discoverTopology to populate device list
    int ret = discoverTopology(engine);
    if (ret != 0) {
        LOG(ERROR) << "discoverTopology failed: " << ret;
        destroyTransferEngine(engine);
        return 1;
    }

    // Install EFA transport — optionally restrict to single NIC
    transport_t xport = nullptr;
    if (!nic.empty()) {
        std::string matrix = "{\"cpu:0\": [[\"" + nic + "\"], []]}";
        LOG(INFO) << "nic_priority_matrix: " << matrix;
        char* matrix_cstr = const_cast<char*>(matrix.c_str());
        void* args[] = {matrix_cstr};
        xport = installTransport(engine, "efa", args);
    } else {
        LOG(INFO) << "Using all available NICs (no nic restriction)";
        xport = installTransport(engine, "efa", nullptr);
    }
    if (!xport) {
        LOG(ERROR) << "installTransport(efa) failed";
        destroyTransferEngine(engine);
        return 1;
    }

    // Determine buffer count and per-buffer size
    size_t chunk_bytes = 0;
    int num_bufs = 1;
    if (FLAGS_chunk_gb > 0) {
        chunk_bytes = static_cast<size_t>(FLAGS_chunk_gb * 1024 * 1024 * 1024);
        num_bufs = (size_bytes + chunk_bytes - 1) / chunk_bytes;
    } else {
        chunk_bytes = size_bytes;
    }
    LOG(INFO) << "Plan: " << num_bufs << " buffers × "
              << chunk_bytes / (1024 * 1024)
              << " MB = " << (num_bufs * chunk_bytes) / (1024ULL * 1024 * 1024)
              << " GB";

    // Allocate all buffers with hugepages
    std::vector<void*> bufs;
    bufs.reserve(num_bufs);
    LOG(INFO) << "Allocating " << num_bufs << " buffers...";
    for (int i = 0; i < num_bufs; ++i) {
        void* buf = mmap(nullptr, chunk_bytes, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (buf == MAP_FAILED) {
            LOG(ERROR) << "mmap failed at buffer " << i << "/" << num_bufs
                       << ": " << strerror(errno);
            for (auto* p : bufs) munmap(p, chunk_bytes);
            destroyTransferEngine(engine);
            return 1;
        }
        bufs.push_back(buf);
        if ((i + 1) % 50 == 0 || i == num_bufs - 1) {
            LOG(INFO) << "  allocated " << (i + 1) << "/" << num_bufs;
        }
    }
    LOG(INFO) << "All buffers allocated";

    // Register each buffer
    LOG(INFO) << "Registering " << num_bufs << " × "
              << chunk_bytes / (1024 * 1024) << " MB on single NIC...";
    auto t0 = std::chrono::steady_clock::now();
    int failures = 0;
    for (int i = 0; i < num_bufs; ++i) {
        ret = registerLocalMemory(engine, bufs[i], chunk_bytes, "cpu:0", 1);
        if (ret != 0) {
            LOG(ERROR) << "FAILED at buffer " << i << "/" << num_bufs
                       << " (addr=" << bufs[i] << "): ret=" << ret;
            failures++;
            break;
        }
        if ((i + 1) % 50 == 0 || i == num_bufs - 1) {
            auto now = std::chrono::steady_clock::now();
            double elapsed = std::chrono::duration<double>(now - t0).count();
            LOG(INFO) << "  registered " << (i + 1) << "/" << num_bufs << " ("
                      << elapsed << "s)";
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    double total_time = std::chrono::duration<double>(t1 - t0).count();

    if (failures == 0) {
        LOG(INFO) << "SUCCESS: registered " << num_bufs << " × "
                  << chunk_bytes / (1024 * 1024) << " MB ("
                  << (num_bufs * chunk_bytes) / (1024ULL * 1024 * 1024)
                  << " GB) in " << total_time << "s";
    } else {
        LOG(ERROR) << "FAILED after " << total_time << "s";
    }

    // Cleanup
    for (auto* p : bufs) {
        unregisterLocalMemory(engine, p);
        munmap(p, chunk_bytes);
    }
    destroyTransferEngine(engine);

    return failures == 0 ? 0 : 1;
}
