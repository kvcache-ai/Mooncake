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

// Device Transport Example — two-rank P2P write + signal via Device API.
//
// This example demonstrates the full Device API lifecycle:
//   1. Host side:  P2pTransport for IPC handle exchange and peer mapping.
//   2. Device side: CommCtx + mc_route_put + mc_signal for GPU-initiated
//      P2P data transfer and notification.
//
// Usage (two terminals on the same node):
//   $ ./device_transport_example --rank=0
//   $ ./device_transport_example --rank=1
//
// The example uses file-based IPC handle exchange (no external dependencies).
// Requires 2 GPUs with P2P access (NVLink or PCIe).

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <cuda_runtime.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <fstream>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "transport/device/comm_device.cuh"
#include "transport/device/device_transport.h"

DEFINE_int32(rank, -1, "Rank of this process (0 or 1)");
DEFINE_int32(world_size, 2, "Total number of ranks");
DEFINE_int32(gpu_id, -1, "GPU ID (defaults to rank)");
DEFINE_string(metadata_server, "P2PHANDSHAKE",
              "Metadata server (P2PHANDSHAKE for no external deps)");
DEFINE_string(local_server_name, "",
              "Local server name (default: 127.0.0.1:<port>)");
DEFINE_string(ipc_dir, "/tmp", "Directory for IPC handle exchange files");
DEFINE_int32(kDataBytes, 4096, "Bytes to transfer in the P2P write");
DEFINE_int32(kSignalOffset, 0, "Offset within buffer for the signal word");

namespace {

static void checkCuda(cudaError_t err, const char* msg) {
    if (err != cudaSuccess) {
        LOG(FATAL) << msg << ": " << cudaGetErrorString(err);
    }
}

// ---------------------------------------------------------------------------
// File-based IPC handle exchange
// ---------------------------------------------------------------------------

static std::string ipcFilePath(int rank) {
    return FLAGS_ipc_dir + "/device_api_ex_rank_" + std::to_string(rank) +
           ".bin";
}

static void writeIpcHandle(int rank, const std::vector<int32_t>& handle) {
    std::string path = ipcFilePath(rank);
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    CHECK(ofs) << "Failed to open " << path << " for writing";
    uint32_t size = static_cast<uint32_t>(handle.size());
    ofs.write(reinterpret_cast<const char*>(&size), sizeof(size));
    ofs.write(reinterpret_cast<const char*>(handle.data()),
              handle.size() * sizeof(int32_t));
    ofs.close();
    LOG(INFO) << "Rank " << rank << " wrote IPC handle (" << handle.size()
              << " int32s) to " << path;
}

static std::vector<int32_t> readIpcHandle(int rank) {
    std::string path = ipcFilePath(rank);
    // Poll until the file appears (peer may not have written it yet).
    for (int attempt = 0; attempt < 300; ++attempt) {
        std::ifstream ifs(path, std::ios::binary);
        if (ifs) {
            uint32_t size = 0;
            ifs.read(reinterpret_cast<char*>(&size), sizeof(size));
            CHECK(size > 0 && size <= 256)
                << "Invalid IPC handle size: " << size;
            std::vector<int32_t> handle(size);
            ifs.read(reinterpret_cast<char*>(handle.data()),
                     size * sizeof(int32_t));
            ifs.close();
            LOG(INFO) << "Rank " << FLAGS_rank << " read IPC handle from rank "
                      << rank << " (" << size << " int32s)";
            return handle;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    LOG(FATAL) << "Timeout waiting for IPC handle from rank " << rank;
    return {};
}

// ---------------------------------------------------------------------------
// Device-side kernel
// ---------------------------------------------------------------------------

// GDR buffer layout:
//   [0 .. kDataBytes)              — data region
//   [kDataBytes .. kDataBytes+4)   — signal word (int32_t)
static constexpr int kSignalWordOffset = 4096;  // must match kDataBytes default

// Rank 0: P2P-write data to rank 1, then signal.
__global__ void senderKernel(mooncake::device::CommCtx ctx, int dst_rank,
                             int data_bytes) {
    using namespace mooncake::device;
    if (threadIdx.x != 0 || blockIdx.x != 0) return;

    // Write a known pattern to the peer's data region via P2P.
    char* local_data = reinterpret_cast<char*>(ctx.p2p.local_base);
    void* peer_data = mc_route_put(ctx, dst_rank, local_data);
    if (peer_data == nullptr) {
        printf("[Rank 0] FAIL: P2P not available to rank %d\n", dst_rank);
        asm("trap;");
        return;
    }

    // Fill local buffer with pattern, then copy to peer via P2P store.
    for (int i = 0; i < data_bytes; ++i) {
        local_data[i] = static_cast<char>(i & 0xFF);
    }
    // Use 16-byte non-temporal stores for the bulk copy.
    for (int i = 0; i < data_bytes; i += 16) {
        int4 val = mc_ld_nc(reinterpret_cast<const int4*>(local_data + i));
        mc_st_na(
            reinterpret_cast<int4*>(reinterpret_cast<char*>(peer_data) + i),
            val);
    }

    // Signal: write 1 to the peer's signal word.
    int* local_sig = reinterpret_cast<int*>(local_data + kSignalWordOffset);
    *local_sig = 1;
    mc_signal(ctx, dst_rank, 0 /*channel*/, 1 /*qps_per_rank*/, local_sig, 1);
}

// Rank 1: wait for signal from rank 0, then verify data.
__global__ void receiverKernel(mooncake::device::CommCtx ctx, int src_rank,
                               int data_bytes) {
    using namespace mooncake::device;
    if (threadIdx.x != 0 || blockIdx.x != 0) return;

    char* local_data = reinterpret_cast<char*>(ctx.p2p.local_base);
    int* local_sig = reinterpret_cast<int*>(local_data + kSignalWordOffset);

    // Spin-wait for the signal.
    // The sender writes the signal via P2P store (mc_p2p_signal →
    // mc_st_release). We use ld_acquire to observe it.
    while (mc_ld_acquire(local_sig) == 0) {
        __threadfence_block();
    }

    // Verify the data written by rank 0.
    int mismatches = 0;
    for (int i = 0; i < data_bytes; ++i) {
        if (local_data[i] != static_cast<char>(i & 0xFF)) {
            ++mismatches;
        }
    }
    if (mismatches > 0) {
        printf("[Rank 1] FAIL: %d byte mismatches in received data\n",
               mismatches);
    } else {
        printf("[Rank 1] PASS: all %d bytes match expected pattern\n",
               data_bytes);
    }
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    int rank = FLAGS_rank;
    int world_size = FLAGS_world_size;
    CHECK(rank == 0 || rank == 1) << "--rank must be 0 or 1";
    CHECK_EQ(world_size, 2) << "This example requires exactly 2 ranks";
    CHECK_EQ(FLAGS_kDataBytes % 16, 0) << "kDataBytes must be a multiple of 16";
    CHECK_LE(FLAGS_kDataBytes, kSignalWordOffset)
        << "kDataBytes cannot exceed " << kSignalWordOffset;

    int gpu_id = FLAGS_gpu_id >= 0 ? FLAGS_gpu_id : rank;
    checkCuda(cudaSetDevice(gpu_id), "cudaSetDevice");
    LOG(INFO) << "Rank " << rank << " using GPU " << gpu_id;

    // -----------------------------------------------------------------------
    // 1. Create TransferEngine and get P2pTransport.
    // -----------------------------------------------------------------------
    auto engine = std::make_unique<mooncake::TransferEngine>(false);

    std::string local_name = FLAGS_local_server_name;
    if (local_name.empty()) {
        local_name = "127.0.0.1:" + std::to_string(12345 + rank);
    }
    engine->init(FLAGS_metadata_server, local_name);

    auto* p2p = engine->getOrCreateP2pTransport(world_size);
    CHECK_NOTNULL(p2p);

    // -----------------------------------------------------------------------
    // 2. Allocate GDR buffer and fill with initial pattern.
    // -----------------------------------------------------------------------
    constexpr size_t kBufSize = kSignalWordOffset + sizeof(int32_t);
    void* gdr_buffer = p2p->allocateBuffer(kBufSize);
    CHECK_NOTNULL(gdr_buffer);
    LOG(INFO) << "Rank " << rank << " allocated GDR buffer: " << gdr_buffer;

    // Zero-initialize the signal word.
    checkCuda(cudaMemset(gdr_buffer, 0, kBufSize), "cudaMemset");

    // -----------------------------------------------------------------------
    // 3. Export IPC handle and exchange with peer.
    // -----------------------------------------------------------------------
    auto my_handle = p2p->exportIpcHandle(gdr_buffer);
    CHECK(!my_handle.empty()) << "exportIpcHandle returned empty vector";
    writeIpcHandle(rank, my_handle);

    // Read peer's handle.
    int peer_rank = 1 - rank;
    auto peer_handle = readIpcHandle(peer_rank);

    // Build remote_handles vector: index by rank.
    std::vector<std::vector<int32_t>> remote_handles(world_size);
    remote_handles[rank] = my_handle;
    remote_handles[peer_rank] = peer_handle;

    std::vector<int> active_ranks_mask(world_size, 1);

    p2p->importPeerHandles(gdr_buffer, rank, world_size, remote_handles,
                           active_ranks_mask);

    CHECK(p2p->allPeersAccessible())
        << "P2P not accessible between rank " << rank << " and peer";
    LOG(INFO) << "Rank " << rank << " P2P peer mapping complete";

    // -----------------------------------------------------------------------
    // 4. Build CommCtx and launch kernel.
    // -----------------------------------------------------------------------
    mooncake::device::CommCtx ctx{};
    ctx.rank = rank;
    ctx.p2p.available = p2p->availableTablePtr();
    ctx.p2p.peer_ptrs = p2p->peerPtrsTablePtr();
    ctx.p2p.local_base = gdr_buffer;
    // IBGDA fields left as nullptr (not used for P2P-only example).

    // Pass CommCtx by value — CUDA copies it to kernel parameter space.
    if (rank == 0) {
        LOG(INFO) << "Rank 0 launching sender kernel...";
        senderKernel<<<1, 1>>>(ctx, peer_rank, FLAGS_kDataBytes);
    } else {
        LOG(INFO) << "Rank 1 launching receiver kernel...";
        receiverKernel<<<1, 1>>>(ctx, peer_rank, FLAGS_kDataBytes);
    }
    checkCuda(cudaDeviceSynchronize(), "cudaDeviceSynchronize");

    // -----------------------------------------------------------------------
    // 5. Barrier: rank 1 signals completion, rank 0 waits before cleanup.
    //    The IPC handle is only valid while the original allocation exists,
    //    so rank 0 must not free its buffer until rank 1 has opened it.
    // -----------------------------------------------------------------------
    if (rank == 1) {
        // Signal rank 0 that we're done.
        std::string done_path = FLAGS_ipc_dir + "/device_api_ex_done.bin";
        std::ofstream ofs(done_path);
        ofs << "1";
        ofs.close();
    } else {
        // Wait for rank 1 to finish.
        std::string done_path = FLAGS_ipc_dir + "/device_api_ex_done.bin";
        for (int attempt = 0; attempt < 300; ++attempt) {
            std::ifstream ifs(done_path);
            if (ifs) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // -----------------------------------------------------------------------
    // 6. Cleanup.
    // -----------------------------------------------------------------------
    p2p->freeBuffer(gdr_buffer);

    // Remove IPC handle files.
    std::remove(ipcFilePath(rank).c_str());
    if (rank == 0) {
        std::remove((FLAGS_ipc_dir + "/device_api_ex_done.bin").c_str());
    }

    LOG(INFO) << "Rank " << rank << " done.";
    return 0;
}
