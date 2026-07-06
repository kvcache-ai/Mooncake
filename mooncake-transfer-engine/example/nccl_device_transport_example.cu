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

// Two-rank NCCL DeviceTransport correctness example.
//
// Launch one process per rank with a bootstrap file visible to both ranks.
// Use --force_gin to exercise GIN even when both ranks are in one LSA team.

#include <cuda_runtime.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "transport/device/cuda/cuda_ops.cuh"
#include "transport/device/device_transport.h"
#include "transport/device/nccl_device.cuh"

DEFINE_int32(rank, -1, "Rank of this process (0 or 1)");
DEFINE_int32(world_size, 2, "Number of ranks; this example requires 2");
DEFINE_int32(gpu_id, -1,
             "CUDA device (default: rank modulo visible device count)");
DEFINE_int32(data_bytes, 4096, "Number of bytes transferred from rank 0");
DEFINE_int32(timeout_seconds, 120, "Bootstrap file wait timeout");
DEFINE_string(bootstrap_file, "/tmp/mooncake_nccl_device_id.bin",
              "Bootstrap file path visible to both ranks");
DEFINE_bool(force_gin, false, "Use GIN even when the peer is LSA reachable");
DEFINE_bool(disable_gin, false,
            "Create an LSA-only device communicator without GIN resources");

namespace {

constexpr size_t kSignalAlignment = 256;

void checkCuda(cudaError_t result, const char* operation) {
    CHECK_EQ(result, cudaSuccess)
        << operation << ": " << cudaGetErrorString(result);
}

size_t alignUp(size_t value, size_t alignment) {
    return (value + alignment - 1) / alignment * alignment;
}

void writeUniqueId(const std::vector<int32_t>& id) {
    const std::string temporary = FLAGS_bootstrap_file + ".tmp";
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    CHECK(output) << "Cannot open " << temporary;
    const uint32_t words = static_cast<uint32_t>(id.size());
    output.write(reinterpret_cast<const char*>(&words), sizeof(words));
    output.write(reinterpret_cast<const char*>(id.data()),
                 id.size() * sizeof(int32_t));
    output.close();
    CHECK_EQ(std::rename(temporary.c_str(), FLAGS_bootstrap_file.c_str()), 0)
        << "Cannot publish " << FLAGS_bootstrap_file;
}

std::vector<int32_t> readUniqueId() {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_timeout_seconds);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream input(FLAGS_bootstrap_file, std::ios::binary);
        if (input) {
            uint32_t words = 0;
            input.read(reinterpret_cast<char*>(&words), sizeof(words));
            if (input && words > 0 && words <= 256) {
                std::vector<int32_t> id(words);
                input.read(reinterpret_cast<char*>(id.data()),
                           id.size() * sizeof(int32_t));
                if (input) return id;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    LOG(FATAL) << "Timed out waiting for " << FLAGS_bootstrap_file;
    return {};
}

__global__ void senderKernel(mooncake::device::NcclDeviceContext ctx,
                             ncclWindow_t window, char* local_buffer,
                             size_t bytes, size_t data_signal_offset,
                             size_t ack_signal_offset, bool force_gin,
                             int* result) {
    using namespace mooncake::device;
    if (blockIdx.x != 0 || threadIdx.x != 0) return;

    constexpr int peer = 1;
    if (!force_gin && mc_nccl_lsa_available(ctx, peer)) {
        char* peer_buffer =
            static_cast<char*>(mc_nccl_lsa_ptr(ctx, window, 0, peer));
        for (size_t i = 0; i < bytes; ++i) peer_buffer[i] = local_buffer[i];
        mc_st_release(reinterpret_cast<int*>(peer_buffer + data_signal_offset),
                      1);

        int* ack = reinterpret_cast<int*>(local_buffer + ack_signal_offset);
        while (mc_ld_acquire(ack) != 1) {
        }
        *result = 0;
        return;
    }

    if (!mc_nccl_gin_available(ctx)) {
        *result = -1;
        return;
    }

    mc_nccl_gin_put_signal(ctx, 0, peer, window, 0, window, 0, bytes, 0);
    mc_nccl_gin_flush(ctx, 0);
    mc_nccl_gin_wait_signal(ctx, 0, 1, 1);
    *result = 0;
}

__global__ void receiverKernel(mooncake::device::NcclDeviceContext ctx,
                               ncclWindow_t window, char* local_buffer,
                               size_t bytes, size_t data_signal_offset,
                               size_t ack_signal_offset, bool force_gin,
                               int* result) {
    using namespace mooncake::device;
    if (blockIdx.x != 0 || threadIdx.x != 0) return;

    constexpr int peer = 0;
    const bool use_lsa = !force_gin && mc_nccl_lsa_available(ctx, peer);
    if (use_lsa) {
        int* signal =
            reinterpret_cast<int*>(local_buffer + data_signal_offset);
        while (mc_ld_acquire(signal) != 1) {
        }
    } else {
        if (!mc_nccl_gin_available(ctx)) {
            *result = -1;
            return;
        }
        mc_nccl_gin_wait_signal(ctx, 0, 0, 1);
    }

    int mismatches = 0;
    for (size_t i = 0; i < bytes; ++i) {
        if (local_buffer[i] != static_cast<char>(i & 0xff)) ++mismatches;
    }
    *result = mismatches;

    if (use_lsa) {
        char* peer_buffer =
            static_cast<char*>(mc_nccl_lsa_ptr(ctx, window, 0, peer));
        mc_st_release(reinterpret_cast<int*>(peer_buffer + ack_signal_offset),
                      1);
    } else {
        mc_nccl_gin_signal(ctx, 0, peer, 1);
        mc_nccl_gin_flush(ctx, 0);
    }
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    CHECK_EQ(FLAGS_world_size, 2);
    CHECK(FLAGS_rank == 0 || FLAGS_rank == 1);
    CHECK_GT(FLAGS_data_bytes, 0);
    CHECK(!(FLAGS_force_gin && FLAGS_disable_gin));

    int device_count = 0;
    checkCuda(cudaGetDeviceCount(&device_count), "cudaGetDeviceCount");
    CHECK_GT(device_count, 0);
    const int gpu_id =
        FLAGS_gpu_id >= 0 ? FLAGS_gpu_id : FLAGS_rank % device_count;
    checkCuda(cudaSetDevice(gpu_id), "cudaSetDevice");

    auto engine = std::make_unique<mooncake::TransferEngine>(false);
    auto* transport = engine->getOrCreateNcclTransport();
    CHECK_NOTNULL(transport);

    std::vector<int32_t> unique_id;
    if (FLAGS_rank == 0) {
        std::remove(FLAGS_bootstrap_file.c_str());
        unique_id = transport->createUniqueId();
        CHECK(!unique_id.empty());
        writeUniqueId(unique_id);
    } else {
        unique_id = readUniqueId();
    }

    mooncake::device::NcclTransportConfig config;
    config.rank = FLAGS_rank;
    config.num_ranks = FLAGS_world_size;
    config.gin_context_count = 1;
    if (FLAGS_disable_gin) {
        config.gin_connection_type = NCCL_GIN_CONNECTION_NONE;
        config.gin_signal_count = 0;
    } else {
        config.gin_signal_count = 2;
    }
    CHECK_EQ(transport->initialize(config, unique_id), 0);

    const size_t data_signal_offset =
        alignUp(static_cast<size_t>(FLAGS_data_bytes), kSignalAlignment);
    const size_t ack_signal_offset =
        data_signal_offset + kSignalAlignment;
    const size_t allocation_bytes =
        ack_signal_offset + kSignalAlignment;

    char* buffer =
        static_cast<char*>(transport->allocateBuffer(allocation_bytes));
    CHECK_NOTNULL(buffer);
    checkCuda(cudaMemset(buffer, 0, allocation_bytes), "cudaMemset");

    if (FLAGS_rank == 0) {
        std::vector<char> pattern(FLAGS_data_bytes);
        for (int i = 0; i < FLAGS_data_bytes; ++i) {
            pattern[i] = static_cast<char>(i & 0xff);
        }
        checkCuda(cudaMemcpy(buffer, pattern.data(), pattern.size(),
                             cudaMemcpyHostToDevice),
                  "cudaMemcpy(pattern)");
    }

    ncclWindow_t window = nullptr;
    CHECK_EQ(transport->registerWindow(buffer, allocation_bytes, &window), 0);

    int* device_result = nullptr;
    checkCuda(cudaMalloc(&device_result, sizeof(int)), "cudaMalloc(result)");
    checkCuda(cudaMemset(device_result, 0xff, sizeof(int)),
              "cudaMemset(result)");

    const auto context = transport->deviceContext();
    if (FLAGS_rank == 0) {
        senderKernel<<<1, 1>>>(context, window, buffer, FLAGS_data_bytes,
                               data_signal_offset, ack_signal_offset,
                               FLAGS_force_gin, device_result);
    } else {
        receiverKernel<<<1, 1>>>(context, window, buffer, FLAGS_data_bytes,
                                 data_signal_offset, ack_signal_offset,
                                 FLAGS_force_gin, device_result);
    }
    checkCuda(cudaGetLastError(), "kernel launch");
    checkCuda(cudaDeviceSynchronize(), "cudaDeviceSynchronize");

    int result = -1;
    checkCuda(cudaMemcpy(&result, device_result, sizeof(result),
                         cudaMemcpyDeviceToHost),
              "cudaMemcpy(result)");
    CHECK_EQ(result, 0) << "Device transfer validation failed";

    const auto& properties = transport->properties();
    LOG(INFO) << "PASS rank=" << FLAGS_rank
              << " gin_type=" << static_cast<int>(properties.gin_type)
              << " gin_connections=" << properties.gin_connection_count
              << " force_gin=" << FLAGS_force_gin
              << " disable_gin=" << FLAGS_disable_gin;

    checkCuda(cudaFree(device_result), "cudaFree(result)");
    CHECK_EQ(transport->deregisterWindow(window), 0);
    CHECK_EQ(transport->freeBuffer(buffer), 0);
    CHECK_EQ(transport->shutdown(), 0);

    if (FLAGS_rank == 0) std::remove(FLAGS_bootstrap_file.c_str());
    return 0;
}
