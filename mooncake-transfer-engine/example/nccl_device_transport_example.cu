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
// Launch one process per rank with the same unique --run_id and a bootstrap
// directory visible to both ranks. Use --force_gin to exercise GIN even when
// both ranks are in one LSA team.

#include <cuda_runtime.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cctype>
#include <chrono>
#include <cstdio>
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
DEFINE_string(bootstrap_dir, "/tmp",
              "Directory visible to both ranks for bootstrap");
DEFINE_string(run_id, "",
              "Unique identifier shared by ranks for this invocation");
DEFINE_bool(force_gin, false, "Use GIN even when the peer is LSA reachable");
DEFINE_bool(disable_gin, false,
            "Create an LSA-only device communicator without GIN resources");
DEFINE_bool(require_lsa_multimem, false,
            "Require LSA multimem resources and use them for LSA barriers");

namespace {

constexpr size_t kSignalAlignment = 256;

void checkCuda(cudaError_t result, const char* operation) {
    CHECK_EQ(result, cudaSuccess)
        << operation << ": " << cudaGetErrorString(result);
}

size_t alignUp(size_t value, size_t alignment) {
    return (value + alignment - 1) / alignment * alignment;
}

std::string bootstrapPath() {
    CHECK(!FLAGS_run_id.empty())
        << "--run_id must be unique for every invocation";
    for (unsigned char c : FLAGS_run_id) {
        CHECK(std::isalnum(c) || c == '-' || c == '_' || c == '.')
            << "--run_id may contain only letters, digits, '.', '-', and '_'";
    }
    return FLAGS_bootstrap_dir + "/mooncake_nccl_device_" + FLAGS_run_id +
           ".bin";
}

void writeUniqueId(const std::string& path, const std::vector<int32_t>& id) {
    const std::string temporary = path + ".tmp";
    std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
    CHECK(output) << "Cannot open " << temporary;
    const uint32_t words = static_cast<uint32_t>(id.size());
    output.write(reinterpret_cast<const char*>(&words), sizeof(words));
    output.write(reinterpret_cast<const char*>(id.data()),
                 id.size() * sizeof(int32_t));
    output.close();
    CHECK_EQ(std::rename(temporary.c_str(), path.c_str()), 0)
        << "Cannot publish " << path;
}

std::vector<int32_t> readUniqueId(const std::string& path) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_timeout_seconds);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream input(path, std::ios::binary);
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
    LOG(FATAL) << "Timed out waiting for " << path;
    return {};
}

void publishMarker(const std::string& path) {
    const std::string temporary = path + ".tmp";
    std::ofstream output(temporary, std::ios::trunc);
    CHECK(output) << "Cannot open " << temporary;
    output << "done\n";
    output.close();
    CHECK_EQ(std::rename(temporary.c_str(), path.c_str()), 0)
        << "Cannot publish " << path;
}

void waitForMarker(const std::string& path) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(FLAGS_timeout_seconds);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream input(path);
        std::string marker;
        if (input >> marker && marker == "done") return;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    LOG(FATAL) << "Timed out waiting for " << path;
}

__global__ void senderKernel(mooncake::device::NcclDeviceContext ctx,
                             char* local_buffer, size_t bytes,
                             size_t data_signal_offset,
                             size_t ack_signal_offset, bool force_gin,
                             int* result) {
    using namespace mooncake::device;
    if (blockIdx.x != 0 || threadIdx.x != 0) return;

    constexpr int peer = 1;
    mc_nccl_lsa_barrier(ctx, 0);

    const NcclDeviceRoute route = mc_nccl_route(ctx, peer);
    const bool use_gin = force_gin || route == NcclDeviceRoute::kGin;
    auto* data_signal =
        reinterpret_cast<uint64_t*>(local_buffer + data_signal_offset);
    auto* ack_signal =
        reinterpret_cast<uint64_t*>(local_buffer + ack_signal_offset);

    if (use_gin) {
        if (!mc_nccl_gin_available(ctx)) {
            *result = -1;
            return;
        }
        mc_nccl_put_with_signal(ctx, 0, peer, 1, local_buffer, local_buffer,
                                static_cast<uint32_t>(bytes), data_signal, 1,
                                0);
        mc_nccl_flush(ctx, 0, 0);
    } else if (route == NcclDeviceRoute::kLsa) {
        char* peer_buffer =
            static_cast<char*>(mc_nccl_peer_ptr(ctx, peer, local_buffer));
        for (size_t i = 0; i < bytes; ++i) peer_buffer[i] = local_buffer[i];
        mc_nccl_signal_add(ctx, peer, 0, 1, data_signal, 1, 0);
    } else {
        *result = -2;
        return;
    }

    mc_nccl_wait_signal(ctx, 0, ack_signal, 1, 0);
    mc_nccl_lsa_barrier(ctx, 0);
    *result = 0;
}

__global__ void receiverKernel(mooncake::device::NcclDeviceContext ctx,
                               char* local_buffer, size_t bytes,
                               size_t data_signal_offset,
                               size_t ack_signal_offset, int* result) {
    using namespace mooncake::device;
    if (blockIdx.x != 0 || threadIdx.x != 0) return;

    constexpr int peer = 0;
    mc_nccl_lsa_barrier(ctx, 0);

    auto* data_signal =
        reinterpret_cast<uint64_t*>(local_buffer + data_signal_offset);
    auto* ack_signal =
        reinterpret_cast<uint64_t*>(local_buffer + ack_signal_offset);
    mc_nccl_wait_signal(ctx, 0, data_signal, 1, 0);

    int mismatches = 0;
    for (size_t i = 0; i < bytes; ++i) {
        if (local_buffer[i] != static_cast<char>(i & 0xff)) ++mismatches;
    }
    *result = mismatches;

    mc_nccl_signal_add(ctx, peer, 0, 1, ack_signal, 1, 0);
    if (mc_nccl_route(ctx, peer) == NcclDeviceRoute::kGin)
        mc_nccl_flush(ctx, 0, 0);

    mc_nccl_lsa_barrier(ctx, 0);
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    CHECK_EQ(FLAGS_world_size, 2);
    CHECK(FLAGS_rank == 0 || FLAGS_rank == 1);
    CHECK_GT(FLAGS_data_bytes, 0);
    CHECK(!(FLAGS_force_gin && FLAGS_disable_gin));
    const std::string bootstrap_file = bootstrapPath();
    const std::string local_done =
        bootstrap_file + ".done." + std::to_string(FLAGS_rank);
    const std::string peer_done =
        bootstrap_file + ".done." + std::to_string(1 - FLAGS_rank);
    const std::string completion_ack = bootstrap_file + ".done.ack";

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
        std::remove(bootstrap_file.c_str());
        std::remove(local_done.c_str());
        std::remove(peer_done.c_str());
        std::remove(completion_ack.c_str());
        unique_id = transport->createUniqueId();
        CHECK(!unique_id.empty());
        writeUniqueId(bootstrap_file, unique_id);
    } else {
        unique_id = readUniqueId(bootstrap_file);
    }

    mooncake::device::NcclTransportConfig config;
    config.rank = FLAGS_rank;
    config.num_ranks = FLAGS_world_size;
    config.enable_gin = !FLAGS_disable_gin;
    config.gin_context_count = config.enable_gin ? 1 : 0;
    config.lsa_barrier_count = 1;
    config.require_lsa_multimem = FLAGS_require_lsa_multimem;
    CHECK_EQ(transport->initialize(config, unique_id), 0);

    const size_t data_signal_offset =
        alignUp(static_cast<size_t>(FLAGS_data_bytes), kSignalAlignment);
    const size_t ack_signal_offset = data_signal_offset + kSignalAlignment;
    const size_t allocation_bytes = ack_signal_offset + kSignalAlignment;

    void* allocation = nullptr;
    mooncake::device::NcclBufferRegistration registration;
    CHECK_EQ(transport->allocateAndRegisterBuffer(allocation_bytes, &allocation,
                                                  &registration),
             0);
    CHECK_NOTNULL(allocation);
    CHECK(registration.valid());
    char* buffer = static_cast<char*>(allocation);
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

    int* device_result = nullptr;
    checkCuda(cudaMalloc(&device_result, sizeof(int)), "cudaMalloc(result)");
    checkCuda(cudaMemset(device_result, 0xff, sizeof(int)),
              "cudaMemset(result)");

    const auto context = transport->deviceContext(registration);
    CHECK(context.valid());
    if (FLAGS_rank == 0) {
        senderKernel<<<1, 1>>>(context, buffer, FLAGS_data_bytes,
                               data_signal_offset, ack_signal_offset,
                               FLAGS_force_gin, device_result);
    } else {
        receiverKernel<<<1, 1>>>(context, buffer, FLAGS_data_bytes,
                                 data_signal_offset, ack_signal_offset,
                                 device_result);
    }
    checkCuda(cudaGetLastError(), "kernel launch");
    checkCuda(cudaDeviceSynchronize(), "cudaDeviceSynchronize");

    int result = -1;
    checkCuda(cudaMemcpy(&result, device_result, sizeof(result),
                         cudaMemcpyDeviceToHost),
              "cudaMemcpy(result)");
    CHECK_EQ(result, 0) << "Device transfer validation failed";

    // The Mooncake API intentionally leaves world synchronization to the
    // caller. Rendezvous before deregistration so neither rank invalidates a
    // window while the peer can still access it. The acknowledgement lets
    // rank 0 remove the run-scoped marker files without racing rank 1.
    publishMarker(local_done);
    waitForMarker(peer_done);
    if (FLAGS_rank == 1) {
        publishMarker(completion_ack);
    } else {
        waitForMarker(completion_ack);
    }

    const auto properties = transport->properties();
    LOG(INFO) << "PASS rank=" << FLAGS_rank
              << " gin_backend=" << static_cast<int>(properties.gin_backend)
              << " gin_connections=" << properties.gin_connection_count
              << " multimem_supported=" << properties.multimem_supported
              << " multimem_enabled=" << properties.lsa_multimem_enabled
              << " force_gin=" << FLAGS_force_gin
              << " disable_gin=" << FLAGS_disable_gin;

    checkCuda(cudaFree(device_result), "cudaFree(result)");
    CHECK_EQ(transport->deregisterBuffer(&registration), 0);
    CHECK_EQ(transport->freeBuffer(buffer), 0);
    CHECK_EQ(transport->shutdown(), 0);

    if (FLAGS_rank == 0) {
        std::remove(bootstrap_file.c_str());
        std::remove(local_done.c_str());
        std::remove(peer_done.c_str());
        std::remove(completion_ack.c_str());
    }
    return 0;
}
