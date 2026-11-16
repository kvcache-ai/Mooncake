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

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "config.h"
#include "cuda_alike.h"
#include "error.h"
#include "topology.h"
#include "transfer_engine.h"
#include "transport/rdma_transport/rdma_transport.h"

namespace mooncake {
namespace {

#if defined(USE_CUDA) || defined(USE_HIP)
struct GpuBuffer {
    void *ptr = nullptr;
    int gpu;
    explicit GpuBuffer(int gpu_id, size_t size) : gpu(gpu_id) {
        if (cudaSetDevice(gpu) == cudaSuccess &&
            cudaMalloc(&ptr, size) != cudaSuccess)
            ptr = nullptr;
    }
    ~GpuBuffer() {
        if (ptr) {
            EXPECT_EQ(cudaSetDevice(gpu), cudaSuccess);
            EXPECT_EQ(cudaFree(ptr), cudaSuccess);
        }
    }
};

class RdmaLocalNicGpuTest
    : public ::testing::TestWithParam<std::tuple<bool, int, bool>> {};

TEST_P(RdmaLocalNicGpuTest, TwoEnginesRegisterAndTransferAcrossMrBoundaries) {
    if (!std::getenv("MC_RUN_RDMA_GPU_TESTS"))
        GTEST_SKIP()
            << "Set MC_RUN_RDMA_GPU_TESTS=1 on an allocated GPU/RDMA node";
    int gpu_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&gpu_count), cudaSuccess);
    ASSERT_GE(gpu_count, 1);
    auto [local, parallel, chunked] = GetParam();
    auto &config = globalConfig();
    struct RestoreConfig {
        GlobalConfig &config;
        NicSelection selection;
        uint64_t max_mr_size;
        int parallel;
        bool affinity;
        ~RestoreConfig() {
            config.rdma_nic_selection = selection;
            config.max_mr_size = max_mr_size;
            config.parallel_reg_mr = parallel;
            config.enable_dest_device_affinity = affinity;
        }
    } restore{config, config.rdma_nic_selection, config.max_mr_size,
              config.parallel_reg_mr, config.enable_dest_device_affinity};
    config.rdma_nic_selection = local ? NicSelection::LOCAL : NicSelection::ALL;
    config.parallel_reg_mr = parallel;
    // Ionic cannot hairpin between different ports on the same host. Keep
    // loopback on the same port even when ALL-mode retries pick another rail.
    config.enable_dest_device_affinity = true;
    constexpr size_t cap = 64 * 1024;
    constexpr size_t size = 3 * cap + 4096;
    config.max_mr_size = chunked ? cap : size;

    // Buffers outlive both engines, including on an assertion failure.
    std::vector<std::unique_ptr<GpuBuffer>> buffers;
    for (int gpu = 0; gpu < std::min(gpu_count, 2); ++gpu) {
        buffers.push_back(std::make_unique<GpuBuffer>(gpu, size));
        ASSERT_NE(buffers.back()->ptr, nullptr);
    }
    std::vector<unsigned char> host(4096, 0x7a);
    std::vector<std::unique_ptr<TransferEngine>> engines;
    for (int i = 0; i < 2; ++i) {
        auto engine = std::make_unique<TransferEngine>(true);
        ASSERT_EQ(engine->init(P2PHANDSHAKE, "127.0.0.1"), 0);
        ASSERT_NE(engine->getTransport("rdma"), nullptr);
        engines.push_back(std::move(engine));
    }
    for (auto &engine : engines) {
        // Scope host registration to RDMA: the existing HIP transport skips
        // host registration but returns NOT_REGISTERED on host unregistration.
        ASSERT_EQ(static_cast<RdmaTransport *>(engine->getTransport("rdma"))
                      ->registerLocalMemory(host.data(), host.size(), "cpu:0",
                                            true, true),
                  0);
        for (const auto &buffer : buffers) {
            ASSERT_EQ(cudaSetDevice(buffer->gpu), cudaSuccess);
            ASSERT_EQ(engine->registerLocalMemory(buffer->ptr, size, "*"), 0);
        }
        auto desc = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        ASSERT_NE(desc, nullptr);
        std::vector<std::string> device_names;
        for (const auto &device : desc->devices)
            device_names.push_back(device.name);
        auto preferred =
            buildPreferredNicIndexMap(desc->topology.getMatrix(), device_names);
        for (const auto &buffer : buffers) {
            size_t chunks = 0;
            const auto location = GPU_PREFIX + std::to_string(buffer->gpu);
            std::vector<size_t> expected_nics;
            if (local) {
                ASSERT_TRUE(preferred.count(location));
                expected_nics = preferred.at(location);
            } else {
                for (size_t n = 0; n < desc->devices.size(); ++n)
                    expected_nics.push_back(n);
            }
            for (const auto &mr : desc->buffers) {
#ifdef ENABLE_MULTI_PROTOCOL
                if (!mr.protocol.empty() && mr.protocol != "rdma") continue;
#endif
                if (mr.addr < reinterpret_cast<uint64_t>(buffer->ptr) ||
                    mr.addr - reinterpret_cast<uint64_t>(buffer->ptr) >= size)
                    continue;
                ++chunks;
                ASSERT_EQ(mr.rkey.size(), desc->devices.size());
                ASSERT_EQ(mr.lkey.size(), desc->devices.size());
                for (size_t n = 0; n < desc->devices.size(); ++n) {
                    bool expected =
                        std::find(expected_nics.begin(), expected_nics.end(),
                                  n) != expected_nics.end();
                    EXPECT_EQ(mr.rkey[n] != 0, expected);
                    EXPECT_EQ(mr.lkey[n] != 0, expected);
                }
            }
            EXPECT_EQ(chunks, chunked ? 4u : 1u);
            std::cout << "GPU_MR_CHECK local=" << local
                      << " parallel=" << parallel << " gpu=" << buffer->gpu
                      << " chunks=" << chunks
                      << " registered_nics=" << expected_nics.size() << "/"
                      << desc->devices.size() << std::endl;
        }
        bool found_host = false;
        for (const auto &mr : desc->buffers) {
            if (mr.addr != reinterpret_cast<uint64_t>(host.data()) ||
                mr.rkey.empty())
                continue;
            found_host = true;
            EXPECT_EQ(static_cast<size_t>(
                          std::count_if(mr.rkey.begin(), mr.rkey.end(),
                                        [](auto key) { return key != 0; })),
                      desc->devices.size());
        }
        EXPECT_TRUE(found_host);
    }

    // Submit directly to RdmaTransport so the HIP/CUDA loopback transport
    // cannot turn this into a memcpy-only test.
    for (auto &engine : engines) {
        auto *rdma = static_cast<RdmaTransport *>(engine->getTransport("rdma"));
        for (const auto &buffer : buffers) {
            ASSERT_EQ(cudaSetDevice(buffer->gpu), cudaSuccess);
            for (auto opcode : {Transport::TransferRequest::READ,
                                Transport::TransferRequest::WRITE}) {
                for (int retry : {0, 1, 5}) {
                    std::vector<unsigned char> initial(size, 0xcc);
                    constexpr size_t src = cap - 1024;
                    constexpr size_t dst = 2 * cap - 1024;
                    constexpr size_t bytes = 4096;
                    for (size_t j = 0; j < bytes; ++j)
                        initial[src + j] = j % 251;
                    auto expected = initial;
                    std::memcpy(expected.data() + dst, initial.data() + src,
                                bytes);
                    ASSERT_EQ(cudaMemcpy(buffer->ptr, initial.data(), size,
                                         cudaMemcpyHostToDevice),
                              cudaSuccess);
                    auto batch = rdma->allocateBatchID(1);
                    Transport::TransferRequest request;
                    request.opcode = opcode;
                    request.source =
                        static_cast<char *>(buffer->ptr) +
                        (opcode == Transport::TransferRequest::READ ? dst
                                                                    : src);
                    request.target_id = LOCAL_SEGMENT_ID;
                    request.target_offset =
                        reinterpret_cast<uint64_t>(buffer->ptr) +
                        (opcode == Transport::TransferRequest::READ ? src
                                                                    : dst);
                    request.length = bytes;
                    request.advise_retry_cnt = retry;
                    ASSERT_TRUE(rdma->submitTransfer(batch, {request}).ok());
                    Transport::TransferStatus status;
                    auto deadline = std::chrono::steady_clock::now() +
                                    std::chrono::seconds(15);
                    while (true) {
                        ASSERT_TRUE(
                            rdma->getTransferStatus(batch, 0, status).ok());
                        if (status.s ==
                                Transport::TransferStatusEnum::COMPLETED ||
                            status.s == Transport::TransferStatusEnum::FAILED)
                            break;
                        if (std::chrono::steady_clock::now() > deadline)
                            LOG(FATAL) << "Timed out with RDMA in flight; "
                                          "exiting test process";
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(1));
                    }
                    ASSERT_EQ(status.s,
                              Transport::TransferStatusEnum::COMPLETED);
                    ASSERT_TRUE(rdma->freeBatchID(batch).ok());
                    std::vector<unsigned char> observed(size);
                    ASSERT_EQ(cudaMemcpy(observed.data(), buffer->ptr, size,
                                         cudaMemcpyDeviceToHost),
                              cudaSuccess);
                    ASSERT_EQ(observed, expected);
                }
            }
            ASSERT_EQ(engine->unregisterLocalMemory(buffer->ptr), 0);
            if (local) {
                // A GPU location without a preferred HCA must fail before
                // creating any MR, and a later valid registration must still
                // succeed.
                EXPECT_EQ(
                    rdma->registerLocalMemory(buffer->ptr, size,
                                              GPU_PREFIX + "999", true, true),
                    ERR_DEVICE_NOT_FOUND);
            }
            // True same-address re-registration checks cleanup of every chunk.
            ASSERT_EQ(engine->registerLocalMemory(buffer->ptr, size, "*"), 0);
            ASSERT_EQ(engine->unregisterLocalMemory(buffer->ptr), 0);
        }
        ASSERT_EQ(rdma->unregisterLocalMemory(host.data()), 0);
    }
}

INSTANTIATE_TEST_SUITE_P(Modes, RdmaLocalNicGpuTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Values(0, 1),
                                            ::testing::Bool()));
#endif
}  // namespace
}  // namespace mooncake
