// Copyright 2025 Mooncake Authors
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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "cuda_alike.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"  // P2PHANDSHAKE
#include "transport/transport.h"

using namespace mooncake;

// startAsyncTransfer() switches the active device to the source GPU; if it does
// not restore it, the calling thread (which also launches the engine's compute
// kernels) is left on the wrong GPU and the next kernel fails with
// hipErrorInvalidDevice. This pins the caller to one GPU, transfers from a
// buffer on a different GPU, and asserts the active device is unchanged.

namespace {
constexpr size_t kLen = 64 * 1024;

void* allocOnDevice(size_t size, int device) {
    EXPECT_EQ(cudaSetDevice(device), cudaSuccess);
    void* ptr = nullptr;
    EXPECT_EQ(cudaMalloc(&ptr, size), cudaSuccess);
    return ptr;
}
}  // namespace

TEST(HipTransportTest, RestoresActiveDeviceAfterTransfer) {
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    if (device_count < 2) {
        GTEST_SKIP() << "Needs >= 2 GPUs: the transfer source must live on a "
                        "different device than the calling thread's device.";
    }

    const int kCallerDevice = 0;  // device the engine thread runs on
    const int kSourceDevice = 1;  // KV source lives on a different GPU

    // P2PHANDSHAKE: self-contained loopback, no external metadata server.
    auto engine = std::make_unique<TransferEngine>(false);
    const std::string server_name = "127.0.0.1:17813";
    if (engine->init(P2PHANDSHAKE, server_name, "127.0.0.1", 17813) != 0) {
        GTEST_SKIP() << "TransferEngine init failed in this environment.";
    }

    Transport* transport = engine->installTransport("hip", nullptr);
    if (transport == nullptr) {
        GTEST_SKIP() << "HIP transport unavailable (built without -DUSE_HIP=ON?).";
    }

    // Both buffers on kSourceDevice so the source GPU differs from the caller.
    void* src = allocOnDevice(kLen, kSourceDevice);
    void* dst = allocOnDevice(kLen, kSourceDevice);
    ASSERT_EQ(engine->registerLocalMemory(src, kLen,
                                          "cuda:" + std::to_string(kSourceDevice)),
              0);
    ASSERT_EQ(engine->registerLocalMemory(dst, kLen,
                                          "cuda:" + std::to_string(kSourceDevice)),
              0);

    auto segment_id = engine->openSegment(server_name);
    ASSERT_GE(segment_id, 0);

    ASSERT_EQ(cudaSetDevice(kSourceDevice), cudaSuccess);
    ASSERT_EQ(cudaMemset(src, 0xAB, kLen), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);

    // Pin the caller to a different device than the source, as the engine does.
    ASSERT_EQ(cudaSetDevice(kCallerDevice), cudaSuccess);

    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kLen;
    entry.source = src;
    entry.target_id = segment_id;
    entry.target_offset = reinterpret_cast<uint64_t>(dst);
    Status s = engine->submitTransfer(batch_id, {entry});
    ASSERT_TRUE(s.ok());

    // Invariant: the caller's device is unchanged (== kSourceDevice before fix).
    int active_after_submit = -1;
    ASSERT_EQ(cudaGetDevice(&active_after_submit), cudaSuccess);
    EXPECT_EQ(active_after_submit, kCallerDevice)
        << "startAsyncTransfer() must restore the caller's active device. "
           "Leaving it on the source GPU corrupts the engine thread's HIP "
           "context, so its next kernel launch fails with hipErrorInvalidDevice.";

    // Drain the transfer (also a basic functional check).
    TransferStatus status;
    do {
        ASSERT_TRUE(engine->getTransferStatus(batch_id, 0, status).ok());
    } while (status.s == TransferStatusEnum::WAITING);
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    int active_after_wait = -1;
    ASSERT_EQ(cudaGetDevice(&active_after_wait), cudaSuccess);
    EXPECT_EQ(active_after_wait, kCallerDevice);

    engine->freeBatchID(batch_id);
    engine->unregisterLocalMemory(src);
    engine->unregisterLocalMemory(dst);
    (void)cudaSetDevice(kSourceDevice);
    (void)cudaFree(src);
    (void)cudaFree(dst);
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
