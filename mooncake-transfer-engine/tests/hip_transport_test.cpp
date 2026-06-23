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

// This test documents *why* HipTransport must restore the caller's active HIP
// device after a transfer.
//
// HipTransport::startAsyncTransfer() calls setDeviceContext(request.source),
// which switches the active device to the GPU that owns the source buffer, and
// enqueues the copy on a stream bound to that device. The engine thread that
// drives transfers is the SAME thread that subsequently launches compute
// kernels — for example, a PyTorch op in a tensor-parallel worker that is also
// pulling KV cache for intra-node prefill/decode disaggregation. If the
// transport returns with the thread still pinned to the source GPU, the next
// kernel launch on that thread targets the wrong device and fails with
// hipErrorInvalidDevice (which surfaces asynchronously, often far from here).
//
// The test pins the calling thread to one GPU, runs a transfer whose source
// buffer lives on a DIFFERENT GPU, and asserts the active device is unchanged
// afterwards. Without the device-restore fix in startAsyncTransfer(), the
// active device is left as the source GPU and the EXPECT_EQ below fails.

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

    // P2PHANDSHAKE keeps the test self-contained (no external etcd/http
    // metadata server). The engine talks only to itself (loopback segment).
    auto engine = std::make_unique<TransferEngine>(false);
    const std::string server_name = "127.0.0.1:17813";
    if (engine->init(P2PHANDSHAKE, server_name, "127.0.0.1", 17813) != 0) {
        GTEST_SKIP() << "TransferEngine init failed in this environment.";
    }

    Transport* transport = engine->installTransport("hip", nullptr);
    if (transport == nullptr) {
        GTEST_SKIP() << "HIP transport unavailable (built without -DUSE_HIP=ON?).";
    }

    // Source and destination buffers both live on kSourceDevice, so the
    // transfer's source GPU differs from the calling thread's device.
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

    // Pin the calling thread to a device DIFFERENT from the source, mimicking
    // the engine thread that will launch the next compute kernel.
    ASSERT_EQ(cudaSetDevice(kCallerDevice), cudaSuccess);

    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kLen;
    entry.source = src;  // on kSourceDevice
    entry.target_id = segment_id;
    entry.target_offset = reinterpret_cast<uint64_t>(dst);
    Status s = engine->submitTransfer(batch_id, {entry});
    ASSERT_TRUE(s.ok());

    // The invariant under test: submitTransfer() must not leave the calling
    // thread pinned to the source GPU. Before the fix this is kSourceDevice.
    int active_after_submit = -1;
    ASSERT_EQ(cudaGetDevice(&active_after_submit), cudaSuccess);
    EXPECT_EQ(active_after_submit, kCallerDevice)
        << "startAsyncTransfer() must restore the caller's active device. "
           "Leaving it on the source GPU corrupts the engine thread's HIP "
           "context, so its next kernel launch fails with hipErrorInvalidDevice.";

    // Drain the transfer; also serves as a basic functional check that the
    // copy still works with the device-restore in place.
    TransferStatus status;
    do {
        ASSERT_TRUE(engine->getTransferStatus(batch_id, 0, status).ok());
    } while (status.s == TransferStatusEnum::WAITING);
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    // The active device must remain restored after completion as well.
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
