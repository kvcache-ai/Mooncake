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

// EFA GPU (CUDA device memory) loopback test.
//
// This is the GPU-memory counterpart of efa_transport_test.cpp, which only
// exercises loopback on host (numa) buffers.  The distinction matters: the
// libfabric EFA provider's SHM intra-node path does a host memcpy into
// FI_HMEM_CUDA device buffers and segfaults on the first same-host transfer
// (ofiwg/libfabric#12328).  Host-memory loopback never hits that bug, so it
// went unnoticed by the existing tests.
//
// Mooncake's fix (EfaContext::tryLoopbackCopy) detects same-process
// self-loopback and satisfies it with a GPU-aware cudaMemcpy instead of
// routing it over EFA.  This test reproduces the crash WITHOUT the fix and
// verifies correct, byte-accurate data movement WITH it — for both WRITE and
// READ opcodes (the two copy in opposite directions).
//
// Requires EFA hardware (fi_info -p efa) AND at least one CUDA GPU.  Self-
// skips cleanly when either is absent.

#include <cuda.h>
#include <cuda_runtime.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "transport/efa_transport/efa_transport.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

namespace {
bool cudaAvailable() {
    int n = 0;
    return cudaGetDeviceCount(&n) == cudaSuccess && n > 0;
}
}  // namespace

class EFAGpuLoopbackTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("EFAGpuLoopbackTest");
        FLAGS_logtostderr = 1;

        const char *env = std::getenv("MC_METADATA_SERVER");
        metadata_server_ = env ? env : "P2PHANDSHAKE";

        env = std::getenv("MC_LOCAL_SERVER_NAME");
        local_server_name_ = env ? env : "127.0.0.1:12345";
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    struct EngineSetup {
        std::unique_ptr<TransferEngine> engine;
        Transport *xport = nullptr;
        void *dev_buf = nullptr;  // CUDA device buffer
        size_t buffer_size = 0;
        SegmentID segment_id = 0;
        bool registered = false;
        bool ok = false;
    };

    // Create engine, install EFA transport, allocate + register a CUDA
    // device buffer, and open our own segment for loopback.
    EngineSetup createEngine(int device_id = 0,
                             size_t buffer_size = 1ull << 26 /* 64 MB */) {
        EngineSetup s;
        s.buffer_size = buffer_size;

        if (cudaSetDevice(device_id) != cudaSuccess) {
            LOG(WARNING) << "cudaSetDevice(" << device_id << ") failed";
            return s;
        }

        s.engine = std::make_unique<TransferEngine>(false);
        s.engine->getLocalTopology()->discover({});
        auto hp = parseHostNameWithPort(local_server_name_);
        int rc = s.engine->init(metadata_server_, local_server_name_,
                                hp.first.c_str(), hp.second);
        EXPECT_EQ(rc, 0) << "engine->init failed";
        if (rc != 0) return s;

        s.xport = s.engine->installTransport("efa", nullptr);
        EXPECT_NE(s.xport, nullptr) << "installTransport(\"efa\") failed";
        if (!s.xport) return s;

        cudaError_t cerr = cudaMalloc(&s.dev_buf, buffer_size);
        EXPECT_EQ(cerr, cudaSuccess)
            << "cudaMalloc failed: " << cudaGetErrorString(cerr);
        if (cerr != cudaSuccess) return s;

        // Register as GPU memory so the EFA transport tags the MR with
        // FI_HMEM_CUDA (the registration path under test).
        std::string location = "cuda:" + std::to_string(device_id);
        rc = s.engine->registerLocalMemory(s.dev_buf, buffer_size, location);
        EXPECT_EQ(rc, 0) << "registerLocalMemory(" << location << ") failed";
        if (rc != 0) return s;
        s.registered = true;

        auto actual_addr = s.engine->getLocalIpAndPort();
        s.segment_id = s.engine->openSegment(actual_addr);
        s.ok = true;
        return s;
    }

    void destroyEngine(EngineSetup &s) {
        if (s.engine && s.dev_buf && s.registered) {
            s.engine->unregisterLocalMemory(s.dev_buf);
            s.registered = false;
        }
        if (s.dev_buf) {
            cudaFree(s.dev_buf);
            s.dev_buf = nullptr;
        }
    }

    class EngineCleanup {
       public:
        EngineCleanup(EFAGpuLoopbackTest *test, EngineSetup &setup)
            : test_(test), setup_(setup) {}
        ~EngineCleanup() { test_->destroyEngine(setup_); }

       private:
        EFAGpuLoopbackTest *test_;
        EngineSetup &setup_;
    };

    bool submitAndWait(TransferEngine *engine, SegmentID segment_id,
                       void *source, uint64_t target_offset, size_t length,
                       TransferRequest::OpCode opcode) {
        auto batch_id = engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = opcode;
        entry.length = length;
        entry.source = (uint8_t *)source;
        entry.target_id = segment_id;
        entry.target_offset = target_offset;

        Status s = engine->submitTransfer(batch_id, {entry});
        if (!s.ok()) {
            LOG(ERROR) << "submitTransfer failed: " << s.ToString();
            engine->freeBatchID(batch_id);
            return false;
        }

        TransferStatus status;
        const int kMaxPollIterations = 2000000;
        for (int i = 0; i < kMaxPollIterations; ++i) {
            s = engine->getTransferStatus(batch_id, 0, status);
            if (!s.ok()) {
                engine->freeBatchID(batch_id);
                return false;
            }
            if (status.s == TransferStatusEnum::COMPLETED) {
                engine->freeBatchID(batch_id);
                return true;
            }
            if (status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "Transfer FAILED";
                engine->freeBatchID(batch_id);
                return false;
            }
        }
        LOG(ERROR) << "Transfer timed out";
        engine->freeBatchID(batch_id);
        return false;
    }

    std::string metadata_server_;
    std::string local_server_name_;
};

// A loopback copy submitted from a fresh thread must not activate GPU 0 or
// leave a CUDA context current on the caller's thread.
TEST_F(EFAGpuLoopbackTest, LoopbackPreservesEmptyCallerContext) {
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    if (device_count < 2) GTEST_SKIP() << "At least two CUDA GPUs required";

    CUdevice device_zero;
    ASSERT_EQ(cuDeviceGet(&device_zero, 0), CUDA_SUCCESS);

    auto setup = createEngine(1);
    EngineCleanup cleanup(this, setup);
    if (!setup.ok) GTEST_SKIP() << "EFA/CUDA setup unavailable";

    unsigned int flags = 0;
    int device_zero_active_before = 0;
    ASSERT_EQ(cuDevicePrimaryCtxGetState(device_zero, &flags,
                                         &device_zero_active_before),
              CUDA_SUCCESS);

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 4096;
    uint8_t *dev = (uint8_t *)setup.dev_buf;
    const uint64_t destination = setup.buffer_size / 2;
    ASSERT_EQ(cudaMemset(dev, 0xAB, kDataLength), cudaSuccess);
    ASSERT_EQ(cudaMemset(dev + destination, 0, kDataLength), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);

    bool transfer_ok = false;
    CUresult before_result = CUDA_ERROR_UNKNOWN;
    CUresult after_result = CUDA_ERROR_UNKNOWN;
    CUcontext before_context = nullptr;
    CUcontext after_context = nullptr;
    std::thread submitter([&] {
        before_result = cuCtxGetCurrent(&before_context);
        transfer_ok = submitAndWait(setup.engine.get(), setup.segment_id,
                                    setup.dev_buf, remote_base + destination,
                                    kDataLength, TransferRequest::WRITE);
        after_result = cuCtxGetCurrent(&after_context);
    });
    submitter.join();

    ASSERT_EQ(before_result, CUDA_SUCCESS);
    ASSERT_EQ(before_context, nullptr);
    ASSERT_TRUE(transfer_ok);
    ASSERT_EQ(after_result, CUDA_SUCCESS);
    EXPECT_EQ(after_context, before_context)
        << "loopback copy changed the caller's CUDA context";

    int device_zero_active_after = 0;
    ASSERT_EQ(cuDevicePrimaryCtxGetState(device_zero, &flags,
                                         &device_zero_active_after),
              CUDA_SUCCESS);
    if (!device_zero_active_before) {
        EXPECT_EQ(device_zero_active_after, 0)
            << "loopback copy activated CUDA device 0";
    }

    std::vector<uint8_t> readback(kDataLength);
    ASSERT_EQ(cudaMemcpy(readback.data(), dev + destination, kDataLength,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    std::vector<uint8_t> expected(kDataLength, 0xAB);
    EXPECT_EQ(0, memcmp(readback.data(), expected.data(), kDataLength))
        << "loopback copy did not produce the expected bytes";
}

// A loopback copy to another GPU must preserve an existing caller device so
// subsequent CUDA work remains on the device selected by the application.
TEST_F(EFAGpuLoopbackTest, LoopbackPreservesCallerDevice) {
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    if (device_count < 2) GTEST_SKIP() << "At least two CUDA GPUs required";

    auto setup = createEngine(1);
    EngineCleanup cleanup(this, setup);
    if (!setup.ok) GTEST_SKIP() << "EFA/CUDA setup unavailable";

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 4096;
    ASSERT_EQ(cudaMemset(setup.dev_buf, 0xAB, kDataLength), cudaSuccess);

    bool transfer_ok = false;
    int device_before = -1;
    int device_after = -1;
    int allocation_device = -1;
    cudaError_t set_result = cudaErrorUnknown;
    cudaError_t before_result = cudaErrorUnknown;
    cudaError_t after_result = cudaErrorUnknown;
    cudaError_t allocation_result = cudaErrorUnknown;
    cudaError_t attributes_result = cudaErrorUnknown;
    cudaError_t free_result = cudaErrorUnknown;
    std::thread submitter([&] {
        set_result = cudaSetDevice(0);
        before_result = cudaGetDevice(&device_before);
        transfer_ok =
            submitAndWait(setup.engine.get(), setup.segment_id, setup.dev_buf,
                          remote_base + (setup.buffer_size / 2), kDataLength,
                          TransferRequest::WRITE);
        after_result = cudaGetDevice(&device_after);

        void *probe = nullptr;
        allocation_result = cudaMalloc(&probe, 1 << 20);
        if (allocation_result == cudaSuccess) {
            cudaPointerAttributes attributes;
            attributes_result = cudaPointerGetAttributes(&attributes, probe);
            if (attributes_result == cudaSuccess) {
                allocation_device = attributes.device;
            }
            free_result = cudaFree(probe);
        }
    });
    submitter.join();

    ASSERT_EQ(set_result, cudaSuccess);
    ASSERT_EQ(before_result, cudaSuccess);
    ASSERT_TRUE(transfer_ok);
    ASSERT_EQ(after_result, cudaSuccess);
    EXPECT_EQ(device_after, device_before)
        << "loopback copy changed the caller's CUDA device";
    ASSERT_EQ(allocation_result, cudaSuccess);
    ASSERT_EQ(attributes_result, cudaSuccess);
    EXPECT_EQ(allocation_device, device_before)
        << "subsequent allocation landed on the wrong GPU";
    EXPECT_EQ(free_result, cudaSuccess);
}

// Test 1: GPU loopback WRITE must not crash.
//
// Without the fix this segfaults inside the EFA provider's SHM path
// (host memcpy into a device pointer).  With the fix it completes via a
// local cudaMemcpy.
TEST_F(EFAGpuLoopbackTest, GpuLoopbackWrite) {
    if (!cudaAvailable()) GTEST_SKIP() << "No CUDA GPU present";
    auto setup = createEngine();
    if (!setup.ok) GTEST_SKIP() << "EFA/CUDA setup unavailable";

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 4096;
    ASSERT_EQ(cudaMemset(setup.dev_buf, 0xAB, kDataLength), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);

    // Write src(=dev_buf) -> remote(=dev_buf + offset), both on the GPU.
    bool ok = submitAndWait(setup.engine.get(), setup.segment_id, setup.dev_buf,
                            remote_base + (setup.buffer_size / 2), kDataLength,
                            TransferRequest::WRITE);
    EXPECT_TRUE(ok) << "GPU loopback write should succeed";

    destroyEngine(setup);
}

// Test 2: GPU loopback WRITE then READ, with byte-accurate verification.
//
// The first half holds the source pattern; we WRITE it into the second
// half, scribble the first half, then READ the second half back into the
// first half and compare.  This exercises BOTH copy directions of
// tryLoopbackCopy (WRITE: src->dst, READ: dst->src) and proves the data is
// actually moved correctly, not merely "did not crash".
TEST_F(EFAGpuLoopbackTest, GpuLoopbackWriteThenRead) {
    if (!cudaAvailable()) GTEST_SKIP() << "No CUDA GPU present";
    auto setup = createEngine();
    if (!setup.ok) GTEST_SKIP() << "EFA/CUDA setup unavailable";

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 4ull << 20;  // 4 MB
    ASSERT_LE(2 * kDataLength, setup.buffer_size);

    uint8_t *dev = (uint8_t *)setup.dev_buf;
    const uint64_t second_half = setup.buffer_size / 2;

    // Build a known pattern on the host and copy it into the first half.
    std::vector<uint8_t> pattern(kDataLength);
    for (size_t i = 0; i < kDataLength; ++i)
        pattern[i] = (uint8_t)(i * 131 + 7);
    ASSERT_EQ(
        cudaMemcpy(dev, pattern.data(), kDataLength, cudaMemcpyHostToDevice),
        cudaSuccess);

    // WRITE: first half -> second half (loopback).
    ASSERT_TRUE(submitAndWait(setup.engine.get(), setup.segment_id, dev,
                              remote_base + second_half, kDataLength,
                              TransferRequest::WRITE))
        << "WRITE should succeed";

    // Corrupt the first half so the READ-back has to actually move bytes.
    ASSERT_EQ(cudaMemset(dev, 0x00, kDataLength), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);

    // READ: second half -> first half (loopback, opposite direction).
    ASSERT_TRUE(submitAndWait(setup.engine.get(), setup.segment_id, dev,
                              remote_base + second_half, kDataLength,
                              TransferRequest::READ))
        << "READ should succeed";

    // Verify the first half now matches the original pattern.
    std::vector<uint8_t> readback(kDataLength, 0xFF);
    ASSERT_EQ(
        cudaMemcpy(readback.data(), dev, kDataLength, cudaMemcpyDeviceToHost),
        cudaSuccess);
    EXPECT_EQ(0, memcmp(readback.data(), pattern.data(), kDataLength))
        << "READ-back GPU data should match the written pattern";

    destroyEngine(setup);
}

// Test 3: batch of GPU loopback writes (multiple slices), to exercise the
// per-slice loopback short-circuit inside the grouping loop.
TEST_F(EFAGpuLoopbackTest, GpuLoopbackMultiWrite) {
    if (!cudaAvailable()) GTEST_SKIP() << "No CUDA GPU present";
    auto setup = createEngine();
    if (!setup.ok) GTEST_SKIP() << "EFA/CUDA setup unavailable";

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kSliceLen = 65536;
    const int kBatchSize = 16;
    const uint64_t dst_region = setup.buffer_size / 2;
    ASSERT_LE(dst_region + (uint64_t)kBatchSize * kSliceLen, setup.buffer_size);

    ASSERT_EQ(cudaMemset(setup.dev_buf, 0x5A, kBatchSize * kSliceLen),
              cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);

    auto batch_id = setup.engine->allocateBatchID(kBatchSize);
    std::vector<TransferRequest> requests;
    for (int i = 0; i < kBatchSize; ++i) {
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kSliceLen;
        entry.source = (uint8_t *)setup.dev_buf + i * kSliceLen;
        entry.target_id = setup.segment_id;
        entry.target_offset = remote_base + dst_region + i * kSliceLen;
        requests.push_back(entry);
    }

    Status s = setup.engine->submitTransfer(batch_id, requests);
    ASSERT_TRUE(s.ok()) << "submitTransfer failed: " << s.ToString();

    for (int task_id = 0; task_id < kBatchSize; ++task_id) {
        TransferStatus status;
        const int kMaxPollIterations = 2000000;
        for (int i = 0; i < kMaxPollIterations; ++i) {
            s = setup.engine->getTransferStatus(batch_id, task_id, status);
            ASSERT_TRUE(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) break;
            ASSERT_NE(status.s, TransferStatusEnum::FAILED)
                << "task " << task_id << " failed";
        }
        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED)
            << "task " << task_id << " did not complete";
    }

    s = setup.engine->freeBatchID(batch_id);
    ASSERT_TRUE(s.ok());

    destroyEngine(setup);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
