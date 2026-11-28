// Copyright 2026 KVCache.AI
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

// Drives XpuTransport's staging hop against the real oneAPI SYCL backend.
// USE_XPU is a direct-link build, so there is no mock adapter: the transport
// classifies and copies XPU memory through the active XpuPlatform
// (Platform::getLoader()). The backend falls back to the OpenCL CPU runtime
// when no Intel GPU is present, and the suite skips when no SYCL device is
// visible at all.
//
// The cases mirror the requests ProxyManager actually issues: a local stage
// whose `source` is a device pointer at an interior offset and whose
// `target_offset` is the host staging buffer, and the delegated remote stage
// where the device side is `target_offset` instead. The interior offsets are
// the real detectors of the "device pointer misclassified as host memory"
// failure mode, since the backend must resolve base + offset against its
// allocation registry.

#include "tent/transport/xpu/xpu_transport.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/platform.h"

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kChunk = 64ul << 10;  // 64 KiB
constexpr size_t kBufSize = 3 * kChunk;

class XpuTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Force the singleton platform loader to exist; in a USE_XPU build this
        // is an XpuPlatform. Probe for a visible device and skip otherwise.
        auto conf = std::make_shared<Config>();
        loader_ = &Platform::getLoader(conf);
        MemoryOptions options;
        options.location = "xpu:0";
        void *probe = nullptr;
        if (!loader_->allocate(&probe, kChunk, options).ok() || !probe) {
            GTEST_SKIP() << "no SYCL device available; skipping XPU transport "
                            "test";
        }
        loader_->free(probe, kChunk);

        std::string segment = "local";
        ASSERT_TRUE(
            transport_.install(segment, nullptr, nullptr, nullptr).ok());
    }

    void TearDown() override { transport_.uninstall(); }

    // Aborts the test immediately if device allocation fails, so callers never
    // operate on a null pointer. ASSERT lives here (void return) and callers
    // wrap the call in ASSERT_NO_FATAL_FAILURE to propagate the abort.
    void allocDevice(uint8_t **out, size_t size) {
        MemoryOptions options;
        options.location = "xpu:0";
        *out = nullptr;
        void *p = nullptr;
        ASSERT_TRUE(loader_->allocate(&p, size, options).ok());
        ASSERT_NE(p, nullptr);
        *out = static_cast<uint8_t *>(p);
    }

    // Polls task `task_id` until it leaves PENDING (copies are enqueued
    // asynchronously and complete on a later status poll).
    TransferStatus wait(Transport::SubBatchRef batch, int task_id) {
        TransferStatus status{};
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        do {
            EXPECT_TRUE(
                transport_.getTransferStatus(batch, task_id, status).ok());
            if (status.s != TransferStatusEnum::PENDING) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        return status;
    }

    // Runs one request through the transport and returns its final status.
    TransferStatus run(const Request &request) {
        Transport::SubBatchRef batch = nullptr;
        EXPECT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
        EXPECT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
        TransferStatus status = wait(batch, 0);
        EXPECT_TRUE(transport_.freeSubBatch(batch).ok());
        return status;
    }

    static Request makeRequest(Request::OpCode op, void *source,
                               uint64_t target, size_t length) {
        Request r;
        r.opcode = op;
        r.source = source;
        r.length = length;
        r.target_id = LOCAL_SEGMENT_ID;
        r.target_offset = target;
        return r;
    }

    Platform *loader_ = nullptr;
    XpuTransport transport_;
};

// The local WRITE stage of chunk #1: source is an interior device address.
TEST_F(XpuTransportTest, LocalStageCopiesFromInteriorDeviceOffset) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    std::vector<uint8_t> seed(kBufSize);
    for (size_t i = 0; i < kBufSize; ++i) seed[i] = (uint8_t)(i % 251);
    ASSERT_TRUE(loader_->copy(dev, seed.data(), kBufSize).ok());
    std::vector<uint8_t> staging(kChunk, 0);

    // Chunk #1: device + 64 KiB -> host staging buffer (D2H).
    auto status = run(makeRequest(Request::WRITE, dev + kChunk,
                                  (uint64_t)staging.data(), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kChunk);
    EXPECT_EQ(0, std::memcmp(staging.data(), seed.data() + kChunk, kChunk));

    loader_->free(dev, kBufSize);
}

// The mirrored remote stage: the device side is `target_offset`, at an offset.
TEST_F(XpuTransportTest, RemoteStageCopiesToInteriorDeviceOffset) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    std::vector<uint8_t> zero(kBufSize, 0);
    ASSERT_TRUE(loader_->copy(dev, zero.data(), kBufSize).ok());
    std::vector<uint8_t> staging(kChunk);
    for (size_t i = 0; i < kChunk; ++i) staging[i] = (uint8_t)(i % 197);

    // WRITE with a device target: host staging -> device + 64 KiB (H2D).
    auto status = run(makeRequest(Request::WRITE, staging.data(),
                                  (uint64_t)(dev + kChunk), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);

    std::vector<uint8_t> readback(kChunk, 0);
    ASSERT_TRUE(loader_->copy(readback.data(), dev + kChunk, kChunk).ok());
    EXPECT_EQ(0, std::memcmp(readback.data(), staging.data(), kChunk));

    loader_->free(dev, kBufSize);
}

// A READ stage moves host staging -> device (H2D).
TEST_F(XpuTransportTest, ReadStageCopiesHostToDevice) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kBufSize));
    std::vector<uint8_t> staging(kChunk, 0xAB);

    auto status = run(makeRequest(Request::READ, dev + 2 * kChunk,
                                  (uint64_t)staging.data(), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);

    std::vector<uint8_t> readback(kChunk, 0);
    ASSERT_TRUE(loader_->copy(readback.data(), dev + 2 * kChunk, kChunk).ok());
    EXPECT_EQ(0, std::memcmp(readback.data(), staging.data(), kChunk));

    loader_->free(dev, kBufSize);
}

// Neither side is device memory: a routing bug or an unclassified pointer. The
// transport must fail rather than let Platform::copy memcpy between two host
// buffers as if it were a staging hop.
TEST_F(XpuTransportTest, FailsWhenNeitherSideIsDeviceMemory) {
    std::vector<uint8_t> host_a(4096, 1), host_b(4096, 2);
    auto status = run(makeRequest(Request::WRITE, host_a.data(),
                                  (uint64_t)host_b.data(), host_a.size()));
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    // The destination is untouched -- no silent partial copy.
    EXPECT_EQ(host_b[0], 2);
}

// Both sides device: a process-local VRAM->VRAM request is executed as one
// device copy (same XPU here) rather than staged.
TEST_F(XpuTransportTest, CopiesDeviceToDevice) {
    uint8_t *a = nullptr;
    uint8_t *b = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&a, kBufSize));
    ASSERT_NO_FATAL_FAILURE(allocDevice(&b, kBufSize));
    std::vector<uint8_t> seed(kBufSize), zero(kBufSize, 0);
    for (size_t i = 0; i < kBufSize; ++i) seed[i] = (uint8_t)(i % 239);
    ASSERT_TRUE(loader_->copy(a, seed.data(), kBufSize).ok());
    ASSERT_TRUE(loader_->copy(b, zero.data(), kBufSize).ok());

    // WRITE a + 64 KiB -> b + 128 KiB: both sides interior device addresses.
    auto status = run(makeRequest(Request::WRITE, a + kChunk,
                                  (uint64_t)(b + 2 * kChunk), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kChunk);

    std::vector<uint8_t> readback(kBufSize, 0);
    ASSERT_TRUE(loader_->copy(readback.data(), b, kBufSize).ok());
    EXPECT_EQ(0, std::memcmp(readback.data() + 2 * kChunk, seed.data() + kChunk,
                             kChunk));
    // Outside the copied chunk b is untouched.
    EXPECT_EQ(0, std::memcmp(readback.data(), zero.data(), 2 * kChunk));

    loader_->free(a, kBufSize);
    loader_->free(b, kBufSize);
}

// One submit carrying many requests in both directions, plus one that must
// fail: the copies are enqueued together, each task completes with its own
// bytes, and the failed one does not hold the others back.
TEST_F(XpuTransportTest, BatchedSubmitCompletesEveryTask) {
    constexpr size_t kTasks = 16;
    constexpr size_t kLen = 4096 + 128;  // not a power of two
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kTasks * kLen));
    std::vector<uint8_t> seed(kTasks * kLen), host(kTasks * kLen, 0);
    for (size_t i = 0; i < seed.size(); ++i) seed[i] = (uint8_t)(i % 233);
    std::vector<uint8_t> zero(kTasks * kLen, 0);
    ASSERT_TRUE(loader_->copy(dev, zero.data(), zero.size()).ok());
    // Even chunks start on the host and are pushed to the device (H2D);
    // odd chunks start on the device and are pulled to the host (D2H).
    for (size_t t = 1; t < kTasks; t += 2) {
        ASSERT_TRUE(
            loader_->copy(dev + t * kLen, seed.data() + t * kLen, kLen).ok());
    }
    std::vector<uint8_t> src_host(seed);

    std::vector<Request> requests;
    for (size_t t = 0; t < kTasks; ++t) {
        if (t % 2 == 0)
            requests.push_back(
                makeRequest(Request::READ, dev + t * kLen,
                            (uint64_t)(src_host.data() + t * kLen), kLen));
        else
            requests.push_back(makeRequest(Request::WRITE, dev + t * kLen,
                                           (uint64_t)(host.data() + t * kLen),
                                           kLen));
    }
    std::vector<uint8_t> host_a(64, 1), host_b(64, 2);
    requests.push_back(makeRequest(Request::WRITE, host_a.data(),
                                   (uint64_t)host_b.data(), host_a.size()));

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, requests.size()).ok());
    ASSERT_TRUE(transport_.submitTransferTasks(batch, requests).ok());
    ASSERT_EQ(batch->size(), requests.size());
    for (size_t t = 0; t < kTasks; ++t) {
        auto status = wait(batch, (int)t);
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED) << "task " << t;
        EXPECT_EQ(status.transferred_bytes, kLen) << "task " << t;
    }
    EXPECT_EQ(wait(batch, (int)kTasks).s, TransferStatusEnum::FAILED);
    EXPECT_EQ(host_b[0], 2);
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());

    std::vector<uint8_t> readback(kTasks * kLen, 0);
    ASSERT_TRUE(loader_->copy(readback.data(), dev, readback.size()).ok());
    for (size_t t = 0; t < kTasks; ++t) {
        const uint8_t *got =
            (t % 2 == 0 ? readback.data() : host.data()) + t * kLen;
        EXPECT_EQ(0, std::memcmp(got, seed.data() + t * kLen, kLen))
            << "task " << t;
    }

    loader_->free(dev, kTasks * kLen);
}

// Freeing a sub-batch whose copies were never polled waits for them, so the
// caller may release the buffers right after freeSubBatch().
TEST_F(XpuTransportTest, FreeSubBatchWaitsForPendingCopies) {
    constexpr size_t kLarge = 64ul << 20;  // 64 MiB
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kLarge));
    std::vector<uint8_t> seed(kLarge);
    for (size_t i = 0; i < kLarge; ++i) seed[i] = (uint8_t)(i % 241);
    ASSERT_TRUE(loader_->copy(dev, seed.data(), kLarge).ok());

    std::vector<uint8_t> host(kLarge, 0);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    ASSERT_TRUE(transport_
                    .submitTransferTasks(
                        batch, {makeRequest(Request::WRITE, dev,
                                            (uint64_t)host.data(), kLarge)})
                    .ok());
    ASSERT_TRUE(transport_.freeSubBatch(batch).ok());
    EXPECT_EQ(0, std::memcmp(host.data(), seed.data(), kLarge));

    loader_->free(dev, kLarge);
}

// A non-local target the transport has not mapped in (no prepareRemoteBuffer
// succeeded for that segment) is rejected synchronously, before any task is
// appended, so the engine can fail the request over to another transport.
TEST_F(XpuTransportTest, RejectsUnmappedNonLocalTarget) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kChunk));
    std::vector<uint8_t> staging(kChunk, 0);

    auto request =
        makeRequest(Request::WRITE, dev, (uint64_t)staging.data(), kChunk);
    request.target_id = LOCAL_SEGMENT_ID + 1;
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
    EXPECT_FALSE(transport_.submitTransferTasks(batch, {request}).ok());
    EXPECT_EQ(batch->size(), 0u);
    EXPECT_TRUE(transport_.freeSubBatch(batch).ok());

    loader_->free(dev, kChunk);
}

// A peer keys its mapping on the published attribute, so two registrations
// of one allocation must not publish the same text: a freed and reallocated
// buffer can come back with the same address, size and descriptor number.
TEST_F(XpuTransportTest, ReRegistrationPublishesDistinctIpcAttribute) {
    uint8_t *dev = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocDevice(&dev, kChunk));
    BufferDesc desc;
    desc.addr = reinterpret_cast<uint64_t>(dev);
    desc.length = kChunk;
    desc.location = "xpu:0";
    MemoryOptions options;
    ASSERT_TRUE(transport_.addMemoryBuffer(desc, options).ok());
    auto first = desc.transport_attrs.find(TransportType::XPU);
    if (first == desc.transport_attrs.end()) {
        loader_->free(dev, kChunk);
        GTEST_SKIP() << "buffer is not shareable (no Level Zero IPC)";
    }
    const std::string attr = first->second;
    ASSERT_TRUE(transport_.removeMemoryBuffer(desc).ok());
    ASSERT_TRUE(transport_.addMemoryBuffer(desc, options).ok());
    ASSERT_TRUE(desc.transport_attrs.count(TransportType::XPU));
    EXPECT_NE(desc.transport_attrs[TransportType::XPU], attr);
    ASSERT_TRUE(transport_.removeMemoryBuffer(desc).ok());

    loader_->free(dev, kChunk);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
