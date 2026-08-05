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

// Drives TpuTransport's staging hop against the mock PJRT adapter. No TPU
// hardware or PJRT runtime required.
//
// The cases here mirror the requests ProxyManager actually issues: a local
// stage whose `source` is `device_token + chunk_offset` and whose
// `target_offset` is the host staging buffer, and the delegated remote stage
// where the device side is `target_offset` instead.
//
// The mock's device tokens are poisoned (see mock_tpu_pjrt_adapter.cpp), so a
// staging copy that bypasses the adapter and memcpy()s straight from a token
// yields 0xDD rather than the buffer's data. That makes the data assertions
// below real detectors of the "device pointer misclassified as host memory"
// failure mode, instead of accidentally passing.

#include "tent/transport/tpu/tpu_transport.h"

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <vector>

#ifndef MOCK_TPU_PJRT_LIB
#error "MOCK_TPU_PJRT_LIB must be defined by the build (path to mock adapter)"
#endif

namespace mooncake {
namespace tent {
namespace {

using RegisterFn = void *(*)(size_t, int);
using DeviceDataFn = void *(*)(const void *);
using PoisonFn = unsigned char (*)();
using ResetFn = void (*)();

constexpr size_t kChunk = 4ul << 20;  // ProxyManager's default chunk_size
constexpr size_t kBufSize = 3 * kChunk;

class TpuTransportTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        ::setenv("MC_TPU_PJRT_LIB", MOCK_TPU_PJRT_LIB, /*overwrite=*/1);
    }

    void SetUp() override {
        mock_ = dlopen(MOCK_TPU_PJRT_LIB, RTLD_NOW | RTLD_GLOBAL);
        ASSERT_NE(mock_, nullptr) << dlerror();
        register_ = reinterpret_cast<RegisterFn>(
            dlsym(mock_, "mock_tpu_pjrt_register_device"));
        device_data_ = reinterpret_cast<DeviceDataFn>(
            dlsym(mock_, "mock_tpu_pjrt_device_data"));
        poison_ = reinterpret_cast<PoisonFn>(
            dlsym(mock_, "mock_tpu_pjrt_poison_byte"));
        reset_ = reinterpret_cast<ResetFn>(dlsym(mock_, "mock_tpu_pjrt_reset"));
        ASSERT_NE(register_, nullptr);
        ASSERT_NE(device_data_, nullptr);
        ASSERT_NE(poison_, nullptr);
        ASSERT_NE(reset_, nullptr);
        reset_();

        std::string segment = "local";
        ASSERT_TRUE(
            transport_.install(segment, nullptr, nullptr, nullptr).ok());
    }

    void TearDown() override {
        transport_.uninstall();
        if (reset_) reset_();
        if (mock_) dlclose(mock_);
    }

    // Runs one request through the transport and returns its final status.
    TransferStatus run(const Request &request) {
        Transport::SubBatchRef batch = nullptr;
        EXPECT_TRUE(transport_.allocateSubBatch(batch, 1).ok());
        EXPECT_TRUE(transport_.submitTransferTasks(batch, {request}).ok());
        TransferStatus status{};
        EXPECT_TRUE(transport_.getTransferStatus(batch, 0, status).ok());
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

    void *mock_ = nullptr;
    RegisterFn register_ = nullptr;
    DeviceDataFn device_data_ = nullptr;
    PoisonFn poison_ = nullptr;
    ResetFn reset_ = nullptr;
    TpuTransport transport_;
};

// The local WRITE stage of chunk #1: source is an interior device address.
// Before interior pointers were part of the adapter contract this silently
// memcpy()d from a non-data token and still reported COMPLETED.
TEST_F(TpuTransportTest, LocalStageCopiesFromInteriorDeviceOffset) {
    auto *token = static_cast<uint8_t *>(register_(kBufSize, /*index=*/0));
    ASSERT_NE(token, nullptr);
    auto *data = static_cast<uint8_t *>(device_data_(token));
    for (size_t i = 0; i < kBufSize; ++i) data[i] = (uint8_t)(i % 251);
    std::vector<uint8_t> staging(kChunk, 0);

    // Chunk #1: device_token + 4 MiB -> host staging buffer.
    auto status = run(makeRequest(Request::WRITE, token + kChunk,
                                  (uint64_t)staging.data(), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kChunk);
    EXPECT_EQ(0, std::memcmp(staging.data(), data + kChunk, kChunk));
    // Not the token's poison: the adapter really did the copy.
    EXPECT_NE(staging[0], poison_());
}

// The mirrored remote stage: the device side is `target_offset`, at an offset.
TEST_F(TpuTransportTest, RemoteStageCopiesToInteriorDeviceOffset) {
    auto *token = static_cast<uint8_t *>(register_(kBufSize, /*index=*/0));
    ASSERT_NE(token, nullptr);
    std::vector<uint8_t> staging(kChunk);
    for (size_t i = 0; i < kChunk; ++i) staging[i] = (uint8_t)(i % 197);

    // WRITE with a device target: host staging -> device_token + 4 MiB.
    auto status = run(makeRequest(Request::WRITE, staging.data(),
                                  (uint64_t)(token + kChunk), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
    auto *data = static_cast<uint8_t *>(device_data_(token));
    EXPECT_EQ(0, std::memcmp(data + kChunk, staging.data(), kChunk));
}

// A READ stage moves host staging -> device.
TEST_F(TpuTransportTest, ReadStageCopiesHostToDevice) {
    auto *token = static_cast<uint8_t *>(register_(kBufSize, /*index=*/0));
    ASSERT_NE(token, nullptr);
    std::vector<uint8_t> staging(kChunk, 0xAB);

    auto status = run(makeRequest(Request::READ, token + 2 * kChunk,
                                  (uint64_t)staging.data(), kChunk));
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
    auto *data = static_cast<uint8_t *>(device_data_(token));
    EXPECT_EQ(0, std::memcmp(data + 2 * kChunk, staging.data(), kChunk));
}

// Neither side is device memory: the adapter is missing, or it failed to
// classify an interior pointer. Either way the transport must fail rather than
// let Platform::copy memcpy from a token that is not the buffer's data.
TEST_F(TpuTransportTest, FailsWhenNeitherSideIsDeviceMemory) {
    std::vector<uint8_t> host_a(4096, 1), host_b(4096, 2);
    auto status = run(makeRequest(Request::WRITE, host_a.data(),
                                  (uint64_t)host_b.data(), host_a.size()));
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    // The destination is untouched -- no silent partial copy.
    EXPECT_EQ(host_b[0], 2);
}

// Both sides device: HBM<->HBM is not a staging hop and must be rejected.
TEST_F(TpuTransportTest, FailsWhenBothSidesAreDeviceMemory) {
    auto *a = static_cast<uint8_t *>(register_(kBufSize, /*index=*/0));
    auto *b = static_cast<uint8_t *>(register_(kBufSize, /*index=*/1));
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);

    auto status = run(makeRequest(Request::WRITE, a, (uint64_t)b, kChunk));
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
}

// TPU HBM can never be a remote peer; a non-local target is a routing bug.
TEST_F(TpuTransportTest, FailsOnNonLocalTarget) {
    auto *token = static_cast<uint8_t *>(register_(kBufSize, /*index=*/0));
    ASSERT_NE(token, nullptr);
    std::vector<uint8_t> staging(kChunk, 0);

    auto request =
        makeRequest(Request::WRITE, token, (uint64_t)staging.data(), kChunk);
    request.target_id = LOCAL_SEGMENT_ID + 1;
    auto status = run(request);
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
