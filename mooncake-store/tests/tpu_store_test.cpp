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

// Store client tests for TPU buffers, run against TENT's mock PJRT adapter
// (tent/tests/tpu/mock_tpu_pjrt_adapter.cpp). No TPU hardware or PJRT runtime
// is needed.
//
// A mock device buffer is a token, as on a real TPU: reading it yields poison
// and writing it faults, while the bytes live where only the adapter reaches
// them. A round trip therefore passes only if the Store never dereferenced
// the token.

#include <dlfcn.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "device/accelerator_registry.h"
#include "real_client.h"
#include "test_server_helpers.h"

#ifndef MOCK_TPU_PJRT_LIB
#error "MOCK_TPU_PJRT_LIB must be defined by the build (path to mock adapter)"
#endif

namespace mooncake::testing {
namespace {

// Spans two Store slices and several TENT staging chunks, the last one short,
// so the adapter is handed interior addresses by both.
constexpr size_t kSize = 20 * 1024 * 1024 + 123;
constexpr size_t kSegmentSize = 64 * 1024 * 1024;
constexpr size_t kLocalBufferSize = 32 * 1024 * 1024;

class TpuStoreTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("tpu_store_test");
        FLAGS_logtostderr = 1;
        // TpuPjrtShim loads the adapter once, on first use: name the mock
        // before any Store call classifies a pointer.
        setenv("MC_TPU_PJRT_LIB", MOCK_TPU_PJRT_LIB, 1);
        // dlopen is reference-counted, so this is the image the shim loads.
        void* mock = dlopen(MOCK_TPU_PJRT_LIB, RTLD_NOW | RTLD_GLOBAL);
        ASSERT_NE(mock, nullptr) << dlerror();
        register_device_ = reinterpret_cast<void* (*)(size_t, int)>(
            dlsym(mock, "mock_tpu_pjrt_register_device"));
        device_data_ = reinterpret_cast<void* (*)(const void*)>(
            dlsym(mock, "mock_tpu_pjrt_device_data"));
        reset_ =
            reinterpret_cast<void (*)()>(dlsym(mock, "mock_tpu_pjrt_reset"));
        ASSERT_TRUE(register_device_ && device_data_ && reset_);
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        // Only TENT moves TPU memory between clients. Set per test, because
        // the classic-engine case turns it off.
        setenv("MC_USE_TENT", "1", 1);
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
    }

    void TearDown() override {
        for (auto& client : clients_) client->tearDownAll();
        clients_.clear();
        master_.Stop();
        reset_();
    }

    // `local_memcpy` selects how a client reaches a replica it hosts itself:
    // the Store's memcpy workers, or the transfer engine like any other.
    std::shared_ptr<RealClient> NewClient(
        size_t segment_size, bool local_memcpy,
        size_t local_buffer_size = kLocalBufferSize) {
        setenv("MC_STORE_MEMCPY", local_memcpy ? "1" : "0", 1);
        auto client = RealClient::create();
        EXPECT_EQ(client->setup_real("127.0.0.1:0", "P2PHANDSHAKE",
                                     segment_size, local_buffer_size, "tcp", "",
                                     master_.master_address()),
                  0);
        clients_.push_back(client);
        return client;
    }

    static char* NewDeviceBuffer(size_t size, int device = 0) {
        return static_cast<char*>(register_device_(size, device));
    }

    static std::vector<char> Payload(size_t size) {
        std::vector<char> data(size);
        for (size_t i = 0; i < size; ++i) data[i] = (i * 17 + 3) % 251;
        return data;
    }

    static std::vector<char> DeviceBytes(const char* token, size_t size) {
        auto* data = static_cast<const char*>(device_data_(token));
        return std::vector<char>(data, data + size);
    }

    // Writes an object from one device buffer and reads it back into another.
    void RoundTrip(RealClient& client, const std::string& key) {
        const auto payload = Payload(kSize);
        char* source = NewDeviceBuffer(kSize);
        char* target = NewDeviceBuffer(kSize, /*device=*/1);
        ASSERT_TRUE(source && target);
        std::memcpy(device_data_(source), payload.data(), kSize);
        ASSERT_EQ(client.register_buffer(source, kSize), 0);
        ASSERT_EQ(client.register_buffer(target, kSize), 0);

        ASSERT_EQ(client.put_from(key, source, kSize), 0);
        ASSERT_EQ(client.get_into(key, target, kSize),
                  static_cast<int64_t>(kSize));
        EXPECT_EQ(DeviceBytes(target, kSize), payload);

        // batch_get_into never stages in the client: the engine or the
        // memcpy workers get the device destination as it is.
        std::memset(device_data_(target), 0, kSize);
        EXPECT_EQ(client.batch_get_into({key}, {target}, {kSize}),
                  std::vector<int64_t>{static_cast<int64_t>(kSize)});
        EXPECT_EQ(DeviceBytes(target, kSize), payload);

        EXPECT_EQ(client.unregister_buffer(source), 0);
        EXPECT_EQ(client.unregister_buffer(target), 0);
    }

    static inline void* (*register_device_)(size_t, int) = nullptr;
    static inline void* (*device_data_)(const void*) = nullptr;
    static inline void (*reset_)() = nullptr;

    InProcMaster master_;
    std::vector<std::shared_ptr<RealClient>> clients_;
};

TEST_F(TpuStoreTest, RegistryClassifiesAndCopiesTpuMemory) {
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    const size_t size = 4096;
    char* token = NewDeviceBuffer(size, /*device=*/2);
    ASSERT_NE(token, nullptr);
    auto host = Payload(size);

    device::PointerInfo info;
    auto* accelerator = runtime.FindDeviceForPointer(token + 100, &info);
    ASSERT_NE(accelerator, nullptr);
    EXPECT_EQ(accelerator->Vendor(), device::AcceleratorVendor::kTpu);
    EXPECT_EQ(info.device_id, 2);
    EXPECT_EQ(runtime.FindDeviceForPointer(host.data()), nullptr);

    ASSERT_TRUE(runtime.CopyFromHost(token, host.data(), size));
    EXPECT_EQ(DeviceBytes(token, size), host);
    std::vector<char> read_back(size);
    ASSERT_TRUE(runtime.CopyToHost(read_back.data(), token, size));
    EXPECT_EQ(read_back, host);

    // CopyAuto leaves the direction to the device.
    std::vector<char> interior(100);
    ASSERT_TRUE(runtime.CopyAuto(interior.data(), token + 100, 100));
    EXPECT_EQ(std::memcmp(interior.data(), host.data() + 100, 100), 0);
    // The adapter has no HBM-to-HBM copy.
    EXPECT_FALSE(runtime.CopyAuto(NewDeviceBuffer(size), token, size));
}

TEST_F(TpuStoreTest, LocalReplicaRoundTripsThroughMemcpyWorkers) {
    auto client = NewClient(kSegmentSize, /*local_memcpy=*/true);
    RoundTrip(*client, "tpu-local");
}

TEST_F(TpuStoreTest, RemoteReplicaRoundTripsThroughTent) {
    NewClient(kSegmentSize, /*local_memcpy=*/false);  // hosts every replica
    auto client = NewClient(0, /*local_memcpy=*/false);
    RoundTrip(*client, "tpu-remote");
}

// TENT stages a TPU read in chunks, so the object does not have to fit in the
// client's local buffer.
TEST_F(TpuStoreTest, RemoteReadLargerThanLocalBuffer) {
    NewClient(kSegmentSize, /*local_memcpy=*/false);  // hosts every replica
    auto client = NewClient(0, /*local_memcpy=*/false, kSize / 2);
    RoundTrip(*client, "tpu-large");
}

// The classic engine cannot address TPU memory, so the client stages the read
// in its local buffer and copies it to the device itself.
TEST_F(TpuStoreTest, ClassicEngineStagesReadInClient) {
    setenv("MC_USE_TENT", "0", 1);
    NewClient(kSegmentSize, /*local_memcpy=*/false);  // hosts every replica
    auto client = NewClient(0, /*local_memcpy=*/false);
    const auto payload = Payload(kSize);
    char* target = NewDeviceBuffer(kSize);
    ASSERT_NE(target, nullptr);

    ASSERT_EQ(client->put("tpu-classic", payload), 0);
    ASSERT_EQ(client->get_into("tpu-classic", target, kSize),
              static_cast<int64_t>(kSize));
    EXPECT_EQ(DeviceBytes(target, kSize), payload);
}

}  // namespace
}  // namespace mooncake::testing
