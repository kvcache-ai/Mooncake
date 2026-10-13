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

// Tests Mooncake's built-in TPU adapter (libmooncake_tpu_pjrt.so) end to end
// against the mock buffer provider: provider binding, handle ownership, the
// mc_tpu_pjrt_* ABI, and finally TENT's real TpuPjrtShim loading the adapter.
// No TPU hardware or real provider required.
//
// The adapter keeps process-wide state (one bound provider, a sticky device
// table, and TpuPjrtShim's singleton), so the tests in this file build on each
// other and rely on gtest's default in-order execution.
//
// TPU_PJRT_ADAPTER_LIB and MOCK_TPU_BUFFER_PROVIDER_LIB are injected by CMake.

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "tent/platform/tpu_pjrt_shim.h"

#ifndef TPU_PJRT_ADAPTER_LIB
#error "TPU_PJRT_ADAPTER_LIB must be defined by the build"
#endif
#ifndef MOCK_TPU_BUFFER_PROVIDER_LIB
#error "MOCK_TPU_BUFFER_PROVIDER_LIB must be defined by the build"
#endif

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kChunk = 4ul << 20;  // ProxyManager's default chunk size

// Adapter control API (tpu_pjrt_adapter.h) and data-path ABI.
using LoadProviderFn = int (*)(const char *);
using RegisterFn = int (*)(uint64_t, uint64_t *, uint64_t *, int *);
using UnregisterFn = int (*)(uint64_t);
using ReleaseFn = int (*)(uint64_t);
using InitFn = int (*)();
using CopyD2HFn = int (*)(void *, const void *, size_t);
using CopyH2DFn = int (*)(void *, const void *, size_t);

// Mock provider test helpers.
using CreateFn = uint64_t (*)(size_t, int, int);
using DataFn = uint8_t *(*)(uint64_t);
using CountFn = int (*)();
using SetFailFn = void (*)(int);

template <typename Fn>
Fn sym(void *lib, const char *name) {
    return reinterpret_cast<Fn>(dlsym(lib, name));
}

class TpuPjrtAdapterTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        // Same file the shim will load, so both see one registry.
        ::setenv("MC_TPU_PJRT_LIB", TPU_PJRT_ADAPTER_LIB, /*overwrite=*/1);
        adapter_ = dlopen(TPU_PJRT_ADAPTER_LIB, RTLD_NOW | RTLD_LOCAL);
        provider_ = dlopen(MOCK_TPU_BUFFER_PROVIDER_LIB, RTLD_NOW | RTLD_LOCAL);
    }

    void SetUp() override {
        ASSERT_NE(adapter_, nullptr) << dlerror();
        ASSERT_NE(provider_, nullptr) << dlerror();
        load_ = sym<LoadProviderFn>(adapter_, "mc_tpu_adapter_load_provider");
        register_ = sym<RegisterFn>(adapter_, "mc_tpu_adapter_register");
        unregister_ = sym<UnregisterFn>(adapter_, "mc_tpu_adapter_unregister");
        release_ = sym<ReleaseFn>(adapter_, "mc_tpu_adapter_release_buffer");
        init_ = sym<InitFn>(adapter_, "mc_tpu_pjrt_init");
        d2h_ = sym<CopyD2HFn>(adapter_, "mc_tpu_pjrt_copy_d2h");
        h2d_ = sym<CopyH2DFn>(adapter_, "mc_tpu_pjrt_copy_h2d");
        create_ = sym<CreateFn>(provider_, "mock_provider_create_buffer");
        data_ = sym<DataFn>(provider_, "mock_provider_buffer_data");
        live_ = sym<CountFn>(provider_, "mock_provider_live_count");
        released_ = sym<CountFn>(provider_, "mock_provider_released_count");
        set_fail_ = sym<SetFailFn>(provider_, "mock_provider_set_fail_copies");
        for (void *fn : {(void *)load_, (void *)register_, (void *)unregister_,
                         (void *)release_, (void *)init_, (void *)d2h_,
                         (void *)h2d_, (void *)create_, (void *)data_,
                         (void *)live_, (void *)released_, (void *)set_fail_}) {
            ASSERT_NE(fn, nullptr);
        }
    }

    void TearDown() override {
        if (set_fail_) set_fail_(0);
    }

    struct Registered {
        uint64_t handle = 0;
        uint64_t token = 0;
        uint64_t size = 0;
        int device = -1;
    };

    Registered registerNew(size_t size, int device, int numa) {
        Registered r;
        r.handle = create_(size, device, numa);
        EXPECT_EQ(register_(r.handle, &r.token, &r.size, &r.device), 0);
        return r;
    }

    static void *adapter_;
    static void *provider_;
    LoadProviderFn load_ = nullptr;
    RegisterFn register_ = nullptr;
    UnregisterFn unregister_ = nullptr;
    ReleaseFn release_ = nullptr;
    InitFn init_ = nullptr;
    CopyD2HFn d2h_ = nullptr;
    CopyH2DFn h2d_ = nullptr;
    CreateFn create_ = nullptr;
    DataFn data_ = nullptr;
    CountFn live_ = nullptr;
    CountFn released_ = nullptr;
    SetFailFn set_fail_ = nullptr;
};

void *TpuPjrtAdapterTest::adapter_ = nullptr;
void *TpuPjrtAdapterTest::provider_ = nullptr;

TEST_F(TpuPjrtAdapterTest, RejectsIncompatibleProvidersThenBindsOnce) {
    uint64_t token = 0, size = 0;
    int device = -1;
    EXPECT_NE(register_(1, &token, &size, &device), 0);  // nothing bound yet

    EXPECT_NE(load_(nullptr), 0);
    EXPECT_NE(load_(""), 0);
    EXPECT_NE(load_("/nonexistent/libprovider.so"), 0);
    // A loadable library without the provider entry point.
    EXPECT_NE(load_(TPU_PJRT_ADAPTER_LIB), 0);

    ASSERT_EQ(load_(MOCK_TPU_BUFFER_PROVIDER_LIB), 0);
    EXPECT_EQ(load_(MOCK_TPU_BUFFER_PROVIDER_LIB), 0);  // idempotent
    EXPECT_NE(load_(TPU_PJRT_ADAPTER_LIB), 0);          // already bound
}

// TENT disables TPU support if the adapter reports no devices at init time,
// i.e. if the engine is created before any buffer is registered.
TEST_F(TpuPjrtAdapterTest, InitFailsUntilABufferIsRegistered) {
    EXPECT_NE(init_(), 0);
    Registered r = registerNew(kChunk, /*device=*/0, /*numa=*/0);
    EXPECT_EQ(init_(), 0);
    EXPECT_EQ(unregister_(r.token), 0);
    EXPECT_EQ(init_(), 0);  // devices stay known after unregistering
}

TEST_F(TpuPjrtAdapterTest, RegisterAdoptsHandleAndUnregisterReleasesIt) {
    const int live_before = live_();
    const int released_before = released_();
    Registered r = registerNew(3 * kChunk, /*device=*/2, /*numa=*/1);
    EXPECT_NE(r.token, 0u);
    EXPECT_EQ(r.size, 3 * kChunk);
    EXPECT_EQ(r.device, 2);
    EXPECT_EQ(live_(), live_before + 1);

    EXPECT_EQ(unregister_(r.token), 0);
    EXPECT_EQ(released_(), released_before + 1);
    EXPECT_EQ(live_(), live_before);
    EXPECT_EQ(data_(r.handle), nullptr);
    EXPECT_NE(unregister_(r.token), 0);
}

TEST_F(TpuPjrtAdapterTest, FailedRegistrationStillReleasesTheHandle) {
    uint64_t token = 0, size = 0;
    int device = -1;
    // Invalid device index: the registry refuses the region.
    const uint64_t bad = create_(kChunk, /*device=*/-1, /*numa=*/0);
    const int released_before = released_();
    EXPECT_NE(register_(bad, &token, &size, &device), 0);
    EXPECT_EQ(released_(), released_before + 1);
    EXPECT_EQ(data_(bad), nullptr);

    EXPECT_NE(register_(0, &token, &size, &device), 0);
}

TEST_F(TpuPjrtAdapterTest, ReleaseBufferDropsAnUnadoptedHandle) {
    const uint64_t handle = create_(kChunk, 0, 0);
    ASSERT_NE(data_(handle), nullptr);
    EXPECT_EQ(release_(handle), 0);
    EXPECT_EQ(data_(handle), nullptr);
    EXPECT_NE(release_(0), 0);
}

TEST_F(TpuPjrtAdapterTest, AbiCopiesThroughInteriorTokensReachTheProvider) {
    Registered r = registerNew(3 * kChunk, 0, 0);
    uint8_t *device_bytes = data_(r.handle);
    ASSERT_NE(device_bytes, nullptr);
    auto *token = reinterpret_cast<uint8_t *>(r.token);
    std::vector<uint8_t> host(kChunk);

    // Chunk #1, D2H through an interior address.
    ASSERT_EQ(d2h_(host.data(), token + kChunk, kChunk), 0);
    EXPECT_EQ(0, std::memcmp(host.data(), device_bytes + kChunk, kChunk));

    // Chunk #2, H2D through an interior address.
    std::memset(host.data(), 0x5A, kChunk);
    ASSERT_EQ(h2d_(token + 2 * kChunk, host.data(), kChunk), 0);
    EXPECT_EQ(device_bytes[2 * kChunk], 0x5A);
    EXPECT_EQ(device_bytes[3 * kChunk - 1], 0x5A);
    EXPECT_NE(device_bytes[2 * kChunk - 1], 0x5A);

    // Past the end fails instead of truncating; provider errors propagate.
    EXPECT_NE(d2h_(host.data(), token + 2 * kChunk + 1, kChunk), 0);
    set_fail_(1);
    EXPECT_NE(d2h_(host.data(), token, kChunk), 0);
    EXPECT_NE(h2d_(token, host.data(), kChunk), 0);

    EXPECT_EQ(unregister_(r.token), 0);
}

// The shim is what TENT uses. Buffers are registered before its singleton is
// first touched, as mooncake.tpu requires before creating the engine.
TEST_F(TpuPjrtAdapterTest, RealShimLoadsAdapterAndCopies) {
    Registered r0 = registerNew(3 * kChunk, /*device=*/0, /*numa=*/0);
    Registered r3 = registerNew(kChunk, /*device=*/3, /*numa=*/1);
    uint8_t *device_bytes = data_(r0.handle);
    ASSERT_NE(device_bytes, nullptr);

    auto &shim = TpuPjrtShim::instance();
    ASSERT_TRUE(shim.available());
    EXPECT_EQ(shim.deviceCount(), 4);
    EXPECT_EQ(shim.deviceNumaNode(3), 1);

    auto *token = reinterpret_cast<uint8_t *>(r0.token);
    std::vector<uint8_t> host(kChunk);
    EXPECT_TRUE(shim.isDevicePtr(token + kChunk + 5));
    EXPECT_FALSE(shim.isDevicePtr(host.data()));
    EXPECT_EQ(shim.deviceIndex(reinterpret_cast<void *>(r3.token + 7)), 3);

    ASSERT_TRUE(shim.copyD2H(host.data(), token + kChunk, kChunk).ok());
    EXPECT_EQ(0, std::memcmp(host.data(), device_bytes + kChunk, kChunk));
    std::memset(host.data(), 0x33, kChunk);
    ASSERT_TRUE(shim.copyH2D(token, host.data(), kChunk).ok());
    EXPECT_EQ(device_bytes[0], 0x33);
    EXPECT_FALSE(shim.copyD2H(host.data(), token + 3 * kChunk, 1).ok());

    EXPECT_EQ(unregister_(r0.token), 0);
    EXPECT_EQ(unregister_(r3.token), 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
