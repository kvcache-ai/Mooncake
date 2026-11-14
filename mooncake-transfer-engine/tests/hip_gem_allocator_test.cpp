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

// A buffer of hip_gem_allocator.so (mooncake.allocator.HipGemAllocator) lowers
// the KFD free VRAM that hipMemGetInfo reports and new device allocations draw
// from by its size once, at allocation, and not again when it is registered
// for RDMA (a hipMalloc buffer is charged a second time on amdgpu-dkms 6.18.4
// through 7.1.9). It is registered through its own dma-buf, RDMA writes into
// it land, and its location names the GPU it was allocated on.
//
// Needs an AMD GPU and an RDMA NIC that takes dma-buf MRs (the location test
// two GPUs). Pick NICs with MC_TEST_NICS=nic0[,nic1]; the allocator with
// MC_HIP_GEM_ALLOCATOR (default: the one in this build tree).

#include <dlfcn.h>
#include <gtest/gtest.h>
#include <hip/hip_runtime.h>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "cuda_alike.h"
#include "memory_location.h"
#include "transfer_engine.h"

using namespace mooncake;

namespace {

constexpr size_t kBufferSize = 4ull << 30;
constexpr size_t kCopySize = 64ull << 20;
// KFD reports availability in 2 MiB steps; allow for runtime allocations.
constexpr size_t kSlack = 64ull << 20;

size_t kfdFreeVram() {
    size_t free = 0, total = 0;
    EXPECT_EQ(hipMemGetInfo(&free, &total), hipSuccess);
    return free;
}

std::vector<std::string> testNics() {
    std::vector<std::string> nics;
    if (const char *env = std::getenv("MC_TEST_NICS")) {
        std::stringstream ss(env);
        for (std::string nic; std::getline(ss, nic, ',');)
            if (!nic.empty()) nics.push_back(nic);
    }
    return nics;
}

struct GemAllocator {
    void *(*malloc_fn)(ssize_t, int, hipStream_t) = nullptr;
    void (*free_fn)(void *, ssize_t, int, hipStream_t) = nullptr;
    int (*lookup_fn)(const void *, void **, size_t *, int *, int *) = nullptr;

    bool load() {
        const char *path = std::getenv("MC_HIP_GEM_ALLOCATOR");
        void *lib =
            dlopen(path ? path : HIP_GEM_ALLOCATOR_SO, RTLD_NOW | RTLD_GLOBAL);
        if (!lib) return false;
        malloc_fn = reinterpret_cast<decltype(malloc_fn)>(
            dlsym(lib, "mc_hip_gem_malloc"));
        free_fn =
            reinterpret_cast<decltype(free_fn)>(dlsym(lib, "mc_hip_gem_free"));
        lookup_fn = reinterpret_cast<decltype(lookup_fn)>(
            dlsym(lib, "mc_hip_gem_lookup"));
        return malloc_fn && free_fn && lookup_fn;
    }
};

class HipGemAllocatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(hipSetDevice(0), hipSuccess);
        ASSERT_TRUE(gem_.load())
            << "cannot load hip_gem_allocator.so: " << dlerror();
        if (kfdFreeVram() < 2 * kBufferSize + kSlack)
            GTEST_SKIP() << "needs " << ((2 * kBufferSize + kSlack) >> 20)
                         << " MiB of free VRAM";
        engine_ = std::make_unique<TransferEngine>(true, testNics());
        ASSERT_EQ(engine_->init("P2PHANDSHAKE", "hip_gem_test"), 0);
    }

    void TearDown() override {
        engine_.reset();
        for (auto &b : buffers_) gem_.free_fn(b.ptr, b.size, b.device, nullptr);
    }

    void *allocate(size_t size, int device = 0) {
        void *ptr = gem_.malloc_fn(size, device, nullptr);
        if (ptr) buffers_.push_back({ptr, size, device});
        return ptr;
    }

    struct Buffer {
        void *ptr;
        size_t size;
        int device;
    };
    GemAllocator gem_;
    std::unique_ptr<TransferEngine> engine_;
    std::vector<Buffer> buffers_;
};

TEST_F(HipGemAllocatorTest, AllocationChargedOnceAndLookedUp) {
    const size_t before = kfdFreeVram();
    void *buf = allocate(kBufferSize);
    ASSERT_NE(buf, nullptr);
    const size_t after = kfdFreeVram();
    EXPECT_LE(before - after, kBufferSize + kSlack);
    EXPECT_GE(before - after + kSlack, kBufferSize);

    void *base = nullptr;
    size_t size = 0;
    int device = -1;
    ASSERT_EQ(gem_.lookup_fn(static_cast<char *>(buf) + (1 << 20), &base, &size,
                             nullptr, &device),
              0);
    EXPECT_EQ(base, buf);
    EXPECT_EQ(size, kBufferSize);
    EXPECT_EQ(device, 0);
    EXPECT_NE(gem_.lookup_fn(static_cast<char *>(buf) + kBufferSize, nullptr,
                             nullptr, nullptr, nullptr),
              0);
}

// HIP reports imported memory as device 0's whichever device imported it; the
// location, which picks the NICs on both ends of a transfer, must name the GPU
// the buffer was allocated on.
TEST_F(HipGemAllocatorTest, LocationNamesTheAllocatingGpu) {
    int count = 0;
    ASSERT_EQ(hipGetDeviceCount(&count), hipSuccess);
    if (count < 2) GTEST_SKIP() << "needs two GPUs";
    const int device = count - 1;
    void *buf = allocate(kCopySize, device);
    ASSERT_NE(buf, nullptr);

    int looked_up = -1;
    ASSERT_EQ(gem_.lookup_fn(buf, nullptr, nullptr, nullptr, &looked_up), 0);
    EXPECT_EQ(looked_up, device);
    const auto entries = getMemoryLocation(buf, kCopySize, true);
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].location, GPU_PREFIX + std::to_string(device));

    hipPointerAttribute_t attr{};
    if (hipPointerGetAttributes(&attr, buf) == hipSuccess)
        std::printf("hipPointerGetAttributes reports device %d for it\n",
                    attr.device);
}

TEST_F(HipGemAllocatorTest, RegistrationKeepsKfdFreeVram) {
    void *buf = allocate(kBufferSize);
    ASSERT_NE(buf, nullptr);
    const size_t before = kfdFreeVram();
    ASSERT_EQ(engine_->registerLocalMemory(buf, kBufferSize), 0);
    const size_t now = kfdFreeVram();
    EXPECT_LE(before, now + kSlack) << "registered GEM buffer charged twice";
    // A new hardware queue needs VRAM; this is what failed in production.
    hipStream_t stream;
    ASSERT_EQ(hipStreamCreate(&stream), hipSuccess);
    EXPECT_EQ(hipMemsetAsync(buf, 0, 1 << 20, stream), hipSuccess);
    EXPECT_EQ(hipStreamSynchronize(stream), hipSuccess);
    EXPECT_EQ(hipStreamDestroy(stream), hipSuccess);
    // The first stream and kernel keep HIP runtime state in VRAM.
    const size_t registered = kfdFreeVram();
    ASSERT_EQ(engine_->unregisterLocalMemory(buf), 0);
    EXPECT_LE(registered, kfdFreeVram() + kSlack);
}

TEST_F(HipGemAllocatorTest, RdmaWriteLandsInGemBuffer) {
    char *src = static_cast<char *>(allocate(kCopySize));
    char *dst = static_cast<char *>(allocate(kBufferSize));
    ASSERT_NE(src, nullptr);
    ASSERT_NE(dst, nullptr);
    std::vector<uint8_t> pattern(kCopySize), back(kCopySize);
    for (size_t i = 0; i < kCopySize; ++i)
        pattern[i] = (i * 7 + i / 4093) & 0xff;
    ASSERT_EQ(hipMemcpy(src, pattern.data(), kCopySize, hipMemcpyHostToDevice),
              hipSuccess);
    ASSERT_EQ(hipMemset(dst, 0, kBufferSize), hipSuccess);
    ASSERT_EQ(engine_->registerLocalMemory(src, kCopySize), 0);
    ASSERT_EQ(engine_->registerLocalMemory(dst, kBufferSize), 0);

    // Past the first 1 GiB MR chunk of dst, so the chunk offset is exercised.
    const uint64_t target = reinterpret_cast<uint64_t>(dst) + (3ull << 29);
    const SegmentID segment =
        engine_->openSegment(engine_->getLocalIpAndPort());
    const BatchID batch = engine_->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.source = src;
    entry.target_id = segment;
    entry.target_offset = target;
    entry.length = kCopySize;
    ASSERT_TRUE(engine_->submitTransfer(batch, {entry}).ok());
    TransferStatus status;
    for (int i = 0; i < 20000000; ++i) {
        ASSERT_TRUE(engine_->getTransferStatus(batch, 0, status).ok());
        if (status.s == TransferStatusEnum::COMPLETED ||
            status.s == TransferStatusEnum::FAILED)
            break;
    }
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
    engine_->freeBatchID(batch);

    ASSERT_EQ(hipMemcpy(back.data(), reinterpret_cast<void *>(target),
                        kCopySize, hipMemcpyDeviceToHost),
              hipSuccess);
    EXPECT_EQ(back, pattern);
    ASSERT_EQ(engine_->unregisterLocalMemory(src), 0);
    ASSERT_EQ(engine_->unregisterLocalMemory(dst), 0);
}

}  // namespace
