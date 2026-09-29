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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <thread>

#include "cuda_alike.h"
#include "gpu_vendor/xpu.h"

// Helper: returns true if at least one Intel XPU device is reachable via
// Level Zero on the current host. Tests that require GPU hardware call this
// in SetUp() and GTEST_SKIP() when it returns false so that the suite runs
// cleanly on head-nodes and CI machines that lack GPUs.
static bool hasXpuDevice() {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || count == 0) return false;
    return true;
}

class XpuTransportTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("XpuTransportTest");
        FLAGS_logtostderr = 1;
        gpu_available_ = hasXpuDevice();
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        if (!gpu_available_) {
            GTEST_SKIP() << "No Intel XPU device available on this host";
        }
    }

    static bool gpu_available_;
};

bool XpuTransportTest::gpu_available_ = false;

// Test Level Zero initialization via cuda-alike shim
TEST_F(XpuTransportTest, LevelZeroInit) {
    // If we got here, SetUp() confirmed a device is available.
    int count = 0;
    auto err = cudaGetDeviceCount(&count);
    ASSERT_EQ(err, cudaSuccess) << "Level Zero initialization failed";
    ASSERT_GT(count, 0) << "No Intel XPU devices found";
    LOG(INFO) << "Found " << count << " Intel XPU device(s)";
}

// Test device selection
TEST_F(XpuTransportTest, SetDevice) {
    auto err = cudaSetDevice(0);
    ASSERT_EQ(err, cudaSuccess) << "Failed to set device 0";
}

// Test device memory allocation and free
TEST_F(XpuTransportTest, DeviceAllocFree) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    void *ptr = nullptr;
    auto err = cudaMalloc(&ptr, 4096);
    ASSERT_EQ(err, cudaSuccess) << "cudaMalloc (zeMemAllocDevice) failed";
    ASSERT_NE(ptr, nullptr);

    err = cudaFree(ptr);
    ASSERT_EQ(err, cudaSuccess) << "cudaFree (zeMemFree) failed";
}

// Test host memory allocation
TEST_F(XpuTransportTest, HostAlloc) {
    void *ptr = nullptr;
    auto err = cudaHostAlloc(&ptr, 4096, 0);
    ASSERT_EQ(err, cudaSuccess) << "cudaHostAlloc (zeMemAllocHost) failed";
    ASSERT_NE(ptr, nullptr);

    // Host memory should be writable
    std::memset(ptr, 0xAB, 4096);

    err = cudaFree(ptr);
    ASSERT_EQ(err, cudaSuccess);
}

// Test pointer attributes for device memory
TEST_F(XpuTransportTest, PointerAttributesDevice) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    void *ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&ptr, 4096), cudaSuccess);

    cudaPointerAttributes attrs;
    auto err = cudaPointerGetAttributes(&attrs, ptr);
    ASSERT_EQ(err, cudaSuccess)
        << "cudaPointerGetAttributes failed for device memory";
    ASSERT_EQ(attrs.type, cudaMemoryTypeDevice)
        << "Expected device memory type";

    cudaFree(ptr);
}

// Test pointer attributes for host memory
TEST_F(XpuTransportTest, PointerAttributesHost) {
    void *ptr = nullptr;
    ASSERT_EQ(cudaHostAlloc(&ptr, 4096, 0), cudaSuccess);

    cudaPointerAttributes attrs;
    auto err = cudaPointerGetAttributes(&attrs, ptr);
    ASSERT_EQ(err, cudaSuccess)
        << "cudaPointerGetAttributes failed for host memory";
    ASSERT_EQ(attrs.type, cudaMemoryTypeHost) << "Expected host memory type";

    cudaFree(ptr);
}

// Test memcpy host-to-device and device-to-host
TEST_F(XpuTransportTest, MemcpyHostDeviceRoundtrip) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    const size_t size = 1024;
    std::vector<uint8_t> src(size, 0);
    std::vector<uint8_t> dst(size, 0);

    // Fill source with pattern
    for (size_t i = 0; i < size; ++i) src[i] = static_cast<uint8_t>(i & 0xFF);

    void *dev_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&dev_ptr, size), cudaSuccess);

    // Host -> Device
    auto err = cudaMemcpy(dev_ptr, src.data(), size, cudaMemcpyHostToDevice);
    ASSERT_EQ(err, cudaSuccess) << "H2D memcpy failed";

    // Device -> Host
    err = cudaMemcpy(dst.data(), dev_ptr, size, cudaMemcpyDeviceToHost);
    ASSERT_EQ(err, cudaSuccess) << "D2H memcpy failed";

    EXPECT_EQ(src, dst) << "Data mismatch after H2D + D2H roundtrip";

    cudaFree(dev_ptr);
}

// Test isDeviceMemory helper
TEST_F(XpuTransportTest, IsDeviceMemory) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    void *dev_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&dev_ptr, 4096), cudaSuccess);

    EXPECT_TRUE(mooncake::xpu::isDeviceMemory(dev_ptr))
        << "Device pointer not recognized as device memory";

    void *host_ptr = nullptr;
    ASSERT_EQ(cudaHostAlloc(&host_ptr, 4096, 0), cudaSuccess);

    EXPECT_FALSE(mooncake::xpu::isDeviceMemory(host_ptr))
        << "Host pointer incorrectly identified as device memory";

    // Stack memory should not be device memory
    int stack_var = 42;
    EXPECT_FALSE(mooncake::xpu::isDeviceMemory(&stack_var));

    cudaFree(dev_ptr);
    cudaFree(host_ptr);
}

// Test DMA-BUF export for RDMA registration
TEST_F(XpuTransportTest, ExportDmaBufFd) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    const size_t size = 4096;
    void *dev_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&dev_ptr, size), cudaSuccess);

    int fd = mooncake::xpu::exportDmaBufFd(dev_ptr, size);
    EXPECT_GE(fd, 0) << "DMA-BUF fd export failed (fd=" << fd << ")";

    if (fd >= 0) {
        close(fd);
    }

    cudaFree(dev_ptr);
}

// The fd must be caller-owned: two exports give independent descriptors, and
// closing one leaves the other valid. A driver-owned fd would alias.
TEST_F(XpuTransportTest, ExportDmaBufFdIsCallerOwned) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    const size_t size = 4096;
    void *dev_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&dev_ptr, size), cudaSuccess);

    int fd1 = mooncake::xpu::exportDmaBufFd(dev_ptr, size);
    int fd2 = mooncake::xpu::exportDmaBufFd(dev_ptr, size);
    ASSERT_GE(fd1, 0);
    ASSERT_GE(fd2, 0);
    EXPECT_NE(fd1, fd2) << "exports must not alias the same descriptor";

    close(fd1);
    // fd2 must still be valid after fd1 is closed.
    EXPECT_EQ(fcntl(fd2, F_GETFD) >= 0, true)
        << "fd2 invalidated by closing fd1";
    close(fd2);

    cudaFree(dev_ptr);
}

// An interior pointer must report the allocation base and matching offset: the
// RDMA dmabuf path registers at base+offset, so a wrong base registers the
// wrong bytes.
TEST_F(XpuTransportTest, GetAllocBaseReportsOffset) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    const size_t size = 1 << 20;
    void *dev_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&dev_ptr, size), cudaSuccess);

    void *base = nullptr;
    size_t alloc_size = 0;
    ASSERT_EQ(mooncake::xpu::getAllocBase(dev_ptr, &base, &alloc_size), 0);
    EXPECT_EQ(base, dev_ptr);
    EXPECT_GE(alloc_size, size);

    // An interior pointer resolves to the same base, with a real offset.
    const size_t kOffset = 4096;
    void *interior = static_cast<char *>(dev_ptr) + kOffset;
    void *base2 = nullptr;
    size_t alloc_size2 = 0;
    ASSERT_EQ(mooncake::xpu::getAllocBase(interior, &base2, &alloc_size2), 0);
    EXPECT_EQ(base2, dev_ptr);
    EXPECT_EQ(static_cast<char *>(interior) - static_cast<char *>(base2),
              (ptrdiff_t)kOffset);

    cudaFree(dev_ptr);
}

// ---------- Asynchronous copies / streams ----------

// A copy must be complete once the stream is synchronised. Also catches an
// "append never executed" bug (e.g. a missing close/execute on the list).
TEST_F(XpuTransportTest, MemcpyAsyncCompletesAfterStreamSynchronize) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    cudaStream_t stream = nullptr;
    ASSERT_EQ(cudaStreamCreate(&stream), cudaSuccess);
    ASSERT_NE(stream, nullptr);

    const size_t size = 64 * 1024;
    std::vector<uint8_t> src(size), dst(size, 0);
    for (size_t i = 0; i < size; ++i) src[i] = static_cast<uint8_t>(i & 0xFF);

    void *dev = nullptr;
    ASSERT_EQ(cudaMalloc(&dev, size), cudaSuccess);

    ASSERT_EQ(
        cudaMemcpyAsync(dev, src.data(), size, cudaMemcpyHostToDevice, stream),
        cudaSuccess);
    ASSERT_EQ(
        cudaMemcpyAsync(dst.data(), dev, size, cudaMemcpyDeviceToHost, stream),
        cudaSuccess);
    ASSERT_EQ(cudaStreamSynchronize(stream), cudaSuccess);

    EXPECT_EQ(src, dst) << "async H2D + D2H roundtrip lost data";

    cudaFree(dev);
    EXPECT_EQ(cudaStreamDestroy(stream), cudaSuccess);
}

// Work submitted to one stream must execute in submission order, so a later
// write to an overlapping region wins over an earlier one.
//
// This is a real detector: with ZE_COMMAND_QUEUE_FLAG_IN_ORDER removed it fails
// (reads 0xAA instead of 0xBB), which is the corruption the flag prevents.
//
// Do not simplify the shape. Weakening any one of these makes the test pass
// against the broken implementation:
//   - write-after-write to the SAME address, not a write-then-read pair;
//   - ASYMMETRIC sizes (large, then <= 4 KiB) -- small copies take a different
//     path than multi-MiB blits and overtake them;
//   - PINNED host sources -- a malloc'd buffer is staged, which serialises.
TEST_F(XpuTransportTest, StreamPreservesSubmissionOrder) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    cudaStream_t stream = nullptr;
    ASSERT_EQ(cudaStreamCreate(&stream), cudaSuccess);

    const size_t big = 64u << 20;  // 64 MiB
    const size_t small = 4096;     // <= 4 KiB takes the faster path
    std::vector<uint8_t> out(small, 0);

    void *first = nullptr, *second = nullptr;
    ASSERT_EQ(cudaHostAlloc(&first, big, 0), cudaSuccess);
    ASSERT_EQ(cudaHostAlloc(&second, small, 0), cudaSuccess);
    std::memset(first, 0xAA, big);
    std::memset(second, 0xBB, small);

    void *dev = nullptr;
    ASSERT_EQ(cudaMalloc(&dev, big), cudaSuccess);

    for (int iter = 0; iter < 10; ++iter) {
        // Large write of 0xAA over the whole buffer, then a small write of 0xBB
        // over its start. In order, the first `small` bytes must end up 0xBB.
        ASSERT_EQ(
            cudaMemcpyAsync(dev, first, big, cudaMemcpyHostToDevice, stream),
            cudaSuccess);
        ASSERT_EQ(
            cudaMemcpyAsync(dev, second, small, cudaMemcpyHostToDevice, stream),
            cudaSuccess);
        ASSERT_EQ(cudaStreamSynchronize(stream), cudaSuccess);
        ASSERT_EQ(cudaMemcpy(out.data(), dev, small, cudaMemcpyDeviceToHost),
                  cudaSuccess);
        ASSERT_EQ(out[0], 0xBB)
            << "iteration " << iter
            << ": the small copy was overtaken by the large one -- the stream "
               "is not in-order";
    }

    cudaFree(dev);
    cudaFreeHost(first);
    cudaFreeHost(second);
    cudaStreamDestroy(stream);
}

// Only the settled state is asserted: whether a fresh copy is still pending
// when polled is a race.
TEST_F(XpuTransportTest, StreamQueryReportsIdleAfterSync) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    cudaStream_t stream = nullptr;
    ASSERT_EQ(cudaStreamCreate(&stream), cudaSuccess);
    EXPECT_EQ(cudaStreamQuery(stream), cudaSuccess)
        << "new stream should be idle";

    const size_t size = 1 << 20;
    std::vector<uint8_t> host(size, 0x5A);
    void *dev = nullptr;
    ASSERT_EQ(cudaMalloc(&dev, size), cudaSuccess);

    ASSERT_EQ(
        cudaMemcpyAsync(dev, host.data(), size, cudaMemcpyHostToDevice, stream),
        cudaSuccess);
    // Either still running or already done -- both are legal here.
    cudaError_t q = cudaStreamQuery(stream);
    EXPECT_TRUE(q == cudaSuccess || q == cudaErrorNotReady)
        << "unexpected cudaStreamQuery result: " << q;

    ASSERT_EQ(cudaStreamSynchronize(stream), cudaSuccess);
    EXPECT_EQ(cudaStreamQuery(stream), cudaSuccess)
        << "stream still busy after cudaStreamSynchronize";

    cudaFree(dev);
    cudaStreamDestroy(stream);
}

// The null stream is synchronous: complete on return, no synchronise needed.
TEST_F(XpuTransportTest, MemcpyAsyncOnNullStreamIsSynchronous) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    const size_t size = 4096;
    std::vector<uint8_t> src(size, 0x3C), dst(size, 0);
    void *dev = nullptr;
    ASSERT_EQ(cudaMalloc(&dev, size), cudaSuccess);

    ASSERT_EQ(
        cudaMemcpyAsync(dev, src.data(), size, cudaMemcpyHostToDevice, nullptr),
        cudaSuccess);
    ASSERT_EQ(
        cudaMemcpyAsync(dst.data(), dev, size, cudaMemcpyDeviceToHost, nullptr),
        cudaSuccess);
    // Deliberately no synchronise.
    EXPECT_EQ(src, dst) << "null-stream copy was not synchronous";
    EXPECT_EQ(cudaStreamSynchronize(nullptr), cudaSuccess);
    EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);

    cudaFree(dev);
}

// Two streams must not interfere: each sees only its own writes.
TEST_F(XpuTransportTest, MultipleStreamsAreIndependent) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    cudaStream_t s1 = nullptr, s2 = nullptr;
    ASSERT_EQ(cudaStreamCreate(&s1), cudaSuccess);
    ASSERT_EQ(cudaStreamCreate(&s2), cudaSuccess);
    ASSERT_NE(s1, s2);

    const size_t size = 32 * 1024;
    std::vector<uint8_t> a(size, 0x11), b(size, 0x22);
    std::vector<uint8_t> oa(size, 0), ob(size, 0);
    void *da = nullptr, *db = nullptr;
    ASSERT_EQ(cudaMalloc(&da, size), cudaSuccess);
    ASSERT_EQ(cudaMalloc(&db, size), cudaSuccess);

    ASSERT_EQ(cudaMemcpyAsync(da, a.data(), size, cudaMemcpyHostToDevice, s1),
              cudaSuccess);
    ASSERT_EQ(cudaMemcpyAsync(db, b.data(), size, cudaMemcpyHostToDevice, s2),
              cudaSuccess);
    ASSERT_EQ(cudaMemcpyAsync(oa.data(), da, size, cudaMemcpyDeviceToHost, s1),
              cudaSuccess);
    ASSERT_EQ(cudaMemcpyAsync(ob.data(), db, size, cudaMemcpyDeviceToHost, s2),
              cudaSuccess);

    ASSERT_EQ(cudaStreamSynchronize(s1), cudaSuccess);
    ASSERT_EQ(cudaStreamSynchronize(s2), cudaSuccess);

    EXPECT_EQ(oa, a);
    EXPECT_EQ(ob, b);

    cudaFree(da);
    cudaFree(db);
    cudaStreamDestroy(s1);
    cudaStreamDestroy(s2);
}

// A stream must run on its creation device even after the calling thread
// switches, i.e. the stream carries its own device.
TEST_F(XpuTransportTest, StreamRunsOnItsCreationDevice) {
    int count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&count), cudaSuccess);
    if (count < 2) GTEST_SKIP() << "needs at least 2 Intel XPU devices";

    ASSERT_EQ(cudaSetDevice(1), cudaSuccess);
    cudaStream_t stream = nullptr;
    ASSERT_EQ(cudaStreamCreate(&stream), cudaSuccess);

    // Large on purpose. All devices share one context, so appending to the
    // wrong device's list still lands the right bytes; what this catches is a
    // synchronise on the wrong list, which returns early and leaves the
    // destination unwritten.
    const size_t size = 16u << 20;
    std::vector<uint8_t> src(size, 0x77), dst(size, 0);
    void *dev = nullptr;
    ASSERT_EQ(cudaMalloc(&dev, size), cudaSuccess);  // allocated on device 1

    // Switch the calling thread back to device 0; the stream must still work.
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(
        cudaMemcpyAsync(dev, src.data(), size, cudaMemcpyHostToDevice, stream),
        cudaSuccess);
    ASSERT_EQ(
        cudaMemcpyAsync(dst.data(), dev, size, cudaMemcpyDeviceToHost, stream),
        cudaSuccess);
    ASSERT_EQ(cudaStreamSynchronize(stream), cudaSuccess);
    EXPECT_EQ(src, dst);

    ASSERT_EQ(cudaSetDevice(1), cudaSuccess);
    cudaFree(dev);
    cudaStreamDestroy(stream);
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
}

// Destroying a stream with work queued must drain it, not tear the list out
// from under an in-flight copy.
TEST_F(XpuTransportTest, StreamDestroyDrainsInFlightWork) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    cudaStream_t stream = nullptr;
    ASSERT_EQ(cudaStreamCreate(&stream), cudaSuccess);

    const size_t size = 4 << 20;  // large enough to plausibly still be running
    std::vector<uint8_t> host(size, 0x6E);
    void *dev = nullptr;
    ASSERT_EQ(cudaMalloc(&dev, size), cudaSuccess);

    for (int i = 0; i < 8; ++i) {
        ASSERT_EQ(cudaMemcpyAsync(dev, host.data(), size,
                                  cudaMemcpyHostToDevice, stream),
                  cudaSuccess);
    }
    // No synchronise: destroy must do it for us.
    EXPECT_EQ(cudaStreamDestroy(stream), cudaSuccess);

    cudaFree(dev);
}

// cudaFree/cudaDeviceSynchronize enumerate the live streams and wait outside
// the registry lock, so a concurrent cudaStreamDestroy() must not free a list
// while it is being waited on. Streams are pinned by shared_ptr to prevent
// that; this races the two paths to give ASan/TSan something to catch if that
// regresses.
TEST_F(XpuTransportTest, StreamDestroyRacesDeviceSynchronize) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    const size_t size = 4 << 20;
    void *host = nullptr;
    ASSERT_EQ(cudaHostAlloc(&host, size, 0), cudaSuccess);
    std::memset(host, 0x3B, size);

    for (int iter = 0; iter < 20; ++iter) {
        cudaStream_t stream = nullptr;
        ASSERT_EQ(cudaStreamCreate(&stream), cudaSuccess);
        void *dev = nullptr;
        ASSERT_EQ(cudaMalloc(&dev, size), cudaSuccess);

        // Queue enough work that the drain below is still running when the
        // other thread destroys the stream.
        for (int i = 0; i < 4; ++i) {
            ASSERT_EQ(cudaMemcpyAsync(dev, host, size, cudaMemcpyHostToDevice,
                                      stream),
                      cudaSuccess);
        }

        std::thread destroyer([stream] { cudaStreamDestroy(stream); });
        EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
        destroyer.join();

        EXPECT_EQ(cudaFree(dev), cudaSuccess);
    }

    cudaFreeHost(host);
}

// Destroying the same stream twice, and destroying a null stream, must be safe.
TEST_F(XpuTransportTest, StreamDestroyIsIdempotentAndNullSafe) {
    EXPECT_EQ(cudaStreamDestroy(nullptr), cudaSuccess);

    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    cudaStream_t stream = nullptr;
    ASSERT_EQ(cudaStreamCreate(&stream), cudaSuccess);
    EXPECT_EQ(cudaStreamDestroy(stream), cudaSuccess);
    // The handle is no longer registered, so a repeat call is a no-op rather
    // than a double free.
    EXPECT_EQ(cudaStreamDestroy(stream), cudaSuccess);
}

// Test multiple allocations and frees
TEST_F(XpuTransportTest, MultipleAllocFree) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    constexpr int N = 16;
    void *ptrs[N] = {};

    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(cudaMalloc(&ptrs[i], 4096 * (i + 1)), cudaSuccess);
        ASSERT_NE(ptrs[i], nullptr);
    }

    // All should be device memory
    for (int i = 0; i < N; ++i) {
        EXPECT_TRUE(mooncake::xpu::isDeviceMemory(ptrs[i]));
    }

    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(cudaFree(ptrs[i]), cudaSuccess);
    }
}

// Test GPU_PREFIX is set correctly (no GPU needed — compile-time constant)
TEST(XpuCompileTest, GpuPrefix) {
    std::string prefix = GPU_PREFIX;
    EXPECT_EQ(prefix, "xpu:")
        << "GPU_PREFIX should be 'xpu:' when USE_XPU is defined";
}
