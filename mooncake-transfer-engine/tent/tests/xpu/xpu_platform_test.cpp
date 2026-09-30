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

// Acceptance test for the Intel XPU platform. Drives XpuPlatform against the
// real oneAPI SYCL backend (USE_XPU is a direct-link build: libsycl is linked
// in, there is no mock adapter). The backend falls back to the OpenCL CPU
// runtime when no Intel GPU is present, so this test runs on any host with a
// SYCL runtime; when no SYCL device is visible at all it skips rather than
// fails.

#include <gtest/gtest.h>
#include <fcntl.h>
#include <sycl/sycl.hpp>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "tent/common/config.h"
#include "tent/platform/xpu.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
namespace {

class XpuPlatformTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto conf = std::make_shared<Config>();
        platform_ = std::make_shared<XpuPlatform>(conf);

        // Count visible XPU devices via probe(); skip the whole suite when the
        // host has no SYCL device (e.g. no oneAPI runtime installed).
        std::vector<Topology::NicEntry> nics;
        std::vector<Topology::MemEntry> mems;
        ASSERT_TRUE(platform_->probe(nics, mems).ok());
        for (const auto &m : mems) {
            if (m.name.rfind("xpu:", 0) == 0) device_count_++;
        }
        if (device_count_ == 0) {
            GTEST_SKIP() << "no SYCL device available; skipping XPU test";
        }
    }

    std::shared_ptr<XpuPlatform> platform_;
    int device_count_ = 0;
};

TEST_F(XpuPlatformTest, TypeString) { EXPECT_EQ(platform_->type(), "xpu"); }

TEST_F(XpuPlatformTest, ProbeRegistersOneMemEntryPerDevice) {
    std::vector<Topology::NicEntry> nics;
    std::vector<Topology::MemEntry> mems;
    Status s = platform_->probe(nics, mems);
    EXPECT_TRUE(s.ok()) << s;

    int xpu_entries = 0;
    for (const auto &m : mems) {
        if (m.name.rfind("xpu:", 0) == 0) xpu_entries++;
    }
    EXPECT_EQ(xpu_entries, device_count_)
        << "probe() should register one node per device";
}

// The oneAPI runtime exposes each Intel GPU through both Level Zero and
// OpenCL; probe() must advertise each physical GPU once, not once per adapter.
TEST_F(XpuPlatformTest, ProbeDoesNotDuplicateGpusAcrossAdapters) {
    int level_zero_gpus = 0;
    for (const auto &d :
         sycl::device::get_devices(sycl::info::device_type::gpu)) {
        if (d.get_backend() == sycl::backend::ext_oneapi_level_zero)
            level_zero_gpus++;
    }
    if (level_zero_gpus == 0) GTEST_SKIP() << "no Level Zero GPU";
    EXPECT_EQ(device_count_, level_zero_gpus)
        << "OpenCL views of the same GPUs must not be advertised as extra "
           "xpu:N nodes";
}

// Acceptance: device allocate -> host->device copy -> device->host copy ->
// byte-equality -> free, all driven through XpuPlatform.
TEST_F(XpuPlatformTest, DeviceAllocCopyFreeRoundTrip) {
    const size_t kSize = 4096;
    MemoryOptions options;
    options.location = "xpu:0";

    void *dev = nullptr;
    Status alloc = platform_->allocate(&dev, kSize, options);
    ASSERT_TRUE(alloc.ok()) << alloc;
    ASSERT_NE(dev, nullptr);

    // classify() == MTYPE_XPU for device USM.
    EXPECT_EQ(platform_->getMemoryType(dev), MTYPE_XPU);

    // getLocation resolves the owning device ordinal.
    auto locs = platform_->getLocation(dev, kSize);
    ASSERT_EQ(locs.size(), 1u);
    EXPECT_EQ(locs[0].location, "xpu:0");

    // host -> device -> host round trip.
    std::vector<uint8_t> src(kSize);
    for (size_t i = 0; i < kSize; ++i) src[i] = static_cast<uint8_t>(i * 7 + 1);
    std::vector<uint8_t> dst(kSize, 0);

    Status h2d = platform_->copy(dev, src.data(), kSize);
    ASSERT_TRUE(h2d.ok()) << h2d;
    Status d2h = platform_->copy(dst.data(), dev, kSize);
    ASSERT_TRUE(d2h.ok()) << d2h;

    EXPECT_EQ(std::memcmp(src.data(), dst.data(), kSize), 0);

    Status freed = platform_->free(dev, kSize);
    EXPECT_TRUE(freed.ok()) << freed;
    // Whether the freed address still classifies as device memory is up to the
    // SYCL runtime (Level Zero keeps freed USM cached and get_pointer_type
    // still reports it as device), so that is deliberately not asserted.
}

TEST_F(XpuPlatformTest, HostPointerClassifiesAsCpu) {
    int host_value = 0;
    EXPECT_EQ(platform_->getMemoryType(&host_value), MTYPE_CPU);
}

// USM device memory the platform did not allocate itself -- the way a PyTorch
// XPU tensor reaches registerLocalMemory -- must still classify as XPU memory
// and be copyable, including at an interior offset. PyTorch allocates from the
// platform default context, so mirror that here.
TEST_F(XpuPlatformTest, ForeignUsmClassifiesAsXpuAndCopies) {
    sycl::device gpu;
    bool found = false;
    for (const auto &d : sycl::device::get_devices()) {
        if (d.is_gpu() && d.has(sycl::aspect::usm_device_allocations)) {
            gpu = d;
            found = true;
            break;
        }
    }
    if (!found) GTEST_SKIP() << "no SYCL GPU device available";

    sycl::context ctx = gpu.get_platform().ext_oneapi_get_default_context();
    sycl::queue q(ctx, gpu);
    const size_t kSize = 8192;
    auto *dev = static_cast<uint8_t *>(sycl::malloc_device(kSize, gpu, ctx));
    ASSERT_NE(dev, nullptr);

    EXPECT_EQ(platform_->getMemoryType(dev), MTYPE_XPU);
    EXPECT_EQ(platform_->getMemoryType(dev + 4096), MTYPE_XPU);
    auto locs = platform_->getLocation(dev, kSize);
    ASSERT_EQ(locs.size(), 1u);
    EXPECT_EQ(locs[0].location.rfind("xpu:", 0), 0u) << locs[0].location;

    std::vector<uint8_t> src(4096), dst(4096, 0);
    for (size_t i = 0; i < src.size(); ++i)
        src[i] = static_cast<uint8_t>(i * 11 + 3);
    Status h2d = platform_->copy(dev + 4096, src.data(), src.size());
    ASSERT_TRUE(h2d.ok()) << h2d;
    Status d2h = platform_->copy(dst.data(), dev + 4096, dst.size());
    ASSERT_TRUE(d2h.ok()) << d2h;
    EXPECT_EQ(src, dst);

    // Cross-check against the owner's own queue: the platform really wrote
    // into this allocation, not a stage.
    std::vector<uint8_t> direct(4096, 0);
    q.memcpy(direct.data(), dev + 4096, direct.size()).wait();
    EXPECT_EQ(src, direct);

    // Host and shared USM are host-accessible and stay classified as CPU.
    void *host_usm = sycl::malloc_host(4096, ctx);
    ASSERT_NE(host_usm, nullptr);
    EXPECT_EQ(platform_->getMemoryType(host_usm), MTYPE_CPU);
    sycl::free(host_usm, ctx);

    sycl::free(dev, ctx);
}

// Interior addresses (chunked staging hands the backend base + offset) must
// classify as device memory, or transfers larger than one chunk corrupt.
TEST_F(XpuPlatformTest, InteriorPointerClassifiesAsXpu) {
    const size_t kSize = 8192;
    MemoryOptions options;
    options.location = "xpu:0";

    void *dev = nullptr;
    ASSERT_TRUE(platform_->allocate(&dev, kSize, options).ok());
    auto *bytes = static_cast<uint8_t *>(dev);

    EXPECT_EQ(platform_->getMemoryType(bytes + 4096), MTYPE_XPU);
    auto locs = platform_->getLocation(bytes + 4096, kSize - 4096);
    ASSERT_EQ(locs.size(), 1u);
    EXPECT_EQ(locs[0].location, "xpu:0");

    // Interior-offset staging copy must round-trip correctly.
    std::vector<uint8_t> src(4096), dst(4096, 0);
    for (size_t i = 0; i < src.size(); ++i)
        src[i] = static_cast<uint8_t>(i * 13 + 5);
    ASSERT_TRUE(platform_->copy(bytes + 4096, src.data(), 4096).ok());
    ASSERT_TRUE(platform_->copy(dst.data(), bytes + 4096, 4096).ok());
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 4096), 0);

    EXPECT_TRUE(platform_->free(dev, kSize).ok());
}

// A bare "xpu" location (no ordinal) must allocate on device 0, not fall
// through to the host allocator.
TEST_F(XpuPlatformTest, BareXpuLocationAllocatesDeviceZero) {
    MemoryOptions options;
    options.location = "xpu";

    void *dev = nullptr;
    Status alloc = platform_->allocate(&dev, 4096, options);
    ASSERT_TRUE(alloc.ok()) << alloc;
    ASSERT_NE(dev, nullptr);
    EXPECT_EQ(platform_->getMemoryType(dev), MTYPE_XPU);
    EXPECT_TRUE(platform_->free(dev, 4096).ok());
}

// A malformed ordinal ("xpu:<garbage>") must be rejected rather than silently
// allocating on device 0.
TEST_F(XpuPlatformTest, MalformedXpuOrdinalIsRejected) {
    MemoryOptions options;
    options.location = "xpu:notanumber";

    void *dev = nullptr;
    Status alloc = platform_->allocate(&dev, 4096, options);
    EXPECT_FALSE(alloc.ok());
    EXPECT_EQ(dev, nullptr);
}

// Host memory is never exported: the RDMA layer registers it with ibv_reg_mr
// instead, and relies on InvalidArgument (not a hard failure) to get there.
TEST_F(XpuPlatformTest, HostPointerIsNotExportable) {
    int host_value = 0;
    DmabufExport out;
    Status s = platform_->exportDmabuf(&host_value, sizeof(host_value), out);
    EXPECT_TRUE(s.IsInvalidArgument()) << s;
    EXPECT_EQ(out.fd, -1);
}

// Device USM exports as a dma-buf covering the whole allocation, so an
// interior pointer maps to a non-zero offset and the fd is a live descriptor.
// Level Zero owns that fd (it caches one per allocation and returns the same
// number on every export), so the test must not close it. Skips (rather than
// fails) on SYCL backends without Level Zero external-memory export, e.g. the
// OpenCL CPU fallback device.
TEST_F(XpuPlatformTest, DeviceAllocationExportsAsDmabuf) {
    const size_t kSize = 1 << 20;
    MemoryOptions options;
    options.location = "xpu:0";
    void *dev = nullptr;
    ASSERT_TRUE(platform_->allocate(&dev, kSize, options).ok());

    DmabufExport base;
    Status s = platform_->exportDmabuf(dev, kSize, base);
    if (s.IsInternalError()) {
        EXPECT_TRUE(platform_->free(dev, kSize).ok());
        GTEST_SKIP() << "SYCL backend has no dma-buf export: " << s;
    }
    ASSERT_TRUE(s.ok()) << s;
    EXPECT_GE(base.fd, 0);
    EXPECT_EQ(base.offset, 0u);
    EXPECT_NE(fcntl(base.fd, F_GETFD), -1);

    const size_t kInterior = 4096 * 3;
    DmabufExport interior;
    s = platform_->exportDmabuf(static_cast<char *>(dev) + kInterior,
                                kSize - kInterior, interior);
    ASSERT_TRUE(s.ok()) << s;
    EXPECT_GE(interior.fd, 0);
    EXPECT_EQ(interior.offset, kInterior);
    // Both exports still refer to live descriptors; nothing was closed.
    EXPECT_NE(fcntl(base.fd, F_GETFD), -1);
    EXPECT_NE(fcntl(interior.fd, F_GETFD), -1);

    EXPECT_TRUE(platform_->free(dev, kSize).ok());
}

// Small allocations made through the platform must not share a dma-buf. The
// Intel compute runtime pools small USM device allocations into 2 MB / 16 MB
// buffers; a pooled allocation exports the pool's fd with no way to recover
// its offset inside, so the backend requests export at allocation time to
// keep its own allocations out of the pool. Two 1 MB buffers therefore export
// two distinct dma-bufs, each sized to its own allocation.
TEST_F(XpuPlatformTest, SeparateAllocationsExportDistinctDmabufs) {
    const size_t kSize = 1 << 20;
    MemoryOptions options;
    options.location = "xpu:0";
    void *a = nullptr;
    void *b = nullptr;
    ASSERT_TRUE(platform_->allocate(&a, kSize, options).ok());
    ASSERT_TRUE(platform_->allocate(&b, kSize, options).ok());

    DmabufExport ea, eb;
    Status s = platform_->exportDmabuf(a, kSize, ea);
    if (s.IsInternalError()) {
        EXPECT_TRUE(platform_->free(a, kSize).ok());
        EXPECT_TRUE(platform_->free(b, kSize).ok());
        GTEST_SKIP() << "SYCL backend has no dma-buf export: " << s;
    }
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_TRUE(platform_->exportDmabuf(b, kSize, eb).ok());
    EXPECT_EQ(ea.offset, 0u);
    EXPECT_EQ(eb.offset, 0u);
    EXPECT_NE(ea.fd, eb.fd);
    EXPECT_EQ(lseek(ea.fd, 0, SEEK_END), static_cast<off_t>(kSize));
    EXPECT_EQ(lseek(eb.fd, 0, SEEK_END), static_cast<off_t>(kSize));

    EXPECT_TRUE(platform_->free(a, kSize).ok());
    EXPECT_TRUE(platform_->free(b, kSize).ok());
}

// Device-to-device copy through copy(): seed device A from the host, copy
// A -> B on the device side, read B back and compare. Both allocations are
// interior-offset by a chunk so the copy exercises interior-pointer
// resolution as well. `dst_location` selects the peer device; "xpu:0" is the
// same-device case.
void deviceToDeviceRoundTrip(XpuPlatform &platform, const char *dst_location) {
    const size_t kSize = (8UL << 20) + 4096;  // crosses the host-bounce chunk
    const size_t kOffset = 4096;
    MemoryOptions a_opts, b_opts;
    a_opts.location = "xpu:0";
    b_opts.location = dst_location;
    void *a = nullptr, *b = nullptr;
    ASSERT_TRUE(platform.allocate(&a, kSize + kOffset, a_opts).ok());
    ASSERT_TRUE(platform.allocate(&b, kSize + kOffset, b_opts).ok());
    auto *a_in = static_cast<uint8_t *>(a) + kOffset;
    auto *b_in = static_cast<uint8_t *>(b) + kOffset;

    std::vector<uint8_t> seed(kSize), zero(kSize, 0), readback(kSize, 0xAA);
    for (size_t i = 0; i < kSize; ++i) seed[i] = static_cast<uint8_t>(i % 251);
    ASSERT_TRUE(platform.copy(a_in, seed.data(), kSize).ok());
    ASSERT_TRUE(platform.copy(b_in, zero.data(), kSize).ok());

    Status d2d = platform.copy(b_in, a_in, kSize);
    ASSERT_TRUE(d2d.ok()) << d2d;

    ASSERT_TRUE(platform.copy(readback.data(), b_in, kSize).ok());
    EXPECT_EQ(readback, seed);

    EXPECT_TRUE(platform.free(a, kSize + kOffset).ok());
    EXPECT_TRUE(platform.free(b, kSize + kOffset).ok());
}

TEST_F(XpuPlatformTest, DeviceToDeviceCopyOnSameDevice) {
    deviceToDeviceRoundTrip(*platform_, "xpu:0");
}

// PCIe P2P acceptance: with two XPUs, a VRAM->VRAM copy between them is
// correct whether the driver grants peer access (direct copy) or not (host
// bounce inside the backend).
TEST_F(XpuPlatformTest, DeviceToDeviceCopyAcrossDevices) {
    if (device_count_ < 2) GTEST_SKIP() << "needs two XPU devices";
    deviceToDeviceRoundTrip(*platform_, "xpu:1");
}

// Cross-process sharing primitives, exercised within one process: exportIpc
// describes the allocation (Level Zero IPC handle, base, size), importIpc
// maps that handle as device memory the given local device can address, and
// copies through the mapping land in the original allocation. `import_device`
// selects where the mapping is opened; "1" makes device 1 reach device 0's
// VRAM over PCIe P2P.
void ipcRoundTrip(XpuPlatform &platform, int import_device) {
    const size_t kSize = (2UL << 20) + 4096;
    const size_t kOffset = 4096;
    MemoryOptions opts;
    opts.location = "xpu:0";
    void *dev = nullptr;
    ASSERT_TRUE(platform.allocate(&dev, kSize, opts).ok());

    XpuPlatform::IpcExport exported;
    Status s = platform.exportIpc(static_cast<char *>(dev) + kOffset,
                                  kSize - kOffset, exported);
    if (s.IsInternalError()) {
        EXPECT_TRUE(platform.free(dev, kSize).ok());
        GTEST_SKIP() << "SYCL backend has no Level Zero IPC export: " << s;
    }
    ASSERT_TRUE(s.ok()) << s;
    const XpuPlatform::IpcHandle empty{};
    EXPECT_NE(exported.handle, empty);
    EXPECT_EQ(exported.base, reinterpret_cast<uint64_t>(dev));
    EXPECT_EQ(exported.size, kSize);
    // A range running past the allocation is refused.
    XpuPlatform::IpcExport overflow;
    EXPECT_TRUE(
        platform.exportIpc(dev, kSize + 1, overflow).IsInvalidArgument());

    void *mapped = nullptr;
    s = platform.importIpc(exported.handle, getpid(), exported.size,
                           import_device, &mapped);
    if (!s.ok()) {
        EXPECT_TRUE(platform.free(dev, kSize).ok());
        GTEST_SKIP() << "Level Zero IPC import unavailable: " << s;
    }
    ASSERT_NE(mapped, nullptr);
    EXPECT_EQ(platform.getMemoryType(mapped), MTYPE_XPU);
    EXPECT_EQ(platform.deviceIndex(mapped), import_device);
    EXPECT_EQ(platform.getMemoryType(static_cast<char *>(mapped) + kOffset),
              MTYPE_XPU);

    // Host -> mapping (interior offset), read back through the original.
    const size_t kLen = kSize - kOffset;
    std::vector<uint8_t> seed(kLen), zero(kSize, 0), readback(kLen, 0xAA);
    for (size_t i = 0; i < kLen; ++i) seed[i] = static_cast<uint8_t>(i % 241);
    ASSERT_TRUE(platform.copy(dev, zero.data(), kSize).ok());
    Status w =
        platform.copy(static_cast<char *>(mapped) + kOffset, seed.data(), kLen);
    ASSERT_TRUE(w.ok()) << w;
    ASSERT_TRUE(
        platform.copy(readback.data(), static_cast<char *>(dev) + kOffset, kLen)
            .ok());
    EXPECT_EQ(readback, seed);

    // Original -> host through the mapping (device -> host read side).
    std::fill(readback.begin(), readback.end(), 0);
    Status r = platform.copy(readback.data(),
                             static_cast<char *>(mapped) + kOffset, kLen);
    ASSERT_TRUE(r.ok()) << r;
    EXPECT_EQ(readback, seed);

    // Device -> mapping: a VRAM<->VRAM copy whose destination is imported.
    void *other = nullptr;
    ASSERT_TRUE(platform.allocate(&other, kLen, opts).ok());
    for (auto &b : seed) b = static_cast<uint8_t>(b ^ 0x5A);
    ASSERT_TRUE(platform.copy(other, seed.data(), kLen).ok());
    Status d2d =
        platform.copy(static_cast<char *>(mapped) + kOffset, other, kLen);
    ASSERT_TRUE(d2d.ok()) << d2d;
    ASSERT_TRUE(
        platform.copy(readback.data(), static_cast<char *>(dev) + kOffset, kLen)
            .ok());
    EXPECT_EQ(readback, seed);

    EXPECT_TRUE(platform.closeImport(mapped).ok());
    EXPECT_EQ(platform.getMemoryType(mapped), MTYPE_CPU);
    EXPECT_TRUE(platform.closeImport(mapped).IsInternalError());
    EXPECT_TRUE(platform.free(other, kLen).ok());
    EXPECT_TRUE(platform.free(dev, kSize).ok());
}

TEST_F(XpuPlatformTest, IpcImportOnSameDevice) { ipcRoundTrip(*platform_, 0); }

TEST_F(XpuPlatformTest, IpcImportOnPeerDevice) {
    if (device_count_ < 2) GTEST_SKIP() << "needs two XPU devices";
    ipcRoundTrip(*platform_, 1);
}

TEST_F(XpuPlatformTest, HostPointerIsNotIpcExportable) {
    int host_value = 0;
    XpuPlatform::IpcExport out;
    EXPECT_TRUE(platform_->exportIpc(&host_value, sizeof(host_value), out)
                    .IsInvalidArgument());
    EXPECT_EQ(out.handle, XpuPlatform::IpcHandle{});
    EXPECT_EQ(out.size, 0u);
}

TEST_F(XpuPlatformTest, ImportRejectsBadArguments) {
    void *mapped = nullptr;
    const XpuPlatform::IpcHandle handle{};
    EXPECT_FALSE(platform_->importIpc(handle, 0, 4096, 0, &mapped).ok());
    EXPECT_FALSE(platform_->importIpc(handle, getpid(), 0, 0, &mapped).ok());
    EXPECT_FALSE(
        platform_->importIpc(handle, getpid(), 4096, device_count_, &mapped)
            .ok());
    EXPECT_EQ(mapped, nullptr);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
