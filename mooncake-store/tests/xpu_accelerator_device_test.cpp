// Tests for the Intel XPU (SYCL) accelerator device used by the Store client:
// concurrent pointer classification and copies, and the MemcpyWorkerPool
// path that runs many small copies in one task.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <sycl/sycl.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <optional>
#include <thread>
#include <vector>

#include "device/accelerator_device.h"
#include "device/accelerator_registry.h"
#include "device/xpu_accelerator_device_internal.h"
#include "transfer_task.h"
#include "types.h"

namespace mooncake {
namespace {

using device::AcceleratorVendor;
using device::CopyDirection;
using device::MemoryKind;
using device::xpu::testing::CopyFault;
using device::xpu::testing::InjectCopyFault;

// The GPUs the XPU device uses, in its own order: the Level Zero platform's
// GPUs that support USM device allocations, on that platform's default
// context (the context PyTorch XPU allocates from).
struct XpuGpus {
    std::optional<sycl::context> context;
    std::vector<sycl::device> devices;
};

XpuGpus FindGpus() {
    XpuGpus gpus;
    for (const auto& platform : sycl::platform::get_platforms()) {
        if (platform.get_backend() != sycl::backend::ext_oneapi_level_zero)
            continue;
        for (const auto& d : platform.get_devices()) {
            if (d.is_gpu() && d.has(sycl::aspect::usm_device_allocations))
                gpus.devices.push_back(d);
        }
        if (!gpus.devices.empty()) {
            gpus.context = platform.ext_oneapi_get_default_context();
            break;
        }
    }
    return gpus;
}

uint8_t PatternByte(size_t seed, size_t i) {
    return static_cast<uint8_t>((seed * 131 + i * 7 + 3) & 0xff);
}

void FillPattern(uint8_t* p, size_t n, size_t seed) {
    for (size_t i = 0; i < n; ++i) p[i] = PatternByte(seed, i);
}

bool HasPattern(const uint8_t* p, size_t n, size_t seed) {
    for (size_t i = 0; i < n; ++i)
        if (p[i] != PatternByte(seed, i)) return false;
    return true;
}

class XpuAcceleratorDeviceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        gpus_ = FindGpus();
        if (gpus_.devices.empty()) GTEST_SKIP() << "no Level Zero GPU";
        accel_ = device::GetAcceleratorRegistry().GetDevice(
            AcceleratorVendor::kIntel);
        ASSERT_NE(accel_, nullptr);
        ASSERT_TRUE(accel_->Available(true));
    }

    void* DeviceAlloc(size_t bytes, size_t dev) {
        void* p =
            sycl::malloc_device(bytes, gpus_.devices[dev], *gpus_.context);
        EXPECT_NE(p, nullptr);
        allocations_.push_back(p);
        return p;
    }

    void TearDown() override {
        InjectCopyFault(CopyFault::kNone);
        for (void* p : allocations_) sycl::free(p, *gpus_.context);
    }

    // Run one MemcpyWorkerPool task and return its result.
    ErrorCode RunTask(MemcpyWorkerPool& pool,
                      std::vector<MemcpyOperation> ops) {
        auto state = std::make_shared<MemcpyOperationState>();
        pool.submitTask(MemcpyTask(std::move(ops), state));
        state->wait_for_completion();
        return state->get_result();
    }

    XpuGpus gpus_;
    const device::AcceleratorDevice* accel_ = nullptr;
    std::vector<void*> allocations_;
};

// (a) Many threads classify and copy at once; every classification and every
// byte must be right.
TEST_F(XpuAcceleratorDeviceTest, ConcurrentQueryPointerAndCopy) {
    constexpr int kThreads = 16;
    constexpr int kIters = 50;
    constexpr size_t kBytes = 64 * 1024;
    const size_t ndev = gpus_.devices.size();

    std::vector<void*> dev_bufs(kThreads);
    std::vector<void*> usm_host_bufs(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        dev_bufs[t] = DeviceAlloc(kBytes, t % ndev);
        usm_host_bufs[t] = sycl::malloc_host(kBytes, *gpus_.context);
        ASSERT_NE(usm_host_bufs[t], nullptr);
    }

    std::atomic<int> bad_kind{0}, bad_index{0}, bad_copy{0}, bad_data{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            std::vector<uint8_t> src(kBytes), dst(kBytes);
            auto* usm_host = static_cast<uint8_t*>(usm_host_bufs[t]);
            const int expected_dev = static_cast<int>(t % ndev);
            for (int it = 0; it < kIters; ++it) {
                const size_t seed = t * 1000 + it;
                auto dinfo = accel_->QueryPointer(
                    static_cast<uint8_t*>(dev_bufs[t]) + it);
                if (dinfo.kind != MemoryKind::kDevice) ++bad_kind;
                if (dinfo.device_id != expected_dev) ++bad_index;
                if (accel_->QueryPointer(src.data()).kind != MemoryKind::kHost)
                    ++bad_kind;
                if (accel_->QueryPointer(usm_host).kind != MemoryKind::kHost)
                    ++bad_kind;

                FillPattern(src.data(), kBytes, seed);
                accel_->SetContext(dinfo.device_id);
                if (!accel_->Copy(dev_bufs[t], src.data(), kBytes,
                                  CopyDirection::kHostToDevice) ||
                    !accel_->Copy(usm_host, dev_bufs[t], kBytes,
                                  CopyDirection::kDeviceToHost) ||
                    !accel_->Copy(dst.data(), usm_host, kBytes,
                                  CopyDirection::kHostToHost)) {
                    ++bad_copy;
                    continue;
                }
                if (!HasPattern(dst.data(), kBytes, seed)) ++bad_data;
            }
        });
    }
    for (auto& th : threads) th.join();
    for (void* p : usm_host_bufs) sycl::free(p, *gpus_.context);

    EXPECT_EQ(bad_kind.load(), 0);
    EXPECT_EQ(bad_index.load(), 0);
    EXPECT_EQ(bad_copy.load(), 0);
    EXPECT_EQ(bad_data.load(), 0);
}

// Perf: many threads classifying device pointers at once.
TEST_F(XpuAcceleratorDeviceTest, ConcurrentQueryPointerThroughput) {
    constexpr int kThreads = 16;
    constexpr int kCalls = 20000;
    void* dev = DeviceAlloc(4096, 0);
    std::atomic<int> wrong{0};
    // Warm up the runtime outside the timed region.
    accel_->QueryPointer(dev);

    auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < kCalls; ++i) {
                if (accel_->QueryPointer(dev).kind != MemoryKind::kDevice)
                    ++wrong;
            }
        });
    }
    for (auto& th : threads) th.join();
    const double secs =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    EXPECT_EQ(wrong.load(), 0);
    const double total = static_cast<double>(kThreads) * kCalls;
    std::cout << "[perf] QueryPointer " << kThreads << " threads x " << kCalls
              << " calls: " << secs * 1e3 << " ms total, "
              << (total / secs / 1e6) << " M calls/s" << std::endl;
}

// (b) One task with many small mixed H2D and D2H ops completes with the
// right bytes. Run with pageable host memory and with pinned USM host memory
// (what AllocatePinnedHost hands out); only the latter copies asynchronously.
class XpuManySmallOpsTest : public XpuAcceleratorDeviceTest,
                            public ::testing::WithParamInterface<bool> {};

TEST_P(XpuManySmallOpsTest, ManySmallOpsInOneTask) {
    const bool pinned = GetParam();
    constexpr size_t kOps = 1024;
    constexpr size_t kSlice = 4096;
    constexpr size_t kBytes = kOps * kSlice;
    constexpr int kReps = 20;
    auto* dev = static_cast<uint8_t*>(DeviceAlloc(kBytes, 0));
    std::vector<uint8_t> pageable;
    uint8_t* host = nullptr;
    if (pinned) {
        host = static_cast<uint8_t*>(sycl::malloc_host(kBytes, *gpus_.context));
        ASSERT_NE(host, nullptr);
    } else {
        pageable.resize(kBytes);
        host = pageable.data();
    }
    std::vector<uint8_t> seed_buf(kBytes), readback(kBytes);
    sycl::queue q(*gpus_.context, gpus_.devices[0]);
    MemcpyWorkerPool pool;

    std::vector<double> times;
    for (int rep = 0; rep < kReps; ++rep) {
        // Even slots are written H2D from host[slot]; odd slots are seeded
        // on the device and read back D2H into host[slot]. No two ops touch
        // the same bytes.
        for (size_t i = 0; i < kOps; ++i) {
            const size_t seed = rep * kOps + i;
            if (i % 2 == 0) {
                FillPattern(&host[i * kSlice], kSlice, seed);
            } else {
                FillPattern(&seed_buf[i * kSlice], kSlice, seed);
                std::memset(&host[i * kSlice], 0, kSlice);
            }
        }
        q.memcpy(dev, seed_buf.data(), kBytes).wait();

        std::vector<MemcpyOperation> ops;
        ops.reserve(kOps);
        for (size_t i = 0; i < kOps; ++i) {
            if (i % 2 == 0)
                ops.emplace_back(dev + i * kSlice, &host[i * kSlice], kSlice);
            else
                ops.emplace_back(&host[i * kSlice], dev + i * kSlice, kSlice);
        }
        auto start = std::chrono::steady_clock::now();
        ASSERT_EQ(RunTask(pool, std::move(ops)), ErrorCode::OK);
        times.push_back(std::chrono::duration<double, std::milli>(
                            std::chrono::steady_clock::now() - start)
                            .count());

        q.memcpy(readback.data(), dev, kBytes).wait();
        for (size_t i = 0; i < kOps; ++i) {
            const size_t seed = rep * kOps + i;
            const uint8_t* got =
                i % 2 == 0 ? &readback[i * kSlice] : &host[i * kSlice];
            ASSERT_TRUE(HasPattern(got, kSlice, seed))
                << (i % 2 == 0 ? "H2D" : "D2H") << " slot " << i;
        }
    }
    if (pinned) sycl::free(host, *gpus_.context);
    std::sort(times.begin(), times.end());
    std::cout << "[perf] task of " << kOps << " x " << kSlice
              << " B mixed H2D/D2H, " << (pinned ? "pinned" : "pageable")
              << " host: median " << times[times.size() / 2] << " ms, best "
              << times.front() << " ms (" << kReps << " runs)" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(HostMemory, XpuManySmallOpsTest, ::testing::Bool(),
                         [](const auto& info) {
                             return info.param ? "Pinned" : "Pageable";
                         });

// Ops that touch the same bytes are not batched: a chain that writes a
// device slot and reads it straight back must see each write.
TEST_F(XpuAcceleratorDeviceTest, OverlappingOpsRunInOrder) {
    constexpr size_t kSlice = 1 << 20;
    constexpr size_t kLinks = 16;
    auto* dev = static_cast<uint8_t*>(DeviceAlloc(kSlice, 0));
    auto* host = static_cast<uint8_t*>(
        sycl::malloc_host((kLinks + 1) * kSlice, *gpus_.context));
    ASSERT_NE(host, nullptr);
    FillPattern(host, kSlice, 4242);
    std::memset(host + kSlice, 0, kLinks * kSlice);
    // host[0] -> dev -> host[1] -> dev -> host[2] ...
    std::vector<MemcpyOperation> ops;
    for (size_t i = 0; i < kLinks; ++i) {
        ops.emplace_back(dev, host + i * kSlice, kSlice);
        ops.emplace_back(host + (i + 1) * kSlice, dev, kSlice);
    }
    MemcpyWorkerPool pool;
    EXPECT_EQ(RunTask(pool, std::move(ops)), ErrorCode::OK);
    for (size_t i = 1; i <= kLinks; ++i)
        EXPECT_TRUE(HasPattern(host + i * kSlice, kSlice, 4242))
            << "link " << i;
    sycl::free(host, *gpus_.context);
}

// (c) A batch whose middle op fails reports failure, and only after every
// copy it had already started has finished: the caller may free the buffers
// as soon as the task completes.
TEST_F(XpuAcceleratorDeviceTest, FailedBatchWaitsForStartedCopies) {
    constexpr size_t kOps = 64;
    constexpr size_t kFailAt = 32;
    constexpr size_t kSlice = 4 << 20;
    auto* dev = static_cast<uint8_t*>(DeviceAlloc(kOps * kSlice, 0));
    std::vector<uint8_t> seed(kOps * kSlice);
    for (size_t i = 0; i < kOps; ++i)
        FillPattern(&seed[i * kSlice], kSlice, 7000 + i);
    sycl::queue q(*gpus_.context, gpus_.devices[0]);
    q.memcpy(dev, seed.data(), seed.size()).wait();

    std::vector<uint8_t> host(kOps * kSlice, 0);
    std::vector<MemcpyOperation> ops;
    for (size_t i = 0; i < kOps; ++i)
        ops.emplace_back(&host[i * kSlice], dev + i * kSlice, kSlice);

    MemcpyWorkerPool pool;
    InjectCopyFault(CopyFault::kSyclError, kFailAt);
    EXPECT_EQ(RunTask(pool, std::move(ops)), ErrorCode::TRANSFER_FAIL);
    // Checked at once: a copy still running would leave bytes unwritten.
    for (size_t i = 0; i < kFailAt; ++i) {
        ASSERT_TRUE(HasPattern(&host[i * kSlice], kSlice, 7000 + i))
            << "op " << i << " was not finished when the task completed";
    }
}

// Gap 4: a non-SYCL exception from a copy is a failed copy, not an escape.
TEST_F(XpuAcceleratorDeviceTest, CopyFaultsReturnFailure) {
    constexpr size_t kBytes = 4096;
    void* dev = DeviceAlloc(kBytes, 0);
    std::vector<uint8_t> host(kBytes, 1);
    accel_->SetContext(0);
    for (CopyFault fault : {CopyFault::kBadAlloc, CopyFault::kSyclError}) {
        InjectCopyFault(fault);
        bool ok = true;
        EXPECT_NO_THROW(ok = accel_->Copy(dev, host.data(), kBytes,
                                          CopyDirection::kHostToDevice));
        EXPECT_FALSE(ok);

        InjectCopyFault(fault);
        EXPECT_NO_THROW(ok = accel_->CopyAsync(dev, host.data(), kBytes,
                                               CopyDirection::kHostToDevice));
        EXPECT_FALSE(ok);
        EXPECT_TRUE(accel_->Synchronize());
    }
    // The fault fired once; later copies work again.
    EXPECT_TRUE(
        accel_->Copy(dev, host.data(), kBytes, CopyDirection::kHostToDevice));
}

// Gap 4: a bad pointer is reported as a failed copy, not thrown.
TEST_F(XpuAcceleratorDeviceTest, CopyWithNullPointerFails) {
    void* dev = DeviceAlloc(4096, 0);
    accel_->SetContext(0);
    bool ok = true;
    EXPECT_NO_THROW(
        ok = accel_->Copy(dev, nullptr, 4096, CopyDirection::kHostToDevice));
    EXPECT_FALSE(ok);
}

// Gap 5: a context with no queue (a pointer on an unenumerated GPU) makes
// the copy fail instead of running on another device.
TEST_F(XpuAcceleratorDeviceTest, CopyWithoutDeviceQueueFails) {
    void* dev = DeviceAlloc(4096, 0);
    std::vector<uint8_t> host(4096);
    accel_->SetContext(-1);
    EXPECT_FALSE(accel_->Copy(host.data(), dev, host.size(),
                              CopyDirection::kDeviceToHost));
    accel_->SetContext(static_cast<int32_t>(gpus_.devices.size()));
    EXPECT_FALSE(accel_->Copy(host.data(), dev, host.size(),
                              CopyDirection::kDeviceToHost));
    accel_->SetContext(0);
    EXPECT_TRUE(accel_->Copy(host.data(), dev, host.size(),
                             CopyDirection::kDeviceToHost));
}

// Gap 5, without a second GPU: the owner lookup and the once-per-device
// warning bookkeeping, on fake devices.
TEST(XpuOwnerQueueIndexTest, MissingOwnerIsUnsupported) {
    const std::vector<int> devices = {10, 11, 12};
    EXPECT_EQ(device::xpu::OwnerQueueIndex(10, devices), 0);
    EXPECT_EQ(device::xpu::OwnerQueueIndex(12, devices), 2);
    EXPECT_EQ(device::xpu::OwnerQueueIndex(99, devices),
              device::xpu::kUnsupportedDevice);
    EXPECT_EQ(device::xpu::OwnerQueueIndex(10, std::vector<int>{}),
              device::xpu::kUnsupportedDevice);
    EXPECT_NE(device::xpu::kUnsupportedDevice, device::xpu::kNotDeviceMemory);
}

TEST(XpuOwnerQueueIndexTest, WarnsOncePerUnknownDevice) {
    device::xpu::OncePerKey<int> once;
    EXPECT_TRUE(once.First(99));
    EXPECT_FALSE(once.First(99));
    EXPECT_FALSE(once.First(99));
    EXPECT_TRUE(once.First(98));
    EXPECT_FALSE(once.First(98));
}

// On hardware the lookup still maps every enumerated GPU to its own index.
TEST_F(XpuAcceleratorDeviceTest, EveryGpuMapsToItsIndex) {
    for (size_t i = 0; i < gpus_.devices.size(); ++i) {
        void* p = DeviceAlloc(256, i);
        auto info = accel_->QueryPointer(p);
        EXPECT_EQ(info.kind, MemoryKind::kDevice);
        EXPECT_EQ(info.device_id, static_cast<int>(i));
    }
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
