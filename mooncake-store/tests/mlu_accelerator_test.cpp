#include "mlu_test_utils.h"

#include <algorithm>
#include <barrier>
#include <thread>
#include <vector>

#include "device/runtime_accelerator.h"
#include "mlu_test_hooks.h"
#include "pinned_buffer_pool.h"
#include "transfer_task.h"

namespace mooncake::testing {
namespace {

using device::CopyDirection;
using device::MemoryKind;

TEST_F(MluTest, RegistryRecognizesDeviceAndInteriorPointers) {
    MluTestBuffer buffer;
    ASSERT_EQ(buffer.allocate(4096), cnrtSuccess);
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators(true);
    device::PointerInfo info;
    EXPECT_EQ(runtime.FindDeviceForPointer(buffer.data(123), &info),
              accelerator_);
    EXPECT_EQ(info.kind, MemoryKind::kDevice);
    EXPECT_EQ(info.device_id, 0);
    char host[16] = {};
    EXPECT_EQ(accelerator_->QueryPointer(host).kind, MemoryKind::kHost);
    EXPECT_EQ(runtime.FindDeviceForPointer(host), nullptr);
    EXPECT_EQ(runtime.FindDeviceForPointer(nullptr), nullptr);
}

TEST_F(MluTest, CopiesOnFreshThreadsUseAllocationDevice) {
    // Visible ordinals can differ from physical card IDs. Run this with a
    // reordered visibility mask as well as with a single visible card.
    for (unsigned int device_id = 0; device_id < device_count_; ++device_id) {
        MluTestBuffer buffer;
        ASSERT_EQ(buffer.allocate(8192, device_id), cnrtSuccess);
        std::thread worker([&] {
            auto runtime =
                device::GetAcceleratorRegistry().RuntimeAccelerators();
            device::PointerInfo info;
            ASSERT_EQ(runtime.FindDeviceForPointer(buffer.data(17), &info),
                      accelerator_);
            EXPECT_EQ(info.device_id, device_id);
            std::vector<char> source(4096, static_cast<char>(device_id + 31));
            std::vector<char> target(source.size());
            ASSERT_TRUE(runtime.CopyFromHost(buffer.data(17), source.data(),
                                             source.size()));
            EXPECT_EQ(accelerator_->CurrentDeviceId(), device_id);
            ASSERT_TRUE(runtime.CopyToHost(target.data(), buffer.data(17),
                                           target.size()));
            EXPECT_EQ(target, source);
        });
        worker.join();
    }
}

TEST_F(MluTest, ExplicitAndAutomaticCopyDirectionsRoundTrip) {
    constexpr size_t size = 65537;
    MluTestBuffer source, target;
    ASSERT_EQ(source.allocate(size + 256), cnrtSuccess);
    ASSERT_EQ(target.allocate(size + 256), cnrtSuccess);
    std::vector<char> expected(size), actual(size);
    for (size_t i = 0; i < size; ++i) expected[i] = (i * 17 + 3) % 251;
    accelerator_->SetContext(0);
    ASSERT_TRUE(accelerator_->Copy(source.data(123), expected.data(), size,
                                   CopyDirection::kHostToDevice));
    ASSERT_TRUE(accelerator_->Copy(target.data(19), source.data(123), size,
                                   CopyDirection::kDeviceToDevice));
    ASSERT_TRUE(accelerator_->Copy(actual.data(), target.data(19), size,
                                   CopyDirection::kDeviceToHost));
    EXPECT_EQ(actual, expected);
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    ASSERT_TRUE(runtime.CopyAuto(source.data(), expected.data(), size));
    ASSERT_TRUE(runtime.CopyAuto(target.data(), source.data(), size));
    ASSERT_TRUE(runtime.CopyAuto(actual.data(), target.data(), size));
    EXPECT_EQ(actual, expected);
    std::fill(actual.begin(), actual.end(), 0);
    ASSERT_TRUE(accelerator_->Copy(actual.data(), expected.data(), size,
                                   CopyDirection::kHostToHost));
    EXPECT_EQ(actual, expected);
}

TEST_F(MluTest, PinnedPoolReusesNeuwareAllocationAndFreesOnAnotherThread) {
    PinnedBufferPool pool(1);
    auto buffer = pool.Acquire(65536);
    ASSERT_NE(buffer.data, nullptr);
    ASSERT_EQ(buffer.pinned_host.addr, buffer.data);
    cnrtPointerAttributes_t attr{};
    ASSERT_EQ(cnrtPointerGetAttributes(&attr, buffer.data), cnrtSuccess);
    EXPECT_EQ(attr.type, cnrtMemTypeHost);
    EXPECT_EQ(accelerator_->QueryPointer(buffer.data).kind, MemoryKind::kHost);
    auto* original = buffer.data;
    pool.Release(std::move(buffer));
    auto reused = pool.Acquire(4096);
    EXPECT_EQ(reused.data, original);
    pool.Release(std::move(reused));
    std::thread releaser([&] { pool.Clear(); });
    releaser.join();
}

TEST_F(MluTest, ReportsRuntimeCopyFailure) {
    MluTestBuffer buffer;
    ASSERT_EQ(buffer.allocate(4096), cnrtSuccess);
    EXPECT_FALSE(accelerator_->Copy(nullptr, buffer.data(), 4096,
                                    CopyDirection::kDeviceToHost));
    cnrtGetLastError();
    EXPECT_FALSE(
        accelerator_->AllocatePinnedHost(static_cast<size_t>(-1)).addr);
}

TEST_F(MluTest, ConcurrentCopiesRefreshRegistryAndReusePinnedBuffers) {
    constexpr size_t threads = 8;
    constexpr size_t size = 4096;
    const auto allocations = mlu_pinned_allocations.load();
    const auto frees = mlu_pinned_frees.load();
    const auto free_errors = mlu_pinned_free_errors.load();
    PinnedBufferPool pool(2);
    std::barrier start(threads + 1);
    std::vector<std::thread> workers;
    for (size_t i = 0; i < threads; ++i) {
        workers.emplace_back([&, i] {
            start.arrive_and_wait();
            MluTestBuffer buffer;
            const int device_id = i % device_count_;
            ASSERT_EQ(buffer.allocate(size, device_id), cnrtSuccess);
            for (int iteration = 0; iteration < 50; ++iteration) {
                auto runtime =
                    device::GetAcceleratorRegistry().RuntimeAccelerators(
                        iteration % 7 == 0);
                auto host = pool.Acquire(size);
                ASSERT_NE(host.pinned_host.addr, nullptr);
                const char value = static_cast<char>(i * 17 + iteration);
                std::fill_n(host.data, size, value);
                // Deliberately start on another device where possible.
                accelerator_->SetContext((device_id + 1) % device_count_);
                ASSERT_TRUE(
                    runtime.CopyFromHost(buffer.data(), host.data, size));
                EXPECT_EQ(accelerator_->CurrentDeviceId(), device_id);
                std::fill_n(host.data, size, 0);
                ASSERT_TRUE(runtime.CopyToHost(host.data, buffer.data(), size));
                EXPECT_TRUE(
                    std::all_of(host.data, host.data + size,
                                [value](char c) { return c == value; }));
                pool.Release(std::move(host));
            }
        });
    }
    start.arrive_and_wait();
    for (int i = 0; i < 50; ++i) {
        pool.Clear();
        std::this_thread::yield();
    }
    for (auto& worker : workers) worker.join();
    pool.Clear();
    EXPECT_EQ(mlu_pinned_allocations.load() - allocations,
              mlu_pinned_frees.load() - frees);
    EXPECT_EQ(mlu_pinned_free_errors.load(), free_errors);
}

TEST_F(MluTest, PinnedAllocationFailureFallsBackAndRecovers) {
    PinnedBufferPool pool(0);
    {
        ScopedMluFailure failure(fail_mlu_pinned_allocation);
        EXPECT_EQ(PinnedBufferPool::AllocatePinned(4096).data, nullptr);
        auto buffer = pool.Acquire(4096);
        ASSERT_NE(buffer.data, nullptr);
        EXPECT_EQ(buffer.pinned_host.addr, nullptr);
        EXPECT_EQ(buffer.pageable_host.get(), buffer.data);
        pool.Release(std::move(buffer));
    }
    auto recovered = pool.Acquire(4096);
    EXPECT_EQ(recovered.pinned_host.addr, recovered.data);
    EXPECT_NE(recovered.data, nullptr);
}

TEST_F(MluTest, PinnedMoveAssignmentAndPoolOverflowFreeExactlyOnce) {
    const auto allocations = mlu_pinned_allocations.load();
    const auto frees = mlu_pinned_frees.load();
    const auto free_errors = mlu_pinned_free_errors.load();
    {
        auto first = accelerator_->AllocatePinnedHost(4096);
        auto second = accelerator_->AllocatePinnedHost(8192);
        ASSERT_NE(first.addr, nullptr);
        ASSERT_NE(second.addr, nullptr);
        first = std::move(second);
        EXPECT_EQ(second.addr, nullptr);
        PinnedBufferPool pool(1);
        pool.Release(PinnedBufferPool::Buffer(std::move(first)));
        pool.Release(
            PinnedBufferPool::Buffer(accelerator_->AllocatePinnedHost(4096)));
        std::thread other_device([&] {
            accelerator_->SetContext(device_count_ - 1);
            pool.Clear();
        });
        other_device.join();
    }
    EXPECT_EQ(mlu_pinned_allocations.load() - allocations, 3);
    EXPECT_EQ(mlu_pinned_frees.load() - frees, 3);
    EXPECT_EQ(mlu_pinned_free_errors.load(), free_errors);
}

TEST_F(MluTest, MemcpyWorkerReportsFailureThenDrainsBeforeDestruction) {
    constexpr size_t size = 4096;
    MluTestBuffer target;
    ASSERT_EQ(target.allocate(size), cnrtSuccess);
    ASSERT_EQ(cnrtMemset(target.data(), 0, size), cnrtSuccess);
    std::vector<char> expected(size, 43), actual(size);
    std::vector<std::shared_ptr<MemcpyOperationState>> states;
    const auto failures = mlu_injected_copy_failures.load();
    {
        MemcpyWorkerPool pool;
        auto failed = std::make_shared<MemcpyOperationState>();
        {
            ScopedMluFailure failure(fail_mlu_h2d);
            pool.submitTask(
                MemcpyTask({{target.data(), expected.data(), size}}, failed));
            failed->wait_for_completion();
            EXPECT_EQ(failed->get_result(), ErrorCode::TRANSFER_FAIL);
        }
        EXPECT_GT(mlu_injected_copy_failures.load(), failures);
        for (int i = 0; i < 32; ++i) {
            states.push_back(std::make_shared<MemcpyOperationState>());
            pool.submitTask(MemcpyTask({{target.data(), expected.data(), size}},
                                       states.back()));
        }
        // The pool must drain accepted copies before the caller frees memory.
    }
    for (auto& state : states) {
        EXPECT_TRUE(state->is_completed());
        EXPECT_EQ(state->get_result(), ErrorCode::OK);
    }
    ASSERT_EQ(
        cnrtMemcpy(actual.data(), target.data(), size, cnrtMemcpyDevToHost),
        cnrtSuccess);
    EXPECT_EQ(actual, expected);
}

}  // namespace
}  // namespace mooncake::testing
