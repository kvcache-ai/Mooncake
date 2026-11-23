// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Isolated executable: block the write worker's final eventfd signal without
// putting fault hooks in the production driver.
#include "ssd_uring_pipeline.h"
#include <gtest/gtest.h>
#include <array>
#include <future>
#include <sys/syscall.h>
#include <unistd.h>

namespace {
thread_local bool block_signal = false;
std::promise<void> signal_entered, signal_resume;
std::shared_future<void> resumed = signal_resume.get_future().share();
}  // namespace
extern "C" ssize_t write(int fd, const void* bytes, size_t count) {
    if (block_signal) {
        block_signal = false;
        signal_entered.set_value();
        resumed.wait();
    }
    return ssize_t(syscall(SYS_write, fd, bytes, count));
}
namespace mooncake::ssd_offload {
TEST(SsdUringShutdown, DestructorWaitsForWorkerCompletionSignal) {
    struct Target final : DrainedWriteTarget {
        Error WriteAndDrain(const ReadChunk&, SlotBuffer) noexcept override {
            block_signal = true;
            return Error::kOk;
        }
    };
    char path[] = "/tmp/mooncake_uring_shutdown_XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(write(fd, "s", 1), 1);
    close(fd);
    auto source = OpenPosixReadSource(path, {1, 0, 1}).source;
    ASSERT_TRUE(source);
    unlink(path);
    std::weak_ptr<ReadSource> weak = source;
    alignas(4096) std::array<std::byte, 4096> slot{};
    BoundedExecutor writes(1, 4);
    auto made =
        UringPipelineDriver::Create(source, std::make_shared<Target>(), writes,
                                    {{slot.data(), slot.size()}});
    ASSERT_EQ(made.error, Error::kOk);
    source.reset();
    auto chunk = BuildReadPlan({1, 0, {{0, {{1, 1, 0, 1}}}}}, {4096, 1, 2})
                     .plan.chunks[0];
    auto entered = signal_entered.get_future();
    ASSERT_TRUE(made.driver->SubmitWrite({1, 1, 0, 1, 0}, chunk,
                                         {slot.data(), slot.size()}));
    const auto signalled = entered.wait_for(std::chrono::seconds(2));
    if (signalled != std::future_status::ready) {
        signal_resume.set_value();
        FAIL() << "Write did not reach its completion signal";
        return;
    }
    auto closing = std::async(std::launch::async, [&] { made.driver.reset(); });
    EXPECT_EQ(closing.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    EXPECT_FALSE(weak.expired());
    signal_resume.set_value();
    closing.get();
    EXPECT_TRUE(weak.expired());
}
}  // namespace mooncake::ssd_offload
