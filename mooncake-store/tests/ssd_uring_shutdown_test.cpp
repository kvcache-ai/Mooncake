// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Isolated executable: block the write worker's final eventfd signal without
// putting fault hooks in the production driver.
#include "ssd_uring_pipeline.h"
#include <gtest/gtest.h>
#include <liburing.h>
#include <array>
#include <cerrno>
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
    io_uring probe{};
    const int probe_rc = io_uring_queue_init(2, &probe, 0);
    if (probe_rc == -ENOSYS || probe_rc == -EPERM || probe_rc == -EACCES ||
        probe_rc == -ENOMEM) {
        GTEST_SKIP() << "io_uring unavailable in this environment: "
                     << -probe_rc;
    }
    ASSERT_EQ(probe_rc, 0);
    io_uring_probe* op_probe = io_uring_get_probe_ring(&probe);
    const bool read_supported =
        op_probe && io_uring_opcode_supported(op_probe, IORING_OP_READ) != 0;
    if (op_probe) io_uring_free_probe(op_probe);
    io_uring_queue_exit(&probe);
    if (!read_supported) {
        GTEST_SKIP() << "IORING_OP_READ unavailable on this kernel";
    }

    block_signal = false;
    signal_entered = std::promise<void>{};
    signal_resume = std::promise<void>{};
    resumed = signal_resume.get_future().share();
    bool released = false;
    struct ReleaseOnExit {
        bool& released;
        ~ReleaseOnExit() {
            if (!released) {
                released = true;
                signal_resume.set_value();
            }
        }
    };
    struct Target final : DrainedWriteTarget {
        Error WriteAndDrain(const ReadChunk&, SlotBuffer) noexcept override {
            block_signal = true;
            return Error::kOk;
        }
    };
    char path[] = "/tmp/mooncake_uring_shutdown_XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    struct UnlinkOnExit {
        const char* path;
        ~UnlinkOnExit() { unlink(path); }
    } unlink_on_exit{path};
    ASSERT_EQ(write(fd, "s", 1), 1);
    close(fd);
    auto source = OpenPosixReadSource(path, {1, 0, 1}).source;
    ASSERT_TRUE(source);
    std::weak_ptr<ReadSource> weak = source;
    alignas(4096) std::array<std::byte, 4096> slot{};
    BoundedExecutor writes(1, 4);
    auto chunk = BuildReadPlan({1, 0, {{0, {{1, 1, 0, 1}}}}}, {4096, 1, 2})
                     .plan.chunks[0];
    auto made =
        UringPipelineDriver::Create(source, std::make_shared<Target>(), writes,
                                    {{slot.data(), slot.size()}});
    ASSERT_EQ(made.error, Error::kOk);
    ReleaseOnExit release_on_exit{released};
    source.reset();
    auto entered = signal_entered.get_future();
    ASSERT_TRUE(made.driver->SubmitWrite({1, 1, 0, 1, 0}, chunk,
                                         {slot.data(), slot.size()}));
    const auto signalled = entered.wait_for(std::chrono::seconds(2));
    if (signalled != std::future_status::ready) {
        released = true;
        signal_resume.set_value();
        FAIL() << "Write did not reach its completion signal";
        return;
    }
    auto closing = std::async(std::launch::async, [&] { made.driver.reset(); });
    EXPECT_EQ(closing.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    EXPECT_FALSE(weak.expired());
    released = true;
    signal_resume.set_value();
    closing.get();
    EXPECT_TRUE(weak.expired());
}
}  // namespace mooncake::ssd_offload
