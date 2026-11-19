// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_uring_pipeline.h"
#include <gtest/gtest.h>
#include <array>
#include <atomic>
#include <cstring>
#include <future>
#include <fcntl.h>
#include <limits>
#include <thread>
#include <unistd.h>

namespace mooncake::ssd_offload {
namespace {
using namespace std::chrono_literals;
struct File {
    char path[64] = "/tmp/mooncake_native_uring_XXXXXX";
    std::vector<std::byte> bytes;
    File() : bytes(30037) {
        for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = std::byte(i % 251);
        int fd = mkstemp(path);
        if (fd >= 0) {
            EXPECT_EQ(write(fd, bytes.data(), bytes.size()),
                      ssize_t(bytes.size()));
            close(fd);
        }
    }
    ~File() { unlink(path); }
};
struct Target final : DrainedWriteTarget {
    std::vector<std::byte> bytes =
        std::vector<std::byte>(30064, std::byte{0xA5});
    std::promise<void> entered, resume;
    std::shared_future<void> resumed = resume.get_future().share();
    std::atomic<bool> blocked{false}, signalled{false};
    Error WriteAndDrain(const ReadChunk& chunk,
                        SlotBuffer slot) noexcept override {
        if (blocked) {
            if (!signalled.exchange(true)) entered.set_value();
            resumed.wait();
        }
        for (const auto& f : chunk.fragments)
            std::memcpy(bytes.data() + f.destination.offset,
                        slot.data + f.buffer_offset,
                        size_t(f.destination.length));
        return Error::kOk;
    }
};
struct Arena {
    alignas(4096) std::array<std::byte, 4096> a{}, b{};
    std::vector<SlotBuffer> buffers() {
        return {{a.data(), a.size()}, {b.data(), b.size()}};
    }
};
#if defined(__linux__) && defined(USE_URING)
ReadPlan Plan() {
    return BuildReadPlan({30000, 37, {{0, {{1, 1, 32, 30000}}}}},
                         {4096, 1, 100})
        .plan;
}
void Deliver(ReadPipeline& pipeline, const IoCompletion& event) {
    if (event.stage == SlotStage::kReading)
        EXPECT_EQ(pipeline.ReadDrained(event.token, event.bytes, event.result),
                  Error::kOk);
    else
        EXPECT_EQ(pipeline.WriteDrained(event.token, event.result), Error::kOk);
}
#endif
TEST(SsdUringPipeline, RejectsSourcesWithoutNativeFd) {
    struct NoFd final : ReadSource {
        ReadSourceInfo info() const noexcept override { return {1, 0, 1}; }
        ReadResult ReadAt(std::span<std::byte>, uint64_t) noexcept override {
            return {};
        }
    };
    Arena arena;
    BoundedExecutor writes(1, 4);
    auto made = UringPipelineDriver::Create(std::make_shared<NoFd>(),
                                            std::make_shared<Target>(), writes,
                                            arena.buffers());
    EXPECT_EQ(made.error, Error::kNotSupported);
    EXPECT_FALSE(made.driver);
}
#if defined(__linux__) && defined(USE_URING)
TEST(SsdUringPipeline, InvalidDescriptorCannotBecomeAFixedFile) {
    struct InvalidFd final : ReadSource {
        ReadSourceInfo info() const noexcept override { return {1, 0, 1}; }
        std::optional<int> native_read_fd() const noexcept override {
            return std::numeric_limits<int>::max();
        }
        ReadResult ReadAt(std::span<std::byte>, uint64_t) noexcept override {
            return {0, Error::kIoError};
        }
    };
    Arena arena;
    BoundedExecutor writes(1, 4);
    auto target = std::make_shared<Target>();
    auto made = UringPipelineDriver::Create(std::make_shared<InvalidFd>(),
                                            target, writes, arena.buffers());
    EXPECT_EQ(made.error, Error::kNotSupported);
    EXPECT_FALSE(made.driver);
    EXPECT_TRUE(std::all_of(target->bytes.begin(), target->bytes.end(),
                            [](std::byte b) { return b == std::byte{0xA5}; }));
}

TEST(SsdUringPipeline, NativeCqeCopiesMultipleRollingSlotsAndEofTail) {
    File file;
    auto source = OpenPosixReadSource(file.path, {30000, 37, 1}).source;
    ASSERT_TRUE(source);
    ASSERT_TRUE(source->native_read_fd());
    auto target = std::make_shared<Target>();
    Arena arena;
    BoundedExecutor writes(2, 8);
    auto made =
        UringPipelineDriver::Create(source, target, writes, arena.buffers());
    ASSERT_EQ(made.error, Error::kOk);
    ASSERT_TRUE(made.driver);
    ReadPipeline pipeline(1, 1, Plan(), arena.buffers(), *made.driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    while (pipeline.inflight()) {
        IoCompletion event;
        ASSERT_TRUE(made.driver->WaitForEvent(event, 2s));
        Deliver(pipeline, event);
    }
    EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(
        std::memcmp(target->bytes.data() + 32, file.bytes.data() + 37, 30000),
        0);
    for (size_t i = 0; i < 32; ++i) {
        EXPECT_EQ(target->bytes[i], std::byte{0xA5});
        EXPECT_EQ(target->bytes[target->bytes.size() - 1 - i], std::byte{0xA5});
    }
}
TEST(SsdUringPipeline, ReadCqeProgressesWhileWriteIsBlockedAndCancelDrains) {
    File file;
    auto source = OpenPosixReadSource(file.path, {30000, 37, 1}).source;
    auto target = std::make_shared<Target>();
    target->blocked = true;
    auto entered = target->entered.get_future();
    Arena arena;
    BoundedExecutor writes(1, 4);
    auto made =
        UringPipelineDriver::Create(source, target, writes, arena.buffers());
    ASSERT_EQ(made.error, Error::kOk);
    ReadPipeline pipeline(1, 1, Plan(), arena.buffers(), *made.driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    IoCompletion first, second;
    const bool ready = made.driver->WaitForEvent(first, 2s);
    if (ready) Deliver(pipeline, first);
    const auto started = entered.wait_for(2s);
    const bool next = made.driver->WaitForEvent(second, 2s);
    EXPECT_TRUE(ready);
    EXPECT_EQ(started, std::future_status::ready);
    EXPECT_TRUE(next);
    EXPECT_EQ(second.stage, SlotStage::kReading);
    pipeline.Cancel();
    if (next) Deliver(pipeline, second);
    EXPECT_NE(pipeline.inflight(), 0u);
    target->resume.set_value();
    while (pipeline.inflight()) {
        IoCompletion event;
        ASSERT_TRUE(made.driver->WaitForEvent(event, 2s));
        Deliver(pipeline, event);
    }
    EXPECT_EQ(pipeline.state(), PipelineState::kCancelled);
}

TEST(SsdUringPipeline, RebindsDifferentFilesAfterDeliveredPipelineCompletions) {
    File first, second;
    for (auto& byte : second.bytes) byte ^= std::byte{0x5A};
    int fd = open(second.path, O_WRONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(write(fd, second.bytes.data(), second.bytes.size()),
              ssize_t(second.bytes.size()));
    close(fd);
    auto source = OpenPosixReadSource(first.path, {30000, 37, 1}).source;
    auto next = OpenPosixReadSource(second.path, {25000, 5037, 1}).source;
    ASSERT_TRUE(source);
    ASSERT_TRUE(next);
    std::weak_ptr<ReadSource> retained = source;
    auto target = std::make_shared<Target>();
    Arena arena;
    BoundedExecutor writes(2, 8);
    auto made =
        UringPipelineDriver::Create(source, target, writes, arena.buffers());
    ASSERT_EQ(made.error, Error::kOk);
    source.reset();
    {
        ReadPipeline pipeline(1, 1, Plan(), arena.buffers(), *made.driver);
        ASSERT_EQ(pipeline.Start(), Error::kOk);
        EXPECT_EQ(made.driver->RebindSource(next), Error::kInvalidState);
        while (pipeline.inflight()) {
            IoCompletion event;
            ASSERT_TRUE(made.driver->WaitForEvent(event, 2s));
            Deliver(pipeline, event);
        }
        ASSERT_EQ(pipeline.state(), PipelineState::kSucceeded);
        ASSERT_EQ(std::memcmp(target->bytes.data() + 32,
                              first.bytes.data() + 37, 30000),
                  0);
    }
    EXPECT_FALSE(retained.expired());
    ASSERT_EQ(made.driver->RebindSource(next), Error::kOk);
    EXPECT_TRUE(retained.expired());
    std::fill(target->bytes.begin(), target->bytes.end(), std::byte{0xA5});
    auto planned = BuildReadPlan({25000, 5037, {{7000, {{1, 1, 32, 18000}}}}},
                                 {4096, 1, 100});
    ASSERT_EQ(planned.error, Error::kOk);
    ReadPipeline pipeline(1, 1, std::move(planned.plan), arena.buffers(),
                          *made.driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    while (pipeline.inflight()) {
        IoCompletion event;
        ASSERT_TRUE(made.driver->WaitForEvent(event, 2s));
        Deliver(pipeline, event);
    }
    ASSERT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(std::memcmp(target->bytes.data() + 32,
                          second.bytes.data() + 12037, 18000),
              0);
    for (size_t i = 0; i < target->bytes.size(); ++i) {
        if (i < 32 || i >= 18032) {
            EXPECT_EQ(target->bytes[i], std::byte{0xA5});
        }
    }
}

TEST(SsdUringPipeline, RebindRejectsBlockedWritesAndUndeliveredCompletions) {
    File file;
    auto source = OpenPosixReadSource(file.path, {30000, 37, 1}).source;
    auto next = OpenPosixReadSource(file.path, {30000, 37, 1}).source;
    auto target = std::make_shared<Target>();
    target->blocked = true;
    auto entered = target->entered.get_future();
    Arena arena;
    BoundedExecutor writes(1, 4);
    auto made =
        UringPipelineDriver::Create(source, target, writes, arena.buffers());
    ASSERT_EQ(made.error, Error::kOk);
    auto chunk = Plan().chunks[0];
    ASSERT_TRUE(
        made.driver->SubmitWrite({1, 1, 0, 1, 0}, chunk, arena.buffers()[0]));
    const auto state = entered.wait_for(2s);
    EXPECT_EQ(state, std::future_status::ready);
    EXPECT_EQ(made.driver->RebindSource(next), Error::kInvalidState);
    target->resume.set_value();
    const auto until = std::chrono::steady_clock::now() + 2s;
    while (writes.outstanding() && std::chrono::steady_clock::now() < until)
        std::this_thread::sleep_for(1ms);
    ASSERT_EQ(writes.outstanding(), 0u);
    // The callback has returned and its result is ready but not consumed;
    // zero running callbacks alone does not permit changing the source.
    EXPECT_EQ(made.driver->RebindSource(next), Error::kInvalidState);
    IoCompletion event;
    ASSERT_TRUE(made.driver->WaitForEvent(event, 2s));
    ASSERT_EQ(event.stage, SlotStage::kWriting);
    EXPECT_EQ(made.driver->RebindSource(next), Error::kOk);
}

TEST(SsdUringPipeline, FailedRebindRetainsOldSourceAndStopsNewSubmissions) {
    struct InvalidFd final : ReadSource {
        ReadSourceInfo info() const noexcept override { return {30000, 37, 1}; }
        std::optional<int> native_read_fd() const noexcept override {
            return std::numeric_limits<int>::max();
        }
        ReadResult ReadAt(std::span<std::byte>, uint64_t) noexcept override {
            return {0, Error::kIoError};
        }
    };
    File file;
    auto source = OpenPosixReadSource(file.path, {30000, 37, 1}).source;
    std::weak_ptr<ReadSource> retained = source;
    auto target = std::make_shared<Target>();
    Arena arena;
    BoundedExecutor writes(1, 4);
    auto made =
        UringPipelineDriver::Create(source, target, writes, arena.buffers());
    ASSERT_EQ(made.error, Error::kOk);
    source.reset();
    EXPECT_EQ(made.driver->RebindSource(std::make_shared<InvalidFd>()),
              Error::kIoError);
    EXPECT_FALSE(retained.expired());
    auto chunk = Plan().chunks[0];
    EXPECT_FALSE(
        made.driver->SubmitRead({1, 1, 0, 1, 0}, chunk, arena.buffers()[0]));
    EXPECT_FALSE(
        made.driver->SubmitWrite({1, 1, 0, 1, 0}, chunk, arena.buffers()[0]));
    made.driver.reset();
    EXPECT_TRUE(retained.expired());
    EXPECT_TRUE(std::all_of(target->bytes.begin(), target->bytes.end(),
                            [](std::byte b) { return b == std::byte{0xA5}; }));
}

TEST(SsdUringPipeline, NegativeRebindCannotRemoveTheRegisteredSource) {
    struct NegativeFd final : ReadSource {
        ReadSourceInfo info() const noexcept override { return {30000, 37, 1}; }
        std::optional<int> native_read_fd() const noexcept override {
            return -1;
        }
        ReadResult ReadAt(std::span<std::byte>, uint64_t) noexcept override {
            return {0, Error::kIoError};
        }
    };
    File file;
    auto source = OpenPosixReadSource(file.path, {30000, 37, 1}).source;
    auto target = std::make_shared<Target>();
    Arena arena;
    BoundedExecutor writes(1, 4);
    auto made =
        UringPipelineDriver::Create(source, target, writes, arena.buffers());
    ASSERT_EQ(made.error, Error::kOk);
    EXPECT_EQ(made.driver->RebindSource(nullptr), Error::kInvalidArgument);
    // FILES_UPDATE accepts -1 as a removal entry; it is not a readable source.
    EXPECT_EQ(made.driver->RebindSource(std::make_shared<NegativeFd>()),
              Error::kNotSupported);
    ReadPipeline pipeline(1, 1, Plan(), arena.buffers(), *made.driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    while (pipeline.inflight()) {
        IoCompletion event;
        ASSERT_TRUE(made.driver->WaitForEvent(event, 2s));
        Deliver(pipeline, event);
    }
    EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(
        std::memcmp(target->bytes.data() + 32, file.bytes.data() + 37, 30000),
        0);
}
#endif
}  // namespace
}  // namespace mooncake::ssd_offload
