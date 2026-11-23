// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_pipeline_io.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstring>
#include <future>
#include <mutex>
#include <thread>

namespace mooncake::ssd_offload {
namespace {
using namespace std::chrono_literals;

struct Gate {
    std::promise<void> entered, release;
    std::shared_future<void> released = release.get_future().share();
    std::atomic<bool> signalled{false};
    void Wait() {
        if (!signalled.exchange(true)) entered.set_value();
        released.wait();
    }
};

TEST(SsdExecutor, RejectsInvalidLimits) {
    EXPECT_THROW(BoundedExecutor(0, 1), std::invalid_argument);
    EXPECT_THROW(BoundedExecutor(1, 0), std::invalid_argument);
}

TEST(SsdExecutor, BoundsQueuedAndRunningTasksAndDrainsShutdown) {
    BoundedExecutor executor(1, 2);
    Gate gate;
    auto entered = gate.entered.get_future();
    std::atomic<size_t> completed{0};
    EXPECT_TRUE(executor.TrySubmit([&] {
        gate.Wait();
        ++completed;
    }));
    const auto ready = entered.wait_for(2s);
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_TRUE(executor.TrySubmit([&] { ++completed; }));
    EXPECT_FALSE(executor.TrySubmit([] {}));
    EXPECT_EQ(executor.outstanding(), 2u);
    gate.release.set_value();
    executor.Shutdown();
    EXPECT_EQ(completed.load(), 2u);
    EXPECT_EQ(executor.outstanding(), 0u);
    EXPECT_FALSE(executor.TrySubmit([] {}));
}

class MemorySource final : public ReadSource {
   public:
    explicit MemorySource(size_t size) : bytes(size) {
        for (size_t i = 0; i < size; ++i) bytes[i] = std::byte(i % 251);
    }
    ReadSourceInfo info() const noexcept override {
        return {bytes.size(), 0, 1};
    }
    ReadResult ReadAt(std::span<std::byte> buffer,
                      uint64_t offset) noexcept override {
        ++reads;
        if (gate) gate->Wait();
        if (offset > bytes.size()) return {0, Error::kIoError};
        const size_t count =
            std::min<size_t>(buffer.size(), bytes.size() - offset);
        std::memcpy(buffer.data(), bytes.data() + offset, count);
        return {count, Error::kOk};
    }
    std::vector<std::byte> bytes;
    std::atomic<size_t> reads{0};
    Gate* gate{nullptr};
};

class CopyTarget final : public DrainedWriteTarget {
   public:
    explicit CopyTarget(size_t bytes) : bytes(bytes) {}
    Error WriteAndDrain(const ReadChunk& chunk,
                        SlotBuffer buffer) noexcept override {
        ++writes;
        if (gate) gate->Wait();
        for (const auto& fragment : chunk.fragments) {
            const auto& target = fragment.destination;
            if (target.region_id != 1 || target.generation != 1 ||
                target.offset > bytes.size() ||
                target.length > bytes.size() - target.offset)
                return Error::kTransferError;
            std::memcpy(bytes.data() + target.offset,
                        buffer.data + fragment.buffer_offset, target.length);
        }
        return Error::kOk;
    }
    std::vector<std::byte> bytes;
    std::atomic<size_t> writes{0};
    Gate* gate{nullptr};
};

struct Arena {
    alignas(4096) std::array<std::byte, 4096> a{}, b{};
    std::vector<SlotBuffer> buffers() {
        return {{a.data(), a.size()}, {b.data(), b.size()}};
    }
};

ReadPlan Plan(size_t bytes) {
    return BuildReadPlan({bytes, 0, {{0, {{1, 1, 0, bytes}}}}}, {4096, 1, 64})
        .plan;
}

void Deliver(ReadPipeline& pipeline, const IoCompletion& event) {
    if (event.stage == SlotStage::kReading)
        EXPECT_EQ(pipeline.ReadDrained(event.token, event.bytes, event.result),
                  Error::kOk);
    else
        EXPECT_EQ(pipeline.WriteDrained(event.token, event.result), Error::kOk);
}

TEST(SsdPipelineIo, CopiesThroughRealWorkerCompletions) {
    auto source = std::make_shared<MemorySource>(12000);
    auto target = std::make_shared<CopyTarget>(12000);
    BoundedExecutor reads(2, 8), writes(2, 8);
    Arena arena;
    ThreadedPipelineDriver driver(source, target, reads, writes, 2);
    ReadPipeline pipeline(1, 1, Plan(12000), arena.buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    for (size_t count = 0; count < 6 && pipeline.inflight(); ++count) {
        IoCompletion event;
        ASSERT_TRUE(driver.WaitForEvent(event, 2s));
        Deliver(pipeline, event);
    }
    EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(target->bytes, source->bytes);
    EXPECT_EQ(source->reads.load(), 3u);
    EXPECT_EQ(target->writes.load(), 3u);
}

TEST(SsdPipelineIo, CancelRetainsAcceptedIoUntilItsWorkerReturns) {
    Gate gate;
    auto entered = gate.entered.get_future();
    auto source = std::make_shared<MemorySource>(8192);
    source->gate = &gate;
    auto target = std::make_shared<CopyTarget>(8192);
    BoundedExecutor reads(2, 8), writes(2, 8);
    Arena arena;
    ThreadedPipelineDriver driver(source, target, reads, writes, 2);
    ReadPipeline pipeline(1, 1, Plan(8192), arena.buffers(), driver);
    const auto start = pipeline.Start();
    const auto ready = entered.wait_for(2s);
    EXPECT_EQ(start, Error::kOk);
    EXPECT_EQ(ready, std::future_status::ready);
    pipeline.Cancel();
    EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
    EXPECT_EQ(pipeline.inflight(), 2u);
    gate.release.set_value();
    for (size_t count = 0; count < 2 && pipeline.inflight(); ++count) {
        IoCompletion event;
        ASSERT_TRUE(driver.WaitForEvent(event, 2s));
        Deliver(pipeline, event);
    }
    EXPECT_EQ(pipeline.state(), PipelineState::kCancelled);
    EXPECT_EQ(target->writes.load(), 0u);
}

TEST(SsdPipelineIo, ReadyEventsCannotBeOverwrittenByAnotherSubmission) {
    auto source = std::make_shared<MemorySource>(4096);
    auto target = std::make_shared<CopyTarget>(4096);
    BoundedExecutor reads(1, 4), writes(1, 4);
    Arena arena;
    auto chunk = Plan(4096).chunks[0];
    ThreadedPipelineDriver driver(source, target, reads, writes, 1);
    SlotToken token{1, 1, 0, 1, 0};
    ASSERT_TRUE(driver.SubmitRead(token, chunk, arena.buffers()[0]));
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (reads.outstanding() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_EQ(reads.outstanding(), 0u);
    auto next = token;
    ++next.generation;
    EXPECT_FALSE(driver.SubmitRead(next, chunk, arena.buffers()[0]));
    IoCompletion event;
    ASSERT_TRUE(driver.WaitForEvent(event, 2s));
    EXPECT_EQ(event.token.generation, 1u);
    EXPECT_EQ(event.bytes, 4096u);
    ASSERT_TRUE(driver.SubmitRead(next, chunk, arena.buffers()[0]));
    ASSERT_TRUE(driver.WaitForEvent(event, 2s));
    EXPECT_EQ(event.token.generation, 2u);
}

TEST(SsdPipelineIo, DestructorWaitsForRunningReadAndPinsSource) {
    Gate gate;
    auto entered = gate.entered.get_future();
    auto source = std::make_shared<MemorySource>(4096);
    source->gate = &gate;
    std::weak_ptr<ReadSource> weak_source = source;
    auto target = std::make_shared<CopyTarget>(4096);
    BoundedExecutor reads(1, 4), writes(1, 4);
    Arena arena;
    auto chunk = Plan(4096).chunks[0];
    auto driver = std::make_unique<ThreadedPipelineDriver>(source, target,
                                                           reads, writes, 1);
    EXPECT_TRUE(driver->SubmitRead({1, 1, 0, 1, 0}, chunk, arena.buffers()[0]));
    const auto ready = entered.wait_for(2s);
    EXPECT_EQ(ready, std::future_status::ready);
    source.reset();
    auto closing =
        std::async(std::launch::async,
                   [driver = std::move(driver)]() mutable { driver.reset(); });
    EXPECT_EQ(closing.wait_for(20ms), std::future_status::timeout);
    EXPECT_FALSE(weak_source.expired());
    gate.release.set_value();
    closing.get();
    EXPECT_TRUE(weak_source.expired());
}

TEST(SsdPipelineIo, CancelDrainsRunningWriteAndSkipsQueuedWrite) {
    Gate gate;
    auto entered = gate.entered.get_future();
    auto source = std::make_shared<MemorySource>(8192);
    auto target = std::make_shared<CopyTarget>(8192);
    target->gate = &gate;
    BoundedExecutor reads(2, 8), writes(1, 8);
    Arena arena;
    ThreadedPipelineDriver driver(source, target, reads, writes, 2);
    ReadPipeline pipeline(1, 1, Plan(8192), arena.buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    for (size_t count = 0; count < 2; ++count) {
        IoCompletion event;
        const bool ready = driver.WaitForEvent(event, 2s);
        EXPECT_TRUE(ready);
        if (ready) Deliver(pipeline, event);
    }
    EXPECT_EQ(entered.wait_for(2s), std::future_status::ready);
    pipeline.Cancel();
    EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
    EXPECT_EQ(pipeline.inflight(), 2u);
    gate.release.set_value();
    for (size_t count = 0; count < 2 && pipeline.inflight(); ++count) {
        IoCompletion event;
        ASSERT_TRUE(driver.WaitForEvent(event, 2s));
        Deliver(pipeline, event);
    }
    EXPECT_EQ(pipeline.state(), PipelineState::kCancelled);
    EXPECT_EQ(target->writes.load(), 1u);
}
}  // namespace
}  // namespace mooncake::ssd_offload
