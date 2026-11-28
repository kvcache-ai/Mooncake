// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_pipeline_io.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstring>
#include <future>
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

TEST(SsdExecutor, WaitSubmitAppliesBackpressureAndDrainsShutdown) {
    BoundedExecutor executor(1, 1);
    Gate gate;
    auto entered = gate.entered.get_future();
    ASSERT_TRUE(executor.WaitSubmit([&] { gate.Wait(); }));
    ASSERT_EQ(entered.wait_for(10s), std::future_status::ready);

    std::promise<void> attempted;
    auto attempting = attempted.get_future();
    std::atomic<bool> finished{false};
    auto waiting = std::async(std::launch::async, [&] {
        attempted.set_value();
        return executor.WaitSubmit([&] { finished.store(true); });
    });
    attempting.wait();
    EXPECT_EQ(waiting.wait_for(20ms), std::future_status::timeout);
    gate.release.set_value();
    EXPECT_TRUE(waiting.get());
    executor.Shutdown();
    EXPECT_TRUE(finished.load());
    EXPECT_FALSE(executor.WaitSubmit([] {}));
}

class MemorySource final : public ReadSource {
   public:
    explicit MemorySource(size_t size) : bytes(size) {
        for (size_t i = 0; i < size; ++i) bytes[i] = std::byte(i % 251);
    }
    uint64_t file_bytes() const noexcept override { return bytes.size(); }
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

class CopyTarget final : public WriteTarget {
   public:
    explicit CopyTarget(size_t bytes) : bytes(bytes) {}
    Error Write(const ReadChunk& chunk, SlotBuffer buffer) noexcept override {
        ++writes;
        for (const auto& fragment : chunk.fragments) {
            const auto& target = fragment.destination;
            if (target.destination_index != 0 || target.offset > bytes.size() ||
                target.length > bytes.size() - target.offset)
                return Error::kTransferError;
            std::memcpy(bytes.data() + target.offset,
                        buffer.data + fragment.buffer_offset, target.length);
        }
        return Error::kOk;
    }
    std::vector<std::byte> bytes;
    std::atomic<size_t> writes{0};
};

struct Arena {
    alignas(4096) std::array<std::byte, 4096> a{}, b{};
    std::vector<SlotBuffer> buffers() {
        return {{a.data(), a.size()}, {b.data(), b.size()}};
    }
};

ReadPlan Plan(size_t bytes) {
    return BuildReadPlan({bytes, 0, 0, {{0, 0, bytes}}}, {4096, 1, 64}).plan;
}

TEST(SsdPipelineIo, CopiesThroughRealWorkerCompletions) {
    auto source = std::make_shared<MemorySource>(12000);
    auto target = std::make_shared<CopyTarget>(12000);
    BoundedExecutor reads(2, 8), writes(2, 8);
    Arena arena;
    auto plan = Plan(12000);
    std::vector<size_t> chunk_sources(plan.chunks.size(), 0);
    std::unique_ptr<ReadPipeline> pipeline;
    auto driver = std::make_unique<ThreadedPipelineDriver>(
        std::vector<std::shared_ptr<ReadSource>>{source},
        std::move(chunk_sources), target, reads, writes, 2);
    pipeline = std::make_unique<ReadPipeline>(std::move(plan), arena.buffers(),
                                              *driver);
    ASSERT_EQ(pipeline->Start(), Error::kOk);
    for (size_t count = 0; count < 6 && pipeline->inflight(); ++count) {
        IoCompletion event;
        ASSERT_TRUE(driver->WaitForEvent(event, 10s));
        if (event.stage == SlotStage::kReading) {
            EXPECT_EQ(
                pipeline->ReadDrained(event.token, event.bytes, event.result),
                Error::kOk);
        } else {
            EXPECT_EQ(pipeline->WriteDrained(event.token, event.result),
                      Error::kOk);
        }
    }
    EXPECT_EQ(pipeline->state(), PipelineState::kSucceeded);
    EXPECT_EQ(target->bytes, source->bytes);
    EXPECT_EQ(source->reads.load(), 3u);
    EXPECT_EQ(target->writes.load(), 3u);
}

TEST(SsdPipelineIo, ReadyEventCannotBeOverwrittenBeforeConsumption) {
    auto source = std::make_shared<MemorySource>(4096);
    auto target = std::make_shared<CopyTarget>(4096);
    BoundedExecutor reads(1, 4), writes(1, 4);
    Arena arena;
    auto chunk = Plan(4096).chunks[0];
    ThreadedPipelineDriver driver({source}, {0}, target, reads, writes, 1);
    SlotToken token{0, 1, 0};
    ASSERT_TRUE(driver.SubmitRead(token, chunk, arena.buffers()[0]));
    // Drain the single-worker read executor with a barrier task so the first
    // task is guaranteed to have transitioned the slot record to State::kReady
    // before testing that a duplicate SubmitRead is rejected.
    std::promise<void> barrier;
    ASSERT_TRUE(reads.WaitSubmit([&] { barrier.set_value(); }));
    ASSERT_EQ(barrier.get_future().wait_for(10s), std::future_status::ready);
    auto next = token;
    ++next.generation;
    EXPECT_FALSE(driver.SubmitRead(next, chunk, arena.buffers()[0]));
    IoCompletion event;
    ASSERT_TRUE(driver.WaitForEvent(event, 10s));
    EXPECT_EQ(event.token.generation, 1u);
    EXPECT_EQ(event.bytes, 4096u);
    ASSERT_TRUE(driver.SubmitRead(next, chunk, arena.buffers()[0]));
    ASSERT_TRUE(driver.WaitForEvent(event, 10s));
    EXPECT_EQ(event.token.generation, 2u);
}

TEST(SsdPipelineIo, InlineWriteModeCopiesWithoutWriteThreadPool) {
    auto source = std::make_shared<MemorySource>(12000);
    auto target = std::make_shared<CopyTarget>(12000);
    BoundedExecutor reads(2, 8);
    Arena arena;
    auto plan = Plan(12000);
    std::vector<size_t> chunk_sources(plan.chunks.size(), 0);
    std::unique_ptr<ReadPipeline> pipeline;
    auto driver = std::make_unique<ThreadedPipelineDriver>(
        std::vector<std::shared_ptr<ReadSource>>{source},
        std::move(chunk_sources), target, reads, nullptr, 2);
    pipeline = std::make_unique<ReadPipeline>(std::move(plan), arena.buffers(),
                                              *driver);
    ASSERT_EQ(pipeline->Start(), Error::kOk);
    for (size_t count = 0; count < 6 && pipeline->inflight(); ++count) {
        IoCompletion event;
        ASSERT_TRUE(driver->WaitForEvent(event, 10s));
        if (event.stage == SlotStage::kReading) {
            EXPECT_EQ(
                pipeline->ReadDrained(event.token, event.bytes, event.result),
                Error::kOk);
        } else {
            EXPECT_EQ(pipeline->WriteDrained(event.token, event.result),
                      Error::kOk);
        }
    }
    EXPECT_EQ(pipeline->state(), PipelineState::kSucceeded);
    EXPECT_EQ(target->bytes, source->bytes);
    EXPECT_EQ(source->reads.load(), 3u);
    EXPECT_EQ(target->writes.load(), 3u);
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
    auto driver = std::make_unique<ThreadedPipelineDriver>(
        std::vector<std::shared_ptr<ReadSource>>{source},
        std::vector<size_t>{0}, target, reads, writes, 1);
    EXPECT_TRUE(driver->SubmitRead({0, 1, 0}, chunk, arena.buffers()[0]));
    ASSERT_EQ(entered.wait_for(10s), std::future_status::ready);
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

}  // namespace
}  // namespace mooncake::ssd_offload
