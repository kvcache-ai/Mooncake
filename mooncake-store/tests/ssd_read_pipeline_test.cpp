// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_pipeline.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <vector>

namespace mooncake::ssd_offload {
namespace {

DestinationSlice Destination(uint64_t offset, uint64_t length,
                             size_t index = 0) {
    return {index, offset, length};
}

PlanResult Plan(uint64_t bytes, uint64_t value_offset = 0,
                size_t max_chunks = 64) {
    return BuildReadPlan({bytes, value_offset, 0, {Destination(0, bytes)}},
                         {4096, 4096, max_chunks});
}

class RecordingDriver final : public PipelineDriver {
   public:
    struct Submission {
        SlotToken token;
        const ReadChunk* chunk;
        SlotBuffer buffer;
    };
    bool SubmitRead(const SlotToken& token, const ReadChunk& chunk,
                    SlotBuffer buffer) noexcept override {
        if (reject_read || reads.size() == reject_read_after) return false;
        reads.push_back({token, &chunk, buffer});
        return true;
    }
    bool SubmitWrite(const SlotToken& token, const ReadChunk& chunk,
                     SlotBuffer buffer) noexcept override {
        if (reject_write) return false;
        writes.push_back({token, &chunk, buffer});
        return true;
    }
    std::vector<Submission> reads, writes;
    bool reject_read{false}, reject_write{false};
    size_t reject_read_after{std::numeric_limits<size_t>::max()};
};

struct Arena {
    alignas(4096) std::array<std::byte, 4096> first{}, second{};
    std::vector<SlotBuffer> Buffers() {
        return {{first.data(), first.size()}, {second.data(), second.size()}};
    }
};

TEST(SsdReadPlan, SplitsUnalignedValueAcrossDestinationSlices) {
    auto result = BuildReadPlan(
        {12000, 13, 0, {Destination(100, 6000, 0), Destination(200, 6000, 1)}},
        {4096, 4096, 64});
    ASSERT_EQ(result.error, Error::kOk);
    ASSERT_EQ(result.plan.chunks.size(), 3u);
    EXPECT_EQ(result.plan.payload_bytes, 12000u);
    EXPECT_EQ(result.plan.io_bytes, 12288u);
    const auto& first = result.plan.chunks[0];
    EXPECT_EQ(first.file_offset, 0u);
    EXPECT_EQ(first.payload_offset, 13u);
    EXPECT_EQ(first.payload_bytes, 4083u);
    ASSERT_EQ(first.fragments.size(), 1u);
    EXPECT_EQ(first.fragments[0].buffer_offset, 13u);
    EXPECT_EQ(first.fragments[0].destination.offset, 100u);
    EXPECT_EQ(first.fragments[0].destination.length, 4083u);
    const auto& second = result.plan.chunks[1];
    ASSERT_EQ(second.fragments.size(), 2u);
    EXPECT_EQ(second.fragments[0].destination.offset, 4183u);
    EXPECT_EQ(second.fragments[0].destination.length, 1917u);
    EXPECT_EQ(second.fragments[1].destination.destination_index, 1u);
    EXPECT_EQ(second.fragments[1].destination.offset, 200u);
    EXPECT_EQ(second.fragments[1].buffer_offset, 1917u);
    EXPECT_EQ(second.fragments[1].destination.length, 2179u);
    EXPECT_EQ(result.plan.chunks[2].source_offset, 8179u);
}

TEST(SsdReadPlan, SupportsBufferedRangeAndBoundsLimits) {
    auto buffered =
        BuildReadPlan({100, 7, 80, {Destination(3, 17)}}, {8, 1, 64});
    ASSERT_EQ(buffered.error, Error::kOk);
    ASSERT_EQ(buffered.plan.chunks.size(), 3u);
    EXPECT_EQ(buffered.plan.chunks[0].file_offset, 87u);
    EXPECT_EQ(buffered.plan.payload_bytes, 17u);
    EXPECT_EQ(buffered.plan.io_bytes, 17u);

    auto limited_chunks = Plan(8192, 13, 2);
    EXPECT_EQ(limited_chunks.error, Error::kPlanLimit);
    EXPECT_TRUE(limited_chunks.plan.chunks.empty());

    auto limited_frags = BuildReadPlan({8192, 0, 0, {Destination(0, 8192)}},
                                       {4096, 4096, 64, 1});
    EXPECT_EQ(limited_frags.error, Error::kPlanLimit);
    EXPECT_TRUE(limited_frags.plan.chunks.empty());

    auto empty = Plan(0);
    ASSERT_EQ(empty.error, Error::kOk);
    EXPECT_TRUE(empty.plan.chunks.empty());
    EXPECT_EQ(empty.plan.io_bytes, 0u);
}

TEST(SsdReadPlan, RejectsOverflowOverlapAndInvalidOptions) {
    EXPECT_EQ(
        BuildReadPlan({10, 0, 8, {Destination(0, 3)}}, {4096, 4096, 64}).error,
        Error::kInvalidArgument);
    EXPECT_EQ(BuildReadPlan({10, 0, 0, {Destination(UINT64_MAX, 2)}},
                            {4096, 4096, 64})
                  .error,
              Error::kOverflow);
    EXPECT_EQ(Plan(1, static_cast<uint64_t>(INT64_MAX)).error,
              Error::kOverflow);
    EXPECT_EQ(
        BuildReadPlan({100, 0, 0, {Destination(10, 10), Destination(15, 10)}},
                      {4096, 1, 64})
            .error,
        Error::kInvalidArgument);
    EXPECT_EQ(
        BuildReadPlan({100, 0, 0, {Destination(10, 10), Destination(20, 10)}},
                      {4096, 1, 64})
            .error,
        Error::kOk);
    for (const auto options :
         {PlanOptions{0, 4096, 64}, PlanOptions{4096, 0, 64},
          PlanOptions{4096, 3, 64}, PlanOptions{4097, 4096, 64},
          PlanOptions{4096, 4096, 0}}) {
        EXPECT_EQ(BuildReadPlan({1, 0, 0, {Destination(0, 1)}}, options).error,
                  Error::kInvalidArgument);
    }
}

TEST(UringReadClassifier, HandlesFullShortEofRetryBudgetAndErrors) {
    // Exact full read succeeds with no retry.
    auto step = ClassifyUringReadResult(4096, 0, 4096, 0);
    EXPECT_FALSE(step.retry);
    EXPECT_EQ(step.next_done, 4096u);
    EXPECT_EQ(step.next_retries, 0u);
    EXPECT_EQ(step.error, Error::kOk);
    EXPECT_EQ(step.sys_errno, 0);

    // Partial read retries from advanced offset and resets transient retries.
    step = ClassifyUringReadResult(1500, 0, 4096, 2);
    EXPECT_TRUE(step.retry);
    EXPECT_EQ(step.next_done, 1500u);
    EXPECT_EQ(step.next_retries, 0u);
    EXPECT_EQ(step.error, Error::kOk);

    // Remaining bytes finish without another retry.
    step = ClassifyUringReadResult(2596, 1500, 4096, 0);
    EXPECT_FALSE(step.retry);
    EXPECT_EQ(step.next_done, 4096u);
    EXPECT_EQ(step.error, Error::kOk);

    // EOF returns partial bytes accumulated so far with kOk (ReadPipeline
    // checks payload coverage).
    step = ClassifyUringReadResult(0, 1500, 4096, 0);
    EXPECT_FALSE(step.retry);
    EXPECT_EQ(step.next_done, 1500u);
    EXPECT_EQ(step.error, Error::kOk);
    EXPECT_EQ(step.sys_errno, 0);

    // Transient -EINTR and -EAGAIN retry up to max_retries, then fail with
    // preserved errno.
    for (int err : {EINTR, EAGAIN}) {
        unsigned retries = 0;
        for (unsigned attempt = 0; attempt < 3; ++attempt) {
            step = ClassifyUringReadResult(-err, 512, 4096, retries, 3);
            EXPECT_TRUE(step.retry);
            EXPECT_EQ(step.next_done, 512u);
            EXPECT_EQ(step.next_retries, attempt + 1);
            EXPECT_EQ(step.error, Error::kOk);
            retries = step.next_retries;
        }
        step = ClassifyUringReadResult(-err, 512, 4096, retries, 3);
        EXPECT_FALSE(step.retry);
        EXPECT_EQ(step.next_done, 512u);
        EXPECT_EQ(step.error, Error::kIoError);
        EXPECT_EQ(step.sys_errno, err);
    }

    // Permanent negative errno fails immediately with errno preserved.
    step = ClassifyUringReadResult(-EIO, 0, 4096, 0);
    EXPECT_FALSE(step.retry);
    EXPECT_EQ(step.error, Error::kIoError);
    EXPECT_EQ(step.sys_errno, EIO);

    // INT_MIN does not overflow negation.
    step = ClassifyUringReadResult(std::numeric_limits<int>::min(), 0, 4096, 0);
    EXPECT_FALSE(step.retry);
    EXPECT_EQ(step.error, Error::kIoError);
    EXPECT_EQ(step.sys_errno, 0);

    // Over-long completion or invalid done >= requested fails closed.
    step = ClassifyUringReadResult(3000, 2000, 4096, 0);
    EXPECT_FALSE(step.retry);
    EXPECT_EQ(step.error, Error::kIoError);
    step = ClassifyUringReadResult(-EINTR, 0, 0, 0);
    EXPECT_FALSE(step.retry);
    EXPECT_EQ(step.error, Error::kIoError);
    step = ClassifyUringReadResult(10, 5000, 4096, 0);
    EXPECT_FALSE(step.retry);
    EXPECT_EQ(step.error, Error::kIoError);
}

TEST(SsdReadPipeline, StreamsRefillsAndRejectsStaleEvents) {
    Arena arena;
    RecordingDriver driver;
    auto plan = Plan(12000);
    ASSERT_EQ(plan.error, Error::kOk);
    ReadPipeline pipeline(std::move(plan.plan), arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    ASSERT_EQ(driver.reads.size(), 2u);
    EXPECT_EQ(pipeline.inflight(), 2u);

    auto first = driver.reads[0].token;
    auto second = driver.reads[1].token;
    EXPECT_EQ(pipeline.ReadDrained(first, 4096), Error::kOk);
    EXPECT_EQ(pipeline.ReadDrained(first, 4096), Error::kStaleEvent);
    ASSERT_EQ(driver.writes.size(), 1u);
    EXPECT_EQ(pipeline.ReadDrained(second, 4096), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(first), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(first), Error::kStaleEvent);
    ASSERT_EQ(driver.reads.size(), 3u);

    auto third = driver.reads[2].token;
    EXPECT_EQ(third.slot_index, first.slot_index);
    EXPECT_GT(third.generation, first.generation);
    EXPECT_EQ(pipeline.ReadDrained(third, 3808), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(second), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(third), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(pipeline.inflight(), 0u);
    EXPECT_EQ(pipeline.Start(), Error::kInvalidState);
}

TEST(SsdReadPipeline, DrainsInflightSlotsOnReadAndWriteFailures) {
    // Short read drains sibling read without issuing writes.
    {
        Arena arena;
        RecordingDriver driver;
        ReadPipeline pipeline(Plan(8192, 13).plan, arena.Buffers(), driver);
        ASSERT_EQ(pipeline.Start(), Error::kOk);
        EXPECT_EQ(pipeline.ReadDrained(driver.reads[0].token, 4095),
                  Error::kOk);
        EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
        EXPECT_TRUE(driver.writes.empty());
        EXPECT_EQ(pipeline.ReadDrained(driver.reads[1].token, 4096),
                  Error::kOk);
        EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
        EXPECT_EQ(pipeline.error(), Error::kIoError);
    }
    // Partial admission failure retains already accepted slot until drained.
    {
        Arena arena;
        RecordingDriver driver;
        driver.reject_read_after = 1;
        ReadPipeline pipeline(Plan(8192).plan, arena.Buffers(), driver);
        EXPECT_EQ(pipeline.Start(), Error::kIoError);
        EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
        EXPECT_EQ(pipeline.inflight(), 1u);
        EXPECT_EQ(pipeline.ReadDrained(driver.reads[0].token, 4096),
                  Error::kOk);
        EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
        EXPECT_EQ(pipeline.inflight(), 0u);
    }
    // Write rejection or transfer error drains sibling slot without refilling.
    {
        Arena arena;
        RecordingDriver driver;
        driver.reject_write = true;
        ReadPipeline pipeline(Plan(8192).plan, arena.Buffers(), driver);
        ASSERT_EQ(pipeline.Start(), Error::kOk);
        EXPECT_EQ(pipeline.ReadDrained(driver.reads[0].token, 4096),
                  Error::kOk);
        EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
        EXPECT_EQ(pipeline.ReadDrained(driver.reads[1].token, 4096),
                  Error::kOk);
        EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
        EXPECT_EQ(pipeline.error(), Error::kTransferError);
    }
    {
        Arena arena;
        RecordingDriver driver;
        ReadPipeline pipeline(Plan(12000).plan, arena.Buffers(), driver);
        ASSERT_EQ(pipeline.Start(), Error::kOk);
        ASSERT_EQ(pipeline.ReadDrained(driver.reads[0].token, 4096),
                  Error::kOk);
        ASSERT_EQ(pipeline.ReadDrained(driver.reads[1].token, 4096),
                  Error::kOk);
        EXPECT_EQ(pipeline.WriteDrained(driver.writes[0].token,
                                        Error::kTransferError),
                  Error::kOk);
        EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
        EXPECT_EQ(driver.reads.size(), 2u);
        EXPECT_EQ(pipeline.WriteDrained(driver.writes[1].token), Error::kOk);
        EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
    }
}

TEST(SsdReadPipeline, RejectsInvalidArenaOrTamperedPlanAndHandlesEmptyPlan) {
    Arena arena;
    for (auto buffers :
         {std::vector<SlotBuffer>{}, std::vector<SlotBuffer>{{nullptr, 4096}},
          std::vector<SlotBuffer>{{arena.first.data(), 1}},
          std::vector<SlotBuffer>{{arena.first.data() + 1, 4096}},
          std::vector<SlotBuffer>{{arena.first.data(), 4096},
                                  {arena.first.data(), 4096}}}) {
        RecordingDriver driver;
        ReadPipeline pipeline(Plan(1).plan, buffers, driver);
        EXPECT_EQ(pipeline.Start(), Error::kInvalidArgument);
        EXPECT_TRUE(driver.reads.empty());
    }
    for (size_t change = 0; change < 3; ++change) {
        auto plan = Plan(8192).plan;
        if (change == 0)
            ++plan.chunks[0].fragments[0].buffer_offset;
        else if (change == 1)
            --plan.payload_bytes;
        else
            plan.chunks[0].payload_bytes = 8192;
        RecordingDriver driver;
        ReadPipeline pipeline(std::move(plan), arena.Buffers(), driver);
        EXPECT_EQ(pipeline.Start(), Error::kInvalidArgument) << change;
        EXPECT_TRUE(driver.reads.empty()) << change;
    }
    RecordingDriver driver;
    ReadPipeline empty(Plan(0).plan, {}, driver);
    EXPECT_EQ(empty.Start(), Error::kOk);
    EXPECT_EQ(empty.state(), PipelineState::kSucceeded);
    EXPECT_TRUE(driver.reads.empty());
}

TEST(SsdReadPipeline, RestoresRealFileAcrossSlicesWithOutOfOrderCompletions) {
    char path[] = "/tmp/mooncake-ssd-pipeline-XXXXXX";
    const int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    unlink(path);
    std::vector<std::byte> source(12013);
    for (size_t i = 0; i < source.size(); ++i) source[i] = std::byte(i % 251);
    ASSERT_EQ(write(fd, source.data(), source.size()),
              static_cast<ssize_t>(source.size()));

    auto plan = BuildReadPlan(
        {12000, 13, 0, {Destination(5, 6000, 0), Destination(7, 6000, 1)}},
        {4096, 4096, 64});
    ASSERT_EQ(plan.error, Error::kOk);
    Arena arena;
    RecordingDriver driver;
    std::array<std::byte, 6010> first{}, second{};
    ReadPipeline pipeline(std::move(plan.plan), arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);

    size_t read_cursor = 0, write_cursor = 0;
    while (pipeline.state() == PipelineState::kRunning) {
        const size_t read_end = driver.reads.size();
        for (size_t i = read_end; i > read_cursor; --i) {
            const auto entry = driver.reads[i - 1];
            const auto bytes =
                pread(fd, entry.buffer.data, entry.chunk->read_bytes,
                      static_cast<off_t>(entry.chunk->file_offset));
            ASSERT_GE(bytes, 0);
            ASSERT_EQ(pipeline.ReadDrained(entry.token, bytes), Error::kOk);
        }
        read_cursor = read_end;
        const size_t write_end = driver.writes.size();
        for (size_t i = write_end; i > write_cursor; --i) {
            const auto entry = driver.writes[i - 1];
            for (const auto& fragment : entry.chunk->fragments) {
                auto& destination = fragment.destination.destination_index == 0
                                        ? first
                                        : second;
                ASSERT_LE(
                    fragment.destination.offset + fragment.destination.length,
                    destination.size());
                std::memcpy(destination.data() + fragment.destination.offset,
                            entry.buffer.data + fragment.buffer_offset,
                            fragment.destination.length);
            }
            ASSERT_EQ(pipeline.WriteDrained(entry.token), Error::kOk);
        }
        write_cursor = write_end;
    }
    close(fd);
    ASSERT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(std::memcmp(first.data() + 5, source.data() + 13, 6000), 0);
    EXPECT_EQ(std::memcmp(second.data() + 7, source.data() + 6013, 6000), 0);
    EXPECT_EQ(first[4], std::byte{0});
    EXPECT_EQ(second[6007], std::byte{0});
}

}  // namespace
}  // namespace mooncake::ssd_offload
