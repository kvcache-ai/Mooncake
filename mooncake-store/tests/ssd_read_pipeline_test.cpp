// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_pipeline.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstring>
#include <deque>
#include <limits>
#include <vector>

namespace mooncake::ssd_offload {
namespace {

DestinationSlice Destination(uint64_t offset, uint64_t length,
                             uint64_t region = 1) {
    return {region, 1, offset, length};
}

PlanResult Plan(uint64_t bytes, uint64_t value_offset = 0,
                size_t max_chunks = 64) {
    return BuildReadPlan({bytes, value_offset, {{0, {Destination(0, bytes)}}}},
                         {4096, 4096, max_chunks});
}

class RecordingDriver : public PipelineDriver {
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
    void RequestCancel(const SlotToken& token,
                       SlotStage stage) noexcept override {
        cancellations.emplace_back(token, stage);
    }
    std::vector<Submission> reads, writes;
    std::vector<std::pair<SlotToken, SlotStage>> cancellations;
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
        {12000, 13, {{0, {Destination(100, 6000), Destination(200, 6000, 2)}}}},
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
    EXPECT_EQ(second.fragments[1].destination.region_id, 2u);
    EXPECT_EQ(second.fragments[1].destination.offset, 200u);
    EXPECT_EQ(second.fragments[1].buffer_offset, 1917u);
    EXPECT_EQ(second.fragments[1].destination.length, 2179u);
    EXPECT_EQ(result.plan.chunks[2].source_offset, 8179u);
}

TEST(SsdReadPlan, SupportsIndependentSourceRangesAndBufferedIo) {
    auto result = BuildReadPlan(
        {100, 7, {{80, {Destination(3, 10)}}, {5, {Destination(40, 17)}}}},
        {8, 1, 64});
    ASSERT_EQ(result.error, Error::kOk);
    ASSERT_EQ(result.plan.chunks.size(), 5u);
    EXPECT_EQ(result.plan.chunks[0].file_offset, 87u);
    EXPECT_EQ(result.plan.chunks[2].file_offset, 12u);
    EXPECT_EQ(result.plan.payload_bytes, 27u);
    EXPECT_EQ(result.plan.io_bytes, 27u);
}

TEST(SsdReadPlan, RejectsSourceAndDestinationOverflowWithoutPartialPlan) {
    auto outside =
        BuildReadPlan({10, 0, {{8, {Destination(0, 3)}}}}, {4096, 4096, 64});
    EXPECT_EQ(outside.error, Error::kInvalidArgument);
    EXPECT_TRUE(outside.plan.chunks.empty());
    auto overflow = BuildReadPlan({10, 0, {{0, {Destination(UINT64_MAX, 2)}}}},
                                  {4096, 4096, 64});
    EXPECT_EQ(overflow.error, Error::kOverflow);
    EXPECT_TRUE(overflow.plan.chunks.empty());
}

TEST(SsdReadPlan, RejectsUnrepresentableAlignedFileOffsets) {
    auto result = Plan(1, static_cast<uint64_t>(INT64_MAX));
    EXPECT_EQ(result.error, Error::kOverflow);
    EXPECT_TRUE(result.plan.chunks.empty());
}

TEST(SsdReadPlan, RejectsInvalidAlignmentLimitsAndDestinationIdentity) {
    for (const auto options :
         {PlanOptions{0, 4096, 64}, PlanOptions{4096, 0, 64},
          PlanOptions{4096, 3, 64}, PlanOptions{4097, 4096, 64},
          PlanOptions{4096, 4096, 0}}) {
        EXPECT_EQ(
            BuildReadPlan({1, 0, {{0, {Destination(0, 1)}}}}, options).error,
            Error::kInvalidArgument);
    }
    EXPECT_EQ(
        BuildReadPlan({1, 0, {{0, {{0, 1, 0, 1}}}}}, {4096, 4096, 64}).error,
        Error::kInvalidArgument);
    EXPECT_EQ(
        BuildReadPlan({1, 0, {{0, {{1, 0, 0, 1}}}}}, {4096, 4096, 64}).error,
        Error::kInvalidArgument);
}

TEST(SsdReadPlan, BoundsMetadataAndAcceptsZeroByteReads) {
    auto limited = Plan(8192, 13, 2);
    EXPECT_EQ(limited.error, Error::kPlanLimit);
    EXPECT_TRUE(limited.plan.chunks.empty());
    auto empty = Plan(0);
    ASSERT_EQ(empty.error, Error::kOk);
    EXPECT_TRUE(empty.plan.chunks.empty());
    EXPECT_EQ(empty.plan.io_bytes, 0u);
}

TEST(SsdReadPlan, BoundsTotalScatterFragmentsAcrossChunks) {
    auto result = BuildReadPlan({8192, 0, {{0, {Destination(0, 8192)}}}},
                                {4096, 4096, 64, 1});
    EXPECT_EQ(result.error, Error::kPlanLimit);
    EXPECT_TRUE(result.plan.chunks.empty());
    result =
        BuildReadPlan({2, 0, {{0, {Destination(0, 1), Destination(1, 1)}}}},
                      {4096, 4096, 64, 1});
    EXPECT_EQ(result.error, Error::kPlanLimit);
    EXPECT_TRUE(result.plan.chunks.empty());
}

TEST(SsdReadPlan, RejectsOverlappingWritesButAcceptsAdjacentSlices) {
    auto result = BuildReadPlan(
        {100, 0, {{0, {Destination(10, 10)}}, {50, {Destination(15, 10)}}}},
        {4096, 4096, 64});
    EXPECT_EQ(result.error, Error::kInvalidArgument);
    EXPECT_TRUE(result.plan.chunks.empty());
    result = BuildReadPlan(
        {100, 0, {{0, {Destination(10, 10), Destination(20, 10)}}}},
        {4096, 4096, 64});
    EXPECT_EQ(result.error, Error::kOk);
}

TEST(SsdReadPipeline, StreamsAndRefillsBeforeOtherWritesComplete) {
    Arena arena;
    RecordingDriver driver;
    auto plan = Plan(12000);
    ASSERT_EQ(plan.error, Error::kOk);
    ReadPipeline pipeline(1, 1, std::move(plan.plan), arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    ASSERT_EQ(driver.reads.size(), 2u);
    EXPECT_EQ(pipeline.inflight(), 2u);
    auto first = driver.reads[0].token;
    auto second = driver.reads[1].token;
    EXPECT_EQ(pipeline.ReadDrained(first, 4096), Error::kOk);
    ASSERT_EQ(driver.writes.size(), 1u);
    EXPECT_EQ(driver.reads.size(), 2u);
    EXPECT_EQ(pipeline.ReadDrained(second, 4096), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(first), Error::kOk);
    ASSERT_EQ(driver.reads.size(), 3u);
    auto third = driver.reads[2].token;
    EXPECT_EQ(third.slot_index, first.slot_index);
    EXPECT_GT(third.generation, first.generation);
    EXPECT_EQ(pipeline.state(), PipelineState::kRunning);
    EXPECT_EQ(pipeline.ReadDrained(third, 3808), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(second), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(third), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(pipeline.inflight(), 0u);
    EXPECT_EQ(pipeline.completed_bytes(), 12000u);
    EXPECT_EQ(pipeline.Start(), Error::kInvalidState);
}

TEST(SsdReadPipeline, RejectsStaleDuplicateAndWrongAttemptCompletions) {
    Arena arena;
    RecordingDriver driver;
    ReadPipeline pipeline(7, 9, Plan(12000).plan, arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    auto token = driver.reads[0].token;
    auto wrong = token;
    ++wrong.attempt_id;
    EXPECT_EQ(pipeline.ReadDrained(wrong, 4096), Error::kStaleEvent);
    EXPECT_EQ(pipeline.ReadDrained(token, 4096), Error::kOk);
    EXPECT_EQ(pipeline.ReadDrained(token, 4096), Error::kStaleEvent);
    EXPECT_EQ(pipeline.WriteDrained(token), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(token), Error::kStaleEvent);
    EXPECT_EQ(pipeline.ReadDrained(token, 4096), Error::kStaleEvent);
    EXPECT_EQ(pipeline.inflight(), 2u);
    auto second = driver.reads[1].token;
    auto third = driver.reads[2].token;
    EXPECT_EQ(pipeline.ReadDrained(second, 4096), Error::kOk);
    EXPECT_EQ(pipeline.ReadDrained(third, 3808), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(second), Error::kOk);
    EXPECT_EQ(pipeline.WriteDrained(third), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
}

TEST(SsdReadPipeline, CancelWaitsForAcceptedWritesAndSuppressesNewWrites) {
    Arena arena;
    RecordingDriver driver;
    ReadPipeline pipeline(1, 1, Plan(12000).plan, arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    auto read = driver.reads[1].token;
    auto write = driver.reads[0].token;
    ASSERT_EQ(pipeline.ReadDrained(write, 4096), Error::kOk);
    pipeline.Cancel();
    EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
    EXPECT_EQ(pipeline.inflight(), 2u);
    EXPECT_EQ(driver.cancellations.size(), 2u);
    pipeline.Cancel();
    EXPECT_EQ(driver.cancellations.size(), 2u);
    EXPECT_EQ(pipeline.ReadDrained(read, 4096), Error::kOk);
    EXPECT_EQ(driver.writes.size(), 1u);
    EXPECT_EQ(driver.reads.size(), 2u);
    EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
    EXPECT_EQ(pipeline.WriteDrained(write), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kCancelled);
    EXPECT_EQ(pipeline.error(), Error::kCancelled);
    EXPECT_EQ(pipeline.inflight(), 0u);
}

TEST(SsdReadPipeline, ShortReadFailsAndDrainsOtherAcceptedRead) {
    Arena arena;
    RecordingDriver driver;
    ReadPipeline pipeline(1, 1, Plan(8192, 13).plan, arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    EXPECT_EQ(pipeline.ReadDrained(driver.reads[0].token, 4095), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
    EXPECT_TRUE(driver.writes.empty());
    EXPECT_EQ(pipeline.ReadDrained(driver.reads[1].token, 4096), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
    EXPECT_EQ(pipeline.error(), Error::kIoError);
    EXPECT_TRUE(driver.writes.empty());
}

TEST(SsdReadPipeline, ReadSubmissionRejectionStartsNoTransfer) {
    Arena arena;
    RecordingDriver driver;
    driver.reject_read = true;
    ReadPipeline pipeline(1, 1, Plan(8192).plan, arena.Buffers(), driver);
    EXPECT_EQ(pipeline.Start(), Error::kIoError);
    EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
    EXPECT_EQ(pipeline.inflight(), 0u);
    EXPECT_TRUE(driver.writes.empty());
}

TEST(SsdReadPipeline, LaterReadRejectionRetainsAcceptedSlotUntilDrain) {
    Arena arena;
    RecordingDriver driver;
    driver.reject_read_after = 1;
    ReadPipeline pipeline(1, 1, Plan(8192).plan, arena.Buffers(), driver);
    EXPECT_EQ(pipeline.Start(), Error::kIoError);
    ASSERT_EQ(driver.reads.size(), 1u);
    EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
    EXPECT_EQ(pipeline.inflight(), 1u);
    EXPECT_EQ(driver.cancellations.size(), 1u);
    EXPECT_EQ(pipeline.ReadDrained(driver.reads[0].token, 4096), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
    EXPECT_EQ(pipeline.inflight(), 0u);
    EXPECT_TRUE(driver.writes.empty());
}

TEST(SsdReadPipeline, WriteSubmissionRejectionDrainsOtherIo) {
    Arena arena;
    RecordingDriver driver;
    driver.reject_write = true;
    ReadPipeline pipeline(1, 1, Plan(8192).plan, arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    EXPECT_EQ(pipeline.ReadDrained(driver.reads[0].token, 4096), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
    EXPECT_EQ(pipeline.ReadDrained(driver.reads[1].token, 4096), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
    EXPECT_EQ(pipeline.error(), Error::kTransferError);
}

TEST(SsdReadPipeline, TransferFailureDoesNotReuseOtherInFlightSlot) {
    Arena arena;
    RecordingDriver driver;
    ReadPipeline pipeline(1, 1, Plan(12000).plan, arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    for (size_t i = 0; i < 2; ++i)
        ASSERT_EQ(pipeline.ReadDrained(driver.reads[i].token, 4096),
                  Error::kOk);
    EXPECT_EQ(
        pipeline.WriteDrained(driver.writes[0].token, Error::kTransferError),
        Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kDraining);
    EXPECT_EQ(driver.reads.size(), 2u);
    EXPECT_EQ(pipeline.WriteDrained(driver.writes[1].token), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
}

TEST(SsdReadPipeline, RejectsInvalidArenaBeforeSubmittingIo) {
    Arena arena;
    for (auto buffers :
         {std::vector<SlotBuffer>{}, std::vector<SlotBuffer>{{nullptr, 4096}},
          std::vector<SlotBuffer>{{arena.first.data(), 1}},
          std::vector<SlotBuffer>{{arena.first.data() + 1, 4096}},
          std::vector<SlotBuffer>{{arena.first.data(), 4096},
                                  {arena.first.data(), 4096}}}) {
        RecordingDriver driver;
        ReadPipeline pipeline(1, 1, Plan(1).plan, buffers, driver);
        EXPECT_EQ(pipeline.Start(), Error::kInvalidArgument);
        EXPECT_TRUE(driver.reads.empty());
    }
}

TEST(SsdReadPipeline, RejectsTamperedPlanBeforeSubmittingIo) {
    Arena arena;
    for (size_t change = 0; change < 4; ++change) {
        auto plan = Plan(8192).plan;
        if (change == 0)
            plan.chunks[1].fragments[0].destination.offset = 0;
        else if (change == 1)
            ++plan.chunks[0].fragments[0].buffer_offset;
        else if (change == 2)
            --plan.payload_bytes;
        else
            plan.chunks[0].payload_bytes = 8192;
        RecordingDriver driver;
        ReadPipeline pipeline(1, 1, std::move(plan), arena.Buffers(), driver);
        EXPECT_EQ(pipeline.Start(), Error::kInvalidArgument) << change;
        EXPECT_TRUE(driver.reads.empty()) << change;
    }
}

TEST(SsdReadPipeline, EmptyPlanCompletesWithoutArenaOrDriverWork) {
    RecordingDriver driver;
    ReadPipeline pipeline(1, 1, Plan(0).plan, {}, driver);
    EXPECT_EQ(pipeline.Start(), Error::kOk);
    EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
    pipeline.Cancel();
    EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_TRUE(driver.reads.empty());
}

TEST(SsdReadPipeline, CancelBeforeStartDoesNotSubmitWork) {
    Arena arena;
    RecordingDriver driver;
    ReadPipeline pipeline(1, 1, Plan(8192).plan, arena.Buffers(), driver);
    pipeline.Cancel();
    EXPECT_EQ(pipeline.state(), PipelineState::kCancelled);
    EXPECT_EQ(pipeline.Start(), Error::kInvalidState);
    EXPECT_TRUE(driver.reads.empty());
}

class TemporaryFile {
   public:
    TemporaryFile() {
        char path[] = "/tmp/mooncake-ssd-pipeline-XXXXXX";
        fd = mkstemp(path);
        if (fd >= 0) unlink(path);
    }
    ~TemporaryFile() {
        if (fd >= 0) close(fd);
    }
    int fd{-1};
};

// Exercises the production planner/scheduler against actual file bytes.
// The queue controls event ordering; it does not claim to emulate RNIC drain.
TEST(SsdReadPipeline, RestoresRealFileAcrossSlicesAndAcceptsEofPadding) {
    TemporaryFile file;
    ASSERT_GE(file.fd, 0);
    std::vector<std::byte> source(12013);
    for (size_t i = 0; i < source.size(); ++i) source[i] = std::byte(i % 251);
    ASSERT_EQ(write(file.fd, source.data(), source.size()),
              static_cast<ssize_t>(source.size()));
    auto plan = BuildReadPlan(
        {12000, 13, {{0, {Destination(5, 6000), Destination(7, 6000, 2)}}}},
        {4096, 4096, 64});
    ASSERT_EQ(plan.error, Error::kOk);
    Arena arena;
    RecordingDriver driver;
    std::array<std::byte, 6010> first{}, second{};
    ReadPipeline pipeline(1, 1, std::move(plan.plan), arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    size_t read_index = 0, write_index = 0;
    while (pipeline.state() == PipelineState::kRunning) {
        if (read_index < driver.reads.size()) {
            const auto entry = driver.reads[read_index++];
            const auto bytes =
                pread(file.fd, entry.buffer.data, entry.chunk->read_bytes,
                      static_cast<off_t>(entry.chunk->file_offset));
            ASSERT_GE(bytes, 0);
            ASSERT_EQ(pipeline.ReadDrained(entry.token, bytes), Error::kOk);
        }
        if (write_index < driver.writes.size()) {
            const auto entry = driver.writes[write_index++];
            for (const auto& fragment : entry.chunk->fragments) {
                auto& destination =
                    fragment.destination.region_id == 1 ? first : second;
                const auto offset = fragment.destination.offset;
                const auto length = fragment.destination.length;
                ASSERT_LE(offset + length, destination.size());
                std::memcpy(destination.data() + offset,
                            entry.buffer.data + fragment.buffer_offset, length);
            }
            ASSERT_EQ(pipeline.WriteDrained(entry.token), Error::kOk);
        }
        ASSERT_TRUE(read_index < driver.reads.size() ||
                    write_index < driver.writes.size() ||
                    pipeline.state() == PipelineState::kSucceeded);
    }
    ASSERT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(std::memcmp(first.data() + 5, source.data() + 13, 6000), 0);
    EXPECT_EQ(std::memcmp(second.data() + 7, source.data() + 6013, 6000), 0);
    EXPECT_EQ(first[4], std::byte{0});
    EXPECT_EQ(second[6007], std::byte{0});
}

TEST(SsdReadPipeline, RestoresIndependentRangesWithReverseCompletions) {
    TemporaryFile file;
    ASSERT_GE(file.fd, 0);
    std::vector<std::byte> source(10023);
    for (size_t i = 0; i < source.size(); ++i) source[i] = std::byte(i % 251);
    ASSERT_EQ(write(file.fd, source.data(), source.size()),
              static_cast<ssize_t>(source.size()));
    auto plan = BuildReadPlan(
        {10000,
         23,
         {{5000, {Destination(5, 3000)}}, {17, {Destination(7, 5000, 2)}}}},
        {4096, 4096, 64});
    ASSERT_EQ(plan.error, Error::kOk);
    Arena arena;
    RecordingDriver driver;
    std::vector<std::byte> first(3010), second(5010);
    ReadPipeline pipeline(1, 1, std::move(plan.plan), arena.Buffers(), driver);
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    size_t read_cursor = 0, write_cursor = 0;
    for (size_t round = 0;
         round < 8 && pipeline.state() == PipelineState::kRunning; ++round) {
        const size_t read_end = driver.reads.size();
        for (size_t i = read_end; i > read_cursor; --i) {
            const auto entry = driver.reads[i - 1];
            const auto bytes =
                pread(file.fd, entry.buffer.data, entry.chunk->read_bytes,
                      static_cast<off_t>(entry.chunk->file_offset));
            ASSERT_GE(bytes, 0);
            ASSERT_EQ(pipeline.ReadDrained(entry.token, bytes), Error::kOk);
        }
        read_cursor = read_end;
        const size_t write_end = driver.writes.size();
        for (size_t i = write_end; i > write_cursor; --i) {
            const auto entry = driver.writes[i - 1];
            for (const auto& fragment : entry.chunk->fragments) {
                auto& destination =
                    fragment.destination.region_id == 1 ? first : second;
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
    ASSERT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(std::memcmp(first.data() + 5, source.data() + 5023, 3000), 0);
    EXPECT_EQ(std::memcmp(second.data() + 7, source.data() + 40, 5000), 0);
    EXPECT_EQ(first[4], std::byte{0});
    EXPECT_EQ(second[5007], std::byte{0});
}

}  // namespace
}  // namespace mooncake::ssd_offload
