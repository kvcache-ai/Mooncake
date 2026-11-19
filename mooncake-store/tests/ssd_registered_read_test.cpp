// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_protocol.h"
#include "ssd_pipeline_io.h"

#include <array>
#include <algorithm>
#include <cstring>
#include <unistd.h>
#include <gtest/gtest.h>

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
class SourceFile {
   public:
    SourceFile() {
        char name[] = "/tmp/mooncake-ssd-registered-XXXXXX";
        fd = mkstemp(name);
        path = name;
        for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = std::byte(i % 251);
        if (fd >= 0) written = write(fd, bytes.data(), bytes.size());
    }
    ~SourceFile() {
        if (fd >= 0) close(fd);
        unlink(path.c_str());
    }
    int fd{-1};
    ssize_t written{-1};
    std::string path;
    std::array<std::byte, 12017> bytes;
};
// A synchronous CPU test transport. It writes only ranges authorized by the
// published lease and does not claim to implement RDMA or network drain.
class RegisteredCpuTarget final : public DrainedWriteTarget {
   public:
    RegisteredCpuTarget(std::vector<RegisteredDestination> slices,
                        std::vector<ResolvedDestination> views)
        : slices_(std::move(slices)), views_(std::move(views)) {}
    Error WriteAndDrain(const ReadChunk& chunk,
                        SlotBuffer buffer) noexcept override {
        for (const auto& fragment : chunk.fragments) {
            bool found = false;
            const auto& dst = fragment.destination;
            if (fragment.buffer_offset > buffer.capacity ||
                dst.length > buffer.capacity - fragment.buffer_offset)
                return Error::kTransferError;
            for (size_t i = 0; i < slices_.size(); ++i) {
                const auto& slice = slices_[i];
                const auto& view = views_[i];
                if (slice.region.region_id != dst.region_id ||
                    slice.region.generation != dst.generation ||
                    dst.offset < slice.offset ||
                    dst.offset - slice.offset > view.length ||
                    dst.length > view.length - (dst.offset - slice.offset))
                    continue;
                if (view.memory != DestinationMemory::kHost)
                    return Error::kTransferError;
                std::memcpy(reinterpret_cast<void*>(view.address + dst.offset -
                                                    slice.offset),
                            buffer.data + fragment.buffer_offset, dst.length);
                found = true;
                break;
            }
            if (!found) return Error::kTransferError;
        }
        return Error::kOk;
    }

   private:
    std::vector<RegisteredDestination> slices_;
    std::vector<ResolvedDestination> views_;
};

void Drive(ReadPipeline& pipeline, ThreadedPipelineDriver& driver) {
    while (pipeline.inflight()) {
        IoCompletion event;
        if (!driver.WaitForEvent(event, std::chrono::seconds(1))) continue;
        if (event.stage == SlotStage::kReading)
            EXPECT_EQ(
                pipeline.ReadDrained(event.token, event.bytes, event.result),
                Error::kOk);
        else
            EXPECT_EQ(pipeline.WriteDrained(event.token, event.result),
                      Error::kOk);
    }
}

TEST(SsdRegisteredRead,
     RestoresRealFileScatterThroughCheckedProtocolAndPinnedLease) {
    SourceFile file;
    ASSERT_GE(file.fd, 0);
    ASSERT_EQ(file.written, ssize_t(file.bytes.size()));
    auto destination = std::make_shared<std::array<std::byte, 12032>>();
    destination->fill(std::byte{0xA5});
    const auto base = reinterpret_cast<uint64_t>(destination->data());
    DestinationRegistry registry(requester, "cpu-test");
    auto first = registry.Register({base + 16, 5000, base, destination->size()},
                                   {11, 12}, destination);
    auto second = registry.Register(
        {base + 5016, 7000, base, destination->size()}, {21, 22}, destination);
    ASSERT_EQ(first.error, DestinationError::kOk);
    ASSERT_EQ(second.error, DestinationError::kOk);
    SsdReadRequestV1 request{
        1,
        requester,
        {owner, 1, 1},
        "tenant-a",
        100,
        0,
        {{"key",
          12000,
          std::nullopt,
          {{1234, {{first.region, 0, 3000}, {second.region, 10, 3500}}}}}}};
    ASSERT_EQ(ValidateSsdReadRequestV1(request, owner), DestinationError::kOk);
    const auto slices = request.objects[0].ranges[0].destinations;
    auto acquired = registry.Acquire(request.attempt, slices);
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
    auto opened = OpenPosixReadSource(file.path, {12000, 17, 1});
    ASSERT_EQ(opened.error, Error::kOk);
    ReadRange range{1234, {}};
    for (const auto& slice : slices)
        range.destinations.push_back({slice.region.region_id,
                                      slice.region.generation, slice.offset,
                                      slice.length});
    auto planned = BuildReadPlan({12000, 17, {range}}, {4096, 1, 64});
    ASSERT_EQ(planned.error, Error::kOk);
    alignas(4096) std::array<std::byte, 8192> arena{};
    std::vector<SlotBuffer> buffers{{arena.data(), 4096},
                                    {arena.data() + 4096, 4096}};
    BoundedExecutor reads(2, 8), writes(2, 8);
    auto target = std::make_shared<RegisteredCpuTarget>(slices, views);
    std::unique_ptr<ReadPipeline> pipeline;
    ThreadedPipelineDriver driver(opened.source, target, reads, writes, 2);
    pipeline = std::make_unique<ReadPipeline>(1, 1, std::move(planned.plan),
                                              buffers, driver);
    EXPECT_EQ(pipeline->Start(), Error::kOk);
    Drive(*pipeline, driver);
    ASSERT_EQ(pipeline->state(), PipelineState::kSucceeded);
    auto expected = std::array<std::byte, 12032>{};
    expected.fill(std::byte{0xA5});
    std::copy_n(file.bytes.begin() + 17 + 1234, 3000, expected.begin() + 16);
    std::copy_n(file.bytes.begin() + 17 + 1234 + 3000, 3500,
                expected.begin() + 5026);
    EXPECT_EQ(*destination, expected);
    SsdReadResultV1 result{
        1,
        request.attempt,
        true,
        {{0, AttemptOutcome::kSucceeded, 6500, std::nullopt}},
        requester};
    ASSERT_EQ(ValidateSsdReadResultV1(request, result), DestinationError::kOk);
    EXPECT_EQ(
        acquired.attempt.Complete({request.attempt, AttemptOutcome::kSucceeded,
                                   6500, true, requester}),
        DestinationError::kOk);
    EXPECT_TRUE(acquired.attempt.snapshot().publishable);
}

TEST(SsdRegisteredRead,
     CancellationDrainsAcceptedFileIoBeforeDestinationRetirement) {
    SourceFile file;
    ASSERT_GE(file.fd, 0);
    ASSERT_EQ(file.written, ssize_t(file.bytes.size()));
    auto destination = std::make_shared<std::array<std::byte, 12000>>();
    destination->fill(std::byte{0xA5});
    const auto base = reinterpret_cast<uint64_t>(destination->data());
    DestinationRegistry registry(requester, "cpu-test");
    auto region =
        registry.Register({base, 12000, base, 12000}, {11, 12}, destination);
    ASSERT_EQ(region.error, DestinationError::kOk);
    std::vector<RegisteredDestination> slices{{region.region, 0, 12000}};
    auto acquired = registry.Acquire({owner, 1, 1}, slices);
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
    auto opened = OpenPosixReadSource(file.path, {12000, 17, 1});
    ASSERT_EQ(opened.error, Error::kOk);
    auto planned = BuildReadPlan(
        {12000,
         17,
         {{0,
           {{region.region.region_id, region.region.generation, 0, 12000}}}}},
        {4096, 1, 64});
    ASSERT_EQ(planned.error, Error::kOk);
    alignas(4096) std::array<std::byte, 8192> arena{};
    BoundedExecutor reads(2, 8), writes(2, 8);
    auto target = std::make_shared<RegisteredCpuTarget>(slices, views);
    std::unique_ptr<ReadPipeline> pipeline;
    ThreadedPipelineDriver driver(opened.source, target, reads, writes, 2);
    pipeline = std::make_unique<ReadPipeline>(
        1, 1, std::move(planned.plan),
        std::vector<SlotBuffer>{{arena.data(), 4096},
                                {arena.data() + 4096, 4096}},
        driver);
    EXPECT_EQ(pipeline->Start(), Error::kOk);
    EXPECT_EQ(acquired.attempt.Timeout(), DestinationError::kOk);
    pipeline->Cancel();
    EXPECT_EQ(registry.BeginRevoke(region.region), DestinationError::kOk);
    EXPECT_EQ(registry.Retire(region.region), DestinationError::kBusy);
    Drive(*pipeline, driver);
    EXPECT_EQ(pipeline->state(), PipelineState::kCancelled);
    EXPECT_TRUE(std::all_of(destination->begin(), destination->end(),
                            [](std::byte b) { return b == std::byte{0xA5}; }));
    EXPECT_EQ(
        acquired.attempt.Complete(
            {{owner, 1, 1}, AttemptOutcome::kCancelled, 0, true, requester}),
        DestinationError::kOk);
    EXPECT_EQ(registry.Retire(region.region), DestinationError::kOk);
    EXPECT_FALSE(acquired.attempt.snapshot().publishable);
}
}  // namespace
}  // namespace mooncake::ssd_offload
