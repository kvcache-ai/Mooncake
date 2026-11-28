// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include <gtest/gtest.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <thread>
#include <sstream>
#include "tent/metrics/tent_metrics.h"
#include "tent/common/config_lifecycle.h"

#include "nds_test_backend.h"
#include "tent/common/config.h"
#include "tent/transfer_engine.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake::tent {
namespace {
using nds_test::state;
constexpr size_t kSize = 48 * 1024 * 1024;

void expectFileRange(const std::string& file, size_t offset, size_t length,
                     unsigned char expected) {
    int fd = open(file.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    std::vector<unsigned char> actual(length);
    ssize_t bytes = pread(fd, actual.data(), length, offset);
    EXPECT_EQ(close(fd), 0);
    ASSERT_EQ(bytes, static_cast<ssize_t>(length));
    EXPECT_EQ(actual, std::vector<unsigned char>(length, expected));
}

class NdsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        state = {};
        ASSERT_EQ(posix_memalign(&buffer, 4096, kSize), 0);
        state.npu_base = reinterpret_cast<uint64_t>(buffer);
        state.npu_size = kSize;
        memset(buffer, 0x5a, kSize);
        auto name =
            (std::filesystem::temp_directory_path() / "tent-nds-test-XXXXXX")
                .string();
        int fd = mkstemp(name.data());
        ASSERT_GE(fd, 0);
        path = name;
        ASSERT_EQ(ftruncate(fd, kSize), 0);
        close(fd);
        cfg = std::make_shared<Config>();
        cfg->set("metadata_type", "p2p");
        cfg->set("rpc_server_hostname", "127.0.0.1");
        cfg->set("rpc_server_port", "0");
        cfg->set("log_level", "error");
        for (const auto* name : {"tcp", "rdma", "shm", "io_uring", "gds",
                                 "nvlink", "mnnvl", "ascend_direct"})
            cfg->set(std::string("transports/") + name + "/enable", false);
        cfg->set("transports/nds/enable", true);
        cfg->set("transports/nds/io_batch_depth", 2);
        startEngine();
    }

    void startEngine() {
        engine.reset();
        engine = std::make_unique<TransferEngine>(cfg);
        ASSERT_TRUE(engine->available());
        MemoryOptions options;
        options.location =
            "npu:0";  // CPU storage emulates registered HBM in this test.
        options.type = NDS;
        ASSERT_TRUE(engine->registerLocalMemory(buffer, kSize, options).ok());
        ASSERT_TRUE(engine->openSegment(segment, "file://" + path).ok());
    }

    void TearDown() override {
        state.destroy_error = state.unpin_error = false;
        engine.reset();
        EXPECT_EQ(state.live_contexts, 0);
        EXPECT_EQ(state.live_files, 0);
        EXPECT_EQ(state.live_topologies, 0);
        EXPECT_EQ(state.pins, state.unpins);
        EXPECT_EQ(state.opens, state.closes);
        free(buffer);
        unlink(path.c_str());
    }

    Request request(Request::OpCode opcode, size_t length = 4096,
                    size_t offset = 0) {
        return request(opcode, length, offset, offset);
    }

    Request request(Request::OpCode opcode, size_t length, size_t source_offset,
                    size_t file_offset) {
        Request req{};
        req.opcode = opcode;
        req.source = static_cast<char*>(buffer) + source_offset;
        req.target_id = segment;
        req.target_offset = file_offset;
        req.length = length;
        return req;
    }

    TransferStatus poll(BatchID batch, size_t task = 0) {
        TransferStatus status{};
        for (int i = 0; i < 16; ++i) {
            auto result = engine->getTransferStatus(batch, task, status);
            EXPECT_TRUE(result.ok()) << result.ToString();
            if (!result.ok() || status.s != PENDING) break;
        }
        return status;
    }

    std::shared_ptr<Config> cfg;
    std::unique_ptr<TransferEngine> engine;
    void* buffer = nullptr;
    std::string path;
    SegmentID segment = 0;
};

TEST_F(NdsTest, WindowedRoundTripAndIndependentTasks) {
    auto batch = engine->allocateBatch(3);
    ASSERT_TRUE(
        engine->submitTransfer(batch, {request(Request::WRITE, kSize)}).ok());
    auto status = poll(batch);
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kSize);
    EXPECT_EQ(state.submits, 2);
    EXPECT_EQ(state.registered_ios, 3);
    memset(buffer, 0, kSize);
    // Reuse the batch and verify stable task IDs for appended requests.
    ASSERT_TRUE(
        engine
            ->submitTransfer(batch, {request(Request::READ, 4096),
                                     request(Request::READ, 8192, 4096)})
            .ok());
    status = poll(batch, 2);
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 8192);
    status = poll(batch, 1);
    EXPECT_EQ(status.transferred_bytes, 4096);
    for (size_t i = 0; i < 12288; ++i)
        ASSERT_EQ(static_cast<unsigned char*>(buffer)[i], 0x5a);
    ASSERT_TRUE(engine->freeBatch(batch).ok());
}

TEST_F(NdsTest, CoalescesMixedHbmRangesForWriteAndRead) {
    cfg->set("merge_requests", false);
    ASSERT_NO_FATAL_FAILURE(startEngine());

    auto* hbm = static_cast<unsigned char*>(buffer);
    std::vector<Request> requests = {
        request(Request::WRITE),
        request(Request::WRITE, 4096, 8192, 4096),
        request(Request::WRITE, 4096, 12288, 8192),
    };
    const unsigned char values[] = {0x11, 0x22, 0x33};
    for (size_t i = 0; i < requests.size(); ++i)
        memset(requests[i].source, values[i], requests[i].length);

    auto batch = engine->allocateBatch(2 * requests.size());
    ASSERT_TRUE(engine->submitTransfer(batch, requests).ok());
    EXPECT_EQ(state.submitted_iov_lengths,
              (std::vector<std::vector<size_t>>{{4096, 8192}}));
    for (size_t i = 0; i < requests.size(); ++i) {
        auto status = poll(batch, i);
        ASSERT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, requests[i].length);
        expectFileRange(path, requests[i].target_offset, requests[i].length,
                        values[i]);
        memset(requests[i].source, 0, requests[i].length);
        requests[i].opcode = Request::READ;
    }

    ASSERT_TRUE(engine->submitTransfer(batch, requests).ok());
    EXPECT_EQ(state.submitted_iov_lengths,
              (std::vector<std::vector<size_t>>{{4096, 8192}, {4096, 8192}}));
    for (size_t i = 0; i < requests.size(); ++i) {
        auto status = poll(batch, requests.size() + i);
        ASSERT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, requests[i].length);
        auto* begin = static_cast<const unsigned char*>(requests[i].source);
        EXPECT_TRUE(
            std::all_of(begin, begin + requests[i].length,
                        [value = values[i]](auto b) { return b == value; }));
    }
    EXPECT_TRUE(
        std::all_of(hbm + 4096, hbm + 8192, [](auto b) { return b == 0x5a; }));
    EXPECT_TRUE(engine->freeBatch(batch).ok());
}

TEST_F(NdsTest, CoalescedRequestsSplitAtIoBytes) {
    cfg->set("merge_requests", false);
    cfg->set("transports/nds/io_bytes", 8192);
    ASSERT_NO_FATAL_FAILURE(startEngine());

    auto* hbm = static_cast<char*>(buffer);
    const std::vector<Request> requests = {
        request(Request::WRITE),
        request(Request::WRITE, 8192, 8192, 4096),
        request(Request::WRITE, 4096, 16384, 12288),
    };
    memset(hbm, 0x11, 4096);
    memset(hbm + 8192, 0x22, 4096);
    memset(hbm + 12288, 0x33, 4096);
    memset(hbm + 16384, 0x44, 4096);

    auto batch = engine->allocateBatch(requests.size());
    ASSERT_TRUE(engine->submitTransfer(batch, requests).ok());
    EXPECT_EQ(state.submitted_iov_lengths,
              (std::vector<std::vector<size_t>>{{4096, 4096}, {8192}}));
    TransferStatus status{};
    ASSERT_TRUE(engine->getTransferStatus(batch, 1, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(status.transferred_bytes, 4096);
    for (size_t i = 0; i < requests.size(); ++i) {
        status = poll(batch, i);
        EXPECT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, requests[i].length);
    }
    expectFileRange(path, 0, 4096, 0x11);
    expectFileRange(path, 4096, 4096, 0x22);
    expectFileRange(path, 8192, 4096, 0x33);
    expectFileRange(path, 12288, 4096, 0x44);
    EXPECT_TRUE(engine->freeBatch(batch).ok());
}

TEST_F(NdsTest, PartialCompletionSurvivesZeroEventPolls) {
    auto batch = engine->allocateBatch(1);
    ASSERT_TRUE(
        engine->submitTransfer(batch, {request(Request::READ, kSize)}).ok());
    TransferStatus status{};
    ASSERT_TRUE(engine->getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(status.transferred_bytes, 16 * 1024 * 1024);
    EXPECT_EQ(state.submits, 2);  // Refill before the first window drains.
    EXPECT_EQ(state.max_inflight, 2);
    state.pause = true;
    ASSERT_TRUE(engine->getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(status.transferred_bytes, 16 * 1024 * 1024);
    state.pause = false;
    EXPECT_EQ(poll(batch).s, COMPLETED);
}

TEST_F(NdsTest, PartialWriteSubmitDrainsBeforeFailureWithoutFallback) {
    state.partial_submit = true;
    auto batch = engine->allocateBatch(1);
    ASSERT_TRUE(
        engine->submitTransfer(batch, {request(Request::WRITE, kSize)}).ok());
    EXPECT_EQ(state.drains, 1);
    EXPECT_EQ(poll(batch).s, FAILED);
    EXPECT_EQ(poll(batch).s, FAILED);
    EXPECT_EQ(state.submits, 1);
    EXPECT_EQ(state.live_contexts, 0);
    expectFileRange(path, 0, 4096, 0x5a);
    expectFileRange(path, kSize - 4096, 4096, 0);
}

TEST_F(NdsTest, NegativeSubmitLeavesFileUnmodified) {
    state.submit_error = -EIO;
    auto batch = engine->allocateBatch(1);
    ASSERT_TRUE(engine->submitTransfer(batch, {request(Request::WRITE)}).ok());
    EXPECT_EQ(poll(batch).s, FAILED);
    EXPECT_EQ(state.submitted_ios, 0);
    EXPECT_EQ(state.drains, 0);
    EXPECT_EQ(state.live_contexts, 0);
    expectFileRange(path, 0, 4096, 0);
    ASSERT_TRUE(engine->freeBatch(batch).ok());
}

TEST_F(NdsTest, DestroyErrorConsumesContextWithoutRetry) {
    state.partial_submit = state.destroy_error = true;
    auto batch = engine->allocateBatch(1);
    ASSERT_TRUE(
        engine->submitTransfer(batch, {request(Request::READ, kSize)}).ok());
    TransferStatus status{};
    EXPECT_TRUE(engine->getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
    EXPECT_EQ(state.live_contexts, 0);
    EXPECT_EQ(state.unpins, 0);
    EXPECT_TRUE(engine->unregisterLocalMemory(buffer, kSize).ok());
    EXPECT_EQ(state.live_contexts, 0);
    EXPECT_EQ(state.unpins, 1);
    EXPECT_EQ(poll(batch).s, FAILED);
}

TEST_F(NdsTest, PollErrorAndMalformedCookieFenceOutstandingIo) {
    for (bool corrupt_cookie : {false, true}) {
        state.poll_error = !corrupt_cookie;
        state.bad_cookie = corrupt_cookie;
        auto batch = engine->allocateBatch(1);
        ASSERT_TRUE(
            engine->submitTransfer(batch, {request(Request::READ, kSize)})
                .ok());
        EXPECT_EQ(poll(batch).s, FAILED);
        EXPECT_EQ(state.live_contexts, 0);
        EXPECT_TRUE(engine->freeBatch(batch).ok());
    }
}

TEST_F(NdsTest, UnpinFailureCanBeRetried) {
    state.unpin_error = true;
    EXPECT_FALSE(engine->unregisterLocalMemory(buffer, kSize).ok());
    state.unpin_error = false;
    EXPECT_TRUE(engine->unregisterLocalMemory(buffer, kSize).ok());
    EXPECT_EQ(state.unpins, 1);
}

TEST_F(NdsTest, SeparateRequestsValidateEachBufferRange) {
    auto first = request(Request::WRITE);
    auto second = request(Request::WRITE);
    second.source = static_cast<char*>(buffer) + kSize / 2;
    second.target_offset = 4096;
    auto batch = engine->allocateBatch(4);
    ASSERT_TRUE(engine->submitTransfer(batch, {first, second}).ok());
    EXPECT_EQ(poll(batch, 0).transferred_bytes, 4096);
    EXPECT_EQ(poll(batch, 1).s, COMPLETED);
    EXPECT_EQ(state.submitted_iov_lengths,
              (std::vector<std::vector<size_t>>{{4096, 4096}}));
    first.length = kSize + 4096;
    second.source = static_cast<char*>(buffer) + kSize;
    ASSERT_TRUE(engine->submitTransfer(batch, {first, second}).ok());
    EXPECT_EQ(poll(batch, 2).s, FAILED);
    EXPECT_EQ(poll(batch, 3).s, FAILED);
    EXPECT_EQ(state.submitted_ios, 1);
}

TEST_F(NdsTest, RepeatedTargetRequestsUseSegmentLengthAndIndependentBounds) {
    auto first = request(Request::WRITE, 4096);
    auto second = request(Request::WRITE, 8192, 16384);
    auto tail = request(Request::WRITE, 4096, kSize - 4096);
    auto outside = request(Request::WRITE, 4096, 32768);
    outside.target_offset = kSize;
    memset(first.source, 0x11, first.length);
    memset(second.source, 0x22, second.length);
    memset(tail.source, 0x33, tail.length);

    auto batch = engine->allocateBatch(4);
    ASSERT_TRUE(
        engine->submitTransfer(batch, {first, second, tail, outside}).ok());
    EXPECT_EQ(poll(batch, 0).s, COMPLETED);
    EXPECT_EQ(poll(batch, 0).transferred_bytes, 4096);
    EXPECT_EQ(poll(batch, 1).s, COMPLETED);
    EXPECT_EQ(poll(batch, 1).transferred_bytes, 8192);
    EXPECT_EQ(poll(batch, 2).s, COMPLETED);
    EXPECT_EQ(poll(batch, 2).transferred_bytes, 4096);
    EXPECT_EQ(poll(batch, 3).s, FAILED);
    EXPECT_EQ(state.submitted_ios, 3);
    ASSERT_TRUE(engine->freeBatch(batch).ok());
    expectFileRange(path, 0, 4096, 0x11);
    expectFileRange(path, 16384, 8192, 0x22);
    expectFileRange(path, kSize - 4096, 4096, 0x33);
}

TEST_F(NdsTest, MixedTargetsDoNotShareValidatedPathOrLength) {
    constexpr size_t other_size = 16 * 1024;
    auto other_name =
        (std::filesystem::temp_directory_path() / "tent-nds-test-XXXXXX")
            .string();
    int other_fd = mkstemp(other_name.data());
    ASSERT_GE(other_fd, 0);
    ASSERT_EQ(ftruncate(other_fd, other_size), 0);
    ASSERT_EQ(close(other_fd), 0);
    SegmentID other_segment = 0;
    ASSERT_TRUE(
        engine->openSegment(other_segment, "file://" + other_name).ok());

    auto first = request(Request::WRITE, 4096);
    auto second = request(Request::WRITE, 4096, 4096);
    second.target_id = other_segment;
    second.target_offset = other_size - second.length;
    auto outside = request(Request::WRITE, 4096, 8192);
    outside.target_id = other_segment;
    outside.target_offset = other_size;
    memset(first.source, 0x44, first.length);
    memset(second.source, 0x55, second.length);

    auto batch = engine->allocateBatch(3);
    ASSERT_TRUE(engine->submitTransfer(batch, {first, second, outside}).ok());
    EXPECT_EQ(poll(batch, 0).s, COMPLETED);
    EXPECT_EQ(poll(batch, 1).s, COMPLETED);
    EXPECT_EQ(poll(batch, 2).s, FAILED);
    EXPECT_EQ(state.submitted_ios, 2);
    ASSERT_TRUE(engine->freeBatch(batch).ok());

    expectFileRange(path, 0, 4096, 0x44);
    expectFileRange(other_name, other_size - 4096, 4096, 0x55);
    expectFileRange(path, other_size - 4096, 4096, 0);
    EXPECT_EQ(unlink(other_name.c_str()), 0);
}

TEST_F(NdsTest, SeparateRequestsCrossRegistrationsAndReuseContext) {
    engine.reset();
    cfg->set("merge_requests", false);
    engine = std::make_unique<TransferEngine>(cfg);
    MemoryOptions options;
    options.type = NDS;
    options.location = "npu:0";
    auto* second = static_cast<char*>(buffer) + kSize / 2;
    auto* first = second - 4096;
    ASSERT_TRUE(engine->registerLocalMemory(buffer, kSize / 2, options).ok());
    ASSERT_TRUE(engine->registerLocalMemory(second, kSize / 2, options).ok());
    ASSERT_TRUE(engine->openSegment(segment, "file://" + path).ok());
    int pins = state.pins;
    for (auto opcode : {Request::WRITE, Request::READ}) {
        auto batch = engine->allocateBatch(2);
        std::vector<Request> requests = {
            request(opcode, 4096, kSize / 2 - 4096, 0),
            request(opcode, 4096, kSize / 2, 4096),
        };
        if (opcode == Request::READ) {
            memset(first, 0, 4096);
            memset(second, 0, 4096);
        }
        ASSERT_TRUE(engine->submitTransfer(batch, requests).ok());
        EXPECT_EQ(poll(batch, 1).s, COMPLETED);
        EXPECT_EQ(poll(batch, 0).transferred_bytes, 4096);
        EXPECT_TRUE(engine->freeBatch(batch).ok());
    }
    EXPECT_EQ(state.submitted_iov_lengths,
              (std::vector<std::vector<size_t>>{{4096, 4096}, {4096, 4096}}));
    EXPECT_EQ(state.registered_ios, 2);
    EXPECT_EQ(state.context_setups, 1);
    EXPECT_EQ(state.pins, pins);
    for (size_t i = 0; i < 4096; ++i) {
        EXPECT_EQ(static_cast<unsigned char>(first[i]), 0x5a);
        EXPECT_EQ(static_cast<unsigned char>(second[i]), 0x5a);
    }
    auto pending = engine->allocateBatch(2);
    std::vector<Request> reads = {
        request(Request::READ, 4096, kSize / 2 - 4096, 0),
        request(Request::READ, 4096, kSize / 2, 4096),
    };
    state.pause = true;
    ASSERT_TRUE(engine->submitTransfer(pending, reads).ok());
    ASSERT_TRUE(engine->unregisterLocalMemory(second, kSize / 2).ok());
    EXPECT_EQ(state.drains, 1);  // Removing either buffer fences the batch.
    state.pause = false;
    EXPECT_EQ(poll(pending).s, FAILED);
}

TEST_F(NdsTest, Requires4KiBAlignedRequests) {
    const std::vector<Request> requests{
        request(Request::READ, 512),
        request(Request::READ, 4096, 2048, 0),
        request(Request::READ, 4096, 0, 512),
        request(Request::READ, 4096, 4096),
    };
    auto batch = engine->allocateBatch(requests.size());
    ASSERT_TRUE(engine->submitTransfer(batch, requests).ok());
    for (size_t i = 0; i < 3; ++i) EXPECT_EQ(poll(batch, i).s, FAILED);
    const auto status = poll(batch, 3);
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 4096);
    EXPECT_EQ(state.registered_ios, 1);
}

TEST_F(NdsTest, Requires4KiBAlignedMemoryRegistration) {
    ASSERT_TRUE(engine->unregisterLocalMemory(buffer, kSize).ok());
    MemoryOptions options;
    options.type = NDS;
    options.location = "npu:0";
    auto* addr = static_cast<char*>(buffer);
    EXPECT_FALSE(engine->registerLocalMemory(addr + 512, 4096, options).ok());
    EXPECT_FALSE(engine->registerLocalMemory(addr, 2048, options).ok());
    EXPECT_EQ(state.pins, 1);
    ASSERT_TRUE(engine->registerLocalMemory(addr + 4096, 4096, options).ok());
    EXPECT_EQ(state.pins, 2);
}

TEST_F(NdsTest, RejectsOutOfRangeAndCpuMemory) {
    auto out_of_range = request(Request::READ);
    out_of_range.length = kSize + 4096;
    auto unregistered = request(Request::READ);
    unregistered.source = static_cast<char*>(buffer) + kSize;
    auto batch = engine->allocateBatch(2);
    ASSERT_TRUE(
        engine->submitTransfer(batch, {out_of_range, unregistered}).ok());
    for (size_t i = 0; i < 2; ++i) EXPECT_EQ(poll(batch, i).s, FAILED);
    EXPECT_EQ(state.submits, 0);
    ASSERT_TRUE(engine->unregisterLocalMemory(buffer, kSize).ok());
    MemoryOptions cpu;
    cpu.type = NDS;
    cpu.location = "cpu:0";
    state.npu_size = 0;
    ASSERT_TRUE(engine->registerLocalMemory(buffer, kSize, cpu).ok());
    batch = engine->allocateBatch(1);
    ASSERT_TRUE(engine->submitTransfer(batch, {request(Request::READ)}).ok());
    EXPECT_EQ(poll(batch).s, FAILED);
    EXPECT_EQ(state.pins, 1);
}

TEST_F(NdsTest, SharedDriverAndRegistrationSurviveOtherEngineShutdown) {
    auto other = std::make_unique<TransferEngine>(cfg);
    ASSERT_TRUE(other->available());
    MemoryOptions options;
    options.type = NDS;
    options.location = "npu:0";
    ASSERT_TRUE(other->registerLocalMemory(buffer, kSize, options).ok());
    EXPECT_EQ(state.opens, 1);
    EXPECT_EQ(state.pins, 1);
    other.reset();
    EXPECT_EQ(state.closes, 0);
    EXPECT_EQ(state.unpins, 0);
    auto batch = engine->allocateBatch(1);
    ASSERT_TRUE(engine->submitTransfer(batch, {request(Request::READ)}).ok());
    EXPECT_EQ(poll(batch).s, COMPLETED);
}

TEST_F(NdsTest, SharedPinSurvivesPendingIoAndCanBeReused) {
    auto other = std::make_unique<TransferEngine>(cfg);
    ASSERT_TRUE(other->available());
    MemoryOptions options;
    options.type = NDS;
    options.location = "npu:0";
    ASSERT_TRUE(other->registerLocalMemory(buffer, kSize, options).ok());
    SegmentID other_segment = 0;
    ASSERT_TRUE(other->openSegment(other_segment, "file://" + path).ok());
    auto batch = other->allocateBatch(1);
    state.pause = true;
    ASSERT_TRUE(other
                    ->submitTransfer(batch, {{Request::WRITE, buffer,
                                              other_segment, 0, 4096}})
                    .ok());
    ASSERT_TRUE(engine->unregisterLocalMemory(buffer, kSize).ok());
    EXPECT_EQ(state.unpins, 0);
    // Rejoining reuses the live SDK registration.
    ASSERT_TRUE(engine->registerLocalMemory(buffer, kSize, options).ok());
    EXPECT_EQ(state.pins, 1);
    ASSERT_TRUE(engine->unregisterLocalMemory(buffer, kSize).ok());
    EXPECT_EQ(state.unpins, 0);
    // Last-user unregister fences the pending write before unpinning. A failed
    // unpin must preserve ownership so the caller can retry safely.
    state.unpin_error = true;
    EXPECT_FALSE(other->unregisterLocalMemory(buffer, kSize).ok());
    EXPECT_GT(state.drains, 0);
    EXPECT_EQ(state.unpins, 0);
    state.unpin_error = false;
    ASSERT_TRUE(other->unregisterLocalMemory(buffer, kSize).ok());
    EXPECT_EQ(state.unpins, 1);
    ASSERT_TRUE(other->freeBatch(batch).ok());
    EXPECT_EQ(state.live_contexts, 0);
    expectFileRange(path, 0, 4096, 0x5a);
}

TEST_F(NdsTest, RuntimeGateAndDriverFailureLeaveTransportUnavailable) {
    engine.reset();
    cfg->set("transports/nds/enable", false);
    engine = std::make_unique<TransferEngine>(cfg);
    ASSERT_TRUE(engine->available());
    MemoryOptions options;
    options.type = NDS;
    options.location = "npu:0";
    EXPECT_FALSE(engine->registerLocalMemory(buffer, kSize, options).ok());
    engine.reset();
    cfg->set("transports/nds/enable", true);
    state.open_error = true;
    engine = std::make_unique<TransferEngine>(cfg);
    ASSERT_TRUE(engine->available());
    EXPECT_FALSE(engine->registerLocalMemory(buffer, kSize, options).ok());
}

TEST_F(NdsTest, InvalidDepthIsRejectedAtInstall) {
    engine.reset();
    cfg->set("transports/nds/io_batch_depth", 1025);
    engine = std::make_unique<TransferEngine>(cfg);
    ASSERT_TRUE(engine->available());
    MemoryOptions options;
    options.type = NDS;
    options.location = "npu:0";
    EXPECT_FALSE(engine->registerLocalMemory(buffer, kSize, options).ok());
}

TEST_F(NdsTest, Requires4KiBAlignedIoBytes) {
    MemoryOptions options;
    options.type = NDS;
    options.location = "npu:0";
    for (int bytes : {512, 2048, 4608}) {
        SCOPED_TRACE(bytes);
        engine.reset();
        cfg->set("transports/nds/io_bytes", bytes);
        engine = std::make_unique<TransferEngine>(cfg);
        ASSERT_TRUE(engine->available());
        EXPECT_FALSE(engine->registerLocalMemory(buffer, kSize, options).ok());
    }

    cfg->set("transports/nds/io_bytes", 4096);
    startEngine();
    auto batch = engine->allocateBatch(1);
    ASSERT_TRUE(engine->submitTransfer(batch, {request(Request::READ)}).ok());
    EXPECT_EQ(poll(batch).s, COMPLETED);
}

TEST_F(NdsTest, UnregisterAndShutdownDrainPendingIo) {
    auto batch = engine->allocateBatch(1);
    ASSERT_TRUE(
        engine->submitTransfer(batch, {request(Request::WRITE, kSize)}).ok());
    ASSERT_TRUE(engine->unregisterLocalMemory(buffer, kSize).ok());
    EXPECT_EQ(state.drains, 2);
    EXPECT_EQ(state.unpins, 1);
    EXPECT_EQ(poll(batch).s, FAILED);
}

TEST_F(NdsTest, ReopensFileAfterPathReplacement) {
    auto batch = engine->allocateBatch(2);
    ASSERT_TRUE(engine->submitTransfer(batch, {request(Request::WRITE)}).ok());
    EXPECT_EQ(poll(batch).s, COMPLETED);
    ASSERT_EQ(unlink(path.c_str()), 0);
    int fd = open(path.c_str(), O_CREAT | O_RDWR, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, kSize), 0);
    close(fd);
    ASSERT_TRUE(engine->submitTransfer(batch, {request(Request::READ)}).ok());
    EXPECT_EQ(poll(batch, 1).s, COMPLETED);
    EXPECT_EQ(static_cast<unsigned char*>(buffer)[0], 0);
}

TEST_F(NdsTest, TopologyFdSurvivesHandleFailureUntilDriverClose) {
    state.handle_error = true;
    auto batch = engine->allocateBatch(2);
    ASSERT_TRUE(engine->submitTransfer(batch, {request(Request::READ)}).ok());
    EXPECT_EQ(poll(batch).s, FAILED);
    ASSERT_EQ(state.live_topologies, 1);
    EXPECT_EQ(state.live_files, 0);
    const int topology_fd = state.topology_fd;
    EXPECT_GE(fcntl(topology_fd, F_GETFD), 0);
    state.handle_error = false;
    ASSERT_TRUE(engine->submitTransfer(batch, {request(Request::READ)}).ok());
    EXPECT_EQ(poll(batch, 1).s, COMPLETED);
    EXPECT_EQ(state.live_files, 1);
    EXPECT_EQ(state.live_topologies, 1);
    EXPECT_EQ(state.topology_fd, topology_fd);
    ASSERT_TRUE(engine->freeBatch(batch).ok());
    engine.reset();
    EXPECT_EQ(state.live_topologies, 0);
    EXPECT_EQ(fcntl(topology_fd, F_GETFD), -1);
}

TEST_F(NdsTest, ConcurrentBatchesUseIndependentTaskState) {
    std::vector<std::thread> workers;
    for (size_t i = 0; i < 4; ++i) {
        workers.emplace_back([&, i] {
            auto batch = engine->allocateBatch(1);
            ASSERT_TRUE(engine
                            ->submitTransfer(
                                batch, {request(Request::READ, 4096, i * 4096)})
                            .ok());
            auto status = poll(batch);
            EXPECT_EQ(status.s, COMPLETED);
            EXPECT_EQ(status.transferred_bytes, 4096);
            EXPECT_TRUE(engine->freeBatch(batch).ok());
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(state.submits, 4);
}

TEST_F(NdsTest, ShutdownFencesLiveBatchBeforeDriverClose) {
    auto batch = engine->allocateBatch(1);
    ASSERT_TRUE(
        engine->submitTransfer(batch, {request(Request::READ, kSize)}).ok());
    engine.reset();
    EXPECT_EQ(state.drains, 2);
    EXPECT_EQ(state.live_contexts, 0);
    EXPECT_EQ(state.unpins, 1);
    EXPECT_EQ(state.closes, 1);
}

class FileFallback : public Transport {
   public:
    struct Batch : SubBatch {
        std::vector<Request> requests;
        size_t size() const override { return requests.size(); }
    };
    FileFallback() {
        caps.dram_to_file = true;
        caps.gpu_to_file =
            true;  // CPU test storage also runs under AscendPlatform.
    }
    Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new Batch;
        return Status::OK();
    }
    Status freeSubBatch(SubBatchRef& batch) override {
        delete batch;
        batch = nullptr;
        return Status::OK();
    }
    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        if (require_fence) {
            EXPECT_EQ(state.live_contexts, 0);
        }
        ++submits;
        auto& stored = static_cast<Batch*>(batch)->requests;
        stored.insert(stored.end(), requests.begin(), requests.end());
        return Status::OK();
    }
    Status getTransferStatus(SubBatchRef batch, int id,
                             TransferStatus& status) override {
        status = {COMPLETED, static_cast<Batch*>(batch)->requests[id].length};
        return Status::OK();
    }
    int submits = 0;
    bool require_fence = true;
};

TEST_F(NdsTest, ReadAndWriteFailoverWaitForFence) {
    TransferEngineImpl other(cfg);
    ASSERT_TRUE(other.available());
    auto fallback = std::make_shared<FileFallback>();
    other.swapTransportForTest(IOURING, fallback);
    MemoryOptions options;
    options.location = "npu:0";
    options.type = NDS;
    ASSERT_TRUE(other.registerLocalMemory({buffer}, {kSize}, options).ok());
    SegmentID target;
    ASSERT_TRUE(other.openSegment(target, "file://" + path).ok());
    state.partial_submit = true;
    for (auto opcode : {Request::WRITE, Request::READ}) {
        const int previous_submits = fallback->submits;
        auto batch = other.allocateBatch(1);
        auto req = request(opcode, kSize);
        req.target_id = target;
        ASSERT_TRUE(other.submitTransfer(batch, {req}).ok());
        TransferStatus status{};
        ASSERT_TRUE(other.getTransferStatus(batch, 0, status).ok());
        EXPECT_EQ(status.s, PENDING);
        EXPECT_EQ(state.live_contexts, 0);
        ASSERT_TRUE(other.getTransferStatus(batch, 0, status).ok());
        EXPECT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kSize);
        EXPECT_EQ(fallback->submits, previous_submits + 1);
        ASSERT_TRUE(other.freeBatch(batch).ok());
    }
}

TEST_F(NdsTest, PolicyAndMixedHintsRespectNdsEligibility) {
    cfg->set("policy", nlohmann::json::parse(R"([
        {"name":"nds_storage","segment_type":"file",
         "transports":["nds","io_uring"]},
        {"name":"cpu_only","segment_type":"file","transports":["io_uring"]}
    ])"));
    TransferEngineImpl other(cfg);
    auto fallback = std::make_shared<FileFallback>();
    fallback->require_fence = false;
    other.swapTransportForTest(IOURING, fallback);
    MemoryOptions opts;
    opts.location = "npu:0";
    opts.type = NDS;
    ASSERT_TRUE(other.registerLocalMemory({buffer}, {kSize}, opts).ok());
    SegmentID target;
    ASSERT_TRUE(other.openSegment(target, "file://" + path).ok());
    auto req = request(Request::READ);
    req.target_id = target;
    req.policy_name = "nds_storage";
    req.transport_hint = NDS;
    auto batch = other.allocateBatch(2);
    auto second = req;
    second.transport_hint = IOURING;
    ASSERT_TRUE(other.submitTransfer(batch, {req, second}).ok());
    TransferStatus status{};
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(other.getTransferStatus(batch, i, status).ok());
        EXPECT_EQ(status.s, COMPLETED);
    }
    EXPECT_EQ(state.submits, 1);
    EXPECT_EQ(fallback->submits, 1);
    ASSERT_TRUE(other.freeBatch(batch).ok());
    // An explicit NDS hint cannot override a policy that only permits io_uring.
    req.policy_name = "cpu_only";
    batch = other.allocateBatch(1);
    auto submitted = other.submitTransfer(batch, {req});
    if (submitted.ok()) {
        ASSERT_TRUE(other.getTransferStatus(batch, 0, status).ok());
        EXPECT_EQ(status.s, FAILED);
    }
    EXPECT_EQ(state.submits, 1);
    EXPECT_EQ(fallback->submits, 1);
}

TEST_F(NdsTest, NdsOnlyPolicyPreventsReadAndWriteFailover) {
    cfg->set("policy", nlohmann::json::parse(R"([
        {"name":"nds_storage","segment_type":"file","transports":["nds"]}
    ])"));
    TransferEngineImpl other(cfg);
    auto fallback = std::make_shared<FileFallback>();
    other.swapTransportForTest(IOURING, fallback);
    MemoryOptions opts;
    opts.type = NDS;
    opts.location = "npu:0";
    ASSERT_TRUE(other.registerLocalMemory({buffer}, {kSize}, opts).ok());
    SegmentID target;
    ASSERT_TRUE(other.openSegment(target, "file://" + path).ok());
    state.partial_submit = true;
    for (auto opcode : {Request::WRITE, Request::READ}) {
        auto req = request(opcode, kSize);
        req.target_id = target;
        req.transport_hint = NDS;
        auto batch = other.allocateBatch(1);
        ASSERT_TRUE(other.submitTransfer(batch, {req}).ok());
        TransferStatus status{};
        ASSERT_TRUE(other.getTransferStatus(batch, 0, status).ok());
        EXPECT_EQ(status.s, FAILED);
        EXPECT_EQ(fallback->submits, 0);
        EXPECT_EQ(state.live_contexts, 0);
        ASSERT_TRUE(other.freeBatch(batch).ok());
    }
}

TEST_F(NdsTest, RuntimeQueueProgressCompletesSeparateRequests) {
    engine.reset();
    cfg->set("enable_runtime_queue", true);
    engine = std::make_unique<TransferEngine>(cfg);
    ASSERT_TRUE(engine->available());
    MemoryOptions opts;
    opts.location = "npu:0";
    opts.type = NDS;
    ASSERT_TRUE(engine->registerLocalMemory(buffer, kSize, opts).ok());
    ASSERT_TRUE(engine->openSegment(segment, "file://" + path).ok());
    auto first = request(Request::READ);
    auto second = request(Request::READ);
    second.source = static_cast<char*>(buffer) + 8192;
    second.target_offset = 4096;
    auto batch = engine->allocateBatch(2);
    ASSERT_TRUE(engine->submitTransfer(batch, {first, second}).ok());
    TransferStatus status{PENDING, 0};
    for (int i = 0; i < 500 && status.s == PENDING; ++i) {
        ASSERT_TRUE(engine->progressBatch(batch, status).ok());
        if (status.s == PENDING)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 8192);
    ASSERT_TRUE(engine->freeBatch(batch).ok());
}

TEST_F(NdsTest, NdsConfigurationIsBootstrapOnly) {
    for (const auto* key : {"enable", "io_bytes", "io_batch_depth"})
        EXPECT_EQ(classifyConfigPath(std::string("transports/nds/") + key),
                  ConfigLifecycle::kBootstrapOnly);
    EXPECT_EQ(parseTransportType("nds"), NDS);
    EXPECT_STREQ(transportTypeName(NDS), "nds");
    EXPECT_EQ(c_to_transport_hint(TRANSPORT_NDS), NDS);
}

#if TENT_METRICS_ENABLED
TEST_F(NdsTest, MetricsCountLogicalBytesAndFailureOnce) {
    auto& metrics = TentMetrics::instance();
    struct MetricsCleanup {
        TentMetrics& metrics;
        ~MetricsCleanup() { metrics.shutdown(); }
    } cleanup{metrics};

    MetricsConfig config;
    config.enabled = true;
    config.http_host = "127.0.0.1";
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(socket_fd, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(
        bind(socket_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);
    socklen_t address_size = sizeof(address);
    ASSERT_EQ(getsockname(socket_fd, reinterpret_cast<sockaddr*>(&address),
                          &address_size),
              0);
    config.http_port = ntohs(address.sin_port);
    close(socket_fd);
    config.report_interval_seconds = 0;
    metrics.shutdown();
    const auto status = metrics.initialize(config);
    ASSERT_TRUE(status.ok()) << status.ToString();
    TentMetrics::setEnabled(true);
    auto value = [&](const std::string& series) {
        std::istringstream lines(metrics.getPrometheusMetrics());
        std::string line;
        while (std::getline(lines, line))
            if (line.starts_with(series + " "))
                return std::stod(line.substr(series.size() + 1));
        return 0.0;
    };
    const std::string bytes = "tent_read_bytes_total{transport=\"nds\"}";
    const std::string requests = "tent_read_requests_total{transport=\"nds\"}";
    const std::string errors = "tent_write_failures_total{transport=\"nds\"}";
    const double before_bytes = value(bytes), before_requests = value(requests),
                 before_errors = value(errors);
    auto batch = engine->allocateBatch(2);
    ASSERT_TRUE(
        engine->submitTransfer(batch, {request(Request::READ, kSize)}).ok());
    EXPECT_EQ(poll(batch).s, COMPLETED);
    EXPECT_EQ(poll(batch).s, COMPLETED);
    EXPECT_EQ(value(bytes) - before_bytes, kSize);
    EXPECT_EQ(value(requests) - before_requests, 1);
    state.partial_submit = true;
    ASSERT_TRUE(
        engine->submitTransfer(batch, {request(Request::WRITE, kSize)}).ok());
    EXPECT_EQ(poll(batch, 1).s, FAILED);
    EXPECT_EQ(poll(batch, 1).s, FAILED);
    EXPECT_EQ(value(errors) - before_errors, 1);
    ASSERT_TRUE(engine->freeBatch(batch).ok());
}
#endif
}  // namespace
}  // namespace mooncake::tent
