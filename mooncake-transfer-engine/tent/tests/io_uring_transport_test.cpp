// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/io_uring/io_uring_transport.h"

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kPageSize = 4096;
constexpr SegmentID kUnknownSegment = 0x7fff;

class ScopedTempFile {
   public:
    explicit ScopedTempFile(size_t length) {
        // /dev/shm (tmpfs) rather than /tmp: on overlayfs-backed /tmp an
        // O_DIRECT open succeeds but the io_uring operations fail with
        // EINVAL on older kernels. On tmpfs the O_DIRECT open itself fails,
        // so the transport falls back to buffered I/O and the requests
        // succeed.
        char tmpl[] = "/dev/shm/mooncake_tent_io_uring_test_XXXXXX";
        fd_ = mkstemp(tmpl);
        EXPECT_GE(fd_, 0);
        if (fd_ >= 0) {
            path_ = tmpl;
            EXPECT_EQ(ftruncate(fd_, (off_t)length), 0);
        }
    }

    ~ScopedTempFile() {
        if (fd_ >= 0) close(fd_);
        if (!path_.empty()) unlink(path_.c_str());
    }

    int fd() const { return fd_; }
    const std::string& path() const { return path_; }

   private:
    int fd_{-1};
    std::string path_;
};

Status installLocalFileSegment(ControlService& metadata,
                               const std::string& path, size_t length) {
    return metadata.segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            segment.name = "io_uring_test_segment";
            segment.machine_id = "io_uring_test_machine";
            segment.type = SegmentType::File;
            segment.detail = FileSegmentDesc{};
            auto& files = std::get<FileSegmentDesc>(segment.detail);
            files.buffers.clear();
            FileBufferDesc buffer;
            buffer.path = path;
            buffer.length = length;
            buffer.offset = 0;
            files.buffers.push_back(std::move(buffer));
            return Status::OK();
        });
}

// Reports a positive short submission result on the first successful call
// while letting the real io_uring_submit() through, so the kernel-side
// state matches a genuine short submission.
class ShortSubmitIOUringTransport : public IOUringTransport {
   public:
    void injectShortSubmit(int reported) { short_report_ = reported; }

    int submitCalls() const { return submit_calls_; }

   protected:
    int submitSqes(struct io_uring* ring) override {
        int rc = IOUringTransport::submitSqes(ring);
        ++submit_calls_;
        if (short_report_ > 0 && rc > short_report_) {
            deficit_ = rc - short_report_;
            int reported = short_report_;
            short_report_ = 0;
            return reported;
        }
        if (deficit_ > 0) {
            rc += deficit_;
            deficit_ = 0;
        }
        return rc;
    }

   private:
    int short_report_ = 0;
    int deficit_ = 0;
    int submit_calls_ = 0;
};

Status installTransport(IOUringTransport& transport,
                        std::shared_ptr<ControlService>& metadata) {
    std::string local_segment_name = "io_uring_test_segment";
    return transport.install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>());
}

TransferStatus pollUntilSettled(IOUringTransport& transport,
                                Transport::SubBatchRef batch, int task_id) {
    TransferStatus ts{};
    for (int attempts = 0; attempts < 10000; ++attempts) {
        EXPECT_TRUE(transport.getTransferStatus(batch, task_id, ts).ok());
        if (ts.s != TransferStatusEnum::PENDING &&
            ts.s != TransferStatusEnum::INITIAL)
            return ts;
        usleep(1000);
    }
    return ts;
}

class IOUringFileContextTest : public ::testing::TestWithParam<bool> {};

TEST_P(IOUringFileContextTest, DoesNotReuseAnotherTransportsFile) {
    ScopedTempFile first_file(kPageSize);
    ScopedTempFile second_file(kPageSize);
    ASSERT_GE(first_file.fd(), 0);
    ASSERT_GE(second_file.fd(), 0);
    std::vector<uint8_t> first_contents(kPageSize, 0x11);
    std::vector<uint8_t> second_contents(kPageSize, 0x22);
    ASSERT_EQ(pwrite(first_file.fd(), first_contents.data(), kPageSize, 0),
              static_cast<ssize_t>(kPageSize));
    ASSERT_EQ(pwrite(second_file.fd(), second_contents.data(), kPageSize, 0),
              static_cast<ssize_t>(kPageSize));

    auto first_metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    auto second_metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    SegmentID first_id = 0;
    SegmentID second_id = 0;
    ASSERT_TRUE(
        first_metadata->segmentManager()
            .openRemote(first_id, kLocalFileSegmentPrefix + first_file.path())
            .ok());
    ASSERT_TRUE(
        second_metadata->segmentManager()
            .openRemote(second_id, kLocalFileSegmentPrefix + second_file.path())
            .ok());
    ASSERT_EQ(first_id, second_id);

    auto first = std::make_unique<IOUringTransport>();
    auto installed = installTransport(*first, first_metadata);
    if (!installed.ok()) GTEST_SKIP() << installed.message();
    IOUringTransport second;
    ASSERT_TRUE(installTransport(second, second_metadata).ok());

    auto read_then_write = [&](IOUringTransport& transport, SegmentID id,
                               uint8_t expected, uint8_t replacement) {
        void* allocation = nullptr;
        ASSERT_EQ(posix_memalign(&allocation, kPageSize, kPageSize), 0);
        std::unique_ptr<void, decltype(&free)> memory(allocation, free);
        auto* bytes = static_cast<uint8_t*>(allocation);
        std::memset(bytes, 0, kPageSize);
        Transport::SubBatchRef batch = nullptr;
        ASSERT_TRUE(transport.allocateSubBatch(batch, 2).ok());
        Request request{};
        request.opcode = Request::READ;
        request.source = bytes;
        request.target_id = id;
        request.length = kPageSize;
        ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
        auto status = pollUntilSettled(transport, batch, 0);
        EXPECT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kPageSize);
        EXPECT_EQ(std::vector<uint8_t>(bytes, bytes + kPageSize),
                  std::vector<uint8_t>(kPageSize, expected));
        std::memset(bytes, replacement, kPageSize);
        request.opcode = Request::WRITE;
        ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
        status = pollUntilSettled(transport, batch, 1);
        EXPECT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kPageSize);
        ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    };

    read_then_write(*first, first_id, 0x11, 0xA1);
    if (GetParam()) first.reset();
    read_then_write(second, second_id, 0x22, 0xB2);
    ASSERT_EQ(pread(first_file.fd(), first_contents.data(), kPageSize, 0),
              static_cast<ssize_t>(kPageSize));
    ASSERT_EQ(pread(second_file.fd(), second_contents.data(), kPageSize, 0),
              static_cast<ssize_t>(kPageSize));
    EXPECT_EQ(first_contents, std::vector<uint8_t>(kPageSize, 0xA1));
    EXPECT_EQ(second_contents, std::vector<uint8_t>(kPageSize, 0xB2));
}

INSTANTIATE_TEST_SUITE_P(OwnerLifetime, IOUringFileContextTest,
                         ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                             return info.param ? "RetiredOwner" : "LiveOwners";
                         });

// A positive short io_uring_submit() result must not fail the submission:
// part of the batch is already dispatched, so an error return would make
// the engine's submit-stage failover re-execute those requests.
TEST(IOUringTransportTest, ShortSubmitIsNotAnErrorAndExecutesExactlyOnce) {
    const size_t kFileLength = 4 * kPageSize;
    const size_t kRequestCount = 3;

    ScopedTempFile file(kFileLength);
    ASSERT_GE(file.fd(), 0);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(
        installLocalFileSegment(*metadata, file.path(), kFileLength).ok());

    ShortSubmitIOUringTransport transport;
    transport.injectShortSubmit(1);
    auto install_status = installTransport(transport, metadata);
    if (!install_status.ok()) {
        GTEST_SKIP() << "io_uring unavailable: " << install_status.message();
    }

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 8).ok());

    // Page-aligned source keeps every request on the direct (non-staging)
    // path and valid under O_DIRECT.
    void* source = nullptr;
    ASSERT_EQ(posix_memalign(&source, kPageSize, kRequestCount * kPageSize), 0);
    auto* source_bytes = static_cast<uint8_t*>(source);

    std::vector<Request> requests;
    for (size_t i = 0; i < kRequestCount; ++i) {
        Request req;
        req.opcode = Request::WRITE;
        req.source = source_bytes + i * kPageSize;
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = i * kPageSize;
        req.length = kPageSize;
        // Distinct pattern per request: verifies exactly-once placement.
        std::memset(req.source, 0x10 + i, kPageSize);
        requests.push_back(req);
    }

    auto status = transport.submitTransferTasks(batch, requests);
    // The old code returned InternalError here while the SQEs were already
    // in flight.
    EXPECT_TRUE(status.ok());
    // The flush loop retried after the short report.
    EXPECT_GE(transport.submitCalls(), 2);

    for (size_t i = 0; i < kRequestCount; ++i) {
        auto ts = pollUntilSettled(transport, batch, (int)i);
        EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED);
    }

    // Exactly-once: every page holds its own request's pattern.
    std::vector<uint8_t> expected(kFileLength, 0);
    for (size_t i = 0; i < kRequestCount; ++i)
        std::memset(expected.data() + i * kPageSize, 0x10 + i, kPageSize);
    std::vector<uint8_t> actual(kFileLength, 0);
    ASSERT_EQ(pread(file.fd(), actual.data(), kFileLength, 0),
              (ssize_t)kFileLength);
    EXPECT_EQ(std::memcmp(actual.data(), expected.data(), kFileLength), 0);

    free(source);
    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    ASSERT_TRUE(transport.uninstall().ok());
}

// Regression for the all-or-nothing submission contract: a mid-batch
// segment-lookup failure (the second of three requests targets an unknown
// segment) must return an error without appending tasks, preparing SQEs or
// executing anything. The old loop appended each task before validating it
// and left the earlier requests' SQEs queued in the ring, so the next
// submission on the same sub-batch would have flushed them.
TEST(IOUringTransportTest, SubmitIsAllOrNothingWhenSegmentLookupFailsMidBatch) {
    const size_t kFileLength = 2 * kPageSize;

    ScopedTempFile file(kFileLength);
    ASSERT_GE(file.fd(), 0);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(
        installLocalFileSegment(*metadata, file.path(), kFileLength).ok());

    IOUringTransport transport;
    auto install_status = installTransport(transport, metadata);
    if (!install_status.ok()) {
        GTEST_SKIP() << "io_uring unavailable: " << install_status.message();
    }

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 8).ok());
    auto* uring_batch = dynamic_cast<IOUringSubBatch*>(batch);
    ASSERT_NE(uring_batch, nullptr);
    const int sq_space_before = io_uring_sq_space_left(&uring_batch->ring);

    // Heap (page-unaligned) source exercises the staging-buffer allocation
    // that must be rolled back on failure.
    std::vector<uint8_t> source(kPageSize, 0xA5);
    auto makeRequest = [&](SegmentID target_id, uint64_t target_offset) {
        Request req;
        req.opcode = Request::WRITE;
        req.source = source.data();
        req.target_id = target_id;
        req.target_offset = target_offset;
        req.length = kPageSize;
        return req;
    };
    std::vector<Request> requests{
        makeRequest(LOCAL_SEGMENT_ID, 0),
        // Unknown target segment: lookup fails fast with an error.
        makeRequest(kUnknownSegment, 0),
        makeRequest(LOCAL_SEGMENT_ID, kPageSize)};

    auto status = transport.submitTransferTasks(batch, requests);
    EXPECT_FALSE(status.ok());

    // Nothing was accepted: no tasks appended ...
    EXPECT_EQ(uring_batch->task_list.size(), 0u);
    // ... and no SQE was prepared for the rejected batch.
    EXPECT_EQ(io_uring_sq_space_left(&uring_batch->ring), sq_space_before);

    // Nothing was executed: the file stays zeroed.
    std::vector<uint8_t> actual(kFileLength, 1);
    ASSERT_EQ(pread(file.fd(), actual.data(), kFileLength, 0),
              (ssize_t)kFileLength);
    for (size_t i = 0; i < kFileLength; ++i) EXPECT_EQ(actual[i], 0u);

    // The transport stays usable after the rejected batch: the valid
    // requests (minus the bad one) submit and complete cleanly.
    std::vector<Request> valid_requests{makeRequest(LOCAL_SEGMENT_ID, 0)};
    EXPECT_TRUE(transport.submitTransferTasks(batch, valid_requests).ok());
    EXPECT_EQ(uring_batch->task_list.size(), 1u);
    auto ts = pollUntilSettled(transport, batch, 0);
    EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED);

    std::memset(actual.data(), 1, kFileLength);
    ASSERT_EQ(pread(file.fd(), actual.data(), kFileLength, 0),
              (ssize_t)kFileLength);
    for (size_t i = 0; i < kPageSize; ++i) EXPECT_EQ(actual[i], 0xA5);
    // The never-submitted second page stays zeroed: no phantom SQE from the
    // rejected batch leaked into this submission.
    for (size_t i = kPageSize; i < kFileLength; ++i) EXPECT_EQ(actual[i], 0u);

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    ASSERT_TRUE(transport.uninstall().ok());
}

using CompletionParams = std::tuple<bool, size_t>;

class PoisonedReadIOUringTransport : public IOUringTransport {
   public:
    IOUringSubBatch* batch = nullptr;

   protected:
    int submitSqes(struct io_uring* ring) override {
        if (!initialized_) {
            // Poison before kernel submission, so no test code touches the
            // staging buffer while the read is in flight.
            for (auto& task : batch->task_list) {
                if (task.buffer)
                    std::memset(task.buffer, 0xD7, task.request.length);
            }
            initialized_ = true;
        }
        return IOUringTransport::submitSqes(ring);
    }

   private:
    bool initialized_ = false;
};

class IOUringReadCompletionTest
    : public ::testing::TestWithParam<CompletionParams> {};

TEST_P(IOUringReadCompletionTest, ReportsActualBytesAndPreservesUnreadTail) {
    const auto [staged, file_length] = GetParam();
    ScopedTempFile file(file_length);
    ASSERT_GE(file.fd(), 0);
    std::vector<uint8_t> file_data(file_length, 0xA5);
    ASSERT_EQ(pwrite(file.fd(), file_data.data(), file_length, 0),
              static_cast<ssize_t>(file_length));
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(
        installLocalFileSegment(*metadata, file.path(), kPageSize).ok());
    PoisonedReadIOUringTransport transport;
    auto installed = installTransport(transport, metadata);
    if (!installed.ok()) GTEST_SKIP() << installed.message();

    void* allocation = nullptr;
    ASSERT_EQ(posix_memalign(&allocation, kPageSize, 2 * kPageSize), 0);
    std::unique_ptr<void, decltype(&free)> memory(allocation, free);
    auto* destination = static_cast<uint8_t*>(allocation) + (staged ? 1 : 0);
    std::memset(destination, 0x5A, kPageSize);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    transport.batch = static_cast<IOUringSubBatch*>(batch);
    Request request{};
    request.opcode = Request::READ;
    request.source = destination;
    request.target_id = LOCAL_SEGMENT_ID;
    request.length = kPageSize;
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
    auto* uring_batch = static_cast<IOUringSubBatch*>(batch);
    auto& task = uring_batch->task_list[0];
    ASSERT_EQ(task.buffer != nullptr, staged);
    const auto status = pollUntilSettled(transport, batch, 0);
    EXPECT_EQ(status.s, file_length == kPageSize ? COMPLETED : FAILED);
    EXPECT_EQ(status.transferred_bytes, file_length);
    EXPECT_EQ(task.buffer, nullptr);
    for (size_t i = 0; i < kPageSize; ++i) {
        ASSERT_EQ(destination[i], i < file_length ? 0xA5 : 0x5A) << i;
    }
    TransferStatus repeated{};
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, repeated).ok());
    EXPECT_EQ(repeated.s, status.s);
    EXPECT_EQ(repeated.transferred_bytes, status.transferred_bytes);
    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
}

INSTANTIATE_TEST_SUITE_P(
    AlignedAndStaged, IOUringReadCompletionTest,
    ::testing::Combine(::testing::Bool(),
                       ::testing::Values(size_t{0}, kPageSize / 4, kPageSize)));

// Limit the submitted iovec while retaining the original request length.
// This produces a real zero/short WRITE CQE without relying on a full disk.
class ShortWriteIOUringTransport : public IOUringTransport {
   public:
    IOUringSubBatch* batch = nullptr;
    size_t limit = 0;

   protected:
    int submitSqes(struct io_uring* ring) override {
        if (!limited_) {
            for (auto& task : batch->task_list) task.iov.iov_len = limit;
            limited_ = true;
        }
        return IOUringTransport::submitSqes(ring);
    }

   private:
    bool limited_ = false;
};

class IOUringWriteCompletionTest
    : public ::testing::TestWithParam<CompletionParams> {};

TEST_P(IOUringWriteCompletionTest, ShortWriteFailsWithActualByteCount) {
    const auto [staged, written] = GetParam();
    ScopedTempFile file(kPageSize);
    ASSERT_GE(file.fd(), 0);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(
        installLocalFileSegment(*metadata, file.path(), kPageSize).ok());
    ShortWriteIOUringTransport transport;
    auto installed = installTransport(transport, metadata);
    if (!installed.ok()) GTEST_SKIP() << installed.message();
    void* allocation = nullptr;
    ASSERT_EQ(posix_memalign(&allocation, kPageSize, 2 * kPageSize), 0);
    std::unique_ptr<void, decltype(&free)> memory(allocation, free);
    auto* source = static_cast<uint8_t*>(allocation) + (staged ? 1 : 0);
    std::memset(source, 0xA5, kPageSize);
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    transport.batch = static_cast<IOUringSubBatch*>(batch);
    transport.limit = written;
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source;
    request.target_id = LOCAL_SEGMENT_ID;
    request.length = kPageSize;
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
    ASSERT_EQ(transport.batch->task_list[0].buffer != nullptr, staged);
    auto status = pollUntilSettled(transport, batch, 0);
    EXPECT_EQ(status.s, FAILED);
    EXPECT_EQ(status.transferred_bytes, written);
    EXPECT_EQ(transport.batch->task_list[0].buffer, nullptr);
    std::vector<uint8_t> contents(kPageSize);
    ASSERT_EQ(pread(file.fd(), contents.data(), contents.size(), 0),
              static_cast<ssize_t>(contents.size()));
    for (size_t i = 0; i < contents.size(); ++i) {
        ASSERT_EQ(contents[i], i < written ? 0xA5 : 0) << i;
    }
    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
}

INSTANTIATE_TEST_SUITE_P(AlignedAndStaged, IOUringWriteCompletionTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Values(size_t{0},
                                                              kPageSize / 4)));

TEST(IOUringTransportTest, FailedCompletionReleasesStagingBuffer) {
    IOUringTransport transport;
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    auto installed = installTransport(transport, metadata);
    if (!installed.ok()) GTEST_SKIP() << installed.message();
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    auto* uring_batch = static_cast<IOUringSubBatch*>(batch);
    uring_batch->task_list.emplace_back();
    auto& task = uring_batch->task_list.back();
    std::vector<uint8_t> destination(kPageSize, 0x5A);
    task.request = {};
    task.request.opcode = Request::READ;
    task.request.source = destination.data();
    task.request.length = kPageSize;
    task.status_word = PENDING;
    task.transferred_bytes = 0;
    task.buffer = malloc(kPageSize);
    ASSERT_NE(task.buffer, nullptr);
    task.iov = {task.buffer, kPageSize};
    auto* sqe = io_uring_get_sqe(&uring_batch->ring);
    ASSERT_NE(sqe, nullptr);
    io_uring_prep_readv(sqe, -1, &task.iov, 1, 0);
    sqe->user_data = reinterpret_cast<uintptr_t>(&task);
    ASSERT_EQ(io_uring_submit(&uring_batch->ring), 1);
    const auto status = pollUntilSettled(transport, batch, 0);
    EXPECT_EQ(status.s, FAILED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(task.buffer, nullptr);
    EXPECT_EQ(destination, std::vector<uint8_t>(kPageSize, 0x5A));
    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
