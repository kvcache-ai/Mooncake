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

}  // namespace
}  // namespace tent
}  // namespace mooncake
