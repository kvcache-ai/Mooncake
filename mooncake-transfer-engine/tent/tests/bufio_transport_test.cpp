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
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/bufio/bufio_transport.h"

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kPageSize = 4096;
size_t short_write_limit = 0;

class ScopedTempFile {
   public:
    explicit ScopedTempFile(size_t length) {
        char tmpl[] = "/dev/shm/mooncake_tent_bufio_test_XXXXXX";
        fd_ = mkstemp(tmpl);
        EXPECT_GE(fd_, 0);
        if (fd_ >= 0) {
            path_ = tmpl;
            EXPECT_EQ(ftruncate(fd_, static_cast<off_t>(length)), 0);
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
            segment.name = "bufio_test_segment";
            segment.machine_id = "bufio_test_machine";
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

Status installTransport(BufIoTransport& transport,
                        std::shared_ptr<ControlService>& metadata) {
    std::string local_segment_name = "bufio_test_segment";
    return transport.install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>());
}

TEST(BufIoTransportTest, ShortReadFailsAndPreservesActualBytes) {
    const size_t file_length = kPageSize / 2;
    ScopedTempFile file(file_length);
    ASSERT_GE(file.fd(), 0);
    std::vector<uint8_t> file_bytes(file_length, 0x5A);
    ASSERT_EQ(pwrite(file.fd(), file_bytes.data(), file_length, 0),
              static_cast<ssize_t>(file_length));

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(
        installLocalFileSegment(*metadata, file.path(), file_length).ok());
    BufIoTransport transport;
    ASSERT_TRUE(installTransport(transport, metadata).ok());
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());

    std::vector<uint8_t> destination(kPageSize, 0xA5);
    Request request{};
    request.opcode = Request::READ;
    request.source = destination.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = 0;
    request.length = kPageSize;
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus status{};
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, file_length);
    EXPECT_EQ(std::vector<uint8_t>(destination.begin(),
                                   destination.begin() + file_length),
              file_bytes);
    EXPECT_EQ(std::vector<uint8_t>(destination.begin() + file_length,
                                   destination.end()),
              std::vector<uint8_t>(kPageSize - file_length, 0xA5));
    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
}

TEST(BufIoTransportTest, ShortWriteFailsAndPreservesActualBytes) {
    ScopedTempFile file(kPageSize);
    ASSERT_GE(file.fd(), 0);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    SegmentID id = 0;
    ASSERT_TRUE(metadata->segmentManager()
                    .openRemote(id, kLocalFileSegmentPrefix + file.path())
                    .ok());
    BufIoTransport transport;
    ASSERT_TRUE(installTransport(transport, metadata).ok());
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());

    std::vector<uint8_t> source(2 * kPageSize, 0xC3);
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = id;
    request.target_offset = 0;
    request.length = 2 * kPageSize;
    mooncake::tent::short_write_limit = kPageSize;
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus status{};
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, kPageSize);

    std::vector<uint8_t> expected(2 * kPageSize, 0);
    std::fill_n(expected.begin(), kPageSize, 0xC3);
    std::vector<uint8_t> file_contents(2 * kPageSize, 0);
    ASSERT_EQ(pread(file.fd(), file_contents.data(), 2 * kPageSize, 0),
              static_cast<ssize_t>(kPageSize));
    EXPECT_EQ(file_contents, expected);
    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

extern "C" ssize_t __real_pwrite(int fd, const void* buffer, size_t count,
                                 off_t offset);

extern "C" ssize_t __wrap_pwrite(int fd, const void* buffer, size_t count,
                                 off_t offset) {
    if (mooncake::tent::short_write_limit == 0) {
        return __real_pwrite(fd, buffer, count, offset);
    }
    const size_t allowed = std::min(count, mooncake::tent::short_write_limit);
    mooncake::tent::short_write_limit = 0;
    return __real_pwrite(fd, buffer, allowed, offset);
}
