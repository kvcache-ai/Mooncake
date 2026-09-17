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

// End-to-end payload integrity test for the TENT TCP data path: a forked
// server process serves a registered DRAM buffer over loopback, the client
// WRITEs a pattern into it and READs it back, comparing byte for byte. This
// covers the SendData/RecvData RPC handlers (including the copy-reduction
// paths in ControlClient/ControlService) on any machine, no RDMA required.

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transfer_engine.h"

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kDataLength = 4 * 1024 * 1024;
constexpr size_t kTaskCount = 8;
constexpr size_t kStride = 8 * 1024 * 1024;
constexpr size_t kBufferLength =
    2 * kTaskCount * kStride;  // sources in first half, READ dests in second

class ChildProcessGuard {
   public:
    ChildProcessGuard(pid_t pid, int stop_fd) : pid_(pid), stop_fd_(stop_fd) {}

    ~ChildProcessGuard() {
        if (pid_ <= 0) return;
        close(stop_fd_);
        (void)waitpid(pid_, nullptr, 0);
    }

    int finish() {
        close(stop_fd_);
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

    int reap() {
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        close(stop_fd_);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

   private:
    pid_t pid_;
    int stop_fd_;
};

std::shared_ptr<Config> makeTcpConfig(size_t rpc_server_threads) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("transports/tcp/enable", true);
    config->set("transports/rdma/enable", false);
    config->set("transports/shm/enable", false);
    config->set("rpc_server_threads", rpc_server_threads);
    return config;
}

bool waitBatchDone(TransferEngine& engine, BatchID batch) {
    TransferStatus status;
    for (int i = 0; i < 10000; ++i) {
        auto result = engine.getTransferStatus(batch, status);
        if (!result.ok() || status.s == TransferStatusEnum::FAILED)
            return false;
        if (status.s == TransferStatusEnum::COMPLETED) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

// Drives WRITE-then-READ of kTaskCount non-contiguous slices between two
// forked processes and verifies the pattern survives the round trip.
void runWriteThenReadAcrossProcesses(size_t rpc_server_threads) {
    int ready_pipe[2];
    int stop_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);
    ASSERT_EQ(pipe(stop_pipe), 0);

    pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(ready_pipe[0]);
        close(stop_pipe[1]);

        TransferEngine server(makeTcpConfig(rpc_server_threads));
        if (!server.available()) _exit(2);
        std::vector<uint8_t> buffer(kBufferLength);
        if (!server.registerLocalMemory(buffer.data(), buffer.size()).ok())
            _exit(3);

        const std::string segment = server.getSegmentName();
        uint32_t length = static_cast<uint32_t>(segment.size());
        if (write(ready_pipe[1], &length, sizeof(length)) != sizeof(length))
            _exit(4);
        if (write(ready_pipe[1], segment.data(), length) !=
            static_cast<ssize_t>(length))
            _exit(5);

        char stop = 0;
        const ssize_t stop_result = read(stop_pipe[0], &stop, 1);
        (void)stop_result;
        (void)server.unregisterLocalMemory(buffer.data(), buffer.size());
        _exit(0);
    }

    close(ready_pipe[1]);
    close(stop_pipe[0]);
    ChildProcessGuard child_guard(child, stop_pipe[1]);

    uint32_t segment_length = 0;
    ssize_t received =
        read(ready_pipe[0], &segment_length, sizeof(segment_length));
    if (received != static_cast<ssize_t>(sizeof(segment_length))) {
        const int status = child_guard.reap();
        GTEST_SKIP() << "TCP server initialization failed, child status "
                     << status;
    }
    std::string server_segment(segment_length, '\0');
    ASSERT_EQ(read(ready_pipe[0], server_segment.data(), segment_length),
              static_cast<ssize_t>(segment_length));

    TransferEngine client(makeTcpConfig(rpc_server_threads));
    ASSERT_TRUE(client.available());
    std::vector<uint8_t> buffer(kBufferLength, 0);
    for (size_t task = 0; task < kTaskCount; ++task) {
        uint8_t* slice = buffer.data() + task * kStride;
        for (size_t i = 0; i < kDataLength; ++i) {
            slice[i] = static_cast<uint8_t>((task * 131 + i * 7) & 0xff);
        }
    }
    ASSERT_TRUE(client.registerLocalMemory(buffer.data(), buffer.size()).ok());

    SegmentID segment = 0;
    Status result;
    for (int i = 0; i < 100; ++i) {
        result = client.openSegment(segment, server_segment);
        if (result.ok()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(result.ok()) << result.ToString();

    SegmentInfo info;
    ASSERT_TRUE(client.getSegmentInfo(segment, info).ok());
    ASSERT_FALSE(info.buffers.empty());

    // READ destination: a second region of the same registered buffer.
    std::vector<Request> write_requests;
    std::vector<Request> read_requests;
    for (size_t task = 0; task < kTaskCount; ++task) {
        Request write_request{};
        write_request.opcode = Request::WRITE;
        write_request.source = buffer.data() + task * kStride;
        write_request.target_id = segment;
        write_request.target_offset = info.buffers[0].base + task * kStride;
        write_request.length = kDataLength;
        write_request.transport_hint = TCP;
        write_requests.push_back(write_request);

        Request read_request{};
        read_request.opcode = Request::READ;
        read_request.source =
            buffer.data() + kBufferLength / 2 + task * kDataLength;
        read_request.target_id = segment;
        read_request.target_offset = info.buffers[0].base + task * kStride;
        read_request.length = kDataLength;
        read_request.transport_hint = TCP;
        read_requests.push_back(read_request);
    }
    // Patterned sources live in the first half (4MB of each 8MB stride);
    // READ destinations occupy the second half. No overlap.

    BatchID batch = client.allocateBatch(kTaskCount);
    ASSERT_TRUE(client.submitTransfer(batch, write_requests).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());

    batch = client.allocateBatch(kTaskCount);
    ASSERT_TRUE(client.submitTransfer(batch, read_requests).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());

    for (size_t task = 0; task < kTaskCount; ++task) {
        const uint8_t* written = buffer.data() + task * kStride;
        // Patterned sources occupy the first 4MB of each 8MB stride in the
        // buffer's first half; READ destinations are packed contiguously
        // into the second half, so they never overlap a source.
        const uint8_t* read_back =
            buffer.data() + kBufferLength / 2 + task * kDataLength;
        EXPECT_EQ(std::memcmp(written, read_back, kDataLength), 0)
            << "slice " << task << " mismatch";
    }

    EXPECT_TRUE(client.closeSegment(segment).ok());
    EXPECT_TRUE(
        client.unregisterLocalMemory(buffer.data(), buffer.size()).ok());

    const int status = child_guard.finish();
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(TcpDataPathRoundtripTest, WriteThenReadAcrossProcesses) {
    runWriteThenReadAcrossProcesses(1);
}

// Same round trip with a multi-threaded RPC server. SendData/RecvData are
// offloaded, so this also covers concurrent bulk copies overlapping other
// RPCs (MC_TENT_RPC_THREADS / rpc_server_threads).
TEST(TcpDataPathRoundtripTest, WriteThenReadAcrossProcessesMultiThreadedRpc) {
    runWriteThenReadAcrossProcesses(4);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
