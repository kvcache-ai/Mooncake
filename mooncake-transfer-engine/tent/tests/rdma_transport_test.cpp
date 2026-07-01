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
#include <infiniband/verbs.h>
#include <sys/wait.h>
#include <unistd.h>

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
#include "tent/transport/rdma/params.h"
#include "tent/transport/rdma/rdma_transport.h"

namespace mooncake {
namespace tent {
namespace {

bool hasRdmaDevice() {
    int count = 0;
    ibv_device** devices = ibv_get_device_list(&count);
    const bool available = devices != nullptr && count > 0;
    if (devices) ibv_free_device_list(devices);
    return available;
}

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

std::shared_ptr<Config> makeRdmaConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("transports/rdma/enable", true);
    config->set("transports/tcp/enable", false);
    config->set("transports/shm/enable", false);
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

TEST(RdmaParamsTest, DefaultsKeepLaneCountsAligned) {
    RdmaParams params;

    EXPECT_EQ(params.num_lanes, 6);
    EXPECT_EQ(params.device.num_cq_list, params.num_lanes);
    EXPECT_EQ(params.endpoint.qp_mul_factor, params.num_lanes);
    EXPECT_EQ(params.workers.num_workers, params.num_lanes);
    EXPECT_EQ(params.endpoint.path_mtu, IBV_MTU_4096);
}

TEST(RdmaSubBatchTest, ReportsTaskCount) {
    RdmaSubBatch batch;
    batch.max_size = 8;

    EXPECT_EQ(batch.size(), 0);
    batch.task_list.push_back(nullptr);
    batch.task_list.push_back(nullptr);
    EXPECT_EQ(batch.size(), 2);
}

TEST(RdmaTransportIntegrationTest, WriteThenReadAcrossProcesses) {
    if (!hasRdmaDevice()) GTEST_SKIP() << "no RDMA device detected";

    constexpr size_t kDataLength = 4 * 1024 * 1024;
    int ready_pipe[2];
    int stop_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);
    ASSERT_EQ(pipe(stop_pipe), 0);

    pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(ready_pipe[0]);
        close(stop_pipe[1]);

        TransferEngine server(makeRdmaConfig());
        if (!server.available()) _exit(2);
        std::vector<uint8_t> buffer(kDataLength);
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
        GTEST_SKIP() << "RDMA server initialization failed, child status "
                     << status;
    }
    std::string server_segment(segment_length, '\0');
    ASSERT_EQ(read(ready_pipe[0], server_segment.data(), segment_length),
              static_cast<ssize_t>(segment_length));

    TransferEngine client(makeRdmaConfig());
    ASSERT_TRUE(client.available());
    std::vector<uint8_t> buffer(kDataLength * 2);
    for (size_t i = 0; i < kDataLength; ++i) {
        buffer[i] = static_cast<uint8_t>((i * 31) & 0xff);
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

    Request request{};
    request.opcode = Request::WRITE;
    request.source = buffer.data();
    request.target_id = segment;
    request.target_offset = info.buffers[0].base;
    request.length = kDataLength;
    request.transport_hint = RDMA;

    BatchID batch = client.allocateBatch(1);
    ASSERT_TRUE(client.submitTransfer(batch, {request}).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());

    request.opcode = Request::READ;
    request.source = buffer.data() + kDataLength;
    batch = client.allocateBatch(1);
    ASSERT_TRUE(client.submitTransfer(batch, {request}).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());
    EXPECT_EQ(
        std::memcmp(buffer.data(), buffer.data() + kDataLength, kDataLength),
        0);

    EXPECT_TRUE(client.closeSegment(segment).ok());
    EXPECT_TRUE(
        client.unregisterLocalMemory(buffer.data(), buffer.size()).ok());

    const int status = child_guard.finish();
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
