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

// Tests for MpcommTransport.
//
// TransportTypeMapping needs no hardware and always runs.
//
// WriteAndRead forks a target process and drives a WRITE followed by a READ
// back over MPComm, verifying the payload. It requires RDMA devices and a
// working MPComm installation, and is skipped when the engine cannot be
// brought up. Parent and child use distinct MPCOMM_TCP_PORT values because
// MPComm's metadata handshake listener would otherwise collide; the ports are
// derived from the pid to keep concurrent runs apart, and are kept outside
// 15000-17000 which CoroRpcAgent may pick at random for the tent RPC server.
//
// The target publishes its real segment name (host:port, assigned by the tent
// RPC server) over a pipe, so no tent port has to be hardcoded here.

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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
constexpr size_t kPageSize = 4096;
constexpr char kPattern = 'M';

// Allocate page-aligned host memory directly instead of going through
// TransferEngine::allocateLocalMemory(): that helper maps UNSPEC to RDMA or
// TCP and fails outright when neither is enabled, and this test deliberately
// leaves mpcomm as the only enabled transport.
void* AllocAligned(size_t size) {
    void* p = nullptr;
    if (posix_memalign(&p, kPageSize, size) != 0) return nullptr;
    return p;
}

std::shared_ptr<Config> makeConfig(const std::string& segment_name) {
    auto conf = std::make_shared<Config>();
    conf->set("metadata_type", "p2p");
    conf->set("metadata_servers", "P2PHANDSHAKE");
    conf->set("local_segment_name", segment_name);
    // Enable mpcomm only: with every other transport disabled a routing
    // regression fails the test instead of silently falling back to RDMA.
    conf->set("transports/mpcomm/enable", true);
    conf->set("transports/rdma/enable", false);
    conf->set("transports/tcp/enable", false);
    conf->set("transports/shm/enable", false);
    conf->set("transports/nvlink/enable", false);
    conf->set("transports/mnnvl/enable", false);
    conf->set("transports/gds/enable", false);
    conf->set("transports/io_uring/enable", false);
    return conf;
}

void SetMpcommPort(int port) {
    const std::string value = std::to_string(port);
    setenv("MPCOMM_TCP_PORT", value.c_str(), 1);
}

void WaitBatchDone(TransferEngine* engine, BatchID batch_id) {
    TransferStatus status;
    for (int i = 0; i < 5000; ++i) {
        auto s = engine->getTransferStatus(batch_id, status);
        ASSERT_TRUE(s.ok()) << "getTransferStatus failed: " << s.ToString();
        if (status.s == TransferStatusEnum::COMPLETED) return;
        ASSERT_NE(status.s, TransferStatusEnum::FAILED);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    FAIL() << "timeout waiting batch completion";
}

// Exercises the mpcomm-specific entries of the transport name mappings. A
// missing entry would make policies naming "mpcomm" silently resolve to
// UNSPEC, so keep this covered even though it needs no hardware.
TEST(MpcommTransportTest, TransportTypeMapping) {
    EXPECT_STREQ(transportTypeName(MPCOMM), "mpcomm");
    EXPECT_EQ(parseTransportType("mpcomm"), MPCOMM);
    EXPECT_LT(static_cast<int>(MPCOMM), kSupportedTransportTypes);
}

TEST(MpcommTransportTest, WriteAndRead) {
    const int base_port = 23000 + static_cast<int>(getpid() % 1000);
    const std::string target_name = "mpcomm_ut_target";

    int ready_pipe[2];
    int stop_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);
    ASSERT_EQ(pipe(stop_pipe), 0);

    pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(ready_pipe[0]);
        close(stop_pipe[1]);
        SetMpcommPort(base_port);
        auto engine = std::make_unique<TransferEngine>(makeConfig(target_name));
        if (!engine->available()) _exit(2);

        void* buffer = AllocAligned(kDataLength);
        if (buffer == nullptr) {
            fprintf(stderr, "[target] posix_memalign failed\n");
            _exit(3);
        }
        MemoryOptions options;
        options.location = "cpu:0";
        auto rs = engine->registerLocalMemory(buffer, kDataLength, options);
        if (!rs.ok()) {
            fprintf(stderr, "[target] registerLocalMemory failed: %s\n",
                    rs.ToString().c_str());
            _exit(4);
        }

        // The tent RPC port is assigned during construction, so the segment
        // name is only final at this point.
        const std::string segment = engine->getSegmentName();
        const uint32_t len = static_cast<uint32_t>(segment.size());
        if (write(ready_pipe[1], &len, sizeof(len)) != sizeof(len)) _exit(5);
        if (write(ready_pipe[1], segment.data(), len) !=
            static_cast<ssize_t>(len))
            _exit(6);

        char stop = 0;
        (void)read(stop_pipe[0], &stop, 1);
        (void)engine->unregisterLocalMemory(buffer, kDataLength);
        free(buffer);
        _exit(0);
    }

    close(ready_pipe[1]);
    close(stop_pipe[0]);

    uint32_t segment_len = 0;
    const ssize_t got = read(ready_pipe[0], &segment_len, sizeof(segment_len));
    if (got != static_cast<ssize_t>(sizeof(segment_len))) {
        int wstatus = 0;
        (void)waitpid(child, &wstatus, 0);
        if (got == 0 && WIFEXITED(wstatus)) {
            GTEST_SKIP() << "mpcomm target init failed, child exit="
                         << WEXITSTATUS(wstatus)
                         << " (2=engine unavailable, 3=alloc, 4=register;"
                            " see [target] lines above for details)";
        }
        ASSERT_EQ(got, static_cast<ssize_t>(sizeof(segment_len)));
    }
    ASSERT_GT(segment_len, 0u);
    std::string target_segment(segment_len, '\0');
    ASSERT_EQ(read(ready_pipe[0], target_segment.data(), segment_len),
              static_cast<ssize_t>(segment_len));

    // Distinct handshake port: the target already listens on base_port.
    SetMpcommPort(base_port + 1);
    auto engine = std::make_unique<TransferEngine>(makeConfig("mpcomm_ut"));
    ASSERT_TRUE(engine->available());

    // Two halves: the first is written out, the second receives the read back.
    void* buffer = AllocAligned(kDataLength * 2);
    ASSERT_NE(buffer, nullptr);
    MemoryOptions options;
    options.location = "cpu:0";
    auto s = engine->registerLocalMemory(buffer, kDataLength * 2, options);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto* bytes = static_cast<uint8_t*>(buffer);
    memset(bytes, kPattern, kDataLength);
    memset(bytes + kDataLength, 0, kDataLength);

    SegmentID segment_id = 0;
    for (int i = 0; i < 100; ++i) {
        s = engine->openSegment(segment_id, target_segment);
        if (s.ok()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(s.ok()) << s.ToString();

    SegmentInfo segment_info;
    s = engine->getSegmentInfo(segment_id, segment_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(segment_info.buffers.empty());
    const uint64_t remote_base = segment_info.buffers[0].base;

    BatchID batch = engine->allocateBatch(1);
    Request request;
    request.opcode = Request::WRITE;
    request.length = kDataLength;
    request.source = bytes;
    request.target_id = segment_id;
    request.target_offset = remote_base;
    request.transport_hint = TransportType::MPCOMM;
    s = engine->submitTransfer(batch, {request});
    ASSERT_TRUE(s.ok()) << s.ToString();
    WaitBatchDone(engine.get(), batch);
    ASSERT_TRUE(engine->freeBatch(batch).ok());

    batch = engine->allocateBatch(1);
    request.opcode = Request::READ;
    request.source = bytes + kDataLength;
    s = engine->submitTransfer(batch, {request});
    ASSERT_TRUE(s.ok()) << s.ToString();
    WaitBatchDone(engine.get(), batch);
    ASSERT_TRUE(engine->freeBatch(batch).ok());

    for (size_t i = 0; i < kDataLength; ++i) {
        ASSERT_EQ(bytes[kDataLength + i], kPattern) << "mismatch at " << i;
    }

    ASSERT_TRUE(engine->unregisterLocalMemory(buffer, kDataLength * 2).ok());
    free(buffer);

    const char stop = 'Q';
    ASSERT_EQ(write(stop_pipe[1], &stop, 1), 1);
    int wstatus = 0;
    ASSERT_EQ(waitpid(child, &wstatus, 0), child);
    ASSERT_TRUE(WIFEXITED(wstatus));
    ASSERT_EQ(WEXITSTATUS(wstatus), 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
