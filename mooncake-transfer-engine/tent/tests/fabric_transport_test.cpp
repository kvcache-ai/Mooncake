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

// End-to-end tests for the TENT fabric transport on libfabric's tcp;ofi_rxm
// provider, so they run on any Linux host with libfabric installed. A forked
// server process registers a buffer; the client WRITEs a pattern into it and
// READs it back over the fabric transport only. Set
// MC_FABRIC_TEST_PROVIDER=efa to run the same cases on EFA hardware.

#include <gtest/gtest.h>
#include <rdma/fabric.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
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

constexpr size_t kBufferLength = 64 * 1024 * 1024;

// Transport provider name and the libfabric provider string behind it.
std::string testProvider() {
    const char* env = std::getenv("MC_FABRIC_TEST_PROVIDER");
    return env && *env ? env : "tcp";
}

std::string libfabricProvider() {
    const std::string provider = testProvider();
    return provider == "tcp" ? "tcp;ofi_rxm" : provider;
}

bool providerAvailable() {
    struct fi_info* hints = fi_allocinfo();
    hints->ep_attr->type = FI_EP_RDM;
    hints->caps = FI_RMA;
    hints->fabric_attr->prov_name = strdup(libfabricProvider().c_str());
    struct fi_info* info = nullptr;
    int rc = fi_getinfo(fi_version(), nullptr, nullptr, 0, hints, &info);
    fi_freeinfo(hints);
    if (info) fi_freeinfo(info);
    return rc == 0;
}

std::shared_ptr<Config> makeFabricConfig(uint64_t max_mr_size) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("transports/tcp/enable", false);
    config->set("transports/rdma/enable", false);
    config->set("transports/shm/enable", false);
    config->set("transports/fabric/enable", true);
    config->set("transports/fabric/provider", testProvider());
    config->set("transports/fabric/slice_size", 256 * 1024);
    config->set("transports/fabric/max_mr_size", max_mr_size);
    config->set("transports/fabric/post_timeout_ms", 5000);
    config->set("transports/fabric/op_timeout_ms", 3000);
    return config;
}

int waitBatch(TransferEngine& engine, BatchID batch, int timeout_ms = 20000) {
    TransferStatus status;
    for (int i = 0; i < timeout_ms; ++i) {
        auto result = engine.getTransferStatus(batch, status);
        if (!result.ok() || status.s == TransferStatusEnum::FAILED) return -1;
        if (status.s == TransferStatusEnum::COMPLETED) return 1;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return 0;
}

uint8_t pattern(size_t i, size_t seed) {
    return static_cast<uint8_t>((i * 7 + seed * 131 + (i >> 12)) & 0xff);
}

struct Server {
    pid_t pid = -1;
    int stop_fd = -1;
    std::string segment;

    ~Server() { stop(); }

    // Returns false if the child could not start a fabric engine.
    bool start(uint64_t max_mr_size) {
        int ready[2], stop_pipe[2];
        if (pipe(ready) || pipe(stop_pipe)) return false;
        pid = fork();
        if (pid < 0) return false;
        if (pid == 0) {
            close(ready[0]);
            close(stop_pipe[1]);
            TransferEngine server(makeFabricConfig(max_mr_size));
            if (!server.available()) _exit(2);
            std::vector<uint8_t> buffer(kBufferLength, 0);
            if (!server.registerLocalMemory(buffer.data(), kBufferLength).ok())
                _exit(3);
            const std::string name = server.getSegmentName();
            uint32_t len = name.size();
            if (write(ready[1], &len, sizeof(len)) != sizeof(len)) _exit(4);
            if (write(ready[1], name.data(), len) != (ssize_t)len) _exit(5);
            char c;
            (void)!read(stop_pipe[0], &c, 1);
            (void)server.unregisterLocalMemory(buffer.data(), kBufferLength);
            _exit(0);
        }
        close(ready[1]);
        close(stop_pipe[0]);
        stop_fd = stop_pipe[1];
        uint32_t len = 0;
        bool ok = read(ready[0], &len, sizeof(len)) == sizeof(len);
        if (ok) {
            segment.resize(len);
            ok = read(ready[0], segment.data(), len) == (ssize_t)len;
        }
        close(ready[0]);
        return ok;
    }

    int stop() {
        if (pid <= 0) return 0;
        close(stop_fd);
        int status = 0;
        waitpid(pid, &status, 0);
        pid = -1;
        return status;
    }

    void kill() {
        if (pid <= 0) return;
        ::kill(pid, SIGKILL);
        int status = 0;
        waitpid(pid, &status, 0);
        close(stop_fd);
        pid = -1;
    }
};

class FabricTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (!providerAvailable())
            GTEST_SKIP() << "libfabric provider " << libfabricProvider()
                         << " not available";
    }

    // Starts the server and a client engine with a registered buffer, and
    // opens the server segment. Chunk sizes may differ on the two sides.
    void connect(uint64_t server_mr, uint64_t client_mr) {
        ASSERT_TRUE(server_.start(server_mr)) << "fabric server did not start";
        client_ = std::make_unique<TransferEngine>(makeFabricConfig(client_mr));
        ASSERT_TRUE(client_->available());
        buffer_.assign(kBufferLength, 0);
        ASSERT_TRUE(
            client_->registerLocalMemory(buffer_.data(), kBufferLength).ok());
        Status result;
        for (int i = 0; i < 100; ++i) {
            result = client_->openSegment(segment_, server_.segment);
            if (result.ok()) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ASSERT_TRUE(result.ok()) << result.ToString();
        SegmentInfo info;
        ASSERT_TRUE(client_->getSegmentInfo(segment_, info).ok());
        ASSERT_FALSE(info.buffers.empty());
        remote_base_ = info.buffers[0].base;
    }

    void TearDown() override {
        if (client_) {
            if (segment_) (void)client_->closeSegment(segment_);
            (void)client_->unregisterLocalMemory(buffer_.data(), kBufferLength);
            client_.reset();
        }
        server_.stop();
    }

    Request makeRequest(Request::OpCode op, size_t local, size_t remote,
                        size_t length) {
        Request request{};
        request.opcode = op;
        request.source = buffer_.data() + local;
        request.target_id = segment_;
        request.target_offset = remote_base_ + remote;
        request.length = length;
        request.transport_hint = FABRIC;
        return request;
    }

    int run(const std::vector<Request>& requests) {
        BatchID batch = client_->allocateBatch(requests.size());
        auto status = client_->submitTransfer(batch, requests);
        if (!status.ok()) {
            (void)client_->freeBatch(batch);
            return -2;
        }
        int done = waitBatch(*client_, batch);
        (void)client_->freeBatch(batch);
        return done;
    }

    // WRITE n slices [local_off + i*stride) -> [remote_off + i*stride), READ
    // them back into the upper half of the local buffer and compare.
    void roundTrip(size_t local_off, size_t remote_off, size_t length,
                   size_t count, size_t stride) {
        const size_t readback = kBufferLength / 2;
        ASSERT_LE(local_off + count * stride, readback);
        std::vector<Request> writes, reads;
        for (size_t t = 0; t < count; ++t) {
            uint8_t* src = buffer_.data() + local_off + t * stride;
            for (size_t i = 0; i < length; ++i) src[i] = pattern(i, t);
            writes.push_back(makeRequest(Request::WRITE, local_off + t * stride,
                                         remote_off + t * stride, length));
            reads.push_back(makeRequest(Request::READ,
                                        readback + local_off + t * stride,
                                        remote_off + t * stride, length));
        }
        ASSERT_EQ(run(writes), 1);
        ASSERT_EQ(run(reads), 1);
        for (size_t t = 0; t < count; ++t) {
            EXPECT_EQ(
                std::memcmp(buffer_.data() + local_off + t * stride,
                            buffer_.data() + readback + local_off + t * stride,
                            length),
                0)
                << "slice " << t;
        }
    }

    Server server_;
    std::unique_ptr<TransferEngine> client_;
    std::vector<uint8_t> buffer_;
    SegmentID segment_ = 0;
    uint64_t remote_base_ = 0;
};

TEST_F(FabricTransportTest, WriteThenRead) {
    connect(0, 0);
    roundTrip(0, 0, 4 * 1024 * 1024, 8, 4 * 1024 * 1024);
}

TEST_F(FabricTransportTest, SmallUnalignedTransfers) {
    connect(0, 0);
    roundTrip(3, 1001, 1, 4, 4099);
    roundTrip(17, 5, 777, 16, 8191);
}

// Registrations split into chunks of different sizes on each side, so slices
// must be cut at both chunk boundaries.
TEST_F(FabricTransportTest, TransfersStraddleChunks) {
    connect(3 * 1024 * 1024, 5 * 1024 * 1024);
    roundTrip(1024 * 1024 - 100, 2 * 1024 * 1024 + 7, 9 * 1024 * 1024 + 3, 2,
              11 * 1024 * 1024);
}

TEST_F(FabricTransportTest, LoopbackToOwnSegment) {
    connect(0, 0);
    const size_t length = 1024 * 1024;
    for (size_t i = 0; i < length; ++i) buffer_[i] = pattern(i, 9);
    Request request{};
    request.opcode = Request::WRITE;
    request.source = buffer_.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset =
        reinterpret_cast<uint64_t>(buffer_.data()) + 8 * 1024 * 1024;
    request.length = length;
    request.transport_hint = FABRIC;
    ASSERT_EQ(run({request}), 1);
    EXPECT_EQ(
        std::memcmp(buffer_.data(), buffer_.data() + 8 * 1024 * 1024, length),
        0);
}

TEST_F(FabricTransportTest, UnregisteredTargetIsRejected) {
    connect(0, 0);
    auto request = makeRequest(Request::WRITE, 0, kBufferLength - 10, 100);
    EXPECT_NE(run({request}), 1);
}

TEST_F(FabricTransportTest, PeerLossFails) {
    connect(0, 0);
    roundTrip(0, 0, 64 * 1024, 1, 64 * 1024);
    server_.kill();
    std::vector<Request> requests;
    for (size_t t = 0; t < 4; ++t)
        requests.push_back(makeRequest(Request::WRITE, t * 1024 * 1024,
                                       t * 1024 * 1024, 1024 * 1024));
    const int result = run(requests);
    EXPECT_TRUE(result == -1 || result == -2) << "result " << result;
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
