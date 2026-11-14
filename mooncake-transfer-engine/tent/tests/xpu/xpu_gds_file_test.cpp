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

// End-to-end XPU <-> file transfers through GDS on a cuFile-compatible
// library (USE_XPU_GDS). Exercises registration -> routing -> GdsTransport ->
// the real library, so it needs an XPU and a file system the library supports.
// Set MC_TENT_XPU_GDS_DIR to a directory on that file system; the test skips
// when it is unset. Set MC_TENT_XPU_GDS_KEEP to keep the per-test files there
// for inspection instead of deleting them.
#include <gtest/gtest.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static tent::SelectionResult route(tent::TransferEngineImpl& engine,
                                       const tent::Request& request) {
        return engine.resolveTransport(request, 0);
    }
    static bool hasTag(tent::TransferEngineImpl& engine, void* address,
                       tent::TransportType type) {
        auto desc = engine.metadata_->segmentManager().getLocal();
        auto* buffer = desc->findBuffer(reinterpret_cast<uint64_t>(address), 1);
        return buffer &&
               std::find(buffer->transports.begin(), buffer->transports.end(),
                         type) != buffer->transports.end();
    }
};
namespace tent {
namespace {
// Spans two GDS slices (16 MiB + 4 KiB tail), a multiple of 4 KiB for
// O_DIRECT.
constexpr size_t kLength = (16UL << 20) + 4096;
constexpr uint64_t kFileOffset = 4096;

const char* gdsDir() { return std::getenv("MC_TENT_XPU_GDS_DIR"); }

std::vector<uint8_t> pattern(uint8_t salt) {
    std::vector<uint8_t> data(kLength);
    for (size_t i = 0; i < data.size(); ++i) data[i] = (i * 7 + salt) % 251;
    return data;
}

// Parameter: use_legacy_transport_selection.
class XpuGdsFileTest : public ::testing::TestWithParam<bool> {
   protected:
    std::shared_ptr<Config> config(bool io_uring) {
        auto c = std::make_shared<Config>();
        c->set("metadata_type", "p2p");
        c->set("metadata_servers", "");
        c->set("rpc_server_hostname", "127.0.0.1");
        c->set("rpc_server_port", "0");
        c->set("log_level", "warning");
        c->set("transports/tcp/enable", true);
        c->set("transports/rdma/enable", false);
        c->set("transports/shm/enable", false);
        c->set("transports/mpcomm/enable", false);
        c->set("transports/xpu/enable", true);
        c->set("transports/gds/enable", true);
        // Off unless a test needs it, so a GDS failure cannot be hidden by
        // falling back to io_uring.
        c->set("transports/io_uring/enable", io_uring);
        c->set("use_legacy_transport_selection", GetParam());
        return c;
    }
    void SetUp() override {
        if (!gdsDir()) GTEST_SKIP() << "MC_TENT_XPU_GDS_DIR is not set";
        auto& platform = Platform::getLoader(config(false));
        MemoryOptions opts;
        opts.location = "xpu:0";
        void* probe = nullptr;
        if (!platform.allocate(&probe, 4096, opts).ok())
            GTEST_SKIP() << "No SYCL device available";
        ASSERT_TRUE(platform.free(probe, 4096).ok());

        std::string tmpl =
            std::string(gdsDir()) + "/mooncake_tent_xpu_gds_XXXXXX";
        int fd = mkstemp(tmpl.data());
        ASSERT_GE(fd, 0) << "Cannot create a file in " << gdsDir();
        path_ = tmpl;
        int sized = ftruncate(fd, kFileOffset + kLength);
        close(fd);
        ASSERT_EQ(sized, 0);
    }
    void TearDown() override {
        if (engine_) {
            for (auto batch : batches_)
                EXPECT_TRUE(engine_->freeBatch(batch).ok());
            engine_.reset();
        }
        if (path_.empty()) return;
        if (std::getenv("MC_TENT_XPU_GDS_KEEP"))
            std::cout << "Kept " << path_ << std::endl;
        else
            unlink(path_.c_str());
    }
    void start(bool io_uring) {
        engine_ = std::make_unique<TransferEngineImpl>(config(io_uring));
        ASSERT_TRUE(engine_->available());
        ASSERT_TRUE(
            engine_->openSegment(file_, kLocalFileSegmentPrefix + path_).ok());
    }
    void allocate(const char* location, void** out) {
        MemoryOptions opts;
        opts.location = location;
        ASSERT_TRUE(engine_->allocateLocalMemory(out, kLength, opts).ok());
        ASSERT_NE(*out, nullptr);
        // allocateLocalMemory narrows opts.type to the allocating transport
        // (TCP here); register with UNSPEC so GDS also sees the buffer.
        opts.type = UNSPEC;
        ASSERT_TRUE(engine_->registerLocalMemory({*out}, {kLength}, opts).ok());
        // Only device memory can be registered with the library.
        EXPECT_EQ(TransferEngineImplTestPeer::hasTag(*engine_, *out, GDS),
                  std::string(location) != "cpu:0")
            << location;
    }
    Request request(Request::OpCode opcode, void* buffer) {
        Request r{};
        r.opcode = opcode;
        r.source = buffer;
        r.target_id = file_;
        r.target_offset = kFileOffset;
        r.length = kLength;
        return r;
    }
    void run(const Request& r) {
        auto batch = engine_->allocateBatch(1);
        ASSERT_NE(batch, 0u);
        batches_.push_back(batch);
        ASSERT_TRUE(engine_->submitTransfer(batch, {r}).ok());
        TransferStatus status{};
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        do {
            auto s = engine_->progressBatch(batch, status);
            ASSERT_TRUE(s.ok()) << s.ToString();
            if (status.s != PENDING) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        ASSERT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kLength);
    }
    // Write `buffer` (holding `data`) to the file, then check the file with a
    // plain pread, independent of the library.
    void writeAndCheckFile(void* buffer, const std::vector<uint8_t>& data) {
        ASSERT_NO_FATAL_FAILURE(run(request(Request::WRITE, buffer)));
        std::vector<uint8_t> file_data(kLength);
        int fd = open(path_.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        auto n = pread(fd, file_data.data(), kLength, kFileOffset);
        close(fd);
        ASSERT_EQ(n, static_cast<ssize_t>(kLength));
        EXPECT_EQ(file_data, data);
    }
    std::unique_ptr<TransferEngineImpl> engine_;
    std::vector<BatchID> batches_;
    std::string path_;
    SegmentID file_ = 0;
};

TEST_P(XpuGdsFileTest, DeviceWriteReachesFileThroughGds) {
    ASSERT_NO_FATAL_FAILURE(start(false));
    void* device = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate("xpu:0", &device));
    auto r = request(Request::WRITE, device);
    ASSERT_EQ(TransferEngineImplTestPeer::route(*engine_, r).transport, GDS);
    auto data = pattern(1);
    ASSERT_TRUE(Platform::getLoader().copy(device, data.data(), kLength).ok());
    ASSERT_NO_FATAL_FAILURE(writeAndCheckFile(device, data));
}

TEST_P(XpuGdsFileTest, DeviceReadReturnsFileContentsThroughGds) {
    ASSERT_NO_FATAL_FAILURE(start(false));
    auto data = pattern(2);
    int fd = open(path_.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    auto n = pwrite(fd, data.data(), kLength, kFileOffset);
    int synced = fsync(fd);
    close(fd);
    ASSERT_EQ(n, static_cast<ssize_t>(kLength));
    ASSERT_EQ(synced, 0);

    void* device = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate("xpu:0", &device));
    auto& platform = Platform::getLoader();
    std::vector<uint8_t> readback(kLength, 0);
    ASSERT_TRUE(platform.copy(device, readback.data(), kLength).ok());
    auto r = request(Request::READ, device);
    ASSERT_EQ(TransferEngineImplTestPeer::route(*engine_, r).transport, GDS);
    ASSERT_NO_FATAL_FAILURE(run(r));
    ASSERT_TRUE(platform.copy(readback.data(), device, kLength).ok());
    EXPECT_EQ(readback, data);
}

// The library cannot register host memory, so GDS does not advertise
// DRAM <-> file and host buffers have no route with GDS alone.
TEST_P(XpuGdsFileTest, HostBufferHasNoRouteWithGdsOnly) {
    ASSERT_NO_FATAL_FAILURE(start(false));
    void* host = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate("cpu:0", &host));
    auto r = request(Request::WRITE, host);
    EXPECT_EQ(TransferEngineImplTestPeer::route(*engine_, r).transport, UNSPEC);
}

// With io_uring enabled as well, device buffers keep using GDS and host
// buffers go to io_uring.
TEST_P(XpuGdsFileTest, HostBufferUsesIoUringNextToGds) {
    ASSERT_NO_FATAL_FAILURE(start(true));
    void *device = nullptr, *host = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate("xpu:0", &device));
    ASSERT_NO_FATAL_FAILURE(allocate("cpu:0", &host));
    EXPECT_EQ(TransferEngineImplTestPeer::route(*engine_,
                                                request(Request::WRITE, device))
                  .transport,
              GDS);
    auto r = request(Request::WRITE, host);
    ASSERT_EQ(TransferEngineImplTestPeer::route(*engine_, r).transport,
              IOURING);
    auto data = pattern(3);
    ASSERT_TRUE(Platform::getLoader().copy(host, data.data(), kLength).ok());
    ASSERT_NO_FATAL_FAILURE(writeAndCheckFile(host, data));
}

INSTANTIATE_TEST_SUITE_P(SelectionModes, XpuGdsFileTest, ::testing::Bool());
}  // namespace
}  // namespace tent
}  // namespace mooncake
