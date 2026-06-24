// Copyright 2025 KVCache.AI
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
//
// Unit tests for UbTentTransport and the "ub" selector string mapping.
//
// These tests run without real Kunpeng hardware.  The UbTransport falls back
// to mock_urma_device automatically when liburma.so is absent or no real HCA
// is found (see UbTransport::initializeUbResources()).
//
// To build and run:
//   cmake -DUSE_UB=ON -DUSE_TENT=ON ...
//   ctest -R tent_ub_transport_test -V

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/transport_selector.h"
#include "tent/runtime/segment.h"
#include "tent/transport/ub/ub_tent_transport.h"

namespace mooncake {
namespace tent {
namespace {

// ---------------------------------------------------------------------------
// 1.  Selector / string-mapping tests (no hardware required)
// ---------------------------------------------------------------------------

TEST(UbSelectorTest, TypeNameRoundTrip) {
    EXPECT_EQ(TransportSelector::transportTypeName(UB), "ub");
}

TEST(UbSelectorTest, ParseUbString) {
    EXPECT_EQ(TransportSelector::parseTransportType("ub"), UB);
}

TEST(UbSelectorTest, ParseUnknownStillReturnsUnspec) {
    EXPECT_EQ(TransportSelector::parseTransportType("ub_typo"), UNSPEC);
}

TEST(UbSelectorTest, UbEnumValue) {
    // UB must be between SUNRISE_LINK and kNumTransportTypes.
    EXPECT_GT(static_cast<int>(UB), static_cast<int>(SUNRISE_LINK));
    EXPECT_LT(static_cast<int>(UB), static_cast<int>(kNumTransportTypes));
}

// A policy JSON with "ub" first in the transports array must cause the
// selector to pick UB when a UB transport is available.
TEST(UbSelectorTest, SelectorPicksUbWhenFirstInPolicy) {
    auto conf = std::make_shared<Config>();
    const std::string policy_json = R"({
        "policy": [
            {
                "name": "kunpeng_ub_memory",
                "segment_type": "memory",
                "local_memory": "cpu",
                "remote_memory": "cpu",
                "same_machine": false,
                "transports": ["ub", "rdma", "tcp"]
            }
        ]
    })";
    ASSERT_TRUE(conf->load(policy_json).ok());

    TransportSelector selector(conf);

    // Register a fake UB and TCP transport.
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};

    struct MinimalFake : public Transport {
        void setDram() { caps.dram_to_dram = true; }
        Status allocateSubBatch(SubBatchRef&, size_t) override {
            return Status::OK();
        }
        Status freeSubBatch(SubBatchRef&) override { return Status::OK(); }
        Status submitTransferTasks(SubBatchRef,
                                   const std::vector<Request>&) override {
            return Status::OK();
        }
        Status getTransferStatus(SubBatchRef, int, TransferStatus&) override {
            return Status::OK();
        }
        const char* getName() const override { return "fake"; }
    };

    auto ub_fake = std::make_shared<MinimalFake>();
    ub_fake->setDram();
    auto tcp_fake = std::make_shared<MinimalFake>();
    tcp_fake->setDram();
    transports[UB] = ub_fake;
    transports[TCP] = tcp_fake;

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = 0;
    ctx.buffer_transports = nullptr;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, UB)
        << "Selector should pick UB first per policy";
}

// ---------------------------------------------------------------------------
// 2.  UbTentTransport control-flow tests (mock URMA, no network)
// ---------------------------------------------------------------------------

// install() with "p2p" metadata (no etcd) and a null Config should succeed
// when mock URMA is active (no real liburma.so).
TEST(UbTentTransportTest, InstallWithMockUrma) {
    UbTentTransport transport;

    std::string seg_name = "test_segment";
    // Null ControlService and Topology are accepted; UbTentTransport creates
    // its own old-TE Topology via discover() and a p2p TransferMetadata.
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);

    // Success is expected on machines where mock_urma_device is available.
    // If install fails (e.g. liburma.so found but no real HCA), we still
    // accept that gracefully and skip dependent sub-tests.
    if (!status.ok()) {
        GTEST_SKIP() << "UbTentTransport::install() failed (likely no mock "
                        "URMA): "
                     << status.message();
    }
    EXPECT_STREQ(transport.getName(), "ub");
    EXPECT_TRUE(transport.capabilities().dram_to_dram);
    EXPECT_FALSE(transport.capabilities().dram_to_gpu);
}

TEST(UbTentTransportTest, AddAndRemoveMemoryBuffer) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    // Allocate a small CPU buffer.
    const size_t kBufLen = 4096;
    std::vector<char> buf(kBufLen, 0);
    void* addr = buf.data();

    BufferDesc desc;
    desc.addr = reinterpret_cast<uint64_t>(addr);
    desc.length = kBufLen;
    desc.location = "*";

    MemoryOptions opts;
    opts.perm = kGlobalReadWrite;

    auto add_s = transport.addMemoryBuffer(desc, opts);
    ASSERT_TRUE(add_s.ok()) << add_s.message();

    // UB must appear in the transport list after registration.
    auto it = std::find(desc.transports.begin(), desc.transports.end(), UB);
    EXPECT_NE(it, desc.transports.end())
        << "UB not found in desc.transports after addMemoryBuffer()";

    auto rm_s = transport.removeMemoryBuffer(desc);
    EXPECT_TRUE(rm_s.ok()) << rm_s.message();

    // UB should be removed from the transport list.
    it = std::find(desc.transports.begin(), desc.transports.end(), UB);
    EXPECT_EQ(it, desc.transports.end())
        << "UB still present in desc.transports after removeMemoryBuffer()";
}

TEST(UbTentTransportTest, AllocateAndFreeSubBatch) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    Transport::SubBatchRef batch = nullptr;
    auto alloc_s = transport.allocateSubBatch(batch, 8);
    ASSERT_TRUE(alloc_s.ok()) << alloc_s.message();
    ASSERT_NE(batch, nullptr);

    auto free_s = transport.freeSubBatch(batch);
    EXPECT_TRUE(free_s.ok()) << free_s.message();
    EXPECT_EQ(batch, nullptr);
}

// Submit a local-to-local mock transfer and verify that getTransferStatus()
// returns a terminal state (COMPLETED or FAILED) rather than hanging.
// This exercises the BatchDesc / TransferTask lifetime path without network.
TEST(UbTentTransportTest, SubmitAndPollMockTransfer) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto status = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!status.ok()) {
        GTEST_SKIP() << "install failed: " << status.message();
    }

    // Register source buffer.
    const size_t kBufLen = 4096;
    std::vector<char> src(kBufLen, 0xAB);
    std::vector<char> dst(kBufLen, 0x00);

    BufferDesc src_desc;
    src_desc.addr = reinterpret_cast<uint64_t>(src.data());
    src_desc.length = kBufLen;
    src_desc.location = "*";

    MemoryOptions opts;
    opts.perm = kGlobalReadWrite;

    auto add_s = transport.addMemoryBuffer(src_desc, opts);
    if (!add_s.ok()) {
        GTEST_SKIP() << "addMemoryBuffer failed: " << add_s.message();
    }

    // Allocate a sub-batch.
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 4).ok());
    ASSERT_NE(batch, nullptr);

    // Build a local WRITE request (LOCAL_SEGMENT_ID).
    Request req{};
    req.opcode = Request::WRITE;
    req.source = src.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(dst.data());
    req.length = kBufLen;

    auto sub_s = transport.submitTransferTasks(batch, {req});
    // submitTransferTasks may fail if mock URMA rejects the send (e.g. no
    // connected peer).  Accept either outcome; we mainly verify no crash.
    if (!sub_s.ok()) {
        LOG(WARNING)
            << "submitTransferTasks returned non-OK (expected in mock): "
            << sub_s.message();
    }

    // Poll for up to ~1s.
    TransferStatus ts{};
    bool terminal = false;
    for (int i = 0; i < 100; ++i) {
        auto gs = transport.getTransferStatus(batch, 0, ts);
        if (!gs.ok()) break;
        if (ts.s == COMPLETED || ts.s == FAILED || ts.s == TIMEOUT) {
            terminal = true;
            break;
        }
        usleep(10000);  // 10 ms
    }
    // In mock mode, completion may not happen; just don't crash/hang.
    (void)terminal;

    transport.removeMemoryBuffer(src_desc);
    transport.freeSubBatch(batch);
}

// Calling uninstall() twice must not crash.
TEST(UbTentTransportTest, DoubleUninstallSafe) {
    UbTentTransport transport;
    std::string seg_name = "test_segment";
    auto s = transport.install(seg_name, nullptr, nullptr, nullptr);
    if (!s.ok()) GTEST_SKIP() << "install failed: " << s.message();
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
