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

#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/transport_selector.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {
namespace {

// ---------------------------------------------------------------------------
// Minimal FakeTransport for availability checking
// ---------------------------------------------------------------------------

class FakeTransport : public Transport {
   public:
    explicit FakeTransport(TransportType type) : type_(type) {
        // Access protected member directly (inherited from Transport)
    }

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config> = nullptr) override {
        return Status::OK();
    }

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

    // Helper to set capabilities (accessing protected member)
    void setDramToFile(bool val) { caps.dram_to_file = val; }
    void setGpuToFile(bool val) { caps.gpu_to_file = val; }
    void setDramToDram(bool val) { caps.dram_to_dram = val; }
    void setGpuToGpu(bool val) { caps.gpu_to_gpu = val; }
    void setDramToGpu(bool val) { caps.dram_to_gpu = val; }
    void setGpuToDram(bool val) { caps.gpu_to_dram = val; }

   private:
    TransportType type_;
};

// ---------------------------------------------------------------------------
// Test default policies match original behavior
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, DefaultPoliciesFileSegment) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    // Create fake transports
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[GDS] = std::make_shared<FakeTransport>(GDS);
    transports[IOURING] = std::make_shared<FakeTransport>(IOURING);
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);

    // Set capabilities
    auto* gds = static_cast<FakeTransport*>(transports[GDS].get());
    auto* iouring = static_cast<FakeTransport*>(transports[IOURING].get());
    gds->setDramToFile(true);
    iouring->setDramToFile(true);

    // File segment with CPU memory should prefer GDS first
    SelectionContext ctx;
    ctx.segment_type = SegmentType::File;
    ctx.same_machine = true;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = 0;
    ctx.buffer_transports = nullptr;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, GDS) << "File segment should prefer GDS first";
}

TEST(TransportSelectorTest, DefaultPoliciesFileSegmentGpuMemory) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    // Create fake transports
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[GDS] = std::make_shared<FakeTransport>(GDS);
    transports[IOURING] = std::make_shared<FakeTransport>(IOURING);

    // Set capabilities for GPU
    auto* gds = static_cast<FakeTransport*>(transports[GDS].get());
    auto* iouring = static_cast<FakeTransport*>(transports[IOURING].get());
    gds->setGpuToFile(true);
    iouring->setGpuToFile(true);

    // File segment with GPU memory
    SelectionContext ctx;
    ctx.segment_type = SegmentType::File;
    ctx.same_machine = true;
    ctx.local_memory_type = MTYPE_CUDA;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = 0;
    ctx.buffer_transports = nullptr;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, GDS)
        << "File segment with GPU should prefer GDS first";
}

TEST(TransportSelectorTest, DefaultPoliciesMemorySegment) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    // Create fake transports
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    transports[TCP] = std::make_shared<FakeTransport>(TCP);

    // Set capabilities
    auto* rdma = static_cast<FakeTransport*>(transports[RDMA].get());
    rdma->setDramToDram(true);

    // Memory segment should use buffer_transports order
    std::vector<TransportType> buffer_transports = {RDMA, TCP};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = 0;
    ctx.buffer_transports = &buffer_transports;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, RDMA)
        << "Should use first in buffer_transports";
}

// ---------------------------------------------------------------------------
// Test transport type name parsing
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, TransportTypeNameMapping) {
    EXPECT_STREQ(transportTypeName(UNSPEC), "unspec");
    EXPECT_STREQ(transportTypeName(RDMA), "rdma");
    EXPECT_STREQ(transportTypeName(MNNVL), "mnnvl");
    EXPECT_STREQ(transportTypeName(SHM), "shm");
    EXPECT_STREQ(transportTypeName(NVLINK), "nvlink");
    EXPECT_STREQ(transportTypeName(GDS), "gds");
    EXPECT_STREQ(transportTypeName(IOURING), "io_uring");
    EXPECT_STREQ(transportTypeName(TCP), "tcp");
    EXPECT_STREQ(transportTypeName(AscendDirect), "ascend");
    EXPECT_STREQ(transportTypeName(SUNRISE_LINK), "sunrise_link");
}

TEST(TransportSelectorTest, ParseTransportType) {
    EXPECT_EQ(parseTransportType("unspec"), UNSPEC);
    EXPECT_EQ(parseTransportType("rdma"), RDMA);
    EXPECT_EQ(parseTransportType("mnnvl"), MNNVL);
    EXPECT_EQ(parseTransportType("shm"), SHM);
    EXPECT_EQ(parseTransportType("nvlink"), NVLINK);
    EXPECT_EQ(parseTransportType("gds"), GDS);
    EXPECT_EQ(parseTransportType("io_uring"), IOURING);
    EXPECT_EQ(parseTransportType("tcp"), TCP);
    EXPECT_EQ(parseTransportType("ascend"), AscendDirect);
    EXPECT_EQ(parseTransportType("sunrise_link"), SUNRISE_LINK);
    EXPECT_EQ(parseTransportType("unknown"), UNSPEC);
}

// ---------------------------------------------------------------------------
// Test legacy mode
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, LegacyModeDefaultOff) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    EXPECT_FALSE(selector.isLegacyMode())
        << "Legacy mode should be off by default";
}

TEST(TransportSelectorTest, LegacyModeCanBeEnabled) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    selector.setLegacyMode(true);
    EXPECT_TRUE(selector.isLegacyMode());

    selector.setLegacyMode(false);
    EXPECT_FALSE(selector.isLegacyMode());
}

// ---------------------------------------------------------------------------
// Test transport availability based on capabilities
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, TransportCapabilityDramToDram) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    auto* rdma = static_cast<FakeTransport*>(transports[RDMA].get());
    rdma->setDramToDram(true);

    std::vector<TransportType> buffer_transports = {RDMA};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, RDMA)
        << "RDMA should be available for CPU-to-CPU";
}

TEST(TransportSelectorTest, TransportCapabilityGpuToGpu) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    auto* rdma = static_cast<FakeTransport*>(transports[RDMA].get());
    rdma->setGpuToGpu(true);

    std::vector<TransportType> buffer_transports = {RDMA};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.local_memory_type = MTYPE_CUDA;
    ctx.remote_memory_type = MTYPE_CUDA;
    ctx.buffer_transports = &buffer_transports;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, RDMA)
        << "RDMA should be available for CUDA-to-CUDA";
}

TEST(TransportSelectorTest, TransportCapabilityDramToGpu) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    auto* rdma = static_cast<FakeTransport*>(transports[RDMA].get());
    rdma->setDramToGpu(true);

    std::vector<TransportType> buffer_transports = {RDMA};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CUDA;
    ctx.buffer_transports = &buffer_transports;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, RDMA)
        << "RDMA should be available for CPU-to-CUDA";
}

TEST(TransportSelectorTest, TransportCapabilityGpuToDram) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    auto* rdma = static_cast<FakeTransport*>(transports[RDMA].get());
    rdma->setGpuToDram(true);

    std::vector<TransportType> buffer_transports = {RDMA};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.local_memory_type = MTYPE_CUDA;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, RDMA)
        << "RDMA should be available for CUDA-to-CPU";
}

TEST(TransportSelectorTest, FileSegmentDramToFile) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[GDS] = std::make_shared<FakeTransport>(GDS);
    auto* gds = static_cast<FakeTransport*>(transports[GDS].get());
    gds->setDramToFile(true);

    SelectionContext ctx;
    ctx.segment_type = SegmentType::File;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.buffer_transports = nullptr;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, GDS)
        << "GDS should be available for File segment with CPU memory";
}

TEST(TransportSelectorTest, FileSegmentGpuToFile) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[GDS] = std::make_shared<FakeTransport>(GDS);
    auto* gds = static_cast<FakeTransport*>(transports[GDS].get());
    gds->setGpuToFile(true);

    SelectionContext ctx;
    ctx.segment_type = SegmentType::File;
    ctx.local_memory_type = MTYPE_CUDA;
    ctx.buffer_transports = nullptr;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, GDS)
        << "GDS should be available for File segment with CUDA memory";
}

// ---------------------------------------------------------------------------
// Test priority_offset for fallback
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, PriorityOffsetFallback) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    transports[TCP] = std::make_shared<FakeTransport>(TCP);

    auto* rdma = static_cast<FakeTransport*>(transports[RDMA].get());
    rdma->setDramToDram(true);
    auto* tcp = static_cast<FakeTransport*>(transports[TCP].get());
    tcp->setDramToDram(true);

    std::vector<TransportType> buffer_transports = {RDMA, TCP};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;

    // priority_offset=0: first transport
    auto result = selector.select(ctx, transports, 0);
    EXPECT_EQ(result.transport, RDMA);

    // priority_offset=1: second transport (fallback)
    result = selector.select(ctx, transports, 1);
    EXPECT_EQ(result.transport, TCP);

    // priority_offset=2: no more transports
    result = selector.select(ctx, transports, 2);
    EXPECT_EQ(result.transport, UNSPEC);
}

// ---------------------------------------------------------------------------
// Test device mask handling
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, DeviceMaskDefaultAll) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.buffer_transports = nullptr;

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.device_mask, ~0ULL)
        << "Default device mask should be all devices";
}

// ---------------------------------------------------------------------------
// Test NVLINK and SHM same-machine constraint (via select)
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, NvLinkRequiresSameMachine) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[NVLINK] = std::make_shared<FakeTransport>(NVLINK);
    auto* nvlink = static_cast<FakeTransport*>(transports[NVLINK].get());
    nvlink->setGpuToGpu(true);

    std::vector<TransportType> buffer_transports = {NVLINK};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;  // Remote
    ctx.local_memory_type = MTYPE_CUDA;
    ctx.remote_memory_type = MTYPE_CUDA;
    ctx.buffer_transports = &buffer_transports;

    // NVLINK should not be available for remote transfers
    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, UNSPEC)
        << "NVLINK should not be available for remote transfers";
}

TEST(TransportSelectorTest, NvLinkAvailableSameMachine) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[NVLINK] = std::make_shared<FakeTransport>(NVLINK);
    auto* nvlink = static_cast<FakeTransport*>(transports[NVLINK].get());
    nvlink->setGpuToGpu(true);

    std::vector<TransportType> buffer_transports = {NVLINK};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = true;
    ctx.local_memory_type = MTYPE_CUDA;
    ctx.remote_memory_type = MTYPE_CUDA;
    ctx.buffer_transports = &buffer_transports;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, NVLINK)
        << "NVLINK should be available for same-machine transfers";
}

// ---------------------------------------------------------------------------
// Test ROCm memory type
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, RocmMemoryTypeSupported) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    auto* rdma = static_cast<FakeTransport*>(transports[RDMA].get());
    rdma->setGpuToGpu(true);

    std::vector<TransportType> buffer_transports = {RDMA};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = true;
    ctx.local_memory_type = MTYPE_ROCM;
    ctx.remote_memory_type = MTYPE_ROCM;
    ctx.buffer_transports = &buffer_transports;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, RDMA)
        << "RDMA should be available for ROCm-to-ROCm";
}

// ---------------------------------------------------------------------------
// Test with configuration (policy-based selection)
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, ConfigBasedPolicySelection) {
    auto conf = std::make_shared<Config>();

    // Set up a custom policy via JSON config
    conf->set("policy", json::array());
    auto policies = conf->getArray<json>("policy");

    json policy;
    policy["name"] = "test_memory_policy";
    policy["segment_type"] = "memory";
    policy["transports"] = {"tcp", "rdma"};  // Prefer TCP over RDMA

    // We can't easily modify the config's internal JSON structure,
    // so this test verifies the selector at least loads without error

    TransportSelector selector(conf);

    // Default behavior should still work
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    transports[TCP] = std::make_shared<FakeTransport>(TCP);

    auto* rdma = static_cast<FakeTransport*>(transports[RDMA].get());
    rdma->setDramToDram(true);
    auto* tcp = static_cast<FakeTransport*>(transports[TCP].get());
    tcp->setDramToDram(true);

    std::vector<TransportType> buffer_transports = {RDMA, TCP};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;

    auto result = selector.select(ctx, transports);
    // With default policies, should use buffer_transports order (RDMA first)
    EXPECT_EQ(result.transport, RDMA);
}

// ---------------------------------------------------------------------------
// hint: per-request transport_hint hook on select()
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, FailoverFromHintAdvancesByIndex) {
    // Policy order [TCP, RDMA], hint=RDMA. Hint is prepended; failover by
    // bumping transport_index walks past the hint into the rest of raw
    // (with RDMA de-duped), so index=1 returns TCP — never returns RDMA
    // again.
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    transports[TCP] = std::make_shared<FakeTransport>(TCP);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);
    static_cast<FakeTransport*>(transports[TCP].get())->setDramToDram(true);

    std::vector<TransportType> buffer_transports = {TCP, RDMA};
    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;

    EXPECT_EQ(
        selector.select(ctx, transports, /*index=*/0, /*hint=*/RDMA).transport,
        RDMA);
    EXPECT_EQ(
        selector.select(ctx, transports, /*index=*/1, /*hint=*/RDMA).transport,
        TCP);
    // Past the end -> UNSPEC.
    EXPECT_EQ(
        selector.select(ctx, transports, /*index=*/2, /*hint=*/RDMA).transport,
        UNSPEC);
}

TEST(TransportSelectorTest, HintIsPrependedToCandidateList) {
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    transports[TCP] = std::make_shared<FakeTransport>(TCP);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);
    static_cast<FakeTransport*>(transports[TCP].get())->setDramToDram(true);

    // Default-policy memory path uses buffer_transports order = [RDMA, TCP].
    // hint=TCP must put TCP first, RDMA second.
    std::vector<TransportType> buffer_transports = {RDMA, TCP};
    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;

    auto r0 = selector.select(ctx, transports, /*index=*/0, /*hint=*/TCP);
    EXPECT_EQ(r0.transport, TCP);
    auto r1 = selector.select(ctx, transports, /*index=*/1, /*hint=*/TCP);
    EXPECT_EQ(r1.transport, RDMA);
}

TEST(TransportSelectorTest, HintNotInMatchingPolicyReturnsUnspec) {
    // Selector mode: the matching policy's transports list is the
    // authorization whitelist. A hint that is not in it produces UNSPEC
    // (downstream the engine turns this into task = FAILED).
    auto conf = std::make_shared<Config>();
    TransportSelector selector(conf);

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    transports[TCP] = std::make_shared<FakeTransport>(TCP);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);
    static_cast<FakeTransport*>(transports[TCP].get())->setDramToDram(true);

    // Buffer is only registered with TCP. hint=RDMA must fail even though
    // RDMA is installed and capable.
    std::vector<TransportType> buffer_transports = {TCP};
    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;

    auto r = selector.select(ctx, transports, /*index=*/0, /*hint=*/RDMA);
    EXPECT_EQ(r.transport, UNSPEC);
}

// RFC #2519 / #2568 step 1: a policy's link-layer QoS (service_level /
// traffic_class / qp_pool) is parsed from JSON and carried out via
// SelectionResult. (Step 1 only plumbs the values; applying them at QP setup
// is the per-class QP pool follow-up.)
TEST(TransportSelectorTest, PolicyLinkLayerQoSIsParsedAndCarried) {
    auto conf = std::make_shared<Config>();
    json policy;
    policy["name"] = "kv-critical";
    policy["segment_type"] = "memory";
    policy["transports"] = {"rdma"};
    policy["service_level"] = 3;
    policy["traffic_class"] = 96;
    policy["qp_pool"] = "kv";
    conf->set("policy", json::array({policy}));

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);

    std::vector<TransportType> buffer_transports = {RDMA};
    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;
    ctx.policy_name = "kv-critical";

    auto r = selector.select(ctx, transports, /*index=*/0);
    ASSERT_TRUE(r.service_level.has_value());
    EXPECT_EQ(r.service_level.value(), 3);
    ASSERT_TRUE(r.traffic_class.has_value());
    EXPECT_EQ(r.traffic_class.value(), 96);
    ASSERT_TRUE(r.qp_pool.has_value());
    EXPECT_EQ(r.qp_pool.value(), "kv");
}

// Out-of-range SL/TC are ignored (left as nullopt) so a bad config never
// changes selection behavior.
TEST(TransportSelectorTest, PolicyLinkLayerQoSOutOfRangeIgnored) {
    auto conf = std::make_shared<Config>();
    json policy;
    policy["name"] = "bad-qos";
    policy["segment_type"] = "memory";
    policy["transports"] = {"rdma"};
    policy["service_level"] = 99;    // > 15, invalid
    policy["traffic_class"] = 9999;  // > 255, invalid
    conf->set("policy", json::array({policy}));

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);

    std::vector<TransportType> buffer_transports = {RDMA};
    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;
    ctx.policy_name = "bad-qos";

    auto r = selector.select(ctx, transports, /*index=*/0);
    EXPECT_FALSE(r.service_level.has_value());
    EXPECT_FALSE(r.traffic_class.has_value());
}

// An empty-string or non-string qp_pool is treated as unset (nullopt), so a
// blank / mistyped config value never looks like an explicit pool.
TEST(TransportSelectorTest, PolicyQpPoolEmptyOrNonStringIsUnset) {
    auto conf = std::make_shared<Config>();
    json empty_pool;
    empty_pool["name"] = "empty-pool";
    empty_pool["segment_type"] = "memory";
    empty_pool["transports"] = {"rdma"};
    empty_pool["qp_pool"] = "";  // empty -> unset
    json bad_pool;
    bad_pool["name"] = "bad-pool";
    bad_pool["segment_type"] = "memory";
    bad_pool["transports"] = {"rdma"};
    bad_pool["qp_pool"] = 42;  // non-string -> ignored
    conf->set("policy", json::array({empty_pool, bad_pool}));

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);
    std::vector<TransportType> buffer_transports = {RDMA};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.buffer_transports = &buffer_transports;

    ctx.policy_name = "empty-pool";
    EXPECT_FALSE(
        selector.select(ctx, transports, /*index=*/0).qp_pool.has_value());
    ctx.policy_name = "bad-pool";
    EXPECT_FALSE(
        selector.select(ctx, transports, /*index=*/0).qp_pool.has_value());
}

// Intent-specific policies bind Request::intent_type to transport and
// link-layer QoS selection. Policies are first-match, so the specific entry is
// deliberately placed before the catch-all fallback.
TEST(TransportSelectorTest, IntentSpecificPolicyIsSelected) {
    auto conf = std::make_shared<Config>();
    json foreground;
    foreground["name"] = "foreground";
    foreground["segment_type"] = "memory";
    foreground["intent_type"] = "foreground_get";
    foreground["transports"] = {"rdma"};
    foreground["service_level"] = 3;
    foreground["traffic_class"] = 96;
    foreground["qp_pool"] = "foreground";
    json fallback;
    fallback["name"] = "fallback";
    fallback["segment_type"] = "memory";
    fallback["transports"] = {"tcp"};
    conf->set("policy", json::array({foreground, fallback}));

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    transports[TCP] = std::make_shared<FakeTransport>(TCP);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);
    static_cast<FakeTransport*>(transports[TCP].get())->setDramToDram(true);
    std::vector<TransportType> buffer_transports = {RDMA, TCP};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = PRIO_HIGH;
    ctx.buffer_transports = &buffer_transports;
    ctx.intent_type = IntentType::FOREGROUND_GET;

    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, RDMA);
    EXPECT_EQ(result.service_level, 3);
    EXPECT_EQ(result.traffic_class, 96);
    EXPECT_EQ(result.qp_pool, "foreground");
}

TEST(TransportSelectorTest, IntentMismatchFallsThroughToCatchAll) {
    auto conf = std::make_shared<Config>();
    json foreground;
    foreground["name"] = "foreground";
    foreground["segment_type"] = "memory";
    foreground["intent_type"] = "foreground_get";
    foreground["transports"] = {"rdma"};
    json fallback;
    fallback["name"] = "fallback";
    fallback["segment_type"] = "memory";
    fallback["transports"] = {"tcp"};
    conf->set("policy", json::array({foreground, fallback}));

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    transports[TCP] = std::make_shared<FakeTransport>(TCP);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);
    static_cast<FakeTransport*>(transports[TCP].get())->setDramToDram(true);
    std::vector<TransportType> buffer_transports = {RDMA, TCP};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = PRIO_LOW;
    ctx.buffer_transports = &buffer_transports;
    ctx.intent_type = IntentType::CHECKPOINT;

    EXPECT_EQ(selector.select(ctx, transports).transport, TCP);
}

TEST(TransportSelectorTest, PolicyWithoutIntentMatchesAnyIntent) {
    auto conf = std::make_shared<Config>();
    json policy;
    policy["name"] = "legacy";
    policy["segment_type"] = "memory";
    policy["transports"] = {"rdma"};
    conf->set("policy", json::array({policy}));

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);
    std::vector<TransportType> buffer_transports = {RDMA};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = PRIO_LOW;
    ctx.buffer_transports = &buffer_transports;
    ctx.intent_type = IntentType::CHECKPOINT;

    EXPECT_EQ(selector.select(ctx, transports).transport, RDMA);
}

TEST(TransportSelectorTest, NumericIntentValueIsAccepted) {
    auto conf = std::make_shared<Config>();
    json policy;
    policy["name"] = "checkpoint";
    policy["segment_type"] = "memory";
    policy["intent_type"] = static_cast<int>(IntentType::CHECKPOINT);
    policy["transports"] = {"tcp"};
    conf->set("policy", json::array({policy}));

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[TCP] = std::make_shared<FakeTransport>(TCP);
    static_cast<FakeTransport*>(transports[TCP].get())->setDramToDram(true);
    std::vector<TransportType> buffer_transports = {TCP};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = PRIO_LOW;
    ctx.buffer_transports = &buffer_transports;
    ctx.intent_type = IntentType::CHECKPOINT;

    EXPECT_EQ(selector.select(ctx, transports).transport, TCP);
}

TEST(TransportSelectorTest, InvalidIntentPolicyIsSkipped) {
    auto conf = std::make_shared<Config>();
    json bad_name;
    bad_name["name"] = "bad-name";
    bad_name["segment_type"] = "memory";
    bad_name["intent_type"] = "not_an_intent";
    bad_name["transports"] = {"rdma"};
    json bad_number;
    bad_number["name"] = "bad-number";
    bad_number["segment_type"] = "memory";
    bad_number["intent_type"] = 999;
    bad_number["transports"] = {"rdma"};
    json bad_type;
    bad_type["name"] = "bad-type";
    bad_type["segment_type"] = "memory";
    bad_type["intent_type"] = true;
    bad_type["transports"] = {"rdma"};
    json bad_unsigned;
    bad_unsigned["name"] = "bad-unsigned";
    bad_unsigned["segment_type"] = "memory";
    bad_unsigned["intent_type"] = std::numeric_limits<uint64_t>::max();
    bad_unsigned["transports"] = {"rdma"};
    json fallback;
    fallback["name"] = "fallback";
    fallback["segment_type"] = "memory";
    fallback["transports"] = {"tcp"};
    conf->set("policy", json::array({bad_name, bad_number, bad_type,
                                     bad_unsigned, fallback}));

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[RDMA] = std::make_shared<FakeTransport>(RDMA);
    transports[TCP] = std::make_shared<FakeTransport>(TCP);
    static_cast<FakeTransport*>(transports[RDMA].get())->setDramToDram(true);
    static_cast<FakeTransport*>(transports[TCP].get())->setDramToDram(true);
    std::vector<TransportType> buffer_transports = {RDMA, TCP};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = PRIO_HIGH;
    ctx.buffer_transports = &buffer_transports;
    ctx.intent_type = IntentType::FOREGROUND_GET;

    EXPECT_EQ(selector.select(ctx, transports).transport, TCP);
}

TEST(TransportSelectorTest, ExplicitPolicyNameOverridesIntentFilter) {
    auto conf = std::make_shared<Config>();
    json policy;
    policy["name"] = "operator-override";
    policy["segment_type"] = "memory";
    policy["intent_type"] = "checkpoint";
    policy["transports"] = {"tcp"};
    conf->set("policy", json::array({policy}));

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[TCP] = std::make_shared<FakeTransport>(TCP);
    static_cast<FakeTransport*>(transports[TCP].get())->setDramToDram(true);
    std::vector<TransportType> buffer_transports = {TCP};

    SelectionContext ctx;
    ctx.segment_type = SegmentType::Memory;
    ctx.same_machine = false;
    ctx.local_memory_type = MTYPE_CPU;
    ctx.remote_memory_type = MTYPE_CPU;
    ctx.transfer_size = 4096;
    ctx.priority_level = PRIO_HIGH;
    ctx.buffer_transports = &buffer_transports;
    ctx.intent_type = IntentType::FOREGROUND_GET;
    ctx.policy_name = "operator-override";

    EXPECT_EQ(selector.select(ctx, transports).transport, TCP);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
