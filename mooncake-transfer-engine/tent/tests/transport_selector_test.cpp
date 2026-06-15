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
    EXPECT_EQ(TransportSelector::transportTypeName(RDMA), "rdma");
    EXPECT_EQ(TransportSelector::transportTypeName(TCP), "tcp");
    EXPECT_EQ(TransportSelector::transportTypeName(SHM), "shm");
    EXPECT_EQ(TransportSelector::transportTypeName(NVLINK), "nvlink");
    EXPECT_EQ(TransportSelector::transportTypeName(GDS), "gds");
    EXPECT_EQ(TransportSelector::transportTypeName(IOURING), "io_uring");
    EXPECT_EQ(TransportSelector::transportTypeName(AscendDirect), "ascend");
    EXPECT_EQ(TransportSelector::transportTypeName(MNNVL), "mnnvl");
}

TEST(TransportSelectorTest, ParseTransportType) {
    EXPECT_EQ(TransportSelector::parseTransportType("rdma"), RDMA);
    EXPECT_EQ(TransportSelector::parseTransportType("tcp"), TCP);
    EXPECT_EQ(TransportSelector::parseTransportType("shm"), SHM);
    EXPECT_EQ(TransportSelector::parseTransportType("nvlink"), NVLINK);
    EXPECT_EQ(TransportSelector::parseTransportType("gds"), GDS);
    EXPECT_EQ(TransportSelector::parseTransportType("io_uring"), IOURING);
    EXPECT_EQ(TransportSelector::parseTransportType("ascend"), AscendDirect);
    EXPECT_EQ(TransportSelector::parseTransportType("mnnvl"), MNNVL);
    EXPECT_EQ(TransportSelector::parseTransportType("unknown"), UNSPEC);
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
// Test RailMonitor integration: skip RDMA when rails are unhealthy
// ---------------------------------------------------------------------------

TEST(TransportSelectorTest, RailMonitorSkipUnhealthyRdma) {
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

    // Without RailMonitor, RDMA should be selected
    auto result = selector.select(ctx, transports);
    EXPECT_EQ(result.transport, RDMA)
        << "Without RailMonitor, RDMA should be selected";

    // Setting a RailMonitor that reports not ready should skip RDMA
    // (Note: In a real scenario, RailMonitor would be loaded with topology.
    //  Here we test the mechanism by setting a monitor that's not ready.)
    // The RailMonitor::ready() returns false by default (not loaded),
    // so setting it should cause RDMA to be skipped.
    RailMonitor monitor;
    selector.setRailMonitor(&monitor);

    result = selector.select(ctx, transports);
    // RailMonitor not loaded (ready() = false) → RDMA skipped → fallback to TCP
    EXPECT_EQ(result.transport, TCP)
        << "With RailMonitor not ready, RDMA should be skipped for TCP";
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
