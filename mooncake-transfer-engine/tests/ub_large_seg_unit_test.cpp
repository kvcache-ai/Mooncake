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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "config.h"
#include "topology.h"
#include "transfer_metadata.h"
#include "transport/kunpeng_transport/ub_context.h"
#include "transport/kunpeng_transport/ub_transport.h"
#include "transport/kunpeng_transport/urma/urma_endpoint.h"

namespace mooncake {

class UbTransportTestPeer {
   public:
    static void bindMetadata(UbTransport& transport,
                             std::shared_ptr<TransferMetadata> metadata,
                             std::string local_server_name) {
        transport.metadata_ = std::move(metadata);
        transport.local_server_name_ = std::move(local_server_name);
    }

    static void bindTopology(UbTransport& transport,
                             std::shared_ptr<Topology> topology) {
        transport.local_topology_ = std::move(topology);
    }

    static void addContext(UbTransport& transport,
                           std::shared_ptr<UbContext> context) {
        transport.context_list_.push_back(std::move(context));
    }

    static int allocateLocalSegmentID(UbTransport& transport) {
        return transport.allocateLocalSegmentID();
    }

    static int registerLocalMemory(UbTransport& transport, void* addr,
                                   size_t length, const std::string& location,
                                   bool remote_accessible = true,
                                   bool update_metadata = true) {
        return transport.registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata);
    }

    static int unregisterLocalMemory(UbTransport& transport, void* addr,
                                     bool update_metadata = true) {
        return transport.unregisterLocalMemory(addr, update_metadata);
    }

    static size_t chunkMapSize(UbTransport& transport) {
        std::lock_guard<std::mutex> lock(transport.chunk_map_mutex_);
        return transport.chunk_map_.size();
    }
};

}  // namespace mooncake

using namespace mooncake;

namespace {

using SegmentDesc = TransferMetadata::SegmentDesc;
using BufferDesc = TransferMetadata::BufferDesc;

class ScopedUbConfig {
   public:
    ScopedUbConfig(uint64_t max_seg_size, size_t slice_size,
                   size_t fragment_limit)
        : saved_(globalConfig()) {
        globalConfig().max_seg_size = max_seg_size;
        globalConfig().slice_size = slice_size;
        globalConfig().fragment_limit = fragment_limit;
    }

    ~ScopedUbConfig() { globalConfig() = saved_; }

   private:
    GlobalConfig saved_;
};

class FakeUbContext final : public UbContext {
   public:
    struct PostedSlice {
        void* source_addr;
        uint64_t dest_addr;
        size_t length;
        Transport::TransferRequest::OpCode opcode;
        void* l_seg;
    };

    FakeUbContext(UbTransport& engine, const std::string& device_name,
                  uint64_t seg_tag_base)
        : UbContext(engine, device_name, 16), seg_tag_base_(seg_tag_base) {}

    int registerMemoryRegion(uint64_t va, size_t length) override {
        const int call_idx = register_call_count++;
        register_calls.emplace_back(va, length);
        if (fail_on_register_call >= 0 && call_idx == fail_on_register_call) {
            return ERR_CONTEXT;
        }
        const uint32_t idx = static_cast<uint32_t>(local_segs_.size());
        void* token = reinterpret_cast<void*>(seg_tag_base_ + (idx + 1) * 0x10);
        local_segs_.push_back(token);
        live_regions_[va] = {va, length, idx, token};
        return 0;
    }

    int unregisterMemoryRegion(uint64_t va) override {
        unregister_calls.push_back(va);
        for (auto it = live_regions_.begin(); it != live_regions_.end(); ++it) {
            if (it->second.va <= va && va < it->second.va + it->second.length) {
                local_segs_[it->second.index] = nullptr;
                live_regions_.erase(it);
                return 0;
            }
        }
        return 0;
    }

    int buildLocalBufferDesc(uint64_t addr,
                             UbTransport::BufferDesc& buffer_desc) override {
        const int call_idx = build_desc_call_count++;
        if (fail_on_build_desc_call >= 0 &&
            call_idx == fail_on_build_desc_call) {
            return ERR_CONTEXT;
        }
        for (const auto& [_, region] : live_regions_) {
            if (region.va <= addr && addr < region.va + region.length) {
                buffer_desc.tseg.push_back(deviceName() + ":" +
                                           std::to_string(region.va));
                buffer_desc.l_seg_index.push_back(region.index);
                return 0;
            }
        }
        return ERR_CONTEXT;
    }

    void* localSegWithIndex(unsigned value) override {
        return local_segs_.at(value);
    }

    std::shared_ptr<UbEndPoint> makeEndpoint() override { return nullptr; }
    int doProcessContextEvents() override { return 0; }
    void* retrieveRemoteSeg(const std::string&) override { return nullptr; }
    int openDevice(const std::string&, uint8_t, int&) override { return 0; }
    int poll(int, Transport::Slice**, int& num_failed,
             std::unordered_map<volatile int*, int>&, int) override {
        num_failed = 0;
        return 0;
    }
    volatile int* outstandingCount(int) override { return &outstanding_; }
    int jfcCount() override { return 1; }
    int getAsyncFd() override { return -1; }
    std::string getEid() override {
        return "00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:01";
    }
    std::string toString() override {
        return "FakeUbContext(" + deviceName() + ")";
    }

    int submitPostSend(
        const std::vector<Transport::Slice*>& slice_list) override {
        for (auto* slice : slice_list) {
            posted_slices.push_back({slice->source_addr, slice->ub.dest_addr,
                                     slice->length, slice->opcode,
                                     slice->ub.l_seg});
            slice->markSuccess();
        }
        return 0;
    }

    struct Region {
        uint64_t va;
        size_t length;
        uint32_t index;
        void* seg;
    };

    int register_call_count = 0;
    int build_desc_call_count = 0;
    int fail_on_register_call = -1;
    int fail_on_build_desc_call = -1;
    std::vector<std::pair<uint64_t, size_t>> register_calls;
    std::vector<uint64_t> unregister_calls;
    std::unordered_map<uint64_t, Region> live_regions_;
    std::vector<void*> local_segs_;
    std::vector<PostedSlice> posted_slices;

   private:
    int construct(GlobalConfig&) override { return 0; }
    int deconstruct() override { return 0; }

    uint64_t seg_tag_base_;
    int outstanding_ = 0;
};

class UbLargeSegUnitTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kMaxSegSize = 4096;
    static constexpr size_t kSliceSize = 1024;
    static constexpr size_t kFragmentLimit = 256;

    std::unique_ptr<ScopedUbConfig> config_guard_;
    std::shared_ptr<TransferMetadata> metadata_;
    std::shared_ptr<Topology> topology_;
    std::unique_ptr<UbTransport> transport_;
    std::vector<std::shared_ptr<FakeUbContext>> contexts_;

    void SetUp() override {
        config_guard_ = std::make_unique<ScopedUbConfig>(
            kMaxSegSize, kSliceSize, kFragmentLimit);
        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        topology_ = std::make_shared<Topology>();
        ASSERT_EQ(topology_->parse(R"({"cpu:0": [["ub_0", "ub_1"], []]})"), 0);

        transport_ = std::make_unique<UbTransport>();
        UbTransportTestPeer::bindMetadata(*transport_, metadata_,
                                          "ub-unit-test:1234");
        UbTransportTestPeer::bindTopology(*transport_, topology_);

        const auto& hcas = topology_->getHcaList();
        for (size_t i = 0; i < hcas.size(); ++i) {
            auto ctx = std::make_shared<FakeUbContext>(*transport_, hcas[i],
                                                       0x100000ULL * (i + 1));
            contexts_.push_back(ctx);
            UbTransportTestPeer::addContext(*transport_, std::move(ctx));
        }
        ASSERT_EQ(UbTransportTestPeer::allocateLocalSegmentID(*transport_), 0);
    }

    void addRemoteSegment(
        Transport::SegmentID target_id, const std::string& name,
        const std::vector<std::pair<uint64_t, size_t>>& chunks) {
        auto desc = std::make_shared<SegmentDesc>();
        desc->name = name;
        desc->protocol = "ub";
        desc->topology = *topology_;
        for (const auto& ctx : contexts_) {
            TransferMetadata::DeviceDesc dev;
            dev.name = ctx->deviceName();
            dev.eid = ctx->getEid();
            desc->devices.push_back(dev);
        }
        for (const auto& [addr, len] : chunks) {
            BufferDesc buf;
            buf.name = "cpu:0";
            buf.addr = addr;
            buf.length = len;
            for (const auto& ctx : contexts_) {
                buf.tseg.push_back("remote:" + ctx->deviceName() + ":" +
                                   std::to_string(addr));
            }
            desc->buffers.push_back(std::move(buf));
        }
        desc->rebuildBufferRangeIndex();
        metadata_->addLocalSegment(target_id, name, std::move(desc));
    }

    std::vector<FakeUbContext::PostedSlice> allPostedSlices() const {
        std::vector<FakeUbContext::PostedSlice> result;
        for (const auto& ctx : contexts_) {
            result.insert(result.end(), ctx->posted_slices.begin(),
                          ctx->posted_slices.end());
        }
        return result;
    }
};

TEST_F(UbLargeSegUnitTest, UrmaContextRejectsOversizedSegment) {
    UrmaContext urma_ctx(*transport_, "mock_urma_device", 1);
    EXPECT_EQ(urma_ctx.registerMemoryRegion(0x10000, kMaxSegSize + 1),
              ERR_INVALID_ARGUMENT);
    UbTransport::BufferDesc buf;
    EXPECT_EQ(urma_ctx.buildLocalBufferDesc(0x10000, buf), ERR_CONTEXT);
}

TEST_F(UbLargeSegUnitTest, RegisterExactMaxSegSizeStaysSingleChunk) {
    void* base = reinterpret_cast<void*>(0x100000);
    ASSERT_EQ(UbTransportTestPeer::registerLocalMemory(*transport_, base,
                                                       kMaxSegSize, "cpu:0"),
              0);

    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(desc, nullptr);
    ASSERT_EQ(desc->buffers.size(), 1u);
    EXPECT_EQ(desc->buffers[0].addr, 0x100000u);
    EXPECT_EQ(desc->buffers[0].length, kMaxSegSize);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);

    for (const auto& ctx : contexts_) {
        ASSERT_EQ(ctx->register_calls.size(), 1u);
        EXPECT_EQ(ctx->live_regions_.size(), 1u);
    }

    ASSERT_EQ(UbTransportTestPeer::unregisterLocalMemory(*transport_, base), 0);
    desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    EXPECT_TRUE(desc->buffers.empty());
    for (const auto& ctx : contexts_) {
        EXPECT_TRUE(ctx->live_regions_.empty());
    }
}

TEST_F(UbLargeSegUnitTest, RegisterAutoChunksAndUnregistersAllChunks) {
    void* base = reinterpret_cast<void*>(0x200000);
    const size_t total_len = kMaxSegSize * 3 + 512;  // 4 chunks
    ASSERT_EQ(UbTransportTestPeer::registerLocalMemory(*transport_, base,
                                                       total_len, "cpu:0"),
              0);

    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(desc, nullptr);
    ASSERT_EQ(desc->buffers.size(), 4u);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 1u);

    const std::vector<size_t> expected_lens = {kMaxSegSize, kMaxSegSize,
                                               kMaxSegSize, 512};
    for (size_t i = 0; i < 4; ++i) {
        const uint64_t expected_addr = 0x200000u + i * kMaxSegSize;
        EXPECT_EQ(desc->buffers[i].name, "cpu:0");
        EXPECT_EQ(desc->buffers[i].addr, expected_addr);
        EXPECT_EQ(desc->buffers[i].length, expected_lens[i]);
        EXPECT_EQ(desc->buffers[i].tseg.size(), contexts_.size());
        EXPECT_EQ(desc->buffers[i].l_seg_index.size(), contexts_.size());
        for (size_t ci = 0; ci < contexts_.size(); ++ci) {
            EXPECT_EQ(desc->buffers[i].l_seg_index[ci], i);
        }
    }

    for (const auto& ctx : contexts_) {
        ASSERT_EQ(ctx->register_calls.size(), 4u);
        EXPECT_EQ(ctx->live_regions_.size(), 4u);
    }

    ASSERT_EQ(UbTransportTestPeer::unregisterLocalMemory(*transport_, base), 0);
    desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    EXPECT_TRUE(desc->buffers.empty());
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
    for (const auto& ctx : contexts_) {
        EXPECT_TRUE(ctx->live_regions_.empty());
        EXPECT_EQ(ctx->unregister_calls.size(), 4u);
    }
}

TEST_F(UbLargeSegUnitTest,
       RegisterRollbackOnPartialContextAndLaterChunkFailure) {
    void* base = reinterpret_cast<void*>(0x300000);
    const size_t total_len = kMaxSegSize * 3 + 128;

    // Fail on context 1 during chunk 0 registration.
    contexts_[1]->fail_on_register_call = 0;
    EXPECT_NE(UbTransportTestPeer::registerLocalMemory(*transport_, base,
                                                       total_len, "cpu:0"),
              0);
    EXPECT_TRUE(
        metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
    for (const auto& ctx : contexts_) {
        EXPECT_TRUE(ctx->live_regions_.empty());
    }

    // Fail on context 1 during chunk 2 (chunks 0 and 1 already committed).
    contexts_[1]->fail_on_register_call = -1;
    contexts_[1]->register_call_count = 0;
    contexts_[1]->fail_on_register_call = 2;
    EXPECT_NE(UbTransportTestPeer::registerLocalMemory(*transport_, base,
                                                       total_len, "cpu:0"),
              0);
    EXPECT_TRUE(
        metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
    for (const auto& ctx : contexts_) {
        EXPECT_TRUE(ctx->live_regions_.empty());
    }

    // Fail buildLocalBufferDesc on chunk 1 on context 0 after registering MR.
    contexts_[1]->fail_on_register_call = -1;
    contexts_[0]->build_desc_call_count = 0;
    contexts_[0]->fail_on_build_desc_call = 1;
    EXPECT_NE(UbTransportTestPeer::registerLocalMemory(*transport_, base,
                                                       total_len, "cpu:0"),
              0);
    EXPECT_TRUE(
        metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
    for (const auto& ctx : contexts_) {
        EXPECT_TRUE(ctx->live_regions_.empty());
    }
}

TEST_F(UbLargeSegUnitTest, SubmitSplitsTargetStraddlingChunkBoundary) {
    // Source sits inside one chunk; remote target is split into two chunks.
    void* local_base = reinterpret_cast<void*>(0x400000);
    ASSERT_EQ(UbTransportTestPeer::registerLocalMemory(*transport_, local_base,
                                                       kMaxSegSize, "cpu:0"),
              0);

    const Transport::SegmentID kRemoteSegId = 42;
    const uint64_t kRemoteBase = 0x800000;
    addRemoteSegment(
        kRemoteSegId, "remote-node:1234",
        {{kRemoteBase, kMaxSegSize}, {kRemoteBase + kMaxSegSize, kMaxSegSize}});

    // Small request (length <= slice_size + fragment_limit) that would trigger
    // merge_final_slice, straddling the remote chunk boundary.
    const size_t req_len = 512;
    const uint64_t target_offset = kRemoteBase + kMaxSegSize - 200;
    auto batch_id = transport_->allocateBatchID(1);
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = local_base;
    req.target_id = kRemoteSegId;
    req.target_offset = target_offset;
    req.length = req_len;

    ASSERT_EQ(transport_->submitTransfer(batch_id, {req}), Status::OK());
    TransferStatus status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, 0, status), Status::OK());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, req_len);
    ASSERT_EQ(transport_->freeBatchID(batch_id), Status::OK());

    const auto slices = allPostedSlices();
    ASSERT_EQ(slices.size(), 2u);
    EXPECT_EQ(slices[0].source_addr, local_base);
    EXPECT_EQ(slices[0].dest_addr, target_offset);
    EXPECT_EQ(slices[0].length, 200u);
    EXPECT_EQ(slices[1].source_addr, static_cast<char*>(local_base) + 200);
    EXPECT_EQ(slices[1].dest_addr, kRemoteBase + kMaxSegSize);
    EXPECT_EQ(slices[1].length, 312u);
}

TEST_F(UbLargeSegUnitTest, SubmitSplitsSourceStraddlingChunkBoundary) {
    void* local_base = reinterpret_cast<void*>(0x500000);
    ASSERT_EQ(UbTransportTestPeer::registerLocalMemory(
                  *transport_, local_base, kMaxSegSize * 2, "cpu:0"),
              0);

    const Transport::SegmentID kRemoteSegId = 43;
    const uint64_t kRemoteBase = 0x900000;
    addRemoteSegment(kRemoteSegId, "remote-node:2345",
                     {{kRemoteBase, kMaxSegSize * 2}});

    const size_t req_len = 1500;
    char* source_ptr = static_cast<char*>(local_base) + kMaxSegSize - 600;
    auto batch_id = transport_->allocateBatchID(1);
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = source_ptr;
    req.target_id = kRemoteSegId;
    req.target_offset = kRemoteBase;
    req.length = req_len;

    ASSERT_EQ(transport_->submitTransfer(batch_id, {req}), Status::OK());
    TransferStatus status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, 0, status), Status::OK());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, req_len);
    ASSERT_EQ(transport_->freeBatchID(batch_id), Status::OK());

    // Chunk 0 remaining is 600; chunk 1 remaining is 900 <= slice_size (1024).
    const auto slices = allPostedSlices();
    ASSERT_EQ(slices.size(), 2u);
    EXPECT_EQ(slices[0].source_addr, source_ptr);
    EXPECT_EQ(slices[0].length, 600u);
    EXPECT_EQ(slices[1].source_addr, source_ptr + 600);
    EXPECT_EQ(slices[1].length, 900u);
    EXPECT_NE(slices[0].l_seg, slices[1].l_seg);
}

TEST_F(UbLargeSegUnitTest, SubmitSplitsReadStraddlingBothSides) {
    void* local_base = reinterpret_cast<void*>(0x600000);
    ASSERT_EQ(UbTransportTestPeer::registerLocalMemory(
                  *transport_, local_base, kMaxSegSize * 3, "cpu:0"),
              0);

    const Transport::SegmentID kRemoteSegId = 44;
    const uint64_t kRemoteBase = 0xA00000;
    addRemoteSegment(kRemoteSegId, "remote-node:3456",
                     {{kRemoteBase, kMaxSegSize},
                      {kRemoteBase + kMaxSegSize, kMaxSegSize},
                      {kRemoteBase + kMaxSegSize * 2, kMaxSegSize}});

    // Local starts 300 bytes before chunk 0->1 seam;
    // Remote starts 700 bytes before chunk 1->2 seam; total length 1000.
    // Cuts must happen at offset 300 (local seam) and offset 700 (remote seam),
    // yielding 3 slices: 300, 400, 300.
    char* local_ptr = static_cast<char*>(local_base) + kMaxSegSize - 300;
    const uint64_t remote_ptr = kRemoteBase + kMaxSegSize * 2 - 700;
    const size_t req_len = 1000;

    auto batch_id = transport_->allocateBatchID(1);
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::READ;
    req.source = local_ptr;
    req.target_id = kRemoteSegId;
    req.target_offset = remote_ptr;
    req.length = req_len;

    ASSERT_EQ(transport_->submitTransfer(batch_id, {req}), Status::OK());
    TransferStatus status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, 0, status), Status::OK());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, req_len);
    ASSERT_EQ(transport_->freeBatchID(batch_id), Status::OK());

    const auto slices = allPostedSlices();
    ASSERT_EQ(slices.size(), 3u);
    EXPECT_EQ(slices[0].opcode, Transport::TransferRequest::READ);
    EXPECT_EQ(slices[0].length, 300u);
    EXPECT_EQ(slices[0].source_addr, local_ptr);
    EXPECT_EQ(slices[0].dest_addr, remote_ptr);

    EXPECT_EQ(slices[1].length, 400u);
    EXPECT_EQ(slices[1].source_addr, local_ptr + 300);
    EXPECT_EQ(slices[1].dest_addr, remote_ptr + 300);

    EXPECT_EQ(slices[2].length, 300u);
    EXPECT_EQ(slices[2].source_addr, local_ptr + 700);
    EXPECT_EQ(slices[2].dest_addr, remote_ptr + 700);

    EXPECT_NE(slices[0].l_seg, slices[1].l_seg);
    EXPECT_EQ(slices[1].l_seg, slices[2].l_seg);
}

TEST_F(UbLargeSegUnitTest,
       SubmitInactivePrimaryContextFallsBackToActiveContext) {
    void* local_base = reinterpret_cast<void*>(0x700000);
    ASSERT_EQ(UbTransportTestPeer::registerLocalMemory(*transport_, local_base,
                                                       kMaxSegSize, "cpu:0"),
              0);

    contexts_[0]->set_active(false);

    auto batch_id = transport_->allocateBatchID(1);
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = local_base;
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(local_base);
    req.length = 512;

    ASSERT_EQ(transport_->submitTransfer(batch_id, {req}), Status::OK());
    TransferStatus status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, 0, status), Status::OK());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    ASSERT_EQ(transport_->freeBatchID(batch_id), Status::OK());

    EXPECT_TRUE(contexts_[0]->posted_slices.empty());
    ASSERT_EQ(contexts_[1]->posted_slices.size(), 1u);
}

TEST_F(UbLargeSegUnitTest, SubmitFailureMarksQueuedUnpostedSlicesFailed) {
    void* local_base = reinterpret_cast<void*>(0x800000);
    ASSERT_EQ(UbTransportTestPeer::registerLocalMemory(*transport_, local_base,
                                                       kMaxSegSize, "cpu:0"),
              0);

    // First slice [local_base + kMaxSegSize - 256, local_base + kMaxSegSize) is
    // registered; the trailing 256 bytes fall outside any registered chunk.
    auto batch_id = transport_->allocateBatchID(1);
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = static_cast<char*>(local_base) + kMaxSegSize - 256;
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(local_base);
    req.length = 512;

    Status s = transport_->submitTransfer(batch_id, {req});
    EXPECT_TRUE(s.IsAddressNotRegistered());

    auto& batch_desc = Transport::toBatchDesc(batch_id);
    ASSERT_EQ(batch_desc.task_list.size(), 1u);
    ASSERT_EQ(batch_desc.task_list[0].slice_list.size(), 1u);
    EXPECT_EQ(batch_desc.task_list[0].slice_list[0]->status,
              Transport::Slice::FAILED);
    EXPECT_EQ(batch_desc.task_list[0].failed_slice_count, 1u);

    TransferStatus status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, 0, status), Status::OK());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(transport_->freeBatchID(batch_id), Status::OK());
}

}  // namespace
