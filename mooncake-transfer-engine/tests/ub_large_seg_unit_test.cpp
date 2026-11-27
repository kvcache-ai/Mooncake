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
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common.h"
#include "config.h"
#include "memory_location.h"
#include "topology.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
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
using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;

constexpr char kLocalServerName[] = "ub-unit-test:1234";

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

// In-memory MetadataStoragePlugin whose set() can be armed to fail. P2P
// handshake metadata never fails updateLocalSegmentDesc(), so the publish
// failure paths need this seam.
class FakeMetadataStoragePlugin : public MetadataStoragePlugin {
   public:
    bool get(const std::string& key, Json::Value& value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = store_.find(key);
        if (it == store_.end()) return false;
        value = it->second;
        return true;
    }

    bool set(const std::string& key, const Json::Value& value) override {
        int remaining = fail_sets_remaining_.load();
        while (remaining > 0 && !fail_sets_remaining_.compare_exchange_weak(
                                    remaining, remaining - 1)) {
        }
        if (remaining > 0) return false;
        std::lock_guard<std::mutex> lock(mutex_);
        store_[key] = value;
        return true;
    }

    bool remove(const std::string& key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        store_.erase(key);
        return true;
    }

    void failNextSet(int n) { fail_sets_remaining_ = n; }

   private:
    std::mutex mutex_;
    std::map<std::string, Json::Value> store_;
    std::atomic<int> fail_sets_remaining_{0};
};

// Exposes the protected storage-plugin injection seam.
class TestableTransferMetadata : public TransferMetadata {
   public:
    explicit TestableTransferMetadata(
        std::shared_ptr<MetadataStoragePlugin> storage_plugin)
        : TransferMetadata(std::move(storage_plugin)) {}
};

class FakeUbContext final : public UbContext {
   public:
    struct PostedSlice {
        void* source_addr;
        uint64_t dest_addr;
        size_t length;
        TransferRequest::OpCode opcode;
        // Start address of the local chunk that owns the slice's l_seg. l_seg
        // tokens are per-context, so tests compare chunks through this field.
        uint64_t local_chunk_va;
    };

    FakeUbContext(UbTransport& engine, const std::string& device_name,
                  uint64_t seg_tag_base)
        : UbContext(engine, device_name, 16), seg_tag_base_(seg_tag_base) {}

    int registerMemoryRegion(uint64_t va, size_t length) override {
        std::lock_guard<std::mutex> lock(mu_);
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
        std::lock_guard<std::mutex> lock(mu_);
        const int call_idx = unregister_call_count++;
        unregister_calls.push_back(va);
        if (fail_on_unregister_call >= 0 &&
            call_idx == fail_on_unregister_call) {
            return ERR_CONTEXT;
        }
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
        std::lock_guard<std::mutex> lock(mu_);
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
        std::lock_guard<std::mutex> lock(mu_);
        if (value >= local_segs_.size()) {
            return nullptr;
        }
        return local_segs_[value];
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
        std::lock_guard<std::mutex> lock(mu_);
        for (auto* slice : slice_list) {
            uint64_t chunk_va = 0;
            for (const auto& [_, region] : live_regions_) {
                if (region.seg == slice->ub.l_seg) chunk_va = region.va;
            }
            posted_slices.push_back({slice->source_addr, slice->ub.dest_addr,
                                     slice->length, slice->opcode, chunk_va});
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

    std::mutex mu_;
    int register_call_count = 0;
    int unregister_call_count = 0;
    int build_desc_call_count = 0;
    int fail_on_register_call = -1;
    int fail_on_unregister_call = -1;
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

TransferRequest makeRequest(TransferRequest::OpCode opcode, void* source,
                            Transport::SegmentID target_id,
                            uint64_t target_offset, size_t length) {
    TransferRequest req;
    req.opcode = opcode;
    req.source = source;
    req.target_id = target_id;
    req.target_offset = target_offset;
    req.length = length;
    return req;
}

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
        topology_ = std::make_shared<Topology>();
        ASSERT_EQ(topology_->parse(R"({"cpu:0": [["ub_0", "ub_1"], []]})"), 0);

        transport_ = std::make_unique<UbTransport>();
        UbTransportTestPeer::bindTopology(*transport_, topology_);
        const auto& hcas = topology_->getHcaList();
        for (size_t i = 0; i < hcas.size(); ++i) {
            auto ctx = std::make_shared<FakeUbContext>(*transport_, hcas[i],
                                                       0x100000ULL * (i + 1));
            contexts_.push_back(ctx);
            UbTransportTestPeer::addContext(*transport_, std::move(ctx));
        }
        bindMetadata(std::make_shared<TransferMetadata>(P2PHANDSHAKE));
    }

    void bindMetadata(std::shared_ptr<TransferMetadata> metadata) {
        metadata_ = std::move(metadata);
        UbTransportTestPeer::bindMetadata(*transport_, metadata_,
                                          kLocalServerName);
        ASSERT_EQ(UbTransportTestPeer::allocateLocalSegmentID(*transport_), 0);
    }

    // Switches to metadata backed by a storage plugin whose publish can be
    // armed to fail.
    std::shared_ptr<FakeMetadataStoragePlugin> usePublishFailurePlugin() {
        auto plugin = std::make_shared<FakeMetadataStoragePlugin>();
        bindMetadata(std::make_shared<TestableTransferMetadata>(plugin));
        return plugin;
    }

    int registerBuffer(void* addr, size_t length) {
        return UbTransportTestPeer::registerLocalMemory(*transport_, addr,
                                                        length, "cpu:0");
    }

    int unregisterBuffer(void* addr) {
        return UbTransportTestPeer::unregisterLocalMemory(*transport_, addr);
    }

    size_t localBufferCount() {
        return metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.size();
    }

    size_t liveSegmentCount() const {
        size_t n = 0;
        for (const auto& ctx : contexts_) n += ctx->live_regions_.size();
        return n;
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

    // Submits one request in its own batch, observes its terminal state (which
    // freeBatchID() requires) and frees the batch.
    Status submitSingle(const TransferRequest& req,
                        TransferStatus* out = nullptr) {
        auto batch_id = transport_->allocateBatchID(1);
        Status s = transport_->submitTransfer(batch_id, {req});
        TransferStatus status;
        EXPECT_EQ(transport_->getTransferStatus(batch_id, 0, status),
                  Status::OK());
        EXPECT_EQ(transport_->freeBatchID(batch_id), Status::OK());
        if (out) *out = status;
        return s;
    }

    // Slices of a request whose source spans several chunks pick a device
    // per slice (randomly at retry 0), so return them in source order rather
    // than grouped by context.
    std::vector<FakeUbContext::PostedSlice> takePostedSlices() {
        std::vector<FakeUbContext::PostedSlice> result;
        for (const auto& ctx : contexts_) {
            result.insert(result.end(), ctx->posted_slices.begin(),
                          ctx->posted_slices.end());
            ctx->posted_slices.clear();
        }
        std::sort(result.begin(), result.end(),
                  [](const auto& a, const auto& b) {
                      return a.source_addr < b.source_addr;
                  });
        return result;
    }
};

TEST_F(UbLargeSegUnitTest, UrmaContextBoundaryChecks) {
    UrmaContext urma_ctx(*transport_, "mock_urma_device", 1);
    EXPECT_EQ(urma_ctx.registerMemoryRegion(0x10000, kMaxSegSize + 1),
              ERR_INVALID_ARGUMENT);
    UbTransport::BufferDesc buf;
    EXPECT_EQ(urma_ctx.buildLocalBufferDesc(0x10000, buf), ERR_CONTEXT);
    EXPECT_EQ(urma_ctx.localSegWithIndex(0), nullptr);
    EXPECT_EQ(urma_ctx.localSegWithIndex(999), nullptr);
}

TEST_F(UbLargeSegUnitTest, RegisterExactMaxSegSizeStaysSingleChunk) {
    void* base = reinterpret_cast<void*>(0x100000);
    ASSERT_EQ(registerBuffer(base, kMaxSegSize), 0);

    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(desc->buffers.size(), 1u);
    EXPECT_EQ(desc->buffers[0].addr, 0x100000u);
    EXPECT_EQ(desc->buffers[0].length, kMaxSegSize);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
    for (const auto& ctx : contexts_) {
        EXPECT_EQ(ctx->register_calls.size(), 1u);
    }

    ASSERT_EQ(unregisterBuffer(base), 0);
    EXPECT_EQ(localBufferCount(), 0u);
    EXPECT_EQ(liveSegmentCount(), 0u);
}

TEST_F(UbLargeSegUnitTest, RegisterAutoChunksAndUnregistersAllChunks) {
    void* base = reinterpret_cast<void*>(0x200000);
    ASSERT_EQ(registerBuffer(base, kMaxSegSize * 3 + 512), 0);  // 4 chunks

    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(desc->buffers.size(), 4u);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 1u);
    const std::vector<size_t> expected_lens = {kMaxSegSize, kMaxSegSize,
                                               kMaxSegSize, 512};
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(desc->buffers[i].name, "cpu:0");
        EXPECT_EQ(desc->buffers[i].addr, 0x200000u + i * kMaxSegSize);
        EXPECT_EQ(desc->buffers[i].length, expected_lens[i]);
        ASSERT_EQ(desc->buffers[i].l_seg_index.size(), contexts_.size());
        EXPECT_EQ(desc->buffers[i].tseg.size(), contexts_.size());
        for (size_t ci = 0; ci < contexts_.size(); ++ci) {
            EXPECT_EQ(desc->buffers[i].l_seg_index[ci], i);
        }
    }
    EXPECT_EQ(liveSegmentCount(), 4 * contexts_.size());

    ASSERT_EQ(unregisterBuffer(base), 0);
    EXPECT_EQ(localBufferCount(), 0u);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
    EXPECT_EQ(liveSegmentCount(), 0u);
}

TEST_F(UbLargeSegUnitTest,
       RegisterRollbackOnPartialContextAndLaterChunkFailure) {
    void* base = reinterpret_cast<void*>(0x300000);
    const size_t total_len = kMaxSegSize * 3 + 128;
    auto expect_rolled_back = [&] {
        EXPECT_EQ(localBufferCount(), 0u);
        EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
        EXPECT_EQ(liveSegmentCount(), 0u);
    };

    // Context 1 fails on chunk 0: only context 0's segment is unregistered,
    // and context 1, which never registered it, is left untouched.
    contexts_[1]->fail_on_register_call = 0;
    EXPECT_NE(registerBuffer(base, total_len), 0);
    expect_rolled_back();
    EXPECT_EQ(contexts_[0]->unregister_calls.size(), 1u);
    EXPECT_TRUE(contexts_[1]->unregister_calls.empty());

    // Context 1 fails on chunk 2, after chunks 0 and 1 were committed.
    contexts_[1]->register_call_count = 0;
    contexts_[1]->fail_on_register_call = 2;
    EXPECT_NE(registerBuffer(base, total_len), 0);
    expect_rolled_back();

    // buildLocalBufferDesc fails on chunk 1 after its segment was registered.
    contexts_[1]->fail_on_register_call = -1;
    contexts_[0]->build_desc_call_count = 0;
    contexts_[0]->fail_on_build_desc_call = 1;
    EXPECT_NE(registerBuffer(base, total_len), 0);
    expect_rolled_back();
}

TEST_F(UbLargeSegUnitTest, RegisterWildcardLocationResolvesAfterPinning) {
    std::vector<char> backing(kMaxSegSize * 2 + 512, 0);
    void* base = backing.data();
    ASSERT_EQ(UbTransportTestPeer::registerLocalMemory(
                  *transport_, base, backing.size(), kWildcardLocation),
              0);

    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_EQ(desc->buffers.size(), 3u);
    EXPECT_FALSE(desc->buffers[0].name.empty());
    for (const auto& buf : desc->buffers) {
        EXPECT_EQ(buf.name, desc->buffers[0].name);
    }
    ASSERT_EQ(unregisterBuffer(base), 0);
    EXPECT_EQ(liveSegmentCount(), 0u);
}

TEST_F(UbLargeSegUnitTest, HugeTlbPageCheckOnlyAppliesToChunkedBuffers) {
    constexpr size_t kHugepageSize = 2ull << 20;
    constexpr int kHuge2MBFlag = 21 << MAP_HUGE_SHIFT;
    void* huge =
        mmap(nullptr, kHugepageSize, PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | kHuge2MBFlag, -1, 0);
    if (huge == MAP_FAILED) {
        GTEST_SKIP() << "cannot map a 2 MiB huge page (no hugetlb pages "
                        "reserved?)";
    }
    static_cast<char*>(huge)[0] = 1;  // fault the page in for smaps
    if (detectBufferPageSize(huge) != kHugepageSize) {
        munmap(huge, kHugepageSize);
        GTEST_SKIP() << "mapping is not reported as a 2 MiB HugeTLB VMA";
    }

    // max_seg_size below the huge page size: buffers that fit in one segment
    // must still register, only buffers that need chunking are rejected.
    globalConfig().max_seg_size = kHugepageSize / 2;
    EXPECT_EQ(registerBuffer(huge, kHugepageSize / 4), 0);
    EXPECT_EQ(unregisterBuffer(huge), 0);
    EXPECT_EQ(registerBuffer(huge, kHugepageSize), ERR_INVALID_ARGUMENT);
    EXPECT_EQ(liveSegmentCount(), 0u);

    munmap(huge, kHugepageSize);
}

TEST_F(UbLargeSegUnitTest, RegisterAndUnregisterBatchHandlesOversizedBuffers) {
    void* buf0 = reinterpret_cast<void*>(0x1000000);
    void* buf1 = reinterpret_cast<void*>(0x2000000);
    ASSERT_EQ(transport_->registerLocalMemoryBatch(
                  {{buf0, kMaxSegSize * 2 + 128}, {buf1, kMaxSegSize + 256}},
                  "cpu:0"),
              0);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 2u);
    EXPECT_EQ(localBufferCount(), 5u);

    ASSERT_EQ(transport_->unregisterLocalMemoryBatch({buf0, buf1}), 0);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
    EXPECT_EQ(localBufferCount(), 0u);
    EXPECT_EQ(liveSegmentCount(), 0u);
}

TEST_F(UbLargeSegUnitTest, UnregisterRetriesAfterPartialSegmentFailure) {
    for (size_t len : {size_t{kMaxSegSize}, size_t{kMaxSegSize * 3}}) {
        SCOPED_TRACE(len);
        void* base = reinterpret_cast<void*>(0x3A0000);
        ASSERT_EQ(registerBuffer(base, len), 0);

        // Context 1 fails to release the last chunk's segment.
        contexts_[1]->unregister_call_count = 0;
        contexts_[1]->fail_on_unregister_call =
            static_cast<int>(len / kMaxSegSize) - 1;
        EXPECT_NE(unregisterBuffer(base), 0);
        EXPECT_EQ(localBufferCount(), 0u);
        EXPECT_EQ(liveSegmentCount(), 1u);
        EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 1u);

        // The retry releases the surviving segment even though its
        // BufferDesc is already gone.
        contexts_[1]->fail_on_unregister_call = -1;
        ASSERT_EQ(unregisterBuffer(base), 0);
        EXPECT_EQ(liveSegmentCount(), 0u);
        EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
        EXPECT_EQ(unregisterBuffer(base), ERR_ADDRESS_NOT_REGISTERED);
    }
}

TEST_F(UbLargeSegUnitTest, UnregisterRetriesAfterPublishFailure) {
    auto plugin = usePublishFailurePlugin();
    for (size_t len : {size_t{kMaxSegSize}, size_t{kMaxSegSize * 3}}) {
        SCOPED_TRACE(len);
        void* base = reinterpret_cast<void*>(0x3C0000);
        ASSERT_EQ(registerBuffer(base, len), 0);

        // The segments are released before the publish, so a failed publish
        // leaks nothing; the retry only has to republish.
        plugin->failNextSet(1);
        EXPECT_NE(unregisterBuffer(base), 0);
        EXPECT_EQ(localBufferCount(), 0u);
        EXPECT_EQ(liveSegmentCount(), 0u);
        EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 1u);

        ASSERT_EQ(unregisterBuffer(base), 0);
        EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
        EXPECT_EQ(unregisterBuffer(base), ERR_ADDRESS_NOT_REGISTERED);
    }
}

TEST_F(UbLargeSegUnitTest, ReRegisterAfterFailedPublishSupersedesStaleEntry) {
    auto plugin = usePublishFailurePlugin();
    void* base = reinterpret_cast<void*>(0x3E0000);
    ASSERT_EQ(registerBuffer(base, kMaxSegSize * 3), 0);
    plugin->failNextSet(1);
    EXPECT_NE(unregisterBuffer(base), 0);
    ASSERT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 1u);

    // A single-chunk registration at the same addr must replace the
    // metadata-only entry, or the next unregister would skip its segments.
    ASSERT_EQ(registerBuffer(base, kMaxSegSize), 0);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
    ASSERT_EQ(unregisterBuffer(base), 0);
    EXPECT_EQ(localBufferCount(), 0u);
    EXPECT_EQ(liveSegmentCount(), 0u);
}

TEST_F(UbLargeSegUnitTest, UnregisterBatchRetriesAfterPublishFailure) {
    auto plugin = usePublishFailurePlugin();
    void* buf0 = reinterpret_cast<void*>(0x1000000);
    void* buf1 = reinterpret_cast<void*>(0x2000000);
    ASSERT_EQ(transport_->registerLocalMemoryBatch(
                  {{buf0, kMaxSegSize * 2}, {buf1, kMaxSegSize}}, "cpu:0"),
              0);

    plugin->failNextSet(1);
    EXPECT_NE(transport_->unregisterLocalMemoryBatch({buf0, buf1}), 0);
    EXPECT_EQ(liveSegmentCount(), 0u);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 2u);

    ASSERT_EQ(transport_->unregisterLocalMemoryBatch({buf0, buf1}), 0);
    EXPECT_EQ(UbTransportTestPeer::chunkMapSize(*transport_), 0u);
    EXPECT_EQ(localBufferCount(), 0u);
}

// Each slice must stay inside one local and one remote chunk. Local chunks
// sit at local_base + k * kMaxSegSize; remote chunks at kRemoteBase + k *
// kMaxSegSize. kSliceSize = 1024 and kFragmentLimit = 256, so a remainder of
// at most 1280 bytes is merged into the final slice.
TEST_F(UbLargeSegUnitTest, SubmitSplitsAtSourceAndTargetChunkSeams) {
    char* local = reinterpret_cast<char*>(0x400000);
    ASSERT_EQ(registerBuffer(local, kMaxSegSize * 3), 0);
    const Transport::SegmentID kRemoteSegId = 42;
    const uint64_t kRemoteBase = 0x800000;
    addRemoteSegment(kRemoteSegId, "remote-node:1234",
                     {{kRemoteBase, kMaxSegSize},
                      {kRemoteBase + kMaxSegSize, kMaxSegSize},
                      {kRemoteBase + kMaxSegSize * 2, kMaxSegSize}});

    struct Case {
        const char* name;
        TransferRequest::OpCode opcode;
        size_t local_offset;
        uint64_t remote_offset;
        size_t length;
        std::vector<size_t> expected_lengths;
    };
    const std::vector<Case> cases = {
        // A short request that would otherwise be one merged slice.
        {"target seam",
         TransferRequest::WRITE,
         0,
         kMaxSegSize - 200,
         512,
         {200, 312}},
        {"source seam",
         TransferRequest::WRITE,
         kMaxSegSize - 600,
         0,
         1500,
         {600, 900}},
        {"both seams",
         TransferRequest::READ,
         kMaxSegSize - 300,
         kMaxSegSize * 2 - 700,
         1000,
         {300, 400, 300}},
        // slice_size cuts and a seam cut in the same request.
        {"slice size and seam",
         TransferRequest::WRITE,
         kMaxSegSize - 1500,
         0,
         3000,
         {1024, 476, 1024, 476}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.name);
        char* source = local + c.local_offset;
        const uint64_t target = kRemoteBase + c.remote_offset;
        TransferStatus status;
        ASSERT_EQ(submitSingle(makeRequest(c.opcode, source, kRemoteSegId,
                                           target, c.length),
                               &status),
                  Status::OK());
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
        EXPECT_EQ(status.transferred_bytes, c.length);

        const auto slices = takePostedSlices();
        ASSERT_EQ(slices.size(), c.expected_lengths.size());
        size_t offset = 0;
        for (size_t i = 0; i < slices.size(); ++i) {
            const auto& s = slices[i];
            EXPECT_EQ(s.opcode, c.opcode);
            EXPECT_EQ(s.length, c.expected_lengths[i]);
            EXPECT_EQ(s.source_addr, source + offset);
            EXPECT_EQ(s.dest_addr, target + offset);
            const uint64_t src = reinterpret_cast<uint64_t>(s.source_addr);
            EXPECT_EQ(s.local_chunk_va,
                      reinterpret_cast<uint64_t>(local) +
                          (src - reinterpret_cast<uint64_t>(local)) /
                              kMaxSegSize * kMaxSegSize);
            EXPECT_LE(src + s.length, s.local_chunk_va + kMaxSegSize);
            EXPECT_EQ((s.dest_addr - kRemoteBase) / kMaxSegSize,
                      (s.dest_addr + s.length - 1 - kRemoteBase) / kMaxSegSize);
            offset += s.length;
        }
        EXPECT_EQ(offset, c.length);
    }
}

TEST_F(UbLargeSegUnitTest,
       SubmitInactivePrimaryContextFallsBackToActiveContext) {
    void* local = reinterpret_cast<void*>(0x700000);
    ASSERT_EQ(registerBuffer(local, kMaxSegSize), 0);
    contexts_[0]->set_active(false);

    TransferStatus status;
    ASSERT_EQ(submitSingle(
                  makeRequest(TransferRequest::WRITE, local, LOCAL_SEGMENT_ID,
                              reinterpret_cast<uint64_t>(local), 512),
                  &status),
              Status::OK());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(contexts_[0]->posted_slices.empty());
    EXPECT_EQ(contexts_[1]->posted_slices.size(), 1u);
}

TEST_F(UbLargeSegUnitTest, SubmitRespectsAdviseRetryCnt) {
    void* local = reinterpret_cast<void*>(0x780000);
    ASSERT_EQ(registerBuffer(local, kMaxSegSize), 0);

    auto req = makeRequest(TransferRequest::WRITE, local, LOCAL_SEGMENT_ID,
                           reinterpret_cast<uint64_t>(local), 512);
    // Topology::selectDevice() maps retry_count N > 0 to preferred device
    // (N - 1) % count, so retry 2 deterministically lands on ub_1.
    req.advise_retry_cnt = 2;
    TransferStatus status;
    ASSERT_EQ(submitSingle(req, &status), Status::OK());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(contexts_[0]->posted_slices.empty());
    EXPECT_EQ(contexts_[1]->posted_slices.size(), 1u);
}

TEST_F(UbLargeSegUnitTest, SubmitRefreshesStaleRemoteSegmentDesc) {
    globalConfig().metacache = true;
    void* local = reinterpret_cast<void*>(0x580000);
    ASSERT_EQ(registerBuffer(local, kMaxSegSize), 0);

    // A peer whose SegmentDesc is served over the P2P handshake.
    TransferMetadata peer_meta(P2PHANDSHAKE);
    int sockfd = -1;
    const uint16_t port = findAvailableTcpPort(sockfd);
    ASSERT_NE(port, 0);
    const std::string host = globalConfig().use_ipv6 ? "::1" : "127.0.0.1";
    const std::string peer_name =
        maybeWrapIpV6(host) + ":" + std::to_string(port);
    auto peer_desc = std::make_shared<SegmentDesc>();
    peer_desc->name = peer_name;
    peer_desc->protocol = "ub";
    peer_desc->topology = *topology_;
    for (const auto& ctx : contexts_) {
        TransferMetadata::DeviceDesc dev;
        dev.name = ctx->deviceName();
        dev.eid = ctx->getEid();
        peer_desc->devices.push_back(dev);
    }
    ASSERT_EQ(peer_meta.addLocalSegment(LOCAL_SEGMENT_ID, peer_name,
                                        std::move(peer_desc)),
              0);
    TransferMetadata::RpcMetaDesc rpc{};
    rpc.ip_or_host_name = host;
    rpc.rpc_port = port;
    rpc.sockfd = sockfd;
    ASSERT_EQ(peer_meta.addRpcMetaEntry(peer_name, rpc), 0);
    const int daemon_rc = peer_meta.startHandshakeDaemon(
        [](const TransferMetadata::HandShakeDesc&,
           TransferMetadata::HandShakeDesc&) { return 0; },
        port, sockfd);
    if (daemon_rc != 0 && sockfd >= 0) close(sockfd);
    ASSERT_EQ(daemon_rc, 0);

    // Cache the peer's initially empty SegmentDesc, then mount two chunks.
    const Transport::SegmentID peer_id = metadata_->getSegmentID(peer_name);
    ASSERT_GE(peer_id, 0);
    ASSERT_TRUE(metadata_->getSegmentDescByID(peer_id)->buffers.empty());
    const uint64_t kRemoteBase = 0xC00000;
    for (size_t chunk = 0; chunk < 2; ++chunk) {
        BufferDesc buf;
        buf.name = "cpu:0";
        buf.addr = kRemoteBase + chunk * kMaxSegSize;
        buf.length = kMaxSegSize;
        for (const auto& ctx : contexts_) {
            buf.tseg.push_back("remote:" + ctx->deviceName() + ":" +
                               std::to_string(buf.addr));
        }
        ASSERT_EQ(
            peer_meta.addLocalMemoryBuffer(buf, /*update_metadata=*/false), 0);
    }

    const uint64_t target = kRemoteBase + kMaxSegSize - 200;
    TransferStatus status;
    ASSERT_EQ(submitSingle(makeRequest(TransferRequest::WRITE, local, peer_id,
                                       target, 512),
                           &status),
              Status::OK());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    const auto slices = takePostedSlices();
    ASSERT_EQ(slices.size(), 2u);
    EXPECT_EQ(slices[0].dest_addr, target);
    EXPECT_EQ(slices[0].length, 200u);
    EXPECT_EQ(slices[1].dest_addr, kRemoteBase + kMaxSegSize);
    EXPECT_EQ(slices[1].length, 312u);
}

TEST_F(UbLargeSegUnitTest,
       SubmitReturnsAddressNotRegisteredWhenLocalSegUnregistered) {
    void* local = reinterpret_cast<void*>(0x680000);
    ASSERT_EQ(registerBuffer(local, kMaxSegSize), 0);
    // Concurrent context unregistration while metadata still holds the stale
    // BufferDesc snapshot.
    for (const auto& ctx : contexts_) {
        ASSERT_EQ(
            ctx->unregisterMemoryRegion(reinterpret_cast<uint64_t>(local)), 0);
    }

    TransferStatus status;
    EXPECT_TRUE(submitSingle(
                    makeRequest(TransferRequest::WRITE, local, LOCAL_SEGMENT_ID,
                                reinterpret_cast<uint64_t>(local), 512),
                    &status)
                    .IsAddressNotRegistered());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_TRUE(takePostedSlices().empty());
}

TEST_F(UbLargeSegUnitTest, SubmitFailureMarksQueuedUnpostedSlicesFailed) {
    char* local = reinterpret_cast<char*>(0x800000);
    ASSERT_EQ(registerBuffer(local, kMaxSegSize), 0);

    // The first 256 bytes are registered and get queued; the rest fall
    // outside any registered chunk.
    auto batch_id = transport_->allocateBatchID(1);
    EXPECT_TRUE(
        transport_
            ->submitTransfer(
                batch_id,
                {makeRequest(TransferRequest::WRITE, local + kMaxSegSize - 256,
                             LOCAL_SEGMENT_ID,
                             reinterpret_cast<uint64_t>(local), 512)})
            .IsAddressNotRegistered());
    auto& task = Transport::toBatchDesc(batch_id).task_list[0];
    ASSERT_FALSE(task.slice_list.empty());
    EXPECT_EQ(task.slice_list[0]->status, Transport::Slice::FAILED);
    EXPECT_TRUE(takePostedSlices().empty());

    TransferStatus status;
    ASSERT_EQ(transport_->getTransferStatus(batch_id, 0, status), Status::OK());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(transport_->freeBatchID(batch_id), Status::OK());
}

TEST_F(UbLargeSegUnitTest, SubmitFailureAfterWatermarkFailsRemainingTasks) {
    // A watermark of one slice posts every slice as soon as it is queued.
    globalConfig().max_wr = 1;
    globalConfig().num_jetty_per_ep = 1;
    char* local = reinterpret_cast<char*>(0x880000);
    ASSERT_EQ(registerBuffer(local, kMaxSegSize), 0);
    const uint64_t target = reinterpret_cast<uint64_t>(local);

    // Task 0 completes. Task 1 posts its first 256 bytes, then runs past the
    // registered buffer. Task 2 never starts.
    auto batch_id = transport_->allocateBatchID(3);
    EXPECT_TRUE(transport_
                    ->submitTransfer(
                        batch_id, {makeRequest(TransferRequest::WRITE, local,
                                               LOCAL_SEGMENT_ID, target, 512),
                                   makeRequest(TransferRequest::WRITE,
                                               local + kMaxSegSize - 256,
                                               LOCAL_SEGMENT_ID, target, 512),
                                   makeRequest(TransferRequest::WRITE, local,
                                               LOCAL_SEGMENT_ID, target, 512)})
                    .IsAddressNotRegistered());
    EXPECT_EQ(takePostedSlices().size(), 2u);

    const std::vector<TransferStatusEnum> expected = {
        TransferStatusEnum::COMPLETED, TransferStatusEnum::FAILED,
        TransferStatusEnum::FAILED};
    for (size_t i = 0; i < expected.size(); ++i) {
        SCOPED_TRACE(i);
        TransferStatus status;
        ASSERT_EQ(transport_->getTransferStatus(batch_id, i, status),
                  Status::OK());
        EXPECT_EQ(status.s, expected[i]);
    }
    EXPECT_EQ(transport_->freeBatchID(batch_id), Status::OK());
}

}  // namespace
