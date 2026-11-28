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

#include "common.h"
#include "config.h"
#include "multi_transport.h"
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include "transfer_metadata.h"

namespace mooncake {
class MultiTransportBatchTestPeer {
   public:
    static void install(MultiTransport &multi, const std::string &name,
                        std::shared_ptr<Transport> transport) {
        multi.transport_map_[name] = std::move(transport);
    }

    static int registerMemory(Transport& transport, void* addr, size_t length) {
        return transport.registerLocalMemory(addr, length, "*", true, false);
    }

    static int unregisterMemory(Transport& transport, void* addr) {
        return transport.unregisterLocalMemory(addr, false);
    }

    static int registerBatch(
        Transport& transport,
        const std::vector<Transport::BufferEntry>& entries) {
        return transport.registerLocalMemoryBatch(entries, "*");
    }

    static int unregisterBatch(Transport& transport,
                               const std::vector<void*>& addresses) {
        return transport.unregisterLocalMemoryBatch(addresses);
    }
};
namespace {
#ifdef USE_TCP
TEST(TcpMemoryBatchTest, RepeatedRegistrationAndMixedRemovalMatchSingles) {
    using Peer = MultiTransportBatchTestPeer;
    char buffer[32]{};
    void* a = buffer;
    void* b = buffer + 8;
    void* missing = buffer + 16;
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:0", "127.0.0.1", 0), 0);
    auto* tcp = engine.installTransport("tcp", nullptr);
    ASSERT_NE(tcp, nullptr);
    auto metadata = tcp->meta();

    TransferMetadata::BufferDesc foreign{};
    foreign.name = "shm";
    foreign.addr = reinterpret_cast<uint64_t>(a);
    foreign.length = 8;
    foreign.shm_name = "/shm-buffer";
#ifdef ENABLE_MULTI_PROTOCOL
    foreign.protocol = "shm";
#endif
    ASSERT_EQ(metadata->addLocalMemoryBuffer(foreign, false), 0);
    const auto local = [&]() {
        return metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    };
    const std::vector<Transport::BufferEntry> entries{{a, 8}, {b, 8}};
    for (int i = 0; i < 2; ++i) {
        for (const auto& entry : entries)
            ASSERT_EQ(Peer::registerMemory(*tcp, entry.addr, entry.length), 0);
    }
    auto registered = local();
    for (void* addr : {a, b, a, b})
        ASSERT_EQ(Peer::unregisterMemory(*tcp, addr), 0);
    ASSERT_EQ(local()->buffers.size(), 1U);
    ASSERT_EQ(local()->buffers.front(), foreign);

    EXPECT_EQ(Peer::registerBatch(*tcp, {}), 0);
    EXPECT_EQ(Peer::unregisterBatch(*tcp, {}), 0);
    ASSERT_EQ(Peer::registerBatch(*tcp, entries), 0);
    ASSERT_EQ(Peer::registerBatch(*tcp, entries), 0);
    auto current = local();
    ASSERT_EQ(current->buffers.size(), registered->buffers.size());
    for (size_t i = 0; i < current->buffers.size(); ++i) {
        const auto& actual = current->buffers[i];
        const auto& expected = registered->buffers[i];
        EXPECT_EQ(actual.addr, expected.addr);
        EXPECT_EQ(actual.length, expected.length);
        EXPECT_EQ(actual.name, expected.name);
        EXPECT_EQ(actual.shm_name, expected.shm_name);
#ifdef ENABLE_MULTI_PROTOCOL
        EXPECT_EQ(actual.protocol, expected.protocol);
#endif
    }

    EXPECT_EQ(Peer::unregisterBatch(*tcp, {a, missing, a, a, b}),
              ERR_ADDRESS_NOT_REGISTERED);
    ASSERT_EQ(local()->buffers.size(), 2U);
    EXPECT_EQ(local()->buffers.front(), foreign);
    EXPECT_EQ(local()->buffer_range_index.findCovering(
                  reinterpret_cast<uint64_t>(b), 8),
              1);
    EXPECT_EQ(Peer::unregisterBatch(*tcp, {b}), 0);
    EXPECT_EQ(Peer::unregisterBatch(*tcp, {a, b}), ERR_ADDRESS_NOT_REGISTERED);
    ASSERT_EQ(local()->buffers.size(), 1U);
    EXPECT_EQ(local()->buffers.front(), foreign);
}
#endif

class RecordingTransport : public Transport {
   public:
    std::vector<uint64_t> addresses;
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest> &) override {
        return Status::OK();
    }
    Status submitTransferTask(
        const std::vector<TransferTask *> &tasks) override {
        for (auto *task : tasks) {
            addresses.push_back(task->request->target_offset);
            task->is_finished = true;
        }
        return Status::OK();
    }
    Status getTransferStatus(BatchID, size_t, TransferStatus &) override {
        return Status::OK();
    }

   private:
    int registerLocalMemory(void *, size_t, const std::string &, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void *, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry> &,
                                 const std::string &) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void *> &) override {
        return 0;
    }
    const char *getName() const override { return "recording"; }
};
struct ScopedMetacache {
    bool old;
    explicit ScopedMetacache(bool enabled) : old(globalConfig().metacache) {
        globalConfig().metacache = enabled;
    }
    ~ScopedMetacache() { globalConfig().metacache = old; }
};
class BatchRouting : public ::testing::Test {
   protected:
    std::string local = "local:1";
    std::shared_ptr<TransferMetadata> md =
        std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    MultiTransport multi{md, local};
    std::shared_ptr<RecordingTransport> rdma =
        std::make_shared<RecordingTransport>();
    std::shared_ptr<RecordingTransport> tcp =
        std::make_shared<RecordingTransport>();
    void SetUp() override {
        MultiTransportBatchTestPeer::install(multi, "rdma", rdma);
        MultiTransportBatchTestPeer::install(multi, "tcp", tcp);
    }
    std::shared_ptr<TransferMetadata::SegmentDesc> add(uint64_t id,
                                                       std::string proto) {
        auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
        desc->name = "remote:" + std::to_string(id);
        desc->protocol = proto;
        auto held = desc;
        md->addLocalSegment(id, desc->name, std::move(held));
        return desc;
    }
    Status submit(
        std::initializer_list<std::pair<uint64_t, uint64_t>> targets) {
        std::vector<Transport::TransferRequest> entries;
        for (auto [id, addr] : targets) {
            Transport::TransferRequest entry{};
            entry.target_id = id;
            entry.target_offset = addr;
            entry.length = 16;
            entries.push_back(entry);
        }
        auto batch = multi.allocateBatchID(entries.size());
        auto status = multi.submitTransfer(batch, entries);
        EXPECT_TRUE(multi.freeBatchID(batch).ok());
        return status;
    }
};
TEST_F(BatchRouting, RepeatedAndAlternatingTargetsReachCorrectTransport) {
    add(1, "rdma");
    add(2, "tcp");
    ASSERT_TRUE(
        submit({{1, 100}, {1, 200}, {2, 300}, {1, 400}, {2, 500}}).ok());
    EXPECT_EQ(rdma->addresses, (std::vector<uint64_t>{100, 200, 400}));
    EXPECT_EQ(tcp->addresses, (std::vector<uint64_t>{300, 500}));
}
TEST_F(BatchRouting, NextBatchObservesMetadataReplacement) {
    add(1, "rdma");
    ASSERT_TRUE(submit({{1, 100}, {1, 200}}).ok());
    add(1, "tcp");
    ASSERT_TRUE(submit({{1, 300}, {1, 400}}).ok());
    EXPECT_EQ(rdma->addresses.size(), 2u);
    EXPECT_EQ(tcp->addresses.size(), 2u);
}
TEST_F(BatchRouting, UnsupportedTargetDoesNotPartiallySubmit) {
    add(1, "rdma");
    add(2, "not-installed");
    ASSERT_FALSE(submit({{1, 100}, {2, 200}}).ok());
    EXPECT_TRUE(rdma->addresses.empty());
}
TEST_F(BatchRouting, TwoSidedFallbackIsRetained) {
    auto other = std::make_shared<RecordingTransport>();
    MultiTransport second(md, local);
    MultiTransportBatchTestPeer::install(second, "rdma_twosided", other);
    add(1, "rdma");
    auto batch = second.allocateBatchID(2);
    Transport::TransferRequest request{};
    request.target_id = 1;
    request.length = 16;
    ASSERT_TRUE(second.submitTransfer(batch, {request, request}).ok());
    EXPECT_EQ(other->addresses.size(), 2u);
    EXPECT_TRUE(second.freeBatchID(batch).ok());
}
TEST_F(BatchRouting, DisabledMetacacheStillRoutesLocalSegment) {
    ScopedMetacache disable(false);
    // LOCAL_SEGMENT_ID is excluded from getSegmentDescByID's metacache-off
    // refresh (segment_id != LOCAL_SEGMENT_ID). This only checks that
    // MC_DISABLE_METACACHE=1 still routes a local segment; it does not
    // verify per-request remote refresh.
    add(LOCAL_SEGMENT_ID, "rdma");
    ASSERT_TRUE(
        submit({{LOCAL_SEGMENT_ID, 100}, {LOCAL_SEGMENT_ID, 200}}).ok());
    EXPECT_EQ(rdma->addresses, (std::vector<uint64_t>{100, 200}));
}
#ifdef ENABLE_MULTI_PROTOCOL
TEST_F(BatchRouting, MixedProtocolSameTargetStillRoutesByAddress) {
    auto desc = add(1, "rdma,tcp");
    TransferMetadata::BufferDesc a;
    a.addr = 100;
    a.length = 100;
    a.protocol = "rdma";
    TransferMetadata::BufferDesc b;
    b.addr = 300;
    b.length = 100;
    b.protocol = "tcp";
    desc->buffers = {a, b};
    ASSERT_TRUE(submit({{1, 100}, {1, 300}, {1, 116}, {1, 316}}).ok());
    EXPECT_EQ(rdma->addresses, (std::vector<uint64_t>{100, 116}));
    EXPECT_EQ(tcp->addresses, (std::vector<uint64_t>{300, 316}));
}
#endif
}  // namespace
}  // namespace mooncake
