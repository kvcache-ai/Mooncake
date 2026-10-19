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
//
// CP-3: prepareSubmit reuses SelectionResult for owners that share
// (peer, local MR, remote MR, hint, intent, length, priority, policy).
// NIXL-style strided descriptors in one MR must not call getTransportType
// once per request.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {
namespace {

class FakeSubBatch : public Transport::SubBatch {
   public:
    size_t size() const override { return task_count; }
    size_t task_count = 0;
    std::vector<TransferStatus> statuses;
};

class FakeTransport : public Transport {
   public:
    explicit FakeTransport(TransportType self_type) : self_type_(self_type) {
        caps.dram_to_dram = true;
    }

    std::atomic<int> submit_calls{0};
    std::vector<int> submitted_memory_types;

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config> = nullptr) override {
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new FakeSubBatch();
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        delete static_cast<FakeSubBatch*>(batch);
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& reqs) override {
        ++submit_calls;
        auto* fb = static_cast<FakeSubBatch*>(batch);
        for (const auto& req : reqs) {
            submitted_memory_types.push_back(req.source_memory_type);
            fb->statuses.push_back({TransferStatusEnum::COMPLETED, req.length});
            fb->task_count++;
        }
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto* fb = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fb->statuses.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        status = fb->statuses[task_id];
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(self_type_);
        return Status::OK();
    }

    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override {
        for (auto& desc : desc_list) {
            auto s = addMemoryBuffer(desc, options);
            if (!s.ok()) return s;
        }
        return Status::OK();
    }

    Status removeMemoryBuffer(BufferDesc&) override { return Status::OK(); }

    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions&) override {
        *addr = std::malloc(size);
        return *addr ? Status::OK()
                     : Status::InternalError("malloc failed" LOC_MARK);
    }

    Status freeLocalMemory(void* addr, size_t) override {
        std::free(addr);
        return Status::OK();
    }

    bool warmupMemory(void*, size_t) override { return false; }

    const char* getName() const override {
        return self_type_ == RDMA ? "<fake-rdma>" : "<fake-tcp>";
    }

   private:
    TransportType self_type_;
};

std::shared_ptr<Config> makeConfig(bool merge_requests) {
    auto cfg = std::make_shared<Config>();
    cfg->set("metadata_type", "p2p");
    cfg->set("metadata_servers", "");
    cfg->set("rpc_server_hostname", "127.0.0.1");
    cfg->set("rpc_server_port", "0");
    cfg->set("log_level", "warning");
    cfg->set("merge_requests", merge_requests);
    cfg->set("transports/tcp/enable", false);
    cfg->set("transports/shm/enable", false);
    cfg->set("transports/rdma/enable", false);
    cfg->set("transports/io_uring/enable", false);
    cfg->set("transports/nvlink/enable", false);
    cfg->set("transports/mnnvl/enable", false);
    cfg->set("transports/gds/enable", false);
    cfg->set("transports/ascend_direct/enable", false);
    cfg->set("enable_auto_failover_on_poll", false);
    cfg->set("max_failover_attempts", 0);
    return cfg;
}

void installFakeRdmaAndTcp(TransferEngineImpl& engine,
                           std::shared_ptr<FakeTransport>& rdma,
                           std::shared_ptr<FakeTransport>& tcp) {
    rdma = std::make_shared<FakeTransport>(RDMA);
    tcp = std::make_shared<FakeTransport>(TCP);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(rdma->install(seg, nullptr, nullptr).ok());
    ASSERT_TRUE(tcp->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, rdma);
    engine.swapTransportForTest(TCP, tcp);
}

Request makeLocalWrite(uint8_t* ptr, size_t len) {
    Request req;
    req.opcode = Request::WRITE;
    req.source = ptr;
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(ptr);
    req.length = len;
    return req;
}

// 16 B gaps so merge_requests cannot collapse owners. Same covering MR.
constexpr size_t kChunk = 4096;
constexpr size_t kGap = 16;
constexpr size_t kStride = kChunk + kGap;

std::vector<Request> stridedWrites(uint8_t* base, size_t count) {
    std::vector<Request> reqs;
    reqs.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        reqs.push_back(makeLocalWrite(base + i * kStride, kChunk));
    }
    return reqs;
}

TEST(TransportRouteCache, SameMrReusesRoute) {
    auto cfg = makeConfig(true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    constexpr size_t kCount = 8;
    std::vector<uint8_t> buf(kCount * kStride, 0x11);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    engine.resetTransportResolveCallsForTest();
    BatchID batch_id = engine.allocateBatch(kCount);
    ASSERT_TRUE(
        engine.submitTransfer(batch_id, stridedWrites(buf.data(), kCount))
            .ok());

    EXPECT_EQ(engine.transportResolveCallsForTest(), 1u)
        << "strided descriptors in one MR must share one SelectionResult";
    EXPECT_EQ(fake_rdma->submit_calls.load() + fake_tcp->submit_calls.load(),
              1);
    ASSERT_EQ(fake_rdma->submitted_memory_types.size() +
                  fake_tcp->submitted_memory_types.size(),
              kCount);
    for (int t : fake_rdma->submitted_memory_types) {
        EXPECT_EQ(t, static_cast<int>(MTYPE_CPU));
    }
    for (int t : fake_tcp->submitted_memory_types) {
        EXPECT_EQ(t, static_cast<int>(MTYPE_CPU));
    }

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

TEST(TransportRouteCache, SameMrReusesRouteWithoutMerge) {
    auto cfg = makeConfig(false);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    constexpr size_t kCount = 8;
    std::vector<uint8_t> buf(kCount * kStride, 0x22);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    engine.resetTransportResolveCallsForTest();
    BatchID batch_id = engine.allocateBatch(kCount);
    ASSERT_TRUE(
        engine.submitTransfer(batch_id, stridedWrites(buf.data(), kCount))
            .ok());
    EXPECT_EQ(engine.transportResolveCallsForTest(), 1u);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

TEST(TransportRouteCache, DistinctMrsResolveOnceEach) {
    auto cfg = makeConfig(true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    constexpr size_t kCount = 4;
    std::vector<uint8_t> a(kCount * kStride, 0x31);
    std::vector<uint8_t> b(kCount * kStride, 0x32);
    ASSERT_TRUE(engine.registerLocalMemory(a.data(), a.size()).ok());
    ASSERT_TRUE(engine.registerLocalMemory(b.data(), b.size()).ok());

    auto reqs = stridedWrites(a.data(), kCount);
    auto more = stridedWrites(b.data(), kCount);
    reqs.insert(reqs.end(), more.begin(), more.end());

    engine.resetTransportResolveCallsForTest();
    BatchID batch_id = engine.allocateBatch(reqs.size());
    ASSERT_TRUE(engine.submitTransfer(batch_id, reqs).ok());
    EXPECT_EQ(engine.transportResolveCallsForTest(), 2u);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(a.data(), a.size()).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(b.data(), b.size()).ok());
}

TEST(TransportRouteCache, DistinctHintDoesNotReuse) {
    auto cfg = makeConfig(true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    std::vector<uint8_t> buf(2 * kStride, 0x41);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    Request rdma = makeLocalWrite(buf.data(), kChunk);
    rdma.transport_hint = RDMA;
    Request tcp = makeLocalWrite(buf.data() + kStride, kChunk);
    tcp.transport_hint = TCP;

    engine.resetTransportResolveCallsForTest();
    BatchID batch_id = engine.allocateBatch(2);
    ASSERT_TRUE(engine.submitTransfer(batch_id, {rdma, tcp}).ok());
    EXPECT_EQ(engine.transportResolveCallsForTest(), 2u);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

TEST(TransportRouteCache, DistinctIntentDoesNotReuse) {
    auto cfg = makeConfig(true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    std::vector<uint8_t> buf(2 * kStride, 0x51);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    Request a = makeLocalWrite(buf.data(), kChunk);
    a.intent_type = IntentType::FOREGROUND_GET;
    Request b = makeLocalWrite(buf.data() + kStride, kChunk);
    b.intent_type = IntentType::BACKGROUND_PREFETCH;

    engine.resetTransportResolveCallsForTest();
    BatchID batch_id = engine.allocateBatch(2);
    ASSERT_TRUE(engine.submitTransfer(batch_id, {a, b}).ok());
    EXPECT_EQ(engine.transportResolveCallsForTest(), 2u);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

TEST(TransportRouteCache, DistinctLengthDoesNotReuse) {
    auto cfg = makeConfig(true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    std::vector<uint8_t> buf(kChunk + kGap + kChunk * 2, 0x61);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    Request a = makeLocalWrite(buf.data(), kChunk);
    Request b = makeLocalWrite(buf.data() + kChunk + kGap, kChunk * 2);

    engine.resetTransportResolveCallsForTest();
    BatchID batch_id = engine.allocateBatch(2);
    ASSERT_TRUE(engine.submitTransfer(batch_id, {a, b}).ok());
    EXPECT_EQ(engine.transportResolveCallsForTest(), 2u);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
