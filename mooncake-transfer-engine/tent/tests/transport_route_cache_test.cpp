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
// Strided descriptors in one MR must not call getTransportType once per
// request. Reuse is the findBuffer buffer, not any range that contains the
// address. policy_name nullopt is not the same key as "".

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
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
    std::atomic<int> submitted_tasks{0};
    // Fired from selection. A publish here happens after findBuffer keys were
    // copied and before the route is stored.
    mutable std::function<void()> on_capabilities;

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
            ++submitted_tasks;
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

    const Capabilities capabilities() const override {
        if (on_capabilities) on_capabilities();
        return caps;
    }

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

class TransportRouteCacheTest : public ::testing::TestWithParam<bool> {
   protected:
    void SetUp() override {
        engine_ = std::make_unique<TransferEngineImpl>(makeConfig(GetParam()));
        ASSERT_TRUE(engine_->available());
        installFakeRdmaAndTcp(*engine_, rdma_, tcp_);
    }

    void TearDown() override {
        for (auto& buf : buffers_) {
            if (!buf.empty())
                engine_->unregisterLocalMemory(buf.data(), buf.size());
        }
    }

    uint8_t* registerBytes(size_t bytes, uint8_t fill) {
        buffers_.emplace_back(bytes, fill);
        EXPECT_TRUE(
            engine_->registerLocalMemory(buffers_.back().data(), bytes).ok());
        return buffers_.back().data();
    }

    uint64_t submitAndCountResolves(const std::vector<Request>& reqs) {
        engine_->resetTransportResolveCallsForTest();
        BatchID batch_id = engine_->allocateBatch(reqs.size());
        EXPECT_TRUE(engine_->submitTransfer(batch_id, reqs).ok());
        const uint64_t calls = engine_->transportResolveCallsForTest();
        EXPECT_TRUE(engine_->freeBatch(batch_id).ok());
        return calls;
    }

    std::unique_ptr<TransferEngineImpl> engine_;
    std::shared_ptr<FakeTransport> rdma_, tcp_;
    std::vector<std::vector<uint8_t>> buffers_;
};

INSTANTIATE_TEST_SUITE_P(MergeOnOff, TransportRouteCacheTest,
                         ::testing::Bool());

TEST_P(TransportRouteCacheTest, SameMrReusesRoute) {
    constexpr size_t kCount = 8;
    uint8_t* base = registerBytes(kCount * kStride, 0x11);
    EXPECT_EQ(submitAndCountResolves(stridedWrites(base, kCount)), 1u);
}

TEST_P(TransportRouteCacheTest, DistinctMrsResolveOnceEach) {
    constexpr size_t kCount = 4;
    uint8_t* a = registerBytes(kCount * kStride, 0x31);
    uint8_t* b = registerBytes(kCount * kStride, 0x32);
    auto reqs = stridedWrites(a, kCount);
    auto more = stridedWrites(b, kCount);
    reqs.insert(reqs.end(), more.begin(), more.end());
    EXPECT_EQ(submitAndCountResolves(reqs), 2u);
}

TEST_P(TransportRouteCacheTest, DistinctKeyFieldDoesNotReuse) {
    uint8_t* base = registerBytes(kStride + kChunk * 2, 0x41);
    const std::pair<const char*, void (*)(Request&)> cases[] = {
        {"hint", [](Request& req) { req.transport_hint = TCP; }},
        {"intent",
         [](Request& req) {
             req.intent_type = IntentType::BACKGROUND_PREFETCH;
         }},
        {"length", [](Request& req) { req.length = kChunk * 2; }},
        {"priority", [](Request& req) { req.priority = PRIO_LOW; }},
    };
    for (const auto& [name, mutate] : cases) {
        SCOPED_TRACE(name);
        Request first = makeLocalWrite(base, kChunk);
        if (std::string(name) == "intent")
            first.intent_type = IntentType::FOREGROUND_GET;
        Request second = makeLocalWrite(base + kChunk + kGap, kChunk);
        mutate(second);
        EXPECT_EQ(submitAndCountResolves({first, second}), 2u);
    }
}

TEST_P(TransportRouteCacheTest, PolicyNameNulloptDoesNotReuseEmptyString) {
    uint8_t* base = registerBytes(2 * kStride, 0x81);
    Request unbound = makeLocalWrite(base, kChunk);
    Request empty_name = makeLocalWrite(base + kStride, kChunk);
    empty_name.policy_name = std::string{};
    // Default policy resolves once. "" is a different key and selects UNSPEC,
    // so resolveTransport calls getTransportType again after invalidateRemote.
    EXPECT_EQ(submitAndCountResolves({unbound, empty_name}), 3u);
    EXPECT_EQ(rdma_->submitted_tasks.load(), 1);
    EXPECT_EQ(tcp_->submitted_tasks.load(), 0);
}

TEST_P(TransportRouteCacheTest, UnspecMissIsNotCached) {
    uint8_t* base = registerBytes(2 * kStride, 0x91);
    Request a = makeLocalWrite(base, kChunk);
    a.policy_name = std::string{};
    Request b = makeLocalWrite(base + kStride, kChunk);
    b.policy_name = std::string{};
    // Each UNSPEC miss calls getTransportType twice. The first miss also marks
    // the peer refreshed, so the second owner cannot reuse a cached UNSPEC.
    // Caching the miss would report 2.
    EXPECT_EQ(submitAndCountResolves({a, b}), 4u);
    EXPECT_EQ(rdma_->submitted_tasks.load() + tcp_->submitted_tasks.load(), 0);
}

TEST_P(TransportRouteCacheTest, SnapshotPublishSkipsRouteCache) {
    uint8_t* base = registerBytes(2 * kStride, 0xb1);
    auto reqs = stridedWrites(base, 2);
    for (const char* which : {"local", "remote"}) {
        SCOPED_TRACE(which);
        bool published = false;
        auto publish_once = [&]() {
            if (published) return;
            published = true;
            engine_->publishSegmentSnapshotForTest(std::string(which) ==
                                                   "local");
        };
        rdma_->on_capabilities = publish_once;
        tcp_->on_capabilities = publish_once;
        // Same MR would resolve once. The publication moves a generation the
        // key was drawn from, so the second owner resolves again.
        EXPECT_EQ(submitAndCountResolves(reqs), 2u);
        rdma_->on_capabilities = nullptr;
        tcp_->on_capabilities = nullptr;
    }
}

TEST_P(TransportRouteCacheTest, HashMapHitAfterInterleaving) {
    uint8_t* base = registerBytes(3 * kStride, 0xa1);
    Request a = makeLocalWrite(base, kChunk);
    a.transport_hint = RDMA;
    Request mid = makeLocalWrite(base + kStride, kChunk);
    mid.transport_hint = TCP;
    Request again = makeLocalWrite(base + 2 * kStride, kChunk);
    again.transport_hint = RDMA;
    EXPECT_EQ(submitAndCountResolves({a, mid, again}), 2u);
    EXPECT_EQ(rdma_->submitted_tasks.load(), 2);
    EXPECT_EQ(tcp_->submitted_tasks.load(), 1);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
