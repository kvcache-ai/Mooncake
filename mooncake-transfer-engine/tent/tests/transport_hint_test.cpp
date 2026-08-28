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
// End-to-end tests for Request::transport_hint added to
// TransferEngineImpl::submitTransfer:
//   * hint == UNSPEC keeps existing policy-driven behavior;
//   * hint pinned to RDMA / TCP routes the request onto that transport;
//   * hint to a disabled / capability-mismatched transport returns
//     InvalidArgument and does not mutate batch state;
//   * out-of-range hint is rejected up front.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {
namespace {

// ---------------------------------------------------------------------------
// FakeTransport: minimal Transport that records itself on BufferDesc, claims
// dram_to_dram capability, and completes transfers synchronously. The
// per-instance submit_calls counter is what each test inspects to learn
// which transport the engine routed onto for a given request.
// ---------------------------------------------------------------------------
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
    std::vector<uint64_t> submitted_device_masks;
    std::vector<std::string> submitted_qp_pools;
    std::vector<size_t> submitted_request_counts;
    std::string fail_qp_pool_once;

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
        submitted_device_masks.push_back(fb->device_mask);
        submitted_qp_pools.push_back(fb->qp_pool);
        submitted_request_counts.push_back(reqs.size());
        if (!fail_qp_pool_once.empty() && fb->qp_pool == fail_qp_pool_once) {
            fail_qp_pool_once.clear();
            return Status::InternalError("injected submit failure" LOC_MARK);
        }
        for (const auto& req : reqs) {
            fb->statuses.push_back({TransferStatusEnum::COMPLETED, req.length});
            fb->task_count++;
        }
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto* fb = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fb->statuses.size())
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        status = fb->statuses[task_id];
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(self_type_);
        return Status::OK();
    }
    Status addMemoryBuffer(std::vector<BufferDesc>& list,
                           const MemoryOptions& options) override {
        for (auto& d : list) {
            auto s = addMemoryBuffer(d, options);
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

std::shared_ptr<Config> makeMinimalP2PConfig() {
    auto cfg = std::make_shared<Config>();
    cfg->set("metadata_type", "p2p");
    cfg->set("metadata_servers", "");
    cfg->set("rpc_server_hostname", "127.0.0.1");
    cfg->set("rpc_server_port", "0");
    cfg->set("log_level", "warning");
    cfg->set("merge_requests", false);
    cfg->set("transports/tcp/enable", false);
    cfg->set("transports/shm/enable", false);
    cfg->set("transports/rdma/enable", false);
    cfg->set("transports/io_uring/enable", false);
    cfg->set("transports/nvlink/enable", false);
    cfg->set("transports/mnnvl/enable", false);
    cfg->set("transports/gds/enable", false);
    cfg->set("transports/ascend_direct/enable", false);
    return cfg;
}

void installFakeRdmaAndTcp(TransferEngineImpl& engine,
                           std::shared_ptr<FakeTransport>& rdma,
                           std::shared_ptr<FakeTransport>& tcp) {
    rdma = std::make_shared<FakeTransport>(RDMA);
    tcp = std::make_shared<FakeTransport>(TCP);
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(rdma->install(seg_name, nullptr, nullptr).ok());
    ASSERT_TRUE(tcp->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, rdma);
    engine.swapTransportForTest(TCP, tcp);
}

Request makeLocalWriteRequest(uint8_t* buf, size_t len) {
    Request r;
    r.opcode = Request::WRITE;
    r.source = buf;
    r.target_id = LOCAL_SEGMENT_ID;
    r.target_offset = reinterpret_cast<uint64_t>(buf);
    r.length = len;
    return r;
}

// ---------------------------------------------------------------------------
// 1. hint=RDMA / hint=TCP routing
// ---------------------------------------------------------------------------

TEST(TransportHint, RoutesPinnedRequestsToHintedTransport) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x01);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    // Two batches, each with one hinted request. Use separate batches so
    // submit_calls is unambiguous about which transport handled which.
    BatchID b_rdma = engine.allocateBatch(2);
    BatchID b_tcp = engine.allocateBatch(2);
    ASSERT_NE(b_rdma, (BatchID)0);
    ASSERT_NE(b_tcp, (BatchID)0);

    Request r_rdma = makeLocalWriteRequest(buf.data(), kBufLen);
    r_rdma.transport_hint = RDMA;
    Request r_tcp = makeLocalWriteRequest(buf.data(), kBufLen);
    r_tcp.transport_hint = TCP;

    ASSERT_TRUE(engine.submitTransfer(b_rdma, {r_rdma}).ok());
    ASSERT_TRUE(engine.submitTransfer(b_tcp, {r_tcp}).ok());

    EXPECT_EQ(fake_rdma->submit_calls.load(), 1)
        << "RDMA hint should route onto the RDMA fake exactly once";
    EXPECT_EQ(fake_tcp->submit_calls.load(), 1)
        << "TCP hint should route onto the TCP fake exactly once";

    EXPECT_TRUE(engine.freeBatch(b_rdma).ok());
    EXPECT_TRUE(engine.freeBatch(b_tcp).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// 2. Disabled transport rejected before batch state mutates
// ---------------------------------------------------------------------------

TEST(TransportHint, RejectsHintForTransportNotInstalled) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);
    // Intentionally leave GDS uninstalled.

    constexpr size_t kBufLen = 1024;
    std::vector<uint8_t> buf(kBufLen, 0x77);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);
    Request bad = makeLocalWriteRequest(buf.data(), kBufLen);
    bad.transport_hint = GDS;

    auto status = engine.submitTransfer(batch_id, {bad});
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 0);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// 3. Out-of-range hint value rejected (defends against uninitialized C
//    struct fields and stray casts)
// ---------------------------------------------------------------------------

TEST(TransportHint, RejectsHintWithOutOfRangeValue) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    constexpr size_t kBufLen = 1024;
    std::vector<uint8_t> buf(kBufLen);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);

    Request too_big = makeLocalWriteRequest(buf.data(), kBufLen);
    too_big.transport_hint = static_cast<TransportType>(123);
    EXPECT_TRUE(engine.submitTransfer(batch_id, {too_big}).IsInvalidArgument());

    Request negative = makeLocalWriteRequest(buf.data(), kBufLen);
    negative.transport_hint = static_cast<TransportType>(-1);
    EXPECT_TRUE(
        engine.submitTransfer(batch_id, {negative}).IsInvalidArgument());

    // Neither submission should have reached any transport.
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 0);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// 4. UNSPEC keeps original policy-driven behavior intact
// ---------------------------------------------------------------------------

TEST(TransportHint, UnspecHintPreservesPolicyDrivenSelection) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0xAA);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);
    Request r = makeLocalWriteRequest(buf.data(), kBufLen);
    // r.transport_hint defaults to UNSPEC.

    ASSERT_TRUE(engine.submitTransfer(batch_id, {r}).ok());

    // The default policy prefers RDMA over TCP for memory segments, so
    // an UNSPEC request should land on RDMA. This guards the early-out
    // path that the change explicitly preserves.
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 0);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// 5. Mixed hints in one batch: each request lands on its hinted transport
// ---------------------------------------------------------------------------

TEST(TransportHint, MixedHintsInBatchSplitAcrossTransports) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    constexpr size_t kBufLen = 8192;
    std::vector<uint8_t> buf(kBufLen, 0xBB);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(4);

    Request r_rdma = makeLocalWriteRequest(buf.data(), 4096);
    r_rdma.transport_hint = RDMA;
    Request r_tcp = makeLocalWriteRequest(buf.data() + 4096, 4096);
    r_tcp.transport_hint = TCP;

    ASSERT_TRUE(engine.submitTransfer(batch_id, {r_rdma, r_tcp}).ok());

    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// 6. Requests using different RDMA policies must not share one scalar
//    SubBatch policy. The A/B/A order also verifies physical task IDs after
//    policy grouping remain aligned with the transport's append order.
// ---------------------------------------------------------------------------

TEST(TransportPolicy, SplitsRdmaSubBatchesByResolvedPolicy) {
    auto cfg = makeMinimalP2PConfig();
    cfg->set("merge_requests", true);
    json topology;
    topology["cpu:0"] = {{"mlx5_0"}, {"mlx5_1"}};
    cfg->set("topology/priority_matrix", topology);
    json policy_a;
    policy_a["name"] = "rdma-a";
    policy_a["segment_type"] = "memory";
    policy_a["transports"] = {"rdma"};
    policy_a["devices"] = {"mlx5_0"};
    policy_a["qp_pool"] = "pool-a";
    json policy_b;
    policy_b["name"] = "rdma-b";
    policy_b["segment_type"] = "memory";
    policy_b["transports"] = {"rdma"};
    policy_b["devices"] = {"mlx5_1"};
    policy_b["qp_pool"] = "pool-b";
    cfg->set("policy", json::array({policy_a, policy_b}));

    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);

    constexpr size_t kRequestLengths[] = {1024, 2048, 3072};
    constexpr size_t kBufLen =
        kRequestLengths[0] + kRequestLengths[1] + kRequestLengths[2];
    std::vector<uint8_t> buf(kBufLen, 0xCC);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    Request request_a0 = makeLocalWriteRequest(buf.data(), kRequestLengths[0]);
    request_a0.policy_name = "rdma-a";
    Request request_b = makeLocalWriteRequest(buf.data() + kRequestLengths[0],
                                              kRequestLengths[1]);
    request_b.policy_name = "rdma-b";
    Request request_a1 = makeLocalWriteRequest(
        buf.data() + kRequestLengths[0] + kRequestLengths[1],
        kRequestLengths[2]);
    request_a1.policy_name = "rdma-a";

    BatchID batch_id = engine.allocateBatch(4);
    ASSERT_TRUE(
        engine.submitTransfer(batch_id, {request_a0, request_b, request_a1})
            .ok());

    ASSERT_EQ(fake_rdma->submit_calls.load(), 2);
    ASSERT_EQ(fake_rdma->submitted_request_counts.size(), 2u);
    EXPECT_EQ(fake_rdma->submitted_request_counts[0], 2u);
    EXPECT_EQ(fake_rdma->submitted_request_counts[1], 1u);
    ASSERT_EQ(fake_rdma->submitted_device_masks.size(), 2u);
    EXPECT_EQ(fake_rdma->submitted_device_masks[0], 1ULL);
    EXPECT_EQ(fake_rdma->submitted_device_masks[1], 1ULL << 1);
    ASSERT_EQ(fake_rdma->submitted_qp_pools.size(), 2u);
    EXPECT_EQ(fake_rdma->submitted_qp_pools[0], "pool-a");
    EXPECT_EQ(fake_rdma->submitted_qp_pools[1], "pool-b");

    for (size_t task_id = 0; task_id < 3; ++task_id) {
        TransferStatus status;
        ASSERT_TRUE(engine.getTransferStatus(batch_id, task_id, status).ok());
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kRequestLengths[task_id]);
    }

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// A failed homogeneous group may return without appending any transport task.
// The next group must derive its task ID from the SubBatch's actual size, not
// from the number of requests the engine intended to submit.
TEST(TransportPolicy, SyncFailureDoesNotShiftLaterGroupTaskIds) {
    auto cfg = makeMinimalP2PConfig();
    json topology;
    topology["cpu:0"] = {{"mlx5_0"}, {"mlx5_1"}};
    cfg->set("topology/priority_matrix", topology);
    json failing_policy;
    failing_policy["name"] = "rdma-fail";
    failing_policy["segment_type"] = "memory";
    failing_policy["transports"] = {"rdma"};
    failing_policy["devices"] = {"mlx5_0"};
    failing_policy["qp_pool"] = "pool-fail";
    json succeeding_policy;
    succeeding_policy["name"] = "rdma-ok";
    succeeding_policy["segment_type"] = "memory";
    succeeding_policy["transports"] = {"rdma"};
    succeeding_policy["devices"] = {"mlx5_1"};
    succeeding_policy["qp_pool"] = "pool-ok";
    cfg->set("policy", json::array({failing_policy, succeeding_policy}));

    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::shared_ptr<FakeTransport> fake_rdma, fake_tcp;
    installFakeRdmaAndTcp(engine, fake_rdma, fake_tcp);
    fake_rdma->fail_qp_pool_once = "pool-fail";

    constexpr size_t kRequestLength = 4096;
    constexpr size_t kBufLen = 2 * kRequestLength;
    std::vector<uint8_t> buf(kBufLen, 0xDD);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    Request failing = makeLocalWriteRequest(buf.data(), kRequestLength);
    failing.policy_name = "rdma-fail";
    Request succeeding =
        makeLocalWriteRequest(buf.data() + kRequestLength, kRequestLength);
    succeeding.policy_name = "rdma-ok";

    BatchID batch_id = engine.allocateBatch(2);
    ASSERT_TRUE(engine.submitTransfer(batch_id, {failing, succeeding}).ok());
    ASSERT_EQ(fake_rdma->submit_calls.load(), 2);

    TransferStatus failed_status;
    ASSERT_TRUE(engine.getTransferStatus(batch_id, 0, failed_status).ok());
    EXPECT_EQ(failed_status.s, TransferStatusEnum::FAILED);

    TransferStatus completed_status;
    ASSERT_TRUE(engine.getTransferStatus(batch_id, 1, completed_status).ok());
    EXPECT_EQ(completed_status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
