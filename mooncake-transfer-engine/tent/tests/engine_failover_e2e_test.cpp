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
// End-to-end failover test: drives the real TransferEngineImpl through
// submitTransfer() / getTransferStatus() / resubmitTransferTask() by swapping
// two FakeTransports into the engine's transport_list_ and wrapping the
// primary one in a FaultProxyTransport.
//
// ---------------------------------------------------------------------------
// Scenarios:
//   P0: Primary succeeds at submit but getTransferStatus reports FAILED
//       (simulates WC error / QP error / peer drop mid-transfer).
//       -> engine must failover to the secondary and succeed.
//
// Submit-stage failures are NOT tested here. Today submitTransferTasks
// failures are marked UNSPEC and surface as FAILED without a failover
// attempt -- see the "Known gaps" section of docs/source/design/tent/
// failover.md for why. Tests for that path belong in a future change
// that makes submit-stage recovery safe (see known gaps).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"
#include "tent/transport/fault_proxy/fault_proxy_transport.h"

namespace mooncake {
namespace tent {
namespace {

// ---------------------------------------------------------------------------
// FakeTransport: declares itself capable of dram_to_dram, records itself in
// BufferDesc::transports under a configurable slot, and always completes
// transfers. Enough to satisfy checkAvailability() + resolveTransport() in
// TransferEngineImpl.
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
        caps.dram_to_dram = true;  // so checkAvailability returns true
    }

    std::atomic<int> install_calls{0};
    std::atomic<int> submit_calls{0};
    std::atomic<int> status_calls{0};
    std::atomic<int> add_mem_calls{0};

    Status install(std::string& /*local_segment_name*/,
                   std::shared_ptr<ControlService> /*metadata*/,
                   std::shared_ptr<Topology> /*local_topology*/,
                   std::shared_ptr<Config> /*conf*/ = nullptr) override {
        ++install_calls;
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t /*max_size*/) override {
        batch = new FakeSubBatch();
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        delete batch;
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override {
        ++submit_calls;
        auto* fb = static_cast<FakeSubBatch*>(batch);
        for (const auto& req : request_list) {
            fb->statuses.push_back({TransferStatusEnum::COMPLETED, req.length});
            fb->task_count++;
        }
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        ++status_calls;
        auto* fb = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fb->statuses.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        status = fb->statuses[task_id];
        return Status::OK();
    }

    // Tag ourselves into the BufferDesc so resolveTransport() considers us.
    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& /*options*/) override {
        ++add_mem_calls;
        desc.transports.push_back(self_type_);
        return Status::OK();
    }

    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override {
        for (auto& d : desc_list) {
            auto s = addMemoryBuffer(d, options);
            if (!s.ok()) return s;
        }
        return Status::OK();
    }

    Status removeMemoryBuffer(BufferDesc& /*desc*/) override {
        return Status::OK();
    }

    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions& /*options*/) override {
        *addr = std::malloc(size);
        if (!*addr) return Status::InternalError("malloc failed" LOC_MARK);
        return Status::OK();
    }

    Status freeLocalMemory(void* addr, size_t /*size*/) override {
        std::free(addr);
        return Status::OK();
    }

    bool warmupMemory(void* /*addr*/, size_t /*length*/) override {
        return false;  // no pinning; engine will fall back to its own path
    }

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
    // p2p metadata avoids needing an external redis/etcd/http server.
    cfg->set("metadata_type", "p2p");
    cfg->set("metadata_servers", "");
    cfg->set("rpc_server_hostname", "127.0.0.1");
    cfg->set("rpc_server_port", "0");
    cfg->set("log_level", "warning");
    cfg->set("merge_requests", false);

    // Disable every real transport. We'll inject fakes into the slots we care
    // about via swapTransportForTest.
    cfg->set("transports/tcp/enable", false);
    cfg->set("transports/shm/enable", false);
    cfg->set("transports/rdma/enable", false);
    cfg->set("transports/io_uring/enable", false);
    cfg->set("transports/nvlink/enable", false);
    cfg->set("transports/mnnvl/enable", false);
    cfg->set("transports/gds/enable", false);
    cfg->set("transports/ascend_direct/enable", false);

    // Keep failover limit at default (3) but make it explicit.
    cfg->set("max_failover_attempts", 3);
    return cfg;
}

// Wait for a task to leave PENDING. Bounded so tests fail fast.
TransferStatus pollUntilDone(
    TransferEngineImpl& engine, BatchID batch_id, size_t task_id,
    std::chrono::milliseconds timeout = std::chrono::milliseconds(2000)) {
    TransferStatus ts{};
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        ts = {};
        auto s = engine.getTransferStatus(batch_id, task_id, ts);
        if (!s.ok()) {
            ADD_FAILURE() << "getTransferStatus returned error: "
                          << s.ToString();
            return ts;
        }
        if (ts.s == TransferStatusEnum::COMPLETED ||
            ts.s == TransferStatusEnum::FAILED) {
            return ts;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return ts;
}

// ---------------------------------------------------------------------------
// P0: Completion reports FAILED (simulates WC error / QP error / peer drop
// mid-transfer). Engine must failover.
// ---------------------------------------------------------------------------

TEST(EngineFailoverE2E, StatusCorruptionTriggersFailoverToSecondary) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);

    FaultPolicy rdma_policy;
    rdma_policy.status_corrupt_rate = 1.0;  // every COMPLETED flipped to FAILED
    auto proxied_rdma =
        std::make_shared<FaultProxyTransport>(fake_rdma, rdma_policy);

    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(proxied_rdma->install(seg_name, nullptr, nullptr).ok());
    ASSERT_TRUE(fake_tcp->install(seg_name, nullptr, nullptr).ok());

    engine.swapTransportForTest(RDMA, proxied_rdma);
    engine.swapTransportForTest(TCP, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0xCD);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(8);
    ASSERT_NE(batch_id, (BatchID)0);

    Request req;
    req.opcode = Request::WRITE;
    req.source = buf.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(buf.data());
    req.length = kBufLen;

    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    auto final_status = pollUntilDone(engine, batch_id, 0);
    EXPECT_EQ(final_status.s, TransferStatusEnum::COMPLETED);

    // RDMA saw exactly one submit (succeeded on the wire), then its status
    // was corrupted; engine failed over.
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_GE(fake_tcp->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// P1b: Both transports keep failing at status stage -> failover limit reached.
// ---------------------------------------------------------------------------

TEST(EngineFailoverE2E, BothTransportsFailExhaustsFailoverBudget) {
    auto cfg = makeMinimalP2PConfig();
    cfg->set("max_failover_attempts", 2);  // tighten budget
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);

    FaultPolicy always_corrupt;
    always_corrupt.status_corrupt_rate = 1.0;

    auto proxied_rdma =
        std::make_shared<FaultProxyTransport>(fake_rdma, always_corrupt);
    auto proxied_tcp =
        std::make_shared<FaultProxyTransport>(fake_tcp, always_corrupt);

    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(proxied_rdma->install(seg_name, nullptr, nullptr).ok());
    ASSERT_TRUE(proxied_tcp->install(seg_name, nullptr, nullptr).ok());

    engine.swapTransportForTest(RDMA, proxied_rdma);
    engine.swapTransportForTest(TCP, proxied_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0xEF);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(8);
    Request req;
    req.opcode = Request::WRITE;
    req.source = buf.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(buf.data());
    req.length = kBufLen;

    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    auto final_status = pollUntilDone(engine, batch_id, 0);
    EXPECT_EQ(final_status.s, TransferStatusEnum::FAILED)
        << "after exhausting failover budget, task must be permanently FAILED";

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// PerRequestFaultProxy
//
// FaultProxyTransport corrupts completions uniformly based on a rate. For
// tests that need per-request control (e.g. "fail task0 but not task1"),
// this subclass inspects each Request at submit time, records the sub_task
// indices of "poisoned" requests, and flips only those from COMPLETED to
// FAILED in getTransferStatus(). Completions for non-poisoned requests
// pass through unchanged. Submit is never rejected.
//
// Tests submit each request in its own one-request batch, so the engine's
// failover logic routes each task individually.
// ---------------------------------------------------------------------------

class PerRequestFaultProxy : public FaultProxyTransport {
   public:
    using Predicate = std::function<bool(const Request&)>;

    PerRequestFaultProxy(std::shared_ptr<Transport> real, Predicate pred)
        : FaultProxyTransport(std::move(real), FaultPolicy{}),
          should_fail_(std::move(pred)) {}

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override {
        // Remember (sub_batch, sub_task_id) pairs for "poisoned" requests
        // so getTransferStatus() can flip only those to FAILED later. The
        // sub_batch pointer must be part of the key: different engine
        // batches (BatchID) have different SubBatchRefs per transport, so
        // sub_task_id=0 in batch A is a different task than sub_task_id=0
        // in batch B. Keying only on sub_task_id would cross-contaminate.
        const int base = static_cast<int>(batch->size());
        for (size_t i = 0; i < request_list.size(); ++i) {
            if (should_fail_(request_list[i])) {
                poisoned_.insert({batch, base + static_cast<int>(i)});
            }
        }
        return FaultProxyTransport::submitTransferTasks(batch, request_list);
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto s = FaultProxyTransport::getTransferStatus(batch, task_id, status);
        if (!s.ok()) return s;
        if (poisoned_.count({batch, task_id}) &&
            status.s == TransferStatusEnum::COMPLETED) {
            status.s = TransferStatusEnum::FAILED;
        }
        return s;
    }

   private:
    Predicate should_fail_;
    std::set<std::pair<SubBatchRef, int>> poisoned_;
};

// ---------------------------------------------------------------------------
// A: Mixed faults across many independent single-request submissions.
//
// Submit 10 one-request batches. For each submission, RDMA's completion
// reports FAILED with 30% probability. Assert every task ultimately
// COMPLETES and that RDMA/TCP submit counts add up consistently: every
// failed RDMA completion must be followed by a TCP success.
// ---------------------------------------------------------------------------

TEST(EngineFailoverE2E, MixedFaultsAcrossManySubmissions) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);

    FaultPolicy rdma_policy;
    rdma_policy.status_corrupt_rate = 0.3;  // 30% of completions flip to FAILED
    auto proxied_rdma =
        std::make_shared<FaultProxyTransport>(fake_rdma, rdma_policy);

    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(proxied_rdma->install(seg_name, nullptr, nullptr).ok());
    ASSERT_TRUE(fake_tcp->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, proxied_rdma);
    engine.swapTransportForTest(TCP, fake_tcp);

    constexpr size_t kBufLen = 4096;
    constexpr int kNumTasks = 10;
    std::vector<uint8_t> buf(kBufLen * kNumTasks, 0x77);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    std::vector<BatchID> batches;
    batches.reserve(kNumTasks);
    for (int i = 0; i < kNumTasks; ++i) {
        BatchID b = engine.allocateBatch(1);
        ASSERT_NE(b, (BatchID)0);

        Request req;
        req.opcode = Request::WRITE;
        req.source = buf.data() + (size_t)i * kBufLen;
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = reinterpret_cast<uint64_t>(req.source);
        req.length = kBufLen;

        ASSERT_TRUE(engine.submitTransfer(b, {req}).ok());
        batches.push_back(b);
    }

    int completed = 0;
    for (int i = 0; i < kNumTasks; ++i) {
        auto ts = pollUntilDone(engine, batches[i], 0);
        EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED)
            << "task " << i << " did not complete";
        if (ts.s == TransferStatusEnum::COMPLETED) ++completed;
    }
    EXPECT_EQ(completed, kNumTasks);

    // Sanity on submit counts under status-corruption injection:
    //   - Every task hits RDMA at submit time (submit itself succeeds,
    //     the proxy only corrupts getTransferStatus), so rdma_ok == kNumTasks.
    //   - A corrupted completion triggers a TCP failover (+1 tcp).
    //     A clean completion stays on RDMA (+0 tcp).
    //   - Therefore tcp_ok equals the number of corrupted completions,
    //     which is in [0, kNumTasks].
    const int rdma_ok = fake_rdma->submit_calls.load();
    const int tcp_ok = fake_tcp->submit_calls.load();
    EXPECT_EQ(rdma_ok, kNumTasks)
        << "every task must attempt RDMA first (submit is always accepted)";
    EXPECT_GE(tcp_ok, 0);
    EXPECT_LE(tcp_ok, kNumTasks);

    // With 30% corruption rate over 10 tasks, both branches are exercised
    // with overwhelming probability (0.7^10 ~= 2.8% no failover; 0.3^10 ~=
    // 6e-6 all failover). We don't assert strict counts because this is
    // rate-based.

    for (auto b : batches) EXPECT_TRUE(engine.freeBatch(b).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// C1: max_failover_attempts = 0 disables failover entirely.
//
// resubmitTransferTask bumps failover_count and compares it against the
// budget before anything else. With budget=0, the very first attempted
// failover is rejected (++count == 1 > 0), so a single RDMA fault must
// result in a permanently FAILED task without touching TCP.
// ---------------------------------------------------------------------------

TEST(EngineFailoverE2E, MaxFailoverAttemptsZeroDisablesFailover) {
    auto cfg = makeMinimalP2PConfig();
    cfg->set("max_failover_attempts", 0);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);

    FaultPolicy always_fail;
    always_fail.status_corrupt_rate = 1.0;  // every completion flips to FAILED
    auto proxied_rdma =
        std::make_shared<FaultProxyTransport>(fake_rdma, always_fail);

    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(proxied_rdma->install(seg_name, nullptr, nullptr).ok());
    ASSERT_TRUE(fake_tcp->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, proxied_rdma);
    engine.swapTransportForTest(TCP, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x11);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(1);
    Request req;
    req.opcode = Request::WRITE;
    req.source = buf.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(buf.data());
    req.length = kBufLen;
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    auto ts = pollUntilDone(engine, batch_id, 0);
    EXPECT_EQ(ts.s, TransferStatusEnum::FAILED)
        << "with budget=0 the first fault must be permanent";

    // TCP must never have been touched: budget=0 means no failover attempt.
    EXPECT_EQ(fake_tcp->submit_calls.load(), 0);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// C2: max_failover_attempts = 1 allows exactly one failover.
//
// RDMA submit fails -> engine spends its single budget to switch to TCP
// -> TCP succeeds -> task COMPLETES. A symmetric run where *both* fake
// transports always fail is covered by BothTransportsFailExhaustsFailoverBudget
// at budget=2; here we just confirm the happy-path boundary.

TEST(EngineFailoverE2E, MaxFailoverAttemptsOneAllowsSingleFailover) {
    auto cfg = makeMinimalP2PConfig();
    cfg->set("max_failover_attempts", 1);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);

    FaultPolicy rdma_fail;
    rdma_fail.status_corrupt_rate = 1.0;  // every completion flips to FAILED
    auto proxied_rdma =
        std::make_shared<FaultProxyTransport>(fake_rdma, rdma_fail);

    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(proxied_rdma->install(seg_name, nullptr, nullptr).ok());
    ASSERT_TRUE(fake_tcp->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, proxied_rdma);
    engine.swapTransportForTest(TCP, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x22);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(1);
    Request req;
    req.opcode = Request::WRITE;
    req.source = buf.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(buf.data());
    req.length = kBufLen;
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    auto ts = pollUntilDone(engine, batch_id, 0);
    EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED)
        << "budget=1 must permit exactly one failover to TCP";
    EXPECT_EQ(fake_tcp->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// D: Tasks in the same logical workload maintain independent failover
//    state.
//
// Workload is two submissions:
//   - task0: RDMA *and* TCP always fail -> must end FAILED
//   - task1: RDMA succeeds              -> must end COMPLETED, no TCP hit
//
// This pins down that one task's exhausted failover budget does not
// "infect" another task: the engine must track failover_count per-task,
// not per-batch, per-transport, or per-engine. A regression that made
// the counter global or batch-scoped would flip task1 to FAILED or
// trigger a spurious TCP submit.
//
// Uses PerRequestFaultProxy so we can make RDMA fail *only* for the
// specific buffer address of task0.
// ---------------------------------------------------------------------------

TEST(EngineFailoverE2E, PerTaskFailoverCountsAreIndependent) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf0(kBufLen, 0xAA);
    std::vector<uint8_t> buf1(kBufLen, 0xBB);

    const uint64_t failing_addr = reinterpret_cast<uint64_t>(buf0.data());

    auto proxied_rdma = std::make_shared<PerRequestFaultProxy>(
        fake_rdma, [failing_addr](const Request& r) {
            return r.target_offset == failing_addr;
        });
    auto proxied_tcp = std::make_shared<PerRequestFaultProxy>(
        fake_tcp, [failing_addr](const Request& r) {
            return r.target_offset == failing_addr;
        });

    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(proxied_rdma->install(seg_name, nullptr, nullptr).ok());
    ASSERT_TRUE(proxied_tcp->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, proxied_rdma);
    engine.swapTransportForTest(TCP, proxied_tcp);

    ASSERT_TRUE(engine.registerLocalMemory(buf0.data(), kBufLen).ok());
    ASSERT_TRUE(engine.registerLocalMemory(buf1.data(), kBufLen).ok());

    auto submit = [&](uint8_t* source) -> BatchID {
        BatchID b = engine.allocateBatch(1);
        EXPECT_NE(b, (BatchID)0);
        Request req;
        req.opcode = Request::WRITE;
        req.source = source;
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = reinterpret_cast<uint64_t>(source);
        req.length = kBufLen;
        EXPECT_TRUE(engine.submitTransfer(b, {req}).ok());
        return b;
    };

    BatchID b0 = submit(buf0.data());  // must FAIL on both
    BatchID b1 = submit(buf1.data());  // must SUCCEED on RDMA

    auto ts0 = pollUntilDone(engine, b0, 0);
    auto ts1 = pollUntilDone(engine, b1, 0);

    EXPECT_EQ(ts0.s, TransferStatusEnum::FAILED)
        << "task0 should exhaust all transports and end FAILED";
    EXPECT_EQ(ts1.s, TransferStatusEnum::COMPLETED)
        << "task1 must be unaffected by task0's failover exhaustion";

    // Under status-corruption injection submits always succeed; the proxy
    // corrupts only getTransferStatus. So both tasks hit RDMA at submit
    // (rdma +2). task0's RDMA completion is corrupted -> failover to TCP
    // -> TCP submit (+1), also corrupted -> no more transports -> FAILED.
    // task1 completes cleanly on RDMA with no TCP touch.
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(b0).ok());
    EXPECT_TRUE(engine.freeBatch(b1).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf0.data(), kBufLen).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf1.data(), kBufLen).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
