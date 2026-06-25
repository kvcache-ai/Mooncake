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
// Tests the ProgressWorker skeleton (issue #2116, follow-up to PR #2160).
// Goals:
//   * default-off behavior is byte-identical to the pre-worker world;
//   * with the worker enabled and enable_auto_failover_on_poll=false, a
//     caller that only submits + observes status (never calls
//     progressBatch / waitTransferCompletion) still sees its batch
//     progress through failover;
//   * one notify advances the engine by exactly one progress step;
//   * freeBatch racing the worker, including cross-thread free, is safe;
//   * worker shuts down cleanly on engine destruction.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
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
// FakeTransport — same minimal shape used by engine_failover_e2e_test.cpp.
// Kept local to avoid cross-test linkage; sources of truth diverging is OK
// because we only exercise the "completes / status-can-be-overridden" surface.
// ---------------------------------------------------------------------------

class FakeSubBatch : public Transport::SubBatch {
   public:
    size_t size() const override { return task_count; }
    size_t task_count = 0;
    std::vector<Request> requests;
    std::vector<TransferStatus> statuses;
    std::vector<int> poll_counts;
};

class FakeTransport : public Transport {
   public:
    using StatusFactory = std::function<TransferStatus(const Request&)>;
    using PollStatusFactory =
        std::function<TransferStatus(const Request&, int)>;

    explicit FakeTransport(TransportType self_type,
                           StatusFactory status_factory = {},
                           PollStatusFactory poll_status_factory = {})
        : self_type_(self_type),
          status_factory_(std::move(status_factory)),
          poll_status_factory_(std::move(poll_status_factory)) {
        caps.dram_to_dram = true;
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
            if (status_factory_) {
                fb->statuses.push_back(status_factory_(req));
            } else {
                fb->statuses.push_back(
                    {TransferStatusEnum::COMPLETED, req.length});
            }
            fb->requests.push_back(req);
            fb->poll_counts.push_back(0);
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
        ++fb->poll_counts[task_id];
        if (poll_status_factory_) {
            status = poll_status_factory_(fb->requests[task_id],
                                          fb->poll_counts[task_id]);
        } else {
            status = fb->statuses[task_id];
        }
        return Status::OK();
    }

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
        return false;
    }

    const char* getName() const override {
        return self_type_ == RDMA ? "<fake-rdma>" : "<fake-tcp>";
    }

   private:
    TransportType self_type_;
    StatusFactory status_factory_;
    PollStatusFactory poll_status_factory_;
};

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

    cfg->set("max_failover_attempts", 3);
    return cfg;
}

// ---------------------------------------------------------------------------
// 1. Default config: worker is not constructed, notifyBatchMaybeReady is a
// no-op, and behavior matches PR #2160 exactly.
// ---------------------------------------------------------------------------

TEST(ProgressWorker, DisabledByDefaultLeavesBehaviorUnchanged) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    ASSERT_TRUE(fake_tcp->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);
    engine.swapTransportForTest(TCP, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x10);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(1);
    ASSERT_NE(batch_id, (BatchID)0);

    Request req;
    req.opcode = Request::WRITE;
    req.source = buf.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(buf.data());
    req.length = kBufLen;
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    // No-op when the worker isn't constructed.
    engine.notifyBatchMaybeReady(batch_id);
    engine.notifyBatchMaybeReady((BatchID)0);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 0);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// 2. Worker drives failover when the caller does not poll with
// allow_failover. This is the integration shape mooncake-pg needs.
// ---------------------------------------------------------------------------

TEST(ProgressWorker, ProgressesWithoutPollAutoFailover) {
    auto cfg = makeMinimalP2PConfig();
    cfg->set("enable_auto_failover_on_poll", false);
    cfg->set("enable_progress_worker", true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);

    FaultPolicy rdma_policy;
    rdma_policy.status_corrupt_rate = 1.0;
    auto proxied_rdma =
        std::make_shared<FaultProxyTransport>(fake_rdma, rdma_policy);

    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(proxied_rdma->install(seg, nullptr, nullptr).ok());
    ASSERT_TRUE(fake_tcp->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, proxied_rdma);
    engine.swapTransportForTest(TCP, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0xC1);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(1);
    ASSERT_NE(batch_id, (BatchID)0);

    Request req;
    req.opcode = Request::WRITE;
    req.source = buf.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(buf.data());
    req.length = kBufLen;
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    // Drive the worker until terminal. We deliberately never call
    // progressBatch / waitTransferCompletion here — only
    // notifyBatchMaybeReady + observation-only getTransferStatus
    // (which, with auto-failover-on-poll disabled, will not advance failover
    // by itself).
    TransferStatus status{};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(2000);
    while (std::chrono::steady_clock::now() < deadline) {
        engine.notifyBatchMaybeReady(batch_id);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        if (fake_tcp->submit_calls.load() == 0) continue;
        status = {};
        ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
        if (status.s == TransferStatusEnum::COMPLETED) break;
    }
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED)
        << "progress worker must drive failover when caller never calls "
           "progressBatch";
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_GE(fake_tcp->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// 3. One notify == one progress step. The worker must not loop internally
// until completion; the next step requires another notify.
// ---------------------------------------------------------------------------

TEST(ProgressWorker, SingleNotifyAdvancesOneStep) {
    auto cfg = makeMinimalP2PConfig();
    cfg->set("enable_auto_failover_on_poll", false);
    cfg->set("enable_progress_worker", true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, FakeTransport::StatusFactory{},
        [](const Request& req, int poll_count) {
            if (poll_count < 3) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED, req.length};
        });
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);

    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    ASSERT_TRUE(fake_tcp->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);
    engine.swapTransportForTest(TCP, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0xC2);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(1);
    ASSERT_NE(batch_id, (BatchID)0);

    Request req;
    req.opcode = Request::WRITE;
    req.source = buf.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(buf.data());
    req.length = kBufLen;
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    // Initial state: nothing polled yet.
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    // Drive exactly one progress step via the worker. We can't observe the
    // step instantly, but we can wait until status_calls increments by 1
    // and then assert it does NOT keep climbing to 3 on its own.
    engine.notifyBatchMaybeReady(batch_id);
    const auto step_deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    while (std::chrono::steady_clock::now() < step_deadline &&
           fake_rdma->status_calls.load() == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    ASSERT_EQ(fake_rdma->status_calls.load(), 1)
        << "worker should issue exactly one poll for a single notify";

    // Give the worker a generous window to misbehave. status_calls must
    // stay at 1 because we did not notify again and the engine did not
    // reach a terminal state.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(fake_rdma->status_calls.load(), 1)
        << "worker must not loop internally until completion";

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// 4. freeBatch races with worker notifications. With ASAN/UBSAN this
// catches missing alive_batches_ / progress_mutex_ coverage.
// ---------------------------------------------------------------------------

TEST(ProgressWorker, FreeBatchRacesWithWorker) {
    auto cfg = makeMinimalP2PConfig();
    cfg->set("enable_auto_failover_on_poll", false);
    cfg->set("enable_progress_worker", true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    ASSERT_TRUE(fake_tcp->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);
    engine.swapTransportForTest(TCP, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0xC3);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    // Concurrently spam stale notifications from a second thread while the
    // main thread submits, frees, and re-allocates batches.
    std::atomic<bool> stop{false};
    std::atomic<BatchID> latest{0};
    std::thread spammer([&] {
        while (!stop.load(std::memory_order_acquire)) {
            BatchID bid = latest.load(std::memory_order_acquire);
            engine.notifyBatchMaybeReady(bid);
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    });

    constexpr int kRounds = 100;
    for (int i = 0; i < kRounds; ++i) {
        BatchID batch_id = engine.allocateBatch(1);
        ASSERT_NE(batch_id, (BatchID)0);

        Request req;
        req.opcode = Request::WRITE;
        req.source = buf.data();
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = reinterpret_cast<uint64_t>(buf.data());
        req.length = kBufLen;
        ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

        latest.store(batch_id, std::memory_order_release);
        engine.notifyBatchMaybeReady(batch_id);

        // Free immediately; the worker may pick the notification up after
        // free. The progress_mutex_ + alive_batches_ guard must keep this
        // safe.
        EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    }

    stop.store(true, std::memory_order_release);
    spammer.join();

    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

TEST(ProgressWorker, BatchMayBeFreedFromDifferentThread) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0xC4);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = 0;
    std::thread allocator([&] { batch_id = engine.allocateBatch(1); });
    allocator.join();
    ASSERT_NE(batch_id, (BatchID)0);

    Request req;
    req.opcode = Request::WRITE;
    req.source = buf.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(buf.data());
    req.length = kBufLen;
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);

    std::atomic<bool> free_ok{false};
    std::thread freer([&] { free_ok.store(engine.freeBatch(batch_id).ok()); });
    freer.join();
    EXPECT_TRUE(free_ok.load());
    EXPECT_TRUE(engine.getTransferStatus(batch_id, status).IsInvalidArgument());

    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// ---------------------------------------------------------------------------
// 5. Engine teardown joins the worker cleanly even with pending notifies.
// ---------------------------------------------------------------------------

TEST(ProgressWorker, EngineDestructorJoinsWorker) {
    auto cfg = makeMinimalP2PConfig();
    cfg->set("enable_auto_failover_on_poll", false);
    cfg->set("enable_progress_worker", true);
    {
        TransferEngineImpl engine(cfg);
        ASSERT_TRUE(engine.available());

        auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
        auto fake_tcp = std::make_shared<FakeTransport>(TCP);
        std::string seg = engine.getSegmentName();
        ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
        ASSERT_TRUE(fake_tcp->install(seg, nullptr, nullptr).ok());
        engine.swapTransportForTest(RDMA, fake_rdma);
        engine.swapTransportForTest(TCP, fake_tcp);

        // Push some notifies for non-existent batches; worker must reject
        // them via alive_batches_ check and stay alive.
        for (int i = 0; i < 8; ++i) {
            engine.notifyBatchMaybeReady((BatchID)(uintptr_t)0xdeadbeef);
        }
    }
    // If teardown hangs or crashes here, gtest fails this test on timeout
    // / signal — no further assert needed.
    SUCCEED();
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
