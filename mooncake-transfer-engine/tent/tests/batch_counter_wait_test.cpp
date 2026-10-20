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
// TENT analogue of classic TE PR #4199: RDMA-only batches skip the owner
// walk in getBatchStatus while the CQ/worker counter says the batch is
// still in flight. Notify uses the same batch-complete timing: an earlier
// interval's hook waits until the publisher is fully done. Mixed RDMA+TCP,
// append of a non-publisher, rejected submit (UNSPEC / submitTransferTasks
// failure), and failover onto TCP keep the walk — those paths cannot use
// the CQ counter.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
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
    std::vector<Request> requests;
    std::vector<TransferStatus> statuses;
};

class FakeTransport : public Transport {
   public:
    explicit FakeTransport(TransportType self_type,
                           bool publishes_counter = false)
        : self_type_(self_type), publishes_counter_(publishes_counter) {
        caps.dram_to_dram = true;
    }

    std::atomic<int> submit_calls{0};
    std::atomic<int> status_calls{0};
    std::atomic<bool> fail_next_submit{false};
    FakeSubBatch* last_batch{nullptr};

    TransportType type() const { return self_type_; }

    Status install(std::string& /*local_segment_name*/,
                   std::shared_ptr<ControlService> /*metadata*/,
                   std::shared_ptr<Topology> /*local_topology*/,
                   std::shared_ptr<Config> /*conf*/ = nullptr) override {
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t /*max_size*/) override {
        batch = new FakeSubBatch();
        last_batch = static_cast<FakeSubBatch*>(batch);
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        if (last_batch == batch) last_batch = nullptr;
        delete batch;
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override {
        ++submit_calls;
        if (fail_next_submit.exchange(false)) {
            return Status::InternalError(
                "FakeTransport forced submit failure" LOC_MARK);
        }
        auto* fb = static_cast<FakeSubBatch*>(batch);
        last_batch = fb;
        for (const auto& req : request_list) {
            fb->statuses.push_back({TransferStatusEnum::PENDING, 0});
            fb->requests.push_back(req);
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

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& /*options*/) override {
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

    const char* getName() const override {
        return self_type_ == RDMA ? "<fake-rdma>" : "<fake-tcp>";
    }

    bool publishesBatchCompletionCounter() const override {
        return publishes_counter_;
    }

    void completeAllSuccess() {
        ASSERT_NE(last_batch, nullptr);
        for (size_t i = 0; i < last_batch->statuses.size(); ++i) {
            last_batch->statuses[i] = {TransferStatusEnum::COMPLETED,
                                       last_batch->requests[i].length};
        }
        last_batch->finished_task_count.store(last_batch->size(),
                                              std::memory_order_release);
    }

    void completeOneSuccess(size_t index) {
        ASSERT_NE(last_batch, nullptr);
        ASSERT_LT(index, last_batch->statuses.size());
        last_batch->statuses[index] = {TransferStatusEnum::COMPLETED,
                                       last_batch->requests[index].length};
        last_batch->finished_task_count.fetch_add(1, std::memory_order_release);
    }

    void publishFailure(size_t index) {
        ASSERT_NE(last_batch, nullptr);
        ASSERT_LT(index, last_batch->statuses.size());
        last_batch->statuses[index] = {TransferStatusEnum::FAILED, 0};
        last_batch->has_failure.store(true, std::memory_order_release);
    }

   private:
    TransportType self_type_;
    bool publishes_counter_;
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
    cfg->set("enable_auto_failover_on_poll", false);
    cfg->set("max_failover_attempts", 0);
    return cfg;
}

Request makeLocalWrite(void* buf, size_t len, TransportType hint = UNSPEC) {
    Request req;
    req.opcode = Request::WRITE;
    req.source = buf;
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(buf);
    req.length = len;
    req.transport_hint = hint;
    return req;
}

void installFake(TransferEngineImpl& engine,
                 const std::shared_ptr<FakeTransport>& fake) {
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(fake->type(), fake);
}

TEST(BatchCounterWait, InFlightSkipThenWalkOnCompletion) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x11);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);
    ASSERT_NE(batch_id, (BatchID)0);
    auto req = makeLocalWrite(buf.data(), kBufLen);
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req, req}).ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    fake_rdma->completeOneSuccess(0);
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    fake_rdma->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 2 * kBufLen);
    EXPECT_EQ(fake_rdma->status_calls.load(), 2);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

TEST(BatchCounterWait, NonPublisherKeepsOwnerWalk) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, false);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x22);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(1);
    ASSERT_NE(batch_id, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(batch_id, {makeLocalWrite(buf.data(), kBufLen)})
            .ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_GE(fake_rdma->status_calls.load(), 1);

    fake_rdma->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

TEST(BatchCounterWait, FailureDoesNotReportSuccess) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x33);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);
    auto req = makeLocalWrite(buf.data(), kBufLen);
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req, req}).ok());

    fake_rdma->completeOneSuccess(0);
    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    fake_rdma->publishFailure(1);
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_GE(fake_rdma->status_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

TEST(BatchCounterWait, NotifyBatchSkipsInFlightAndFiresOnCompletion) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x44);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    Notification notifi;
    notifi.name = "local";
    notifi.msg = "batch-counter";

    BatchID batch_id = engine.allocateBatch(1);
    ASSERT_TRUE(engine
                    .submitTransfer(
                        batch_id, {makeLocalWrite(buf.data(), kBufLen)}, notifi)
                    .ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    fake_rdma->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->status_calls.load(), 1);

    std::vector<Notification> received;
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "batch-counter");

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

TEST(BatchCounterWait, NotifyWaitsForFullPublisherAfterAppend) {
    // TE #4199: notify is batch-complete, not interval-complete. Submit A
    // with notify, append B, finish only A — still PENDING, bytes 0, A's
    // hook must not fire. Finish B — hook fires on the completion walk.
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x46);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    Notification notifi;
    notifi.name = "local";
    notifi.msg = "interval-a";

    BatchID batch_id = engine.allocateBatch(2);
    auto req = makeLocalWrite(buf.data(), kBufLen);
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}, notifi).ok());
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    fake_rdma->completeOneSuccess(0);
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    // FakeTransport does not implement receiveNotification. An empty poll
    // therefore reports "Notification not supported" unless sendNotification
    // already queued a local payload. The pin is that A's hook has not
    // delivered anything yet.
    std::vector<Notification> received;
    (void)engine.receiveNotification(received);
    EXPECT_TRUE(received.empty());

    fake_rdma->completeOneSuccess(1);
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->status_calls.load(), 2);

    received.clear();
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "interval-a");

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

TEST(BatchCounterWait, AppendAfterCompletionUsesCounterAgain) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    std::string seg = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x55);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);
    auto req = makeLocalWrite(buf.data(), kBufLen);
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());
    fake_rdma->completeAllSuccess();
    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    const int calls_after_first = fake_rdma->status_calls.load();

    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(fake_rdma->status_calls.load(), calls_after_first);

    fake_rdma->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// TCP (and any other non-publisher) progress is driven by getTransferStatus.
// A mixed batch must keep walking even when the RDMA sub-batch publishes.
TEST(BatchCounterWait, MixedRdmaAndTcpKeepsOwnerWalk) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP, false);
    installFake(engine, fake_rdma);
    installFake(engine, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x61);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);
    ASSERT_TRUE(engine
                    .submitTransfer(batch_id,
                                    {makeLocalWrite(buf.data(), kBufLen, RDMA),
                                     makeLocalWrite(buf.data(), kBufLen, TCP)})
                    .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 1);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_GE(fake_rdma->status_calls.load(), 1);
    EXPECT_GE(fake_tcp->status_calls.load(), 1);

    fake_rdma->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, kBufLen);

    fake_tcp->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 2 * kBufLen);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// First submit is publisher-only and eligible. Appending TCP must drop the
// shortcut so later in-flight polls walk both owners.
TEST(BatchCounterWait, AppendNonPublisherDisablesShortcut) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP, false);
    installFake(engine, fake_rdma);
    installFake(engine, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x62);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);
    ASSERT_TRUE(engine
                    .submitTransfer(batch_id,
                                    {makeLocalWrite(buf.data(), kBufLen, RDMA)})
                    .ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 0);

    ASSERT_TRUE(engine
                    .submitTransfer(batch_id,
                                    {makeLocalWrite(buf.data(), kBufLen, TCP)})
                    .ok());
    EXPECT_EQ(fake_tcp->submit_calls.load(), 1);

    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_GE(fake_rdma->status_calls.load(), 1);
    EXPECT_GE(fake_tcp->status_calls.load(), 1);

    fake_rdma->completeAllSuccess();
    fake_tcp->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// Failed submitTransferTasks does not append owners, so the CQ counter still
// looks in-flight. Eligibility must drop or getBatchStatus would skip forever.
TEST(BatchCounterWait, FailedSubmitDisablesShortcut) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    installFake(engine, fake_rdma);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x63);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);
    auto req = makeLocalWrite(buf.data(), kBufLen, RDMA);
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    fake_rdma->fail_next_submit = true;
    ASSERT_TRUE(engine.submitTransfer(batch_id, {req}).ok());

    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_GE(fake_rdma->status_calls.load(), 1);

    fake_rdma->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// UNSPEC owners have no SubBatch, so the mixed-publisher check cannot see
// them. Only counter_eligible=false prevents an in-flight skip.
TEST(BatchCounterWait, UnspecSubmitDisablesShortcut) {
    auto cfg = makeMinimalP2PConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    installFake(engine, fake_rdma);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x64);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(2);
    ASSERT_TRUE(engine
                    .submitTransfer(batch_id,
                                    {makeLocalWrite(buf.data(), kBufLen, RDMA)})
                    .ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    Request missing = makeLocalWrite(buf.data(), kBufLen, RDMA);
    missing.target_offset = 0x100000000ULL;
    ASSERT_TRUE(engine.submitTransfer(batch_id, {missing}).ok());

    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_GE(fake_rdma->status_calls.load(), 1);

    fake_rdma->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

// Poll-time failover onto TCP must keep walking: TCP does not publish, and
// the surviving RDMA sub-batch's CQ counter is no longer the live transport.
TEST(BatchCounterWait, FailoverFromPublisherToTcpKeepsOwnerWalk) {
    auto cfg = makeMinimalP2PConfig();
    cfg->set("enable_auto_failover_on_poll", true);
    cfg->set("max_failover_attempts", 3);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, true);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP, false);
    installFake(engine, fake_rdma);
    installFake(engine, fake_tcp);

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> buf(kBufLen, 0x65);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), kBufLen).ok());

    BatchID batch_id = engine.allocateBatch(1);
    ASSERT_TRUE(engine
                    .submitTransfer(batch_id,
                                    {makeLocalWrite(buf.data(), kBufLen, RDMA)})
                    .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 0);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_rdma->status_calls.load(), 0);

    fake_rdma->publishFailure(0);
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_GE(fake_rdma->status_calls.load(), 1);
    EXPECT_GE(fake_tcp->submit_calls.load(), 1);

    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_GE(fake_tcp->status_calls.load(), 1);

    fake_tcp->completeAllSuccess();
    status = {};
    ASSERT_TRUE(engine.getTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kBufLen);

    EXPECT_TRUE(engine.freeBatch(batch_id).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), kBufLen).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
