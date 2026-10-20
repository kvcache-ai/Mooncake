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
// walk in getBatchStatus while any submit interval is still unfinished.
// Notify stays on TENT #4001 interval timing. The skip path evaluates hooks
// from interval counters only — it must not scan TaskInfo.

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
    std::vector<std::atomic<uint64_t>*> task_finished_counters;

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
        task_finished_counters.clear();
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
        auto* counter = fb->publish_finished_counter
                            ? fb->publish_finished_counter
                            : &fb->finished_task_count;
        for (const auto& req : request_list) {
            fb->statuses.push_back({TransferStatusEnum::PENDING, 0});
            fb->requests.push_back(req);
            fb->task_count++;
            task_finished_counters.push_back(counter);
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
            if (last_batch->statuses[i].s == TransferStatusEnum::COMPLETED) {
                continue;
            }
            last_batch->statuses[i] = {TransferStatusEnum::COMPLETED,
                                       last_batch->requests[i].length};
            if (i < task_finished_counters.size() &&
                task_finished_counters[i]) {
                task_finished_counters[i]->fetch_add(1,
                                                     std::memory_order_release);
            }
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
        if (index < task_finished_counters.size() &&
            task_finished_counters[index]) {
            task_finished_counters[index]->fetch_add(1,
                                                     std::memory_order_release);
        }
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

class BatchCounterWaitTest : public ::testing::Test {
   protected:
    static constexpr size_t kBufLen = 4096;

    void TearDown() override {
        if (engine_ && batch_id_) {
            EXPECT_TRUE(engine_->freeBatch(batch_id_).ok());
            batch_id_ = 0;
        }
        if (engine_ && registered_) {
            EXPECT_TRUE(
                engine_->unregisterLocalMemory(buf_.data(), kBufLen).ok());
        }
    }

    void Start(bool failover = false) {
        cfg_ = makeMinimalP2PConfig();
        if (failover) {
            cfg_->set("enable_auto_failover_on_poll", true);
            cfg_->set("max_failover_attempts", 3);
        }
        engine_ = std::make_unique<TransferEngineImpl>(cfg_);
        ASSERT_TRUE(engine_->available());
        buf_.assign(kBufLen, 0x11);
    }

    void RegisterMem() {
        if (registered_) return;
        ASSERT_TRUE(engine_->registerLocalMemory(buf_.data(), kBufLen).ok());
        registered_ = true;
    }

    std::shared_ptr<FakeTransport> AddRdma(bool publishes) {
        auto fake = std::make_shared<FakeTransport>(RDMA, publishes);
        installFake(*engine_, fake);
        rdma_ = fake;
        return fake;
    }

    std::shared_ptr<FakeTransport> AddTcp() {
        auto fake = std::make_shared<FakeTransport>(TCP, false);
        installFake(*engine_, fake);
        tcp_ = fake;
        return fake;
    }

    Request Req(TransportType hint = UNSPEC) {
        return makeLocalWrite(buf_.data(), kBufLen, hint);
    }

    static Notification LocalNotify(const char* msg) {
        Notification n;
        n.name = "local";
        n.msg = msg;
        return n;
    }

    void Alloc(size_t n) {
        RegisterMem();
        batch_id_ = engine_->allocateBatch(n);
        ASSERT_NE(batch_id_, (BatchID)0);
    }

    void Submit(std::vector<Request> reqs,
                const Notification* notify = nullptr) {
        if (notify) {
            ASSERT_TRUE(engine_->submitTransfer(batch_id_, reqs, *notify).ok());
        } else {
            ASSERT_TRUE(engine_->submitTransfer(batch_id_, reqs).ok());
        }
    }

    TransferStatus Poll() {
        TransferStatus status{};
        EXPECT_TRUE(engine_->getTransferStatus(batch_id_, status).ok());
        return status;
    }

    void ExpectPendingSkip(int status_calls = 0) {
        auto status = Poll();
        EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
        EXPECT_EQ(status.transferred_bytes, 0u);
        ASSERT_NE(rdma_, nullptr);
        EXPECT_EQ(rdma_->status_calls.load(), status_calls);
        EXPECT_EQ(engine_->hookOwnerScansForTest(), 0u);
    }

    std::vector<Notification> Recv() {
        std::vector<Notification> received;
        EXPECT_TRUE(engine_->receiveNotification(received).ok());
        return received;
    }

    std::shared_ptr<Config> cfg_;
    std::unique_ptr<TransferEngineImpl> engine_;
    std::vector<uint8_t> buf_;
    bool registered_{false};
    BatchID batch_id_{0};
    std::shared_ptr<FakeTransport> rdma_;
    std::shared_ptr<FakeTransport> tcp_;
};

TEST_F(BatchCounterWaitTest, InFlightSkipThenWalkOnCompletion) {
    Start();
    AddRdma(true);
    Alloc(2);
    Submit({Req(), Req()});
    ExpectPendingSkip();

    rdma_->completeOneSuccess(0);
    ExpectPendingSkip();

    rdma_->completeAllSuccess();
    auto status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 2 * kBufLen);
    EXPECT_EQ(rdma_->status_calls.load(), 2);
}

TEST_F(BatchCounterWaitTest, FailureDoesNotReportSuccess) {
    Start();
    AddRdma(true);
    Alloc(2);
    Submit({Req(), Req()});
    rdma_->completeOneSuccess(0);
    ExpectPendingSkip();

    rdma_->publishFailure(1);
    auto status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_GE(rdma_->status_calls.load(), 1);
}

TEST_F(BatchCounterWaitTest, NotifySkipUsesCounterOnlyNoOwnerScan) {
    Start();
    AddRdma(true);
    constexpr size_t kOwners = 32;
    Alloc(kOwners);
    std::vector<Request> reqs(kOwners, Req());
    auto notify = LocalNotify("counter-only");
    Submit(reqs, &notify);
    ExpectPendingSkip();

    for (size_t i = 0; i + 1 < kOwners; ++i) rdma_->completeOneSuccess(i);
    ExpectPendingSkip();
    std::vector<Notification> silent;
    (void)engine_->receiveNotification(silent);
    EXPECT_TRUE(silent.empty());

    rdma_->completeOneSuccess(kOwners - 1);
    auto status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(rdma_->status_calls.load(), static_cast<int>(kOwners));
    // Completion walk still fires from the interval counter, so TaskInfo is
    // not scanned even when allow_owner_scan is true.
    EXPECT_EQ(engine_->hookOwnerScansForTest(), 0u);
    auto received = Recv();
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "counter-only");
}

TEST_F(BatchCounterWaitTest,
       NotifyFiresForCompletedIntervalWhileAppendPending) {
    Start();
    AddRdma(true);
    Alloc(2);
    auto notify = LocalNotify("interval-a");
    Submit({Req()}, &notify);
    Submit({Req()});
    ExpectPendingSkip();

    rdma_->completeOneSuccess(0);
    ExpectPendingSkip();
    auto received = Recv();
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "interval-a");

    rdma_->completeOneSuccess(1);
    auto status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(rdma_->status_calls.load(), 2);
    received.clear();
    (void)engine_->receiveNotification(received);
    EXPECT_TRUE(received.empty());
}

TEST_F(BatchCounterWaitTest, NotifySecondIntervalStaysSilentUntilComplete) {
    Start();
    AddRdma(true);
    Alloc(2);
    auto first = LocalNotify("first");
    auto second = LocalNotify("second");
    Submit({Req()}, &first);
    Submit({Req()}, &second);

    rdma_->completeOneSuccess(0);
    ExpectPendingSkip();
    auto received = Recv();
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "first");

    rdma_->completeOneSuccess(1);
    auto status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    received.clear();
    received = Recv();
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "second");
}

TEST_F(BatchCounterWaitTest, AppendAfterCompletionUsesCounterAgain) {
    Start();
    AddRdma(true);
    Alloc(2);
    Submit({Req()});
    rdma_->completeAllSuccess();
    EXPECT_EQ(Poll().s, TransferStatusEnum::COMPLETED);
    const int calls_after_first = rdma_->status_calls.load();

    Submit({Req()});
    auto status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(status.transferred_bytes, 0u);
    EXPECT_EQ(rdma_->status_calls.load(), calls_after_first);

    rdma_->completeAllSuccess();
    EXPECT_EQ(Poll().s, TransferStatusEnum::COMPLETED);
}

TEST_F(BatchCounterWaitTest, FailoverFromPublisherToTcpKeepsOwnerWalk) {
    Start(/*failover=*/true);
    AddRdma(true);
    AddTcp();
    Alloc(1);
    Submit({Req(RDMA)});
    EXPECT_EQ(rdma_->submit_calls.load(), 1);
    EXPECT_EQ(tcp_->submit_calls.load(), 0);
    ExpectPendingSkip();

    rdma_->publishFailure(0);
    auto status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_GE(rdma_->status_calls.load(), 1);
    EXPECT_GE(tcp_->submit_calls.load(), 1);

    status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_GE(tcp_->status_calls.load(), 1);

    tcp_->completeAllSuccess();
    status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kBufLen);
}

enum class EligibilityDrop {
    NonPublisher,
    MixedRdmaTcp,
    AppendTcp,
    FailedSubmit,
    UnspecSubmit,
};

class BatchCounterWaitEligibility
    : public BatchCounterWaitTest,
      public ::testing::WithParamInterface<EligibilityDrop> {};

TEST_P(BatchCounterWaitEligibility, KeepsOwnerWalk) {
    Start();
    const bool publishes = GetParam() != EligibilityDrop::NonPublisher;
    AddRdma(publishes);
    if (GetParam() == EligibilityDrop::MixedRdmaTcp ||
        GetParam() == EligibilityDrop::AppendTcp) {
        AddTcp();
    }
    Alloc(2);

    switch (GetParam()) {
        case EligibilityDrop::NonPublisher:
            Submit({Req()});
            break;
        case EligibilityDrop::MixedRdmaTcp:
            Submit({Req(RDMA), Req(TCP)});
            EXPECT_EQ(rdma_->submit_calls.load(), 1);
            EXPECT_EQ(tcp_->submit_calls.load(), 1);
            break;
        case EligibilityDrop::AppendTcp:
            Submit({Req(RDMA)});
            ExpectPendingSkip();
            Submit({Req(TCP)});
            EXPECT_EQ(tcp_->submit_calls.load(), 1);
            break;
        case EligibilityDrop::FailedSubmit:
            Submit({Req(RDMA)});
            ExpectPendingSkip();
            rdma_->fail_next_submit = true;
            Submit({Req(RDMA)});
            break;
        case EligibilityDrop::UnspecSubmit:
            Submit({Req(RDMA)});
            ExpectPendingSkip();
            {
                Request missing = Req(RDMA);
                missing.target_offset = 0x100000000ULL;
                Submit({missing});
            }
            break;
    }

    auto status = Poll();
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_GE(rdma_->status_calls.load(), 1);
    if (tcp_) {
        EXPECT_GE(tcp_->status_calls.load(), 1);
    }

    rdma_->completeAllSuccess();
    if (tcp_) tcp_->completeAllSuccess();
    status = Poll();
    if (GetParam() == EligibilityDrop::FailedSubmit ||
        GetParam() == EligibilityDrop::UnspecSubmit) {
        EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    } else {
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    }
}

std::string EligibilityName(
    const ::testing::TestParamInfo<EligibilityDrop>& info) {
    switch (info.param) {
        case EligibilityDrop::NonPublisher:
            return "NonPublisher";
        case EligibilityDrop::MixedRdmaTcp:
            return "MixedRdmaTcp";
        case EligibilityDrop::AppendTcp:
            return "AppendTcp";
        case EligibilityDrop::FailedSubmit:
            return "FailedSubmit";
        case EligibilityDrop::UnspecSubmit:
            return "UnspecSubmit";
    }
    return "Unknown";
}

INSTANTIATE_TEST_SUITE_P(Cases, BatchCounterWaitEligibility,
                         ::testing::Values(EligibilityDrop::NonPublisher,
                                           EligibilityDrop::MixedRdmaTcp,
                                           EligibilityDrop::AppendTcp,
                                           EligibilityDrop::FailedSubmit,
                                           EligibilityDrop::UnspecSubmit),
                         EligibilityName);

}  // namespace
}  // namespace tent
}  // namespace mooncake
