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

#include <atomic>
#include <chrono>
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

namespace mooncake {
namespace tent {
namespace {

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
    using PollStatusFactory =
        std::function<TransferStatus(const Request&, int)>;

    explicit FakeTransport(TransportType self_type,
                           PollStatusFactory poll_status_factory = {},
                           bool notify_on_submit = false,
                           bool fail_submit = false)
        : self_type_(self_type),
          poll_status_factory_(std::move(poll_status_factory)),
          notify_on_submit_(notify_on_submit),
          fail_submit_(fail_submit) {
        caps.dram_to_dram = true;
    }

    std::atomic<int> submit_calls{0};
    std::atomic<int> status_calls{0};
    std::atomic<int> cancel_calls{0};
    bool cancellation_supported{true};

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
                               const std::vector<Request>& requests) override {
        ++submit_calls;
        if (fail_submit_)
            return Status::InternalError("injected submit failure" LOC_MARK);
        auto* fake = static_cast<FakeSubBatch*>(batch);
        for (const auto& request : requests) {
            fake->requests.push_back(request);
            fake->statuses.push_back(
                {TransferStatusEnum::COMPLETED, request.length});
            fake->poll_counts.push_back(0);
            ++fake->task_count;
        }
        if (notify_on_submit_) batch->notifyProgress();
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        ++status_calls;
        auto* fake = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fake->statuses.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        ++fake->poll_counts[task_id];
        if (poll_status_factory_) {
            status = poll_status_factory_(fake->requests[task_id],
                                          fake->poll_counts[task_id]);
        } else {
            status = fake->statuses[task_id];
        }
        return Status::OK();
    }

    bool supportsCancellation() const override {
        return cancellation_supported;
    }

    Status cancelTransferTask(SubBatchRef batch, int task_id) override {
        auto* fake = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fake->statuses.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        if (!cancellation_supported) {
            return Status::NotImplemented("cancel unsupported" LOC_MARK);
        }
        ++cancel_calls;
        fake->statuses[task_id] = {TransferStatusEnum::CANCELED, 0};
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(self_type_);
        return Status::OK();
    }

    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override {
        for (auto& desc : desc_list) {
            auto status = addMemoryBuffer(desc, options);
            if (!status.ok()) return status;
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

    const char* getName() const override { return "<fake-rdma>"; }

   private:
    TransportType self_type_;
    PollStatusFactory poll_status_factory_;
    bool notify_on_submit_;
    bool fail_submit_;
};

std::shared_ptr<Config> makeRuntimeQueueConfig(size_t max_dispatch_owners,
                                               size_t max_dispatch_bytes,
                                               bool merge_requests = false) {
    auto cfg = std::make_shared<Config>();
    cfg->set("metadata_type", "p2p");
    cfg->set("metadata_servers", "");
    cfg->set("rpc_server_hostname", "127.0.0.1");
    cfg->set("rpc_server_port", "0");
    cfg->set("log_level", "warning");
    cfg->set("merge_requests", merge_requests);
    cfg->set("enable_runtime_queue", true);
    cfg->set("runtime_queue/max_outstanding_owners", 16UL);
    cfg->set("runtime_queue/max_outstanding_bytes", 1UL << 20);
    cfg->set("runtime_queue/max_dispatch_owners", max_dispatch_owners);
    cfg->set("runtime_queue/max_dispatch_bytes", max_dispatch_bytes);
    cfg->set("runtime_queue/staging_owner_reserve", 0UL);
    cfg->set("runtime_queue/staging_byte_reserve", 0UL);
    cfg->set("runtime_queue/progress_fallback_interval_us", 50000UL);

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

void enableProductionReceiverCredit(const std::shared_ptr<Config>& cfg,
                                    uint64_t data_bytes = 1ULL << 20,
                                    uint64_t request_slots = 16) {
    cfg->set("receiver_credit/mode", "required");
    cfg->set("receiver_credit/capacity/data_bytes", data_bytes);
    cfg->set("receiver_credit/capacity/request_slots", request_slots);
    cfg->set("receiver_credit/grant_batch/data_bytes", data_bytes);
    cfg->set("receiver_credit/grant_batch/request_slots", request_slots);
    cfg->set("receiver_credit/control/freshness_ttl_ms", uint64_t{1000});
    cfg->set("receiver_credit/control/retry_after_us", uint64_t{100});
    cfg->set("receiver_credit/control/poll_interval_us", uint64_t{1000});
    cfg->set("receiver_credit/limits/max_peers", uint64_t{16});
}

void installFakeRdma(TransferEngineImpl& engine,
                     const std::shared_ptr<FakeTransport>& fake_rdma) {
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(fake_rdma->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake_rdma);
}

Request makeLocalWrite(uint8_t* ptr, size_t length) {
    Request request;
    request.opcode = Request::WRITE;
    request.source = ptr;
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(ptr);
    request.length = length;
    request.transport_hint = RDMA;
    return request;
}

CreditKey installCreditGate(
    TransferEngineImpl& engine,
    const std::shared_ptr<CreditPeerContextTable>& contexts,
    const std::shared_ptr<SenderCreditLedger>& ledger, uint64_t bytes,
    uint64_t slots) {
    CreditActivationV1 activation;
    activation.receiver_session_id = {11, 22};
    activation.epoch = 7;
    EXPECT_TRUE(contexts->activate(LOCAL_SEGMENT_ID, 200, 3, activation).ok());
    CreditPeerContextSnapshot peer;
    EXPECT_TRUE(contexts->lookup(LOCAL_SEGMENT_ID, 3, peer).ok());
    EXPECT_TRUE(ledger->activate(peer.key, peer.epoch).ok());
    ReceiverCreditUpdateV1 update;
    update.receiver_session_id = peer.key.receiver_session;
    update.qos_class = peer.key.qos_class;
    update.epoch = peer.epoch;
    update.sequence = 1;
    update.grants = {{CreditResource::DataBytes, bytes},
                     {CreditResource::RequestSlots, slots}};
    CreditUpdateDisposition disposition;
    EXPECT_TRUE(ledger->applyUpdate(peer.key, update, disposition).ok());
    EXPECT_TRUE(engine
                    .installReceiverCreditDispatch(
                        contexts, ledger, [](const Request&) { return 3; })
                    .ok());
    return peer.key;
}

TEST(RuntimeQueueDispatch, RejectsReceiverCreditWithoutRuntimeQueue) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("enable_runtime_queue", false);
    cfg->set("receiver_credit/enabled", true);
    TransferEngineImpl engine(cfg);
    EXPECT_FALSE(engine.available());
}

TEST(RuntimeQueueDispatch, ProductionCreditPullGatesDirectRemoteRdmaWrite) {
    auto receiver_config = makeRuntimeQueueConfig(4, 1UL << 20);
    auto sender_config = makeRuntimeQueueConfig(4, 1UL << 20);
    enableProductionReceiverCredit(receiver_config, 4096, 1);
    enableProductionReceiverCredit(sender_config, 4096, 1);

    TransferEngineImpl receiver(receiver_config);
    TransferEngineImpl sender(sender_config);
    ASSERT_TRUE(receiver.available());
    ASSERT_TRUE(sender.available());
    auto receiver_rdma = std::make_shared<FakeTransport>(RDMA);
    auto sender_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(receiver, receiver_rdma);
    installFakeRdma(sender, sender_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> source(kReqLen, 0x42);
    std::vector<uint8_t> target(kReqLen, 0);
    ASSERT_TRUE(sender.registerLocalMemory(source.data(), source.size()).ok());
    ASSERT_TRUE(
        receiver.registerLocalMemory(target.data(), target.size()).ok());

    SegmentID target_id = 0;
    ASSERT_TRUE(sender.openSegment(target_id, receiver.getSegmentName()).ok());
    ASSERT_NE(target_id, LOCAL_SEGMENT_ID);
    Request request;
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = target_id;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = kReqLen;
    request.transport_hint = RDMA;

    BatchID batch = sender.allocateBatch(1);
    ASSERT_TRUE(sender.submitTransfer(batch, {request}).ok());
    TransferStatus status{};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(sender.getTransferStatus(batch, 0, status).ok());
        if (status.s != TransferStatusEnum::PENDING) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(sender_rdma->submit_calls.load(), 1);

    EXPECT_TRUE(sender.freeBatch(batch).ok());
    EXPECT_TRUE(sender.closeSegment(target_id).ok());
    EXPECT_TRUE(
        sender.unregisterLocalMemory(source.data(), source.size()).ok());
    EXPECT_TRUE(
        receiver.unregisterLocalMemory(target.data(), target.size()).ok());
}

TEST(RuntimeQueueDispatch, ProductionCreditBypassesLocalWrite) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    enableProductionReceiverCredit(cfg, 4096, 1);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    std::vector<uint8_t> buffer(4096, 0x43);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), 4096)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsCreditInstallWhenOptInIsDisabled) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto status = engine.installReceiverCreditDispatch(
        std::make_shared<CreditPeerContextTable>(),
        std::make_shared<SenderCreditLedger>());
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(RuntimeQueueDispatch, CreditOptInFailsClosedBeforeInstall) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enabled", true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);
    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen, 0x90);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    auto status =
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)});
    EXPECT_TRUE(status.IsInvalidEntry()) << status.ToString();
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CreditOptInUsesConfiguredDefaultQos) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enabled", true);
    cfg->set("receiver_credit/default_qos_class", uint32_t{5});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    auto contexts = std::make_shared<CreditPeerContextTable>();
    auto ledger = std::make_shared<SenderCreditLedger>();
    CreditActivationV1 activation;
    activation.receiver_session_id = {55, 66};
    activation.epoch = 9;
    ASSERT_TRUE(contexts->activate(LOCAL_SEGMENT_ID, 200, 5, activation).ok());
    CreditPeerContextSnapshot peer;
    ASSERT_TRUE(contexts->lookup(LOCAL_SEGMENT_ID, 5, peer).ok());
    ASSERT_TRUE(ledger->activate(peer.key, peer.epoch).ok());
    constexpr size_t kReqLen = 4096;
    ReceiverCreditUpdateV1 update;
    update.receiver_session_id = peer.key.receiver_session;
    update.qos_class = peer.key.qos_class;
    update.epoch = peer.epoch;
    update.sequence = 1;
    update.grants = {{CreditResource::DataBytes, kReqLen},
                     {CreditResource::RequestSlots, 1}};
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(ledger->applyUpdate(peer.key, update, disposition).ok());
    ASSERT_TRUE(engine.installReceiverCreditDispatch(contexts, ledger).ok());

    std::vector<uint8_t> buffer(kReqLen, 0x96);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    uint64_t bytes = 1;
    ASSERT_TRUE(
        ledger->available(peer.key, CreditResource::DataBytes, bytes).ok());
    EXPECT_EQ(bytes, 0);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CreditGateCommitsSuccessfulTransportSubmit) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enabled", true);
    auto contexts = std::make_shared<CreditPeerContextTable>();
    auto ledger = std::make_shared<SenderCreditLedger>();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);
    constexpr size_t kReqLen = 4096;
    auto credit_key = installCreditGate(engine, contexts, ledger, kReqLen, 1);
    std::vector<uint8_t> buffer(kReqLen, 0x91);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    uint64_t bytes = 1, slots = 1;
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::DataBytes, bytes).ok());
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::RequestSlots, slots)
            .ok());
    EXPECT_EQ(bytes, 0);
    EXPECT_EQ(slots, 0);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CreditGateRollsBackFailedTransportSubmit) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enabled", true);
    auto contexts = std::make_shared<CreditPeerContextTable>();
    auto ledger = std::make_shared<SenderCreditLedger>();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, FakeTransport::PollStatusFactory{}, false, true);
    installFakeRdma(engine, fake_rdma);
    constexpr size_t kReqLen = 4096;
    auto credit_key = installCreditGate(engine, contexts, ledger, kReqLen, 1);
    std::vector<uint8_t> buffer(kReqLen, 0x92);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    uint64_t bytes = 0, slots = 0;
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::DataBytes, bytes).ok());
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::RequestSlots, slots)
            .ok());
    EXPECT_EQ(bytes, kReqLen);
    EXPECT_EQ(slots, 1);
    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CreditGateDefersUntilCreditArrives) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enabled", true);
    auto contexts = std::make_shared<CreditPeerContextTable>();
    auto ledger = std::make_shared<SenderCreditLedger>();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);
    constexpr size_t kReqLen = 4096;
    auto credit_key = installCreditGate(engine, contexts, ledger, 0, 0);
    std::vector<uint8_t> buffer(kReqLen, 0x93);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);

    ReceiverCreditUpdateV1 update;
    update.receiver_session_id = credit_key.receiver_session;
    update.qos_class = credit_key.qos_class;
    update.epoch = 7;
    update.sequence = 2;
    update.grants = {{CreditResource::DataBytes, kReqLen},
                     {CreditResource::RequestSlots, 1}};
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(ledger->applyUpdate(credit_key, update, disposition).ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    uint64_t bytes = 1, slots = 1;
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::DataBytes, bytes).ok());
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::RequestSlots, slots)
            .ok());
    EXPECT_EQ(bytes, 0);
    EXPECT_EQ(slots, 0);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CreditGateRejectsQueuedSnapshotAfterRestart) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enabled", true);
    auto contexts = std::make_shared<CreditPeerContextTable>();
    auto ledger = std::make_shared<SenderCreditLedger>();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);
    constexpr size_t kReqLen = 4096;
    auto old_key = installCreditGate(engine, contexts, ledger, 0, 0);
    std::vector<uint8_t> buffer(kReqLen, 0x94);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);

    CreditActivationV1 restarted;
    restarted.receiver_session_id = {33, 44};
    restarted.epoch = 1;
    ASSERT_TRUE(contexts->activate(LOCAL_SEGMENT_ID, 200, 3, restarted).ok());

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);
    uint64_t bytes = 1;
    ASSERT_TRUE(
        ledger->available(old_key, CreditResource::DataBytes, bytes).ok());
    EXPECT_EQ(bytes, 0);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CreditGateDoesNotOverdrawAcrossQueuedOwners) {
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    cfg->set("receiver_credit/enabled", true);
    auto contexts = std::make_shared<CreditPeerContextTable>();
    auto ledger = std::make_shared<SenderCreditLedger>();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);
    constexpr size_t kReqLen = 4096;
    auto credit_key = installCreditGate(engine, contexts, ledger, kReqLen, 1);
    std::vector<uint8_t> buffer(kReqLen * 2, 0x95);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    uint64_t bytes = 1, slots = 1;
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::DataBytes, bytes).ok());
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::RequestSlots, slots)
            .ok());
    EXPECT_EQ(bytes, 0);
    EXPECT_EQ(slots, 0);

    ReceiverCreditUpdateV1 update;
    update.receiver_session_id = credit_key.receiver_session;
    update.qos_class = credit_key.qos_class;
    update.epoch = 7;
    update.sequence = 2;
    update.grants = {{CreditResource::DataBytes, kReqLen * 2},
                     {CreditResource::RequestSlots, 2}};
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(ledger->applyUpdate(credit_key, update, disposition).ok());

    TransferStatus overall{};
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(engine.getTransferStatus(batch, overall).ok());
        if (overall.s == TransferStatusEnum::COMPLETED) break;
    }
    EXPECT_EQ(overall.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::DataBytes, bytes).ok());
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::RequestSlots, slots)
            .ok());
    EXPECT_EQ(bytes, 0);
    EXPECT_EQ(slots, 0);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CreditGateFillsDispatchWindowWhenCreditAllows) {
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    cfg->set("receiver_credit/enabled", true);
    auto contexts = std::make_shared<CreditPeerContextTable>();
    auto ledger = std::make_shared<SenderCreditLedger>();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);
    constexpr size_t kReqLen = 4096;
    auto credit_key =
        installCreditGate(engine, contexts, ledger, kReqLen * 2, 2);
    std::vector<uint8_t> buffer(kReqLen * 2, 0x97);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);
    uint64_t bytes = 1, slots = 1;
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::DataBytes, bytes).ok());
    ASSERT_TRUE(
        ledger->available(credit_key, CreditResource::RequestSlots, slots)
            .ok());
    EXPECT_EQ(bytes, 0);
    EXPECT_EQ(slots, 0);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CreditGateDefersEveryUnprocessedPickedOwner) {
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    cfg->set("receiver_credit/enabled", true);
    auto contexts = std::make_shared<CreditPeerContextTable>();
    auto ledger = std::make_shared<SenderCreditLedger>();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);
    constexpr size_t kReqLen = 4096;
    auto credit_key = installCreditGate(engine, contexts, ledger, 0, 0);
    std::vector<uint8_t> buffer(kReqLen * 2, 0x98);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);

    ReceiverCreditUpdateV1 update;
    update.receiver_session_id = credit_key.receiver_session;
    update.qos_class = credit_key.qos_class;
    update.epoch = 7;
    update.sequence = 2;
    update.grants = {{CreditResource::DataBytes, kReqLen * 2},
                     {CreditResource::RequestSlots, 2}};
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(ledger->applyUpdate(credit_key, update, disposition).ok());

    TransferStatus overall{};
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(engine.getTransferStatus(batch, overall).ok());
        if (overall.s == TransferStatusEnum::COMPLETED) break;
    }
    EXPECT_EQ(overall.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsOverfullBatchBeforePublishingTasks) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x11);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);

    auto status = engine.submitTransfer(
        batch, {makeLocalWrite(buffer.data(), kReqLen),
                makeLocalWrite(buffer.data() + kReqLen, kReqLen)});
    EXPECT_TRUE(status.IsTooManyRequests()) << status.ToString();
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);

    TransferStatus task_status{};
    EXPECT_TRUE(
        engine.getTransferStatus(batch, 0, task_status).IsInvalidArgument());

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsOwnerLargerThanDispatchByteWindow) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(1, kReqLen - 1);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    std::vector<uint8_t> buffer(kReqLen, 0x77);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);

    auto status =
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)});
    EXPECT_TRUE(status.IsTooManyRequests()) << status.ToString();
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);

    TransferStatus task_status{};
    EXPECT_TRUE(
        engine.getTransferStatus(batch, 0, task_status).IsInvalidArgument());

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsEmptyDispatchWindowConfig) {
    auto cfg = makeRuntimeQueueConfig(0, 1UL << 20);
    TransferEngineImpl engine(cfg);

    EXPECT_FALSE(engine.available());

    cfg = makeRuntimeQueueConfig(1, 0);
    TransferEngineImpl byte_window_engine(cfg);

    EXPECT_FALSE(byte_window_engine.available());
}

TEST(RuntimeQueueDispatch, DispatchesOnlyOneWindowOnSubmit) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x22);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    TransferStatus first{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);

    TransferStatus second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, KeepsDispatchWindowUntilOwnerIsTerminal) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete_first{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete_first](const Request& request, int) {
            if (!complete_first.load()) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x55);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    TransferStatus first{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    TransferStatus second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    complete_first.store(true);
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);

    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CancelsQueuedOwnerWithoutDispatchingIt) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete_first{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete_first](const Request& request, int) {
            if (!complete_first.load()) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x5a);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    ASSERT_EQ(fake_rdma->submit_calls.load(), 1);

    ASSERT_TRUE(engine.cancelTransfer(batch, 1).ok());
    EXPECT_EQ(fake_rdma->cancel_calls.load(), 0);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    TransferStatus second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::CANCELED);

    complete_first.store(true);
    TransferStatus first{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, CancelsDispatchedRdmaTaskIdempotently) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen, 0x6b);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());

    ASSERT_TRUE(engine.cancelTransfer(batch, 0).ok());
    EXPECT_EQ(fake_rdma->cancel_calls.load(), 1);
    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::CANCELED);
    ASSERT_TRUE(engine.cancelTransfer(batch, 0).ok());
    EXPECT_EQ(fake_rdma->cancel_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsCancellationForUnsupportedTransport) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    fake_rdma->cancellation_supported = false;
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen, 0x7c);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());

    EXPECT_TRUE(engine.cancelTransfer(batch, 0).IsNotImplemented());
    EXPECT_EQ(fake_rdma->cancel_calls.load(), 0);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, ProgressWorkerRefillsWindowFromTransportNotify) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("enable_progress_worker", true);
    cfg->set("runtime_queue/progress_fallback_interval_us", 0UL);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, FakeTransport::PollStatusFactory{}, true);
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x66);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    // The submit notification may let the progress worker dispatch the second
    // owner before this thread observes the first one.
    EXPECT_GE(fake_rdma->submit_calls.load(), 1);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(1000);
    while (std::chrono::steady_clock::now() < deadline &&
           fake_rdma->submit_calls.load() < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RuntimeQueueDrainsWithoutUserPolling) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("runtime_queue/progress_fallback_interval_us", 1000UL);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [](const Request& request, int poll_count) {
            if (poll_count == 1) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x88);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(1000);
    while (std::chrono::steady_clock::now() < deadline &&
           (fake_rdma->submit_calls.load() < 2 ||
            fake_rdma->status_calls.load() < 4)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);
    EXPECT_GE(fake_rdma->status_calls.load(), 4);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, EarlyFreeReclaimsAfterQueuedCompletion) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [](const Request& request, int poll_count) {
            if (poll_count == 1) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen, 0x33);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buffer.data(), kReqLen)})
            .ok());

    ASSERT_TRUE(engine.freeBatch(batch).ok());

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(1000);
    while (std::chrono::steady_clock::now() < deadline &&
           fake_rdma->status_calls.load() < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_GE(fake_rdma->status_calls.load(), 2);

    TransferStatus status{};
    EXPECT_TRUE(engine.getTransferStatus(batch, 0, status).IsInvalidArgument());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, PollingDerivedTaskCompletesMergedOwner) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20, true);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 2, 0x44);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    TransferStatus derived{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, derived).ok());
    EXPECT_EQ(derived.s, TransferStatusEnum::COMPLETED);

    TransferStatus owner{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, owner).ok());
    EXPECT_EQ(owner.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
