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
#include "tent/runtime/control_plane.h"
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
                           bool notify_on_submit = false)
        : self_type_(self_type),
          poll_status_factory_(std::move(poll_status_factory)),
          notify_on_submit_(notify_on_submit) {
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
        if (fake->statuses[task_id].s == TransferStatusEnum::CANCELED) {
            status = fake->statuses[task_id];
        } else if (poll_status_factory_) {
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

    complete_first.store(true);

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

    auto fake_rdma =
        std::make_shared<FakeTransport>(RDMA, [](const Request&, int) {
            return TransferStatus{TransferStatusEnum::PENDING, 0};
        });
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
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

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
    for (bool queue : {false, true}) {
        auto cfg = makeRuntimeQueueConfig(1, 1UL << 20, true);
        cfg->set("enable_runtime_queue", queue);
        cfg->set("enable_progress_worker", false);
        TransferEngineImpl engine(cfg);
        ASSERT_TRUE(engine.available());

        auto fake_rdma = std::make_shared<FakeTransport>(
            RDMA, [](const Request& request, int) {
                EXPECT_EQ(request.length, 32u);
                return TransferStatus{COMPLETED, request.length};
            });
        installFakeRdma(engine, fake_rdma);

        constexpr size_t kReqLen = 16;
        std::vector<uint8_t> buffer(kReqLen * 2, 0x44);
        ASSERT_TRUE(
            engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

        BatchID batch = engine.allocateBatch(2);
        ASSERT_NE(batch, (BatchID)0);

        ASSERT_TRUE(
            engine
                .submitTransfer(
                    batch, {makeLocalWrite(buffer.data(), kReqLen),
                            makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
                .ok());
        EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

        TransferStatus derived{};
        ASSERT_TRUE(engine.getTransferStatus(batch, 1, derived).ok());
        EXPECT_EQ(derived.s, TransferStatusEnum::COMPLETED);
        EXPECT_EQ(derived.transferred_bytes, 16u);

        TransferStatus owner{};
        ASSERT_TRUE(engine.getTransferStatus(batch, 0, owner).ok());
        EXPECT_EQ(owner.s, TransferStatusEnum::COMPLETED);
        EXPECT_EQ(owner.transferred_bytes, 16u);
        for (int poll = 0; poll < 2; ++poll) {
            std::vector<TransferStatus> tasks;
            ASSERT_TRUE(engine.getTransferStatus(batch, tasks).ok());
            ASSERT_EQ(tasks.size(), 2u);
            for (const auto& status : tasks) {
                EXPECT_EQ(status.s, COMPLETED);
                EXPECT_EQ(status.transferred_bytes, 16u);
            }
            TransferStatus total{};
            ASSERT_TRUE(engine.getTransferStatus(batch, total).ok());
            EXPECT_EQ(total.s, COMPLETED);
            EXPECT_EQ(total.transferred_bytes, 32u);
        }

        EXPECT_TRUE(engine.freeBatch(batch).ok());
        EXPECT_TRUE(
            engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
    }
}

TEST(RuntimeQueueDispatch, BatchNotificationsFollowSubmitIntervals) {
    for (auto initial : {FAILED, PENDING}) {
        for (bool same_submit : {false, true}) {
            SCOPED_TRACE(testing::Message() << initial << "/" << same_submit);
            auto cfg = makeRuntimeQueueConfig(4, 1024, true);
            cfg->set("enable_runtime_queue", false);
            cfg->set("enable_progress_worker", false);
            cfg->set("enable_auto_failover_on_poll", false);
            TransferEngineImpl engine(cfg);
            std::vector<uint8_t> buffer(128);
            auto second_status = initial;
            auto fake = std::make_shared<FakeTransport>(
                RDMA, [&](const Request& request, int) -> TransferStatus {
                    if (request.source == buffer.data()) {
                        EXPECT_EQ(request.length, 32u);
                    }
                    auto status = request.source == buffer.data() + 64
                                      ? second_status
                                      : COMPLETED;
                    return {status, status == COMPLETED ? request.length : 0};
                });
            installFakeRdma(engine, fake);
            ASSERT_TRUE(
                engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
            std::vector<Request> first{makeLocalWrite(buffer.data(), 16),
                                       makeLocalWrite(buffer.data() + 16, 16)};
            auto second = makeLocalWrite(buffer.data() + 64, 16);
            if (same_submit) first.push_back(second);
            auto batch = engine.allocateBatch(3);
            ASSERT_TRUE(
                engine.submitTransfer(batch, first, {"first", "ready"}).ok());
            if (!same_submit) {
                ASSERT_TRUE(
                    engine.submitTransfer(batch, {second}, {"second", "ready"})
                        .ok());
            }
            for (int phase = 0; phase < 2; ++phase) {
                size_t first_count = 0, second_count = 0;
                for (int poll = 0; poll < 3; ++poll) {
                    TransferStatus status{};
                    ASSERT_TRUE(engine.getTransferStatus(batch, status).ok());
                    EXPECT_EQ(status.s, second_status);
                    std::vector<Notification> received;
                    // No notification transport is installed; an empty poll
                    // reports unsupported, but self notifications are
                    // delivered.
                    (void)engine.receiveNotification(received);
                    for (const auto& item : received) {
                        if (item.name == "first")
                            ++first_count;
                        else if (item.name == "second")
                            ++second_count;
                        else
                            ADD_FAILURE() << item.name;
                    }
                }
                EXPECT_EQ(first_count, phase == 0
                                           ? !same_submit
                                           : same_submit && initial == PENDING);
                EXPECT_EQ(second_count,
                          phase == 1 && !same_submit && initial == PENDING);
                if (initial == PENDING) second_status = COMPLETED;
            }
            EXPECT_TRUE(engine.freeBatch(batch).ok());
            EXPECT_TRUE(
                engine.unregisterLocalMemory(buffer.data(), buffer.size())
                    .ok());
        }
    }
}

class RangeOnlyRecordingTransport : public FakeTransport {
   public:
    explicit RangeOnlyRecordingTransport(TransportType type,
                                         PollStatusFactory poll)
        : FakeTransport(type, std::move(poll)) {}
    static constexpr uint64_t kRange = uint64_t{1} << 34;
    std::vector<Request> posted;

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override {
        // Only the descriptor is large. The fake never accesses its payload.
        desc.length = kRange;
        return FakeTransport::addMemoryBuffer(desc, options);
    }
    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        posted.insert(posted.end(), requests.begin(), requests.end());
        return FakeTransport::submitTransferTasks(batch, requests);
    }
};

TEST(RuntimeQueueDispatch, TcpCapsPreservePublicMappingAndOwnerBytes) {
    for (auto type : {TCP, RDMA, HP_TCP}) {
        for (auto hint : {UNSPEC, type}) {
            for (auto opcode : {Request::READ, Request::WRITE}) {
                SCOPED_TRACE(testing::Message()
                             << type << "/" << hint << "/" << opcode);
                const auto cap = tcpMaxTransferBytes(opcode);
                auto cfg = makeRuntimeQueueConfig(3, 1024, true);
                cfg->set("enable_runtime_queue", false);
                cfg->set("enable_progress_worker", false);
                TransferEngineImpl engine(cfg);
                std::vector<uint8_t> buffer(64);
                const auto addr = reinterpret_cast<uintptr_t>(buffer.data());
                auto tail_status = PENDING;
                auto fake = std::make_shared<RangeOnlyRecordingTransport>(
                    type, [&](const Request& request, int) -> TransferStatus {
                        auto status = reinterpret_cast<uintptr_t>(
                                          request.source) == addr + cap
                                          ? tail_status
                                          : COMPLETED;
                        return {status,
                                status == COMPLETED ? request.length : 0};
                    });
                engine.swapTransportForTest(type, fake);
                ASSERT_TRUE(
                    engine.registerLocalMemory(buffer.data(), buffer.size())
                        .ok());
                std::vector<Request> requests;
                uint64_t offset = 0;
                for (size_t length : {cap - 1, size_t{1}, size_t{1}}) {
                    auto request = makeLocalWrite(buffer.data(), length);
                    request.source = reinterpret_cast<void*>(addr + offset);
                    request.target_offset = addr + offset;
                    request.opcode = opcode;
                    request.transport_hint = hint;
                    requests.push_back(request);
                    offset += length;
                }
                auto batch = engine.allocateBatch(3);
                ASSERT_TRUE(engine.submitTransfer(batch, requests).ok());
                ASSERT_EQ(fake->posted.size(), type == TCP ? 2u : 1u);
                EXPECT_EQ(fake->posted[0].length, type == TCP ? cap : cap + 1);
                EXPECT_EQ(fake->posted[0].source, buffer.data());
                EXPECT_EQ(fake->posted[0].target_offset, addr);
                if (type == TCP) {
                    EXPECT_EQ(fake->posted[1].length, 1u);
                    EXPECT_EQ(
                        reinterpret_cast<uintptr_t>(fake->posted[1].source),
                        addr + cap);
                    EXPECT_EQ(fake->posted[1].target_offset, addr + cap);
                }
                for (int poll = 0; poll < 3; ++poll) {
                    for (size_t task = 0; task < requests.size(); ++task) {
                        TransferStatus status{};
                        ASSERT_TRUE(
                            engine.getTransferStatus(batch, task, status).ok());
                        const bool pending =
                            type == TCP && poll == 0 && task == 2;
                        EXPECT_EQ(status.s, pending ? PENDING : COMPLETED);
                        EXPECT_EQ(status.transferred_bytes,
                                  pending ? 0 : requests[task].length);
                    }
                    TransferStatus status{};
                    ASSERT_TRUE(engine.getTransferStatus(batch, status).ok());
                    EXPECT_EQ(status.s,
                              type == TCP && poll == 0 ? PENDING : COMPLETED);
                    EXPECT_EQ(status.transferred_bytes,
                              type == TCP && poll == 0 ? cap : cap + 1);
                    tail_status = COMPLETED;
                }
                EXPECT_TRUE(engine.freeBatch(batch).ok());
                EXPECT_TRUE(
                    engine
                        .unregisterLocalMemory(
                            buffer.data(), RangeOnlyRecordingTransport::kRange)
                        .ok());
            }
        }
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
