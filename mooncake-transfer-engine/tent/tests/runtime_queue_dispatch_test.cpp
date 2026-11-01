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
#include <cstring>
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
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake {

// Reaches the impl's queue bookkeeping so a test can say what a batch that
// left the normal lifecycle still holds.
class TransferEngineImplTestPeer {
   public:
    static tent::Status submitRequiringPostSubmitCancellation(
        tent::TransferEngineImpl& engine, tent::BatchID batch_id,
        const std::vector<tent::Request>& requests) {
        return engine.submitTransferRequiringPostSubmitCancellation(batch_id,
                                                                    requests);
    }
    static size_t queuedOwners(tent::TransferEngineImpl& engine) {
        return engine.queued_owners_.size();
    }
    static size_t windowOwners(tent::TransferEngineImpl& engine) {
        return engine.dispatch_inflight_owners_;
    }
    static size_t windowBytes(tent::TransferEngineImpl& engine) {
        return engine.dispatch_inflight_bytes_;
    }
    static size_t dispatchingBytes(tent::TransferEngineImpl& engine) {
        return engine.runtime_queue_->dispatchingBytes();
    }
    static size_t queueOutstandingOwners(tent::TransferEngineImpl& engine) {
        return engine.runtime_queue_->outstandingOwners();
    }
    static bool hasActiveRuntimeQueue(tent::TransferEngineImpl& engine) {
        return engine.hasActiveRuntimeQueue();
    }
    // What the RDMA transport would install at construction; the tests swap
    // in a fake RDMA transport afterwards, so they install the provider here.
    static void setBandwidthProvider(tent::TransferEngineImpl& engine,
                                     double bytes_per_second) {
        engine.runtime_queue_->setDegradationPolicy(
            [bytes_per_second]() { return bytes_per_second; },
            tent::DegradationHooks{}, nullptr);
    }
    // Small chunks so a short request splits into several staged chunks.
    static void useSmallStagePool(tent::TransferEngineImpl& engine,
                                  size_t chunk_size, size_t chunk_count) {
        engine.staging_proxy_ = std::make_unique<tent::ProxyManager>(
            &engine, chunk_size, chunk_count);
    }
    static uint64_t creditDemandExchanges(tent::TransferEngineImpl& engine) {
        std::lock_guard<std::recursive_mutex> lk(engine.progress_mutex_);
        return engine.receiver_credit_demand_exchanges_;
    }
    static void adoptDeferredBatches(tent::TransferEngineImpl& engine,
                                     std::vector<tent::BatchID> batches) {
        tent::TransferEngineImpl::DeferredStageTeardown deferred;
        deferred.batches = std::move(batches);
        engine.adoptDeferredStageTeardown(std::move(deferred));
    }
};

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
    std::atomic<int> fail_next_submits{0};
    std::atomic<uint64_t> last_credit_session_high{0};
    std::atomic<uint64_t> last_credit_session_low{0};
    std::atomic<uint64_t> last_credit_epoch{0};
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
        int failures = fail_next_submits.load(std::memory_order_relaxed);
        while (failures > 0 &&
               !fail_next_submits.compare_exchange_weak(
                   failures, failures - 1, std::memory_order_relaxed)) {
        }
        if (failures > 0) {
            return Status::InternalError(
                "injected zero-handoff submit failure" LOC_MARK);
        }
        last_credit_session_high.store(batch->receiver_credit_session_high,
                                       std::memory_order_relaxed);
        last_credit_session_low.store(batch->receiver_credit_session_low,
                                      std::memory_order_relaxed);
        last_credit_epoch.store(batch->receiver_credit_epoch,
                                std::memory_order_relaxed);
        auto* fake = static_cast<FakeSubBatch*>(batch);
        for (const auto& request : requests) {
            fake->requests.push_back(request);
            fake->statuses.push_back(
                {TransferStatusEnum::COMPLETED, request.length});
            fake->poll_counts.push_back(0);
            ++fake->task_count;
        }
        last_batch_ = batch;
        if (notify_on_submit_) batch->notifyProgress();
        return Status::OK();
    }

    void notifyLastBatch() {
        if (last_batch_) last_batch_->notifyProgress();
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
    SubBatchRef last_batch_{nullptr};
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

void installFakeUb(TransferEngineImpl& engine,
                   const std::shared_ptr<FakeTransport>& fake_ub) {
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(fake_ub->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(UB, fake_ub);
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

Request makeLocalUbWrite(uint8_t* ptr, size_t length) {
    auto request = makeLocalWrite(ptr, length);
    request.transport_hint = UB;
    return request;
}

Request makeRemoteUbWrite(uint8_t* source, SegmentID target_id, uint8_t* target,
                          size_t length) {
    auto request = makeLocalUbWrite(source, length);
    request.target_id = target_id;
    request.target_offset = reinterpret_cast<uint64_t>(target);
    return request;
}

Request makeLocalNetworkWrite(uint8_t* ptr, size_t length) {
    auto request = makeLocalWrite(ptr, length);
    request.transport_hint = UNSPEC;
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

    std::atomic<bool> complete{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete](const Request& request, int) {
            return complete.load(std::memory_order_acquire)
                       ? TransferStatus{TransferStatusEnum::COMPLETED,
                                        request.length}
                       : TransferStatus{TransferStatusEnum::PENDING, 0};
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

    complete.store(true, std::memory_order_release);
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

// A drop is terminal on the queue side the moment pickForDispatch makes it,
// but the impl learns of it only through the dropped-owner list. Without that
// the impl's record for the owner outlives the batch, keeps the runtime queue
// "active" for the progress worker, and the batch's task stays PENDING.
TEST(RuntimeQueueDispatch, DroppedOwnerLeavesNoImplRecord) {
    auto cfg = makeRuntimeQueueConfig(4, 1UL << 20);
    cfg->set("runtime_queue/deadline_aware", true);
    cfg->set("runtime_queue/mlu_local_threshold", 1.0);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);
    // One byte per second: any request with a deadline is infeasible.
    TransferEngineImplTestPeer::setBandwidthProvider(engine, 1.0);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen, 0x55);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    auto request = makeLocalWrite(buffer.data(), kReqLen);
    request.deadline_ns =
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count()) +
        1'000'000'000ULL;
    ASSERT_TRUE(engine.submitTransfer(batch, {request}).ok());

    // Dropped, not dispatched.
    EXPECT_EQ(fake_rdma->submit_calls.load(), 0);
    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::CANCELED);

    // And nothing of it is left on either side.
    EXPECT_EQ(TransferEngineImplTestPeer::queuedOwners(engine), 0u);
    EXPECT_EQ(TransferEngineImplTestPeer::windowOwners(engine), 0u);
    EXPECT_EQ(TransferEngineImplTestPeer::queueOutstandingOwners(engine), 0u);
    EXPECT_FALSE(TransferEngineImplTestPeer::hasActiveRuntimeQueue(engine));

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

// A batch handed to the deferred staging teardown is never polled again, so
// a dispatched owner of it would hold its dispatch-window slot and its queue
// bytes for the rest of the process; with a one-owner window that is every
// later request stuck in the queue. An owner of the same batch still waiting
// in the queue has to go too, or it keeps its admission capacity instead.
TEST(RuntimeQueueDispatch, DeferredStagingBatchReleasesItsDispatchWindow) {
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    // Never completes: the owner stays dispatched until something detaches it.
    auto fake_rdma =
        std::make_shared<FakeTransport>(RDMA, [](const Request&, int) {
            return TransferStatus{TransferStatusEnum::PENDING, 0};
        });
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kReqLen = 4096;
    std::vector<uint8_t> buffer(kReqLen * 3, 0x55);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    // Two owners in the stranded batch: the window takes the first, the
    // second waits in the queue behind it.
    BatchID stranded = engine.allocateBatch(2);
    ASSERT_NE(stranded, (BatchID)0);
    ASSERT_TRUE(
        engine
            .submitTransfer(stranded,
                            {makeLocalWrite(buffer.data(), kReqLen),
                             makeLocalWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_EQ(TransferEngineImplTestPeer::windowOwners(engine), 1u);
    EXPECT_EQ(TransferEngineImplTestPeer::dispatchingBytes(engine), kReqLen);
    EXPECT_EQ(TransferEngineImplTestPeer::queuedOwners(engine), 2u);
    EXPECT_EQ(TransferEngineImplTestPeer::queueOutstandingOwners(engine), 2u);

    // The staging proxy gives up on it.
    TransferEngineImplTestPeer::adoptDeferredBatches(engine, {stranded});
    EXPECT_EQ(TransferEngineImplTestPeer::windowOwners(engine), 0u);
    EXPECT_EQ(TransferEngineImplTestPeer::windowBytes(engine), 0u);
    EXPECT_EQ(TransferEngineImplTestPeer::dispatchingBytes(engine), 0u);
    EXPECT_EQ(TransferEngineImplTestPeer::queuedOwners(engine), 0u);
    EXPECT_EQ(TransferEngineImplTestPeer::queueOutstandingOwners(engine), 0u);

    // The window is free again: the next request is dispatched, not queued.
    BatchID next = engine.allocateBatch(1);
    ASSERT_NE(next, (BatchID)0);
    ASSERT_TRUE(
        engine
            .submitTransfer(
                next, {makeLocalWrite(buffer.data() + 2 * kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);

    // `stranded` is parked for teardown and cannot be freed through the API.
    EXPECT_FALSE(engine.freeBatch(stranded).ok());
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

    // Keep the task in flight until the cancellation has been judged. The
    // runtime queue enables the ProgressWorker, which may poll the transport
    // before this test calls cancelTransfer(); a fake that completes on
    // submit would then let cancelTransfer() return OK for an already
    // terminal task instead of reaching the transport capability check.
    std::atomic<bool> complete{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete](const Request& request, int) {
            if (!complete.load()) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
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

    complete.store(true);
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

    // Keep the first owner PENDING until after the submit-window assertion.
    // Otherwise ProgressWorker can observe COMPLETED, refill, and bump
    // submit_calls to 2 before EXPECT_EQ(..., 1). Same race as #3459.
    std::atomic<bool> complete_first{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA,
        [&complete_first](const Request& request, int) {
            if (!complete_first.load()) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        },
        true);
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

    complete_first.store(true);
    fake_rdma->notifyLastBatch();

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

TEST(RuntimeQueueDispatch, AppendedShuffledRequestsKeepPublicTaskIds) {
    for (bool merge : {false, true}) {
        SCOPED_TRACE(merge);
        auto cfg = makeRuntimeQueueConfig(8, 1UL << 20, merge);
        cfg->set("enable_runtime_queue", false);
        cfg->set("enable_progress_worker", false);
        cfg->set("enable_auto_failover_on_poll", false);
        TransferEngineImpl engine(cfg);
        ASSERT_TRUE(engine.available());
        std::vector<uint8_t> buffer(1024);
        auto fake = std::make_shared<FakeTransport>(
            RDMA, [&](const Request& request, int) -> TransferStatus {
                const auto offset =
                    static_cast<uint8_t*>(request.source) - buffer.data();
                if (offset >= 128 && offset < 256) return {FAILED, 0};
                return {COMPLETED, request.length};
            });
        installFakeRdma(engine, fake);
        ASSERT_TRUE(
            engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

        auto batch = engine.allocateBatch(5);
        ASSERT_NE(batch, (BatchID)0);
        ASSERT_TRUE(
            engine
                .submitTransfer(batch, {makeLocalWrite(buffer.data() + 512, 8)})
                .ok());
        // Sorting produces two owners in address order, whereas the public
        // task ids retain submission order and start after the first submit.
        ASSERT_TRUE(
            engine
                .submitTransfer(batch, {makeLocalWrite(buffer.data() + 160, 16),
                                        makeLocalWrite(buffer.data(), 16),
                                        makeLocalWrite(buffer.data() + 128, 32),
                                        makeLocalWrite(buffer.data() + 16, 48)})
                .ok());
        const std::vector<TransferStatus> expected{{COMPLETED, 8},
                                                   {FAILED, 0},
                                                   {COMPLETED, 16},
                                                   {FAILED, 0},
                                                   {COMPLETED, 48}};
        for (size_t i = 0; i < expected.size(); ++i) {
            TransferStatus status{};
            ASSERT_TRUE(engine.getTransferStatus(batch, i, status).ok());
            EXPECT_EQ(status.s, expected[i].s) << "task " << i;
            EXPECT_EQ(status.transferred_bytes, expected[i].transferred_bytes)
                << "task " << i;
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

TEST(RuntimeQueueDispatch, ScatterCannotBypassRequiredReceiverCredit) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_ub = std::make_shared<FakeTransport>(UB);
    installFakeUb(engine, fake_ub);
    std::vector<uint8_t> buffer(kReqLen, 0x71);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    auto status =
        TransferEngineImplTestPeer::submitRequiringPostSubmitCancellation(
            engine, batch, {makeLocalUbWrite(buffer.data(), kReqLen)});
    EXPECT_TRUE(status.IsNotImplemented());
    EXPECT_EQ(fake_ub->submit_calls.load(), 0);
    EXPECT_EQ(TransferEngineImplTestPeer::queuedOwners(engine), 0u);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, ReceiverCreditDefersUbUntilTerminalRelease) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20, true);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    cfg->set("receiver_credit/freshness_ttl_ms", uint32_t{1000});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete_first{false};
    auto fake_ub = std::make_shared<FakeTransport>(
        UB, [&complete_first](const Request& request, int) {
            if (!complete_first.load(std::memory_order_acquire)) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeUb(engine, fake_ub);

    std::vector<uint8_t> buffer(kReqLen * 2, 0x71);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(
                batch, {makeLocalUbWrite(buffer.data(), kReqLen),
                        makeLocalUbWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    // Both owners fit the local dispatch window, but receiver credit permits
    // only one physical UB request. Merging must stop at the advertised hard
    // byte capacity, so the second owner remains queued instead of making an
    // inadmissible 8 KiB owner.
    EXPECT_EQ(fake_ub->submit_calls.load(), 1);
    TransferStatus second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_ub->submit_calls.load(), 1);

    complete_first.store(true, std::memory_order_release);
    TransferStatus first{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_ub->submit_calls.load(), 2);
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, ZeroHandoffUbFailureRollsCreditBack) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_ub = std::make_shared<FakeTransport>(UB);
    fake_ub->fail_next_submits.store(1);
    installFakeUb(engine, fake_ub);
    std::vector<uint8_t> buffer(kReqLen * 2, 0x72);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);

    ASSERT_TRUE(
        engine
            .submitTransfer(
                batch, {makeLocalUbWrite(buffer.data(), kReqLen),
                        makeLocalUbWrite(buffer.data() + kReqLen, kReqLen)})
            .ok());
    // The first submit failed before handoff. Its cumulative rollback is
    // reported to the receiver before the next owner gets a fresh grant.
    EXPECT_EQ(fake_ub->submit_calls.load(), 2);
    TransferStatus first{}, second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::FAILED);
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RemoteReceiverCreditExchangeGatesUbDispatch) {
    constexpr size_t kReqLen = 4096;
    auto receiver_cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    receiver_cfg->set("receiver_credit/receiver_enable", true);
    receiver_cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    receiver_cfg->set("receiver_credit/request_slots", uint64_t{1});
    auto sender_cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    sender_cfg->set("receiver_credit/sender_enable", true);
    sender_cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    sender_cfg->set("receiver_credit/request_slots", uint64_t{1});

    TransferEngineImpl receiver(receiver_cfg);
    TransferEngineImpl sender(sender_cfg);
    ASSERT_TRUE(receiver.available());
    ASSERT_TRUE(sender.available());

    auto receiver_ub = std::make_shared<FakeTransport>(UB);
    installFakeUb(receiver, receiver_ub);
    std::atomic<bool> complete_first{false};
    auto sender_ub = std::make_shared<FakeTransport>(
        UB, [&complete_first](const Request& request, int) {
            if (!complete_first.load(std::memory_order_acquire)) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeUb(sender, sender_ub);

    std::vector<uint8_t> source(kReqLen * 2, 0x81);
    std::vector<uint8_t> target(kReqLen * 2, 0);
    ASSERT_TRUE(sender.registerLocalMemory(source.data(), source.size()).ok());
    ASSERT_TRUE(
        receiver.registerLocalMemory(target.data(), target.size()).ok());
    SegmentID remote = LOCAL_SEGMENT_ID;
    ASSERT_TRUE(sender.openSegment(remote, receiver.getSegmentName()).ok());
    ASSERT_NE(remote, LOCAL_SEGMENT_ID);

    BatchID batch = sender.allocateBatch(2);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        sender
            .submitTransfer(
                batch, {makeRemoteUbWrite(source.data(), remote, target.data(),
                                          kReqLen),
                        makeRemoteUbWrite(source.data() + kReqLen, remote,
                                          target.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(sender_ub->submit_calls.load(), 1);

    // TCP fallback carries the same receiver-session fence on the wire. The
    // receiver rejects an unfenced or stale request even if the sender's
    // metadata cache still describes the old process incarnation.
    const auto receiver_addr = receiver.getSegmentName();
    const uint64_t session_high = sender_ub->last_credit_session_high.load();
    const uint64_t session_low = sender_ub->last_credit_session_low.load();
    const uint64_t epoch = sender_ub->last_credit_epoch.load();
    ASSERT_NE(session_high | session_low, 0u);
    ASSERT_NE(epoch, 0u);

    auto delegated = makeLocalUbWrite(target.data(), kReqLen);
    delegated.receiver_credit_session_high = session_high;
    delegated.receiver_credit_session_low = session_low;
    delegated.receiver_credit_epoch = epoch + 1;
    EXPECT_TRUE(
        ControlClient::delegate(receiver_addr, delegated).IsRpcServiceError());
    delegated.receiver_credit_epoch = epoch;
    ASSERT_TRUE(ControlClient::delegate(receiver_addr, delegated).ok());
    EXPECT_EQ(receiver_ub->submit_calls.load(), 1);

    delegated.receiver_credit_epoch = 0;
    EXPECT_TRUE(
        ControlClient::delegate(receiver_addr, delegated).IsInvalidArgument());

    EXPECT_TRUE(ControlClient::sendData(
                    receiver_addr, reinterpret_cast<uint64_t>(target.data()),
                    source.data(), kReqLen)
                    .IsRpcServiceError());
    EXPECT_EQ(target[0], 0);
    EXPECT_TRUE(ControlClient::sendData(
                    receiver_addr, reinterpret_cast<uint64_t>(target.data()),
                    source.data(), kReqLen, session_high, session_low,
                    epoch + 1)
                    .IsRpcServiceError());
    EXPECT_EQ(target[0], 0);
    const auto fenced_send = ControlClient::sendData(
        receiver_addr, reinterpret_cast<uint64_t>(target.data()), source.data(),
        kReqLen, session_high, session_low, epoch);
    ASSERT_TRUE(fenced_send.ok()) << fenced_send.ToString();
    EXPECT_EQ(target[0], source[0]);

    std::vector<uint8_t> readback(kReqLen, 0);
    EXPECT_TRUE(ControlClient::recvData(
                    receiver_addr, reinterpret_cast<uint64_t>(target.data()),
                    readback.data(), kReqLen, session_high, session_low,
                    epoch + 1)
                    .IsRpcServiceError());
    const auto fenced_recv = ControlClient::recvData(
        receiver_addr, reinterpret_cast<uint64_t>(target.data()),
        readback.data(), kReqLen, session_high, session_low, epoch);
    ASSERT_TRUE(fenced_recv.ok()) << fenced_recv.ToString();
    EXPECT_EQ(readback, std::vector<uint8_t>(kReqLen, 0x81));

    complete_first.store(true, std::memory_order_release);
    TransferStatus first{}, second{};
    ASSERT_TRUE(sender.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(sender_ub->submit_calls.load(), 2);
    ASSERT_TRUE(sender.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(sender.freeBatch(batch).ok());
    EXPECT_TRUE(sender.closeSegment(remote).ok());
    EXPECT_TRUE(
        sender.unregisterLocalMemory(source.data(), source.size()).ok());
    EXPECT_TRUE(
        receiver.unregisterLocalMemory(target.data(), target.size()).ok());
}

TEST(RuntimeQueueDispatch, RdmaFailureGatesUbFailoverWithReceiverCredit) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma =
        std::make_shared<FakeTransport>(RDMA, [](const Request&, int) {
            return TransferStatus{TransferStatusEnum::FAILED, 0};
        });
    auto fake_ub = std::make_shared<FakeTransport>(UB);
    installFakeRdma(engine, fake_rdma);
    installFakeUb(engine, fake_ub);

    std::vector<uint8_t> buffer(kReqLen, 0x91);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(engine
                    .submitTransfer(
                        batch, {makeLocalNetworkWrite(buffer.data(), kReqLen)})
                    .ok());
    ASSERT_EQ(fake_rdma->submit_calls.load(), 1);
    ASSERT_EQ(fake_ub->submit_calls.load(), 0);

    TransferStatus status{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(fake_ub->submit_calls.load(), 1);
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, ReceiverCreditAlsoGatesInitialRdmaPath) {
    constexpr size_t kReqLen = 4096;
    auto cfg = makeRuntimeQueueConfig(2, 1UL << 20);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    std::atomic<bool> complete_first{false};
    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, [&complete_first](const Request& request, int) {
            if (!complete_first.load(std::memory_order_acquire)) {
                return TransferStatus{TransferStatusEnum::PENDING, 0};
            }
            return TransferStatus{TransferStatusEnum::COMPLETED,
                                  request.length};
        });
    installFakeRdma(engine, fake_rdma);

    std::vector<uint8_t> buffer(kReqLen * 2, 0x92);
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
    EXPECT_NE(fake_rdma->last_credit_session_high.load() |
                  fake_rdma->last_credit_session_low.load(),
              0u);
    EXPECT_EQ(fake_rdma->last_credit_epoch.load(), 1u);

    complete_first.store(true, std::memory_order_release);
    TransferStatus first{}, second{};
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, first).ok());
    EXPECT_EQ(first.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(fake_rdma->submit_calls.load(), 2);
    ASSERT_TRUE(engine.getTransferStatus(batch, 1, second).ok());
    EXPECT_EQ(second.s, TransferStatusEnum::COMPLETED);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, RejectsRequestLargerThanReceiverCreditCapacity) {
    constexpr size_t kCapacity = 4096;
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kCapacity});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_ub = std::make_shared<FakeTransport>(UB);
    installFakeUb(engine, fake_ub);
    std::vector<uint8_t> buffer(kCapacity * 2, 0x93);
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);

    const auto status = engine.submitTransfer(
        batch, {makeLocalUbWrite(buffer.data(), buffer.size())});
    EXPECT_TRUE(status.IsTooManyRequests()) << status.ToString();
    EXPECT_EQ(fake_ub->submit_calls.load(), 0);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
}

TEST(RuntimeQueueDispatch, TcpControlDataUsesLegacyFramingWithoutCredit) {
    constexpr size_t kLength = 256;
    auto cfg = makeRuntimeQueueConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake_rdma);

    std::vector<uint8_t> source(kLength, 0xa5);
    std::vector<uint8_t> target(kLength, 0);
    std::vector<uint8_t> readback(kLength, 0);
    ASSERT_TRUE(engine.registerLocalMemory(target.data(), target.size()).ok());
    const auto server_addr = engine.getSegmentName();

    ASSERT_TRUE(ControlClient::sendData(
                    server_addr, reinterpret_cast<uint64_t>(target.data()),
                    source.data(), source.size())
                    .ok());
    EXPECT_EQ(target, source);
    ASSERT_TRUE(ControlClient::recvData(
                    server_addr, reinterpret_cast<uint64_t>(target.data()),
                    readback.data(), readback.size())
                    .ok());
    EXPECT_EQ(readback, source);

    EXPECT_TRUE(ControlClient::sendData(
                    server_addr, reinterpret_cast<uint64_t>(target.data()),
                    source.data(), source.size(), 1, 0, 0)
                    .IsInvalidArgument());
    EXPECT_TRUE(
        engine.unregisterLocalMemory(target.data(), target.size()).ok());
}

// A staged owner holds the receiver credit for all of its chunks; the chunks
// themselves are credit-exempt. If one chunk fails while a sibling's cross
// stage is still writing into the receiver, the owner must not turn terminal
// (and so must not return the credit) until that write has drained.
// Otherwise the receiver could grant the same capacity to another sender
// while the old write is still landing.
TEST(RuntimeQueueDispatch, StagedFailureKeepsReceiverCreditUntilChunksDrain) {
    constexpr size_t kChunk = 64;
    constexpr size_t kStagedLen = 2 * kChunk;
    constexpr size_t kSmallLen = 32;
    auto cfg = makeRuntimeQueueConfig(8, 1UL << 20);
    cfg->set("max_failover_attempts", 0);
    cfg->set("receiver_credit/enable", true);
    cfg->set("receiver_credit/data_bytes", uint64_t{kStagedLen});
    cfg->set("receiver_credit/request_slots", uint64_t{1});
    cfg->set("receiver_credit/freshness_ttl_ms", uint32_t{1000});
    std::vector<uint8_t> source(kStagedLen, 0x5a);
    std::vector<uint8_t> target(kStagedLen, 0);
    std::vector<uint8_t> small_source(kSmallLen, 0x6b);
    std::vector<uint8_t> small_target(kSmallLen, 0);
    // Declared after the buffers the fake transport and hook capture.
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());
    auto in = [](const void* addr, const std::vector<uint8_t>& buf) {
        auto p = reinterpret_cast<uintptr_t>(addr);
        auto base = reinterpret_cast<uintptr_t>(buf.data());
        return p >= base && p < base + buf.size();
    };

    std::atomic<bool> cross_inflight{false};
    std::atomic<bool> release_cross{false};
    std::atomic<int> failed_polls{0};
    std::atomic<int> small_polls{0};
    auto fake_tcp =
        std::make_shared<FakeTransport>(TCP, [&](const Request& request, int) {
            const auto* src = static_cast<const uint8_t*>(request.source);
            const auto* dst =
                reinterpret_cast<const void*>(request.target_offset);
            // Second chunk's local stage: fail only once the first chunk's
            // cross stage is writing into the receiver.
            if (src == source.data() + kChunk) {
                if (!cross_inflight.load()) return TransferStatus{PENDING, 0};
                ++failed_polls;
                return TransferStatus{FAILED, 0};
            }
            // Cross stage into the receiver: held until the test lets go.
            if (in(dst, target)) {
                cross_inflight = true;
                if (!release_cross.load()) return TransferStatus{PENDING, 0};
            }
            if (in(src, small_source)) ++small_polls;
            std::memcpy(const_cast<void*>(dst), request.source, request.length);
            return TransferStatus{COMPLETED, request.length};
        });
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(fake_tcp->install(seg_name, nullptr, nullptr).ok());
    engine.swapTransportForTest(TCP, fake_tcp);
    TransferEngineImplTestPeer::useSmallStagePool(engine, kChunk, 2);
    // CI has no device memory, so force host staging for the large write.
    engine.setStagingPolicyForTest(
        [&](const Request& request, std::vector<std::string>& policy) {
            if (request.source != source.data() || request.length != kStagedLen)
                return false;
            policy = {"", kWildcardLocation, ""};
            return true;
        });

    for (auto* buf : {&source, &target, &small_source, &small_target})
        ASSERT_TRUE(engine.registerLocalMemory(buf->data(), buf->size()).ok());

    auto make_write = [](std::vector<uint8_t>& from, std::vector<uint8_t>& to) {
        Request request;
        request.opcode = Request::WRITE;
        request.source = from.data();
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = reinterpret_cast<uint64_t>(to.data());
        request.length = from.size();
        request.transport_hint = TCP;
        return request;
    };
    auto wait_until = [](const std::function<bool()>& done) {
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!done() && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return done();
    };

    BatchID staged = engine.allocateBatch(1);
    ASSERT_NE(staged, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(staged, {make_write(source, target)}).ok());
    ASSERT_TRUE(wait_until([&] { return failed_polls.load() > 0; }))
        << "second chunk never failed while the first was in its cross stage";
    // Let the failure path run while the first chunk's write is held.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TransferStatus staged_status{};
    ASSERT_TRUE(engine.getTransferStatus(staged, 0, staged_status).ok());
    EXPECT_EQ(staged_status.s, PENDING);

    // The staged owner still holds the only request slot, so a second
    // credit-gated write must wait instead of reaching the transport.
    BatchID small = engine.allocateBatch(1);
    ASSERT_NE(small, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(small, {make_write(small_source, small_target)})
            .ok());
    TransferStatus small_status{};
    ASSERT_TRUE(engine.getTransferStatus(small, 0, small_status).ok());
    EXPECT_EQ(small_status.s, PENDING);
    EXPECT_EQ(small_polls.load(), 0);

    // Once the held write drains, the owner fails and returns the credit.
    release_cross = true;
    ASSERT_TRUE(wait_until([&] {
        return engine.getTransferStatus(staged, 0, staged_status).ok() &&
               staged_status.s != PENDING;
    }));
    EXPECT_EQ(staged_status.s, FAILED);
    ASSERT_TRUE(wait_until([&] {
        return engine.getTransferStatus(small, 0, small_status).ok() &&
               small_status.s != PENDING;
    }));
    EXPECT_EQ(small_status.s, COMPLETED);
    EXPECT_EQ(small_target, small_source);

    EXPECT_TRUE(engine.freeBatch(staged).ok());
    EXPECT_TRUE(engine.freeBatch(small).ok());
    for (auto* buf : {&source, &target, &small_source, &small_target})
        EXPECT_TRUE(
            engine.unregisterLocalMemory(buf->data(), buf->size()).ok());
}

// Receiver credit lives in another engine, so each demand exchange is an RPC
// made under progress_mutex_. A deferred owner must not repeat a refused
// exchange on every poll: back-to-back RPCs hold the lock and starve the
// in-flight owner whose completion is what returns the credit (on two UB
// nodes this collapsed throughput by ~1000x once demand exceeded capacity).
TEST(RuntimeQueueDispatch, RefusedCreditIsNotReexchangedOnEveryPoll) {
    constexpr size_t kReqLen = 4096;
    auto receiver_cfg = makeRuntimeQueueConfig(4, 1UL << 20);
    receiver_cfg->set("receiver_credit/receiver_enable", true);
    receiver_cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    receiver_cfg->set("receiver_credit/request_slots", uint64_t{1});
    auto sender_cfg = makeRuntimeQueueConfig(4, 1UL << 20);
    sender_cfg->set("receiver_credit/sender_enable", true);
    std::vector<uint8_t> source(kReqLen * 2, 0x91);
    std::vector<uint8_t> target(kReqLen * 2, 0);
    TransferEngineImpl receiver(receiver_cfg);
    TransferEngineImpl sender(sender_cfg);
    ASSERT_TRUE(receiver.available());
    ASSERT_TRUE(sender.available());
    installFakeUb(receiver, std::make_shared<FakeTransport>(UB));
    std::atomic<bool> complete_first{false};
    auto sender_ub =
        std::make_shared<FakeTransport>(UB, [&](const Request& request, int) {
            if (request.source == source.data() && !complete_first.load())
                return TransferStatus{PENDING, 0};
            return TransferStatus{COMPLETED, request.length};
        });
    installFakeUb(sender, sender_ub);
    ASSERT_TRUE(sender.registerLocalMemory(source.data(), source.size()).ok());
    ASSERT_TRUE(
        receiver.registerLocalMemory(target.data(), target.size()).ok());
    SegmentID remote = LOCAL_SEGMENT_ID;
    ASSERT_TRUE(sender.openSegment(remote, receiver.getSegmentName()).ok());

    BatchID holder = sender.allocateBatch(1);
    BatchID waiter = sender.allocateBatch(1);
    ASSERT_NE(holder, (BatchID)0);
    ASSERT_NE(waiter, (BatchID)0);
    ASSERT_TRUE(
        sender
            .submitTransfer(holder, {makeRemoteUbWrite(source.data(), remote,
                                                       target.data(), kReqLen)})
            .ok());
    ASSERT_TRUE(
        sender
            .submitTransfer(
                waiter, {makeRemoteUbWrite(source.data() + kReqLen, remote,
                                           target.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(sender_ub->submit_calls.load(), 1);

    // Busy-poll the refused owner. Without the backoff every poll is another
    // exchange (thousands here); with it, a handful (1, 2, 4, ... ms).
    const auto before =
        TransferEngineImplTestPeer::creditDemandExchanges(sender);
    TransferStatus waiter_status{};
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
    while (std::chrono::steady_clock::now() < until) {
        ASSERT_TRUE(sender.getTransferStatus(waiter, 0, waiter_status).ok());
        ASSERT_EQ(waiter_status.s, PENDING);
    }
    const auto refused =
        TransferEngineImplTestPeer::creditDemandExchanges(sender) - before;
    EXPECT_GE(refused, 1u);
    EXPECT_LE(refused, 12u);
    EXPECT_EQ(sender_ub->submit_calls.load(), 1);

    // Returning the credit retries at once, without waiting out the backoff.
    complete_first = true;
    TransferStatus holder_status{};
    ASSERT_TRUE(sender.getTransferStatus(holder, 0, holder_status).ok());
    EXPECT_EQ(holder_status.s, COMPLETED);
    EXPECT_EQ(sender_ub->submit_calls.load(), 2);
    ASSERT_TRUE(sender.getTransferStatus(waiter, 0, waiter_status).ok());
    EXPECT_EQ(waiter_status.s, COMPLETED);

    EXPECT_TRUE(sender.freeBatch(holder).ok());
    EXPECT_TRUE(sender.freeBatch(waiter).ok());
    EXPECT_TRUE(sender.closeSegment(remote).ok());
    EXPECT_TRUE(
        sender.unregisterLocalMemory(source.data(), source.size()).ok());
    EXPECT_TRUE(
        receiver.unregisterLocalMemory(target.data(), target.size()).ok());
}

// The refused sender is not told when another sender returns credit. The
// capped backoff must still let it through without any local release.
TEST(RuntimeQueueDispatch, RefusedCreditRecoversWhenAnotherSenderReleases) {
    constexpr size_t kReqLen = 4096;
    auto receiver_cfg = makeRuntimeQueueConfig(4, 1UL << 20);
    receiver_cfg->set("receiver_credit/receiver_enable", true);
    receiver_cfg->set("receiver_credit/data_bytes", uint64_t{kReqLen});
    receiver_cfg->set("receiver_credit/request_slots", uint64_t{1});
    auto sender_cfg = makeRuntimeQueueConfig(4, 1UL << 20);
    sender_cfg->set("receiver_credit/sender_enable", true);
    std::vector<uint8_t> source_a(kReqLen, 0xa1), source_b(kReqLen, 0xb2);
    std::vector<uint8_t> target(kReqLen * 2, 0);
    TransferEngineImpl receiver(receiver_cfg);
    TransferEngineImpl sender_a(sender_cfg);
    TransferEngineImpl sender_b(sender_cfg);
    ASSERT_TRUE(receiver.available());
    ASSERT_TRUE(sender_a.available());
    ASSERT_TRUE(sender_b.available());
    installFakeUb(receiver, std::make_shared<FakeTransport>(UB));
    std::atomic<bool> complete_a{false};
    auto ub_a =
        std::make_shared<FakeTransport>(UB, [&](const Request& request, int) {
            if (!complete_a.load()) return TransferStatus{PENDING, 0};
            return TransferStatus{COMPLETED, request.length};
        });
    auto ub_b = std::make_shared<FakeTransport>(UB);
    installFakeUb(sender_a, ub_a);
    installFakeUb(sender_b, ub_b);
    ASSERT_TRUE(
        sender_a.registerLocalMemory(source_a.data(), source_a.size()).ok());
    ASSERT_TRUE(
        sender_b.registerLocalMemory(source_b.data(), source_b.size()).ok());
    ASSERT_TRUE(
        receiver.registerLocalMemory(target.data(), target.size()).ok());
    SegmentID remote_a = LOCAL_SEGMENT_ID, remote_b = LOCAL_SEGMENT_ID;
    ASSERT_TRUE(sender_a.openSegment(remote_a, receiver.getSegmentName()).ok());
    ASSERT_TRUE(sender_b.openSegment(remote_b, receiver.getSegmentName()).ok());

    BatchID batch_a = sender_a.allocateBatch(1);
    BatchID batch_b = sender_b.allocateBatch(1);
    ASSERT_TRUE(sender_a
                    .submitTransfer(
                        batch_a, {makeRemoteUbWrite(source_a.data(), remote_a,
                                                    target.data(), kReqLen)})
                    .ok());
    ASSERT_TRUE(
        sender_b
            .submitTransfer(
                batch_b, {makeRemoteUbWrite(source_b.data(), remote_b,
                                            target.data() + kReqLen, kReqLen)})
            .ok());
    EXPECT_EQ(ub_a->submit_calls.load(), 1);
    EXPECT_EQ(ub_b->submit_calls.load(), 0);
    // Let sender B back off a few rounds while A holds the only slot.
    TransferStatus status_b{};
    const auto hold =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(40);
    while (std::chrono::steady_clock::now() < hold) {
        ASSERT_TRUE(sender_b.getTransferStatus(batch_b, 0, status_b).ok());
        ASSERT_EQ(status_b.s, PENDING);
    }

    complete_a = true;
    TransferStatus status_a{};
    ASSERT_TRUE(sender_a.getTransferStatus(batch_a, 0, status_a).ok());
    EXPECT_EQ(status_a.s, COMPLETED);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(sender_b.getTransferStatus(batch_b, 0, status_b).ok());
        if (status_b.s != PENDING) break;
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    EXPECT_EQ(status_b.s, COMPLETED);
    EXPECT_EQ(ub_b->submit_calls.load(), 1);

    EXPECT_TRUE(sender_a.freeBatch(batch_a).ok());
    EXPECT_TRUE(sender_b.freeBatch(batch_b).ok());
    EXPECT_TRUE(sender_a.closeSegment(remote_a).ok());
    EXPECT_TRUE(sender_b.closeSegment(remote_b).ok());
    EXPECT_TRUE(
        sender_a.unregisterLocalMemory(source_a.data(), source_a.size()).ok());
    EXPECT_TRUE(
        sender_b.unregisterLocalMemory(source_b.data(), source_b.size()).ok());
    EXPECT_TRUE(
        receiver.unregisterLocalMemory(target.data(), target.size()).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
