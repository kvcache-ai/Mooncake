// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace {
thread_local long allocation_failure_after = -1;
}

void* operator new(size_t size) {
    if (allocation_failure_after == 0) {
        allocation_failure_after = -1;
        throw std::bad_alloc();
    }
    if (allocation_failure_after > 0) --allocation_failure_after;
    if (void* result = std::malloc(size == 0 ? 1 : size)) return result;
    throw std::bad_alloc();
}
void* operator new[](size_t size) { return ::operator new(size); }
void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete[](void* ptr) noexcept { std::free(ptr); }
void operator delete(void* ptr, size_t) noexcept { std::free(ptr); }
void operator delete[](void* ptr, size_t) noexcept { std::free(ptr); }

namespace mooncake::tent {
namespace {

class ThrowingTransport : public Transport {
   public:
    struct AcceptedBatch : SubBatch {
        size_t size() const override { return requests.size(); }
        std::vector<Request> requests;
    };

    enum class Failure { None, Allocate, BeforePublish, AfterPrefix };
    Failure failure{Failure::None};
    std::atomic<bool> completed{false};
    int submit_calls{0};
    int retry_calls{0};
    bool needs_metadata_refresh{false};
    TransportType self_type;

    explicit ThrowingTransport(TransportType type) : self_type(type) {
        caps.dram_to_dram = true;
    }

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config> = nullptr) override {
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t capacity) override {
        if (failure == Failure::Allocate) throw std::bad_alloc();
        auto accepted = std::make_unique<AcceptedBatch>();
        accepted->requests.reserve(capacity);
        batch = accepted.release();
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        delete static_cast<AcceptedBatch*>(batch);
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        ++submit_calls;
        if (failure == Failure::BeforePublish) throw std::bad_alloc();
        auto& accepted = static_cast<AcceptedBatch*>(batch)->requests;
        for (const auto& request : requests) {
            accepted.push_back(request);
            if (failure == Failure::AfterPrefix) throw std::bad_alloc();
        }
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        const auto& requests = static_cast<AcceptedBatch*>(batch)->requests;
        if (task_id < 0 || static_cast<size_t>(task_id) >= requests.size())
            return Status::InvalidArgument("Unpublished task");
        if (needs_metadata_refresh) {
            status = {FAILED, 0};
            return Status::NeedsRefreshCache("Stale route");
        }
        status = {
            completed.load(std::memory_order_acquire) ? COMPLETED : PENDING,
            requests[task_id].length};
        return Status::OK();
    }

    Status retryTransferTask(SubBatchRef, int, const Request&) override {
        ++retry_calls;
        return Status::OK();
    }

    Status getSubBatchDrainStatus(SubBatchRef batch, bool& drained) override {
        drained =
            batch->size() == 0 || completed.load(std::memory_order_acquire);
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(self_type);
        return Status::OK();
    }

    Status removeMemoryBuffer(BufferDesc&) override { return Status::OK(); }
    const char* getName() const override { return "<throwing-rdma>"; }
};

class SubmitExceptionDrain : public ::testing::Test {
   protected:
    virtual bool queueEnabled() const { return false; }
    virtual TransportType transportType() const { return RDMA; }

    void SetUp() override {
        auto config = std::make_shared<Config>();
        config->set("metadata_type", "p2p");
        config->set("metadata_servers", "");
        config->set("rpc_server_hostname", "127.0.0.1");
        config->set("rpc_server_port", "0");
        config->set("log_level", "warning");
        config->set("merge_requests", false);
        config->set("enable_runtime_queue", queueEnabled());
        config->set("runtime_queue/max_outstanding_owners", 16UL);
        config->set("runtime_queue/max_outstanding_bytes", 1UL << 20);
        config->set("runtime_queue/max_dispatch_owners", 2UL);
        config->set("runtime_queue/max_dispatch_bytes", 1UL << 20);
        config->set("runtime_queue/staging_owner_reserve", 0UL);
        config->set("runtime_queue/staging_byte_reserve", 0UL);
        config->set("runtime_queue/progress_fallback_interval_us", 50000UL);
        for (const auto* name : {"tcp", "shm", "rdma", "io_uring", "nvlink",
                                 "mnnvl", "gds", "ascend_direct"})
            config->set(std::string("transports/") + name + "/enable", false);
        engine = std::make_unique<TransferEngineImpl>(config);
        ASSERT_TRUE(engine->available());
        transport = std::make_shared<ThrowingTransport>(transportType());
        engine->swapTransportForTest(transportType(), transport);
        buffer.resize(8192);
        ASSERT_TRUE(
            engine->registerLocalMemory(buffer.data(), buffer.size()).ok());
        batch = engine->allocateBatch(2);
        ASSERT_NE(batch, 0);
    }

    void TearDown() override {
        transport->completed.store(true, std::memory_order_release);
        if (batch) {
            EXPECT_TRUE(engine->freeBatch(batch).ok());
        }
        EXPECT_TRUE(
            engine->unregisterLocalMemory(buffer.data(), buffer.size()).ok());
    }

    Request request(size_t offset = 0) {
        Request result;
        result.opcode = Request::WRITE;
        result.source = buffer.data() + offset;
        result.target_id = LOCAL_SEGMENT_ID;
        result.target_offset = reinterpret_cast<uint64_t>(result.source);
        result.length = 4096;
        result.transport_hint = transportType();
        return result;
    }

    std::unique_ptr<TransferEngineImpl> engine;
    std::shared_ptr<ThrowingTransport> transport;
    std::vector<uint8_t> buffer;
    BatchID batch{0};
};

class QueuedSubmitExceptionDrain : public SubmitExceptionDrain {
   protected:
    bool queueEnabled() const override { return true; }
};

class HpSubmitExceptionDrain : public SubmitExceptionDrain {
   protected:
    TransportType transportType() const override { return HP_TCP; }
};

TEST_F(SubmitExceptionDrain, AllocationFailureHasNoPendingBufferOwner) {
    transport->failure = ThrowingTransport::Failure::Allocate;
    EXPECT_THROW(engine->submitTransfer(batch, {request()}), std::bad_alloc);
    bool drained = false;
    ASSERT_TRUE(engine->getBatchDrainStatus(batch, drained).ok());
    EXPECT_TRUE(drained);
    TransferStatus status;
    ASSERT_TRUE(engine->getTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, FAILED);
    EXPECT_EQ(transport->submit_calls, 0);
}

TEST_F(SubmitExceptionDrain, SubmitBeforePublicationHasNoPendingBufferOwner) {
    transport->failure = ThrowingTransport::Failure::BeforePublish;
    EXPECT_THROW(engine->submitTransfer(batch, {request()}), std::bad_alloc);
    bool drained = false;
    ASSERT_TRUE(engine->getBatchDrainStatus(batch, drained).ok());
    EXPECT_TRUE(drained);
    TransferStatus status;
    ASSERT_TRUE(engine->getTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, FAILED);
}

TEST_F(SubmitExceptionDrain, AcceptedPrefixDrainsBeforeBufferCanBeReused) {
    transport->failure = ThrowingTransport::Failure::AfterPrefix;
    EXPECT_THROW(engine->submitTransfer(batch, {request(), request(4096)}),
                 std::bad_alloc);
    bool drained = true;
    ASSERT_TRUE(engine->getBatchDrainStatus(batch, drained).ok());
    EXPECT_FALSE(drained);
    TransferStatus unpublished;
    ASSERT_TRUE(engine->getTransferStatus(batch, 1, unpublished).ok());
    EXPECT_EQ(unpublished.s, FAILED);
    transport->completed.store(true, std::memory_order_release);
    ASSERT_TRUE(engine->getBatchDrainStatus(batch, drained).ok());
    EXPECT_TRUE(drained);
    EXPECT_EQ(transport->submit_calls, 1);
}

TEST_F(SubmitExceptionDrain, ExceptionDoesNotRewriteEarlierSubmission) {
    transport->completed.store(true, std::memory_order_release);
    ASSERT_TRUE(engine->submitTransfer(batch, {request()}).ok());
    transport->failure = ThrowingTransport::Failure::BeforePublish;
    EXPECT_THROW(engine->submitTransfer(batch, {request(4096)}),
                 std::bad_alloc);
    TransferStatus earlier;
    ASSERT_TRUE(engine->getTransferStatus(batch, 0, earlier).ok());
    EXPECT_EQ(earlier.s, COMPLETED);
    bool drained = false;
    ASSERT_TRUE(engine->getBatchDrainStatus(batch, drained).ok());
    EXPECT_TRUE(drained);
}

TEST_F(HpSubmitExceptionDrain, AcceptedExceptionDisablesMetadataRetry) {
    transport->failure = ThrowingTransport::Failure::AfterPrefix;
    EXPECT_THROW(engine->submitTransfer(batch, {request()}), std::bad_alloc);
    transport->completed.store(true, std::memory_order_release);
    transport->needs_metadata_refresh = true;
    TransferStatus status;
    ASSERT_TRUE(engine->getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
    EXPECT_EQ(transport->retry_calls, 0);
}

TEST_F(QueuedSubmitExceptionDrain, AllocationFailureReleasesAdmittedOwners) {
    transport->failure = ThrowingTransport::Failure::Allocate;
    EXPECT_THROW(engine->submitTransfer(batch, {request(), request(4096)}),
                 std::bad_alloc);
    bool drained = false;
    ASSERT_TRUE(engine->getBatchDrainStatus(batch, drained).ok());
    EXPECT_TRUE(drained);
    TransferStatus status;
    ASSERT_TRUE(engine->getTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, FAILED);
    EXPECT_EQ(transport->submit_calls, 0);
}

TEST_F(QueuedSubmitExceptionDrain, AcceptedOwnerSurvivesDispatchException) {
    transport->failure = ThrowingTransport::Failure::AfterPrefix;
    EXPECT_THROW(engine->submitTransfer(batch, {request(), request(4096)}),
                 std::bad_alloc);
    bool drained = true;
    ASSERT_TRUE(engine->getBatchDrainStatus(batch, drained).ok());
    EXPECT_FALSE(drained);
    TransferStatus unpublished;
    ASSERT_TRUE(engine->getTransferStatus(batch, 1, unpublished).ok());
    EXPECT_EQ(unpublished.s, FAILED);
    transport->completed.store(true, std::memory_order_release);
    ASSERT_TRUE(engine->getBatchDrainStatus(batch, drained).ok());
    EXPECT_TRUE(drained);
    EXPECT_EQ(transport->submit_calls, 1);
}

TEST_F(QueuedSubmitExceptionDrain, AllocationFaultsLeaveNoQueuedBufferOwner) {
    transport->completed.store(true, std::memory_order_release);
    const std::vector<Request> requests{request(), request(4096)};
    int failures = 0;
    bool exhausted = false;
    for (long fail_after = 0; fail_after < 128; ++fail_after) {
        bool threw = false;
        allocation_failure_after = fail_after;
        try {
            auto status = engine->submitTransfer(batch, requests);
            allocation_failure_after = -1;
            ASSERT_TRUE(status.ok());
        } catch (const std::bad_alloc&) {
            allocation_failure_after = -1;
            threw = true;
        }
        if (threw) ++failures;
        if (!threw) {
            // Ordinary successful submissions progress through the public
            // status API; only failed submissions rely on drain-only polling.
            TransferStatus status;
            ASSERT_TRUE(engine->getTransferStatus(batch, status).ok());
            ASSERT_EQ(status.s, COMPLETED);
        }
        bool drained = false;
        ASSERT_TRUE(engine->getBatchDrainStatus(batch, drained).ok());
        ASSERT_TRUE(drained) << "failure index " << fail_after;
        ASSERT_TRUE(engine->freeBatch(batch).ok());
        batch = 0;
        if (!threw) {
            exhausted = true;
            break;
        }
        batch = engine->allocateBatch(2);
        ASSERT_NE(batch, 0);
    }
    EXPECT_TRUE(exhausted);
    EXPECT_GT(failures, 4);
}

TEST(AdmissionExceptionDrain, InsertionFailureRollsBackAllUnpublishedOwners) {
    int failures = 0;
    bool exhausted = false;
    for (long fail_after = 0; fail_after < 128; ++fail_after) {
        QueueLimits limits;
        limits.max_outstanding_owners = 16;
        limits.max_outstanding_bytes = 1UL << 20;
        LocalTransferAdmissionQueue queue(limits);
        QueueSubmit existing;
        existing.batch_token = 1;
        existing.batch_slots_left = 1;
        QueueOwnerInput old_owner;
        old_owner.request.length = 4096;
        existing.owners.push_back(old_owner);
        std::vector<QueueOwnerId> old_ids;
        ASSERT_TRUE(queue.tryAdmit(existing, old_ids).ok());
        QueueSubmit next;
        next.batch_token = 2;
        next.batch_slots_left = 3;
        auto new_owner = old_owner;
        new_owner.derived_task_ids.push_back(1);
        next.owners.push_back(new_owner);
        new_owner.owner_task_id = 2;
        new_owner.derived_task_ids.clear();
        next.owners.push_back(new_owner);
        std::vector<QueueOwnerId> ids;
        bool threw = false;
        allocation_failure_after = fail_after;
        try {
            auto status = queue.tryAdmit(next, ids);
            allocation_failure_after = -1;
            ASSERT_TRUE(status.ok());
        } catch (const std::bad_alloc&) {
            allocation_failure_after = -1;
            threw = true;
        }
        if (!threw) {
            exhausted = true;
            break;
        }
        ++failures;
        EXPECT_TRUE(ids.empty()) << "failure index " << fail_after;
        EXPECT_EQ(queue.outstandingOwners(), 1);
        EXPECT_EQ(queue.outstandingBytes(), 4096);
        auto picked = queue.pickForDispatch(16, 1UL << 20);
        EXPECT_EQ(picked, old_ids) << "failure index " << fail_after;
        ASSERT_TRUE(queue.tryAdmit(next, ids).ok())
            << "failed insertion left a public task mapping at index "
            << fail_after;
        EXPECT_EQ(ids.size(), 2);
    }
    EXPECT_TRUE(exhausted);
    EXPECT_GT(failures, 4);
}

}  // namespace
}  // namespace mooncake::tent
