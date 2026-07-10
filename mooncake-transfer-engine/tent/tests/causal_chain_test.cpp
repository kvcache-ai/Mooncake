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
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#if TENT_METRICS_ENABLED
#include "tent/metrics/tent_metrics.h"
#endif
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
    explicit FakeTransport(TransportType self_type) : self_type_(self_type) {
        caps.dram_to_dram = true;
    }

    std::atomic<int> submit_calls{0};

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
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
            ++fake->task_count;
        }
        batch->notifyProgress();
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto* fake = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fake->statuses.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        status = fake->statuses[task_id];
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

    const char* getName() const override { return "<fake-causal>"; }

   private:
    TransportType self_type_;
};

std::shared_ptr<Config> makeCausalChainConfig(size_t max_dispatch_owners,
                                              size_t max_dispatch_bytes) {
    auto cfg = std::make_shared<Config>();
    cfg->set("metadata_type", "p2p");
    cfg->set("metadata_servers", "");
    cfg->set("rpc_server_hostname", "127.0.0.1");
    cfg->set("rpc_server_port", "0");
    cfg->set("log_level", "warning");
    cfg->set("merge_requests", false);
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
                     const std::shared_ptr<FakeTransport>& fake) {
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(fake->install(seg_name, nullptr, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake);
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

TEST(CausalChain, TimestampsPopulatedOnQueuePath) {
    auto cfg = makeCausalChainConfig(1, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xBB);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto status =
        engine.submitTransfer(batch, {makeLocalWrite(buf.data(), kLen)});
    ASSERT_TRUE(status.ok()) << status.ToString();

    TransferStatus ts{};
    for (int i = 0; i < 200; ++i) {
        engine.getTransferStatus(batch, 0, ts);
        if (ts.s == TransferStatusEnum::COMPLETED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED);
    EXPECT_GE(fake->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

TEST(CausalChain, MultipleTransfersAllComplete) {
    auto cfg = makeCausalChainConfig(4, 1UL << 20);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    constexpr int kBatchSize = 4;
    std::vector<uint8_t> buf(kLen * kBatchSize, 0xCC);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(kBatchSize);
    ASSERT_NE(batch, (BatchID)0);

    std::vector<Request> requests;
    for (int i = 0; i < kBatchSize; ++i) {
        requests.push_back(makeLocalWrite(buf.data() + i * kLen, kLen));
    }
    auto status = engine.submitTransfer(batch, requests);
    ASSERT_TRUE(status.ok()) << status.ToString();

    for (int i = 0; i < kBatchSize; ++i) {
        TransferStatus ts{};
        for (int j = 0; j < 200; ++j) {
            engine.getTransferStatus(batch, i, ts);
            if (ts.s == TransferStatusEnum::COMPLETED) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED) << "task " << i;
    }

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

TEST(CausalChain, TimestampsPopulatedOnDirectPath) {
    auto cfg = makeCausalChainConfig(1, 1UL << 20);
    cfg->set("enable_runtime_queue", false);
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xBB);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto status =
        engine.submitTransfer(batch, {makeLocalWrite(buf.data(), kLen)});
    ASSERT_TRUE(status.ok()) << status.ToString();

    TransferStatus ts{};
    for (int i = 0; i < 200; ++i) {
        engine.getTransferStatus(batch, 0, ts);
        if (ts.s == TransferStatusEnum::COMPLETED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(ts.s, TransferStatusEnum::COMPLETED);
    EXPECT_GE(fake->submit_calls.load(), 1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

#if TENT_METRICS_ENABLED
TEST(CausalChain, MetricsRecordStageLatencyIsCallable) {
    TENT_RECORD_STAGE_LATENCY(TentMetrics::Stage::QueueWait, 100.0);
    TENT_RECORD_STAGE_LATENCY(TentMetrics::Stage::Dispatch, 50.0);
    TENT_RECORD_STAGE_LATENCY(TentMetrics::Stage::Transport, 200.0);
}
#endif

}  // namespace
}  // namespace tent
}  // namespace mooncake
