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
// Regression test for PR #3526: ProxyManager::deconstruct() bounds its drain
// loop with a deadline, and a timed-out drain must not free resources the
// transport workers can still touch. With a transport whose transfers never
// leave PENDING, the destructor has to (a) return once the configured
// deadline expires instead of spinning forever, and (b) hand the undrained
// batches and their local stage buffer arenas to TransferEngineImpl for
// deferred teardown instead of releasing them while a worker could still
// write slice status into them. Runs on plain CPU; no RDMA device required.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {

namespace {

// Minimal shape borrowed from progress_worker_test.cpp, reduced to the one
// behavior this test needs: every submitted task stays PENDING forever, so
// the shutdown drain loop can only leave through its deadline.
class NeverCompletingSubBatch : public Transport::SubBatch {
   public:
    size_t size() const override { return task_count; }
    size_t task_count = 0;
    std::vector<Request> requests;
};

class NeverCompletingTransport : public Transport {
   public:
    NeverCompletingTransport() { caps.dram_to_dram = true; }

    std::atomic<int> submit_calls{0};
    std::atomic<int> free_local_memory_calls{0};

    Status install(std::string& /*local_segment_name*/,
                   std::shared_ptr<ControlService> /*metadata*/,
                   std::shared_ptr<Topology> /*local_topology*/,
                   std::shared_ptr<Config> /*conf*/ = nullptr) override {
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t /*max_size*/) override {
        batch = new NeverCompletingSubBatch();
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
        auto* fb = static_cast<NeverCompletingSubBatch*>(batch);
        for (const auto& req : request_list) {
            fb->requests.push_back(req);
            fb->task_count++;
        }
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto* fb = static_cast<NeverCompletingSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fb->requests.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        status.s = TransferStatusEnum::PENDING;
        status.transferred_bytes = 0;
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& /*options*/) override {
        desc.transports.push_back(TCP);
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
        ++free_local_memory_calls;
        std::free(addr);
        return Status::OK();
    }

    bool warmupMemory(void* /*addr*/, size_t /*length*/) override {
        return false;
    }

    const char* getName() const override { return "<never-completing>"; }
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

TEST(ProxyShutdownTimeout, TimedOutDrainHandsResourcesToDeferredTeardown) {
    constexpr long kDrainTimeoutMs = 200;

    auto cfg = makeMinimalP2PConfig();
    cfg->set("staging/shutdown_drain_timeout_ms", kDrainTimeoutMs);
    auto engine = std::make_unique<TransferEngineImpl>(cfg);
    ASSERT_TRUE(engine->available());

    auto fake_tcp = std::make_shared<NeverCompletingTransport>();
    std::string seg = engine->getSegmentName();
    ASSERT_TRUE(fake_tcp->install(seg, nullptr, nullptr).ok());
    engine->swapTransportForTest(TCP, fake_tcp);

    constexpr size_t kBufferLength = 128;
    std::vector<uint8_t> source(kBufferLength, 0x91);
    std::vector<uint8_t> target(kBufferLength, 0x00);
    ASSERT_TRUE(engine->registerLocalMemory(source.data(), source.size()).ok());
    ASSERT_TRUE(engine->registerLocalMemory(target.data(), target.size()).ok());

    // 64-byte chunks over a 128-byte request: two chunks, each with its own
    // local-stage batch that will never leave PENDING.
    auto manager = std::make_unique<ProxyManager>(engine.get(), 64, 2);
    Request request;
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = kBufferLength;
    const std::vector<std::string> params = {"", kWildcardLocation, ""};

    TaskInfo task;
    task.request = request;
    task.staging = true;
    ASSERT_TRUE(manager->submit(&task, 0, params).ok());

    // Both chunk batches must be in flight before shutdown, otherwise the
    // drain loop has nothing to time out on.
    const auto submit_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (fake_tcp->submit_calls.load(std::memory_order_acquire) < 2 &&
           std::chrono::steady_clock::now() < submit_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_GE(fake_tcp->submit_calls.load(std::memory_order_acquire), 2);

    // The destructor must come back once the configured deadline expires:
    // well past kDrainTimeoutMs, but nowhere near the 30s default (let alone
    // the unbounded wait this regression test guards against).
    const auto destroy_start = std::chrono::steady_clock::now();
    manager.reset();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - destroy_start);
    EXPECT_GE(elapsed.count(), kDrainTimeoutMs / 2);
    EXPECT_LT(elapsed.count(), 10000);

    // The ownership-transfer path must have run: both undrained batches were
    // detached from the normal lifecycle and adopted by the engine, and the
    // stage buffer arena they write into was not freed through the transport.
    EXPECT_EQ(engine->deferredStageTeardownBatchCountForTest(), 2u);
    EXPECT_EQ(engine->aliveBatchCountForTest(), 0u);
    EXPECT_EQ(fake_tcp->free_local_memory_calls.load(std::memory_order_acquire),
              0);

    EXPECT_TRUE(
        engine->unregisterLocalMemory(source.data(), source.size()).ok());
    EXPECT_TRUE(
        engine->unregisterLocalMemory(target.data(), target.size()).ok());

    // Engine teardown abandons the deferred resources only after the
    // transports quiesce; the arena is intentionally leaked, never released
    // through a destroyed transport. ASan/LSan in CI verify there is no
    // double-free, use-after-free, or unreachable allocation on this path.
    engine.reset();
    EXPECT_EQ(fake_tcp->free_local_memory_calls.load(std::memory_order_acquire),
              0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
