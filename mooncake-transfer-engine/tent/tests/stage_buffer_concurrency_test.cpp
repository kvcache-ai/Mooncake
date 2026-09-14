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

// ProxyManager::stage_buffers_ is reached from the kShards staging workers via
// StageBufferCache::allocateLocal and from the RPC thread via Pin/Unpin. The
// chunk bitmaps are atomic_flags, but the map holding them was unsynchronised,
// so a first-time pin could rehash under another thread's find.
//
// These drive the map concurrently. The unlocked failure is a rehash race,
// which a plain run may not reproduce -- run under -fsanitize=thread for that.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {
namespace {

class FakeSubBatch : public Transport::SubBatch {
   public:
    size_t size() const override { return 0; }
};

// Only the memory hooks matter here: ProxyManager allocates and registers a
// stage buffer through the engine, which forwards to the transport.
class FakeTransport : public Transport {
   public:
    FakeTransport() { caps.dram_to_dram = true; }

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
        delete batch;
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(SubBatchRef,
                               const std::vector<Request>&) override {
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef, int,
                             TransferStatus& status) override {
        status = {TransferStatusEnum::COMPLETED, 0};
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(RDMA);
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

    Status removeMemoryBuffer(BufferDesc&) override { return Status::OK(); }

    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions&) override {
        *addr = std::malloc(size);
        if (!*addr) return Status::InternalError("malloc failed" LOC_MARK);
        return Status::OK();
    }

    Status freeLocalMemory(void* addr, size_t) override {
        std::free(addr);
        return Status::OK();
    }

    bool warmupMemory(void*, size_t) override { return false; }

    const char* getName() const override { return "<fake>"; }
};

std::shared_ptr<Config> makeLoopbackP2PConfig() {
    auto cfg = std::make_shared<Config>();
    cfg->set("metadata_type", "p2p");
    cfg->set("metadata_servers", "");
    cfg->set("rpc_server_hostname", "127.0.0.1");
    cfg->set("rpc_server_port", "0");
    cfg->set("log_level", "warning");
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

// Small enough to keep the test cheap; ProxyManager's real default would
// allocate 4 MiB x 64 per location.
constexpr size_t kChunkSize = 4096;
constexpr size_t kChunkCount = 8;

class StageBufferConcurrencyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        engine_ = std::make_unique<TransferEngineImpl>(makeLoopbackP2PConfig());
        ASSERT_TRUE(engine_->available());
        auto fake = std::make_shared<FakeTransport>();
        std::string seg_name = engine_->getSegmentName();
        ASSERT_TRUE(fake->install(seg_name, nullptr, nullptr).ok());
        engine_->swapTransportForTest(RDMA, fake);
        proxy_ = std::make_unique<ProxyManager>(engine_.get(), kChunkSize,
                                                kChunkCount);
    }

    // Destruction only: ProxyManager::deconstruct() joins its workers and is
    // not idempotent, so calling it here as well would join twice.
    void TearDown() override { proxy_.reset(); }

    std::unique_ptr<TransferEngineImpl> engine_;
    std::unique_ptr<ProxyManager> proxy_;
};

// The dangerous shape: every thread inserts a different entry at the same
// time, so inserts and lookups overlap.
TEST_F(StageBufferConcurrencyTest, ConcurrentFirstPinOfDistinctLocations) {
    constexpr int kThreads = 8;
    std::vector<std::thread> threads;
    std::atomic<int> succeeded{0};
    std::vector<uint64_t> addrs(kThreads, 0);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            uint64_t addr = 0;
            if (proxy_->pinStageBuffer("cpu:" + std::to_string(i), addr).ok()) {
                addrs[i] = addr;
                succeeded.fetch_add(1);
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(succeeded.load(), kThreads);
    std::set<uint64_t> unique(addrs.begin(), addrs.end());
    EXPECT_EQ(unique.size(), static_cast<size_t>(kThreads))
        << "two locations handed out the same chunk";
}

// Same entry from every thread: the bitmap must hand each caller its own
// chunk, and the entry must be created exactly once.
TEST_F(StageBufferConcurrencyTest,
       ConcurrentPinOfOneLocationHandsOutDistinctChunks) {
    std::vector<std::thread> threads;
    std::vector<uint64_t> addrs(kChunkCount, 0);

    for (size_t i = 0; i < kChunkCount; ++i) {
        threads.emplace_back([&, i] {
            uint64_t addr = 0;
            if (proxy_->pinStageBuffer("cpu:0", addr).ok()) addrs[i] = addr;
        });
    }
    for (auto& t : threads) t.join();

    std::set<uint64_t> unique(addrs.begin(), addrs.end());
    EXPECT_EQ(unique.size(), kChunkCount) << "a chunk was pinned twice";
    EXPECT_EQ(unique.count(0), 0u) << "a pin failed";
}

// Unpin walks the whole map, so it reads entries other threads are inserting.
TEST_F(StageBufferConcurrencyTest, PinAndUnpinOverlap) {
    constexpr int kThreads = 8;
    std::vector<std::thread> threads;
    std::atomic<int> failures{0};

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            const std::string location = "cpu:" + std::to_string(i % 4);
            for (int round = 0; round < 32; ++round) {
                uint64_t addr = 0;
                if (!proxy_->pinStageBuffer(location, addr).ok()) continue;
                if (!proxy_->unpinStageBuffer(addr).ok()) failures.fetch_add(1);
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(failures.load(), 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
