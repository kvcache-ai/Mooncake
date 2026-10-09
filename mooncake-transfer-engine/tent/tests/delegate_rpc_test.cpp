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

// End-to-end coverage for the Delegate RPC, which had none:
// ControlClient::delegate -> onDelegate -> TransferEngineImpl::transferSync.
//
// The engine runs in p2p mode on loopback, so getSegmentName() is the address
// to delegate to, and a FakeTransport stands in for real hardware.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/control_plane.h"
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
    std::vector<TransferStatus> statuses;
};

// Minimal always-succeeding transport: enough for checkAvailability() and
// resolveTransport() to pick it, and it records how many submits it saw.
class FakeTransport : public Transport {
   public:
    explicit FakeTransport(TransportType self_type) : self_type_(self_type) {
        caps.dram_to_dram = true;
    }

    std::atomic<int> submit_calls{0};

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

    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override {
        ++submit_calls;
        auto* fb = static_cast<FakeSubBatch*>(batch);
        for (const auto& req : request_list) {
            fb->statuses.push_back({TransferStatusEnum::COMPLETED, req.length});
            fb->task_count++;
        }
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto* fb = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fb->statuses.size())
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        status = fb->statuses[task_id];
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
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

   private:
    TransportType self_type_;
};

std::shared_ptr<Config> makeLoopbackP2PConfig() {
    auto cfg = std::make_shared<Config>();
    // p2p metadata keeps this self-contained: no redis/etcd/http server, and
    // the segment name becomes 127.0.0.1:<bound port>, which is exactly the
    // address Delegate is sent to.
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
    return cfg;
}

constexpr size_t kBufLen = 4096;

class DelegateRpcTest : public ::testing::Test {
   protected:
    void SetUp() override {
        engine_ = std::make_unique<TransferEngineImpl>(makeLoopbackP2PConfig());
        ASSERT_TRUE(engine_->available());

        fake_ = std::make_shared<FakeTransport>(RDMA);
        std::string seg_name = engine_->getSegmentName();
        ASSERT_TRUE(fake_->install(seg_name, nullptr, nullptr).ok());
        engine_->swapTransportForTest(RDMA, fake_);

        buffer_.assign(kBufLen, 0xC1);
        ASSERT_TRUE(engine_->registerLocalMemory(buffer_.data(), kBufLen).ok());
        addr_ = engine_->getSegmentName();
    }

    void TearDown() override {
        if (engine_)
            (void)engine_->unregisterLocalMemory(buffer_.data(), kBufLen);
    }

    // A self-targeted WRITE: the serving node is this same engine, so the
    // pointers carried in the request are valid there.
    Request makeRequest() const {
        Request req;
        req.opcode = Request::WRITE;
        req.source = const_cast<uint8_t*>(buffer_.data());
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = reinterpret_cast<uint64_t>(buffer_.data());
        req.length = kBufLen;
        return req;
    }

    std::unique_ptr<TransferEngineImpl> engine_;
    std::shared_ptr<FakeTransport> fake_;
    std::vector<uint8_t> buffer_;
    std::string addr_;
};

TEST_F(DelegateRpcTest, DelegatedTransferRunsOnTheServingNode) {
    ASSERT_TRUE(ControlClient::delegate(addr_, makeRequest()).ok());
    EXPECT_EQ(fake_->submit_calls.load(), 1)
        << "the delegated transfer never reached the transport";
}

TEST_F(DelegateRpcTest, DelegateReportsFailureBackToTheCaller) {
    // Nothing is registered at this address, so submitTransfer must reject it
    // and the error has to travel back over the RPC rather than being
    // swallowed into an OK response.
    Request req = makeRequest();
    req.target_offset = 0;
    req.source = nullptr;
    EXPECT_FALSE(ControlClient::delegate(addr_, req).ok());
}

// onDelegate is registered with offload=true, so N delegates now run on N
// executor threads instead of being serialized by the single RPC thread. Each
// must still get its own batch and its own correct answer.
TEST_F(DelegateRpcTest, ConcurrentDelegatesAllSucceed) {
    constexpr int kConcurrency = 8;
    std::vector<std::thread> threads;
    std::atomic<int> succeeded{0};
    for (int i = 0; i < kConcurrency; ++i) {
        threads.emplace_back([&] {
            if (ControlClient::delegate(addr_, makeRequest()).ok())
                succeeded.fetch_add(1);
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(succeeded.load(), kConcurrency);
    EXPECT_EQ(fake_->submit_calls.load(), kConcurrency);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
