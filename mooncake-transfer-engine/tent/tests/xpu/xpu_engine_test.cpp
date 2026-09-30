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

// Exercise registration -> routing -> ProxyManager -> real XPU copies, not
// just the standalone transport. Two P2P engines need no external metadata
// server. The same-host peer has a non-local segment ID, just like another
// process; the test never shares the peer's device address with a local copy.
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
// Report one completed device copy as failed, so ProxyManager has to retry
// orchestration. The underlying copy is synchronous and no I/O is abandoned.
class FailOnceXpuTransport : public tent::Transport {
   public:
    explicit FailOnceXpuTransport(std::shared_ptr<tent::Transport> delegate)
        : delegate_(std::move(delegate)) {
        caps = delegate_->capabilities();
    }
    tent::Status allocateSubBatch(SubBatchRef& batch, size_t size) override {
        return delegate_->allocateSubBatch(batch, size);
    }
    tent::Status freeSubBatch(SubBatchRef& batch) override {
        return delegate_->freeSubBatch(batch);
    }
    tent::Status submitTransferTasks(
        SubBatchRef batch,
        const std::vector<tent::Request>& requests) override {
        return delegate_->submitTransferTasks(batch, requests);
    }
    tent::Status getTransferStatus(SubBatchRef batch, int id,
                                   tent::TransferStatus& status) override {
        auto result = delegate_->getTransferStatus(batch, id, status);
        if (result.ok() && status.s == tent::COMPLETED &&
            !failed.exchange(true)) {
            status.s = tent::FAILED;
            status.transferred_bytes = 0;
        }
        return result;
    }
    tent::Status addMemoryBuffer(tent::BufferDesc& desc,
                                 const tent::MemoryOptions& options) override {
        return delegate_->addMemoryBuffer(desc, options);
    }
    tent::Status removeMemoryBuffer(tent::BufferDesc& desc) override {
        return delegate_->removeMemoryBuffer(desc);
    }
    std::atomic<bool> failed{false};

   private:
    std::shared_ptr<tent::Transport> delegate_;
};

class TransferEngineImplTestPeer {
   public:
    // Same entry point submitTransfer uses: a stale remote-segment cache or a
    // pooled RPC connection to a peer that has since exited is refreshed and
    // retried once, instead of surfacing as a spurious UNSPEC.
    static tent::SelectionResult route(tent::TransferEngineImpl& engine,
                                       const tent::Request& request) {
        return engine.resolveTransport(request, 0);
    }
    static bool hasXpuTag(tent::TransferEngineImpl& engine, void* address) {
        return hasTag(engine, address, tent::XPU);
    }
    static bool hasTag(tent::TransferEngineImpl& engine, void* address,
                       tent::TransportType type) {
        auto desc = engine.metadata_->segmentManager().getLocal();
        auto* buffer = desc->findBuffer(reinterpret_cast<uint64_t>(address), 1);
        return buffer &&
               std::find(buffer->transports.begin(), buffer->transports.end(),
                         type) != buffer->transports.end();
    }
    static bool rdmaGpuDirect(tent::TransferEngineImpl& engine) {
        auto& rdma = engine.transport_list_[tent::RDMA];
        return rdma && rdma->capabilities().gpu_to_gpu;
    }
    static std::shared_ptr<FailOnceXpuTransport> failNextCopy(
        tent::TransferEngineImpl& engine) {
        auto wrapped = std::make_shared<FailOnceXpuTransport>(
            engine.transport_list_[tent::XPU]);
        engine.swapTransportForTest(tent::XPU, wrapped);
        return wrapped;
    }
};
namespace tent {
namespace {
using Params = std::tuple<bool, bool, bool>;    // HP TCP, legacy, runtime queue
constexpr size_t kLength = (4UL << 20) + 4096;  // crosses a staging chunk

class XpuEngineTest : public ::testing::TestWithParam<Params> {
   protected:
    std::shared_ptr<Config> config() {
        auto c = std::make_shared<Config>();
        c->set("metadata_type", "p2p");
        c->set("metadata_servers", "");
        c->set("rpc_server_hostname", "127.0.0.1");
        c->set("rpc_server_port", "0");
        c->set("log_level", "warning");
        c->set("transports/tcp/enable", !std::get<0>(GetParam()));
        c->set("transports/hp_tcp/enable", std::get<0>(GetParam()));
        c->set("transports/rdma/enable", false);
        c->set("transports/shm/enable", false);
        c->set("transports/io_uring/enable", false);
        c->set("transports/mpcomm/enable", false);
        c->set("transports/xpu/enable", true);
        c->set("use_legacy_transport_selection", std::get<1>(GetParam()));
        c->set("enable_runtime_queue", std::get<2>(GetParam()));
        c->set("max_failover_attempts", 1);
        return c;
    }
    void SetUp() override {
        auto& platform = Platform::getLoader(config());
        MemoryOptions opts;
        opts.location = "xpu:0";
        void* probe = nullptr;
        if (!platform.allocate(&probe, 4096, opts).ok())
            GTEST_SKIP() << "No SYCL device available";
        ASSERT_TRUE(platform.free(probe, 4096).ok());
        a_ = std::make_unique<TransferEngineImpl>(config());
        b_ = std::make_unique<TransferEngineImpl>(config());
        ASSERT_TRUE(a_->available());
        ASSERT_TRUE(b_->available());
    }
    void TearDown() override {
        for (auto [engine, batch] : batches_)
            EXPECT_TRUE(engine->freeBatch(batch).ok());
        // Engine teardown drains staging before releasing tracked allocations.
        a_.reset();
        b_.reset();
    }
    void allocate(TransferEngineImpl& engine, const char* location, void** out,
                  TransportType registration = UNSPEC) {
        MemoryOptions opts;
        opts.location = location;
        ASSERT_TRUE(engine.allocateLocalMemory(out, kLength, opts).ok());
        ASSERT_NE(*out, nullptr);
        opts.type = registration;
        ASSERT_TRUE(engine.registerLocalMemory({*out}, {kLength}, opts).ok());
        EXPECT_TRUE(TransferEngineImplTestPeer::hasXpuTag(engine, *out));
    }
    TransferStatus wait(BatchID batch) {
        TransferStatus status{};
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(15);
        do {
            auto s = a_->progressBatch(batch, status);
            EXPECT_TRUE(s.ok()) << s.ToString();
            if (!s.ok() || status.s != PENDING) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        return status;
    }
    void transfer(const char* local_location, const char* peer_location,
                  Request::OpCode opcode, bool explicit_peer = false) {
        void *local = nullptr, *peer = nullptr;
        ASSERT_NO_FATAL_FAILURE(allocate(*a_, local_location, &local));
        ASSERT_NO_FATAL_FAILURE(
            allocate(*b_, peer_location, &peer, explicit_peer ? XPU : UNSPEC));
        std::vector<uint8_t> seed(kLength), zero(kLength, 0), readback(kLength);
        for (size_t i = 0; i < seed.size(); ++i) seed[i] = i % 251;
        auto& platform = Platform::getLoader();
        void* source = opcode == Request::WRITE ? local : peer;
        void* dest = opcode == Request::WRITE ? peer : local;
        ASSERT_TRUE(platform.copy(source, seed.data(), kLength).ok());
        ASSERT_TRUE(platform.copy(dest, zero.data(), kLength).ok());
        Request request{};
        request.opcode = opcode;
        request.source = local;
        request.target_offset = reinterpret_cast<uint64_t>(peer);
        request.length = kLength;
        ASSERT_TRUE(
            a_->openSegment(request.target_id, b_->getSegmentName()).ok());
        ASSERT_NE(request.target_id, LOCAL_SEGMENT_ID);
        const auto network = std::get<0>(GetParam()) ? HP_TCP : TCP;
        auto route = TransferEngineImplTestPeer::route(*a_, request);
        ASSERT_EQ(route.transport, network);
        ASSERT_EQ(route.staging_params.size(), 3u);
        // Only device sides are staged; the stage location must resolve.
        EXPECT_EQ(route.staging_params[1].empty(),
                  std::string_view(local_location) == "cpu:0");
        EXPECT_EQ(route.staging_params[2].empty(),
                  std::string_view(peer_location) == "cpu:0");
        // Submit without a hint: the engine must reach staging on its own.
        auto batch = a_->allocateBatch(1);
        ASSERT_NE(batch, 0u);
        batches_.push_back({a_.get(), batch});
        ASSERT_TRUE(a_->submitTransfer(batch, {request}).ok());
        auto status = wait(batch);
        ASSERT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kLength);
        ASSERT_TRUE(platform.copy(readback.data(), dest, kLength).ok());
        EXPECT_EQ(readback, seed);
    }
    std::unique_ptr<TransferEngineImpl> a_, b_;
    std::vector<std::pair<TransferEngineImpl*, BatchID>> batches_;
};

TEST_P(XpuEngineTest, DefaultRegistrationSelectsLocalXpuCopy) {
    void *device = nullptr, *host = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "xpu:0", &device));
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "cpu:0", &host));
    Request request{};
    request.opcode = Request::WRITE;
    request.source = device;
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(host);
    request.length = kLength;
    auto route = TransferEngineImplTestPeer::route(*a_, request);
    EXPECT_EQ(route.transport, XPU);
    EXPECT_TRUE(route.staging_params.empty());
}

TEST_P(XpuEngineTest, StagesDeviceToDeviceReadAndWrite) {
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0", "xpu:0", Request::WRITE, true));
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0", "xpu:0", Request::READ, true));
}

TEST_P(XpuEngineTest, StagesOneDeviceSideReadAndWrite) {
    ASSERT_NO_FATAL_FAILURE(transfer("cpu:0", "xpu:0", Request::WRITE, true));
    ASSERT_NO_FATAL_FAILURE(transfer("cpu:0", "xpu:0", Request::READ, true));
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0", "cpu:0", Request::WRITE));
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0", "cpu:0", Request::READ));
}

TEST_P(XpuEngineTest, RetryKeepsHostTransportBehindStaging) {
    if (!std::get<0>(GetParam()))
        GTEST_SKIP() << "TCP can recover inside the device stage";
    auto injected = TransferEngineImplTestPeer::failNextCopy(*a_);
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0", "xpu:0", Request::WRITE, true));
    EXPECT_TRUE(injected->failed.load());
}

TEST_P(XpuEngineTest, RejectsXpuHintForPeerSegment) {
    void *host = nullptr, *device = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "cpu:0", &host));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "xpu:0", &device));
    Request request{};
    request.source = host;
    request.target_offset = reinterpret_cast<uint64_t>(device);
    request.length = kLength;
    request.transport_hint = XPU;
    ASSERT_TRUE(a_->openSegment(request.target_id, b_->getSegmentName()).ok());
    auto route = TransferEngineImplTestPeer::route(*a_, request);
    EXPECT_EQ(route.transport, UNSPEC);
    EXPECT_TRUE(route.staging_params.empty());
}

// An unstaged host source is used as-is by the cross stage, so it must carry
// the host transport itself; XPU-only registration cannot be routed.
TEST_P(XpuEngineTest, UnstagedHostSourceMustCarryHostTransport) {
    void *host = nullptr, *device = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "cpu:0", &host, XPU));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "xpu:0", &device));
    Request request{};
    request.source = host;
    request.target_offset = reinterpret_cast<uint64_t>(device);
    request.length = kLength;
    ASSERT_TRUE(a_->openSegment(request.target_id, b_->getSegmentName()).ok());
    auto route = TransferEngineImplTestPeer::route(*a_, request);
    EXPECT_EQ(route.transport, UNSPEC);
}

INSTANTIATE_TEST_SUITE_P(NetworkAndDispatchModes, XpuEngineTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool(),
                                            ::testing::Bool()));

// GPUDirect-style path: with an RNIC whose verbs provide ibv_reg_dmabuf_mr,
// XPU buffers are registered through dma-buf export, carry RDMA, and route
// directly without staging. Requires real hardware; skips otherwise.
class XpuDirectRdmaTest : public ::testing::TestWithParam<bool> {
   protected:
    std::shared_ptr<Config> config() {
        auto c = std::make_shared<Config>();
        c->set("metadata_type", "p2p");
        c->set("metadata_servers", "");
        c->set("rpc_server_hostname", "127.0.0.1");
        c->set("rpc_server_port", "0");
        c->set("log_level", "warning");
        c->set("transports/tcp/enable", true);
        c->set("transports/hp_tcp/enable", false);
        c->set("transports/rdma/enable", true);
        c->set("transports/shm/enable", false);
        c->set("transports/io_uring/enable", false);
        c->set("transports/mpcomm/enable", false);
        c->set("transports/xpu/enable", true);
        c->set("use_legacy_transport_selection", GetParam());
        return c;
    }
    void SetUp() override {
        auto& platform = Platform::getLoader(config());
        MemoryOptions opts;
        opts.location = "xpu:0";
        void* probe = nullptr;
        if (!platform.allocate(&probe, 4096, opts).ok())
            GTEST_SKIP() << "No SYCL device available";
        DmabufExport exported;
        auto s = platform.exportDmabuf(probe, 4096, exported);
        ASSERT_TRUE(platform.free(probe, 4096).ok());
        if (!s.ok()) GTEST_SKIP() << "No dma-buf export: " << s;
        a_ = std::make_unique<TransferEngineImpl>(config());
        b_ = std::make_unique<TransferEngineImpl>(config());
        ASSERT_TRUE(a_->available());
        ASSERT_TRUE(b_->available());
        if (!TransferEngineImplTestPeer::rdmaGpuDirect(*a_))
            GTEST_SKIP() << "No RDMA device with ibv_reg_dmabuf_mr";
    }
    void TearDown() override {
        for (auto [engine, batch] : batches_)
            EXPECT_TRUE(engine->freeBatch(batch).ok());
        a_.reset();
        b_.reset();
    }
    void allocate(TransferEngineImpl& engine, const char* location,
                  void** out) {
        MemoryOptions opts;
        opts.location = location;
        ASSERT_TRUE(engine.allocateLocalMemory(out, kLength, opts).ok());
        ASSERT_NE(*out, nullptr);
        ASSERT_TRUE(engine.registerLocalMemory({*out}, {kLength}, opts).ok());
        EXPECT_TRUE(TransferEngineImplTestPeer::hasTag(engine, *out, RDMA))
            << location << " buffer was not registered on RDMA";
    }
    TransferStatus wait(TransferEngineImpl& engine, BatchID batch) {
        TransferStatus status{};
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(15);
        do {
            auto s = engine.progressBatch(batch, status);
            EXPECT_TRUE(s.ok()) << s.ToString();
            if (!s.ok() || status.s != PENDING) break;
            std::this_thread::yield();
        } while (std::chrono::steady_clock::now() < deadline);
        return status;
    }
    void transfer(const char* local_location, const char* peer_location,
                  Request::OpCode opcode) {
        void *local = nullptr, *peer = nullptr;
        ASSERT_NO_FATAL_FAILURE(allocate(*a_, local_location, &local));
        ASSERT_NO_FATAL_FAILURE(allocate(*b_, peer_location, &peer));
        Request request{};
        request.opcode = opcode;
        request.source = local;
        request.target_offset = reinterpret_cast<uint64_t>(peer);
        request.length = kLength;
        ASSERT_TRUE(
            a_->openSegment(request.target_id, b_->getSegmentName()).ok());
        auto route = TransferEngineImplTestPeer::route(*a_, request);
        // Direct: RDMA on the device address, no host stage on either side.
        ASSERT_EQ(route.transport, RDMA);
        EXPECT_TRUE(route.staging_params.empty());
        ASSERT_NO_FATAL_FAILURE(run(*a_, request, local, peer));
    }
    // Submit `request` (local -> peer for WRITE, peer -> local for READ) on
    // `engine` and verify the payload landed intact.
    void run(TransferEngineImpl& engine, const Request& request, void* local,
             void* peer) {
        std::vector<uint8_t> seed(kLength), zero(kLength, 0), readback(kLength);
        for (size_t i = 0; i < seed.size(); ++i) seed[i] = (i * 13) % 251;
        auto& platform = Platform::getLoader();
        void* source = request.opcode == Request::WRITE ? local : peer;
        void* dest = request.opcode == Request::WRITE ? peer : local;
        ASSERT_TRUE(platform.copy(source, seed.data(), kLength).ok());
        ASSERT_TRUE(platform.copy(dest, zero.data(), kLength).ok());
        auto batch = engine.allocateBatch(1);
        ASSERT_NE(batch, 0u);
        batches_.push_back({&engine, batch});
        ASSERT_TRUE(engine.submitTransfer(batch, {request}).ok());
        auto status = wait(engine, batch);
        ASSERT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kLength);
        ASSERT_TRUE(platform.copy(readback.data(), dest, kLength).ok());
        EXPECT_EQ(readback, seed);
    }
    std::unique_ptr<TransferEngineImpl> a_, b_;
    std::vector<std::pair<TransferEngineImpl*, BatchID>> batches_;
};

TEST_P(XpuDirectRdmaTest, DeviceToDeviceReadAndWrite) {
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0", "xpu:0", Request::WRITE));
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0", "xpu:0", Request::READ));
}

TEST_P(XpuDirectRdmaTest, OneDeviceSideReadAndWrite) {
    ASSERT_NO_FATAL_FAILURE(transfer("cpu:0", "xpu:0", Request::WRITE));
    ASSERT_NO_FATAL_FAILURE(transfer("xpu:0", "cpu:0", Request::READ));
}

// The dma-buf direct route exists on RDMA alone. A request hinted to another
// transport must not lose staging just because both buffers could have gone
// over RDMA directly.
TEST_P(XpuDirectRdmaTest, HostTransportHintKeepsStaging) {
    void *local = nullptr, *peer = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "xpu:0", &local));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "xpu:0", &peer));
    Request request{};
    request.opcode = Request::WRITE;
    request.source = local;
    request.target_offset = reinterpret_cast<uint64_t>(peer);
    request.length = kLength;
    request.transport_hint = TCP;
    ASSERT_TRUE(a_->openSegment(request.target_id, b_->getSegmentName()).ok());
    auto route = TransferEngineImplTestPeer::route(*a_, request);
    ASSERT_EQ(route.transport, TCP);
    ASSERT_EQ(route.staging_params.size(), 3u);
    EXPECT_FALSE(route.staging_params[1].empty());
    EXPECT_FALSE(route.staging_params[2].empty());
    ASSERT_NO_FATAL_FAILURE(run(*a_, request, local, peer));
}

// Same with a selector policy that only authorizes a DRAM-only host transport
// (HP TCP advertises no device capability, unlike TCP which copies device
// memory itself): the direct plan yields no route, so selection falls back to
// the staged plan. Both ends run HP TCP so the staged hop can execute.
TEST_P(XpuDirectRdmaTest, HostOnlyPolicyKeepsStaging) {
    if (GetParam()) GTEST_SKIP() << "Policies apply to selector mode only";
    auto peer_config = config();
    peer_config->set("transports/tcp/enable", false);
    peer_config->set("transports/hp_tcp/enable", true);
    auto target = std::make_unique<TransferEngineImpl>(peer_config);
    ASSERT_TRUE(target->available());
    auto c = config();
    c->set("transports/tcp/enable", false);
    c->set("transports/hp_tcp/enable", true);
    // The peer hop may only use HP TCP (XPU is local-segment only); the local
    // stage hop still needs the XPU copy transport.
    c->set("policy", json::parse(R"([{"name":"host_only",
        "segment_type":"memory","transports":["hp_tcp","xpu"]}])"));
    auto initiator = std::make_unique<TransferEngineImpl>(c);
    ASSERT_TRUE(initiator->available());
    void *local = nullptr, *peer = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*initiator, "xpu:0", &local));
    ASSERT_NO_FATAL_FAILURE(allocate(*target, "xpu:0", &peer));
    Request request{};
    request.opcode = Request::READ;
    request.source = local;
    request.target_offset = reinterpret_cast<uint64_t>(peer);
    request.length = kLength;
    ASSERT_TRUE(
        initiator->openSegment(request.target_id, target->getSegmentName())
            .ok());
    auto route = TransferEngineImplTestPeer::route(*initiator, request);
    ASSERT_EQ(route.transport, HP_TCP);
    ASSERT_EQ(route.staging_params.size(), 3u);
    EXPECT_FALSE(route.staging_params[1].empty());
    EXPECT_FALSE(route.staging_params[2].empty());
    ASSERT_NO_FATAL_FAILURE(run(*initiator, request, local, peer));
    // Drain before the engines go away with their batches still tracked.
    for (auto it = batches_.begin(); it != batches_.end();) {
        if (it->first == initiator.get() || it->first == target.get()) {
            EXPECT_TRUE(it->first->freeBatch(it->second).ok());
            it = batches_.erase(it);
        } else {
            ++it;
        }
    }
}

INSTANTIATE_TEST_SUITE_P(SelectionModes, XpuDirectRdmaTest, ::testing::Bool());
}  // namespace
}  // namespace tent
}  // namespace mooncake
