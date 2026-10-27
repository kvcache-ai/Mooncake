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

#include <cnrt.h>
#include <cn_api.h>
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <vector>
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/transport/rdma/context.h"
#include "tent/transport/rdma/rdma_transport.h"

#define MLU_SKIP_UNLESS_REQUIRED(message)         \
    do {                                          \
        if (getenv("MC_TENT_MLU_REQUIRE_RDMA")) { \
            FAIL() << message;                    \
        }                                         \
        GTEST_SKIP() << message;                  \
    } while (false)

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static tent::SelectionResult route(tent::TransferEngineImpl& engine,
                                       const tent::Request& request) {
        return engine.resolveTransport(request, 0);
    }
    static bool hasTag(tent::TransferEngineImpl& engine, void* ptr,
                       tent::TransportType type) {
        auto desc = engine.metadata_->segmentManager().getLocal();
        auto* buffer = desc->findBuffer(reinterpret_cast<uint64_t>(ptr), 1);
        return buffer &&
               std::find(buffer->transports.begin(), buffer->transports.end(),
                         type) != buffer->transports.end();
    }
    static bool rdmaGpuDirect(tent::TransferEngineImpl& engine) {
        return engine.transport_list_[tent::RDMA] &&
               engine.transport_list_[tent::RDMA]->capabilities().gpu_to_gpu;
    }
    static std::shared_ptr<tent::Transport> transport(
        tent::TransferEngineImpl& engine) {
        return engine.transport_list_[tent::RDMA];
    }
};
namespace tent {
class RdmaTransportTestPeer {
   public:
    static void rejectDeviceExport(RdmaTransport& transport) {
        for (auto& context : transport.context_set_) {
            if (context)
                context->setDmabufExporter([](void* ptr, size_t length,
                                              DmabufExport& out) {
                    if (Platform::getLoader().getMemoryType(ptr) == MTYPE_MLU)
                        return Status::NotImplemented(
                            "injected unsupported allocation");
                    return Platform::getLoader().exportDmabuf(ptr, length, out);
                });
        }
    }
};
namespace {
constexpr size_t kLength = (1UL << 20) + 123;
constexpr size_t kAllocation = 2UL << 20;
class MluDirectRdmaTest : public ::testing::TestWithParam<bool> {
   protected:
    std::shared_ptr<Config> config() {
        auto c = std::make_shared<Config>();
        c->set("metadata_type", "p2p");
        c->set("metadata_servers", "");
        c->set("rpc_server_hostname", "127.0.0.1");
        c->set("rpc_server_port", "0");
        c->set("log_level", "warning");
        if (const char* gid = getenv("MC_TENT_TEST_GID_INDEX"))
            c->set("transports/rdma/device/gid_index", std::stoi(gid));
        if (const char* nic = getenv("MC_TENT_TEST_NIC"))
            c->set("topology/rdma_whitelist", std::vector<std::string>{nic});
        c->set("transports/tcp/enable", true);
        c->set("transports/hp_tcp/enable", false);
        c->set("transports/rdma/enable", true);
        c->set("transports/shm/enable", false);
        c->set("transports/io_uring/enable", false);
        c->set("transports/mpcomm/enable", false);
        c->set("transports/mlu/enable", true);
        c->set("use_legacy_transport_selection", GetParam());
        return c;
    }
    void SetUp() override {
        auto& platform = Platform::getLoader(config());
        MemoryOptions opts;
        opts.location = "mlu:0";
        void* probe = nullptr;
        if (!platform.allocate(&probe, 4096, opts).ok())
            MLU_SKIP_UNLESS_REQUIRED("No MLU device available");
        DmabufExport exported;
        auto s = platform.exportDmabuf(probe, 4096, exported);
        ASSERT_TRUE(platform.free(probe, 4096).ok());
        if (!s.ok()) MLU_SKIP_UNLESS_REQUIRED("No dma-buf export: " << s);
        a_ = std::make_unique<TransferEngineImpl>(config());
        b_ = std::make_unique<TransferEngineImpl>(config());
        ASSERT_TRUE(a_->available());
        ASSERT_TRUE(b_->available());
        if (!TransferEngineImplTestPeer::rdmaGpuDirect(*a_))
            MLU_SKIP_UNLESS_REQUIRED("No RDMA device with ibv_reg_dmabuf_mr");
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
        ASSERT_TRUE(engine.allocateLocalMemory(out, kAllocation, opts).ok());
        ASSERT_NE(*out, nullptr);
        MemoryOptions registration;
        ASSERT_TRUE(
            engine.registerLocalMemory({*out}, {kAllocation}, registration)
                .ok());
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

TEST_P(MluDirectRdmaTest, DeviceToDeviceReadAndWrite) {
    ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "mlu:0", Request::WRITE));
    ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "mlu:0", Request::READ));
}

// Both engines use the MLU platform. cpu:0 selects host memory in that
// process; this is not coverage of a CPU-platform initiator (unsupported).
TEST_P(MluDirectRdmaTest, OneDeviceSideReadAndWrite) {
    // Host to device.
    ASSERT_NO_FATAL_FAILURE(transfer("cpu:0", "mlu:0", Request::WRITE));
    ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "cpu:0", Request::READ));
    // Device to host.
    ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "cpu:0", Request::WRITE));
    ASSERT_NO_FATAL_FAILURE(transfer("cpu:0", "mlu:0", Request::READ));
}

// The dma-buf direct route exists on RDMA alone. A request hinted to another
// transport must not lose staging just because both buffers could have gone
// over RDMA directly.
TEST_P(MluDirectRdmaTest, HostTransportHintKeepsStaging) {
    void *local = nullptr, *peer = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "mlu:0", &local));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &peer));
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
TEST_P(MluDirectRdmaTest, HostOnlyPolicyKeepsStaging) {
    if (GetParam()) return;  // Policies apply to selector mode only.
    auto peer_config = config();
    peer_config->set("transports/tcp/enable", false);
    peer_config->set("transports/hp_tcp/enable", true);
    auto target = std::make_unique<TransferEngineImpl>(peer_config);
    ASSERT_TRUE(target->available());
    auto c = config();
    c->set("transports/tcp/enable", false);
    c->set("transports/hp_tcp/enable", true);
    // The peer hop may only use HP TCP (MLU is local-segment only); the local
    // stage hop still needs the MLU copy transport.
    c->set("policy", json::parse(R"([{"name":"host_only",
        "segment_type":"memory","transports":["hp_tcp","mlu"]}])"));
    auto initiator = std::make_unique<TransferEngineImpl>(c);
    ASSERT_TRUE(initiator->available());
    void *local = nullptr, *peer = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*initiator, "mlu:0", &local));
    ASSERT_NO_FATAL_FAILURE(allocate(*target, "mlu:0", &peer));
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

TEST_P(MluDirectRdmaTest, ExportFailureIsStrictForRdmaAndStagesWithUnspec) {
    auto rdma = std::dynamic_pointer_cast<RdmaTransport>(
        TransferEngineImplTestPeer::transport(*a_));
    ASSERT_NE(rdma, nullptr);
    RdmaTransportTestPeer::rejectDeviceExport(*rdma);
    void *local = nullptr, *peer = nullptr;
    MemoryOptions allocation;
    allocation.location = "mlu:0";
    ASSERT_TRUE(a_->allocateLocalMemory(&local, kAllocation, allocation).ok());
    MemoryOptions registration;
    registration.type = RDMA;
    EXPECT_FALSE(
        a_->registerLocalMemory({local}, {kAllocation}, registration).ok());
    EXPECT_FALSE(TransferEngineImplTestPeer::hasTag(*a_, local, RDMA));
    registration.type = UNSPEC;
    ASSERT_TRUE(
        a_->registerLocalMemory({local}, {kAllocation}, registration).ok());
    EXPECT_FALSE(TransferEngineImplTestPeer::hasTag(*a_, local, RDMA));
    EXPECT_TRUE(TransferEngineImplTestPeer::hasTag(*a_, local, MLU));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &peer));
    Request request{};
    request.source = local;
    request.target_offset = reinterpret_cast<uint64_t>(peer);
    request.length = kLength;
    request.opcode = Request::WRITE;
    request.transport_hint = RDMA;
    ASSERT_TRUE(a_->openSegment(request.target_id, b_->getSegmentName()).ok());
    auto route = TransferEngineImplTestPeer::route(*a_, request);
    ASSERT_EQ(route.transport, RDMA);
    ASSERT_EQ(route.staging_params.size(), 3u);
    ASSERT_NO_FATAL_FAILURE(run(*a_, request, local, peer));
}

TEST_P(MluDirectRdmaTest, DisabledDirectAndMluOnlyRegistrationKeepStaging) {
    for (bool disabled : {false, true}) {
        auto c = config();
        c->set("transports/rdma/disable_gpu_direct_rdma", disabled);
        a_ = std::make_unique<TransferEngineImpl>(c);
        ASSERT_TRUE(a_->available());
        void *local = nullptr, *peer = nullptr;
        MemoryOptions allocation;
        allocation.location = "mlu:0";
        ASSERT_TRUE(
            a_->allocateLocalMemory(&local, kAllocation, allocation).ok());
        MemoryOptions registration;
        registration.type = disabled ? UNSPEC : MLU;
        ASSERT_TRUE(
            a_->registerLocalMemory({local}, {kAllocation}, registration).ok());
        ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &peer));
        Request request{};
        request.source = local;
        request.target_offset = reinterpret_cast<uint64_t>(peer);
        request.length = kLength;
        request.opcode = Request::READ;
        request.transport_hint = RDMA;
        ASSERT_TRUE(
            a_->openSegment(request.target_id, b_->getSegmentName()).ok());
        auto route = TransferEngineImplTestPeer::route(*a_, request);
        ASSERT_EQ(route.transport, RDMA);
        EXPECT_EQ(route.staging_params.size(), 3u);
        ASSERT_NO_FATAL_FAILURE(run(*a_, request, local, peer));
        for (auto [engine, batch] : batches_)
            EXPECT_TRUE(engine->freeBatch(batch).ok());
        batches_.clear();
    }
}

INSTANTIATE_TEST_SUITE_P(SelectionModes, MluDirectRdmaTest, ::testing::Bool());
}  // namespace
}  // namespace tent
}  // namespace mooncake
