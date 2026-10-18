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

// Engine-level TPU routing tests against the mock PJRT adapter. No TPU
// hardware or PJRT runtime required.
//
// A request that touches TPU HBM on a peer segment must be planned as a
// host-DRAM staged transfer up front, so the network hop can be carried by any
// host-to-host transport (TCP, HP TCP, RDMA, ...). Previously the staging plan
// was only built after the selector had already picked standard TCP, which
// TCP only won because it advertises device capabilities on non-CPU
// platforms; with standard TCP disabled, TPU transfers had no route.

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <thread>
#include <tuple>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"

#ifndef MOCK_TPU_PJRT_LIB
#error "MOCK_TPU_PJRT_LIB must be defined by the build (path to mock adapter)"
#endif

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static tent::SelectionResult route(tent::TransferEngineImpl& engine,
                                       const tent::Request& request) {
        return engine.resolveTransport(request, 0);
    }
    static void smallStagePool(tent::TransferEngineImpl& engine) {
        engine.staging_proxy_ =
            std::make_unique<tent::ProxyManager>(&engine, 1UL << 20, 4);
    }
    static tent::Status stopStaging(tent::TransferEngineImpl& engine) {
        return engine.staging_proxy_->deconstruct();
    }
};

namespace tent {
namespace {

using RegisterFn = void* (*)(size_t, int);
using DeviceDataFn = void* (*)(const void*);
using ResetFn = void (*)();

using Params = std::tuple<bool, bool>;  // HP TCP instead of TCP, legacy mode
constexpr size_t kLength = (3UL << 20) + 4096;  // Several 1 MiB stage chunks.

class TpuEngineTest : public ::testing::TestWithParam<Params> {
   protected:
    static void SetUpTestSuite() {
        ::setenv("MC_TPU_PJRT_LIB", MOCK_TPU_PJRT_LIB, /*overwrite=*/1);
    }

    bool hpTcp() const { return std::get<0>(GetParam()); }
    TransportType network() const { return hpTcp() ? HP_TCP : TCP; }

    std::shared_ptr<Config> config() {
        auto c = std::make_shared<Config>();
        c->set("metadata_type", "p2p");
        c->set("metadata_servers", "");
        c->set("rpc_server_hostname", "127.0.0.1");
        c->set("rpc_server_port", "0");
        c->set("log_level", "warning");
        // Exactly one host network transport. With HP TCP the only transport
        // advertising device capabilities is TpuTransport itself.
        c->set("transports/tcp/enable", !hpTcp());
        c->set("transports/hp_tcp/enable", hpTcp());
        c->set("transports/rdma/enable", false);
        c->set("transports/shm/enable", false);
        c->set("transports/io_uring/enable", false);
        c->set("transports/mpcomm/enable", false);
        c->set("transports/tpu/enable", true);
        c->set("use_legacy_transport_selection", std::get<1>(GetParam()));
        return c;
    }

    void SetUp() override {
        mock_ = dlopen(MOCK_TPU_PJRT_LIB, RTLD_NOW | RTLD_GLOBAL);
        ASSERT_NE(mock_, nullptr) << dlerror();
        register_ = reinterpret_cast<RegisterFn>(
            dlsym(mock_, "mock_tpu_pjrt_register_device"));
        device_data_ = reinterpret_cast<DeviceDataFn>(
            dlsym(mock_, "mock_tpu_pjrt_device_data"));
        reset_ = reinterpret_cast<ResetFn>(dlsym(mock_, "mock_tpu_pjrt_reset"));
        ASSERT_NE(register_, nullptr);
        ASSERT_NE(device_data_, nullptr);
        ASSERT_NE(reset_, nullptr);
        reset_();

        ASSERT_EQ(Platform::getLoader(config()).type(), "tpu");
        a_ = std::make_unique<TransferEngineImpl>(config());
        b_ = std::make_unique<TransferEngineImpl>(config());
        ASSERT_TRUE(a_->available());
        ASSERT_TRUE(b_->available());
        TransferEngineImplTestPeer::smallStagePool(*a_);
        TransferEngineImplTestPeer::smallStagePool(*b_);
    }

    void TearDown() override {
        // Stop staging while transports and registrations are still alive.
        for (auto* engine : {a_.get(), b_.get()}) {
            if (engine)
                EXPECT_TRUE(
                    TransferEngineImplTestPeer::stopStaging(*engine).ok());
        }
        for (auto [engine, address] : registrations_)
            EXPECT_TRUE(engine->unregisterLocalMemory(address).ok());
        for (auto [engine, address] : allocations_)
            EXPECT_TRUE(engine->freeLocalMemory(address).ok());
        a_.reset();
        b_.reset();
        if (reset_) reset_();
        if (mock_) dlclose(mock_);
    }

    // Returns a registered buffer whose bytes are reachable through `data`:
    // the mock's shadow storage for TPU, the buffer itself for host memory.
    void* buffer(TransferEngineImpl& engine, bool tpu, uint8_t** data) {
        void* address = nullptr;
        if (tpu) {
            address = register_(kLength, /*index=*/0);
            EXPECT_NE(address, nullptr);
            *data = static_cast<uint8_t*>(device_data_(address));
        } else {
            MemoryOptions opts;
            opts.location = "cpu:0";
            EXPECT_TRUE(
                engine.allocateLocalMemory(&address, kLength, opts).ok());
            allocations_.push_back({&engine, address});
            *data = static_cast<uint8_t*>(address);
        }
        if (!address) return nullptr;
        MemoryOptions opts;
        EXPECT_TRUE(
            engine.registerLocalMemory({address}, {kLength}, opts).ok());
        registrations_.push_back({&engine, address});
        return address;
    }

    Request request(Request::OpCode opcode, void* local, void* peer) {
        Request r{};
        r.opcode = opcode;
        r.source = local;
        r.target_offset = reinterpret_cast<uint64_t>(peer);
        r.length = kLength;
        EXPECT_TRUE(a_->openSegment(r.target_id, b_->getSegmentName()).ok());
        EXPECT_NE(r.target_id, LOCAL_SEGMENT_ID);
        return r;
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

    void expectStagedRoute(const Request& r, bool local_tpu, bool peer_tpu) {
        auto route = TransferEngineImplTestPeer::route(*a_, r);
        ASSERT_EQ(route.transport, network());
        ASSERT_EQ(route.staging_params.size(), 3u);
        EXPECT_FALSE(route.staging_params[0].empty());
        EXPECT_EQ(route.staging_params[1].empty(), !local_tpu);
        EXPECT_EQ(route.staging_params[2].empty(), !peer_tpu);
    }

    void transfer(bool local_tpu, bool peer_tpu, Request::OpCode opcode) {
        uint8_t *local_data = nullptr, *peer_data = nullptr;
        void* local = buffer(*a_, local_tpu, &local_data);
        void* peer = buffer(*b_, peer_tpu, &peer_data);
        ASSERT_NE(local, nullptr);
        ASSERT_NE(peer, nullptr);
        uint8_t* source = opcode == Request::WRITE ? local_data : peer_data;
        uint8_t* dest = opcode == Request::WRITE ? peer_data : local_data;
        for (size_t i = 0; i < kLength; ++i) source[i] = (uint8_t)(i % 251);
        std::memset(dest, 0, kLength);

        auto r = request(opcode, local, peer);
        ASSERT_NO_FATAL_FAILURE(expectStagedRoute(r, local_tpu, peer_tpu));
        auto batch = a_->allocateBatch(1);
        ASSERT_NE(batch, 0u);
        ASSERT_TRUE(a_->submitTransfer(batch, {r}).ok());
        auto status = wait(batch);
        EXPECT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kLength);
        EXPECT_EQ(0, std::memcmp(dest, source, kLength));
        EXPECT_TRUE(a_->freeBatch(batch).ok());
    }

    void* mock_ = nullptr;
    RegisterFn register_ = nullptr;
    DeviceDataFn device_data_ = nullptr;
    ResetFn reset_ = nullptr;
    std::unique_ptr<TransferEngineImpl> a_, b_;
    std::vector<std::pair<TransferEngineImpl*, void*>> registrations_;
    std::vector<std::pair<TransferEngineImpl*, void*>> allocations_;
};

TEST_P(TpuEngineTest, PlansHostStagingForEveryTpuSide) {
    uint8_t* unused = nullptr;
    void* local_tpu = buffer(*a_, true, &unused);
    void* local_host = buffer(*a_, false, &unused);
    void* peer_tpu = buffer(*b_, true, &unused);
    void* peer_host = buffer(*b_, false, &unused);
    expectStagedRoute(request(Request::WRITE, local_tpu, peer_tpu), true, true);
    expectStagedRoute(request(Request::WRITE, local_tpu, peer_host), true,
                      false);
    expectStagedRoute(request(Request::WRITE, local_host, peer_tpu), false,
                      true);
}

TEST_P(TpuEngineTest, HostToHostIsNotStaged) {
    uint8_t* unused = nullptr;
    void* local_host = buffer(*a_, false, &unused);
    void* peer_host = buffer(*b_, false, &unused);
    auto route = TransferEngineImplTestPeer::route(
        *a_, request(Request::WRITE, local_host, peer_host));
    EXPECT_EQ(route.transport, network());
    EXPECT_TRUE(route.staging_params.empty());
}

TEST_P(TpuEngineTest, StagesTpuToTpuWrite) {
    transfer(true, true, Request::WRITE);
}

TEST_P(TpuEngineTest, StagesTpuToTpuRead) {
    transfer(true, true, Request::READ);
}

TEST_P(TpuEngineTest, StagesTpuToHostWrite) {
    transfer(true, false, Request::WRITE);
}

TEST_P(TpuEngineTest, StagesHostToTpuRead) {
    transfer(false, true, Request::READ);
}

INSTANTIATE_TEST_SUITE_P(
    NetworkAndSelectionModes, TpuEngineTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const ::testing::TestParamInfo<Params>& info) {
        return std::string(std::get<0>(info.param) ? "HpTcp" : "Tcp") +
               (std::get<1>(info.param) ? "Legacy" : "Selector");
    });

}  // namespace
}  // namespace tent
}  // namespace mooncake
