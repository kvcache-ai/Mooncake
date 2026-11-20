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

// Two engines over loopback TCP on the real TPU adapter and the mock provider.
// Buffers are registered as mooncake.tpu does: provider handle -> adapter fake
// address -> registerLocalMemory(fake address).

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "mock_tpu_env.h"
#include "tent/common/config.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"

#ifndef TPU_PJRT_ADAPTER_LIB
#error "TPU_PJRT_ADAPTER_LIB must be defined by the build"
#endif
#ifndef MOCK_TPU_BUFFER_PROVIDER_LIB
#error "MOCK_TPU_BUFFER_PROVIDER_LIB must be defined by the build"
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
    static tent::Status sweep(tent::TransferEngineImpl& engine) {
        return engine.lazyFreeBatch();
    }
    static std::string bufferLocation(tent::TransferEngineImpl& engine,
                                      void* address) {
        auto desc = engine.metadata_->segmentManager().getLocal();
        auto* buffer = desc->findBuffer(reinterpret_cast<uint64_t>(address), 1);
        return buffer ? buffer->location : "";
    }
};

namespace tent {
namespace {

using Params = std::tuple<bool, bool>;  // legacy selection, runtime queue
constexpr size_t kChunk = 1UL << 20;    // smallStagePool() slot size
// Longer than the whole four-slot staging pool, and not chunk aligned.
constexpr size_t kLength = 4 * kChunk + 4096 + 3;
constexpr uint8_t kGuard = 0xEE;
// tpu-sync only accepts device offsets that are multiples of 32 bytes.
constexpr size_t kTpuSyncAlignment = 32;

std::vector<uint8_t> pattern(size_t size, int seed) {
    std::vector<uint8_t> result(size);
    for (size_t i = 0; i < size; ++i) result[i] = (i * 13 + seed) % 251;
    return result;
}

// The address the engine sees, and where its bytes live (mock storage for TPU).
struct Side {
    void* addr = nullptr;
    uint8_t* bytes = nullptr;
    size_t size = 0;
};

class TpuAdapterEngineTest : public ::testing::TestWithParam<Params> {
   protected:
    static void SetUpTestSuite() {
        auto& env = MockTpuEnv::instance();
        if (!env.load(TPU_PJRT_ADAPTER_LIB, MOCK_TPU_BUFFER_PROVIDER_LIB,
                      &load_error_)) {
            return;
        }
        // Devices 0..3 must be known before the first engine is created.
        anchors_[0] = env.create(4096, /*device=*/0);
        anchors_[1] = env.create(4096, /*device=*/3);
        if (!anchors_[0] || !anchors_[1]) load_error_ = "anchor registration";
    }

    static void TearDownTestSuite() {
        for (auto& anchor : anchors_)
            if (anchor) MockTpuEnv::instance().unregister(anchor);
    }

    std::shared_ptr<Config> config() {
        auto c = std::make_shared<Config>();
        c->set("metadata_type", "p2p");
        c->set("metadata_servers", "");
        c->set("rpc_server_hostname", "127.0.0.1");
        c->set("rpc_server_port", "0");
        c->set("log_level", "warning");
        c->set("transports/tcp/enable", true);
        c->set("transports/tcp/max_retry_count", 0);
        c->set("transports/hp_tcp/enable", false);
        c->set("transports/rdma/enable", false);
        c->set("transports/shm/enable", false);
        c->set("transports/io_uring/enable", false);
        c->set("transports/mpcomm/enable", false);
        c->set("transports/tpu/enable", true);
        c->set("use_legacy_transport_selection", std::get<0>(GetParam()));
        c->set("enable_runtime_queue", std::get<1>(GetParam()));
        c->set("max_failover_attempts", 1);
        return c;
    }

    void SetUp() override {
        ASSERT_TRUE(load_error_.empty()) << load_error_;
        ASSERT_EQ(Platform::getLoader(config()).type(), "tpu");
        live_before_ = env().liveCount();
        a_ = std::make_unique<TransferEngineImpl>(config());
        b_ = std::make_unique<TransferEngineImpl>(config());
        ASSERT_TRUE(a_->available());
        ASSERT_TRUE(b_->available());
        TransferEngineImplTestPeer::smallStagePool(*a_);
        TransferEngineImplTestPeer::smallStagePool(*b_);
        ASSERT_TRUE(a_->openSegment(peer_, b_->getSegmentName()).ok());
        ASSERT_NE(peer_, LOCAL_SEGMENT_ID);
    }

    void TearDown() override {
        env().setFailCopies(false);
        env().setDeviceAlignment(0);
        // Stop staging while transports and registrations are still alive.
        for (auto* engine : {a_.get(), b_.get()}) {
            if (!engine) continue;
            EXPECT_TRUE(TransferEngineImplTestPeer::stopStaging(*engine).ok());
            EXPECT_EQ(engine->aliveBatchCountForTest(), 0u);
        }
        for (auto [engine, address] : registrations_)
            EXPECT_TRUE(engine->unregisterLocalMemory(address).ok());
        for (auto [engine, address] : allocations_)
            EXPECT_TRUE(engine->freeLocalMemory(address).ok());
        for (auto& buffer : tpu_buffers_) EXPECT_TRUE(env().unregister(buffer));
        a_.reset();
        b_.reset();
        if (live_before_ >= 0) {
            EXPECT_EQ(env().liveCount(), live_before_);
        }
    }

    static MockTpuEnv& env() { return MockTpuEnv::instance(); }

    // A mock TPU buffer, registered with the adapter and then with `engine`.
    Side tpu(TransferEngineImpl& engine, size_t size, int device = 0) {
        MockTpuBuffer buffer = env().create(size, device);
        EXPECT_TRUE(buffer) << "adapter registration failed";
        if (!buffer) return {};
        tpu_buffers_.push_back(buffer);
        EXPECT_EQ(buffer.size, size);
        EXPECT_EQ(buffer.device, device);
        MemoryOptions opts;  // Location left to TENT, as mooncake.tpu does.
        EXPECT_TRUE(
            engine.registerLocalMemory({buffer.addr()}, {size}, opts).ok());
        registrations_.push_back({&engine, buffer.addr()});
        return {buffer.addr(), buffer.data, size};
    }

    Side host(TransferEngineImpl& engine, size_t size) {
        void* address = nullptr;
        MemoryOptions opts;
        opts.location = "cpu:0";
        EXPECT_TRUE(engine.allocateLocalMemory(&address, size, opts).ok());
        if (!address) return {};
        allocations_.push_back({&engine, address});
        EXPECT_TRUE(engine.registerLocalMemory({address}, {size}, opts).ok());
        registrations_.push_back({&engine, address});
        return {address, static_cast<uint8_t*>(address), size};
    }

    Request request(Request::OpCode opcode, const Side& local, size_t local_off,
                    const Side& peer, size_t peer_off, size_t length) {
        Request r{};
        r.opcode = opcode;
        r.source = static_cast<uint8_t*>(local.addr) + local_off;
        r.target_id = peer_;
        r.target_offset = reinterpret_cast<uint64_t>(peer.addr) + peer_off;
        r.length = length;
        return r;
    }

    TransferStatus run(const std::vector<Request>& requests) {
        TransferStatus status{};
        auto batch = a_->allocateBatch(requests.size());
        EXPECT_NE(batch, 0u);
        if (!batch) return status;
        auto submitted = a_->submitTransfer(batch, requests);
        EXPECT_TRUE(submitted.ok()) << submitted.ToString();
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (submitted.ok()) {
            auto s = a_->progressBatch(batch, status);
            EXPECT_TRUE(s.ok()) << s.ToString();
            if (!s.ok() || status.s != PENDING ||
                std::chrono::steady_clock::now() > deadline)
                break;
            std::this_thread::yield();
        }
        EXPECT_TRUE(a_->freeBatch(batch).ok());
        return status;
    }

    // Copies one range and checks that no other destination byte changed.
    void transfer(Request::OpCode opcode, const Side& local, size_t local_off,
                  const Side& peer, size_t peer_off, size_t length,
                  TransferStatusEnum expected = COMPLETED) {
        ASSERT_NE(local.addr, nullptr);
        ASSERT_NE(peer.addr, nullptr);
        const bool write = opcode == Request::WRITE;
        const Side& src = write ? local : peer;
        const Side& dst = write ? peer : local;
        const size_t src_off = write ? local_off : peer_off;
        const size_t dst_off = write ? peer_off : local_off;
        auto data = pattern(length, ++seed_);
        std::memcpy(src.bytes + src_off, data.data(), length);
        std::memset(dst.bytes, kGuard, dst.size);

        auto status =
            run({request(opcode, local, local_off, peer, peer_off, length)});
        ASSERT_EQ(status.s, expected);
        if (expected != COMPLETED) {
            EXPECT_EQ(status.transferred_bytes, 0u);
            for (size_t i = 0; i < dst.size; ++i)
                ASSERT_EQ(dst.bytes[i], kGuard) << "byte " << i;
            return;
        }
        EXPECT_EQ(status.transferred_bytes, length);
        EXPECT_EQ(0, std::memcmp(dst.bytes + dst_off, data.data(), length));
        for (size_t i = 0; i < dst_off; ++i)
            ASSERT_EQ(dst.bytes[i], kGuard) << "byte " << i;
        for (size_t i = dst_off + length; i < dst.size; ++i)
            ASSERT_EQ(dst.bytes[i], kGuard) << "byte " << i;
    }

    void roundTrip(const Side& local, const Side& peer, size_t length,
                   size_t local_off = 0, size_t peer_off = 0) {
        for (auto op : {Request::WRITE, Request::READ}) {
            SCOPED_TRACE(op == Request::WRITE ? "WRITE" : "READ");
            ASSERT_NO_FATAL_FAILURE(
                transfer(op, local, local_off, peer, peer_off, length));
        }
    }

    // Every batch is freed and every staging slot can be pinned again.
    void expectIdle() {
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        bool idle = false;
        do {
            idle = true;
            for (auto* engine : {a_.get(), b_.get()}) {
                auto location =
                    engine->getLocalTopology()->findNearMem("tpu:0");
                std::vector<uint64_t> pins;
                for (int i = 0; i < 4; ++i) {
                    auto pin = engine->lockStageBuffer(location);
                    if (pin) pins.push_back(pin);
                }
                idle &= pins.size() == 4;
                for (auto pin : pins)
                    EXPECT_TRUE(engine->unlockStageBuffer(pin).ok());
                EXPECT_TRUE(TransferEngineImplTestPeer::sweep(*engine).ok());
                idle &= engine->aliveBatchCountForTest() == 0;
            }
            if (!idle)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } while (!idle && std::chrono::steady_clock::now() < deadline);
        EXPECT_TRUE(idle) << "batches and staging slots did not drain";
    }

    // A refused device offset fails without touching the destination: rounding
    // it would copy bytes outside the range. The buffers then copy normally.
    void expectRejected(Request::OpCode opcode, const Side& local,
                        size_t local_off, const Side& peer, size_t peer_off) {
        SCOPED_TRACE(opcode == Request::WRITE ? "WRITE" : "READ");
        constexpr size_t length = 2 * kChunk + 5;  // Several staging chunks.
        const int rejected = env().rejectedCount();
        ASSERT_NO_FATAL_FAILURE(
            transfer(opcode, local, local_off, peer, peer_off, length, FAILED));
        EXPECT_GT(env().rejectedCount(), rejected);
        ASSERT_NO_FATAL_FAILURE(transfer(opcode, local, 0, peer, 0, length));
        expectIdle();
    }

    static inline std::string load_error_;
    static inline MockTpuBuffer anchors_[2];

    std::unique_ptr<TransferEngineImpl> a_, b_;
    SegmentID peer_ = LOCAL_SEGMENT_ID;
    int live_before_ = -1;
    int seed_ = 0;
    std::vector<MockTpuBuffer> tpu_buffers_;
    std::vector<std::pair<TransferEngineImpl*, void*>> registrations_;
    std::vector<std::pair<TransferEngineImpl*, void*>> allocations_;
};

TEST_P(TpuAdapterEngineTest, RegisteredFakeAddressesResolveToTpuLocations) {
    Side device3 = tpu(*a_, kLength, /*device=*/3);
    Side cpu = host(*a_, kLength);
    ASSERT_NE(device3.addr, nullptr);
    ASSERT_NE(cpu.addr, nullptr);
    auto& platform = Platform::getLoader();

    auto* interior = static_cast<uint8_t*>(device3.addr) + kChunk + 5;
    auto ranges = platform.getLocation(interior, 64, /*skip_prefault=*/true);
    ASSERT_EQ(ranges.size(), 1u);
    EXPECT_EQ(ranges[0].location, "tpu:3");
    EXPECT_EQ(platform.getMemoryType(interior), MTYPE_TPU);
    EXPECT_EQ(platform.getMemoryType(static_cast<uint8_t*>(device3.addr) +
                                     kLength - 1),
              MTYPE_TPU);
    EXPECT_NE(platform.getMemoryType(cpu.addr), MTYPE_TPU);

    EXPECT_EQ(TransferEngineImplTestPeer::bufferLocation(*a_, device3.addr),
              "tpu:3");
    EXPECT_EQ(TransferEngineImplTestPeer::bufferLocation(*a_, cpu.addr)
                  .rfind("cpu", 0),
              0u);

    // HBM is not NIC-addressable: TCP carries the transfer via host staging.
    Side peer = tpu(*b_, kLength);
    auto route = TransferEngineImplTestPeer::route(
        *a_, request(Request::WRITE, device3, 0, peer, 0, kLength));
    EXPECT_EQ(route.transport, TCP);
}

TEST_P(TpuAdapterEngineTest, StagesOneTpuSideWriteAndRead) {
    {
        SCOPED_TRACE("local TPU, peer host");
        roundTrip(tpu(*a_, kLength), host(*b_, kLength), kLength);
    }
    {
        SCOPED_TRACE("local host, peer TPU");
        roundTrip(host(*a_, kLength), tpu(*b_, kLength), kLength);
    }
}

// The adapter accepts any offset; the TpuSync* tests cover tpu-sync's rule.
TEST_P(TpuAdapterEngineTest, PermissiveProviderCopiesUnalignedInteriorRanges) {
    constexpr size_t kSize = kLength + 8192;
    Side local = tpu(*a_, kSize);
    Side peer = tpu(*b_, kSize);
    for (size_t length : {size_t(1), size_t(65539), 2 * kChunk + 5}) {
        SCOPED_TRACE(::testing::Message() << "length=" << length);
        roundTrip(local, peer, length, /*local_off=*/17, /*peer_off=*/4093);
    }
}

// Multi-chunk ranges stay aligned, since chunks start a chunk size apart.
TEST_P(TpuAdapterEngineTest, TpuSyncAlignedRangesCopyExactly) {
    env().setDeviceAlignment(kTpuSyncAlignment);
    const int rejected = env().rejectedCount();
    constexpr size_t kSize = kLength + 8192;
    Side local = tpu(*a_, kSize);
    Side peer = tpu(*b_, kSize);
    Side cpu = host(*a_, kSize);
    for (size_t length :
         {size_t(1), size_t(33), size_t(65539), 2 * kChunk + 5, kLength}) {
        SCOPED_TRACE(::testing::Message() << "length=" << length);
        roundTrip(local, peer, length, /*local_off=*/4096 + 32,
                  /*peer_off=*/96);
        // Only device offsets are constrained; a host side may start anywhere.
        roundTrip(cpu, peer, length, /*local_off=*/17, /*peer_off=*/96);
    }
    EXPECT_EQ(env().rejectedCount(), rejected);
}

// The source is local for WRITE and remote (delegated stage) for READ.
TEST_P(TpuAdapterEngineTest, TpuSyncMisalignedSourceOffsetFails) {
    env().setDeviceAlignment(kTpuSyncAlignment);
    Side local = tpu(*a_, kLength);
    Side peer = tpu(*b_, kLength);
    expectRejected(Request::WRITE, local, /*local_off=*/17, peer, 0);
    expectRejected(Request::READ, local, 0, peer, /*peer_off=*/17);
}

TEST_P(TpuAdapterEngineTest, TpuSyncMisalignedDestinationOffsetFails) {
    env().setDeviceAlignment(kTpuSyncAlignment);
    Side local = tpu(*a_, kLength);
    Side peer = tpu(*b_, kLength);
    expectRejected(Request::WRITE, local, 0, peer, /*peer_off=*/17);
    expectRejected(Request::READ, local, /*local_off=*/17, peer, 0);
}

TEST_P(TpuAdapterEngineTest, BatchOfRequestsAcrossManyBuffers) {
    constexpr int kBuffers = 4;
    constexpr int kPerBuffer = 4;
    // Odd slice offsets: this relies on the permissive provider.
    constexpr size_t kSlice = 200 * 1024 + 7;
    constexpr size_t kSize = kPerBuffer * (kSlice + 64);
    std::vector<Side> local, peer;
    for (int i = 0; i < kBuffers; ++i) {
        local.push_back(tpu(*a_, kSize, i % 2 ? 3 : 0));
        peer.push_back(tpu(*b_, kSize, i % 2 ? 0 : 3));
    }
    for (auto op : {Request::WRITE, Request::READ}) {
        SCOPED_TRACE(op == Request::WRITE ? "WRITE" : "READ");
        auto& src = op == Request::WRITE ? local : peer;
        auto& dst = op == Request::WRITE ? peer : local;
        std::vector<Request> requests;
        std::vector<std::vector<uint8_t>> expected;
        for (int i = 0; i < kBuffers; ++i) {
            std::memset(dst[i].bytes, kGuard, kSize);
            for (int j = 0; j < kPerBuffer; ++j) {
                const size_t off = j * (kSlice + 64);
                expected.push_back(pattern(kSlice, ++seed_));
                std::memcpy(src[i].bytes + off, expected.back().data(), kSlice);
                requests.push_back(
                    request(op, local[i], off, peer[i], off, kSlice));
            }
        }
        auto status = run(requests);
        ASSERT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, requests.size() * kSlice);
        for (int i = 0; i < kBuffers; ++i) {
            for (int j = 0; j < kPerBuffer; ++j) {
                const size_t off = j * (kSlice + 64);
                EXPECT_EQ(
                    0, std::memcmp(dst[i].bytes + off,
                                   expected[i * kPerBuffer + j].data(), kSlice))
                    << "buffer " << i << " slice " << j;
                EXPECT_EQ(dst[i].bytes[off + kSlice], kGuard);
            }
        }
    }
}

TEST_P(TpuAdapterEngineTest, ProviderCopyFailureFailsThenRecovers) {
    Side local = tpu(*a_, kLength);
    Side peer = tpu(*b_, kLength);
    for (int cycle = 0; cycle < 3; ++cycle) {
        SCOPED_TRACE(::testing::Message() << "cycle " << cycle);
        for (auto op : {Request::WRITE, Request::READ}) {
            SCOPED_TRACE(op == Request::WRITE ? "WRITE" : "READ");
            env().setFailCopies(true);
            ASSERT_NO_FATAL_FAILURE(
                transfer(op, local, 0, peer, 0, kLength, FAILED));
            env().setFailCopies(false);
            ASSERT_NO_FATAL_FAILURE(transfer(op, local, 0, peer, 0, kLength));
        }
    }
    expectIdle();
}

TEST_P(TpuAdapterEngineTest, UnregisterReleasesProviderHandles) {
    MockTpuBuffer buffer = env().create(kLength, /*device=*/0);
    ASSERT_TRUE(buffer);
    EXPECT_EQ(env().liveCount(), live_before_ + 1);
    MemoryOptions opts;
    ASSERT_TRUE(a_->registerLocalMemory({buffer.addr()}, {kLength}, opts).ok());
    Side local{buffer.addr(), buffer.data, kLength};
    Side peer = tpu(*b_, kLength);
    ASSERT_NO_FATAL_FAILURE(roundTrip(local, peer, kLength));

    ASSERT_TRUE(a_->unregisterLocalMemory(buffer.addr()).ok());
    ASSERT_TRUE(env().unregister(buffer));
    EXPECT_EQ(env().liveCount(), live_before_ + 1);  // Only `peer` is left.
    EXPECT_NE(Platform::getLoader().getMemoryType(buffer.addr()), MTYPE_TPU);
    EXPECT_FALSE(env().unregister(buffer));

    roundTrip(tpu(*a_, kLength), peer, kLength);
}

INSTANTIATE_TEST_SUITE_P(
    SelectionAndDispatchModes, TpuAdapterEngineTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const ::testing::TestParamInfo<Params>& info) {
        return std::string(std::get<0>(info.param) ? "Legacy" : "Selector") +
               (std::get<1>(info.param) ? "RuntimeQueue" : "Direct");
    });

}  // namespace
}  // namespace tent
}  // namespace mooncake
