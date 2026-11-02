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
#include <cnrt.h>

#include <algorithm>
#include <atomic>
#include <set>
#include <chrono>
#include <memory>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/proxy_manager.h"

namespace mooncake {
// Fail before dispatch, rather than rewriting an already-completed copy.
// All accepted, non-injected work still runs through the real transports.
class MluFaultTransport : public tent::Transport {
    using Status = tent::Status;
    struct Slot {
        int delegated_id = -1;
    };
    struct Batch : SubBatch {
        SubBatchRef inner = nullptr;
        std::vector<Slot> slots;
        size_t size() const override { return slots.size(); }
    };

   public:
    explicit MluFaultTransport(std::shared_ptr<tent::Transport> delegate)
        : delegate_(std::move(delegate)) {
        caps = delegate_->capabilities();
    }
    tent::Status allocateSubBatch(SubBatchRef& batch, size_t size) override {
        auto wrapped = std::make_unique<Batch>();
        CHECK_STATUS(delegate_->allocateSubBatch(wrapped->inner, size));
        batch = wrapped.release();
        ++live_batches;
        return tent::Status::OK();
    }
    tent::Status freeSubBatch(SubBatchRef& batch) override {
        auto* wrapped = static_cast<Batch*>(batch);
        CHECK_STATUS(delegate_->freeSubBatch(wrapped->inner));
        delete wrapped;
        batch = nullptr;
        --live_batches;
        return tent::Status::OK();
    }
    tent::Status submitTransferTasks(
        SubBatchRef batch,
        const std::vector<tent::Request>& requests) override {
        auto* wrapped = static_cast<Batch*>(batch);
        bool device_copy = false, remote = false;
        auto& platform = tent::Platform::getLoader();
        for (const auto& request : requests) {
            bool local = request.target_id == LOCAL_SEGMENT_ID;
            bool source_device =
                platform.getMemoryType(request.source) == tent::MTYPE_MLU;
            if (local) {
                device_copy |= source_device ||
                               platform.getMemoryType(reinterpret_cast<void*>(
                                   request.target_offset)) == tent::MTYPE_MLU;
            } else {
                remote = true;
                if (source_device) {
                    ++raw_device_network_submits;
                    return tent::Status::InvalidArgument(
                        "host transport received an unstaged device pointer");
                }
            }
        }
        if ((device_copy && consume(copy_failures)) ||
            (remote && !consume(network_passes_before_failure) &&
             consume(network_failures))) {
            ++injected_failures;
            return tent::Status::InternalError("injected pre-dispatch failure");
        }
        // This models an accepted network operation with no completion yet.
        // No payload is dispatched; the test explicitly signals its timeout.
        if (remote && hold_network.load()) {
            wrapped->slots.resize(wrapped->slots.size() + requests.size());
            ++held_submits;
            return tent::Status::OK();
        }
        const int base = static_cast<int>(wrapped->inner->size());
        if (wrapped->slots.empty()) {
            wrapped->inner->progress_batch_id = wrapped->progress_batch_id;
            wrapped->inner->notify_progress = wrapped->notify_progress;
        }
        CHECK_STATUS(delegate_->submitTransferTasks(wrapped->inner, requests));
        for (size_t i = 0; i < requests.size(); ++i)
            wrapped->slots.push_back({base + static_cast<int>(i)});
        return tent::Status::OK();
    }
    tent::Status getTransferStatus(SubBatchRef batch, int id,
                                   tent::TransferStatus& status) override {
        auto* wrapped = static_cast<Batch*>(batch);
        if (id < 0 || static_cast<size_t>(id) >= wrapped->size())
            return tent::Status::InvalidArgument("invalid fault-proxy task");
        int delegated = wrapped->slots[id].delegated_id;
        if (delegated >= 0)
            return delegate_->getTransferStatus(wrapped->inner, delegated,
                                                status);
        status = {expire_network.load() ? tent::TIMEOUT : tent::PENDING, 0};
        return tent::Status::OK();
    }
    tent::Status retryTransferTask(SubBatchRef batch, int id,
                                   const tent::Request& request) override {
        auto* wrapped = static_cast<Batch*>(batch);
        return delegate_->retryTransferTask(
            wrapped->inner, wrapped->slots.at(id).delegated_id, request);
    }
    tent::Status quiesce() override { return delegate_->quiesce(); }
    tent::Status addMemoryBuffer(tent::BufferDesc& desc,
                                 const tent::MemoryOptions& options) override {
        return delegate_->addMemoryBuffer(desc, options);
    }
    tent::Status removeMemoryBuffer(tent::BufferDesc& desc) override {
        return delegate_->removeMemoryBuffer(desc);
    }
    tent::Status allocateLocalMemory(void** ptr, size_t size,
                                     tent::MemoryOptions& options) override {
        CHECK_STATUS(delegate_->allocateLocalMemory(ptr, size, options));
        ++memory_allocations;
        return Status::OK();
    }
    tent::Status freeLocalMemory(void* ptr, size_t size) override {
        return delegate_->freeLocalMemory(ptr, size);
    }
    std::atomic<int> memory_allocations{0};
    std::atomic<int> copy_failures{0}, network_failures{0};  // -1: persistent
    std::atomic<int> network_passes_before_failure{0};
    std::atomic<int> injected_failures{0}, live_batches{0};
    std::atomic<int> raw_device_network_submits{0};
    std::atomic<int> held_submits{0};
    std::atomic<bool> hold_network{false}, expire_network{false};

   private:
    static bool consume(std::atomic<int>& remaining) {
        int count = remaining.load();
        while (count > 0 &&
               !remaining.compare_exchange_weak(count, count - 1)) {
        }
        return count != 0;
    }
    std::shared_ptr<tent::Transport> delegate_;
};

class TransferEngineImplTestPeer {
   public:
    static tent::SelectionResult route(tent::TransferEngineImpl& engine,
                                       const tent::Request& request) {
        return engine.resolveTransport(request, 0);
    }
    static bool hasMluTag(tent::TransferEngineImpl& engine, void* address) {
        auto desc = engine.metadata_->segmentManager().getLocal();
        auto* buffer = desc->findBuffer(reinterpret_cast<uint64_t>(address), 1);
        return buffer &&
               std::find(buffer->transports.begin(), buffer->transports.end(),
                         tent::MLU) != buffer->transports.end();
    }
    static void disableMlu(tent::TransferEngineImpl& engine) {
        engine.swapTransportForTest(tent::MLU, nullptr);
    }
    static std::shared_ptr<MluFaultTransport> wrap(
        tent::TransferEngineImpl& engine, tent::TransportType type) {
        auto wrapped =
            std::make_shared<MluFaultTransport>(engine.transport_list_[type]);
        engine.swapTransportForTest(type, wrapped);
        return wrapped;
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
    static size_t allocations(tent::TransferEngineImpl& engine) {
        std::lock_guard<std::mutex> guard(engine.mutex_);
        return engine.allocated_memory_.size();
    }
    static size_t registrations(tent::TransferEngineImpl& engine) {
        return engine.metadata_->segmentManager()
            .getLocal()
            ->getMemory()
            .buffers.size();
    }
};
namespace tent {
namespace {
using Params = std::tuple<bool, bool, bool>;    // HP TCP, legacy, runtime queue
constexpr size_t kLength = (4UL << 20) + 4096;  // Exceeds four 1 MiB slots.

class MluEngineTest : public ::testing::TestWithParam<Params> {
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
        c->set("transports/mlu/enable", true);
        c->set("use_legacy_transport_selection", std::get<1>(GetParam()));
        c->set("enable_runtime_queue", std::get<2>(GetParam()));
        c->set("max_failover_attempts", 1);
        return c;
    }
    void SetUp() override {
        auto& platform = Platform::getLoader(config());
        MemoryOptions opts;
        opts.location = "mlu:0";
        unsigned int count = 0;
        ASSERT_EQ(cnrtGetDeviceCount(&count), cnrtSuccess);
        if (count == 0) GTEST_SKIP() << "No MLU device visible";
        void* probe = nullptr;
        ASSERT_TRUE(platform.allocate(&probe, 4096, opts).ok());
        ASSERT_TRUE(platform.free(probe, 4096).ok());
        a_ = std::make_unique<TransferEngineImpl>(config());
        b_ = std::make_unique<TransferEngineImpl>(config());
        ASSERT_TRUE(a_->available());
        ASSERT_TRUE(b_->available());
        TransferEngineImplTestPeer::smallStagePool(*a_);
        TransferEngineImplTestPeer::smallStagePool(*b_);
        auto network = std::get<0>(GetParam()) ? HP_TCP : TCP;
        local_copy_ = TransferEngineImplTestPeer::wrap(*a_, MLU);
        local_network_ = TransferEngineImplTestPeer::wrap(*a_, network);
        peer_copy_ = TransferEngineImplTestPeer::wrap(*b_, MLU);
        peer_network_ = TransferEngineImplTestPeer::wrap(*b_, network);
    }
    void TearDown() override {
        for (auto& wrapper :
             {local_copy_, local_network_, peer_copy_, peer_network_})
            if (wrapper) wrapper->expire_network = true;
        for (auto [engine, batch] : batches_)
            EXPECT_TRUE(engine->freeBatch(batch).ok());
        batches_.clear();
        for (auto [engine, pin] : held_pins_)
            EXPECT_TRUE(engine->unlockStageBuffer(pin).ok());
        // Stop staging while transports and registrations are still alive.
        for (auto* engine : {a_.get(), b_.get()}) {
            if (!engine) continue;
            EXPECT_TRUE(TransferEngineImplTestPeer::stopStaging(*engine).ok());
            EXPECT_EQ(engine->aliveBatchCountForTest(), 0u);
            EXPECT_EQ(engine->deferredStageTeardownBatchCountForTest(), 0u);
        }
        for (auto [engine, address] : allocations_) {
            if (TransferEngineImplTestPeer::hasMluTag(*engine, address))
                EXPECT_TRUE(engine->unregisterLocalMemory(address).ok());
            EXPECT_TRUE(engine->freeLocalMemory(address).ok());
        }
        for (auto* engine : {a_.get(), b_.get()}) {
            if (!engine) continue;
            EXPECT_EQ(TransferEngineImplTestPeer::allocations(*engine), 0u);
            EXPECT_EQ(TransferEngineImplTestPeer::registrations(*engine), 0u);
        }
        for (auto& wrapper :
             {local_copy_, local_network_, peer_copy_, peer_network_}) {
            if (!wrapper) continue;
            EXPECT_EQ(wrapper->live_batches.load(), 0);
            EXPECT_EQ(wrapper->raw_device_network_submits.load(), 0);
        }
        a_.reset();
        b_.reset();
    }
    void allocate(TransferEngineImpl& engine, const char* location, void** out,
                  TransportType registration = UNSPEC) {
        MemoryOptions opts;
        opts.location = location;
        ASSERT_TRUE(engine.allocateLocalMemory(out, kLength, opts).ok());
        ASSERT_NE(*out, nullptr);
        allocations_.push_back({&engine, *out});
        opts.type = registration;
        ASSERT_TRUE(engine.registerLocalMemory({*out}, {kLength}, opts).ok());
        EXPECT_TRUE(TransferEngineImplTestPeer::hasMluTag(engine, *out));
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
                  Request::OpCode opcode, bool explicit_peer = false,
                  TransferStatusEnum expected_status = COMPLETED,
                  size_t length = kLength) {
        void *local = nullptr, *peer = nullptr;
        ASSERT_NO_FATAL_FAILURE(allocate(*a_, local_location, &local));
        ASSERT_NO_FATAL_FAILURE(
            allocate(*b_, peer_location, &peer, explicit_peer ? MLU : UNSPEC));
        std::vector<uint8_t> seed(length), zero(length, 0), readback(length);
        for (size_t i = 0; i < seed.size(); ++i) seed[i] = i % 251;
        auto& platform = Platform::getLoader();
        void* source = opcode == Request::WRITE ? local : peer;
        void* dest = opcode == Request::WRITE ? peer : local;
        ASSERT_TRUE(platform.copy(source, seed.data(), length).ok());
        ASSERT_TRUE(platform.copy(dest, zero.data(), length).ok());
        Request request{};
        request.opcode = opcode;
        request.source = local;
        request.target_offset = reinterpret_cast<uint64_t>(peer);
        request.length = length;
        ASSERT_TRUE(
            a_->openSegment(request.target_id, b_->getSegmentName()).ok());
        ASSERT_NE(request.target_id, LOCAL_SEGMENT_ID);
        const auto network = std::get<0>(GetParam()) ? HP_TCP : TCP;
        auto route = TransferEngineImplTestPeer::route(*a_, request);
        ASSERT_EQ(route.transport, network);
        ASSERT_EQ(route.staging_params.size(), 3u);
        EXPECT_EQ(route.staging_params[1].empty(),
                  std::string_view(local_location) == "cpu:0");
        EXPECT_EQ(route.staging_params[2].empty(),
                  std::string_view(peer_location) == "cpu:0");
        auto batch = a_->allocateBatch(1);
        ASSERT_NE(batch, 0u);
        batches_.push_back({a_.get(), batch});
        ASSERT_TRUE(a_->submitTransfer(batch, {request}).ok());
        auto status = wait(batch);
        ASSERT_EQ(status.s, expected_status);
        EXPECT_EQ(status.transferred_bytes,
                  expected_status == COMPLETED ? length : 0u);
        ASSERT_TRUE(platform.copy(readback.data(), dest, length).ok());
        EXPECT_EQ(readback, expected_status == COMPLETED ? seed : zero);
        ASSERT_TRUE(a_->freeBatch(batch).ok());
        batches_.pop_back();
        checkIdleResources();
    }
    void checkIdleResources() {
        // Cleanup is demand-driven. Request one more pin than the pool holds
        // to poll remote cleanup even after all local pins have been released.
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        bool idle = false;
        do {
            idle = true;
            for (auto* engine : {a_.get(), b_.get()}) {
                auto location =
                    engine->getLocalTopology()->findNearMem("mlu:0");
                std::vector<uint64_t> pins;
                for (int i = 0; i < 5; ++i) {
                    auto pin = engine->lockStageBuffer(location);
                    if (pin) pins.push_back(pin);
                }
                EXPECT_EQ(std::set<uint64_t>(pins.begin(), pins.end()).size(),
                          pins.size());
                idle &= pins.size() == 4;
                for (auto pin : pins)
                    EXPECT_TRUE(engine->unlockStageBuffer(pin).ok());
                EXPECT_TRUE(TransferEngineImplTestPeer::sweep(*engine).ok());
                idle &= engine->aliveBatchCountForTest() == 0;
            }
            if (!idle)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } while (!idle && std::chrono::steady_clock::now() < deadline);
        EXPECT_TRUE(idle) << "batches and staging pins did not drain";
    }
    std::shared_ptr<MluFaultTransport> local_copy_, local_network_, peer_copy_,
        peer_network_;
    std::vector<std::pair<TransferEngineImpl*, void*>> allocations_;
    std::unique_ptr<TransferEngineImpl> a_, b_;
    std::vector<std::pair<TransferEngineImpl*, BatchID>> batches_;
    std::vector<std::pair<TransferEngineImpl*, uint64_t>> held_pins_;
};

TEST_P(MluEngineTest, DefaultRegistrationSelectsLocalMluCopy) {
    void *device = nullptr, *host = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "mlu:0", &device));
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "cpu:0", &host));
    Request request{};
    request.opcode = Request::WRITE;
    request.source = device;
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(host);
    request.length = kLength;
    auto route = TransferEngineImplTestPeer::route(*a_, request);
    EXPECT_EQ(route.transport, MLU);
    EXPECT_TRUE(route.staging_params.empty());
}

TEST_P(MluEngineTest, StaleLocalRouteCannotSubmitAfterUnregister) {
    void *device = nullptr, *host = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "mlu:0", &device));
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "cpu:0", &host));
    Request request{};
    request.opcode = Request::WRITE;
    request.source = host;
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(device);
    request.length = kLength;
    ASSERT_EQ(TransferEngineImplTestPeer::route(*a_, request).transport, MLU);
    ASSERT_TRUE(a_->unregisterLocalMemory(device).ok());
    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(local_copy_->allocateSubBatch(batch, 1).ok());
    EXPECT_TRUE(local_copy_->submitTransferTasks(batch, {request}).ok());
    TransferStatus status{};
    EXPECT_TRUE(local_copy_->getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, FAILED);
    EXPECT_TRUE(local_copy_->freeSubBatch(batch).ok());
}

TEST_P(MluEngineTest, StagesDeviceToDeviceReadAndWrite) {
    ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "mlu:0", Request::WRITE, true));
    ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "mlu:0", Request::READ, true));
}

TEST_P(MluEngineTest, MluStagingHonorsRequestedNumaLocation) {
    const auto location = a_->getLocalTopology()->findNearMem("mlu:0");
    const auto pin = a_->lockStageBuffer(location);
    ASSERT_NE(pin, 0u);
    held_pins_.push_back({a_.get(), pin});
    EXPECT_EQ(local_copy_->memory_allocations.load(), 1);
    EXPECT_EQ(local_network_->memory_allocations.load(), 0);
    cnrtPointerAttributes_t attributes{};
    ASSERT_EQ(
        cnrtPointerGetAttributes(&attributes, reinterpret_cast<void*>(pin)),
        cnrtSuccess);
    EXPECT_TRUE(attributes.type == cnrtMemTypeHost ||
                attributes.type == cnrtMemTypeUnregistered);
    const auto ranges = Platform::getLoader().getLocation(
        reinterpret_cast<void*>(pin), 1UL << 20);
    ASSERT_FALSE(ranges.empty());
    for (const auto& range : ranges) EXPECT_EQ(range.location, location);
    ASSERT_TRUE(a_->unlockStageBuffer(pin).ok());
    held_pins_.pop_back();
    checkIdleResources();
}

TEST_P(MluEngineTest, StagesOneDeviceSideReadAndWrite) {
    ASSERT_NO_FATAL_FAILURE(transfer("cpu:0", "mlu:0", Request::WRITE, true));
    ASSERT_NO_FATAL_FAILURE(transfer("cpu:0", "mlu:0", Request::READ, true));
    ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "cpu:0", Request::WRITE));
    ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "cpu:0", Request::READ));
}

TEST_P(MluEngineTest, RemotePinSelectsPeerAllocatorAndSharesLocalPool) {
    void *local = nullptr, *peer = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "cpu:0", &local));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &peer));
    Request request{};
    request.opcode = Request::WRITE;
    request.source = local;
    request.target_offset = reinterpret_cast<uint64_t>(peer);
    request.length = kLength;
    ASSERT_TRUE(a_->openSegment(request.target_id, b_->getSegmentName()).ok());
    const auto route = TransferEngineImplTestPeer::route(*a_, request);
    ASSERT_EQ(route.staging_params.size(), 3u);
    ASSERT_TRUE(route.staging_params[1].empty());
    const auto& server = route.staging_params[0];
    const auto& location = route.staging_params[2];
    ASSERT_FALSE(location.empty());
    const int peer_allocations = peer_copy_->memory_allocations;
    const int network_allocations = peer_network_->memory_allocations;
    uint64_t remote_pin = 0;
    Status status;
    std::thread caller([&] {
        status = ControlClient::pinStageBuffer(server, location, remote_pin);
    });
    caller.join();
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_NE(remote_pin, 0u);
    held_pins_.push_back({b_.get(), remote_pin});
    EXPECT_EQ(peer_copy_->memory_allocations.load(), peer_allocations + 1);
    EXPECT_EQ(peer_network_->memory_allocations.load(), network_allocations);
    EXPECT_EQ(local_copy_->memory_allocations.load(), 0);

    const auto local_pin = b_->lockStageBuffer(location);
    ASSERT_NE(local_pin, 0u);
    held_pins_.push_back({b_.get(), local_pin});
    EXPECT_NE(local_pin, remote_pin);
    EXPECT_EQ(peer_copy_->memory_allocations.load(), peer_allocations + 1);
    ASSERT_TRUE(b_->unlockStageBuffer(local_pin).ok());
    held_pins_.pop_back();
    std::thread release(
        [&] { status = ControlClient::unpinStageBuffer(server, remote_pin); });
    release.join();
    ASSERT_TRUE(status.ok()) << status.ToString();
    held_pins_.pop_back();
    checkIdleResources();
}

TEST_P(MluEngineTest, OrdinaryCpuAllocationsKeepNetworkAllocator) {
    for (bool internal : {false, true}) {
        void* host = nullptr;
        ASSERT_TRUE(
            a_->allocateLocalMemory(&host, 4096, "cpu:0", internal).ok());
        allocations_.push_back({a_.get(), host});
        cnrtPointerAttributes_t attributes{};
        ASSERT_EQ(cnrtPointerGetAttributes(&attributes, host), cnrtSuccess);
        EXPECT_EQ(attributes.type, cnrtMemTypeUnregistered);
    }
    EXPECT_EQ(local_copy_->memory_allocations.load(), 0);
    EXPECT_EQ(local_network_->memory_allocations.load(), 2);
}

TEST_P(MluEngineTest, StageAllocationWithoutMluUsesNetworkAllocator) {
    TransferEngineImplTestPeer::disableMlu(*a_);
    const auto pin = a_->lockStageBuffer("cpu:0");
    ASSERT_NE(pin, 0u);
    held_pins_.push_back({a_.get(), pin});
    EXPECT_EQ(local_copy_->memory_allocations.load(), 0);
    EXPECT_EQ(local_network_->memory_allocations.load(), 1);
}

TEST(MluStageRpcTest, SendsLocationStringToLegacyServer) {
    std::atomic<bool> received_location{false};
    CoroRpcAgent server;
    ASSERT_TRUE(
        server
            .registerFunction(
                Pin,
                [&](const std::string_view& request, std::string& response) {
                    const auto location =
                        nlohmann::json::parse(request).get<std::string>();
                    received_location.store(location == "cpu:0");
                    response = "4096";
                })
            .ok());
    uint16_t port = 0;
    ASSERT_TRUE(server.start(port).ok());
    uint64_t addr = 0;
    Status status;
    // Each repetition starts a new server. Do not reuse a thread-local
    // connection pool if the server gets a port used by an earlier iteration.
    std::thread caller([&] {
        status = ControlClient::pinStageBuffer(
            "127.0.0.1:" + std::to_string(port), "cpu:0", addr);
    });
    caller.join();
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(received_location.load());
    EXPECT_EQ(addr, 4096u);
}

TEST_P(MluEngineTest,
       CopySubmitFailureRecoversWithoutRawDeviceNetworkRequests) {
    // A single chunk isolates retry routing from staging-pool contention.
    for (auto op : {Request::WRITE, Request::READ}) {
        local_copy_->copy_failures = 1;
        int before = local_copy_->injected_failures;
        ASSERT_NO_FATAL_FAILURE(
            transfer("mlu:0", "mlu:0", op, true, COMPLETED, 65539));
        EXPECT_EQ(local_copy_->injected_failures.load(), before + 1);
    }
}

TEST_P(MluEngineTest, NetworkSubmitFailureRestagesAndReclaimsResources) {
    // A single chunk leaves no other in-flight chunk holding staging pins.
    for (auto op : {Request::WRITE, Request::READ}) {
        local_network_->network_failures = 1;
        int before = local_network_->injected_failures;
        ASSERT_NO_FATAL_FAILURE(
            transfer("mlu:0", "mlu:0", op, true, COMPLETED, 65539));
        EXPECT_EQ(local_network_->injected_failures.load(), before + 1);
    }
}

TEST_P(MluEngineTest, PersistentCopyFailureThenRecoveryReclaimsResources) {
    for (int cycle = 0; cycle < 3; ++cycle) {
        SCOPED_TRACE(cycle);
        // Also reject the TCP local-copy fallback: the underlying device copy
        // is unavailable, regardless of which transport attempts it.
        local_copy_->copy_failures = -1;
        local_network_->copy_failures = -1;
        ASSERT_NO_FATAL_FAILURE(
            transfer("mlu:0", "mlu:0", Request::WRITE, true, FAILED));
        local_copy_->copy_failures = 0;
        local_network_->copy_failures = 0;
        ASSERT_NO_FATAL_FAILURE(
            transfer("mlu:0", "mlu:0", Request::WRITE, true));
    }
    EXPECT_GT(local_copy_->injected_failures.load(), 0);
}

TEST_P(MluEngineTest, PersistentNetworkFailureThenRecoveryReclaimsResources) {
    for (auto op : {Request::WRITE, Request::READ}) {
        local_network_->network_failures = -1;
        ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "mlu:0", op, true, FAILED));
        local_network_->network_failures = 0;
        ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "mlu:0", op, true));
    }
    EXPECT_GT(local_network_->injected_failures.load(), 0);
}

TEST_P(MluEngineTest, NetworkTimeoutRetainsPendingBatchThenReclaimsResources) {
    void *local = nullptr, *remote = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "mlu:0", &local));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &remote, MLU));
    Request request{};
    request.opcode = Request::WRITE;
    request.source = local;
    request.target_offset = reinterpret_cast<uint64_t>(remote);
    request.length = kLength;
    ASSERT_TRUE(a_->openSegment(request.target_id, b_->getSegmentName()).ok());
    local_network_->hold_network = true;
    auto batch = a_->allocateBatch(1);
    ASSERT_NE(batch, 0u);
    batches_.push_back({a_.get(), batch});
    ASSERT_TRUE(a_->submitTransfer(batch, {request}).ok());
    TransferStatus status{};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!local_network_->held_submits.load() &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(a_->progressBatch(batch, status).ok());
        std::this_thread::yield();
    }
    ASSERT_GT(local_network_->held_submits.load(), 0);
    ASSERT_TRUE(a_->progressBatch(batch, status).ok());
    ASSERT_EQ(status.s, PENDING);
    // Releasing the public handle must retain the pending physical batches.
    ASSERT_TRUE(a_->freeBatch(batch).ok());
    EXPECT_GT(a_->aliveBatchCountForTest(), 0u);
    EXPECT_GT(local_network_->live_batches.load(), 0);
    local_network_->expire_network = true;
    // The freed handle may disappear during a sweep; never poll it again.
    batches_.pop_back();
    checkIdleResources();
    local_network_->hold_network = false;
    ASSERT_NO_FATAL_FAILURE(transfer("mlu:0", "mlu:0", Request::WRITE, true));
}

TEST_P(MluEngineTest, PoolPressureRejectsThenCallerRetrySucceeds) {
    for (auto* engine : {a_.get(), b_.get()}) {
        for (auto op : {Request::WRITE, Request::READ}) {
            SCOPED_TRACE(::testing::Message()
                         << "local=" << (engine == a_.get()) << " op=" << op);
            void *local = nullptr, *remote = nullptr;
            ASSERT_NO_FATAL_FAILURE(allocate(*a_, "mlu:0", &local));
            ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &remote, MLU));
            auto& platform = Platform::getLoader();
            std::vector<uint8_t> expected(kLength, 0x5a), zero(kLength, 0),
                actual(kLength);
            void* source = op == Request::WRITE ? local : remote;
            void* target = op == Request::WRITE ? remote : local;
            ASSERT_TRUE(platform.copy(source, expected.data(), kLength).ok());
            ASSERT_TRUE(platform.copy(target, zero.data(), kLength).ok());
            Request request{};
            request.opcode = op;
            request.source = local;
            request.target_offset = reinterpret_cast<uint64_t>(remote);
            request.length = kLength;
            ASSERT_TRUE(
                a_->openSegment(request.target_id, b_->getSegmentName()).ok());
            auto location = engine->getLocalTopology()->findNearMem("mlu:0");
            auto pin = engine->lockStageBuffer(location);
            ASSERT_NE(pin, 0u);
            held_pins_.push_back({engine, pin});
            // Admission needs four slots before dispatching any payload.
            for (auto outcome : {FAILED, COMPLETED}) {
                auto batch = a_->allocateBatch(1);
                ASSERT_NE(batch, 0u);
                batches_.push_back({a_.get(), batch});
                ASSERT_TRUE(a_->submitTransfer(batch, {request}).ok());
                auto status = wait(batch);
                ASSERT_EQ(status.s, outcome);
                EXPECT_EQ(status.transferred_bytes,
                          outcome == COMPLETED ? kLength : 0u);
                ASSERT_TRUE(platform.copy(actual.data(), target, kLength).ok());
                EXPECT_EQ(actual, outcome == COMPLETED ? expected : zero);
                ASSERT_TRUE(a_->freeBatch(batch).ok());
                batches_.pop_back();
                if (outcome == FAILED) {
                    ASSERT_TRUE(engine->unlockStageBuffer(pin).ok());
                    held_pins_.pop_back();
                }
                checkIdleResources();
            }
        }
    }
}

TEST_P(MluEngineTest, CallerRetrySucceedsAfterPendingChunksDrain) {
    for (auto op : {Request::WRITE, Request::READ}) {
        SCOPED_TRACE(op);
        void *local = nullptr, *remote = nullptr;
        ASSERT_NO_FATAL_FAILURE(allocate(*a_, "mlu:0", &local));
        ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &remote, MLU));
        auto& platform = Platform::getLoader();
        std::vector<uint8_t> expected(kLength), zero(kLength, 0),
            actual(kLength);
        for (size_t i = 0; i < kLength; ++i) expected[i] = i % 251;
        void* source = op == Request::WRITE ? local : remote;
        void* target = op == Request::WRITE ? remote : local;
        ASSERT_TRUE(platform.copy(source, expected.data(), kLength).ok());
        ASSERT_TRUE(platform.copy(target, zero.data(), kLength).ok());
        Request request{};
        request.opcode = op;
        request.source = local;
        request.target_offset = reinterpret_cast<uint64_t>(remote);
        request.length = kLength;
        ASSERT_TRUE(
            a_->openSegment(request.target_id, b_->getSegmentName()).ok());
        // Accept a pending network chunk before rejecting a later one.
        local_network_->network_passes_before_failure = 1;
        // Keep rejecting the engine's automatic retry too, so this test
        // observes terminal failure before a separate caller retry.
        local_network_->network_failures = -1;
        local_network_->hold_network = true;
        local_network_->expire_network = false;
        int held_before = local_network_->held_submits;
        int failures_before = local_network_->injected_failures;
        auto batch = a_->allocateBatch(1);
        ASSERT_NE(batch, 0u);
        batches_.push_back({a_.get(), batch});
        ASSERT_TRUE(a_->submitTransfer(batch, {request}).ok());
        TransferStatus status{};
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (local_network_->injected_failures.load() == failures_before &&
               std::chrono::steady_clock::now() < deadline) {
            ASSERT_TRUE(a_->progressBatch(batch, status).ok());
            std::this_thread::yield();
        }
        ASSERT_GT(local_network_->injected_failures.load(), failures_before);
        ASSERT_GT(local_network_->held_submits.load(), held_before);
        EXPECT_GT(local_network_->live_batches.load(), 0);
        ASSERT_TRUE(a_->progressBatch(batch, status).ok());
        EXPECT_EQ(status.s, PENDING);
        // A rejected sibling cannot authorize reuse while an accepted chunk
        // still holds the caller's buffer. Confirm its timeout before failure
        // is visible and the caller retries.
        local_network_->hold_network = false;
        local_network_->expire_network = true;
        status = wait(batch);
        ASSERT_EQ(status.s, FAILED);
        EXPECT_EQ(status.transferred_bytes, 0u);
        ASSERT_TRUE(a_->freeBatch(batch).ok());
        batches_.pop_back();
        checkIdleResources();
        local_network_->network_failures = 0;
        batch = a_->allocateBatch(1);
        ASSERT_NE(batch, 0u);
        batches_.push_back({a_.get(), batch});
        ASSERT_TRUE(a_->submitTransfer(batch, {request}).ok());
        status = wait(batch);
        ASSERT_EQ(status.s, COMPLETED);
        EXPECT_EQ(status.transferred_bytes, kLength);
        ASSERT_TRUE(platform.copy(actual.data(), target, kLength).ok());
        EXPECT_EQ(actual, expected);
        ASSERT_TRUE(a_->freeBatch(batch).ok());
        batches_.pop_back();
        checkIdleResources();
    }
}

TEST_P(MluEngineTest, RejectsMluHintForPeerSegment) {
    void *host = nullptr, *device = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "cpu:0", &host));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &device));
    Request request{};
    request.source = host;
    request.target_offset = reinterpret_cast<uint64_t>(device);
    request.length = kLength;
    request.transport_hint = MLU;
    ASSERT_TRUE(a_->openSegment(request.target_id, b_->getSegmentName()).ok());
    auto route = TransferEngineImplTestPeer::route(*a_, request);
    EXPECT_EQ(route.transport, UNSPEC);
    EXPECT_TRUE(route.staging_params.empty());
}

TEST_P(MluEngineTest, UnstagedHostSourceMustCarryHostTransport) {
    void *host = nullptr, *device = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "cpu:0", &host, MLU));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &device));
    Request request{};
    request.source = host;
    request.target_offset = reinterpret_cast<uint64_t>(device);
    request.length = kLength;
    ASSERT_TRUE(a_->openSegment(request.target_id, b_->getSegmentName()).ok());
    auto route = TransferEngineImplTestPeer::route(*a_, request);
    EXPECT_EQ(route.transport, UNSPEC);
}

TEST_P(MluEngineTest, RejectsRegistrationOutsideDeviceAllocation) {
    void* device = nullptr;
    MemoryOptions options;
    options.location = "mlu:0";
    ASSERT_TRUE(a_->allocateLocalMemory(&device, kLength, options).ok());
    allocations_.push_back({a_.get(), device});
    options.type = UNSPEC;
    auto* tail = static_cast<uint8_t*>(device) + kLength - 1;
    EXPECT_FALSE(a_->registerLocalMemory({tail}, {kLength}, options).ok());
    EXPECT_TRUE(a_->registerLocalMemory({device}, {kLength}, options).ok());
}

TEST_P(MluEngineTest, MissingLocalCopyTransportFailsClosed) {
    void *local = nullptr, *remote = nullptr;
    ASSERT_NO_FATAL_FAILURE(allocate(*a_, "mlu:0", &local));
    ASSERT_NO_FATAL_FAILURE(allocate(*b_, "mlu:0", &remote));
    Request request{};
    request.source = local;
    request.target_offset = reinterpret_cast<uint64_t>(remote);
    request.length = kLength;
    ASSERT_TRUE(a_->openSegment(request.target_id, b_->getSegmentName()).ok());
    TransferEngineImplTestPeer::disableMlu(*a_);
    EXPECT_EQ(TransferEngineImplTestPeer::route(*a_, request).transport,
              UNSPEC);
}

INSTANTIATE_TEST_SUITE_P(NetworkAndDispatchModes, MluEngineTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool(),
                                            ::testing::Bool()));
}  // namespace
}  // namespace tent
}  // namespace mooncake
