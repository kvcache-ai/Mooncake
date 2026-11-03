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

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <algorithm>
#include <utility>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"
#include "tent/runtime/proxy_manager.h"

#if defined(__linux__)
namespace {
std::atomic<bool> fail_next_segment_sync{false};
}
extern "C" mooncake::tent::Status
__real__ZN8mooncake4tent14SegmentManager16synchronizeLocalEv(
    mooncake::tent::SegmentManager* manager);
extern "C" mooncake::tent::Status
__wrap__ZN8mooncake4tent14SegmentManager16synchronizeLocalEv(
    mooncake::tent::SegmentManager* manager) {
    if (fail_next_segment_sync.exchange(false))
        return mooncake::tent::Status::MetadataError(
            "injected registry write failure");
    return __real__ZN8mooncake4tent14SegmentManager16synchronizeLocalEv(
        manager);
}

#endif

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static std::vector<tent::BufferDesc> localBuffers(
        tent::TransferEngineImpl& engine) {
        const auto local = engine.metadata_->segmentManager().getLocal();
        return std::get<tent::MemorySegmentDesc>(local->detail).buffers;
    }
};

namespace tent {
namespace {

class FailOnceFreeTransport : public Transport {
   public:
    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions&) override {
        *addr = std::malloc(size);
        return *addr ? Status::OK()
                     : Status::InternalError("malloc failed" LOC_MARK);
    }

    Status freeLocalMemory(void* addr, size_t) override {
        ++free_calls;
        if (free_calls == 1) {
            return Status::InternalError("injected free failure" LOC_MARK);
        }
        std::free(addr);
        return Status::OK();
    }

    const char* getName() const override { return "fail-once-free"; }

    int free_calls{0};
};

class FailOnceRemoveTransport : public Transport {
   public:
    explicit FailOnceRemoveTransport(TransportType type, bool fail = false)
        : type(type), fail(fail) {}
    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(type);
        return Status::OK();
    }
    Status removeMemoryBuffer(BufferDesc& desc) override {
        ++remove_calls;
        if (std::exchange(fail, false))
            return Status::InternalError("injected drain failure");
        desc.transports.erase(
            std::remove(desc.transports.begin(), desc.transports.end(), type),
            desc.transports.end());
        return Status::OK();
    }
    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions&) override {
        ++allocate_calls;
        *addr = std::malloc(size);
        return Status::OK();
    }
    Status freeLocalMemory(void* addr, size_t) override {
        ++free_calls;
        std::free(addr);
        return Status::OK();
    }
    const char* getName() const override { return "fail-once-remove"; }
    TransportType type;
    bool fail;
    int remove_calls = 0, allocate_calls = 0, free_calls = 0;
};

std::shared_ptr<Config> makeConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");

    for (const char* transport : {"tcp", "shm", "rdma", "io_uring", "nvlink",
                                  "mnnvl", "gds", "ascend_direct", "mlu"}) {
        config->set(std::string("transports/") + transport + "/enable", false);
    }
    return config;
}

TEST(LocalMemoryLifecycle, RetainsOwnershipWhenTransportFreeFails) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    auto transport = std::make_shared<FailOnceFreeTransport>();
    engine.swapTransportForTest(RDMA, transport);

    MemoryOptions options;
    options.type = RDMA;
    options.location = "cpu:0";
    void* addr = nullptr;
    ASSERT_TRUE(engine.allocateLocalMemory(&addr, 4096, options).ok());

    auto first = engine.freeLocalMemory(addr);
    EXPECT_FALSE(first.ok());
    EXPECT_EQ(transport->free_calls, 1);

    auto second = engine.freeLocalMemory(addr);
    EXPECT_TRUE(second.ok()) << second.ToString();
    EXPECT_EQ(transport->free_calls, 2);

    auto third = engine.freeLocalMemory(addr);
    EXPECT_TRUE(third.IsInvalidArgument());
    EXPECT_EQ(transport->free_calls, 2);
}

TEST(LocalMemoryLifecycle, SegmentInfoReplacesPreviouslyReportedBuffers) {
    auto config = makeConfig();
    config->set("transports/tcp/enable", true);
    std::vector<char> buffer(4096);
    TransferEngineImpl engine(config);
    ASSERT_TRUE(engine.available());
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    SegmentInfo info;
    ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    ASSERT_EQ(info.buffers.size(), 1u);
    EXPECT_EQ(info.buffers.front().base,
              reinterpret_cast<uint64_t>(buffer.data()));
    EXPECT_EQ(info.buffers.front().length, buffer.size());

    // Reusing the output must not duplicate buffers from the previous call.
    ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    EXPECT_EQ(info.buffers.size(), 1u);

    ASSERT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
    ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    EXPECT_TRUE(info.buffers.empty())
        << "An unregistered buffer must not survive a segment-info refresh";
}

TEST(LocalMemoryLifecycle, ShmAcceptsDefaultAllocationLocation) {
    auto config = makeConfig();
    config->set("transports/shm/enable", true);
    TransferEngineImpl engine(config);
    ASSERT_TRUE(engine.available());

    void* address = nullptr;
    auto status = engine.allocateLocalMemory(&address, 4096, kWildcardLocation);
    ASSERT_TRUE(status.ok()) << status.ToString();
    std::memset(address, 0x5a, 4096);
    EXPECT_TRUE(engine.freeLocalMemory(address).ok());
}

TEST(LocalMemoryLifecycle, DefaultHostOptionsAllocateUsableSharedMemory) {
    auto config = makeConfig();
    config->set("transports/shm/enable", true);
    TransferEngineImpl target(config);
    TransferEngineImpl initiator(config);
    ASSERT_TRUE(target.available());
    ASSERT_TRUE(initiator.available());

    MemoryOptions target_options;
    MemoryOptions source_options;
    source_options.location = "cpu:0";
    void* source = nullptr;
    void* destination = nullptr;
    constexpr size_t kSize = 4096;
    auto status = target.allocateLocalMemory(&source, kSize, target_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = initiator.allocateLocalMemory(&destination, kSize, source_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(target_options.type, SHM);
    EXPECT_EQ(source_options.type, SHM);
    EXPECT_EQ(target_options.location, kWildcardLocation);
    EXPECT_EQ(source_options.location, "cpu:0");
    ASSERT_FALSE(target_options.shm_path.empty());
    ASSERT_TRUE(
        target.registerLocalMemory({source}, {kSize}, target_options).ok());
    ASSERT_TRUE(
        initiator.registerLocalMemory({destination}, {kSize}, source_options)
            .ok());
    std::memset(source, 0x6b, kSize);
    std::memset(destination, 0, kSize);

    SegmentID peer;
    ASSERT_TRUE(initiator.openSegment(peer, target.getSegmentName()).ok());
    Request request;
    request.opcode = Request::READ;
    request.source = destination;
    request.target_id = peer;
    request.target_offset = reinterpret_cast<uint64_t>(source);
    request.length = kSize;
    ASSERT_TRUE(initiator.transferSync({request}).ok());
    EXPECT_EQ(std::memcmp(source, destination, kSize), 0);

    EXPECT_TRUE(initiator.closeSegment(peer).ok());
    EXPECT_TRUE(initiator.unregisterLocalMemory(destination).ok());
    EXPECT_TRUE(target.unregisterLocalMemory(source).ok());
    EXPECT_TRUE(initiator.freeLocalMemory(destination).ok());
    EXPECT_TRUE(target.freeLocalMemory(source).ok());
}

TEST(LocalMemoryLifecycle, BasicShmAllocationsRegisterForReadAndWrite) {
    auto config = makeConfig();
    config->set("transports/shm/enable", true);
    TransferEngineImpl target(config);
    TransferEngineImpl initiator(config);
    ASSERT_TRUE(target.available());
    ASSERT_TRUE(initiator.available());

    constexpr size_t kSize = 4096;
    void* source = nullptr;
    void* destination = nullptr;
    ASSERT_TRUE(target.allocateLocalMemory(&source, kSize).ok());
    ASSERT_TRUE(initiator.allocateLocalMemory(&destination, kSize).ok());
    ASSERT_TRUE(target.registerLocalMemory(source, kSize).ok());
    ASSERT_TRUE(initiator
                    .registerLocalMemory(std::vector<void*>{destination},
                                         std::vector<size_t>{kSize})
                    .ok());

    SegmentID peer;
    ASSERT_TRUE(initiator.openSegment(peer, target.getSegmentName()).ok());
    Request request;
    request.source = destination;
    request.target_id = peer;
    request.target_offset = reinterpret_cast<uint64_t>(source);
    request.length = kSize;
    for (auto opcode : {Request::READ, Request::WRITE}) {
        request.opcode = opcode;
        std::memset(source, 0x6b, kSize);
        std::memset(destination, 0x5a, kSize);
        auto status = initiator.transferSync({request});
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(std::memcmp(source, destination, kSize), 0);
    }

    EXPECT_TRUE(initiator.closeSegment(peer).ok());
    EXPECT_TRUE(initiator.unregisterLocalMemory(destination).ok());
    EXPECT_TRUE(target.unregisterLocalMemory(source).ok());
    EXPECT_TRUE(initiator.freeLocalMemory(destination).ok());
    EXPECT_TRUE(target.freeLocalMemory(source).ok());
}

TEST(LocalMemoryLifecycle, FailedUnregisterRetainsRegistrationForRetry) {
    for (bool vector_api : {false, true}) {
        TransferEngineImpl engine(makeConfig());
        ASSERT_TRUE(engine.available());
        auto rdma = std::make_shared<FailOnceRemoveTransport>(RDMA);
        auto mlu = std::make_shared<FailOnceRemoveTransport>(MLU, true);
        engine.swapTransportForTest(RDMA, rdma);
        engine.swapTransportForTest(MLU, mlu);
        std::vector<char> buffer(4096);
        ASSERT_TRUE(
            engine.registerLocalMemory(buffer.data(), buffer.size()).ok());
        auto unregister = [&] {
            return vector_api
                       ? engine.unregisterLocalMemory(
                             std::vector<void*>{buffer.data()}, {buffer.size()})
                       : engine.unregisterLocalMemory(buffer.data(),
                                                      buffer.size());
        };
        EXPECT_FALSE(unregister().ok());
        SegmentInfo info;
        ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
        ASSERT_EQ(info.buffers.size(), 1u);
        EXPECT_EQ(rdma->remove_calls, 1);
        EXPECT_EQ(mlu->remove_calls, 1);
        EXPECT_TRUE(unregister().ok());
        EXPECT_EQ(rdma->remove_calls, 1)
            << "Do not deregister a successful backend twice";
        EXPECT_EQ(mlu->remove_calls, 2);
        ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
        EXPECT_TRUE(info.buffers.empty());
    }
}

TEST(LocalMemoryLifecycle, InternalHostAllocationDoesNotImplicitlySelectMlu) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());
    auto tcp = std::make_shared<FailOnceRemoveTransport>(TCP);
    auto mlu = std::make_shared<FailOnceRemoveTransport>(MLU);
    engine.swapTransportForTest(TCP, tcp);
    engine.swapTransportForTest(MLU, mlu);
    void* address = nullptr;
    ASSERT_TRUE(engine.allocateLocalMemory(&address, 4096, "cpu:0", true).ok());
    EXPECT_EQ(tcp->allocate_calls, 1);
    EXPECT_EQ(mlu->allocate_calls, 0);
    EXPECT_TRUE(engine.freeLocalMemory(address).ok());
    // An explicit MLU type selects the MLU allocation backend.
    MemoryOptions options;
    options.location = "cpu:0";
    options.type = MLU;
    options.internal = true;
    ASSERT_TRUE(engine.allocateLocalMemory(&address, 4096, options).ok());
    EXPECT_EQ(mlu->allocate_calls, 1);
    EXPECT_TRUE(engine.freeLocalMemory(address).ok());
}

#if defined(__linux__)
TEST(LocalMemoryLifecycle,
     StageMetadataFailureDoesNotLeaveFreedRegisteredBuffer) {
    for (bool with_mlu : {false, true}) {
        SCOPED_TRACE(with_mlu);
        TransferEngineImpl engine(makeConfig());
        ASSERT_TRUE(engine.available());
        auto tcp = std::make_shared<FailOnceRemoveTransport>(TCP);
        auto mlu = std::make_shared<FailOnceRemoveTransport>(MLU);
        engine.swapTransportForTest(TCP, tcp);
        if (with_mlu) engine.swapTransportForTest(MLU, mlu);
        ProxyManager proxy(&engine, 4096, 2);
        fail_next_segment_sync = true;
        uint64_t address = 0;
        const auto status = proxy.pinStageBuffer("cpu:0", address);
        EXPECT_TRUE(status.IsMetadataError()) << status;
        EXPECT_FALSE(fail_next_segment_sync.load());
        EXPECT_EQ((with_mlu ? mlu : tcp)->free_calls, 1);
        SegmentInfo info;
        ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
        EXPECT_TRUE(info.buffers.empty());
        // Internal staging registrations are hidden from getSegmentInfo().
        const auto buffers = TransferEngineImplTestPeer::localBuffers(engine);
        EXPECT_TRUE(buffers.empty())
            << "Registration survives after its stage allocation was freed";
        // Clean up the stale test registration without touching freed memory.
        for (const auto& buffer : buffers)
            EXPECT_TRUE(
                engine
                    .unregisterLocalMemory(reinterpret_cast<void*>(buffer.addr),
                                           buffer.length)
                    .ok());
    }
}

TEST(LocalMemoryLifecycle, StageMetadataFailureRetainsMemoryIfRollbackFails) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());
    auto tcp = std::make_shared<FailOnceRemoveTransport>(TCP, true);
    auto mlu = std::make_shared<FailOnceRemoveTransport>(MLU);
    engine.swapTransportForTest(TCP, tcp);
    engine.swapTransportForTest(MLU, mlu);
    ProxyManager proxy(&engine, 4096, 2);
    fail_next_segment_sync = true;
    uint64_t address = 0;
    EXPECT_TRUE(proxy.pinStageBuffer("cpu:0", address).IsMetadataError());
    EXPECT_EQ(mlu->free_calls, 0);
    EXPECT_EQ(tcp->remove_calls, 1);
    EXPECT_EQ(mlu->remove_calls, 1);
    SegmentInfo info;
    ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    EXPECT_TRUE(info.buffers.empty());
    const auto buffers = TransferEngineImplTestPeer::localBuffers(engine);
    ASSERT_EQ(buffers.size(), 1u);
    EXPECT_TRUE(buffers.front().internal);
    void* retained = reinterpret_cast<void*>(buffers.front().addr);
    // A failed rollback remains owned by the engine and can be retried safely.
    EXPECT_TRUE(engine.unregisterLocalMemory(retained).ok());
    EXPECT_EQ(tcp->remove_calls, 2);
    EXPECT_EQ(mlu->remove_calls, 1);
    EXPECT_TRUE(engine.freeLocalMemory(retained).ok());
    EXPECT_EQ(mlu->free_calls, 1);
    EXPECT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    EXPECT_TRUE(info.buffers.empty());
    EXPECT_TRUE(TransferEngineImplTestPeer::localBuffers(engine).empty());
}

#endif

TEST(LocalMemoryLifecycle, StagePoolSelectsAllocatorFromLocalTransports) {
    for (bool with_mlu : {false, true}) {
        SCOPED_TRACE(with_mlu);
        TransferEngineImpl engine(makeConfig());
        ASSERT_TRUE(engine.available());
        auto tcp = std::make_shared<FailOnceRemoveTransport>(TCP);
        auto mlu = std::make_shared<FailOnceRemoveTransport>(MLU);
        engine.swapTransportForTest(TCP, tcp);
        if (with_mlu) engine.swapTransportForTest(MLU, mlu);
        {
            ProxyManager proxy(&engine, 4096, 2);
            uint64_t first = 0, second = 0;
            ASSERT_TRUE(proxy.pinStageBuffer("cpu:0", first).ok());
            ASSERT_TRUE(proxy.pinStageBuffer("cpu:0", second).ok());
            EXPECT_NE(first, second);
            EXPECT_EQ(tcp->allocate_calls, with_mlu ? 0 : 1);
            EXPECT_EQ(mlu->allocate_calls, with_mlu ? 1 : 0);
            EXPECT_TRUE(proxy.unpinStageBuffer(first).ok());
            EXPECT_TRUE(proxy.unpinStageBuffer(second).ok());
        }
        SegmentInfo info;
        ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
        EXPECT_TRUE(info.buffers.empty());
        EXPECT_EQ(tcp->remove_calls, 1);
        EXPECT_EQ(mlu->remove_calls, with_mlu ? 1 : 0);
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
