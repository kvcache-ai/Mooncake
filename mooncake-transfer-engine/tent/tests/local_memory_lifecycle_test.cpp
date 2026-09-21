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

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake {
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

std::shared_ptr<Config> makeConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");

    for (const char* transport : {"tcp", "shm", "rdma", "io_uring", "nvlink",
                                  "mnnvl", "gds", "ascend_direct"}) {
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

}  // namespace
}  // namespace tent
}  // namespace mooncake
