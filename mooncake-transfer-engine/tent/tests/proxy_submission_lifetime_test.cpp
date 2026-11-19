// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/proxy_manager.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace {
thread_local size_t fail_allocation_size = 0;
thread_local int skip_matching_allocations = 0;
}  // namespace

void* operator new(size_t size) {
    if (size == fail_allocation_size && fail_allocation_size != 0) {
        if (skip_matching_allocations-- == 0) {
            fail_allocation_size = 0;
            throw std::bad_alloc();
        }
    }
    if (void* pointer = std::malloc(size ? size : 1)) return pointer;
    throw std::bad_alloc();
}
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, size_t) noexcept { std::free(pointer); }
void* operator new(size_t size, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(size);
    } catch (...) {
        return nullptr;
    }
}
void operator delete(void* pointer, const std::nothrow_t&) noexcept {
    ::operator delete(pointer);
}

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static void releaseLeakedParent(tent::TransferEngineImpl& engine,
                                    tent::BatchID batch) {
        EXPECT_TRUE(
            engine.releaseBatch(reinterpret_cast<tent::Batch*>(batch)).ok());
    }
};

namespace tent {
namespace {

class ProxySubmissionLifetime : public ::testing::TestWithParam<int> {};

TEST_P(ProxySubmissionLifetime, AllocationFailureDoesNotRetainIdleParent) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");
    for (const auto* name : {"tcp", "shm", "rdma", "io_uring", "nvlink",
                             "mnnvl", "gds", "ascend_direct"}) {
        config->set(std::string("transports/") + name + "/enable", false);
    }
    TransferEngineImpl engine(config);
    ASSERT_TRUE(engine.available());
    auto parent = engine.allocateBatch(0);
    ProxyManager manager(&engine, 64, 2);
    TaskInfo task;
    task.staging = true;
    task.request.opcode = Request::WRITE;
    task.request.length = 1;
    const std::vector<std::string> params{"", kWildcardLocation, ""};

    bool threw = false;
    fail_allocation_size = params.size() * sizeof(std::string);
    skip_matching_allocations = GetParam();
    try {
        auto status = manager.submit(&task, parent, params);
        EXPECT_TRUE(status.ok());
    } catch (const std::bad_alloc&) {
        threw = true;
    }
    fail_allocation_size = 0;
    if (GetParam() == 0) EXPECT_TRUE(threw);

    bool drained = false;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    do {
        ASSERT_TRUE(engine.getBatchDrainStatus(parent, drained).ok());
        if (drained) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (std::chrono::steady_clock::now() < deadline);
    EXPECT_TRUE(drained);
    TransferStatus native;
    ASSERT_TRUE(manager.getStatus(&task, native).ok());
    EXPECT_EQ(native.s, FAILED);
    // Keep RED teardown bounded after observing the leaked runtime reference.
    if (!drained)
        ::mooncake::TransferEngineImplTestPeer::releaseLeakedParent(engine,
                                                                    parent);
    EXPECT_TRUE(engine.freeBatch(parent).ok());
}

INSTANTIATE_TEST_SUITE_P(ParamsOrQueueCopy, ProxySubmissionLifetime,
                         ::testing::Values(0, 1));

}  // namespace
}  // namespace tent
}  // namespace mooncake
