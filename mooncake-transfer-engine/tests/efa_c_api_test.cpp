// Copyright 2024 KVCache.AI
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
#include <numa.h>

#include <cstring>

#include "transfer_engine_c.h"

// Test: discoverTopology + installTransport("efa") via pure C API
TEST(EfaCApiTest, DiscoverTopologyAndInstallEfa) {
    transfer_engine_t engine = createTransferEngine(
        "P2PHANDSHAKE", "127.0.0.1:12345", "127.0.0.1", 12345, 0);
    ASSERT_NE(engine, nullptr) << "createTransferEngine failed";

    int ret = discoverTopology(engine);
    ASSERT_EQ(ret, 0) << "discoverTopology failed with code " << ret;

    transport_t xport = installTransport(engine, "efa", NULL);
    ASSERT_NE(xport, nullptr) << "installTransport(\"efa\") failed";

    destroyTransferEngine(engine);
}

// Test: registerLocalMemory works after C API EFA setup
TEST(EfaCApiTest, RegisterMemoryAfterCApiSetup) {
    transfer_engine_t engine = createTransferEngine(
        "P2PHANDSHAKE", "127.0.0.1:22345", "127.0.0.1", 22345, 0);
    ASSERT_NE(engine, nullptr);

    ASSERT_EQ(discoverTopology(engine), 0);
    ASSERT_NE(installTransport(engine, "efa", NULL), nullptr);

    size_t buf_size = 1 << 20;  // 1 MB
    void* buf = numa_alloc_onnode(buf_size, 0);
    ASSERT_NE(buf, nullptr);
    memset(buf, 0xAB, buf_size);

    int ret = registerLocalMemory(engine, buf, buf_size, "cpu:0", 1);
    EXPECT_EQ(ret, 0) << "registerLocalMemory failed with code " << ret;

    unregisterLocalMemory(engine, buf);
    numa_free(buf, buf_size);
    destroyTransferEngine(engine);
}

// Test: registerLocalMemoryBatch works after C API EFA setup
TEST(EfaCApiTest, RegisterMemoryBatchAfterCApiSetup) {
    transfer_engine_t engine = createTransferEngine(
        "P2PHANDSHAKE", "127.0.0.1:32345", "127.0.0.1", 32345, 0);
    ASSERT_NE(engine, nullptr);

    ASSERT_EQ(discoverTopology(engine), 0);
    ASSERT_NE(installTransport(engine, "efa", NULL), nullptr);

    const int num_bufs = 4;
    size_t buf_size = 1 << 20;
    buffer_entry_t entries[4];
    for (int i = 0; i < num_bufs; i++) {
        entries[i].addr = numa_alloc_onnode(buf_size, 0);
        ASSERT_NE(entries[i].addr, nullptr);
        entries[i].length = buf_size;
        memset(entries[i].addr, 0xCD, buf_size);
    }

    int ret = registerLocalMemoryBatch(engine, entries, num_bufs, "cpu:0");
    EXPECT_EQ(ret, 0) << "registerLocalMemoryBatch failed with code " << ret;

    void* addrs[4];
    for (int i = 0; i < num_bufs; i++) addrs[i] = entries[i].addr;
    unregisterLocalMemoryBatch(engine, addrs, num_bufs);
    for (int i = 0; i < num_bufs; i++) numa_free(entries[i].addr, buf_size);
    destroyTransferEngine(engine);
}

// Negative test: without discoverTopology, installTransport should fail
TEST(EfaCApiTest, InstallEfaWithoutDiscoverFails) {
    transfer_engine_t engine = createTransferEngine(
        "P2PHANDSHAKE", "127.0.0.1:42345", "127.0.0.1", 42345, 0);
    ASSERT_NE(engine, nullptr);

    transport_t xport = installTransport(engine, "efa", NULL);
    EXPECT_EQ(xport, nullptr)
        << "installTransport should fail without discover";

    destroyTransferEngine(engine);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
