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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>

#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

class EFATransportTest : public ::testing::Test {
   public:
   protected:
    void SetUp() override {
        google::InitGoogleLogging("EFATransportTest");
        FLAGS_logtostderr = 1;

        const char *env = std::getenv("MC_METADATA_SERVER");
        if (env)
            metadata_server = env;
        else
            metadata_server = "local://";
        LOG(INFO) << "metadata_server: " << metadata_server;

        env = std::getenv("MC_LOCAL_SERVER_NAME");
        if (env)
            local_server_name = env;
        else
            local_server_name = "127.0.0.2:12345";
        LOG(INFO) << "local_server_name: " << local_server_name;
    }

    void TearDown() override {
        // Clean up glog
        google::ShutdownGoogleLogging();
    }

    std::string metadata_server;
    std::string local_server_name;
};

static void *allocateMemoryPool(size_t size, int socket_id,
                                bool from_vram = false) {
    return numa_alloc_onnode(size, socket_id);
}

TEST_F(EFATransportTest, GetEfaTest) {
    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(false);
    auto hostname_port = parseHostNameWithPort(local_server_name);
    auto rc = engine->init(metadata_server, local_server_name,
                           hostname_port.first.c_str(), hostname_port.second);
    LOG_ASSERT(rc == 0);
    Transport *xport = nullptr;
    xport = engine->installTransport("efa", nullptr);
    LOG_ASSERT(xport != nullptr);
}

TEST_F(EFATransportTest, RegisterMemoryTest) {
    const size_t ram_buffer_size = 1ull << 20;  // 1 MB for testing
    void *addr = nullptr;

    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(false);
    auto hostname_port = parseHostNameWithPort(local_server_name);
    auto rc = engine->init(metadata_server, local_server_name,
                           hostname_port.first.c_str(), hostname_port.second);
    LOG_ASSERT(rc == 0);

    Transport *xport = nullptr;
    xport = engine->installTransport("efa", nullptr);
    LOG_ASSERT(xport != nullptr);

    addr = allocateMemoryPool(ram_buffer_size, 0, false);
    rc = engine->registerLocalMemory(addr, ram_buffer_size, "cpu:0");
    LOG_ASSERT(!rc);

    rc = engine->unregisterLocalMemory(addr);
    LOG_ASSERT(!rc);

    numa_free(addr, ram_buffer_size);
}

TEST_F(EFATransportTest, WriteTest) {
    const size_t kDataLength = 4096000;
    void *addr = nullptr;
    const size_t ram_buffer_size = 1ull << 30;

    // disable topology auto discovery for testing.
    auto engine = std::make_unique<TransferEngine>(false);
    auto hostname_port = parseHostNameWithPort(local_server_name);
    auto rc = engine->init(metadata_server, local_server_name,
                           hostname_port.first.c_str(), hostname_port.second);
    LOG_ASSERT(rc == 0);

    Transport *xport = nullptr;
    xport = engine->installTransport("efa", nullptr);
    LOG_ASSERT(xport != nullptr);

    addr = allocateMemoryPool(ram_buffer_size, 0, false);
    rc = engine->registerLocalMemory(addr, ram_buffer_size, "cpu:0");
    LOG_ASSERT(!rc);

    for (size_t offset = 0; offset < kDataLength; ++offset)
        *((char *)(addr) + offset) = 'a' + lrand48() % 26;

    auto batch_id = engine->allocateBatchID(1);
    Status s;
    auto segment_id = engine->openSegment(local_server_name);
    TransferRequest entry;
    auto segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t *)(addr);
    entry.target_id = segment_id;
    entry.target_offset = remote_base;

    s = engine->submitTransfer(batch_id, {entry});
    LOG_ASSERT(s.ok());

    bool completed = false;
    TransferStatus status;
    while (!completed) {
        Status s = engine->getTransferStatus(batch_id, 0, status);
        ASSERT_EQ(s, Status::OK());
        LOG_ASSERT(status.s != TransferStatusEnum::FAILED);
        if (status.s == TransferStatusEnum::COMPLETED) completed = true;
    }

    s = engine->freeBatchID(batch_id);
    ASSERT_EQ(s, Status::OK());

    numa_free(addr, ram_buffer_size);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
