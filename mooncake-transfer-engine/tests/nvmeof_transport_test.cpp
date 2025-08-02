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
#include "common.h"

using namespace mooncake;

namespace mooncake {

DEFINE_string(local_server_name, getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "127.0.0.1:2379", "etcd server host address");
DEFINE_string(mode, "initiator",
              "Running mode: initiator or target. Initiator node read/write "
              "data blocks from target node");
DEFINE_string(operation, "read", "Operation type: read or write");

DEFINE_string(protocol, "rdma", "Transfer protocol: rdma|tcp");

DEFINE_string(device_name, "erdma_1",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(nic_priority_matrix, "",
              "Path to RDMA NIC priority matrix file (Advanced)");

// python /workspace/Mooncake/mooncake-transfer-engine/scripts/register.py
// localhost test_nvmeof /workspace/sample
DEFINE_string(segment_id, "nvmeof/test_nvmeof", "Segment ID to access data");

static void *allocateMemoryPool(size_t size, int socket_id,
                                bool from_vram = false) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) { numa_free(addr, size); }

class NVMeofTransportTest : public ::testing::Test {
   public:
    std::shared_ptr<mooncake::TransferMetadata> metadata_client;
    void *addr = nullptr;
    std::pair<std::string, uint16_t> hostname_port;
    std::unique_ptr<mooncake::TransferEngine> engine;
    const size_t ram_buffer_size = 1ull << 30;
    Transport *xport;
    std::string nic_priority_matrix;
    void **args;
    mooncake::Transport::SegmentID segment_id;
    std::shared_ptr<TransferMetadata::SegmentDesc> segment_desc;
    uint64_t remote_base;

   protected:
    void SetUp() override {
        static int offset = 0;
        google::InitGoogleLogging("NVMeofTransportTest");
        FLAGS_logtostderr = 1;
        // disable topology auto discovery for testing.
        engine = std::make_unique<TransferEngine>(false);
        hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                     hostname_port.first.c_str(),
                     hostname_port.second + offset++);
        xport = nullptr;
        args = (void **)malloc(2 * sizeof(void *));
        args[0] = nullptr;
        xport = engine->installTransport("nvmeof", args);
        ASSERT_NE(xport, nullptr);
        addr = allocateMemoryPool(ram_buffer_size, 0, false);
        int rc = engine->registerLocalMemory(addr, ram_buffer_size, "cpu:0");
        ASSERT_EQ(rc, 0);
        segment_id = engine->openSegment(FLAGS_segment_id.c_str());
        bindToSocket(0);
        segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
        remote_base = 0;
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, ram_buffer_size);
    }
};

TEST_F(NVMeofTransportTest, MultiWrite) {
    const size_t kDataLength = 4096000;
    int times = 10;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char *)(addr) + offset) = 'a' + lrand48() % 26;
        auto batch_id = xport->allocateBatchID(1);
        Status s;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        s = xport->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                LOG(INFO) << "FAILED";
                completed = true;
            }
        }
        s = xport->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
    }
}

TEST_F(NVMeofTransportTest, MultipleRead) {
    const size_t kDataLength = 4096000;
    int times = 10;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char *)(addr) + offset) = 'a' + lrand48() % 26;

        auto batch_id = xport->allocateBatchID(1);
        Status s;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        s = xport->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                LOG(INFO) << "FAILED";
                completed = true;
            }
        }
        s = engine->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
    }
    times = 10;
    while (times--) {
        auto batch_id = xport->allocateBatchID(1);
        int ret = 0;
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr) + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        Status s;
        s = xport->submitTransfer(batch_id, {entry});
        ASSERT_EQ(s, Status::OK());
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                completed = true;
            }
        }
        s = xport->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
        ret = memcmp((uint8_t *)(addr), (uint8_t *)(addr) + kDataLength,
                     kDataLength);
        ASSERT_EQ(ret, 0);
    }
    engine->unregisterLocalMemory(addr);
    freeMemoryPool(addr, ram_buffer_size);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
