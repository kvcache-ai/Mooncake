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
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "transport/cxl_transport/cxl_transport.h"
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

DEFINE_string(protocol, "cxl", "Transfer protocol: rdma|tcp|cxl");

DEFINE_string(device_name, "tmp_dax_sim", "Device name for cxl");

DEFINE_int64(device_size, 1073741824, "Device Size for cxl");

static void *allocateMemoryPool(size_t size, int socket_id,
                                bool from_vram = false) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) { numa_free(addr, size); }

class CXLTransportTest : public ::testing::Test {
   public:
    std::shared_ptr<mooncake::TransferMetadata> metadata_client;
    int tmp_fd = -1;
    uint8_t *addr = nullptr;
    uint8_t *base_addr;
    std::pair<std::string, uint16_t> hostname_port;
    std::unique_ptr<mooncake::TransferEngine> engine;
    const size_t offset_1 = 2 * 1024 * 1024;
    const size_t offset_2 = 6 * 1024 * 1024;
    const size_t len = 2 * 1024 * 1024;
    CxlTransport *cxl_xport;
    Transport *xport;
    void **args;
    mooncake::Transport::SegmentID segment_id;
    std::shared_ptr<TransferMetadata::SegmentDesc> segment_desc;
    const size_t kDataLength = 4 * 1024;
    bool cxl_available = true;

   protected:
    void SetUp() override {
        static int offset = 0;
        google::InitGoogleLogging("CXLTransportTest");
        FLAGS_logtostderr = 1;

        tmp_fd = open(FLAGS_device_name.c_str(), O_RDWR | O_CREAT, 0666);
        ASSERT_GE(tmp_fd, 0);
        ASSERT_EQ(ftruncate(tmp_fd, FLAGS_device_size), 0);

        // Set device name from gflags parameter
        setenv("MC_CXL_DEV_PATH", FLAGS_device_name.c_str(), 1);

        std::string device_size_str = std::to_string(FLAGS_device_size);
        setenv("MC_CXL_DEV_SIZE", device_size_str.c_str(), 1);

        // cxl setup
        engine = std::make_unique<TransferEngine>(false);
        hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                     hostname_port.first.c_str(),
                     hostname_port.second + offset++);
        xport = nullptr;

        args = (void **)malloc(2 * sizeof(void *));
        args[0] = nullptr;
        xport = engine->installTransport("cxl", args);
        if (xport == nullptr) {
            cxl_available = false;
            LOG(WARNING) << "CXL device not available, tests will be skipped";
            return;
        }

        cxl_xport = dynamic_cast<CxlTransport *>(xport);
        base_addr = (uint8_t *)cxl_xport->getCxlBaseAddr();
        addr = (uint8_t *)allocateMemoryPool(kDataLength, 0, false);
        int rc = engine->registerLocalMemory(base_addr + offset_1, len);
        ASSERT_EQ(rc, 0);

        segment_id = engine->openSegment(FLAGS_local_server_name.c_str());
        // bindToSocket(0);
        segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    }

    void TearDown() override {
        if (tmp_fd >= 0) {
            close(tmp_fd);
            unlink(FLAGS_device_name.c_str());
        }
        if (args) {
            free(args);
        }
        google::ShutdownGoogleLogging();
        if (addr) {
            freeMemoryPool(addr, kDataLength);
        }
    }
};

TEST_F(CXLTransportTest, MultiWrite) {
    if (!cxl_available) {
        GTEST_SKIP() << "CXL device not available";
    }
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
        entry.target_offset = offset_1;
        // s = xport->submitTransfer(batch_id, {entry});
        s = engine->submitTransfer(batch_id, {entry});
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

TEST_F(CXLTransportTest, MultipleRead) {
    if (!cxl_available) {
        GTEST_SKIP() << "CXL device not available";
    }
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
        entry.target_offset = offset_2;
        // s = xport->submitTransfer(batch_id, {entry});
        s = engine->submitTransfer(batch_id, {entry});
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

    // sleep(10);

    times = 10;
    while (times--) {
        auto batch_id = xport->allocateBatchID(1);
        int ret = 0;
        void *src = allocateMemoryPool(kDataLength, 0, false);

        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(src);
        entry.target_id = segment_id;
        entry.target_offset = offset_2;
        Status s;
        // s = xport->submitTransfer(batch_id, {entry});
        s = engine->submitTransfer(batch_id, {entry});
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
        ret = memcmp((uint8_t *)(src), (uint8_t *)(addr), kDataLength);
        ASSERT_EQ(ret, 0);

        freeMemoryPool(src, kDataLength);
    }
    engine->unregisterLocalMemory(addr);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
