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

// Multi-Protocol Transport Test
// Tests mp_registerLocalMemory, mp_unregisterLocalMemory, mp_submitTransfer

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
DEFINE_string(protocol, "cxl,tcp",
              "Multi-protocol combination: cxl,tcp|cxl,rdma");

DEFINE_string(cxl_device, "tmp_dax_sim", "Device name for cxl");
DEFINE_int64(cxl_device_size, 1073741824, "Device Size for cxl");

static void *allocateMemoryPool(size_t size, int socket_id,
                                bool from_vram = false) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) { numa_free(addr, size); }

// Parse protocol string like "cxl,tcp" into vector
static std::vector<std::string> parseProtocols(
    const std::string &protocol_str) {
    std::vector<std::string> protocols;
    std::stringstream ss(protocol_str);
    std::string item;
    while (getline(ss, item, ',')) {
        protocols.push_back(item);
    }
    return protocols;
}

class MultiProtocolTransportTest : public ::testing::Test {
   public:
    int tmp_fd = -1;
    uint8_t *addr = nullptr;
    uint8_t *base_addr = nullptr;
    std::pair<std::string, uint16_t> hostname_port;
    std::unique_ptr<TransferEngine> engine;
    Transport *xport = nullptr;
    CxlTransport *cxl_xport = nullptr;
    void **args = nullptr;
    SegmentID segment_id = -1;
    std::shared_ptr<TransferMetadata::SegmentDesc> segment_desc;
    const size_t kDataLength = 4 * 1024;
    const size_t offset_1 = 2 * 1024 * 1024;
    const size_t len = 2 * 1024 * 1024;

   protected:
    void SetUp() override {
        static int offset = 0;
        google::InitGoogleLogging("MultiProtocolTransportTest");
        FLAGS_logtostderr = 1;

        // Parse protocols
        std::vector<std::string> protocols = parseProtocols(FLAGS_protocol);
        ASSERT_EQ(protocols.size(), 2)
            << "Multi-protocol test requires exactly 2 protocols";

        // Setup CXL device
        tmp_fd = open(FLAGS_cxl_device.c_str(), O_RDWR | O_CREAT, 0666);
        ASSERT_GE(tmp_fd, 0);
        ASSERT_EQ(ftruncate(tmp_fd, FLAGS_cxl_device_size), 0);
        setenv("MC_CXL_DEV_PATH", FLAGS_cxl_device.c_str(), 1);
        setenv("MC_CXL_DEV_SIZE", std::to_string(FLAGS_cxl_device_size).c_str(),
               1);

        // Init engine
        engine = std::make_unique<TransferEngine>(false);
        hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        int rc = engine->init(
            FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
            hostname_port.first.c_str(), hostname_port.second + offset++);
        ASSERT_EQ(rc, 0);

        // Install transports for each protocol
        for (const auto &proto : protocols) {
            if (proto == "cxl") {
                args = (void **)malloc(2 * sizeof(void *));
                args[0] = nullptr;
                xport = engine->installTransport("cxl", args);
                ASSERT_NE(xport, nullptr);
                cxl_xport = dynamic_cast<CxlTransport *>(xport);
                base_addr = (uint8_t *)cxl_xport->getCxlBaseAddr();
            } else if (proto == "tcp") {
                Transport *t = engine->installTransport("tcp", nullptr);
                ASSERT_NE(t, nullptr);
            }
        }

        // Allocate memory
        addr = (uint8_t *)allocateMemoryPool(kDataLength, 0, false);
        ASSERT_NE(addr, nullptr);

        // Register memory using mp_registerLocalMemory
        std::unordered_map<std::string,
                           std::vector<TransferEngine::RegisteredBuffer>>
            buffer_map;
        for (const auto &proto : protocols) {
            if (proto == "cxl") {
                buffer_map[proto].emplace_back(base_addr + offset_1, len);
            } else {
                buffer_map[proto].emplace_back(addr, kDataLength, "cpu:0");
            }
        }
        rc = engine->mp_registerLocalMemory(buffer_map);
        ASSERT_EQ(rc, 0);

        // Open segment
        segment_id = engine->openSegment(FLAGS_local_server_name.c_str());
        segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    }

    void TearDown() override {
        if (tmp_fd >= 0) {
            close(tmp_fd);
            unlink(FLAGS_cxl_device.c_str());
        }
        if (args) free(args);
        google::ShutdownGoogleLogging();
        if (addr) freeMemoryPool(addr, kDataLength);
    }
};

TEST_F(MultiProtocolTransportTest, MultiWrite) {
    std::vector<std::string> protocols = parseProtocols(FLAGS_protocol);
    std::string proto = protocols.empty() ? "cxl" : protocols[0];

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

        s = engine->mp_submitTransfer(batch_id, {entry}, proto);
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

TEST_F(MultiProtocolTransportTest, BasicRegistration) {
    // Test basic registration/unregistration with new memory
    std::vector<std::string> protocols = parseProtocols(FLAGS_protocol);

    // Allocate new memory for this test
    const size_t test_len = 1024 * 1024;  // 1MB
    void *test_addr = allocateMemoryPool(test_len, 0);
    ASSERT_NE(test_addr, nullptr);

    std::unordered_map<std::string,
                       std::vector<TransferEngine::RegisteredBuffer>>
        buffer_map;
    for (const auto &proto : protocols) {
        if (proto == "cxl") {
            // Use different offset for CXL (offset_1 + len to avoid overlap)
            buffer_map[proto].emplace_back(base_addr + offset_1 + len,
                                           test_len);
        } else {
            buffer_map[proto].emplace_back(test_addr, test_len, "cpu:0");
        }
    }

    int rc = engine->mp_registerLocalMemory(buffer_map);
    ASSERT_EQ(rc, 0);

    rc = engine->mp_unregisterLocalMemory(buffer_map);
    ASSERT_EQ(rc, 0);

    freeMemoryPool(test_addr, test_len);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
