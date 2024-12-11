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

#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda_runtime.h>
#include <cufile.h>

#include <cassert>

static void checkCudaError(cudaError_t result, const char *message) {
    if (result != cudaSuccess) {
        LOG(ERROR) << message << " (Error code: " << result << " - "
                   << cudaGetErrorString(result) << ")" << std::endl;
        exit(EXIT_FAILURE);
    }
}

#endif

#include "transfer_engine.h"
#include "transport/transport.h"

#ifdef USE_CUDA
DEFINE_int32(gpu_id, 0, "GPU ID to use");
#endif

using namespace mooncake;

//// etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls
/// http://10.0.0.1:2379 / ./tcp_transport_test
namespace mooncake {

class TCPTransportTest : public ::testing::Test {
   public:
   protected:
    void SetUp() override {
        google::InitGoogleLogging("TCPTransportTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override {
        // 清理 glog
        google::ShutdownGoogleLogging();
    }
};

static void *allocateMemoryPool(size_t size, int socket_id,
                                bool from_vram = false) {
    return numa_alloc_onnode(size, socket_id);
}

TEST_F(TCPTransportTest, GetTcpTest) {
    auto metadata_client = std::make_shared<TransferMetadata>("127.0.0.1:2379");
    LOG_ASSERT(metadata_client);

    auto engine = std::make_unique<TransferEngine>(metadata_client);

    auto hostname_port = parseHostNameWithPort("127.0.0.2:12345");
    engine->init("127.0.0.2:12345", hostname_port.first.c_str(),
                 hostname_port.second);
    Transport *xport = nullptr;
    xport = engine->installOrGetTransport("tcp", nullptr);
    LOG_ASSERT(xport != nullptr);
}

TEST_F(TCPTransportTest, Writetest) {
    const size_t kDataLength = 4096000;
    void *addr = nullptr;
    const size_t ram_buffer_size = 1ull << 30;
    auto metadata_client = std::make_shared<TransferMetadata>("127.0.0.1:2379");
    LOG_ASSERT(metadata_client);

    auto engine = std::make_unique<TransferEngine>(metadata_client);

    auto hostname_port = parseHostNameWithPort("127.0.0.2:12345");
    engine->init("127.0.0.2:12345", hostname_port.first.c_str(),
                 hostname_port.second);
    Transport *xport = nullptr;
    xport = engine->installOrGetTransport("tcp", nullptr);
    LOG_ASSERT(xport != nullptr);

    addr = allocateMemoryPool(ram_buffer_size, 0, false);
    int rc = engine->registerLocalMemory(addr, ram_buffer_size, "cpu:0");
    LOG_ASSERT(!rc);

    for (size_t offset = 0; offset < kDataLength; ++offset)
        *((char *)(addr) + offset) = 'a' + lrand48() % 26;
    auto batch_id = xport->allocateBatchID(1);
    int ret = 0;
    auto segment_id = engine->openSegment("127.0.0.2:12345");
    TransferRequest entry;
    auto segment_desc = xport->meta()->getSegmentDescByID(segment_id);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t *)(addr);
    entry.target_id = segment_id;
    entry.target_offset = remote_base;
    ret = xport->submitTransfer(batch_id, {entry});
    LOG_ASSERT(!ret);
    bool completed = false;
    TransferStatus status;
    while (!completed) {
        int ret = xport->getTransferStatus(batch_id, 0, status);
        ASSERT_EQ(ret, 0);
        LOG_ASSERT(status.s != TransferStatusEnum::FAILED);
        if (status.s == TransferStatusEnum::COMPLETED) completed = true;
    }
    ret = xport->freeBatchID(batch_id);
    ASSERT_EQ(ret, 0);
}

TEST_F(TCPTransportTest, WriteAndReadtest) {
    const size_t kDataLength = 4096000;
    void *addr = nullptr;
    const size_t ram_buffer_size = 1ull << 30;
    auto metadata_client = std::make_shared<TransferMetadata>("127.0.0.1:2379");
    LOG_ASSERT(metadata_client);

    auto engine = std::make_unique<TransferEngine>(metadata_client);

    auto hostname_port = parseHostNameWithPort("127.0.0.2:12345");
    engine->init("127.0.0.2:12345", hostname_port.first.c_str(),
                 hostname_port.second);
    Transport *xport = nullptr;
    xport = engine->installOrGetTransport("tcp", nullptr);
    LOG_ASSERT(xport != nullptr);

    addr = allocateMemoryPool(ram_buffer_size, 0, false);
    int rc = engine->registerLocalMemory(addr, ram_buffer_size, "cpu:0");
    LOG_ASSERT(!rc);
    for (size_t offset = 0; offset < kDataLength; ++offset)
        *((char *)(addr) + offset) = 'a' + lrand48() % 26;

    auto segment_id = engine->openSegment("127.0.0.2:12345");
    auto segment_desc = xport->meta()->getSegmentDescByID(segment_id);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;
    {
        auto batch_id = xport->allocateBatchID(1);
        int ret = 0;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        ret = xport->submitTransfer(batch_id, {entry});
        LOG_ASSERT(!ret);
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            int ret = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(ret, 0);
            LOG_ASSERT(status.s != TransferStatusEnum::FAILED);
            if (status.s == TransferStatusEnum::COMPLETED) completed = true;
        }
        ret = xport->freeBatchID(batch_id);
        ASSERT_EQ(ret, 0);
    }

    {
        auto batch_id = xport->allocateBatchID(1);
        int ret = 0;

        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr) + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        ret = xport->submitTransfer(batch_id, {entry});
        LOG_ASSERT(!ret);
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            int ret = xport->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(!ret);
            if (status.s == TransferStatusEnum::COMPLETED) completed = true;
            LOG_ASSERT(status.s != TransferStatusEnum::FAILED);
        }
        ret = xport->freeBatchID(batch_id);
        LOG_ASSERT(!ret);
    }
    LOG_ASSERT(0 == memcmp((uint8_t *)(addr), (uint8_t *)(addr) + kDataLength,
                           kDataLength));
}

TEST_F(TCPTransportTest, WriteAndRead2test) {
    const size_t kDataLength = 4096000;
    void *addr = nullptr;
    const size_t ram_buffer_size = 1ull << 30;
    auto metadata_client = std::make_shared<TransferMetadata>("127.0.0.1:2379");
    LOG_ASSERT(metadata_client);

    auto engine = std::make_unique<TransferEngine>(metadata_client);

    auto hostname_port = parseHostNameWithPort("127.0.0.2:12345");
    engine->init("127.0.0.2:12345", hostname_port.first.c_str(),
                 hostname_port.second);
    Transport *xport = nullptr;
    xport = engine->installOrGetTransport("tcp", nullptr);
    LOG_ASSERT(xport != nullptr);

    addr = allocateMemoryPool(ram_buffer_size, 0, false);
    int rc = engine->registerLocalMemory(addr, ram_buffer_size, "cpu:0");
    LOG_ASSERT(!rc);
    for (size_t offset = 0; offset < kDataLength; ++offset)
        *((char *)(addr) + offset) = 'a' + lrand48() % 26;

    auto segment_id = engine->openSegment("127.0.0.2:12345");
    auto segment_desc = xport->meta()->getSegmentDescByID(segment_id);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    {
        auto batch_id = xport->allocateBatchID(1);
        int ret = 0;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        ret = xport->submitTransfer(batch_id, {entry});
        LOG_ASSERT(!ret);
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            int ret = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(ret, 0);
            LOG_ASSERT(status.s != TransferStatusEnum::FAILED);
            if (status.s == TransferStatusEnum::COMPLETED) completed = true;
        }
        ret = xport->freeBatchID(batch_id);
        ASSERT_EQ(ret, 0);
    }

    {
        auto batch_id = xport->allocateBatchID(1);
        int ret = 0;
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr) + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        ret = xport->submitTransfer(batch_id, {entry});
        LOG_ASSERT(!ret);
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            int ret = xport->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(!ret);
            if (status.s == TransferStatusEnum::COMPLETED) completed = true;
            LOG_ASSERT(status.s != TransferStatusEnum::FAILED);
        }
        ret = xport->freeBatchID(batch_id);
        LOG_ASSERT(!ret);
    }
    LOG_ASSERT(0 == memcmp((uint8_t *)(addr), (uint8_t *)(addr) + kDataLength,
                           kDataLength));

    for (size_t offset = 0; offset < kDataLength; ++offset)
        *((char *)(addr) + offset) = 'a' + lrand48() % 26;
    {
        auto batch_id = xport->allocateBatchID(1);
        int ret = 0;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        ret = xport->submitTransfer(batch_id, {entry});
        LOG_ASSERT(!ret);
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            int ret = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(ret, 0);
            LOG_ASSERT(status.s != TransferStatusEnum::FAILED);
            if (status.s == TransferStatusEnum::COMPLETED) completed = true;
        }
        ret = xport->freeBatchID(batch_id);
        ASSERT_EQ(ret, 0);
    }

    {
        auto batch_id = xport->allocateBatchID(1);
        int ret = 0;
        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr) + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        ret = xport->submitTransfer(batch_id, {entry});
        LOG_ASSERT(!ret);
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            int ret = xport->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(!ret);
            if (status.s == TransferStatusEnum::COMPLETED) completed = true;
            LOG_ASSERT(status.s != TransferStatusEnum::FAILED);
        }
        ret = xport->freeBatchID(batch_id);
        LOG_ASSERT(!ret);
    }
    LOG_ASSERT(0 == memcmp((uint8_t *)(addr), (uint8_t *)(addr) + kDataLength,
                           kDataLength));
}

}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}