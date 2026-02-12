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
#include <memory>

#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

static void *allocateMemoryPool(size_t size, int socket_id) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) { numa_free(addr, size); }

// ---------------------------------------------------------------------------
// EFA Transport Test Fixture
//
// This test uses the P2PHANDSHAKE metadata backend and performs loopback
// transfers (local_server_name == segment_id), similar to the TCP transport
// tests. It requires EFA hardware to be present (fi_info -p efa must succeed).
//
// Environment variables:
//   MC_METADATA_SERVER  - metadata backend (default: P2PHANDSHAKE)
//   MC_LOCAL_SERVER_NAME - local server name (default: 127.0.0.1:12345)
// ---------------------------------------------------------------------------
class EFATransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("EFATransportTest");
        FLAGS_logtostderr = 1;

        const char *env = std::getenv("MC_METADATA_SERVER");
        metadata_server_ = env ? env : "P2PHANDSHAKE";
        LOG(INFO) << "metadata_server: " << metadata_server_;

        env = std::getenv("MC_LOCAL_SERVER_NAME");
        local_server_name_ = env ? env : "127.0.0.1:12345";
        LOG(INFO) << "local_server_name: " << local_server_name_;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    // Helper: create engine, install EFA transport, register memory
    struct EngineSetup {
        std::unique_ptr<TransferEngine> engine;
        Transport *xport;
        void *addr;
        size_t buffer_size;
        SegmentID segment_id;
    };

    EngineSetup createEngine(size_t buffer_size = 1ull << 30) {
        EngineSetup s;
        s.buffer_size = buffer_size;

        s.engine = std::make_unique<TransferEngine>(false);
        // Manually discover topology to populate EFA device list
        // (same pattern as the Python binding in transfer_engine_py.cpp)
        s.engine->getLocalTopology()->discover({});
        auto hp = parseHostNameWithPort(local_server_name_);
        int rc = s.engine->init(metadata_server_, local_server_name_,
                                hp.first.c_str(), hp.second);
        EXPECT_EQ(rc, 0) << "engine->init failed";

        s.xport = s.engine->installTransport("efa", nullptr);
        EXPECT_NE(s.xport, nullptr) << "installTransport(\"efa\") failed";

        s.addr = allocateMemoryPool(buffer_size, 0);
        EXPECT_NE(s.addr, nullptr) << "allocateMemoryPool failed";

        rc = s.engine->registerLocalMemory(s.addr, buffer_size, "cpu:0");
        EXPECT_EQ(rc, 0) << "registerLocalMemory failed";

        // Use actual RPC address (P2PHANDSHAKE picks a random port)
        auto actual_addr = s.engine->getLocalIpAndPort();
        s.segment_id = s.engine->openSegment(actual_addr);
        return s;
    }

    void destroyEngine(EngineSetup &s) {
        if (s.engine && s.addr) {
            s.engine->unregisterLocalMemory(s.addr);
        }
        if (s.addr) {
            freeMemoryPool(s.addr, s.buffer_size);
            s.addr = nullptr;
        }
    }

    // Helper: submit a single transfer and poll until completion
    bool submitAndWait(TransferEngine *engine, SegmentID segment_id,
                       void *source, uint64_t target_offset, size_t length,
                       TransferRequest::OpCode opcode) {
        auto batch_id = engine->allocateBatchID(1);

        TransferRequest entry;
        entry.opcode = opcode;
        entry.length = length;
        entry.source = (uint8_t *)source;
        entry.target_id = segment_id;
        entry.target_offset = target_offset;

        Status s = engine->submitTransfer(batch_id, {entry});
        if (!s.ok()) {
            LOG(ERROR) << "submitTransfer failed: " << s.ToString();
            engine->freeBatchID(batch_id);
            return false;
        }

        // Poll for completion with timeout
        const int kMaxPollIterations = 1000000;
        TransferStatus status;
        for (int i = 0; i < kMaxPollIterations; ++i) {
            s = engine->getTransferStatus(batch_id, 0, status);
            if (!s.ok()) {
                LOG(ERROR) << "getTransferStatus failed: " << s.ToString();
                engine->freeBatchID(batch_id);
                return false;
            }
            if (status.s == TransferStatusEnum::COMPLETED) {
                engine->freeBatchID(batch_id);
                return true;
            }
            if (status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "Transfer FAILED";
                engine->freeBatchID(batch_id);
                return false;
            }
        }
        LOG(ERROR) << "Transfer timed out";
        engine->freeBatchID(batch_id);
        return false;
    }

    std::string metadata_server_;
    std::string local_server_name_;
};

// Test 1: Verify EFA transport can be installed
TEST_F(EFATransportTest, InstallTransport) {
    auto engine = std::make_unique<TransferEngine>(false);
    engine->getLocalTopology()->discover({});
    auto hp = parseHostNameWithPort(local_server_name_);
    int rc = engine->init(metadata_server_, local_server_name_,
                          hp.first.c_str(), hp.second);
    ASSERT_EQ(rc, 0);

    Transport *xport = engine->installTransport("efa", nullptr);
    ASSERT_NE(xport, nullptr)
        << "EFA transport should be installable on EFA hardware";
}

// Test 2: Basic loopback write
TEST_F(EFATransportTest, LoopbackWrite) {
    auto setup = createEngine();

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 4096;

    // Fill source buffer with known data
    memset(setup.addr, 0xAB, kDataLength);

    bool ok = submitAndWait(setup.engine.get(), setup.segment_id, setup.addr,
                            remote_base, kDataLength, TransferRequest::WRITE);
    EXPECT_TRUE(ok) << "Loopback write should succeed";

    destroyEngine(setup);
}

// Test 3: Write then read, verify data integrity
TEST_F(EFATransportTest, WriteAndRead) {
    auto setup = createEngine();

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 4096000;
    uint8_t *buf = (uint8_t *)setup.addr;

    // Fill first half with random data
    for (size_t i = 0; i < kDataLength; ++i) buf[i] = 'a' + lrand48() % 26;

    // Write local -> remote (loopback)
    bool ok = submitAndWait(setup.engine.get(), setup.segment_id, buf,
                            remote_base, kDataLength, TransferRequest::WRITE);
    ASSERT_TRUE(ok) << "Write should succeed";

    // Read remote -> local (into second half of buffer)
    ok = submitAndWait(setup.engine.get(), setup.segment_id, buf + kDataLength,
                       remote_base, kDataLength, TransferRequest::READ);
    ASSERT_TRUE(ok) << "Read should succeed";

    // Verify data integrity
    EXPECT_EQ(0, memcmp(buf, buf + kDataLength, kDataLength))
        << "Read-back data should match written data";

    destroyEngine(setup);
}

// Test 4: Multiple sequential writes in a batch
TEST_F(EFATransportTest, MultiWrite) {
    auto setup = createEngine();

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 65536;
    const int kBatchSize = 16;

    auto batch_id = setup.engine->allocateBatchID(kBatchSize);

    std::vector<TransferRequest> requests;
    for (int i = 0; i < kBatchSize; ++i) {
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)setup.addr + i * kDataLength;
        entry.target_id = setup.segment_id;
        entry.target_offset = remote_base + i * kDataLength;
        requests.push_back(entry);
    }

    Status s = setup.engine->submitTransfer(batch_id, requests);
    ASSERT_TRUE(s.ok()) << "submitTransfer failed: " << s.ToString();

    // Poll all tasks until completion
    for (int task_id = 0; task_id < kBatchSize; ++task_id) {
        TransferStatus status;
        const int kMaxPollIterations = 1000000;
        for (int i = 0; i < kMaxPollIterations; ++i) {
            s = setup.engine->getTransferStatus(batch_id, task_id, status);
            ASSERT_TRUE(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) break;
            ASSERT_NE(status.s, TransferStatusEnum::FAILED)
                << "Task " << task_id << " failed";
        }
        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED)
            << "Task " << task_id << " did not complete";
    }

    s = setup.engine->freeBatchID(batch_id);
    ASSERT_TRUE(s.ok());

    destroyEngine(setup);
}

// Test 5: Stress test - multiple batches to verify no CQ overflow
TEST_F(EFATransportTest, StressMultipleBatches) {
    auto setup = createEngine();

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 65536;
    const int kBatchSize = 8;
    const int kNumBatches = 20;

    for (int batch = 0; batch < kNumBatches; ++batch) {
        auto batch_id = setup.engine->allocateBatchID(kBatchSize);

        std::vector<TransferRequest> requests;
        for (int i = 0; i < kBatchSize; ++i) {
            TransferRequest entry;
            entry.opcode = TransferRequest::WRITE;
            entry.length = kDataLength;
            entry.source =
                (uint8_t *)setup.addr + (i + batch * kBatchSize) * kDataLength;
            entry.target_id = setup.segment_id;
            entry.target_offset =
                remote_base + (i + batch * kBatchSize) * kDataLength;
            requests.push_back(entry);
        }

        Status s = setup.engine->submitTransfer(batch_id, requests);
        ASSERT_TRUE(s.ok())
            << "Batch " << batch << " submitTransfer failed: " << s.ToString();

        // Wait for all tasks in batch
        for (int task_id = 0; task_id < kBatchSize; ++task_id) {
            TransferStatus status;
            const int kMaxPollIterations = 1000000;
            for (int i = 0; i < kMaxPollIterations; ++i) {
                s = setup.engine->getTransferStatus(batch_id, task_id, status);
                ASSERT_TRUE(s.ok());
                if (status.s == TransferStatusEnum::COMPLETED) break;
                ASSERT_NE(status.s, TransferStatusEnum::FAILED)
                    << "Batch " << batch << " task " << task_id << " failed";
            }
            ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED)
                << "Batch " << batch << " task " << task_id
                << " did not complete";
        }

        s = setup.engine->freeBatchID(batch_id);
        ASSERT_TRUE(s.ok());
    }

    destroyEngine(setup);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
