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
#include "transport/cxi_transport/cxi_transport.h"
#include "transport/transport.h"
#include "cuda_alike.h"

#if defined(USE_CUDA) || defined(USE_HIP)
#define USE_GPU
#endif

using namespace mooncake;

namespace mooncake {

static void *allocateMemoryPool(size_t size, int socket_id) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) { numa_free(addr, size); }

#ifdef USE_GPU
static void *allocateMemoryPoolDevice(size_t size, int device) {
    void *devPtr = nullptr;
    cudaError_t err = cudaMalloc(&devPtr, size);
    cudaDeviceSynchronize();
    if (err != cudaSuccess) {
        LOG(ERROR) << "failed to alloc " << size << " bytes on cuda device "
                   << device;
        return nullptr;
    }
    return devPtr;
}

static void freeMemoryPoolDevice(void *addr) {
    cudaError_t err = cudaFree(addr);
    if (err != cudaSuccess) {
        LOG(ERROR) << "failed to free cuda memory @ " << addr;
    }
}
#endif

// ---------------------------------------------------------------------------
// CXI Transport Test Fixture
//
// This test uses the P2PHANDSHAKE metadata backend and performs loopback
// transfers (local_server_name == segment_id), similar to the TCP transport
// tests. It requires CXI hardware to be present (fi_info -p cxi must succeed).
//
// Environment variables:
//   MC_METADATA_SERVER  - metadata backend (default: P2PHANDSHAKE)
//   MC_LOCAL_SERVER_NAME - local server name (default: 127.0.0.1:12345)
// ---------------------------------------------------------------------------
class CXITransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("CXITransportTest");
        FLAGS_logtostderr = 1;

        const char *env = std::getenv("MC_METADATA_SERVER");
        metadata_server_ = env ? env : "P2PHANDSHAKE";
        LOG(INFO) << "metadata_server: " << metadata_server_;

        env = std::getenv("MC_LOCAL_SERVER_NAME");
        local_server_name_ = env ? env : "127.0.0.1:12345";
        LOG(INFO) << "local_server_name: " << local_server_name_;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    // Helper: create engine, install CXI transport, register memory
    struct EngineSetup {
        std::unique_ptr<TransferEngine> engine;
        Transport *xport;
        void *addr;
        size_t buffer_size;
        SegmentID segment_id;
    };

#ifdef USE_GPU

    EngineSetup createEngineDevice(size_t buffer_size = 1ull << 30) {
        EngineSetup s;
        s.buffer_size = buffer_size;

        s.engine = std::make_unique<TransferEngine>(false);
        // Manually discover topology to populate CXI device list
        // (same pattern as the Python binding in transfer_engine_py.cpp)
        s.engine->getLocalTopology()->discover({});
        auto hp = parseHostNameWithPort(local_server_name_);
        int rc = s.engine->init(metadata_server_, local_server_name_,
                                hp.first.c_str(), hp.second);
        EXPECT_EQ(rc, 0) << "engine->init failed";

        s.xport = s.engine->installTransport("cxi", nullptr);
        EXPECT_NE(s.xport, nullptr) << "installTransport(\"cxi\") failed";

        s.addr = allocateMemoryPoolDevice(buffer_size,
                                          0);  // allocate mempool on cuda:0
        EXPECT_NE(s.addr, nullptr) << "allocateMemoryPool failed";

        rc = s.engine->registerLocalMemory(s.addr, buffer_size,
                                           GPU_PREFIX + "0");
        EXPECT_EQ(rc, 0) << "registerLocalMemory failed";

        // Use actual RPC address (P2PHANDSHAKE picks a random port)
        auto actual_addr = s.engine->getLocalIpAndPort();
        s.segment_id = s.engine->openSegment(actual_addr);
        return s;
    }

    void destroyEngineDevice(EngineSetup &s) {
        if (s.engine && s.addr) {
            s.engine->unregisterLocalMemory(s.addr);
        }
        if (s.addr) {
            freeMemoryPoolDevice(s.addr);
            s.addr = nullptr;
        }
    }

#endif

    EngineSetup createEngine(size_t buffer_size = 1ull << 30) {
        EngineSetup s;
        s.buffer_size = buffer_size;

        s.engine = std::make_unique<TransferEngine>(false);
        // Manually discover topology to populate CXI device list
        // (same pattern as the Python binding in transfer_engine_py.cpp)
        s.engine->getLocalTopology()->discover({});
        auto hp = parseHostNameWithPort(local_server_name_);
        int rc = s.engine->init(metadata_server_, local_server_name_,
                                hp.first.c_str(), hp.second);
        EXPECT_EQ(rc, 0) << "engine->init failed";

        s.xport = s.engine->installTransport("cxi", nullptr);
        EXPECT_NE(s.xport, nullptr) << "installTransport(\"cxi\") failed";

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

// Test 1: Verify CXI transport can be installed
TEST_F(CXITransportTest, InstallTransport) {
    auto engine = std::make_unique<TransferEngine>(false);
    engine->getLocalTopology()->discover({});
    auto hp = parseHostNameWithPort(local_server_name_);
    int rc = engine->init(metadata_server_, local_server_name_,
                          hp.first.c_str(), hp.second);
    ASSERT_EQ(rc, 0);

    Transport *xport = engine->installTransport("cxi", nullptr);
    ASSERT_NE(xport, nullptr)
        << "CXI transport should be installable on CXI hardware";
}

// Test 2: Basic loopback write
TEST_F(CXITransportTest, LoopbackWrite) {
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

#ifdef USE_GPU
TEST_F(CXITransportTest, LoopbackWriteDevice) {
    cudaSetDevice(0);
    auto setup = createEngineDevice();

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 4096;

    // Fill source buffer with known data
    cudaMemset(setup.addr, 0xAB, kDataLength);
    cudaDeviceSynchronize();

    bool ok = submitAndWait(setup.engine.get(), setup.segment_id, setup.addr,
                            remote_base, kDataLength, TransferRequest::WRITE);
    EXPECT_TRUE(ok) << "Loopback write should succeed";

    destroyEngineDevice(setup);
}
#endif

// Test 3: Write then read, verify data integrity
TEST_F(CXITransportTest, WriteAndRead) {
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

#ifdef USE_GPU
TEST_F(CXITransportTest, WriteAndReadDevice) {
    cudaSetDevice(0);
    auto setup = createEngineDevice();

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kDataLength = 4096000;
    uint8_t *buf = (uint8_t *)setup.addr;

    uint8_t random_data = 'a' + lrand48() % 26;
    cudaMemset(buf, random_data, kDataLength);
    cudaDeviceSynchronize();

    // Write local -> remote (loopback)
    bool ok = submitAndWait(setup.engine.get(), setup.segment_id, buf,
                            remote_base, kDataLength, TransferRequest::WRITE);
    ASSERT_TRUE(ok) << "Write should succeed";

    // Read remote -> local (into second half of buffer)
    ok = submitAndWait(setup.engine.get(), setup.segment_id, buf + kDataLength,
                       remote_base, kDataLength, TransferRequest::READ);
    ASSERT_TRUE(ok) << "Read should succeed";
    // copy data back to host
    uint8_t *dataHost = (uint8_t *)allocateMemoryPool(2 * kDataLength, 0);
    cudaMemcpy(dataHost, buf, 2 * kDataLength, cudaMemcpyDeviceToHost);
    cudaDeviceSynchronize();
    // Verify data integrity
    EXPECT_EQ(0, memcmp(dataHost, dataHost + kDataLength, kDataLength))
        << "Read-back data should match written data";

    destroyEngineDevice(setup);
}
#endif

// Test 4: Multiple sequential writes in a batch
TEST_F(CXITransportTest, MultiWrite) {
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
TEST_F(CXITransportTest, StressMultipleBatches) {
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

// Test 6: warmupSegment on loopback peer
//
// Exercises CxiTransport::warmupSegment() which is the C++ entry point
// behind the warmup_efa_segment() Python binding / warmupSegment() C API.
// Loopback is enough to cover the handshake + fi_av_insert path AND the
// idempotent short-circuit on the second call.
TEST_F(CXITransportTest, WarmupSegmentLoopback) {
    auto setup = createEngine();

    auto *cxi = dynamic_cast<CxiTransport *>(setup.xport);
    ASSERT_NE(cxi, nullptr) << "installTransport did not return a CxiTransport";

    // First call: should connect every (local NIC x peer NIC) pair.
    int rc = cxi->warmupSegment(setup.engine->getLocalIpAndPort());
    EXPECT_EQ(rc, 0) << "warmupSegment should succeed on loopback";

    // Second call: should short-circuit (all endpoints already connected).
    rc = cxi->warmupSegment(setup.engine->getLocalIpAndPort());
    EXPECT_EQ(rc, 0) << "warmupSegment should be idempotent";

    // Empty / self-name: short-circuit path returning 0 without touching AV.
    rc = cxi->warmupSegment("");
    EXPECT_EQ(rc, 0) << "warmupSegment(\"\") should be a no-op";

    destroyEngine(setup);
}

// Test 7: warmupSegment on a non-existent segment name should fail cleanly
// (no crash, no hang) rather than blocking for the poll timeout.
TEST_F(CXITransportTest, WarmupSegmentNotFound) {
    auto setup = createEngine();

    auto *cxi = dynamic_cast<CxiTransport *>(setup.xport);
    ASSERT_NE(cxi, nullptr);

    int rc = cxi->warmupSegment("127.0.0.1:1");  // not openSegment'd
    EXPECT_NE(rc, 0) << "warmupSegment should fail for unknown segment";

    destroyEngine(setup);
}

// Test 8: registerLocalMemoryBatch / unregisterLocalMemoryBatch round-trip.
// Covers the batched MR path which the single-buffer tests above never hit.
TEST_F(CXITransportTest, RegisterMemoryBatch) {
    auto engine = std::make_unique<TransferEngine>(false);
    engine->getLocalTopology()->discover({});
    auto hp = parseHostNameWithPort(local_server_name_);
    int rc = engine->init(metadata_server_, local_server_name_,
                          hp.first.c_str(), hp.second);
    ASSERT_EQ(rc, 0);

    Transport *xport = engine->installTransport("cxi", nullptr);
    ASSERT_NE(xport, nullptr);

    const size_t kBufSize = 4ull << 20;  // 4 MB each
    const int kNumBufs = 4;
    std::vector<void *> addrs;
    std::vector<BufferEntry> entries;
    for (int i = 0; i < kNumBufs; ++i) {
        void *a = allocateMemoryPool(kBufSize, 0);
        ASSERT_NE(a, nullptr);
        addrs.push_back(a);
        entries.push_back({a, kBufSize});
    }

    rc = engine->registerLocalMemoryBatch(entries, "cpu:0");
    EXPECT_EQ(rc, 0) << "registerLocalMemoryBatch should succeed";

    rc = engine->unregisterLocalMemoryBatch(addrs);
    EXPECT_EQ(rc, 0) << "unregisterLocalMemoryBatch should succeed";

    for (void *a : addrs) freeMemoryPool(a, kBufSize);
}

// Test 9: Larger transfer (64 MB total split into 1 MB slices) to exercise
// the WR / CQ pacing logic in CxiContext::submitSlicesOnPeer beyond what the
// 16 x 64 KB MultiWrite test reaches.
TEST_F(CXITransportTest, LargeTransfer) {
    const size_t kBufSize = 128ull << 20;  // 128 MB
    auto setup = createEngine(kBufSize);

    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;

    const size_t kSliceLen = 1ull << 20;  // 1 MB per slice
    const int kNumSlices = 64;            // 64 MB total
    ASSERT_LE(static_cast<size_t>(kNumSlices) * kSliceLen, kBufSize / 2);

    // Fill first half with known data
    uint8_t *buf = (uint8_t *)setup.addr;
    for (size_t i = 0; i < static_cast<size_t>(kNumSlices) * kSliceLen; ++i)
        buf[i] = (uint8_t)(i & 0xFF);

    auto batch_id = setup.engine->allocateBatchID(kNumSlices);
    std::vector<TransferRequest> requests;
    requests.reserve(kNumSlices);
    for (int i = 0; i < kNumSlices; ++i) {
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kSliceLen;
        entry.source = buf + i * kSliceLen;
        entry.target_id = setup.segment_id;
        entry.target_offset = remote_base + (kBufSize / 2) + i * kSliceLen;
        requests.push_back(entry);
    }

    Status s = setup.engine->submitTransfer(batch_id, requests);
    ASSERT_TRUE(s.ok()) << "submitTransfer failed: " << s.ToString();

    for (int task_id = 0; task_id < kNumSlices; ++task_id) {
        TransferStatus status;
        const int kMaxPollIterations = 2000000;
        int i = 0;
        for (; i < kMaxPollIterations; ++i) {
            s = setup.engine->getTransferStatus(batch_id, task_id, status);
            ASSERT_TRUE(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED) break;
            ASSERT_NE(status.s, TransferStatusEnum::FAILED);
        }
        ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED)
            << "task " << task_id << " did not complete";
    }

    s = setup.engine->freeBatchID(batch_id);
    ASSERT_TRUE(s.ok());

    // Verify byte-level integrity of the last slice (spot check).
    EXPECT_EQ(0, memcmp(buf + (kNumSlices - 1) * kSliceLen,
                        buf + (kBufSize / 2) + (kNumSlices - 1) * kSliceLen,
                        kSliceLen));

    destroyEngine(setup);
}

// Test 10: Repeated open/close of the same remote segment must not leak AV
// slots or break loopback transfers — this is the setPeerNicPath-detach path
// that target restarts depend on under the SRD shared-endpoint model.
TEST_F(CXITransportTest, RepeatedOpenSegment) {
    auto setup = createEngine();

    auto actual_addr = setup.engine->getLocalIpAndPort();

    // First write via setup.segment_id (from createEngine()).
    auto segment_desc =
        setup.engine->getMetadata()->getSegmentDescByID(setup.segment_id);
    ASSERT_NE(segment_desc, nullptr);
    uint64_t remote_base = (uint64_t)segment_desc->buffers[0].addr;
    memset(setup.addr, 0xCD, 4096);
    EXPECT_TRUE(submitAndWait(setup.engine.get(), setup.segment_id, setup.addr,
                              remote_base, 4096, TransferRequest::WRITE));

    // Re-open same segment several times; each should return a working handle
    // and subsequent writes should still succeed.
    for (int i = 0; i < 5; ++i) {
        SegmentID sid = setup.engine->openSegment(actual_addr);
        ASSERT_NE(sid, (SegmentID)-1);
        auto desc = setup.engine->getMetadata()->getSegmentDescByID(sid);
        ASSERT_NE(desc, nullptr);
        uint64_t base = (uint64_t)desc->buffers[0].addr;
        ASSERT_TRUE(submitAndWait(setup.engine.get(), sid, setup.addr, base,
                                  4096, TransferRequest::WRITE))
            << "write #" << i << " after re-open failed";
    }

    destroyEngine(setup);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
