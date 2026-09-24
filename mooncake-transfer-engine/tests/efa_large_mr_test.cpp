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

// Regression test for issue #3501: a transfer that straddles a BufferDesc chunk
// boundary must be split at that boundary instead of failing.
//
// EfaTransport::registerLocalMemoryInternal() already splits a buffer larger
// than min(max_mr_size, per-NIC PTE budget) into several chunks and publishes
// one BufferDesc per chunk (the EFA counterpart of #2644). Submission, however,
// posted exactly one slice per request and EfaTransport::selectDevice() only
// accepts a buffer that contains the WHOLE request, so any read/write crossing
// a seam died with ERR_ADDRESS_NOT_REGISTERED (source side) or "Cannot select
// device for dest_addr" (target side) -- which is why the Store had to keep
// splitting EFA segments in GetTransportRegistrationLimit().
//
// This forces the condition at a small, hardware-independent size via
// MC_MAX_MR_SIZE, then runs loopback transfers that cross the first seam.
//
// Needs EFA hardware, and MC_MAX_MR_SIZE must be set before the process starts
// (globalConfig() reads it once):
//
//   MC_MAX_MR_SIZE=67108864 ./efa_large_mr_test
//
// Not registered with add_test() for that reason, same as rdma_large_mr_test.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "config.h"
#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

// Small max_mr_size so the multi-chunk path is exercised without a multi-GB
// allocation. 256 MiB / 64 MiB = 4 chunks.
static constexpr size_t kMaxMrSize = 64ull << 20;
static constexpr size_t kBufferSize = 256ull << 20;
static constexpr size_t kExpectedChunks = kBufferSize / kMaxMrSize;
static constexpr size_t kDataLength = 1ull << 20;

class EFALargeMrTest : public ::testing::Test {
   protected:
    std::unique_ptr<TransferEngine> engine;
    void *addr = nullptr;
    SegmentID segment_id = 0;

    void SetUp() override {
        const char *env = std::getenv("MC_METADATA_SERVER");
        std::string metadata_server = env ? env : "P2PHANDSHAKE";
        env = std::getenv("MC_LOCAL_SERVER_NAME");
        std::string local_server_name = env ? env : "127.0.0.1:12345";

        ASSERT_EQ(globalConfig().max_mr_size, kMaxMrSize)
            << "Launch this test process with MC_MAX_MR_SIZE=" << kMaxMrSize;

        engine = std::make_unique<TransferEngine>(false);
        // Manually discover the topology to populate the EFA device list, same
        // pattern as efa_transport_test.
        engine->getLocalTopology()->discover({});
        auto hp = parseHostNameWithPort(local_server_name);
        ASSERT_EQ(engine->init(metadata_server, local_server_name,
                               hp.first.c_str(), hp.second),
                  0);
        ASSERT_NE(engine->installTransport("efa", nullptr), nullptr)
            << "installTransport(\"efa\") failed -- EFA hardware required";

        addr = numa_alloc_onnode(kBufferSize, 0);
        ASSERT_NE(addr, nullptr);
        ASSERT_EQ(engine->registerLocalMemory(addr, kBufferSize, "cpu:0"), 0);

        segment_id = engine->openSegment(engine->getLocalIpAndPort());
        auto desc = engine->getMetadata()->getSegmentDescByID(segment_id);
        ASSERT_NE(desc, nullptr);
        // The premise of every case below: the registration really did split.
        size_t chunks = 0;
        for (const auto &buffer : desc->buffers) {
            if (buffer.addr >= (uint64_t)addr &&
                buffer.addr < (uint64_t)addr + kBufferSize) {
                EXPECT_LE(buffer.length, kMaxMrSize);
                ++chunks;
            }
        }
        ASSERT_EQ(chunks, kExpectedChunks)
            << "buffer was not auto-split into " << kExpectedChunks
            << " chunks; the straddle cases below would be vacuous";
    }

    void TearDown() override {
        if (engine && addr) engine->unregisterLocalMemory(addr);
        if (addr) numa_free(addr, kBufferSize);
    }

    // Submit one request and poll to a terminal state.
    TransferStatusEnum submitAndWait(void *source, uint64_t target_offset,
                                     size_t length,
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
            return TransferStatusEnum::FAILED;
        }

        const int kMaxPollIterations = 10000000;
        TransferStatus status;
        status.s = TransferStatusEnum::WAITING;
        for (int i = 0; i < kMaxPollIterations; ++i) {
            if (!engine->getTransferStatus(batch_id, 0, status).ok()) {
                engine->freeBatchID(batch_id);
                return TransferStatusEnum::FAILED;
            }
            if (status.s != TransferStatusEnum::WAITING) break;
        }
        engine->freeBatchID(batch_id);
        return status.s;
    }

    void fillSource(size_t offset) {
        for (size_t i = 0; i < kDataLength; ++i)
            *((char *)addr + offset + i) = (char)('a' + (lrand48() % 26));
    }
};

// Baseline: the target lands past the first chunk but inside a single later
// chunk. This already worked (each chunk is its own MR with its own key) and
// guards against a regression in the non-straddling multi-chunk path.
TEST_F(EFALargeMrTest, WritePastFirstChunk) {
    const size_t kTargetOffset = kBufferSize - kDataLength;  // chunk 3
    ASSERT_GT(kTargetOffset, kMaxMrSize);

    fillSource(0);
    memset((char *)addr + kTargetOffset, 0, kDataLength);

    ASSERT_EQ(submitAndWait(addr, (uint64_t)addr + kTargetOffset, kDataLength,
                            TransferRequest::WRITE),
              TransferStatusEnum::COMPLETED);
    ASSERT_EQ(0, memcmp(addr, (char *)addr + kTargetOffset, kDataLength));
}

// The TARGET range straddles the seam between chunk 0 and chunk 1. Pre-fix the
// peer-side selectDevice() in EfaContext::submitPostSend() finds no single
// BufferDesc covering the whole range and the transfer is marked FAILED.
TEST_F(EFALargeMrTest, WriteStraddlesChunkBoundary) {
    const size_t kTargetOffset = kMaxMrSize - kDataLength / 2 + 1;
    ASSERT_LT(kTargetOffset, kMaxMrSize);                // starts in chunk 0
    ASSERT_GT(kTargetOffset + kDataLength, kMaxMrSize);  // ends in chunk 1

    fillSource(0);
    memset((char *)addr + kTargetOffset, 0, kDataLength);

    ASSERT_EQ(submitAndWait(addr, (uint64_t)addr + kTargetOffset, kDataLength,
                            TransferRequest::WRITE),
              TransferStatusEnum::COMPLETED)
        << "EFA WRITE straddling a chunk boundary failed -- the request was "
           "not split at the seam (issue #3501)";
    ASSERT_EQ(0, memcmp(addr, (char *)addr + kTargetOffset, kDataLength));
}

// The SOURCE range straddles the seam. Pre-fix EfaTransport::submitTransferTask
// rejects the request outright with ERR_ADDRESS_NOT_REGISTERED.
TEST_F(EFALargeMrTest, WriteWithSourceStraddlingChunkBoundary) {
    const size_t kSourceOffset = kMaxMrSize - kDataLength / 2 + 1;
    const size_t kTargetOffset = 2 * kMaxMrSize;

    fillSource(kSourceOffset);
    memset((char *)addr + kTargetOffset, 0, kDataLength);

    ASSERT_EQ(submitAndWait((char *)addr + kSourceOffset,
                            (uint64_t)addr + kTargetOffset, kDataLength,
                            TransferRequest::WRITE),
              TransferStatusEnum::COMPLETED)
        << "EFA WRITE with a source straddling a chunk boundary failed "
           "(issue #3501)";
    ASSERT_EQ(0, memcmp((char *)addr + kSourceOffset,
                        (char *)addr + kTargetOffset, kDataLength));
}

// Same seam, opposite direction: READ pulls from a remote range that straddles
// the boundary into a local range that straddles the NEXT one, so both sides
// have to be cut.
TEST_F(EFALargeMrTest, ReadStraddlesChunkBoundaryOnBothSides) {
    const size_t kRemoteOffset = kMaxMrSize - kDataLength / 2 + 1;
    const size_t kLocalOffset = 2 * kMaxMrSize - kDataLength / 2 + 1;

    fillSource(kRemoteOffset);
    memset((char *)addr + kLocalOffset, 0, kDataLength);

    ASSERT_EQ(submitAndWait((char *)addr + kLocalOffset,
                            (uint64_t)addr + kRemoteOffset, kDataLength,
                            TransferRequest::READ),
              TransferStatusEnum::COMPLETED)
        << "EFA READ straddling a chunk boundary failed (issue #3501)";
    ASSERT_EQ(0, memcmp((char *)addr + kLocalOffset,
                        (char *)addr + kRemoteOffset, kDataLength));
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    // Once for the whole binary -- calling InitGoogleLogging() per-test in
    // SetUp() aborts on the 2nd TEST_F.
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    ::testing::InitGoogleTest(&argc, argv);
    int rc = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return rc;
}
