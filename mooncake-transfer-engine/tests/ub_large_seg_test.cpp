// Copyright 2026 KVCache.AI
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

// Regression test for issue #3503: UbTransport must auto-chunk registrations
// larger than max_seg_size and split transfers that straddle BufferDesc chunk
// boundaries.
//
// Needs Kunpeng / URMA hardware, and MC_MAX_SEG_SIZE must be set before the
// process starts (globalConfig() reads it once):
//
//   MC_MAX_SEG_SIZE=67108864 ./ub_large_seg_test
//
// Not registered with add_test() for that reason, mirroring rdma_large_mr_test
// and efa_large_mr_test.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common.h"
#include "config.h"
#include "transfer_engine.h"
#include "transport/transport.h"

DEFINE_string(device_name, "",
              "Comma-separated UB device names (empty = auto-discover all)");

using namespace mooncake;

namespace mooncake {

// Small max_seg_size so the multi-chunk path is exercised without a multi-GB
// allocation. 256 MiB / 64 MiB = 4 chunks.
static constexpr size_t kMaxSegSize = 64ull << 20;
static constexpr size_t kBufferSize = 256ull << 20;
static constexpr size_t kExpectedChunks = kBufferSize / kMaxSegSize;
static constexpr size_t kDataLength = 1ull << 20;

class UBLargeSegTest : public ::testing::Test {
   protected:
    std::unique_ptr<TransferEngine> engine;
    void* addr = nullptr;

    void SetUp() override {
        const char* env = std::getenv("MC_METADATA_SERVER");
        std::string metadata_server = env ? env : "P2PHANDSHAKE";
        env = std::getenv("MC_LOCAL_SERVER_NAME");
        std::string local_server_name = env ? env : "127.0.0.1:12345";

        ASSERT_EQ(globalConfig().max_seg_size, kMaxSegSize)
            << "Launch this test process with MC_MAX_SEG_SIZE=" << kMaxSegSize;

        engine = std::make_unique<TransferEngine>(false);
        std::vector<std::string> ub_filter;
        if (!FLAGS_device_name.empty()) {
            std::stringstream ss(FLAGS_device_name);
            std::string item;
            while (std::getline(ss, item, ',')) {
                if (!item.empty()) ub_filter.push_back(item);
            }
        }
        engine->getLocalTopology()->discover(ub_filter);
        ASSERT_EQ(engine->init(metadata_server, local_server_name), 0);
        ASSERT_NE(engine->installTransport("ub", nullptr), nullptr)
            << "installTransport(\"ub\") failed -- Kunpeng/URMA hardware "
               "required";

        addr = numa_alloc_onnode(kBufferSize, 0);
        ASSERT_NE(addr, nullptr);
        ASSERT_EQ(engine->registerLocalMemory(addr, kBufferSize, "cpu:0"), 0);

        auto desc = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        ASSERT_NE(desc, nullptr);
        size_t chunks = 0;
        for (const auto& buffer : desc->buffers) {
            if (buffer.addr >= reinterpret_cast<uint64_t>(addr) &&
                buffer.addr < reinterpret_cast<uint64_t>(addr) + kBufferSize) {
                EXPECT_LE(buffer.length, kMaxSegSize);
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

    TransferStatusEnum submitAndWait(void* source, uint64_t target_offset,
                                     size_t length,
                                     TransferRequest::OpCode opcode) {
        auto batch_id = engine->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = opcode;
        entry.length = length;
        entry.source = source;
        entry.target_id = LOCAL_SEGMENT_ID;
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
        Status fs = engine->freeBatchID(batch_id);
        if (!fs.ok() && !fs.IsBatchCleanupDeferred()) {
            LOG(ERROR) << "freeBatchID failed: " << fs.ToString();
        }
        return status.s;
    }

    void fillSource(size_t offset, size_t length = kDataLength) {
        for (size_t i = 0; i < length; ++i) {
            static_cast<char*>(addr)[offset + i] =
                static_cast<char>('a' + (lrand48() % 26));
        }
    }
};

// Baseline: the target lands past the first chunk (chunk 3). Pre-fix
// UrmaContext truncated the registration to max_seg_size while publishing the
// full length, so writes past chunk 0 failed.
TEST_F(UBLargeSegTest, WritePastFirstChunk) {
    const size_t kTargetOffset = kBufferSize - kDataLength;
    ASSERT_GT(kTargetOffset, kMaxSegSize);

    fillSource(0);
    memset(static_cast<char*>(addr) + kTargetOffset, 0, kDataLength);

    ASSERT_EQ(
        submitAndWait(addr, reinterpret_cast<uint64_t>(addr) + kTargetOffset,
                      kDataLength, TransferRequest::WRITE),
        TransferStatusEnum::COMPLETED)
        << "UB WRITE past max_seg_size failed -- registration was truncated "
           "(issue #3503)";
    ASSERT_EQ(
        0, memcmp(addr, static_cast<char*>(addr) + kTargetOffset, kDataLength));
}

// Target range straddles the seam between chunk 0 and chunk 1.
TEST_F(UBLargeSegTest, WriteStraddlesChunkBoundary) {
    const size_t kTargetOffset = kMaxSegSize - kDataLength / 2 + 1;
    ASSERT_LT(kTargetOffset, kMaxSegSize);
    ASSERT_GT(kTargetOffset + kDataLength, kMaxSegSize);
    ASSERT_NE(kTargetOffset % globalConfig().slice_size, 0u);

    fillSource(0);
    memset(static_cast<char*>(addr) + kTargetOffset, 0, kDataLength);

    ASSERT_EQ(
        submitAndWait(addr, reinterpret_cast<uint64_t>(addr) + kTargetOffset,
                      kDataLength, TransferRequest::WRITE),
        TransferStatusEnum::COMPLETED)
        << "UB WRITE straddling a target chunk boundary failed (issue #3503)";
    ASSERT_EQ(
        0, memcmp(addr, static_cast<char*>(addr) + kTargetOffset, kDataLength));
}

// Small request within slice_size + fragment_limit that still straddles a seam
// (guards against merge_final_slice bypassing chunk boundary splitting).
TEST_F(UBLargeSegTest, WriteSmallMergeFinalStraddlesChunkBoundary) {
    const size_t kSmallLength =
        std::min<size_t>(globalConfig().slice_size, 4096);
    const size_t kTargetOffset = kMaxSegSize - kSmallLength / 2 + 1;
    ASSERT_LT(kTargetOffset, kMaxSegSize);
    ASSERT_GT(kTargetOffset + kSmallLength, kMaxSegSize);

    fillSource(0, kSmallLength);
    memset(static_cast<char*>(addr) + kTargetOffset, 0, kSmallLength);

    ASSERT_EQ(
        submitAndWait(addr, reinterpret_cast<uint64_t>(addr) + kTargetOffset,
                      kSmallLength, TransferRequest::WRITE),
        TransferStatusEnum::COMPLETED)
        << "Small UB WRITE straddling a chunk boundary failed (issue #3503)";
    ASSERT_EQ(0, memcmp(addr, static_cast<char*>(addr) + kTargetOffset,
                        kSmallLength));
}

// Source range straddles the seam between chunk 0 and chunk 1.
TEST_F(UBLargeSegTest, WriteWithSourceStraddlingChunkBoundary) {
    const size_t kSourceOffset = kMaxSegSize - kDataLength / 2 + 1;
    const size_t kTargetOffset = 2 * kMaxSegSize;
    ASSERT_NE(kSourceOffset % globalConfig().slice_size, 0u);

    fillSource(kSourceOffset);
    memset(static_cast<char*>(addr) + kTargetOffset, 0, kDataLength);

    ASSERT_EQ(submitAndWait(static_cast<char*>(addr) + kSourceOffset,
                            reinterpret_cast<uint64_t>(addr) + kTargetOffset,
                            kDataLength, TransferRequest::WRITE),
              TransferStatusEnum::COMPLETED)
        << "UB WRITE with a source straddling a chunk boundary failed "
           "(issue #3503)";
    ASSERT_EQ(0, memcmp(static_cast<char*>(addr) + kSourceOffset,
                        static_cast<char*>(addr) + kTargetOffset, kDataLength));
}

// READ pulls from a remote range straddling seam 0->1 into a local range
// straddling seam 1->2, forcing slices to split at both boundaries.
TEST_F(UBLargeSegTest, ReadStraddlesChunkBoundaryOnBothSides) {
    const size_t kRemoteOffset = kMaxSegSize - kDataLength / 2 + 1;
    const size_t kLocalOffset = 2 * kMaxSegSize - kDataLength / 2 + 1;

    fillSource(kRemoteOffset);
    memset(static_cast<char*>(addr) + kLocalOffset, 0, kDataLength);

    ASSERT_EQ(submitAndWait(static_cast<char*>(addr) + kLocalOffset,
                            reinterpret_cast<uint64_t>(addr) + kRemoteOffset,
                            kDataLength, TransferRequest::READ),
              TransferStatusEnum::COMPLETED)
        << "UB READ straddling chunk boundaries failed (issue #3503)";
    ASSERT_EQ(0, memcmp(static_cast<char*>(addr) + kLocalOffset,
                        static_cast<char*>(addr) + kRemoteOffset, kDataLength));
}

}  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    ::testing::InitGoogleTest(&argc, argv);
    int rc = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return rc;
}
