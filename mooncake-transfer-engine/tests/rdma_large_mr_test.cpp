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

// Regression test for issue #2017: RdmaTransport::registerLocalMemory must NOT
// silently truncate a buffer larger than the device max_mr_size.
//
// Bug: registerMemoryRegionInternal shrank `length` to max_mr_size and
// registered a single MR of that size, but registerLocalMemory still published
// BufferDesc.length = full length with that one (truncated) rkey. A remote RDMA
// op whose target address fell past max_mr_size then failed with
// IBV_WC_REM_ACCESS_ERR (e.g. ionic CQE error 10). It is ADDRESS-driven: ops to
// low addresses succeed, ops past the boundary fail -> PD KV transfer ran for a
// while then a worker died mid-run (seen on MI355x/ionic, per-layer KV ~3.25
// GiB > the ionic 2 GiB max_mr_size).
//
// Fix: split buffers > max_mr_size into <= max_mr_size chunks, register each as
// its own MR, and publish one BufferDesc per chunk.
//
// This test forces the condition at a small, HW-independent size by setting
// MC_MAX_MR_SIZE, then does a LOOPBACK RDMA WRITE whose target lands PAST that
// boundary. Pre-fix: the transfer FAILS (REM_ACCESS_ERR). Post-fix: it
// COMPLETES and the bytes match. Runs on any RDMA device (incl. rdma_rxe /
// loopback).

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <memory>

#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

DEFINE_string(metadata_server, "127.0.0.1:2379",
              "central metadata server for transfer engine");

// Small max_mr_size so the >max_mr_size path is exercised without needing a
// multi-GB allocation. Must be set before TransferEngine init (config reads
// env).
static constexpr size_t kMaxMrSize = 64ull << 20;    // 64 MiB
static constexpr size_t kBufferSize = 256ull << 20;  // 256 MiB -> 4 chunks

class RDMALargeMrTest : public ::testing::Test {
   public:
    void *addr = nullptr;
    std::unique_ptr<mooncake::TransferEngine> engine;

   protected:
    void SetUp() override {
        google::InitGoogleLogging("RDMALargeMrTest");
        FLAGS_logtostderr = 1;
        // Force a small device max_mr_size cap (config caps to min(env,
        // device)).
        setenv("MC_MAX_MR_SIZE", std::to_string(kMaxMrSize).c_str(), 1);
        engine = std::make_unique<TransferEngine>(true);
        engine->init(FLAGS_metadata_server, "test_node_large_mr");
        addr = numa_alloc_onnode(kBufferSize, 0);
        ASSERT_NE(addr, nullptr);
        // Register a buffer LARGER than max_mr_size. Pre-fix this silently
        // truncates the MR to kMaxMrSize; post-fix it splits into 4 chunks.
        int rc = engine->registerLocalMemory(addr, kBufferSize, "cpu:0");
        ASSERT_EQ(rc, 0);
    }

    void TearDown() override {
        if (engine && addr) engine->unregisterLocalMemory(addr);
        if (addr) numa_free(addr, kBufferSize);
        google::ShutdownGoogleLogging();
    }
};

// Loopback RDMA WRITE whose TARGET lands past max_mr_size. The source stays in
// the first MR; only the destination address exercises the truncation boundary.
TEST_F(RDMALargeMrTest, WritePastMaxMrSizeBoundary) {
    const size_t kDataLength = 1ull << 20;  // 1 MiB
    // Target offset is well past kMaxMrSize (in the 4th chunk). Pre-fix: the
    // single 64 MiB MR does not cover this address -> IBV_WC_REM_ACCESS_ERR.
    const size_t kTargetOffset = kBufferSize - kDataLength;  // ~255 MiB
    ASSERT_GT(kTargetOffset, kMaxMrSize);

    for (size_t i = 0; i < kDataLength; ++i)
        *((char *)addr + i) = (char)('a' + (lrand48() % 26));

    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t *)addr;  // src in chunk 0
    entry.target_id = LOCAL_SEGMENT_ID;
    entry.target_offset = (uint64_t)addr + kTargetOffset;  // dst past 64 MiB

    Status s = engine->submitTransfer(batch_id, {entry});
    ASSERT_TRUE(s.ok());

    TransferStatus status;
    bool completed = false;
    while (!completed) {
        Status gs = engine->getTransferStatus(batch_id, 0, status);
        ASSERT_EQ(gs, Status::OK());
        if (status.s == TransferStatusEnum::COMPLETED)
            completed = true;
        else if (status.s == TransferStatusEnum::FAILED)
            break;
    }
    ASSERT_EQ(engine->freeBatchID(batch_id), Status::OK());

    // The regression assertion: pre-fix this is FAILED (remote access error);
    // post-fix it COMPLETES and the bytes match.
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED)
        << "RDMA WRITE to an address past max_mr_size failed -- buffer MR was "
           "truncated and not auto-chunked (issue #2017).";
    ASSERT_EQ(0, memcmp(addr, (char *)addr + kTargetOffset, kDataLength));
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
