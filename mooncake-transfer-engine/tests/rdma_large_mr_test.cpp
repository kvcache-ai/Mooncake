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
#include <atomic>
#include <condition_variable>
#include <dlfcn.h>
#include <future>
#include <mutex>
#include <cstring>
#include <memory>
#include <vector>

#if defined(USE_CUDA) || defined(USE_HIP)
#include "cuda_alike.h"
#endif
#include "config.h"
#include "rdma_test_peers.h"
#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

namespace {
std::atomic<void *> held_dereg_address{nullptr};
std::mutex held_dereg_mutex;
std::condition_variable held_dereg_cv;
bool held_dereg_entered = false;
bool held_dereg_release = false;
}  // namespace

// Test-only interposition: hold one genuine MR inside the driver call, then
// fail it without destroying it. All other deregistrations use real verbs.
extern "C" int ibv_dereg_mr(ibv_mr *mr) {
    void *held = held_dereg_address.load();
    if (held && mr->addr == held) {
        std::unique_lock<std::mutex> lock(held_dereg_mutex);
        held_dereg_entered = true;
        held_dereg_cv.notify_all();
        held_dereg_cv.wait(lock, [] { return held_dereg_release; });
        errno = EBUSY;
        return EBUSY;
    }
    using Deregister = int (*)(ibv_mr *);
    static const auto original =
        reinterpret_cast<Deregister>(dlsym(RTLD_NEXT, "ibv_dereg_mr"));
    if (!original) std::abort();
    return original(mr);
}

namespace mooncake {

// Small max_mr_size so the >max_mr_size path is exercised without needing a
// multi-GB allocation. Must be set before TransferEngine init (config reads
// env).
static constexpr size_t kMaxMrSize = 64ull << 20;    // 64 MiB
static constexpr size_t kBufferSize = 256ull << 20;  // 256 MiB -> 4 chunks

static void releaseCompletedBatch(TransferEngine &engine,
                                  Transport::BatchID batch) {
    const Status result = engine.freeBatchID(batch);
    // Current engine APIs may retire a completed batch asynchronously while
    // completion callbacks unwind. Never retry this invalidated handle; each
    // fixture keeps its allocation alive until engine shutdown drains cleanup.
    EXPECT_TRUE(result.ok() || result.IsBatchCleanupDeferred())
        << result.ToString();
}

class RDMALargeMrTest : public ::testing::Test {
   public:
    void *addr = nullptr;
    std::unique_ptr<mooncake::TransferEngine> engine;

   protected:
    void SetUp() override {
        engine = std::make_unique<TransferEngine>(true);
        const char *env_meta = std::getenv("MC_METADATA_SERVER");
        std::string metadata_server = env_meta ? env_meta : "P2PHANDSHAKE";
        ASSERT_EQ(engine->init(metadata_server, "127.0.0.1"), 0);
        ASSERT_EQ(globalConfig().max_mr_size, kMaxMrSize)
            << "Launch this test process with MC_MAX_MR_SIZE=" << kMaxMrSize;
        addr = numa_alloc_onnode(kBufferSize, 0);
        ASSERT_NE(addr, nullptr);
        // Register a buffer LARGER than max_mr_size. Pre-fix this silently
        // truncates the MR to kMaxMrSize; post-fix it splits into 4 chunks.
        int rc = engine->registerLocalMemory(addr, kBufferSize, "cpu:0");
        ASSERT_EQ(rc, 0);
    }

    void TearDown() override {
        if (engine && addr) engine->unregisterLocalMemory(addr);
        engine.reset();
        if (addr) numa_free(addr, kBufferSize);
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
    releaseCompletedBatch(*engine, batch_id);

    // The regression assertion: pre-fix this is FAILED (remote access error);
    // post-fix it COMPLETES and the bytes match.
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED)
        << "RDMA WRITE to an address past max_mr_size failed -- buffer MR was "
           "truncated and not auto-chunked (issue #2017).";
    ASSERT_EQ(0, memcmp(addr, (char *)addr + kTargetOffset, kDataLength));
}

// Loopback RDMA WRITE whose TARGET range STRADDLES a chunk boundary (the seam
// between chunk 0 and chunk 1 at kMaxMrSize). The single logical transfer spans
// two MRs, so it must be split across the two chunks' rkeys -- otherwise the
// same IBV_WC_REM_ACCESS_ERR class re-surfaces at chunk seams (the concern this
// test guards, complementing WritePastMaxMrSizeBoundary which stays within one
// chunk).
TEST_F(RDMALargeMrTest, WriteStraddlesChunkBoundary) {
    const size_t kDataLength = 1ull << 20;  // 1 MiB
    // Center the target on the first chunk boundary: half in chunk 0, half in
    // chunk 1.
    const size_t kTargetOffset = kMaxMrSize - kDataLength / 2 + 1;
    ASSERT_LT(kTargetOffset, kMaxMrSize);                // starts in chunk 0
    ASSERT_GT(kTargetOffset + kDataLength, kMaxMrSize);  // ends in chunk 1
    ASSERT_NE(kTargetOffset % globalConfig().slice_size, 0u);

    // Distinct source bytes, and poison the destination so a dropped/partial
    // write past the seam is caught by the memcmp below.
    for (size_t i = 0; i < kDataLength; ++i)
        *((char *)addr + i) = (char)('A' + (lrand48() % 26));
    memset((char *)addr + kTargetOffset, 0, kDataLength);

    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t *)addr;  // src in chunk 0
    entry.target_id = LOCAL_SEGMENT_ID;
    entry.target_offset = (uint64_t)addr + kTargetOffset;  // straddles the seam

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
    releaseCompletedBatch(*engine, batch_id);

    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED)
        << "RDMA WRITE straddling a chunk boundary failed -- a cross-chunk "
           "transfer was not split across the two chunks' MRs (issue #2017).";
    ASSERT_EQ(0, memcmp(addr, (char *)addr + kTargetOffset, kDataLength));
}

// Verify RDMA WRITE when local source buffer range straddles an MR boundary.
TEST_F(RDMALargeMrTest, WriteWithSourceStraddlingChunkBoundary) {
    const size_t kDataLength = 1ull << 20;
    const size_t kSourceOffset = kMaxMrSize - kDataLength / 2 + 1;
    const size_t kTargetOffset = 2 * kMaxMrSize;
    ASSERT_NE(kSourceOffset % globalConfig().slice_size, 0u);

    for (size_t i = 0; i < kDataLength; ++i)
        *((char *)addr + kSourceOffset + i) = (char)('A' + (lrand48() % 26));
    memset((char *)addr + kTargetOffset, 0, kDataLength);

    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t *)addr + kSourceOffset;
    entry.target_id = LOCAL_SEGMENT_ID;
    entry.target_offset = (uint64_t)addr + kTargetOffset;

    Status s = engine->submitTransfer(batch_id, {entry});
    ASSERT_TRUE(s.ok());

    TransferStatus status;
    while (true) {
        ASSERT_EQ(engine->getTransferStatus(batch_id, 0, status), Status::OK());
        if (status.s != TransferStatusEnum::WAITING) break;
    }
    releaseCompletedBatch(*engine, batch_id);
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
    ASSERT_EQ(0, memcmp((char *)addr + kSourceOffset,
                        (char *)addr + kTargetOffset, kDataLength));
}

class RDMAParallelHostMrTest : public ::testing::Test {
   protected:
    static constexpr size_t size = (1ull << 30) + 4096;
    void *addr = nullptr;
    std::unique_ptr<TransferEngine> engine;
    size_t old_workers = 0;
    uint64_t old_limit = 0;

    void SetUp() override {
        engine = std::make_unique<TransferEngine>(true);
        ASSERT_EQ(engine->init("P2PHANDSHAKE", "127.0.0.1"), 0);
        old_workers = globalConfig().host_reg_threads;
        old_limit = globalConfig().max_mr_size;
        globalConfig().host_reg_threads = 4;
        // Exercise software splitting with an uneven final chunk, independent
        // of the device limit and the small MC_MAX_MR_SIZE used by other tests.
        globalConfig().max_mr_size = 2ull << 30;
        addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(addr, MAP_FAILED);
    }

    void TearDown() override {
        engine.reset();
        if (addr && addr != MAP_FAILED) munmap(addr, size);
        globalConfig().host_reg_threads = old_workers;
        globalConfig().max_mr_size = old_limit;
    }

    void expectNoRegions() {
        auto desc = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        ASSERT_NE(desc, nullptr);
        EXPECT_TRUE(desc->buffers.empty());
        auto *rdma =
            dynamic_cast<RdmaTransport *>(engine->getTransport("rdma"));
        ASSERT_NE(rdma, nullptr);
        for (const auto &context : rdma->getContextList())
            EXPECT_EQ(RdmaContextTestPeer::memoryRegionCount(*context), 0u);
    }

    void transfer(size_t source_offset, size_t target_offset, bool read) {
        constexpr size_t bytes = 65539;
        auto *source = static_cast<uint8_t *>(addr) + source_offset;
        auto *target = static_cast<uint8_t *>(addr) + target_offset;
        for (size_t i = 0; i < bytes; ++i)
            (read ? target : source)[i] = static_cast<uint8_t>(i * 17 + 3);
        memset(read ? source : target, 0, bytes);
        auto batch = engine->allocateBatchID(1);
        TransferRequest request;
        request.opcode = read ? TransferRequest::READ : TransferRequest::WRITE;
        request.source = source;
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = reinterpret_cast<uint64_t>(target);
        request.length = bytes;
        ASSERT_TRUE(engine->submitTransfer(batch, {request}).ok());
        TransferStatus status;
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        do {
            ASSERT_TRUE(engine->getTransferStatus(batch, 0, status).ok());
        } while (status.s == TransferStatusEnum::WAITING &&
                 std::chrono::steady_clock::now() < deadline);
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
        releaseCompletedBatch(*engine, batch);
        EXPECT_EQ(memcmp(source, target, bytes), 0);
    }

    void checkTransfers(size_t expected_chunks) {
        auto desc = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        ASSERT_NE(desc, nullptr);
        ASSERT_EQ(desc->buffers.size(), expected_chunks);
        for (size_t i = 1; i < desc->buffers.size(); ++i) {
            size_t seam =
                desc->buffers[i].addr - reinterpret_cast<uint64_t>(addr);
            transfer(0, seam - 32767, false);
            transfer(0, seam - 32767, true);
            transfer(seam - 32767, 1ull << 20, false);
        }
        transfer(0, size - 65539, false);
    }
};

TEST_F(RDMAParallelHostMrTest, SplitsHostBufferBelowDeviceLimit) {
    ASSERT_EQ(engine->registerLocalMemory(addr, size, "cpu:0"), 0);
    checkTransfers(4u);
    ASSERT_EQ(engine->unregisterLocalMemory(addr), 0);
    expectNoRegions();
}

TEST_F(RDMAParallelHostMrTest, MoreChunksThanWorkersRollbackAndRetry) {
    // Eleven chunks on four workers: cover uneven worker partitions and the
    // final partial chunk, including failures beyond the first worker wave.
    globalConfig().max_mr_size = 96ull << 20;
    auto *last_page = static_cast<char *>(addr) + size - 4096;
    ASSERT_EQ(mprotect(last_page, 4096, PROT_NONE), 0);
    EXPECT_NE(engine->registerLocalMemory(addr, size, "cpu:0"), 0);
    expectNoRegions();
    ASSERT_EQ(mprotect(last_page, 4096, PROT_READ | PROT_WRITE), 0);

    for (int cycle = 0; cycle < 2; ++cycle) {
        ASSERT_EQ(engine->registerLocalMemory(addr, size, "cpu:0"), 0);
        ASSERT_NO_FATAL_FAILURE(checkTransfers(11u));
        if (cycle == 0) {
            {
                std::lock_guard<std::mutex> lock(held_dereg_mutex);
                held_dereg_entered = false;
                held_dereg_release = true;
            }
            auto desc =
                engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
            held_dereg_address =
                reinterpret_cast<void *>(desc->buffers.back().addr);
            const int rc = engine->unregisterLocalMemory(addr);
            held_dereg_address = nullptr;
            EXPECT_NE(rc, 0);
        }
        ASSERT_EQ(engine->unregisterLocalMemory(addr), 0);
        expectNoRegions();
    }
}

TEST_F(RDMAParallelHostMrTest,
       DeregistrationUnlocksOtherRegionsAndRestoresOnFailure) {
    auto *rdma = dynamic_cast<RdmaTransport *>(engine->getTransport("rdma"));
    ASSERT_NE(rdma, nullptr);
    ASSERT_FALSE(rdma->getContextList().empty());
    auto &context = *rdma->getContextList().front();
    void *other = static_cast<char *>(addr) + 4096;
    ASSERT_EQ(context.registerMemoryRegion(addr, 4096, IBV_ACCESS_LOCAL_WRITE),
              0);
    ASSERT_EQ(context.registerMemoryRegion(other, 4096, IBV_ACCESS_LOCAL_WRITE),
              0);
    const uint32_t key = context.lkey(addr);
    const uint32_t other_key = context.lkey(other);
    {
        std::lock_guard<std::mutex> lock(held_dereg_mutex);
        held_dereg_entered = false;
        held_dereg_release = false;
    }
    held_dereg_address = addr;
    auto blocked = std::async(std::launch::async, [&] {
        return context.unregisterMemoryRegion(addr);
    });
    {
        std::unique_lock<std::mutex> lock(held_dereg_mutex);
        EXPECT_TRUE(held_dereg_cv.wait_for(lock, std::chrono::seconds(5),
                                           [] { return held_dereg_entered; }));
    }
    auto independent = std::async(std::launch::async, [&] {
        const uint32_t observed = context.lkey(other);
        const int rc = context.unregisterMemoryRegion(other);
        return std::pair{observed, rc};
    });
    EXPECT_EQ(independent.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    {
        std::lock_guard<std::mutex> lock(held_dereg_mutex);
        held_dereg_release = true;
    }
    held_dereg_cv.notify_all();
    EXPECT_NE(blocked.get(), 0);
    held_dereg_address = nullptr;
    auto [observed, rc] = independent.get();
    EXPECT_EQ(observed, other_key);
    EXPECT_EQ(rc, 0);
    EXPECT_EQ(context.lkey(addr), key);
    EXPECT_EQ(RdmaContextTestPeer::memoryRegionCount(context), 1u);
    EXPECT_EQ(context.unregisterMemoryRegion(addr), 0);
    EXPECT_EQ(RdmaContextTestPeer::memoryRegionCount(context), 0u);
}

TEST_F(RDMAParallelHostMrTest, ReleasesDataDirectPlaceholderWithoutVerbs) {
    auto *rdma = dynamic_cast<RdmaTransport *>(engine->getTransport("rdma"));
    ASSERT_NE(rdma, nullptr);
    ASSERT_FALSE(rdma->getContextList().empty());
    auto &context = *rdma->getContextList().front();
    RdmaContextTestPeer::seedMemoryRegion(context, addr, 4096, nullptr);
    ASSERT_EQ(context.unregisterMemoryRegion(addr), 0);
    EXPECT_EQ(RdmaContextTestPeer::memoryRegionCount(context), 0u);
}

#if defined(USE_CUDA) || defined(USE_HIP)
class RDMAGpuDmabufChunkTest : public ::testing::TestWithParam<size_t> {
   protected:
    std::unique_ptr<TransferEngine> engine;
    void *gpu_allocation = nullptr;
    void *gpu_addr = nullptr;

    void SetUp() override {
        int device_count = 0;
        ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
        if (device_count == 0) GTEST_SKIP() << "No GPU available";
        ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

#if defined(USE_CUDA)
        ASSERT_TRUE(globalConfig().rdma_data_direct ||
                    !globalConfig().with_nvidia_peermem)
            << "Launch with WITH_NVIDIA_PEERMEM=0 or MC_RDMA_DATA_DIRECT=1 "
               "so this test exercises DMA-BUF registration";
#endif

        engine = std::make_unique<TransferEngine>(true);
        const char *env_meta = std::getenv("MC_METADATA_SERVER");
        std::string metadata_server = env_meta ? env_meta : "P2PHANDSHAKE";
        ASSERT_EQ(engine->init(metadata_server, "127.0.0.1"), 0);
        ASSERT_EQ(globalConfig().max_mr_size, kMaxMrSize)
            << "Launch this test process with MC_MAX_MR_SIZE=" << kMaxMrSize;

        ASSERT_EQ(cudaMalloc(&gpu_allocation, kBufferSize + 65536),
                  cudaSuccess);
        gpu_addr = static_cast<char *>(gpu_allocation) + GetParam();
        ASSERT_EQ(engine->registerLocalMemory(gpu_addr, kBufferSize,
                                              GPU_PREFIX + "0"),
                  0);
    }

    void TearDown() override {
        if (engine && gpu_addr) engine->unregisterLocalMemory(gpu_addr);
        engine.reset();
        if (gpu_allocation) cudaFree(gpu_allocation);
    }
};

// The allocation is split into multiple MRs. Before the fix, every chunk is
// registered with the dma-buf offset of chunk 0. A loopback WRITE targeting
// a later chunk therefore maps the wrong GPU pages, fails
// registration/transfer, or completes without updating the requested
// destination bytes.
TEST_P(RDMAGpuDmabufChunkTest, LaterChunkUsesItsOwnDmabufOffset) {
    const size_t kDataLength = 1ull << 20;
    const size_t kTargetOffset = kBufferSize - kDataLength;
    std::vector<char> source(kDataLength);
    std::vector<char> result(kDataLength, 0);
    for (size_t i = 0; i < source.size(); ++i)
        source[i] = (char)('a' + (lrand48() % 26));

    ASSERT_EQ(cudaMemcpy(gpu_addr, source.data(), kDataLength,
                         cudaMemcpyHostToDevice),
              cudaSuccess);
    ASSERT_EQ(cudaMemset((char *)gpu_addr + kTargetOffset, 0, kDataLength),
              cudaSuccess);

    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = gpu_addr;
    entry.target_id = LOCAL_SEGMENT_ID;
    entry.target_offset = (uint64_t)gpu_addr + kTargetOffset;
    ASSERT_TRUE(engine->submitTransfer(batch_id, {entry}).ok());

    TransferStatus status;
    while (true) {
        ASSERT_EQ(engine->getTransferStatus(batch_id, 0, status), Status::OK());
        if (status.s != TransferStatusEnum::WAITING) break;
    }
    releaseCompletedBatch(*engine, batch_id);
    ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);

    ASSERT_EQ(cudaMemcpy(result.data(), (char *)gpu_addr + kTargetOffset,
                         kDataLength, cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(result, source);
}

TEST_P(RDMAGpuDmabufChunkTest, RegistersInteriorGpuBuffers) {
    ASSERT_EQ(engine->unregisterLocalMemory(gpu_addr), 0);
    constexpr size_t length = (8ull << 20) + 1024;
    const size_t offsets[] = {0, 1024, 4096, 65536, length};
    for (size_t offset : offsets) {
        auto *ptr = static_cast<char *>(gpu_addr) + offset;
        ASSERT_EQ(engine->registerLocalMemory(ptr, length, GPU_PREFIX + "0"), 0)
            << "offset=" << offset;
        EXPECT_EQ(engine->unregisterLocalMemory(ptr), 0);
    }
}

TEST_P(RDMAGpuDmabufChunkTest, HostWorkersDoNotSplitDeviceMemory) {
    ASSERT_EQ(engine->unregisterLocalMemory(gpu_addr), 0);
    ASSERT_EQ(cudaFree(gpu_allocation), cudaSuccess);
    gpu_allocation = nullptr;
    gpu_addr = nullptr;
    constexpr size_t bytes = 1ull << 30;
    ASSERT_EQ(cudaMalloc(&gpu_allocation, bytes + GetParam()), cudaSuccess);
    gpu_addr = static_cast<char *>(gpu_allocation) + GetParam();
    auto &config = globalConfig();
    const auto old_workers = config.host_reg_threads;
    const auto old_limit = config.max_mr_size;
    config.host_reg_threads = 16;
    config.max_mr_size = 2ull << 30;
    int rc = engine->registerLocalMemory(gpu_addr, bytes, GPU_PREFIX + "0");
    config.host_reg_threads = old_workers;
    config.max_mr_size = old_limit;
    ASSERT_EQ(rc, 0);
    auto desc = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(desc, nullptr);
    ASSERT_EQ(desc->buffers.size(), 1u);
    EXPECT_EQ(desc->buffers.front().length, bytes);
}

INSTANTIATE_TEST_SUITE_P(AllocationOffsets, RDMAGpuDmabufChunkTest,
                         ::testing::Values<size_t>(0, 1024));
#endif

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    // Initialize logging once for the whole binary — calling
    // InitGoogleLogging() per-test in SetUp() aborts on the 2nd TEST_F.
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    ::testing::InitGoogleTest(&argc, argv);
    int rc = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return rc;
}
