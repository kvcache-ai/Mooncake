// Copyright 2025 Mooncake Authors
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

#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "cuda_alike.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"  // P2PHANDSHAKE
#include "transport/hip_transport/batch_copy.h"
#include "transport/transport.h"

using namespace mooncake;

// startAsyncTransfer() switches the active device to the source GPU; if it does
// not restore it, the calling thread (which also launches the engine's compute
// kernels) is left on the wrong GPU and the next kernel fails with
// hipErrorInvalidDevice. This pins the caller to one GPU, transfers from a
// buffer on a different GPU, and asserts the active device is unchanged.

namespace {
constexpr size_t kLen = 64 * 1024;

void* allocOnDevice(size_t size, int device) {
    EXPECT_EQ(cudaSetDevice(device), cudaSuccess);
    void* ptr = nullptr;
    EXPECT_EQ(cudaMalloc(&ptr, size), cudaSuccess);
    return ptr;
}

std::vector<uint8_t> randomBytes(size_t len, uint32_t seed) {
    std::mt19937 rng(seed);
    std::vector<uint8_t> bytes(len);
    for (auto& b : bytes) b = static_cast<uint8_t>(rng());
    return bytes;
}

std::vector<uint8_t> readBack(const void* device_ptr, size_t len) {
    std::vector<uint8_t> host(len);
    EXPECT_EQ(cudaMemcpy(host.data(), device_ptr, len, cudaMemcpyDefault),
              cudaSuccess);
    return host;
}
}  // namespace

// The kernel copies ranges of any length and alignment between two GPUs, and
// a batch needing more launches than descriptor slots waits for the oldest
// launch instead of failing.
TEST(BatchCopyKernelTest, CopiesRangesOfAnyAlignment) {
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    if (device_count < 1) GTEST_SKIP() << "Needs a GPU.";
    const int peer = device_count - 1;
    constexpr size_t kBytes = 4 << 20;
    auto expected = randomBytes(kBytes, 1);
    void* src = allocOnDevice(kBytes, 0);
    void* dst = allocOnDevice(kBytes, peer);
    ASSERT_EQ(cudaMemcpy(src, expected.data(), kBytes, cudaMemcpyDefault),
              cudaSuccess);
    ASSERT_EQ(cudaMemset(dst, 0, kBytes), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);

    // Back-to-back ranges with lengths and offsets that are rarely
    // 16-byte aligned, then 64 B ranges (unaligned too) to the end.
    std::vector<BatchCopyKernel::Range> ranges;
    const uint64_t s = (uint64_t)src, d = (uint64_t)dst;
    uint64_t off = 0;
    for (uint64_t len : {1, 3, 15, 16, 17, 255, 4095, 4096, 4097, 65536}) {
        ranges.push_back({s + off, d + off, len});
        off += len;
    }
    for (; off < kBytes; off += 64) {
        ranges.push_back(
            {s + off, d + off, std::min<uint64_t>(64, kBytes - off)});
    }
    ASSERT_GT(ranges.size(), 2 * BatchCopyKernel::kSlotRanges);

    BatchCopyKernel kernel(/*num_slots=*/2, /*max_grid=*/64);
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    if (peer != 0) {
        cudaError_t err = cudaDeviceEnablePeerAccess(peer, 0);
        ASSERT_TRUE(err == cudaSuccess ||
                    err == cudaErrorPeerAccessAlreadyEnabled);
        (void)cudaGetLastError();
    }
    ASSERT_TRUE(kernel.available(0));
    cudaStream_t stream;
    ASSERT_EQ(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking),
              cudaSuccess);
    EXPECT_EQ(kernel.enqueue(0, stream, ranges.data(), ranges.size()),
              ranges.size());
    ASSERT_EQ(cudaStreamSynchronize(stream), cudaSuccess);
    EXPECT_TRUE(readBack(dst, kBytes) == expected);

    (void)cudaStreamDestroy(stream);
    (void)cudaFree(src);
    (void)cudaFree(dst);
}

TEST(HipTransportTest, RestoresActiveDeviceAfterTransfer) {
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    if (device_count < 2) {
        GTEST_SKIP() << "Needs >= 2 GPUs: the transfer source must live on a "
                        "different device than the calling thread's device.";
    }

    const int kCallerDevice = 0;  // device the engine thread runs on
    const int kSourceDevice = 1;  // KV source lives on a different GPU

    // P2PHANDSHAKE: self-contained loopback, no external metadata server.
    auto engine = std::make_unique<TransferEngine>(false);
    const std::string server_name = "127.0.0.1:17813";
    if (engine->init(P2PHANDSHAKE, server_name, "127.0.0.1", 17813) != 0) {
        GTEST_SKIP() << "TransferEngine init failed in this environment.";
    }

    Transport* transport = engine->installTransport("hip", nullptr);
    if (transport == nullptr) {
        GTEST_SKIP()
            << "HIP transport unavailable (built without -DUSE_HIP=ON?).";
    }

    // Both buffers on kSourceDevice so the source GPU differs from the caller.
    void* src = allocOnDevice(kLen, kSourceDevice);
    void* dst = allocOnDevice(kLen, kSourceDevice);
    ASSERT_EQ(engine->registerLocalMemory(
                  src, kLen, GPU_PREFIX + std::to_string(kSourceDevice)),
              0);
    ASSERT_EQ(engine->registerLocalMemory(
                  dst, kLen, GPU_PREFIX + std::to_string(kSourceDevice)),
              0);

    // P2PHANDSHAKE may bind a different RPC port than the one requested.
    auto segment_id = engine->openSegment(engine->getLocalIpAndPort());
    ASSERT_GE(segment_id, 0);

    ASSERT_EQ(cudaSetDevice(kSourceDevice), cudaSuccess);
    ASSERT_EQ(cudaMemset(src, 0xAB, kLen), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);

    // Pin the caller to a different device than the source, as the engine does.
    ASSERT_EQ(cudaSetDevice(kCallerDevice), cudaSuccess);

    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kLen;
    entry.source = src;
    entry.target_id = segment_id;
    entry.target_offset = reinterpret_cast<uint64_t>(dst);
    Status s = engine->submitTransfer(batch_id, {entry});
    ASSERT_TRUE(s.ok());

    // Invariant: the caller's device is unchanged (== kSourceDevice before
    // fix).
    int active_after_submit = -1;
    ASSERT_EQ(cudaGetDevice(&active_after_submit), cudaSuccess);
    EXPECT_EQ(active_after_submit, kCallerDevice)
        << "startAsyncTransfer() must restore the caller's active device. "
           "Leaving it on the source GPU corrupts the engine thread's HIP "
           "context, so its next kernel launch fails with "
           "hipErrorInvalidDevice.";

    // Drain the transfer (also a basic functional check).
    TransferStatus status;
    do {
        ASSERT_TRUE(engine->getTransferStatus(batch_id, 0, status).ok());
    } while (status.s == TransferStatusEnum::WAITING);
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);

    int active_after_wait = -1;
    ASSERT_EQ(cudaGetDevice(&active_after_wait), cudaSuccess);
    EXPECT_EQ(active_after_wait, kCallerDevice);

    engine->freeBatchID(batch_id);
    engine->unregisterLocalMemory(src);
    engine->unregisterLocalMemory(dst);
    (void)cudaSetDevice(kSourceDevice);
    (void)cudaFree(src);
    (void)cudaFree(dst);
}

class HipTransportAsyncTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(cudaGetDeviceCount(&device_count_), cudaSuccess);
        if (device_count_ < 1) GTEST_SKIP() << "Needs a GPU.";
        engine_ = std::make_unique<TransferEngine>(false);
        ASSERT_EQ(
            engine_->init(P2PHANDSHAKE, "127.0.0.1:17814", "127.0.0.1", 17814),
            0);
        ASSERT_NE(engine_->installTransport("hip", nullptr), nullptr);
        // A self-opened segment is LOCAL_SEGMENT_ID: no IPC relocation.
        segment_id_ = engine_->openSegment(engine_->getLocalIpAndPort());
        ASSERT_GE(segment_id_, 0);
    }

    void TearDown() override {
        for (auto [ptr, device] : buffers_) {
            engine_->unregisterLocalMemory(ptr);
            (void)cudaSetDevice(device);
            (void)cudaFree(ptr);
        }
    }

    void* allocRegistered(size_t size, int device, int fill) {
        void* ptr = allocOnDevice(size, device);
        EXPECT_EQ(cudaMemset(ptr, fill, size), cudaSuccess);
        EXPECT_EQ(cudaDeviceSynchronize(), cudaSuccess);
        EXPECT_EQ(engine_->registerLocalMemory(
                      ptr, size, GPU_PREFIX + std::to_string(device)),
                  0);
        buffers_.emplace_back(ptr, device);
        return ptr;
    }

    TransferRequest request(TransferRequest::OpCode op, void* local,
                            void* remote, size_t len) {
        TransferRequest r;
        r.opcode = op;
        r.length = len;
        r.source = local;
        r.target_id = segment_id_;
        r.target_offset = reinterpret_cast<uint64_t>(remote);
        return r;
    }

    TransferStatusEnum wait(Transport::BatchID batch_id, size_t task_id) {
        TransferStatus status;
        do {
            EXPECT_TRUE(
                engine_->getTransferStatus(batch_id, task_id, status).ok());
        } while (status.s == TransferStatusEnum::WAITING);
        return status.s;
    }

    static bool allEqual(const void* device_ptr, size_t len, uint8_t value) {
        std::vector<uint8_t> host(len);
        EXPECT_EQ(cudaMemcpy(host.data(), device_ptr, len, cudaMemcpyDefault),
                  cudaSuccess);
        return std::all_of(host.begin(), host.end(),
                           [value](uint8_t b) { return b == value; });
    }

    int device_count_ = 0;
    std::unique_ptr<TransferEngine> engine_;
    SegmentID segment_id_ = -1;
    std::vector<std::pair<void*, int>> buffers_;
};

#ifndef USE_EVENT_DRIVEN_COMPLETION
// submitTransfer only enqueues the copy: a 1 GiB copy is still in flight at
// the first poll, so freeBatchID defers cleanup, and the copy still lands.
TEST_F(HipTransportAsyncTest, SubmitDoesNotWaitForCopy) {
    constexpr size_t kBigLen = 1ull << 30;
    void* src = allocRegistered(kBigLen, 0, 0x5A);
    void* dst = allocRegistered(kBigLen, 0, 0);

    auto batch_id = engine_->allocateBatchID(1);
    ASSERT_TRUE(engine_
                    ->submitTransfer(batch_id, {request(TransferRequest::WRITE,
                                                        src, dst, kBigLen)})
                    .ok());
    TransferStatus status;
    ASSERT_TRUE(engine_->getTransferStatus(batch_id, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::WAITING);
    EXPECT_TRUE(engine_->freeBatchID(batch_id).IsBatchCleanupDeferred());

    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
    EXPECT_TRUE(allEqual((char*)dst + kBigLen - kLen, kLen, 0x5A));
}
#else
// Event-driven waiters (Mooncake Store) block on the batch's condition
// variable and never poll, so the batch must be finished when submit returns.
TEST_F(HipTransportAsyncTest, SubmitFinishesBatchForEventDrivenWaiters) {
    constexpr size_t kBigLen = 256ull << 20;
    void* src = allocRegistered(kBigLen, 0, 0x5A);
    void* dst = allocRegistered(kBigLen, 0, 0);

    auto batch_id = engine_->allocateBatchID(1);
    ASSERT_TRUE(engine_
                    ->submitTransfer(batch_id, {request(TransferRequest::WRITE,
                                                        src, dst, kBigLen)})
                    .ok());
    EXPECT_TRUE(Transport::toBatchDesc(batch_id).is_finished.load(
        std::memory_order_acquire));
    EXPECT_TRUE(engine_->freeBatchID(batch_id).ok());
    EXPECT_TRUE(allEqual((char*)dst + kBigLen - kLen, kLen, 0x5A));
}
#endif

// Small WRITE and READ requests from buffers on two GPUs are grouped per
// device; every chunk must complete and every byte must be copied.
TEST_F(HipTransportAsyncTest, ManySmallCopiesAcrossDevices) {
    constexpr size_t kChunk = 16 * 1024, kChunks = 256;
    const int num_devices = std::min(device_count_, 2);
    std::vector<TransferRequest> entries;
    std::vector<std::pair<void*, uint8_t>> expected;
    for (int d = 0; d < num_devices; ++d) {
        const uint8_t value = 0x10 + d;
        void* local_src = allocRegistered(kChunk * kChunks, d, value);
        void* local_dst = allocRegistered(kChunk * kChunks, d, 0);
        void* remote = allocRegistered(kChunk * kChunks, device_count_ - 1, 0);
        void* remote_src =
            allocRegistered(kChunk * kChunks, device_count_ - 1, value);
        for (size_t off = 0; off < kChunk * kChunks; off += kChunk) {
            entries.push_back(request(TransferRequest::WRITE,
                                      (char*)local_src + off,
                                      (char*)remote + off, kChunk));
            entries.push_back(request(TransferRequest::READ,
                                      (char*)local_dst + off,
                                      (char*)remote_src + off, kChunk));
        }
        expected.emplace_back(remote, value);
        expected.emplace_back(local_dst, value);
    }

    auto batch_id = engine_->allocateBatchID(entries.size());
    ASSERT_TRUE(engine_->submitTransfer(batch_id, entries).ok());
    for (size_t i = 0; i < entries.size(); ++i) {
        ASSERT_EQ(wait(batch_id, i), TransferStatusEnum::COMPLETED) << i;
    }
    EXPECT_TRUE(engine_->freeBatchID(batch_id).ok());
    for (auto [ptr, value] : expected) {
        EXPECT_TRUE(allEqual(ptr, kChunk * kChunks, value));
    }
}

// If the kernel cannot be set up (here: descriptor memory cannot be
// allocated), the transport falls back to hipMemcpyAsync; the failed HIP call
// must not stay behind as the application's last error.
TEST(BatchCopyKernelTest, FailedSetupLeavesNoHipError) {
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    if (device_count < 1) GTEST_SKIP() << "Needs a GPU.";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    (void)cudaGetLastError();
    BatchCopyKernel kernel(/*num_slots=*/1 << 30, /*max_grid=*/64);
    EXPECT_FALSE(kernel.available(0));
    EXPECT_EQ(cudaGetLastError(), cudaSuccess);
}

// Pageable host memory is not GPU-addressable, so copies from or to it stay
// on hipMemcpyAsync even in a submission large enough for the batch copy
// kernel, which still takes the GPU-to-GPU copies next to them.
TEST_F(HipTransportAsyncTest, HostBuffersInBatchedSubmission) {
    constexpr size_t kChunk = 4096, kChunks = 64, kBytes = kChunk * kChunks;
    auto expected = randomBytes(kBytes, 3);
    std::vector<uint8_t> host_src(expected), host_back(kBytes, 0);
    void* gpu_src = allocRegistered(kBytes, 0, 0);
    void* gpu_dst = allocRegistered(kBytes, device_count_ - 1, 0);
    void* gpu_peer = allocRegistered(kBytes, device_count_ - 1, 0);
    ASSERT_EQ(cudaMemcpy(gpu_src, expected.data(), kBytes, cudaMemcpyDefault),
              cudaSuccess);

    // Host -> GPU and GPU -> GPU in one submission, then GPU -> host.
    std::vector<TransferRequest> writes, reads;
    for (size_t off = 0; off < kBytes; off += kChunk) {
        writes.push_back(request(TransferRequest::WRITE, host_src.data() + off,
                                 (char*)gpu_dst + off, kChunk));
        writes.push_back(request(TransferRequest::WRITE, (char*)gpu_src + off,
                                 (char*)gpu_peer + off, kChunk));
        reads.push_back(request(TransferRequest::READ, host_back.data() + off,
                                (char*)gpu_dst + off, kChunk));
    }
    for (auto* entries : {&writes, &reads}) {
        auto batch_id = engine_->allocateBatchID(entries->size());
        ASSERT_TRUE(engine_->submitTransfer(batch_id, *entries).ok());
        for (size_t i = 0; i < entries->size(); ++i) {
            ASSERT_EQ(wait(batch_id, i), TransferStatusEnum::COMPLETED) << i;
        }
        EXPECT_TRUE(engine_->freeBatchID(batch_id).ok());
    }
    EXPECT_TRUE(readBack(gpu_dst, kBytes) == expected);
    EXPECT_TRUE(readBack(gpu_peer, kBytes) == expected);
    EXPECT_TRUE(host_back == expected);
}

// One submission mixing copies the batch copy kernel takes (short ones of
// any alignment, aligned ones up to 1 MiB) with ones it leaves to
// hipMemcpyAsync (longer, or long and unaligned); every byte must land.
TEST_F(HipTransportAsyncTest, MixedSizesAndAlignments) {
    constexpr size_t kBytes = 40 << 20;
    const int peer = device_count_ - 1;
    auto expected = randomBytes(kBytes, 2);
    void* src = allocRegistered(kBytes, 0, 0);
    void* dst = allocRegistered(kBytes, peer, 0);
    void* back = allocRegistered(kBytes, 0, 0);
    ASSERT_EQ(cudaMemcpy(src, expected.data(), kBytes, cudaMemcpyDefault),
              cudaSuccess);

    std::vector<TransferRequest> writes, reads;
    size_t off = 1;  // every copy starts unaligned
    for (size_t len :
         {1, 7, 100, 4096, 5000, 2112, 64, 16384, 1 << 20, 3 << 20, 9216}) {
        for (int rep = 0; rep < 8; ++rep) {
            writes.push_back(request(TransferRequest::WRITE, (char*)src + off,
                                     (char*)dst + off, len));
            reads.push_back(request(TransferRequest::READ, (char*)back + off,
                                    (char*)dst + off, len));
            off += len;
            off += (off % 16 == 0) ? 0 : 16 - off % 16;  // next one aligned
            if (rep % 2) ++off;                          // ...or not
        }
    }
    ASSERT_LT(off, kBytes);
    for (auto* entries : {&writes, &reads}) {
        auto batch_id = engine_->allocateBatchID(entries->size());
        ASSERT_TRUE(engine_->submitTransfer(batch_id, *entries).ok());
        for (size_t i = 0; i < entries->size(); ++i) {
            ASSERT_EQ(wait(batch_id, i), TransferStatusEnum::COMPLETED) << i;
        }
        EXPECT_TRUE(engine_->freeBatchID(batch_id).ok());
    }
    auto copied = readBack(dst, kBytes);
    auto returned = readBack(back, kBytes);
    for (auto& r : writes) {
        size_t begin = (char*)r.source - (char*)src;
        ASSERT_TRUE(std::equal(expected.begin() + begin,
                               expected.begin() + begin + r.length,
                               copied.begin() + begin))
            << "write at " << begin << " length " << r.length;
        ASSERT_TRUE(std::equal(expected.begin() + begin,
                               expected.begin() + begin + r.length,
                               returned.begin() + begin))
            << "read at " << begin << " length " << r.length;
    }
}

// Batches submitted before any is polled may need more kernel launches than
// there are descriptor slots; submit must wait for the GPU, not for polls.
TEST_F(HipTransportAsyncTest, ManyBatchesBeforeFirstPoll) {
    constexpr size_t kChunk = 4096, kChunks = 32, kBatches = 48;
    const int peer = device_count_ - 1;
    void* src = allocRegistered(kChunk * kChunks * kBatches, 0, 0x6C);
    void* dst = allocRegistered(kChunk * kChunks * kBatches, peer, 0);
    std::vector<Transport::BatchID> batches;
    std::vector<std::vector<TransferRequest>> entries(kBatches);
    for (size_t b = 0; b < kBatches; ++b) {
        for (size_t c = 0; c < kChunks; ++c) {
            size_t off = (b * kChunks + c) * kChunk;
            entries[b].push_back(request(TransferRequest::WRITE,
                                         (char*)src + off, (char*)dst + off,
                                         kChunk));
        }
        batches.push_back(engine_->allocateBatchID(kChunks));
        ASSERT_TRUE(engine_->submitTransfer(batches[b], entries[b]).ok());
    }
    for (auto batch_id : batches) {
        for (size_t i = 0; i < kChunks; ++i) {
            ASSERT_EQ(wait(batch_id, i), TransferStatusEnum::COMPLETED);
        }
        EXPECT_TRUE(engine_->freeBatchID(batch_id).ok());
    }
    EXPECT_TRUE(allEqual(dst, kChunk * kChunks * kBatches, 0x6C));
}

// An unresolvable target fails the whole submission before anything is
// enqueued, and every task still reaches FAILED so the batch can be freed.
// A second engine opens this one as a remote segment so the address is
// relocated, and submits to its HIP transport directly because MultiTransport
// already rejects such a request while routing it.
TEST_F(HipTransportAsyncTest, UnregisteredTargetFailsEveryTask) {
    void* src = allocRegistered(kLen, 0, 0);
    void* dst = allocRegistered(kLen, 0, 0);
    void* unregistered = allocOnDevice(kLen, 0);

    TransferEngine peer(false);
    ASSERT_EQ(peer.init(P2PHANDSHAKE, "127.0.0.1:17815", "127.0.0.1", 17815),
              0);
    Transport* peer_transport = peer.installTransport("hip", nullptr);
    ASSERT_NE(peer_transport, nullptr);
    segment_id_ = peer.openSegment(engine_->getLocalIpAndPort());
    ASSERT_NE(segment_id_, LOCAL_SEGMENT_ID);

    auto good = request(TransferRequest::WRITE, src, dst, kLen);
    auto bad = request(TransferRequest::WRITE, src, unregistered, kLen);
    auto batch_id = peer.allocateBatchID(3);
    EXPECT_FALSE(
        peer_transport->submitTransfer(batch_id, {good, bad, good}).ok());
    for (size_t i = 0; i < 3; ++i) {
        TransferStatus status;
        ASSERT_TRUE(
            peer_transport->getTransferStatus(batch_id, i, status).ok());
        EXPECT_EQ(status.s, TransferStatusEnum::FAILED) << i;
    }
    EXPECT_TRUE(peer.freeBatchID(batch_id).ok());
    (void)cudaFree(unregistered);
}

// Two processes, so the target address goes through a real IPC handle. The
// child is this binary running only HipIpcTest.Target with
// MC_HIP_TEST_IPC_TARGET=<port>: it registers a GPU buffer filled with 0x5A
// and serves it until its stdin closes.
constexpr size_t kIpcLen = 4 << 20;

TEST(HipIpcTest, Target) {
    const char* port = getenv("MC_HIP_TEST_IPC_TARGET");
    if (!port) GTEST_SKIP() << "Runs as the child of TwoProcessCopies.";
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, std::string("127.0.0.1:") + port,
                          "127.0.0.1", atoi(port)),
              0);
    ASSERT_NE(engine.installTransport("hip", nullptr), nullptr);
    void* buf = allocOnDevice(kIpcLen, device_count - 1);
    ASSERT_EQ(cudaMemset(buf, 0x5A, kIpcLen), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
    ASSERT_EQ(engine.registerLocalMemory(
                  buf, kIpcLen, GPU_PREFIX + std::to_string(device_count - 1)),
              0);
    printf("\nIPC_TARGET_READY %s\n", engine.getLocalIpAndPort().c_str());
    fflush(stdout);
    char c;
    while (read(STDIN_FILENO, &c, 1) > 0) {
    }
    engine.unregisterLocalMemory(buf);
    (void)cudaFree(buf);
}

// The calling thread stays on a GPU other than the local buffer's, so the
// IPC handle must still be opened for the local buffer's GPU, which runs the
// copy. READ checks the child's data, WRITE then READ checks a round trip.
TEST(HipIpcTest, TwoProcessCopies) {
    if (getenv("MC_HIP_TEST_IPC_TARGET")) GTEST_SKIP();
    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    if (device_count < 2) GTEST_SKIP() << "Needs two GPUs.";

    int to_child[2], from_child[2];
    ASSERT_EQ(pipe(to_child), 0);
    ASSERT_EQ(pipe(from_child), 0);
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_adddup2(&actions, to_child[0], STDIN_FILENO);
    posix_spawn_file_actions_adddup2(&actions, from_child[1], STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&actions, to_child[1]);
    posix_spawn_file_actions_addclose(&actions, from_child[0]);
    // Ours come first, so they win over the same names in the environment.
    std::vector<std::string> env = {"MC_HIP_TEST_IPC_TARGET=17820",
                                    "GTEST_FILTER=HipIpcTest.Target"};
    for (char** e = environ; *e; ++e) env.emplace_back(*e);
    std::vector<char*> envp;
    for (auto& e : env) envp.push_back(e.data());
    envp.push_back(nullptr);
    char exe[] = "/proc/self/exe";
    char* argv[] = {exe, nullptr};
    pid_t pid;
    ASSERT_EQ(posix_spawn(&pid, exe, &actions, nullptr, argv, envp.data()), 0);
    posix_spawn_file_actions_destroy(&actions);
    close(to_child[0]);
    close(from_child[1]);

    std::string target;
    FILE* out = fdopen(from_child[0], "r");
    char line[512];
    while (target.empty() && fgets(line, sizeof(line), out)) {
        char addr[256];
        if (sscanf(line, "IPC_TARGET_READY %255s", addr) == 1) target = addr;
    }
    ASSERT_FALSE(target.empty()) << "child did not start";

    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:17821", "127.0.0.1", 17821),
              0);
    ASSERT_NE(engine.installTransport("hip", nullptr), nullptr);
    auto segment = engine.openSegment(target);
    ASSERT_GE(segment, 0);
    auto desc = engine.getMetadata()->getSegmentDescByID(segment);
    ASSERT_TRUE(desc && !desc->buffers.empty());
    const uint64_t remote = desc->buffers[0].addr;

    void* local = allocOnDevice(kIpcLen, 0);
    void* back = allocOnDevice(kIpcLen, 0);
    ASSERT_EQ(cudaMemset(local, 0x3C, kIpcLen), cudaSuccess);
    ASSERT_EQ(cudaMemset(back, 0, kIpcLen), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
    for (void* ptr : {local, back}) {
        ASSERT_EQ(engine.registerLocalMemory(ptr, kIpcLen, GPU_PREFIX + "0"),
                  0);
    }
    ASSERT_EQ(cudaSetDevice(1), cudaSuccess);

    auto copy = [&](TransferRequest::OpCode op, void* buf) {
        TransferRequest r;
        r.opcode = op;
        r.length = kIpcLen;
        r.source = buf;
        r.target_id = segment;
        r.target_offset = remote;
        auto batch_id = engine.allocateBatchID(1);
        EXPECT_TRUE(engine.submitTransfer(batch_id, {r}).ok());
        TransferStatus status;
        do {
            EXPECT_TRUE(engine.getTransferStatus(batch_id, 0, status).ok());
        } while (status.s == TransferStatusEnum::WAITING);
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
        EXPECT_TRUE(engine.freeBatchID(batch_id).ok());
    };
    auto all = [](void* ptr, uint8_t value) {
        std::vector<uint8_t> host(kIpcLen);
        EXPECT_EQ(cudaMemcpy(host.data(), ptr, kIpcLen, cudaMemcpyDefault),
                  cudaSuccess);
        return std::all_of(host.begin(), host.end(),
                           [value](uint8_t b) { return b == value; });
    };
    copy(TransferRequest::READ, back);
    EXPECT_TRUE(all(back, 0x5A));
    copy(TransferRequest::WRITE, local);
    copy(TransferRequest::READ, back);
    EXPECT_TRUE(all(back, 0x3C));
    int current = -1;
    EXPECT_EQ(cudaGetDevice(&current), cudaSuccess);
    EXPECT_EQ(current, 1);

    for (void* ptr : {local, back}) {
        engine.unregisterLocalMemory(ptr);
        (void)cudaFree(ptr);
    }
    close(to_child[1]);
    fclose(out);
    int wstatus = 0;
    ASSERT_EQ(waitpid(pid, &wstatus, 0), pid);
    EXPECT_TRUE(WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == 0);
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
