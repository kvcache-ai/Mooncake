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
#include <fcntl.h>
#include <poll.h>

#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <thread>
#include <sstream>
#include <iterator>
#include <cstdint>
#include <cstddef>
#include <csignal>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cuda_alike.h"
#include "common/serialization.h"
#include "multi_transport.h"
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include "transfer_metadata.h"  // P2PHANDSHAKE
#include "transfer_metadata_plugin.h"
#include "transport/hip_transport/hip_transport.h"
#include "transport/transport.h"

extern char** environ;

namespace mooncake {
class HipTransportTestPeer {
   public:
    static hipStream_t nextStream(HipTransport* transport, int device) {
        return transport->stream_pool_.getNextStream(device);
    }

    static std::shared_ptr<TransferMetadata> Configure(
        HipTransport& transport) {
        auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
        segment->name = "local";
        segment->protocol = "hip";
        EXPECT_EQ(metadata->addLocalSegment(LOCAL_SEGMENT_ID, "local",
                                            std::move(segment)),
                  0);
        transport.meta() = metadata;
        transport.use_fabric_mem_ = true;
        return metadata;
    }

    static void SetMetadata(HipTransport& transport,
                            std::shared_ptr<TransferMetadata> metadata) {
        transport.meta() = std::move(metadata);
        transport.use_fabric_mem_ = true;
    }

    static void Add(HipTransport& transport, void* addr, void* base) {
        transport.registered_addr_to_base_[(uint64_t)addr] = (uint64_t)base;
        ++transport.registered_base_refs_[(uint64_t)base];
    }

    static int Unregister(HipTransport& transport, void* addr) {
        return transport.unregisterLocalMemory(addr, true);
    }
};

class TransferEngineImplTestPeer {
   public:
    static void ReplaceTransport(TransferEngineImpl& engine,
                                 std::shared_ptr<Transport> transport) {
        engine.multi_transports_->transport_map_.clear();
        engine.multi_transports_->transport_map_.emplace("hip",
                                                         std::move(transport));
    }

    static void AddTransport(TransferEngineImpl& engine,
                             std::shared_ptr<Transport> transport) {
        engine.multi_transports_->transport_map_.emplace("other",
                                                         std::move(transport));
    }

    static void SeedRegion(TransferEngineImpl& engine, void* addr,
                           size_t length) {
        std::unique_lock<std::shared_mutex> lock(engine.mutex_);
        engine.insertMemoryRegionLocked(
            TransferEngineImpl::MemoryRegion{addr, length, "gpu:0", true});
    }

    static bool HasRegion(TransferEngineImpl& engine, void* addr) {
        std::shared_lock<std::shared_mutex> lock(engine.mutex_);
        return engine.local_memory_regions_.count((uint64_t)addr) != 0;
    }
};
}  // namespace mooncake

using namespace mooncake;

TEST(HipTransportTest, FabricUnregisterRequiresOriginalRegistration) {
    HipTransport transport;
    auto metadata = HipTransportTestPeer::Configure(transport);
    constexpr uint64_t base = 0x100000;
    auto* first = reinterpret_cast<void*>(base + 0x100);
    auto* second = reinterpret_cast<void*>(base + 0x200);
    auto* never_registered = reinterpret_cast<void*>(base + 0x300);
    TransferMetadata::BufferDesc desc;
    desc.addr = base;
    desc.length = 0x1000;
    desc.shm_name = "fabric-handle";
#ifdef ENABLE_MULTI_PROTOCOL
    desc.protocol = "hip";
#endif
    ASSERT_EQ(metadata->addLocalMemoryBuffer(desc, false), 0);
    for (void* addr : {first, second})
        HipTransportTestPeer::Add(transport, addr,
                                  reinterpret_cast<void*>(base));
    auto count = [&] {
        return metadata->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.size();
    };
    ASSERT_EQ(count(), 1U);
    EXPECT_EQ(HipTransportTestPeer::Unregister(transport, never_registered),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(count(), 1U);
    EXPECT_EQ(HipTransportTestPeer::Unregister(transport, first), 0);
    EXPECT_EQ(count(), 1U);
    EXPECT_EQ(HipTransportTestPeer::Unregister(transport, first),
              ERR_ADDRESS_NOT_REGISTERED);
    EXPECT_EQ(count(), 1U);
    EXPECT_EQ(HipTransportTestPeer::Unregister(transport, second), 0);
    EXPECT_EQ(count(), 0U);
}

namespace {
class FailOnceMetadataStorage : public MetadataStoragePlugin {
   public:
    explicit FailOnceMetadataStorage(int failures = 1)
        : failures_left(failures) {}
    bool get(const std::string&, Json::Value&) override { return false; }
    bool set(const std::string&, const Json::Value&) override {
        ++set_calls;
        if (failures_left > 0) {
            --failures_left;
            return false;
        }
        return true;
    }
    bool remove(const std::string&) override { return true; }

    int failures_left;
    int set_calls = 0;
};

class SuccessfulCleanupTransport : public Transport {
   public:
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }
    Status getTransferStatus(BatchID, size_t, TransferStatus&) override {
        return Status::OK();
    }

   private:
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void*, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }
    const char* getName() const override { return "other"; }
};

class MetadataWithStorage : public TransferMetadata {
   public:
    explicit MetadataWithStorage(std::shared_ptr<MetadataStoragePlugin> storage)
        : TransferMetadata(std::move(storage)) {}
};

std::shared_ptr<MetadataWithStorage> ConfigurePendingCleanup(
    HipTransport& transport, void* addr, uint64_t base,
    std::shared_ptr<FailOnceMetadataStorage> storage) {
    auto metadata = std::make_shared<MetadataWithStorage>(storage);
    auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
    segment->name = "local";
    segment->protocol = "hip";
    EXPECT_EQ(metadata->addLocalSegment(LOCAL_SEGMENT_ID, "local",
                                        std::move(segment)),
              0);
    HipTransportTestPeer::SetMetadata(transport, metadata);
    TransferMetadata::BufferDesc desc;
    desc.addr = base;
    desc.length = 0x1000;
    desc.shm_name = "fabric-handle";
#ifdef ENABLE_MULTI_PROTOCOL
    desc.protocol = "hip";
#endif
    EXPECT_EQ(metadata->addLocalMemoryBuffer(desc, false), 0);
    HipTransportTestPeer::Add(transport, addr, (void*)base);
    return metadata;
}
}  // namespace

TEST(HipTransportTest, MetadataPublicationFailureRetriesThroughEngine) {
    constexpr uint64_t base = 0x300000;
    auto* addr = reinterpret_cast<void*>(base + 0x100);
    auto transport = std::make_shared<HipTransport>();
    auto storage = std::make_shared<FailOnceMetadataStorage>();
    auto metadata = ConfigurePendingCleanup(*transport, addr, base, storage);
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    TransferEngineImplTestPeer::ReplaceTransport(engine, transport);
    TransferEngineImplTestPeer::SeedRegion(engine, addr, 0x100);

    EXPECT_EQ(engine.unregisterLocalMemory(addr), ERR_METADATA);
    EXPECT_TRUE(TransferEngineImplTestPeer::HasRegion(engine, addr));
    EXPECT_TRUE(transport->hasPendingUnregistration(addr));
    EXPECT_EQ(engine.unregisterLocalMemory(addr), 0);
    EXPECT_FALSE(TransferEngineImplTestPeer::HasRegion(engine, addr));
    EXPECT_FALSE(transport->hasPendingUnregistration(addr));
    EXPECT_EQ(storage->set_calls, 2);
    EXPECT_TRUE(
        metadata->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
}

TEST(HipTransportTest, BatchMetadataFailureRetriesThroughEngine) {
    constexpr uint64_t base = 0x400000;
    auto* addr = reinterpret_cast<void*>(base + 0x100);
    auto transport = std::make_shared<HipTransport>();
    auto storage = std::make_shared<FailOnceMetadataStorage>();
    auto metadata = ConfigurePendingCleanup(*transport, addr, base, storage);
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    TransferEngineImplTestPeer::ReplaceTransport(engine, transport);
    TransferEngineImplTestPeer::SeedRegion(engine, addr, 0x100);

    EXPECT_EQ(engine.unregisterLocalMemoryBatch({addr}), ERR_METADATA);
    EXPECT_TRUE(TransferEngineImplTestPeer::HasRegion(engine, addr));
    EXPECT_TRUE(transport->hasPendingUnregistration(addr));
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({addr}), 0);
    EXPECT_FALSE(TransferEngineImplTestPeer::HasRegion(engine, addr));
    EXPECT_FALSE(transport->hasPendingUnregistration(addr));
    EXPECT_EQ(storage->set_calls, 2);
    EXPECT_TRUE(
        metadata->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
}

TEST(HipTransportTest, BatchRetryWithMultipleAliasesKeepsReservations) {
    constexpr uint64_t base = 0x500000;
    auto* first = reinterpret_cast<void*>(base + 0x100);
    auto* second = reinterpret_cast<void*>(base + 0x200);
    auto transport = std::make_shared<HipTransport>();
    auto storage = std::make_shared<FailOnceMetadataStorage>(2);
    auto metadata = ConfigurePendingCleanup(*transport, first, base, storage);
    HipTransportTestPeer::Add(*transport, second, (void*)base);
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12345"), 0);
    TransferEngineImplTestPeer::ReplaceTransport(engine, transport);
    TransferEngineImplTestPeer::AddTransport(
        engine, std::make_shared<SuccessfulCleanupTransport>());
    TransferEngineImplTestPeer::SeedRegion(engine, first, 0x80);
    TransferEngineImplTestPeer::SeedRegion(engine, second, 0x80);

    for (int attempt = 0; attempt < 2; ++attempt) {
        EXPECT_EQ(engine.unregisterLocalMemoryBatch({first, second}),
                  ERR_METADATA);
        EXPECT_TRUE(TransferEngineImplTestPeer::HasRegion(engine, first));
        EXPECT_TRUE(TransferEngineImplTestPeer::HasRegion(engine, second));
        EXPECT_TRUE(transport->hasPendingUnregistration(second));
    }
    EXPECT_EQ(engine.unregisterLocalMemoryBatch({first, second}), 0);
    EXPECT_FALSE(TransferEngineImplTestPeer::HasRegion(engine, first));
    EXPECT_FALSE(TransferEngineImplTestPeer::HasRegion(engine, second));
    EXPECT_FALSE(transport->hasPendingUnregistration(second));
    EXPECT_EQ(storage->set_calls, 3);
    EXPECT_TRUE(
        metadata->getSegmentDescByID(LOCAL_SEGMENT_ID)->buffers.empty());
}

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
}  // namespace

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
// Holds every copy stream of a device behind a GPU-side wait on a host flag
// until release() or destruction, so a submitted copy cannot finish early.
class StreamGate {
   public:
    StreamGate(Transport* transport, int device) {
        int supported = 0;
        if (hipDeviceGetAttribute(&supported,
                                  hipDeviceAttributeCanUseStreamWaitValue,
                                  device) != hipSuccess ||
            !supported) {
            return;
        }
        if (hipExtMallocWithFlags(reinterpret_cast<void**>(&flag_),
                                  sizeof(*flag_),
                                  hipMallocSignalMemory) != hipSuccess) {
            flag_ = nullptr;
            return;
        }
        *flag_ = 0;
        // The pool hands out its streams round robin: gate each one until it
        // comes around again.
        auto* hip = static_cast<HipTransport*>(transport);
        hipStream_t first = HipTransportTestPeer::nextStream(hip, device);
        hipStream_t stream = first;
        do {
            if (stream == nullptr ||
                hipStreamWaitValue64(stream, flag_, 1, hipStreamWaitValueEq) !=
                    hipSuccess) {
                release();
                return;
            }
            stream = HipTransportTestPeer::nextStream(hip, device);
        } while (stream != first);
        armed_ = true;
    }
    ~StreamGate() { release(); }

    bool armed() const { return armed_; }

    void release() {
        if (flag_ == nullptr) return;
        __atomic_store_n(flag_, uint64_t{1}, __ATOMIC_RELEASE);
        (void)hipDeviceSynchronize();
        (void)hipFree(flag_);
        flag_ = nullptr;
    }

   private:
    uint64_t* flag_ = nullptr;
    bool armed_ = false;
};

// submitTransfer only enqueues the copy: while the copy streams are held,
// the task stays WAITING and freeBatchID defers cleanup; once they are let
// go, the copy lands.
TEST_F(HipTransportAsyncTest, SubmitDoesNotWaitForCopy) {
    void* src = allocRegistered(kLen, 0, 0x5A);
    void* dst = allocRegistered(kLen, 0, 0);

    StreamGate gate(engine_->getTransport("hip"), 0);
    if (!gate.armed()) GTEST_SKIP() << "hipStreamWaitValue64 unsupported.";
    auto batch_id = engine_->allocateBatchID(1);
    ASSERT_TRUE(engine_
                    ->submitTransfer(batch_id, {request(TransferRequest::WRITE,
                                                        src, dst, kLen)})
                    .ok());
    TransferStatus status;
    ASSERT_TRUE(engine_->getTransferStatus(batch_id, 0, status).ok());
    EXPECT_EQ(status.s, TransferStatusEnum::WAITING);
    EXPECT_TRUE(engine_->freeBatchID(batch_id).IsBatchCleanupDeferred());

    gate.release();
    EXPECT_TRUE(allEqual(dst, kLen, 0x5A));
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
    // Inherited GoogleTest settings are dropped: under sharding the child's
    // only test may belong to another shard, and it must not write the
    // parent's report.
    std::vector<std::string> env = {"MC_HIP_TEST_IPC_TARGET=17820",
                                    "GTEST_FILTER=HipIpcTest.Target"};
    for (char** e = environ; *e; ++e) {
        if (strncmp(*e, "GTEST_", 6) != 0) env.emplace_back(*e);
    }
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
    // Read the child's output to EOF: it still prints GoogleTest results
    // after its stdin closes, and closing the pipe first could SIGPIPE it.
    close(to_child[1]);
    while (fgets(line, sizeof(line), out)) {
    }
    fclose(out);
    int wstatus = 0;
    ASSERT_EQ(waitpid(pid, &wstatus, 0), pid);
    EXPECT_TRUE(WIFEXITED(wstatus) && WEXITSTATUS(wstatus) == 0);
}

// hipIpcOpenMemHandle maps the whole allocation that holds a registered
// buffer, at the allocation's base. Transfers to and from a buffer that starts
// inside a larger allocation (as caching allocators produce) must still hit the
// buffer, not the allocation base.
//
// The destination runs in a second process, since transfers to this process's
// own segment use LOCAL_SEGMENT_ID, which bypasses relocation and the IPC path.
// It is this binary re-executed with --hip_ipc_dst, not a fork, since the
// parent may already have initialized HIP. It registers three buffers in one
// zeroed allocation and reports their addresses; the parent writes two of them
// (one of them twice, which takes the cached-mapping path) and reads the third,
// closes its mappings, and then the destination checks every byte of the
// allocation.

DEFINE_bool(hip_ipc_dst, false, "Internal: run as the IPC destination.");
DEFINE_int32(hip_ipc_dst_device, 0,
             "Device ordinal, among the ones the IPC destination sees, that it "
             "allocates its buffers on.");
DEFINE_string(hip_ipc_dst_visible_devices, "",
              "If set, HIP_VISIBLE_DEVICES for the IPC destination process.");
DEFINE_int32(hip_ipc_expect_devices, 0,
             "If set, the number of devices this process must see.");
DEFINE_int32(hip_ipc_dst_expect_devices, 0,
             "If set, the number of devices the IPC destination must see.");
DEFINE_string(
    hip_ipc_rerun_marker, "",
    "If set, the IPC transfer test creates this file when it passes.");
DEFINE_bool(hip_ipc_legacy_owner_handle, false,
            "Internal: publish an older-style HIP IPC owner ordinal.");

namespace {
constexpr size_t kAllocLen = 4 * 1024 * 1024;
constexpr size_t kSliceLen = 64 * 1024;
// Offset 0 keeps the legacy handle-only payload; 512-byte alignment is what
// PyTorch's caching allocator produces.
constexpr size_t kSliceOffsets[] = {0, 1024 * 1024 + 512,
                                    2 * 1024 * 1024 + 4096};
constexpr unsigned char kWritten0 = 0xA0;
constexpr unsigned char kWritten1First = 0xB1;
constexpr unsigned char kWritten1 = 0xB2;
constexpr unsigned char kReadSource = 0x5C;
constexpr char kDstMarker[] = "HIP_IPC_DST ";
constexpr int kDstOk = 0, kDstWrongBytes = 1, kDstRegisterFailed = 2,
              kDstSetupFailed = 3, kDstNoGo = 4;

const char* describeDstExit(int rc) {
    switch (rc) {
        case kDstOk:
            return "ok";
        case kDstWrongBytes:
            return "a transfer reached the wrong bytes of the destination "
                   "allocation (its stderr shows the first one)";
        case kDstRegisterFailed:
            return "the destination could not register its buffers";
        case kDstSetupFailed:
            return "the destination failed to set up (see its stderr)";
        case kDstNoGo:
            return "the parent stopped before the check";
        default:
            return "the destination crashed or was killed";
    }
}

int dstFailed(int rc, const char* what) {
    fprintf(stderr, "IPC destination: %s failed\n", what);
    return rc;
}

unsigned char expectedDstByte(size_t i) {
    for (size_t s = 0; s < std::size(kSliceOffsets); ++s) {
        if (i >= kSliceOffsets[s] && i < kSliceOffsets[s] + kSliceLen)
            return s == 0 ? kWritten0 : s == 1 ? kWritten1 : kReadSource;
    }
    return 0;
}

int runIpcDestination() {
    int device_count = 0;
    if (FLAGS_hip_ipc_expect_devices > 0 &&
        (cudaGetDeviceCount(&device_count) != cudaSuccess ||
         device_count != FLAGS_hip_ipc_expect_devices))
        return dstFailed(kDstSetupFailed, "the visible device count check");
    auto engine = std::make_unique<TransferEngine>(false);
    // P2PHANDSHAKE binds a free port (the one given here is ignored); the
    // address it really uses is reported below.
    if (engine->init(P2PHANDSHAKE, "127.0.0.1:17815", "127.0.0.1", 17815) != 0)
        return dstFailed(kDstSetupFailed, "TransferEngine::init");
    if (engine->installTransport("hip", nullptr) == nullptr)
        return dstFailed(kDstSetupFailed, "installTransport");

    char* alloc = nullptr;
    const int device = FLAGS_hip_ipc_dst_device;
    if (cudaSetDevice(device) != cudaSuccess ||
        cudaMalloc(reinterpret_cast<void**>(&alloc), kAllocLen) !=
            cudaSuccess ||
        cudaMemset(alloc, 0, kAllocLen) != cudaSuccess ||
        cudaMemset(alloc + kSliceOffsets[2], kReadSource, kSliceLen) !=
            cudaSuccess ||
        cudaDeviceSynchronize() != cudaSuccess)
        return dstFailed(kDstSetupFailed, "preparing the allocation");
    std::string report = kDstMarker + engine->getLocalIpAndPort();
    for (size_t off : kSliceOffsets) {
        if (engine->registerLocalMemory(alloc + off, kSliceLen,
                                        GPU_PREFIX + std::to_string(device)) !=
            0)
            return dstFailed(kDstRegisterFailed, "registerLocalMemory");
        report +=
            " " + std::to_string(reinterpret_cast<uintptr_t>(alloc + off));
    }
    if (FLAGS_hip_ipc_legacy_owner_handle) {
        auto metadata = engine->getMetadata();
        auto local = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!local || local->buffers.empty())
            return dstFailed(kDstRegisterFailed, "legacy IPC descriptor");
        for (auto& buffer : local->buffers) {
            std::vector<unsigned char> handle;
            deserializeBinaryData(buffer.shm_name, handle);
            if (handle.size() != sizeof(hipIpcMemHandle_t))
                return dstFailed(kDstRegisterFailed, "legacy IPC handle size");
            // ROCclr LP64's owner ordinal is at byte 52. A device-1 export
            // from an older exporter would carry this value.
            constexpr int older_owner_ordinal = 1;
            memcpy(handle.data() + 52, &older_owner_ordinal,
                   sizeof(older_owner_ordinal));
            buffer.shm_name = serializeBinaryData(handle.data(), handle.size());
        }
        if (metadata->updateLocalSegmentDesc() != 0)
            return dstFailed(kDstRegisterFailed, "legacy IPC publish");
    }
    printf("%s\n", report.c_str());
    fflush(stdout);

    char go = 0;
    ssize_t n;
    do {
        n = read(STDIN_FILENO, &go, 1);
    } while (n < 0 && errno == EINTR);
    if (n != 1) return kDstNoGo;

    std::vector<unsigned char> host(kAllocLen);
    if (cudaMemcpy(host.data(), alloc, kAllocLen, cudaMemcpyDeviceToHost) !=
        cudaSuccess)
        return dstFailed(kDstSetupFailed, "copying the allocation back");
    int rc = kDstOk;
    for (size_t i = 0; i < kAllocLen; ++i) {
        if (host[i] != expectedDstByte(i)) {
            fprintf(stderr, "destination byte %zu is 0x%02x, expected 0x%02x\n",
                    i, host[i], expectedDstByte(i));
            rc = kDstWrongBytes;
            break;
        }
    }
    for (size_t off : kSliceOffsets) engine->unregisterLocalMemory(alloc + off);
    engine.reset();
    (void)cudaFree(alloc);
    return rc;
}

// A copy of this process's environment with `name` set to `value` (or
// removed, for nullptr).
std::vector<std::string> environWith(const std::string& name,
                                     const char* value) {
    std::vector<std::string> env;
    for (char** e = environ; *e != nullptr; ++e) {
        if (std::string(*e).rfind(name + "=", 0) != 0) env.emplace_back(*e);
    }
    if (value != nullptr) env.push_back(name + "=" + value);
    return env;
}

std::vector<char*> cStrings(std::vector<std::string>& strings) {
    std::vector<char*> out;
    for (auto& s : strings) out.push_back(s.data());
    out.push_back(nullptr);
    return out;
}

// Owns the destination process and reaps it when destroyed.
class DestinationProcess {
   public:
    bool start() {
        int to_child[2], from_child[2];
        if (pipe(to_child) != 0) return false;
        if (pipe(from_child) != 0) {
            close(to_child[0]);
            close(to_child[1]);
            return false;
        }
        char exe[] = "/proc/self/exe";
        std::vector<std::string> args = {
            exe, "--hip_ipc_dst",
            "--hip_ipc_dst_device=" + std::to_string(FLAGS_hip_ipc_dst_device),
            "--hip_ipc_expect_devices=" +
                std::to_string(FLAGS_hip_ipc_dst_expect_devices)};
        auto argv = cStrings(args);
        std::vector<std::string> env_strings =
            environWith("HIP_VISIBLE_DEVICES",
                        FLAGS_hip_ipc_dst_visible_devices.empty()
                            ? getenv("HIP_VISIBLE_DEVICES")
                            : FLAGS_hip_ipc_dst_visible_devices.c_str());
        auto envp = cStrings(env_strings);
        posix_spawn_file_actions_t actions;
        int rc = posix_spawn_file_actions_init(&actions);
        if (rc == 0) {
            rc = posix_spawn_file_actions_adddup2(&actions, to_child[0],
                                                  STDIN_FILENO);
            if (rc == 0)
                rc = posix_spawn_file_actions_adddup2(&actions, from_child[1],
                                                      STDOUT_FILENO);
            // A pipe end can itself be fd 0 or 1 if the parent ran with stdio
            // closed; those were just replaced by dup2 and must stay open.
            for (int fd :
                 {to_child[0], to_child[1], from_child[0], from_child[1]}) {
                if (rc == 0 && fd > STDERR_FILENO)
                    rc = posix_spawn_file_actions_addclose(&actions, fd);
            }
            if (rc == 0)
                rc = posix_spawn(&pid_, exe, &actions, nullptr, argv.data(),
                                 envp.data());
            posix_spawn_file_actions_destroy(&actions);
        }
        close(to_child[0]);
        close(from_child[1]);
        if (rc != 0) {
            close(to_child[1]);
            close(from_child[0]);
            pid_ = -1;
            return false;
        }
        to_child_ = to_child[1];
        from_child_ = from_child[0];
        return true;
    }

    // Reads the destination's report line: handshake name, then addresses.
    bool readReport(std::string* name, std::vector<uint64_t>* addrs) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(60);
        std::string line;
        char c;
        while (true) {
            const auto left =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - std::chrono::steady_clock::now())
                    .count();
            if (left <= 0) return false;
            pollfd pfd{from_child_, POLLIN, 0};
            const int ready = poll(&pfd, 1, static_cast<int>(left));
            if (ready < 0 && errno == EINTR) continue;
            if (ready <= 0) return false;
            const ssize_t n = read(from_child_, &c, 1);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            if (c != '\n') {
                line.push_back(c);
                continue;
            }
            if (line.rfind(kDstMarker, 0) == 0) break;
            line.clear();  // other output the engine wrote to stdout
        }
        std::istringstream in(line.substr(sizeof(kDstMarker) - 1));
        in >> *name;
        uint64_t addr;
        while (in >> addr) addrs->push_back(addr);
        reported_ = true;
        return !name->empty();
    }

    // Tells the destination to check its memory (only if it reported) and
    // returns its exit code, or -1 if it had to be killed.
    int finish() {
        if (pid_ < 0) return -1;
        if (to_child_ >= 0) {
            if (reported_) {
                const char go = 1;
                while (write(to_child_, &go, 1) < 0 && errno == EINTR) {
                }
            }
            close(to_child_);
            to_child_ = -1;
        }
        int status = 0;
        for (int i = 0; i < 600; ++i) {  // up to 60 s
            if (waitpid(pid_, &status, WNOHANG) == pid_) {
                pid_ = -1;
                return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
            }
            usleep(100 * 1000);
        }
        kill(pid_, SIGKILL);
        while (waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
        }
        pid_ = -1;
        return -1;
    }

    ~DestinationProcess() {
        reported_ = false;  // an early return must not trigger the check
        finish();
        if (from_child_ >= 0) close(from_child_);
    }

   private:
    pid_t pid_ = -1;
    int to_child_ = -1;
    int from_child_ = -1;
    bool reported_ = false;
};

// Sets an environment variable for the lifetime of the object.
class ScopedEnv {
   public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        const char* old = getenv(name);
        had_old_ = old != nullptr;
        if (had_old_) old_ = old;
        setenv(name, value, 1);
    }
    ~ScopedEnv() {
        if (had_old_)
            setenv(name_, old_.c_str(), 1);
        else
            unsetenv(name_);
    }

   private:
    const char* name_;
    std::string old_;
    bool had_old_ = false;
};

bool runTransfer(TransferEngine* engine, TransferRequest::OpCode opcode,
                 void* local, Transport::SegmentID segment, uint64_t remote) {
    auto batch_id = engine->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = opcode;
    entry.length = kSliceLen;
    entry.source = local;
    entry.target_id = segment;
    entry.target_offset = remote;
    bool ok = engine->submitTransfer(batch_id, {entry}).ok();
    TransferStatus status;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (ok) {
        ok = engine->getTransferStatus(batch_id, 0, status).ok();
        if (!ok || status.s != TransferStatusEnum::WAITING) break;
        if (std::chrono::steady_clock::now() > deadline) ok = false;
    }
    ok = ok && status.s == TransferStatusEnum::COMPLETED;
    const bool freed = engine->freeBatchID(batch_id).ok();
    return ok && freed;
}
}  // namespace

TEST(HipTransportTest, IpcTransfersHitBuffersInsideLargerAllocation) {
    int device_count = 0;
    const auto count_err = cudaGetDeviceCount(&device_count);
    if (count_err == cudaErrorNoDevice ||
        count_err == cudaErrorInsufficientDriver ||
        (count_err == cudaSuccess && device_count < 1))
        GTEST_SKIP() << "Needs a GPU.";
    ASSERT_EQ(count_err, cudaSuccess);
    if (FLAGS_hip_ipc_expect_devices > 0) {
        ASSERT_EQ(device_count, FLAGS_hip_ipc_expect_devices);
    }
    // MC_USE_HIP_IPC=0 or MC_USE_NVLINK_IPC=0 selects fabric handles; both
    // processes must use IPC.
    ScopedEnv hip_ipc("MC_USE_HIP_IPC", "1");
    ScopedEnv nvlink_ipc("MC_USE_NVLINK_IPC", "1");
    DestinationProcess dst;
    ASSERT_TRUE(dst.start());
    // The destination may exit before reading the go byte.
    struct IgnoreSigpipe {
        void (*previous)(int) = signal(SIGPIPE, SIG_IGN);
        ~IgnoreSigpipe() { signal(SIGPIPE, previous); }
    } ignore_sigpipe;
    std::string dst_name;
    std::vector<uint64_t> dst_addrs;
    if (!dst.readReport(&dst_name, &dst_addrs)) {
        const int rc = dst.finish();
        FAIL() << "IPC destination did not report: " << describeDstExit(rc)
               << " (exit " << rc << ")";
    }
    ASSERT_EQ(dst_addrs.size(), std::size(kSliceOffsets));

    auto engine = std::make_unique<TransferEngine>(false);
    // As in the destination, the port is ignored in P2PHANDSHAKE mode.
    ASSERT_EQ(engine->init(P2PHANDSHAKE, "127.0.0.1:17816", "127.0.0.1", 17816),
              0);
    ASSERT_NE(engine->installTransport("hip", nullptr), nullptr);
    // The source buffer is on another GPU than the destination's when one can
    // reach it, and is registered from a thread that has never touched the
    // GPU runtime, so registration cannot rely on the caller's current device
    // or context.
    int local_device = 0;
    for (int d = device_count - 1; d > 0; --d) {
        int can_access = 0;
        if (cudaDeviceCanAccessPeer(&can_access, d, 0) == cudaSuccess &&
            can_access) {
            local_device = d;
            break;
        }
    }
    const std::string local_location =
        GPU_PREFIX + std::to_string(local_device);
    ASSERT_EQ(cudaSetDevice(local_device), cudaSuccess);
    void* local = nullptr;
    ASSERT_EQ(cudaMalloc(&local, kSliceLen), cudaSuccess);
    char* past_end = static_cast<char*>(local) + 512;
    int past_end_rc = -1, wrap_rc = -1, local_rc = -1;
    bool past_end_unpublished = false, wrap_unpublished = false;
    std::thread([&] {
        // With only HIP installed, unsupported ranges fail registration and
        // publish no IPC buffer.
        past_end_rc =
            engine->registerLocalMemory(past_end, kAllocLen, local_location);
        auto metadata = engine->getMetadata();
        auto desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        past_end_unpublished = desc && desc->buffers.empty();
        wrap_rc = engine->registerLocalMemory(past_end, SIZE_MAX - 256,
                                              local_location);
        desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        wrap_unpublished = desc && desc->buffers.empty();
        local_rc =
            engine->registerLocalMemory(local, kSliceLen, local_location);
    }).join();
    EXPECT_EQ(past_end_rc, ERR_INVALID_ARGUMENT);
    EXPECT_EQ(wrap_rc, ERR_INVALID_ARGUMENT);
    EXPECT_TRUE(past_end_unpublished);
    EXPECT_TRUE(wrap_unpublished);
    ASSERT_EQ(local_rc, 0);
    auto segment = engine->openSegment(dst_name);
    ASSERT_NE(segment, static_cast<Transport::SegmentID>(-1));

    auto fill = [&](unsigned char value) {
        return cudaMemset(local, value, kSliceLen) == cudaSuccess &&
               cudaDeviceSynchronize() == cudaSuccess;
    };
    ASSERT_TRUE(fill(kWritten0));
    ASSERT_TRUE(runTransfer(engine.get(), TransferRequest::WRITE, local,
                            segment, dst_addrs[0]));
    ASSERT_TRUE(fill(kWritten1First));
    ASSERT_TRUE(runTransfer(engine.get(), TransferRequest::WRITE, local,
                            segment, dst_addrs[1]));
    // Second write to the same buffer: takes the cached mapping.
    ASSERT_TRUE(fill(kWritten1));
    ASSERT_TRUE(runTransfer(engine.get(), TransferRequest::WRITE, local,
                            segment, dst_addrs[1]));
    ASSERT_TRUE(fill(0));
    ASSERT_TRUE(runTransfer(engine.get(), TransferRequest::READ, local, segment,
                            dst_addrs[2]));
    std::vector<unsigned char> read_back(kSliceLen);
    ASSERT_EQ(
        cudaMemcpy(read_back.data(), local, kSliceLen, cudaMemcpyDeviceToHost),
        cudaSuccess);
    EXPECT_EQ(std::count(read_back.begin(), read_back.end(), kReadSource),
              static_cast<std::ptrdiff_t>(kSliceLen))
        << "the READ did not come from the registered buffer";

    // Close this process's mappings of the destination's memory before the
    // destination frees it.
    EXPECT_EQ(engine->unregisterLocalMemory(local), 0);
    engine.reset();
    (void)cudaFree(local);
    const int dst_rc = dst.finish();
    EXPECT_EQ(dst_rc, kDstOk) << describeDstExit(dst_rc);
    if (!FLAGS_hip_ipc_rerun_marker.empty() && !HasFailure())
        close(
            open(FLAGS_hip_ipc_rerun_marker.c_str(), O_CREAT | O_WRONLY, 0600));
}

// Same-node peers need not see the same devices. This reruns the transfer test
// with the destination seeing two devices and allocating on the second, while
// the source sees only the first. The exporter must stamp the handle with a
// device ordinal the source can open, without the private-layout workaround.
void runCrossVisibleSetIpc(bool legacy_handle) {
    ScopedEnv workaround("MC_HIP_IPC_OWNER_DEVICE_WORKAROUND",
                         legacy_handle ? "1" : "0");
    if (!FLAGS_hip_ipc_rerun_marker.empty())
        GTEST_SKIP() << "Already running as the restricted rerun.";
    int device_count = 0;
    const auto count_err = cudaGetDeviceCount(&device_count);
    if (count_err == cudaErrorNoDevice ||
        count_err == cudaErrorInsufficientDriver ||
        (count_err == cudaSuccess && device_count < 2))
        GTEST_SKIP() << "Needs >= 2 GPUs.";
    ASSERT_EQ(count_err, cudaSuccess);
    // The children's device sets are written as HIP_VISIBLE_DEVICES indices
    // into what ROCr exposes, which only matches this process's ordinals
    // when HIP applies no filter of its own.
    for (const char* name : {"HIP_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES"}) {
        const char* value = getenv(name);
        if (value != nullptr && *value != '\0')
            GTEST_SKIP() << name << " is set; this test sets it itself, so "
                         << "restrict GPUs with ROCR_VISIBLE_DEVICES instead.";
    }
    // The source's only device must reach the destination's second one.
    int peer = -1;
    for (int d = 1; d < device_count && peer < 0; ++d) {
        int can_access = 0;
        ASSERT_EQ(cudaDeviceCanAccessPeer(&can_access, 0, d), cudaSuccess);
        if (can_access) peer = d;
    }
    if (peer < 0) GTEST_SKIP() << "No GPU has peer access to GPU 0.";

    char marker[] = "/tmp/hip_ipc_rerun_XXXXXX";
    const int marker_fd = mkstemp(marker);
    ASSERT_GE(marker_fd, 0);
    close(marker_fd);
    unlink(marker);
    std::vector<std::string> args = {
        "/proc/self/exe",
        "--hip_ipc_dst_device=1",
        "--hip_ipc_dst_visible_devices=0," + std::to_string(peer),
        "--hip_ipc_dst_expect_devices=2",
        "--hip_ipc_expect_devices=1",
        std::string("--hip_ipc_rerun_marker=") + marker};
    if (legacy_handle) args.push_back("--hip_ipc_legacy_owner_handle");
    auto argv = cStrings(args);
    std::vector<std::string> env_strings =
        environWith("HIP_VISIBLE_DEVICES", "0");
    // HIP also honors CUDA_VISIBLE_DEVICES; only HIP_VISIBLE_DEVICES may
    // restrict the rerun. gflags rejects --gtest_* flags, so the filter goes
    // through the environment. Drop inherited GoogleTest settings so the
    // child runs this test once, regardless of parent sharding or repeats.
    for (auto it = env_strings.begin(); it != env_strings.end();) {
        const bool drop = it->rfind("CUDA_VISIBLE_DEVICES=", 0) == 0 ||
                          it->rfind("GTEST_", 0) == 0;
        it = drop ? env_strings.erase(it) : it + 1;
    }
    env_strings.push_back(
        "GTEST_FILTER=HipTransportTest."
        "IpcTransfersHitBuffersInsideLargerAllocation");
    auto envp = cStrings(env_strings);
    // Its own process group, so a timeout also kills the destination the
    // rerun starts.
    posix_spawnattr_t attr;
    ASSERT_EQ(posix_spawnattr_init(&attr), 0);
    ASSERT_EQ(posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETPGROUP), 0);
    ASSERT_EQ(posix_spawnattr_setpgroup(&attr, 0), 0);
    pid_t pid = -1;
    const int spawn_rc =
        posix_spawn(&pid, argv[0], nullptr, &attr, argv.data(), envp.data());
    posix_spawnattr_destroy(&attr);
    ASSERT_EQ(spawn_rc, 0);
    struct ScopedProcessGroup {
        pid_t pid;
        ~ScopedProcessGroup() { stop(false); }
        void stop(bool leader_reaped) {
            if (pid <= 0) return;
            kill(-pid, SIGKILL);
            if (!leader_reaped) {
                int ignored_status = 0;
                while (waitpid(pid, &ignored_status, 0) < 0 && errno == EINTR) {
                }
            }
            pid = -1;
        }
    } group{pid};
    int status = 0;
    pid_t waited = 0;
    for (int i = 0; i < 2400 && waited == 0; ++i) {  // up to 240 s
        waited = waitpid(pid, &status, WNOHANG);
        if (waited < 0 && errno == EINTR) waited = 0;
        if (waited == 0) usleep(100 * 1000);
    }
    if (waited == 0) {
        group.stop(false);
        unlink(marker);
        FAIL() << "the rerun did not finish within 240 s";
    }
    const int wait_errno = errno;
    group.stop(waited == pid);
    ASSERT_EQ(waited, pid) << "waitpid failed: " << strerror(wait_errno);
    const bool passed = access(marker, F_OK) == 0;
    unlink(marker);
    ASSERT_TRUE(WIFEXITED(status)) << "the rerun was killed";
    EXPECT_EQ(WEXITSTATUS(status), 0)
        << "transfers to a destination on device ordinal 1 failed from a "
           "process that sees one device (its output is above)";
    EXPECT_TRUE(passed) << "the rerun did not run the transfer test to the end";
}

TEST(HipTransportTest, IpcTransfersAcrossDifferentVisibleDeviceSets) {
    runCrossVisibleSetIpc(false);
}

TEST(HipTransportTest, IpcImportsOlderOwnerOrdinalWithWorkaround) {
    runCrossVisibleSetIpc(true);
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    if (FLAGS_hip_ipc_dst) return runIpcDestination();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
