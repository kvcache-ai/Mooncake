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

#include <gtest/gtest.h>

#include <cstring>
#include <cstdlib>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/shm_transport/shm_transport.h"

namespace mooncake {

class ShmTransportTestPeer {
   public:
    static size_t mappingCount(ShmTransport& transport,
                               Transport::SegmentID target_id) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        auto it = transport.relocate_map_.find(target_id);
        return it == transport.relocate_map_.end() ? 0 : it->second.size();
    }
};

namespace {

std::string UniqueServerName(uint16_t extra) {
    return "127.0.0.1:" +
           std::to_string(21000 + static_cast<int>(getpid() % 1000) + extra);
}

class ScopedEnv {
   public:
    ScopedEnv(const char* name, const char* value) : name_(name) {
        const char* old = std::getenv(name);
        if (old) {
            had_old_ = true;
            old_ = old;
        }
        if (value)
            setenv(name_.c_str(), value, 1);
        else
            unsetenv(name_.c_str());
    }

    ~ScopedEnv() {
        if (had_old_)
            setenv(name_.c_str(), old_.c_str(), 1);
        else
            unsetenv(name_.c_str());
    }

   private:
    std::string name_;
    std::string old_;
    bool had_old_{false};
};

bool WaitStatus(TransferEngine& engine, BatchID batch_id,
                TransferStatusEnum expected) {
    TransferStatus status;
    for (int i = 0; i < 1000; ++i) {
        Status s = engine.getTransferStatus(batch_id, 0, status);
        if (!s.ok()) return false;
        if (status.s == expected) return true;
        if (status.s == TransferStatusEnum::FAILED &&
            expected != TransferStatusEnum::FAILED)
            return false;
        usleep(1000);
    }
    return false;
}

bool WaitCompleted(TransferEngine& engine, BatchID batch_id) {
    return WaitStatus(engine, batch_id, TransferStatusEnum::COMPLETED);
}

std::unique_ptr<TransferEngine> MakeEngine(const std::string& server_name,
                                           bool install_shm = true) {
    auto engine = std::make_unique<TransferEngine>(false);
    auto host_port = parseHostNameWithPort(server_name);
    int rc = engine->init(P2PHANDSHAKE, server_name, host_port.first.c_str(),
                          host_port.second);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(engine->installTransport("tcp", nullptr), nullptr);
    if (install_shm) {
        EXPECT_NE(engine->installTransport("shm", nullptr), nullptr);
        EXPECT_TRUE(engine->isTcpOnly());
    } else {
        EXPECT_EQ(engine->getTransport("shm"), nullptr);
    }
    return engine;
}

void ExpectShmWriteAndRead(TransferEngine& owner, TransferEngine& peer,
                           void* remote, uint64_t remote_base, size_t length) {
    std::vector<uint8_t> payload(length);
    std::vector<uint8_t> readback(length, 0);
    for (size_t i = 0; i < length; ++i) payload[i] = static_cast<uint8_t>(i);
    ASSERT_EQ(peer.registerLocalMemory(payload.data(), length, "cpu:0"), 0);

    auto segment_id = peer.openSegment(owner.getLocalIpAndPort());
    {
        auto batch = peer.allocateBatchID(1);
        TransferRequest write;
        write.opcode = TransferRequest::WRITE;
        write.source = payload.data();
        write.target_id = segment_id;
        write.target_offset = remote_base;
        write.length = length;
        ASSERT_TRUE(peer.submitTransfer(batch, {write}).ok());
        ASSERT_TRUE(WaitCompleted(peer, batch));
        ASSERT_TRUE(peer.freeBatchID(batch).ok());
    }

    auto* shm = dynamic_cast<ShmTransport*>(peer.getTransport("shm"));
    ASSERT_NE(shm, nullptr);
    EXPECT_GE(ShmTransportTestPeer::mappingCount(*shm, segment_id), 1u);
    EXPECT_EQ(std::memcmp(remote, payload.data(), length), 0);

    ASSERT_EQ(peer.registerLocalMemory(readback.data(), length, "cpu:0"), 0);
    {
        auto batch = peer.allocateBatchID(1);
        TransferRequest read;
        read.opcode = TransferRequest::READ;
        read.source = readback.data();
        read.target_id = segment_id;
        read.target_offset = remote_base;
        read.length = length;
        ASSERT_TRUE(peer.submitTransfer(batch, {read}).ok());
        ASSERT_TRUE(WaitCompleted(peer, batch));
        ASSERT_TRUE(peer.freeBatchID(batch).ok());
    }
    EXPECT_EQ(readback, payload);
}

const TransferMetadata::BufferDesc* FindPosixShmBuffer(
    const TransferMetadata::SegmentDesc& desc) {
    for (const auto& buffer : desc.buffers) {
        if (isPosixShmName(buffer.shm_name)) return &buffer;
    }
    return nullptr;
}

}  // namespace

TEST(ShmTransportE2E, WriteAndReadBetweenEngines) {
    const size_t length = 2 * 1024 * 1024;
    auto engine_a = MakeEngine(UniqueServerName(0));
    auto engine_b = MakeEngine(UniqueServerName(1));
    ASSERT_TRUE(engine_a);
    ASSERT_TRUE(engine_b);

    void* remote = engine_a->allocateSharedMemory(length);
    ASSERT_NE(remote, nullptr);
    std::memset(remote, 0, length);
    ASSERT_EQ(engine_a->registerLocalMemory(remote, length, "cpu:0"), 0);

    auto desc = engine_a->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(desc);
    ASSERT_FALSE(desc->buffers.empty());
    EXPECT_NE(desc->protocol.find("shm"), std::string::npos);
#ifdef ENABLE_MULTI_PROTOCOL
    EXPECT_NE(desc->protocol.find("tcp"), std::string::npos);
    auto* shm_buffer = FindPosixShmBuffer(*desc);
    ASSERT_NE(shm_buffer, nullptr);
    EXPECT_EQ(shm_buffer->protocol, "shm");
#else
    EXPECT_EQ(desc->protocol, "shm");
#endif

    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    ASSERT_FALSE(remote_desc->buffers.empty());
    auto* remote_shm = FindPosixShmBuffer(*remote_desc);
    ASSERT_NE(remote_shm, nullptr);
    uint64_t remote_base = remote_shm->addr;
    EXPECT_NE(remote_desc->protocol.find("shm"), std::string::npos);

    ASSERT_NO_FATAL_FAILURE(ExpectShmWriteAndRead(*engine_a, *engine_b, remote,
                                                  remote_base, length));
    ASSERT_EQ(engine_a->freeSharedMemory(remote), 0);
}

TEST(ShmTransportE2E, WriteAndRead4K) {
    const size_t length = 4096;
    auto engine_a = MakeEngine(UniqueServerName(6));
    auto engine_b = MakeEngine(UniqueServerName(7));
    void* remote = engine_a->allocateSharedMemory(length);
    ASSERT_NE(remote, nullptr);
    std::memset(remote, 0, length);
    ASSERT_EQ(engine_a->registerLocalMemory(remote, length, "cpu:0"), 0);
    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    auto* remote_shm = FindPosixShmBuffer(*remote_desc);
    ASSERT_NE(remote_shm, nullptr);
    uint64_t remote_base = remote_shm->addr;
    ASSERT_NO_FATAL_FAILURE(ExpectShmWriteAndRead(*engine_a, *engine_b, remote,
                                                  remote_base, length));
    ASSERT_EQ(engine_a->freeSharedMemory(remote), 0);
}

TEST(ShmTransportE2E, WriteAndReadCrossPage) {
    const size_t length = static_cast<size_t>(sysconf(_SC_PAGESIZE)) + 64;
    auto engine_a = MakeEngine(UniqueServerName(8));
    auto engine_b = MakeEngine(UniqueServerName(9));
    void* remote = engine_a->allocateSharedMemory(length);
    ASSERT_NE(remote, nullptr);
    std::memset(remote, 0, length);
    ASSERT_EQ(engine_a->registerLocalMemory(remote, length, "cpu:0"), 0);
    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    auto* remote_shm = FindPosixShmBuffer(*remote_desc);
    ASSERT_NE(remote_shm, nullptr);
    uint64_t remote_base = remote_shm->addr;
    ASSERT_NO_FATAL_FAILURE(ExpectShmWriteAndRead(*engine_a, *engine_b, remote,
                                                  remote_base, length));
    ASSERT_EQ(engine_a->freeSharedMemory(remote), 0);
}

TEST(ShmTransportE2E, RegisterSubRangeFails) {
    const size_t page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const size_t length = page_size * 2;
    auto engine_a = MakeEngine(UniqueServerName(24));
    auto engine_b = MakeEngine(UniqueServerName(25));
    void* remote = engine_a->allocateSharedMemory(length);
    ASSERT_NE(remote, nullptr);
    auto* mid = static_cast<char*>(remote) + page_size;
    EXPECT_EQ(engine_a->registerLocalMemory(mid, page_size, "cpu:0"),
              ERR_INVALID_ARGUMENT);
    EXPECT_EQ(engine_a->registerLocalMemory(remote, length + 1, "cpu:0"),
              ERR_INVALID_ARGUMENT);

    ASSERT_EQ(engine_a->registerLocalMemory(remote, page_size, "cpu:0"), 0);
    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    auto* remote_shm = FindPosixShmBuffer(*remote_desc);
    ASSERT_NE(remote_shm, nullptr);
    EXPECT_EQ(remote_shm->length, page_size);
    ASSERT_NO_FATAL_FAILURE(ExpectShmWriteAndRead(*engine_a, *engine_b, remote,
                                                  remote_shm->addr, page_size));
    ASSERT_EQ(engine_a->freeSharedMemory(remote), 0);
}

TEST(ShmTransportE2E, TransferFailsAfterFreeSharedMemory) {
    const size_t length = 4096;
    auto engine_a = MakeEngine(UniqueServerName(10));
    auto engine_b = MakeEngine(UniqueServerName(11));
    void* remote = engine_a->allocateSharedMemory(length);
    ASSERT_NE(remote, nullptr);
    std::memset(remote, 0, length);
    ASSERT_EQ(engine_a->registerLocalMemory(remote, length, "cpu:0"), 0);

    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    auto* remote_shm = FindPosixShmBuffer(*remote_desc);
    ASSERT_NE(remote_shm, nullptr);
    uint64_t remote_base = remote_shm->addr;

    std::vector<uint8_t> payload(length, 0x5a);
    ASSERT_EQ(engine_b->registerLocalMemory(payload.data(), length, "cpu:0"),
              0);
    ASSERT_EQ(engine_a->freeSharedMemory(remote), 0);

    auto batch = engine_b->allocateBatchID(1);
    TransferRequest write;
    write.opcode = TransferRequest::WRITE;
    write.source = payload.data();
    write.target_id = segment_id;
    write.target_offset = remote_base;
    write.length = length;
    EXPECT_FALSE(engine_b->submitTransfer(batch, {write}).ok());
    auto* shm = dynamic_cast<ShmTransport*>(engine_b->getTransport("shm"));
    ASSERT_NE(shm, nullptr);
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(*shm, segment_id), 0u);
    ASSERT_TRUE(WaitStatus(*engine_b, batch, TransferStatusEnum::FAILED));
    ASSERT_TRUE(engine_b->freeBatchID(batch).ok());
}

TEST(ShmTransportE2E, FreeSharedMemoryDoesNotUnregisterMalloc) {
    const size_t length = 4096;
    auto engine_a = MakeEngine(UniqueServerName(20));
    auto engine_b = MakeEngine(UniqueServerName(21));
    std::vector<uint8_t> remote(length, 0);
    std::vector<uint8_t> payload(length, 0x9a);
    ASSERT_EQ(engine_a->registerLocalMemory(remote.data(), length, "cpu:0"), 0);
    ASSERT_EQ(engine_b->registerLocalMemory(payload.data(), length, "cpu:0"),
              0);
    EXPECT_EQ(engine_a->freeSharedMemory(remote.data()), ERR_INVALID_ARGUMENT);

    // Local metadata still has the malloc buffer; freeSharedMemory must not
    // unregister transports it does not own.
    auto local_desc =
        engine_a->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(local_desc);
    ASSERT_FALSE(local_desc->buffers.empty());
    EXPECT_EQ(local_desc->buffers[0].addr,
              reinterpret_cast<uint64_t>(remote.data()));

#ifdef ENABLE_MULTI_PROTOCOL
    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    ASSERT_FALSE(remote_desc->buffers.empty());
    auto batch = engine_b->allocateBatchID(1);
    TransferRequest write;
    write.opcode = TransferRequest::WRITE;
    write.source = payload.data();
    write.target_id = segment_id;
    write.target_offset = remote_desc->buffers[0].addr;
    write.length = length;
    ASSERT_TRUE(engine_b->submitTransfer(batch, {write}).ok());
    ASSERT_TRUE(WaitCompleted(*engine_b, batch));
    ASSERT_TRUE(engine_b->freeBatchID(batch).ok());
    EXPECT_EQ(remote, payload);
#else
    // installTransport("shm") replaced protocol with "shm", so decode skips
    // malloc buffers (empty shm_name). Do not index buffers[0].
    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    EXPECT_EQ(FindPosixShmBuffer(*remote_desc), nullptr);
    EXPECT_TRUE(remote_desc->buffers.empty());
#endif
}

TEST(ShmTransportE2E, TransferAfterFreeAndReallocateSharedMemory) {
    const size_t length = 4096;
    auto engine_a = MakeEngine(UniqueServerName(22));
    auto engine_b = MakeEngine(UniqueServerName(23));
    std::vector<uint8_t> first_payload(length, 0xa1);
    std::vector<uint8_t> second_payload(length, 0xb2);
    ASSERT_EQ(
        engine_b->registerLocalMemory(first_payload.data(), length, "cpu:0"),
        0);
    ASSERT_EQ(
        engine_b->registerLocalMemory(second_payload.data(), length, "cpu:0"),
        0);

    void* remote = engine_a->allocateSharedMemory(length);
    ASSERT_NE(remote, nullptr);
    std::memset(remote, 0, length);
    ASSERT_EQ(engine_a->registerLocalMemory(remote, length, "cpu:0"), 0);

    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    auto* remote_shm = FindPosixShmBuffer(*remote_desc);
    ASSERT_NE(remote_shm, nullptr);
    uint64_t remote_base = remote_shm->addr;

    auto submit_write = [&](std::vector<uint8_t>& payload) -> bool {
        auto batch = engine_b->allocateBatchID(1);
        TransferRequest write;
        write.opcode = TransferRequest::WRITE;
        write.source = payload.data();
        write.target_id = segment_id;
        write.target_offset = remote_base;
        write.length = length;
        if (!engine_b->submitTransfer(batch, {write}).ok()) {
            (void)engine_b->freeBatchID(batch);
            return false;
        }
        if (!WaitCompleted(*engine_b, batch)) {
            (void)engine_b->freeBatchID(batch);
            return false;
        }
        if (!engine_b->freeBatchID(batch).ok()) return false;
        return std::memcmp(remote, payload.data(), length) == 0;
    };
    EXPECT_TRUE(submit_write(first_payload));

    ASSERT_EQ(engine_a->freeSharedMemory(remote), 0);
    remote = engine_a->allocateSharedMemory(length);
    ASSERT_NE(remote, nullptr);
    std::memset(remote, 0, length);
    ASSERT_EQ(engine_a->registerLocalMemory(remote, length, "cpu:0"), 0);

    // Do not call syncSegmentCache: relocate must notice the unlinked object,
    // drop the orphaned mmap, and refetch the peer descriptor.
    if (!submit_write(second_payload)) {
        remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
        ASSERT_TRUE(remote_desc);
        remote_shm = FindPosixShmBuffer(*remote_desc);
        ASSERT_NE(remote_shm, nullptr);
        remote_base = remote_shm->addr;
        ASSERT_TRUE(submit_write(second_payload));
    }
    ASSERT_EQ(engine_a->freeSharedMemory(remote), 0);
}

TEST(ShmTransportE2E, UnsetMcForceShmDoesNotInstallShm) {
    ScopedEnv disable_force("MC_FORCE_SHM", nullptr);
    const size_t length = 4096;
    auto engine_a = MakeEngine(UniqueServerName(12), /*install_shm=*/false);
    auto engine_b = MakeEngine(UniqueServerName(13), /*install_shm=*/false);
    ASSERT_EQ(engine_a->getTransport("shm"), nullptr);
    ASSERT_EQ(engine_b->getTransport("shm"), nullptr);
    EXPECT_EQ(engine_a->allocateSharedMemory(length), nullptr);

    std::vector<uint8_t> remote(length, 0);
    std::vector<uint8_t> payload(length, 0x3c);
    ASSERT_EQ(engine_a->registerLocalMemory(remote.data(), length, "cpu:0"), 0);
    ASSERT_EQ(engine_b->registerLocalMemory(payload.data(), length, "cpu:0"),
              0);
    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    EXPECT_FALSE(isPosixShmName(remote_desc->buffers[0].shm_name));

    auto batch = engine_b->allocateBatchID(1);
    TransferRequest write;
    write.opcode = TransferRequest::WRITE;
    write.source = payload.data();
    write.target_id = segment_id;
    write.target_offset = remote_desc->buffers[0].addr;
    write.length = length;
    ASSERT_TRUE(engine_b->submitTransfer(batch, {write}).ok());
    ASSERT_TRUE(WaitCompleted(*engine_b, batch));
    ASSERT_TRUE(engine_b->freeBatchID(batch).ok());
    EXPECT_EQ(remote, payload);
}

#ifndef ENABLE_MULTI_PROTOCOL
TEST(ShmTransportE2E, McForceShmInstallsShmOnly) {
    ScopedEnv force_shm("MC_FORCE_SHM", "1");
    const std::string server_name = UniqueServerName(16);
    auto engine = std::make_unique<TransferEngine>(true);
    auto host_port = parseHostNameWithPort(server_name);
    ASSERT_EQ(engine->init(P2PHANDSHAKE, server_name, host_port.first.c_str(),
                           host_port.second),
              0);
    EXPECT_NE(engine->getTransport("shm"), nullptr);
    EXPECT_EQ(engine->getTransport("tcp"), nullptr);
    EXPECT_EQ(engine->getTransport("rdma"), nullptr);
    auto desc = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(desc);
    EXPECT_EQ(desc->protocol, "shm");
    void* buf = engine->allocateSharedMemory(4096);
    ASSERT_NE(buf, nullptr);
    EXPECT_EQ(engine->freeSharedMemory(buf), 0);
}
#endif

#ifdef ENABLE_MULTI_PROTOCOL
TEST(ShmTransportE2E, McForceShmCoexistsWithHostTransport) {
    ScopedEnv force_shm("MC_FORCE_SHM", "1");
    const std::string server_name = UniqueServerName(18);
    auto engine = std::make_unique<TransferEngine>(true);
    auto host_port = parseHostNameWithPort(server_name);
    ASSERT_EQ(engine->init(P2PHANDSHAKE, server_name, host_port.first.c_str(),
                           host_port.second),
              0);
    EXPECT_NE(engine->getTransport("shm"), nullptr);
    EXPECT_TRUE(engine->getTransport("tcp") != nullptr ||
                engine->getTransport("rdma") != nullptr);
    auto desc = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(desc);
    EXPECT_NE(desc->protocol.find("shm"), std::string::npos);
    EXPECT_NE(desc->protocol.find(','), std::string::npos);
}

TEST(ShmTransportE2E, MallocBufferUsesTcp) {
    const size_t length = 4096;
    const std::string name_a = UniqueServerName(2);
    const std::string name_b = UniqueServerName(3);
    std::vector<uint8_t> remote(length, 0);
    std::vector<uint8_t> payload(length, 0x7e);

    auto engine_a = MakeEngine(name_a);
    auto engine_b = MakeEngine(name_b);

    ASSERT_EQ(engine_a->registerLocalMemory(remote.data(), length, "cpu:0"), 0);
    ASSERT_EQ(engine_b->registerLocalMemory(payload.data(), length, "cpu:0"),
              0);

    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    uint64_t remote_base = remote_desc->buffers[0].addr;
    EXPECT_EQ(FindPosixShmBuffer(*remote_desc), nullptr);

    auto batch = engine_b->allocateBatchID(1);
    TransferRequest write;
    write.opcode = TransferRequest::WRITE;
    write.source = payload.data();
    write.target_id = segment_id;
    write.target_offset = remote_base;
    write.length = length;
    ASSERT_TRUE(engine_b->submitTransfer(batch, {write}).ok());
    ASSERT_TRUE(WaitCompleted(*engine_b, batch));
    ASSERT_TRUE(engine_b->freeBatchID(batch).ok());

    auto* shm = dynamic_cast<ShmTransport*>(engine_b->getTransport("shm"));
    ASSERT_NE(shm, nullptr);
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(*shm, segment_id), 0u);
    EXPECT_EQ(remote, payload);
}

TEST(ShmTransportE2E, SubmitTransferPrefersShmOnSameHost) {
    const size_t length = 4096;
    auto engine_a = MakeEngine(UniqueServerName(4));
    auto engine_b = MakeEngine(UniqueServerName(5));
    std::vector<uint8_t> payload(length, 0xa5);

    void* remote = engine_a->allocateSharedMemory(length);
    ASSERT_NE(remote, nullptr);
    std::memset(remote, 0, length);
    ASSERT_EQ(engine_a->registerLocalMemory(remote, length, "cpu:0"), 0);
    auto local_desc =
        engine_a->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(local_desc);
    EXPECT_NE(local_desc->protocol.find("tcp"), std::string::npos);
    EXPECT_NE(local_desc->protocol.find("shm"), std::string::npos);
    auto* shm_buffer = FindPosixShmBuffer(*local_desc);
    ASSERT_NE(shm_buffer, nullptr);
    EXPECT_EQ(shm_buffer->protocol, "shm");
    ASSERT_EQ(engine_b->registerLocalMemory(payload.data(), length, "cpu:0"),
              0);

    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    auto* remote_shm = FindPosixShmBuffer(*remote_desc);
    ASSERT_NE(remote_shm, nullptr);
    uint64_t remote_base = remote_shm->addr;

    auto batch = engine_b->allocateBatchID(1);
    TransferRequest write;
    write.opcode = TransferRequest::WRITE;
    write.source = payload.data();
    write.target_id = segment_id;
    write.target_offset = remote_base;
    write.length = length;
    ASSERT_TRUE(engine_b->submitTransfer(batch, {write}).ok());
    ASSERT_TRUE(WaitCompleted(*engine_b, batch));
    ASSERT_TRUE(engine_b->freeBatchID(batch).ok());

    auto* shm = dynamic_cast<ShmTransport*>(engine_b->getTransport("shm"));
    ASSERT_NE(shm, nullptr);
    EXPECT_GE(ShmTransportTestPeer::mappingCount(*shm, segment_id), 1u);
    EXPECT_EQ(std::memcmp(remote, payload.data(), length), 0);
    ASSERT_EQ(engine_a->freeSharedMemory(remote), 0);
}

TEST(ShmTransportE2E, MpSubmitTransferHonorsTcpPreference) {
    const size_t length = 4096;
    auto engine_a = MakeEngine(UniqueServerName(14));
    auto engine_b = MakeEngine(UniqueServerName(15));
    std::vector<uint8_t> payload(length, 0x3d);

    void* remote = engine_a->allocateSharedMemory(length);
    ASSERT_NE(remote, nullptr);
    std::memset(remote, 0, length);
    ASSERT_EQ(engine_a->registerLocalMemory(remote, length, "cpu:0"), 0);
    ASSERT_EQ(engine_b->registerLocalMemory(payload.data(), length, "cpu:0"),
              0);

    auto segment_id = engine_b->openSegment(engine_a->getLocalIpAndPort());
    auto remote_desc = engine_b->getMetadata()->getSegmentDescByID(segment_id);
    ASSERT_TRUE(remote_desc);
    auto* remote_shm = FindPosixShmBuffer(*remote_desc);
    ASSERT_NE(remote_shm, nullptr);

    auto batch = engine_b->allocateBatchID(1);
    TransferRequest write;
    write.opcode = TransferRequest::WRITE;
    write.source = payload.data();
    write.target_id = segment_id;
    write.target_offset = remote_shm->addr;
    write.length = length;
    std::string proto = "tcp";
    ASSERT_TRUE(engine_b->mp_submitTransfer(batch, {write}, proto).ok());
    ASSERT_TRUE(WaitCompleted(*engine_b, batch));
    ASSERT_TRUE(engine_b->freeBatchID(batch).ok());

    auto* shm = dynamic_cast<ShmTransport*>(engine_b->getTransport("shm"));
    ASSERT_NE(shm, nullptr);
    EXPECT_EQ(ShmTransportTestPeer::mappingCount(*shm, segment_id), 0u);
    EXPECT_EQ(std::memcmp(remote, payload.data(), length), 0);
    ASSERT_EQ(engine_a->freeSharedMemory(remote), 0);
}
#endif

}  // namespace mooncake
