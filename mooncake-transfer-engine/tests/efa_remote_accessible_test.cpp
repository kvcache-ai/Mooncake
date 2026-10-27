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

// registerLocalMemory(..., remote_accessible=false) on EFA.
//
// EfaTransport used to ignore remote_accessible: every MR got remote read and
// write rights and every rkey was published in the segment descriptor, so any
// peer that opened the segment could RMA into "local-only" memory.
// RdmaTransport already honors the flag. The fix registers such buffers without
// remote rights and publishes no rkey, and makes the local-side selectDevice()
// look at lkeys so the owner can still use them as a source or destination.
//
// Two engines in one process (distinct RPC ports) so transfers go over EFA:
// EfaContext::tryLoopbackCopy() memcpys a transfer whose peer is the same
// engine, which would bypass the peer-side checks under test.
//
// Needs an EFA device; skips otherwise.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <numa.h>

#include <cstring>
#include <memory>
#include <string>

#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

static constexpr size_t kBufferSize = 4ull << 20;
static constexpr size_t kDataLength = 1ull << 20;

class EFARemoteAccessibleTest : public ::testing::Test {
   protected:
    std::unique_ptr<TransferEngine> initiator;
    std::unique_ptr<TransferEngine> target;
    // initiator: local-only source/destination.
    char *local_buf = nullptr;
    // target: one remote-accessible buffer, one local-only buffer.
    char *public_buf = nullptr;
    char *private_buf = nullptr;
    SegmentID target_segment = 0;

    static std::unique_ptr<TransferEngine> makeEngine(const std::string &name) {
        auto engine = std::make_unique<TransferEngine>(false);
        engine->getLocalTopology()->discover({});
        auto hp = parseHostNameWithPort(name);
        if (engine->init("P2PHANDSHAKE", name, hp.first.c_str(), hp.second))
            return nullptr;
        if (!engine->installTransport("efa", nullptr)) return nullptr;
        return engine;
    }

    static char *alloc() {
        return static_cast<char *>(numa_alloc_onnode(kBufferSize, 0));
    }

    void SetUp() override {
        target = makeEngine("127.0.0.1:12401");
        if (!target) GTEST_SKIP() << "EFA transport unavailable";
        initiator = makeEngine("127.0.0.1:12402");
        ASSERT_NE(initiator, nullptr);

        local_buf = alloc();
        public_buf = alloc();
        private_buf = alloc();
        ASSERT_TRUE(local_buf && public_buf && private_buf);
        ASSERT_EQ(initiator->registerLocalMemory(local_buf, kBufferSize,
                                                 "cpu:0", false),
                  0);
        ASSERT_EQ(
            target->registerLocalMemory(public_buf, kBufferSize, "cpu:0", true),
            0);
        ASSERT_EQ(target->registerLocalMemory(private_buf, kBufferSize, "cpu:0",
                                              false),
                  0);

        target_segment = initiator->openSegment(target->getLocalIpAndPort());
        ASSERT_GE(target_segment, 0);
    }

    void TearDown() override {
        if (initiator && local_buf) initiator->unregisterLocalMemory(local_buf);
        if (target && public_buf) target->unregisterLocalMemory(public_buf);
        if (target && private_buf) target->unregisterLocalMemory(private_buf);
        for (char *buf : {local_buf, public_buf, private_buf})
            if (buf) numa_free(buf, kBufferSize);
    }

    // Submit one request from the initiator to the target and poll to a
    // terminal state. A rejected submission counts as FAILED.
    TransferStatusEnum transfer(void *source, void *target_addr, size_t length,
                                TransferRequest::OpCode opcode) {
        auto batch_id = initiator->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = opcode;
        entry.length = length;
        entry.source = static_cast<uint8_t *>(source);
        entry.target_id = target_segment;
        entry.target_offset = reinterpret_cast<uint64_t>(target_addr);
        if (!initiator->submitTransfer(batch_id, {entry}).ok()) {
            initiator->freeBatchID(batch_id);
            return TransferStatusEnum::FAILED;
        }
        TransferStatus status;
        status.s = TransferStatusEnum::WAITING;
        for (int i = 0; i < 10000000; ++i) {
            if (!initiator->getTransferStatus(batch_id, 0, status).ok()) {
                status.s = TransferStatusEnum::FAILED;
                break;
            }
            if (status.s != TransferStatusEnum::WAITING &&
                status.s != TransferStatusEnum::PENDING)
                break;
        }
        initiator->freeBatchID(batch_id);
        return status.s;
    }

    static const TransferMetadata::BufferDesc *findBuffer(
        const TransferMetadata::SegmentDesc &desc, const void *addr) {
        for (const auto &buffer : desc.buffers)
            if (buffer.addr == reinterpret_cast<uint64_t>(addr)) return &buffer;
        return nullptr;
    }

    static void fill(char *buf, char seed) {
        for (size_t i = 0; i < kDataLength; ++i) buf[i] = (char)(seed + i % 61);
    }
};

// The descriptor a peer fetches carries an rkey for the public buffer only.
// lkeys are still recorded for the owner's own use.
TEST_F(EFARemoteAccessibleTest, PrivateBufferPublishesNoRemoteKey) {
    auto desc = initiator->getMetadata()->getSegmentDescByID(target_segment);
    ASSERT_NE(desc, nullptr);
    auto *pub = findBuffer(*desc, public_buf);
    auto *priv = findBuffer(*desc, private_buf);
    ASSERT_NE(pub, nullptr);
    ASSERT_NE(priv, nullptr);
    EXPECT_FALSE(pub->rkey.empty());
    EXPECT_TRUE(priv->rkey.empty())
        << "remote_accessible=false buffer published an rkey";
    EXPECT_FALSE(priv->lkey.empty());
}

// A local-only buffer still works as the source of a WRITE and the
// destination of a READ. Before the local-side selectDevice() looked at lkeys
// this failed with "Memory region not registered by any active EFA device".
TEST_F(EFARemoteAccessibleTest, LocalOnlyBufferIsUsableLocally) {
    fill(local_buf, 'a');
    memset(public_buf, 0, kDataLength);
    ASSERT_EQ(
        transfer(local_buf, public_buf, kDataLength, TransferRequest::WRITE),
        TransferStatusEnum::COMPLETED);
    EXPECT_EQ(0, memcmp(local_buf, public_buf, kDataLength));

    char *read_into = local_buf + kDataLength;
    memset(read_into, 0, kDataLength);
    ASSERT_EQ(
        transfer(read_into, public_buf, kDataLength, TransferRequest::READ),
        TransferStatusEnum::COMPLETED);
    EXPECT_EQ(0, memcmp(read_into, public_buf, kDataLength));
}

// A peer cannot write into the target's local-only buffer.
TEST_F(EFARemoteAccessibleTest, PeerCannotWritePrivateBuffer) {
    fill(local_buf, 'k');
    memset(private_buf, 0x5a, kDataLength);
    EXPECT_NE(
        transfer(local_buf, private_buf, kDataLength, TransferRequest::WRITE),
        TransferStatusEnum::COMPLETED);
    for (size_t i = 0; i < kDataLength; ++i)
        ASSERT_EQ(private_buf[i], 0x5a) << "private buffer modified at " << i;
}

// A peer cannot read the target's local-only buffer.
TEST_F(EFARemoteAccessibleTest, PeerCannotReadPrivateBuffer) {
    fill(private_buf, 's');
    memset(local_buf, 0, kDataLength);
    EXPECT_NE(
        transfer(local_buf, private_buf, kDataLength, TransferRequest::READ),
        TransferStatusEnum::COMPLETED);
    EXPECT_NE(0, memcmp(local_buf, private_buf, kDataLength));
}

}  // namespace mooncake
