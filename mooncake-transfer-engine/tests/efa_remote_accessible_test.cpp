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
// On EFA the lkey is the same MR key as the rkey, and libfabric before 2.6
// gives every MR remote rights anyway, so the published lkey is a placeholder.
//
// Two engines in one process (distinct RPC ports) so transfers go over EFA:
// EfaContext::tryLoopbackCopy() memcpys a transfer whose peer is the same
// engine, which would bypass the peer-side checks under test. The EFA
// provider also hands same-host peers to its shm provider, which drops an RMA
// with a bad key on the target yet completes it on the initiator, so shm
// transfer is turned off to keep every RMA on the EFA device.
//
// Needs an EFA device; skips otherwise.

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <numa.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

#include "transfer_engine.h"
#include "transport/efa_transport/efa_transport.h"
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
        // Read when libfabric initializes, before the first engine.
        setenv("FI_EFA_ENABLE_SHM_TRANSFER", "0", 1);
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
    // terminal state. A rejected submission counts as FAILED; a request still
    // in flight when the spin budget runs out comes back as WAITING.
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

    // Every byte of the first kDataLength is still `value`.
    static void expectUnchanged(const char *buf, char value) {
        for (size_t i = 0; i < kDataLength; ++i)
            ASSERT_EQ(buf[i], value) << "buffer modified at " << i;
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
    // The lkey a peer sees for the private buffer is not its MR key: on EFA
    // that is also the rkey.
    ASSERT_EQ(priv->lkey.size(), pub->lkey.size());
    bool any_nic = false;
    for (size_t i = 0; i < priv->lkey.size(); ++i) {
        EXPECT_TRUE(priv->lkey[i] == 0 ||
                    priv->lkey[i] == EfaTransport::kLocalOnlyKey)
            << "lkey[" << i << "] = " << priv->lkey[i];
        any_nic |= priv->lkey[i] != 0;
    }
    EXPECT_TRUE(any_nic);
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
    EXPECT_EQ(
        transfer(local_buf, private_buf, kDataLength, TransferRequest::WRITE),
        TransferStatusEnum::FAILED);
    expectUnchanged(private_buf, 0x5a);
}

// A peer cannot read the target's local-only buffer.
TEST_F(EFARemoteAccessibleTest, PeerCannotReadPrivateBuffer) {
    fill(private_buf, 's');
    memset(local_buf, 0, kDataLength);
    EXPECT_EQ(
        transfer(local_buf, private_buf, kDataLength, TransferRequest::READ),
        TransferStatusEnum::FAILED);
    expectUnchanged(local_buf, 0);
}

// The tests above stop at the initiator's selectDevice(), which finds no rkey.
// A peer that does not stop there posts RMA with the key it was given, so use
// the private buffer's published lkeys as rkeys in the initiator's copy of the
// descriptor: neither direction may reach the buffer.
TEST_F(EFARemoteAccessibleTest, PeerCannotUsePublishedLkeyAsRkey) {
    auto desc = initiator->getMetadata()->getSegmentDescByID(target_segment);
    ASSERT_NE(desc, nullptr);
    TransferMetadata::BufferDesc *priv = nullptr;
    for (auto &buffer : desc->buffers)
        if (buffer.addr == reinterpret_cast<uint64_t>(private_buf))
            priv = &buffer;
    ASSERT_NE(priv, nullptr);
    priv->rkey = priv->lkey;

    fill(local_buf, 'k');
    memset(private_buf, 0x5a, kDataLength);
    EXPECT_EQ(
        transfer(local_buf, private_buf, kDataLength, TransferRequest::WRITE),
        TransferStatusEnum::FAILED);
    expectUnchanged(private_buf, 0x5a);

    fill(private_buf, 's');
    memset(local_buf, 0, kDataLength);
    EXPECT_EQ(
        transfer(local_buf, private_buf, kDataLength, TransferRequest::READ),
        TransferStatusEnum::FAILED);
    expectUnchanged(local_buf, 0);
}

}  // namespace mooncake
