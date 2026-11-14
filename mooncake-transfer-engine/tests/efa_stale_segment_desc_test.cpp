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

// An initiator that cached a peer's segment descriptor must still reach a
// buffer the peer registered afterwards. A Store client does this when it
// mounts a large segment in several chunks and takes traffic for the first
// ones while it registers the rest. Needs an EFA device; skips otherwise.

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "transfer_engine.h"
#include "transport/transport.h"

namespace mooncake {
namespace {

constexpr size_t kBufferSize = 4 << 20;

std::unique_ptr<TransferEngine> MakeEfaEngine() {
    auto engine = std::make_unique<TransferEngine>(false);
    engine->getLocalTopology()->discover({});
    if (engine->init(P2PHANDSHAKE, "127.0.0.1:0", "127.0.0.1", 0) != 0) {
        return nullptr;
    }
    if (!engine->installTransport("efa", nullptr)) return nullptr;
    return engine;
}

bool Transfer(TransferEngine &engine, SegmentID target, void *source,
              uint64_t target_offset, size_t length,
              TransferRequest::OpCode opcode) {
    auto batch = engine.allocateBatchID(1);
    TransferRequest request;
    request.opcode = opcode;
    request.source = source;
    request.target_id = target;
    request.target_offset = target_offset;
    request.length = length;
    bool ok = engine.submitTransfer(batch, {request}).ok();
    TransferStatus status;
    for (int i = 0; ok && i < 10000000; ++i) {
        if (!engine.getTransferStatus(batch, 0, status).ok()) {
            ok = false;
        } else if (status.s == TransferStatusEnum::COMPLETED) {
            break;
        } else if (status.s == TransferStatusEnum::FAILED) {
            ok = false;
        }
    }
    ok = ok && status.s == TransferStatusEnum::COMPLETED;
    engine.freeBatchID(batch);
    return ok;
}

TEST(EfaStaleSegmentDescTest, ReachesBufferRegisteredAfterFirstTransfer) {
    auto target = MakeEfaEngine();
    if (!target) GTEST_SKIP() << "EFA transport unavailable";
    auto initiator = MakeEfaEngine();
    ASSERT_NE(initiator, nullptr);

    std::vector<uint8_t> first(kBufferSize, 0), second(kBufferSize, 0);
    std::vector<uint8_t> source(kBufferSize);
    for (size_t i = 0; i < source.size(); ++i) source[i] = i * 7 + 3;
    ASSERT_EQ(target->registerLocalMemory(first.data(), kBufferSize, "cpu:0"),
              0);
    ASSERT_EQ(
        initiator->registerLocalMemory(source.data(), kBufferSize, "cpu:0"), 0);

    // The first transfer caches the target's descriptor on the initiator.
    auto segment = initiator->openSegment(target->getLocalIpAndPort());
    ASSERT_TRUE(Transfer(*initiator, segment, source.data(),
                         (uint64_t)first.data(), kBufferSize,
                         TransferRequest::WRITE));
    EXPECT_EQ(first, source);

    // A buffer the target registers now is missing from that cached copy.
    ASSERT_EQ(target->registerLocalMemory(second.data(), kBufferSize, "cpu:0"),
              0);
    ASSERT_TRUE(Transfer(*initiator, segment, source.data(),
                         (uint64_t)second.data(), kBufferSize,
                         TransferRequest::WRITE));
    EXPECT_EQ(second, source);

    std::vector<uint8_t> readback(kBufferSize, 0);
    ASSERT_EQ(
        initiator->registerLocalMemory(readback.data(), kBufferSize, "cpu:0"),
        0);
    ASSERT_TRUE(Transfer(*initiator, segment, readback.data(),
                         (uint64_t)second.data(), kBufferSize,
                         TransferRequest::READ));
    EXPECT_EQ(readback, source);

    initiator->unregisterLocalMemory(readback.data());
    initiator->unregisterLocalMemory(source.data());
    target->unregisterLocalMemory(second.data());
    target->unregisterLocalMemory(first.data());
}

}  // namespace
}  // namespace mooncake
