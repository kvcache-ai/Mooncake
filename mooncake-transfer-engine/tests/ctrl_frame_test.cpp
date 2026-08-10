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

#include "transport/rdma_twosided/ctrl_frame.h"

using namespace mooncake;

TEST(CtrlFrameTest, NotifyCompatRoundTrip) {
    TransferMetadata::NotifyDesc in{"hello", "world"};
    std::vector<uint8_t> payload;
    ASSERT_EQ(encodeNotifyCompatPayload(in, payload), 0);

    CtrlFrame frame;
    frame.type = CtrlFrameType::NOTIFY_COMPAT;
    frame.session = 42;
    frame.epoch = 7;
    frame.seq = 3;
    frame.payload = payload;

    std::vector<uint8_t> wired;
    ASSERT_EQ(encodeCtrlFrame(frame, wired), 0);
    EXPECT_TRUE(isCtrlFrameMagic(wired.data(), wired.size()));
    EXPECT_EQ(wired.size(), kCtrlFrameHeaderSize + payload.size());

    CtrlFrame out;
    ASSERT_EQ(decodeCtrlFrame(wired.data(), wired.size(), out), 0);
    EXPECT_EQ(out.type, CtrlFrameType::NOTIFY_COMPAT);
    EXPECT_EQ(out.session, 42u);
    EXPECT_EQ(out.epoch, 7u);
    EXPECT_EQ(out.seq, 3u);

    TransferMetadata::NotifyDesc decoded;
    ASSERT_EQ(decodeNotifyCompatPayload(out.payload.data(), out.payload.size(),
                                        decoded),
              0);
    EXPECT_EQ(decoded.name, "hello");
    EXPECT_EQ(decoded.notify_msg, "world");
}

TEST(CtrlFrameTest, CreditGrantAndDataAckRoundTrip) {
    std::vector<CreditAmount> grants = {
        {CreditResource::BounceSlots, 64},
        {CreditResource::BounceBytes, 64 * 65536},
    };
    std::vector<uint8_t> grant_payload;
    ASSERT_EQ(encodeCreditGrantPayload(grants, grant_payload), 0);
    std::vector<CreditAmount> grants_out;
    ASSERT_EQ(decodeCreditGrantPayload(grant_payload.data(),
                                       grant_payload.size(), grants_out),
              0);
    ASSERT_EQ(grants_out.size(), 2u);
    EXPECT_EQ(grants_out[0].resource, CreditResource::BounceSlots);
    EXPECT_EQ(grants_out[0].grant_total, 64u);

    std::vector<DataAckEntry> acks = {{11, 100}, {12, 200}};
    std::vector<uint8_t> ack_payload;
    ASSERT_EQ(encodeDataAckPayload(acks, ack_payload), 0);
    std::vector<DataAckEntry> acks_out;
    ASSERT_EQ(
        decodeDataAckPayload(ack_payload.data(), ack_payload.size(), acks_out),
        0);
    ASSERT_EQ(acks_out.size(), 2u);
    EXPECT_EQ(acks_out[1].task_id, 12u);
    EXPECT_EQ(acks_out[1].acked_bytes, 200u);
}

TEST(CtrlFrameTest, SessionOpenPayload) {
    std::vector<uint8_t> payload;
    ASSERT_EQ(encodeSessionOpenPayload(64, 65536 + 64, payload), 0);
    uint32_t slots = 0, size = 0;
    ASSERT_EQ(
        decodeSessionOpenPayload(payload.data(), payload.size(), slots, size),
        0);
    EXPECT_EQ(slots, 64u);
    EXPECT_EQ(size, 65536u + 64u);
}

TEST(CtrlFrameTest, RejectsBadMagic) {
    std::vector<uint8_t> junk(kCtrlFrameHeaderSize, 0xAB);
    CtrlFrame out;
    EXPECT_NE(decodeCtrlFrame(junk.data(), junk.size(), out), 0);
    EXPECT_FALSE(isCtrlFrameMagic(junk.data(), junk.size()));
}
