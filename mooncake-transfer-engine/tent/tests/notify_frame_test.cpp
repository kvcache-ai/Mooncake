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

// The typed frame codec, the sender's stamps and the receiver's window.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "tent/common/notify_frame.h"

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;

std::vector<char> encodeFrame(const NotifyFrameHeader& header,
                              const std::string& name, const std::string& msg) {
    std::vector<char> out(65536);
    uint32_t len = 0;
    EXPECT_TRUE(
        encodeNotifyFrame(header, name, msg, out.data(), out.size(), &len));
    out.resize(len);
    return out;
}

template <typename T>
void append(std::vector<char>& out, T value) {
    const size_t at = out.size();
    out.resize(at + sizeof(value));
    std::memcpy(out.data() + at, &value, sizeof(value));
}

TEST(NotifyFrameTest, RoundTrip) {
    NotifyFrameHeader header;
    header.session = 0x1122334455667788ull;
    header.seq = 99;
    const auto wire = encodeFrame(header, "req-7", "done");
    ASSERT_EQ(wire.size(), kNotifyFrameHeaderSize + 4 + 5 + 4 + 4);
    EXPECT_TRUE(isNotifyFrameMagic(wire.data(), wire.size()));

    NotifyFrameHeader out;
    std::string name, msg;
    ASSERT_TRUE(decodeNotifyFrame(wire.data(), wire.size(), &out, &name, &msg));
    EXPECT_EQ(out.version, kNotifyFrameVersion);
    EXPECT_EQ(out.type, kNotifyFrameTypeNotifyCompat);
    EXPECT_EQ(out.flags, 0);
    EXPECT_EQ(out.session, header.session);
    EXPECT_EQ(out.epoch, 0u);
    EXPECT_EQ(out.seq, 99u);
    EXPECT_EQ(out.ack_seq, 0u);
    EXPECT_EQ(name, "req-7");
    EXPECT_EQ(msg, "done");
}

// The classic NotifyCompatRoundTrip frame, written out from ctrl_frame.h.
TEST(NotifyFrameTest, MatchesTheClassicWireLayout) {
    std::vector<char> classic;
    append<uint32_t>(classic, 0x434B434Du);
    append<uint8_t>(classic, 1);
    append<uint8_t>(classic, 10);
    append<uint16_t>(classic, 0);
    append<uint64_t>(classic, 42);
    append<uint64_t>(classic, 7);
    append<uint64_t>(classic, 3);
    append<uint64_t>(classic, 0);
    append<uint32_t>(classic, 4 + 5 + 4 + 5);
    append<uint32_t>(classic, 5);
    classic.insert(classic.end(), {'h', 'e', 'l', 'l', 'o'});
    append<uint32_t>(classic, 5);
    classic.insert(classic.end(), {'w', 'o', 'r', 'l', 'd'});
    ASSERT_EQ(classic.size(), kNotifyFrameHeaderSize + 18);

    NotifyFrameHeader out;
    std::string name, msg;
    ASSERT_TRUE(
        decodeNotifyFrame(classic.data(), classic.size(), &out, &name, &msg));
    EXPECT_EQ(out.session, 42u);
    EXPECT_EQ(out.epoch, 7u);
    EXPECT_EQ(out.seq, 3u);
    EXPECT_EQ(name, "hello");
    EXPECT_EQ(msg, "world");

    NotifyFrameHeader header;
    header.session = 42;
    header.epoch = 7;
    header.seq = 3;
    EXPECT_EQ(encodeFrame(header, "hello", "world"), classic);
}

// A raw payload's leading name length can never read as the magic.
TEST(NotifyFrameTest, RawPayloadIsNotAFrame) {
    std::vector<char> raw(65536);
    uint32_t len = 0;
    ASSERT_TRUE(
        encodeNotifyPayload("name", "message", raw.data(), raw.size(), &len));
    raw.resize(len);
    EXPECT_FALSE(isNotifyFrameMagic(raw.data(), raw.size()));
    std::string name, msg;
    ASSERT_TRUE(decodeNotifyPayload(raw.data(), raw.size(), &name, &msg));
    EXPECT_EQ(name, "name");
    EXPECT_EQ(msg, "message");
    // Trailing bytes after a raw payload are ignored, as they always were.
    std::vector<char> padded = raw;
    padded.resize(raw.size() + 100, '\xee');
    EXPECT_TRUE(decodeNotifyPayload(padded.data(), padded.size(), &name, &msg));
    EXPECT_EQ(msg, "message");
}

TEST(NotifyFrameTest, RejectsWhatItDoesNotUnderstand) {
    NotifyFrameHeader header;
    header.session = 1;
    header.seq = 1;
    const auto good = encodeFrame(header, "n", "m");
    NotifyFrameHeader out;
    std::string name, msg;

    auto corrupt = [&](size_t offset, char value) {
        std::vector<char> copy = good;
        copy[offset] = value;
        return copy;
    };
    // magic
    auto bad = corrupt(0, 'X');
    EXPECT_FALSE(isNotifyFrameMagic(bad.data(), bad.size()));
    EXPECT_FALSE(decodeNotifyFrame(bad.data(), bad.size(), &out, &name, &msg));
    // version 2
    bad = corrupt(4, 2);
    EXPECT_FALSE(decodeNotifyFrame(bad.data(), bad.size(), &out, &name, &msg));
    // type CREDIT_GRANT
    bad = corrupt(5, 1);
    EXPECT_FALSE(decodeNotifyFrame(bad.data(), bad.size(), &out, &name, &msg));
    // unknown flag bit
    bad = corrupt(7, '\x80');
    EXPECT_FALSE(decodeNotifyFrame(bad.data(), bad.size(), &out, &name, &msg));
    // payload length disagrees with the frame length
    bad = good;
    bad.push_back('x');
    EXPECT_FALSE(decodeNotifyFrame(bad.data(), bad.size(), &out, &name, &msg));
    bad = good;
    bad.pop_back();
    EXPECT_FALSE(decodeNotifyFrame(bad.data(), bad.size(), &out, &name, &msg));
    // a trailing byte inside payload_len, which the classic codec rejects
    bad = good;
    bad.push_back('x');
    bad[kNotifyFrameHeaderSize - 4] += 1;
    EXPECT_FALSE(decodeNotifyFrame(bad.data(), bad.size(), &out, &name, &msg));
    // shorter than a header
    EXPECT_FALSE(decodeNotifyFrame(good.data(), 10, &out, &name, &msg));
    // the good one still decodes
    EXPECT_TRUE(decodeNotifyFrame(good.data(), good.size(), &out, &name, &msg));

    // Encoding refuses what it could not decode back, and what does not fit.
    NotifyFrameHeader unknown_flag;
    unknown_flag.flags = 0x8000;
    std::vector<char> buf(256);
    uint32_t len = 0;
    EXPECT_FALSE(encodeNotifyFrame(unknown_flag, "n", "m", buf.data(),
                                   buf.size(), &len));
    EXPECT_FALSE(encodeNotifyFrame(header, "n", std::string(300, 'z'),
                                   buf.data(), buf.size(), &len));
    // The payload alone fits, the header on top of it does not.
    EXPECT_FALSE(encodeNotifyFrame(header, "n", std::string(210, 'z'),
                                   buf.data(), buf.size(), &len));
}

TEST(NotifyDedupWindowTest, InOrderIsAdmittedOnce) {
    NotifyDedupWindow window;
    for (uint64_t seq = 1; seq <= 10; ++seq) {
        EXPECT_TRUE(window.admit(7, seq)) << seq;
        EXPECT_FALSE(window.admit(7, seq)) << seq;
    }
    EXPECT_EQ(window.sessions(), 1u);
}

TEST(NotifyDedupWindowTest, ReorderedInsideTheWindowIsAdmitted) {
    NotifyDedupWindow window;
    EXPECT_TRUE(window.admit(7, 5));
    EXPECT_TRUE(window.admit(7, 3));
    EXPECT_TRUE(window.admit(7, 4));
    EXPECT_FALSE(window.admit(7, 4));
    EXPECT_TRUE(window.admit(7, 1));
    EXPECT_FALSE(window.admit(7, 5));
}

TEST(NotifyDedupWindowTest, BehindTheWindowIsRefused) {
    NotifyDedupWindow window;
    const uint64_t w = NotifyDedupWindow::kWindow;
    EXPECT_TRUE(window.admit(7, 1));
    EXPECT_TRUE(window.admit(7, w + 1));  // slides past 1
    EXPECT_FALSE(window.admit(7, 1));     // too old
    EXPECT_TRUE(window.admit(7, 2));      // just inside, never seen
    EXPECT_FALSE(window.admit(7, 2));
    // A jump of more than a window forgets everything in between.
    EXPECT_TRUE(window.admit(7, 10 * w));
    EXPECT_TRUE(window.admit(7, 10 * w - 1));
    EXPECT_FALSE(window.admit(7, 10 * w - 1));
    EXPECT_FALSE(window.admit(7, 9 * w));
    // More than a window behind, on a slot that is free again.
    EXPECT_TRUE(window.admit(8, 1));
    EXPECT_TRUE(window.admit(8, w + 2));
    EXPECT_FALSE(window.admit(8, 1));
}

TEST(NotifyDedupWindowTest, SessionsAreIndependent) {
    NotifyDedupWindow window;
    EXPECT_TRUE(window.admit(1, 1));
    EXPECT_TRUE(window.admit(2, 1));
    EXPECT_FALSE(window.admit(1, 1));
    EXPECT_FALSE(window.admit(2, 1));
    EXPECT_EQ(window.sessions(), 2u);
}

TEST(NotifyDedupWindowTest, UnstampedAlwaysPasses) {
    NotifyDedupWindow window;
    EXPECT_TRUE(window.admit(0, 0));
    EXPECT_TRUE(window.admit(0, 0));
    EXPECT_TRUE(window.admit(0, 5));
    EXPECT_TRUE(window.admit(5, 0));
    EXPECT_EQ(window.sessions(), 0u);
}

TEST(NotifyDedupWindowTest, IdleSessionsAreForgotten) {
    NotifyDedupWindow window;
    const auto t0 = NotifyDedupWindow::Clock::now();
    EXPECT_TRUE(window.admit(1, 1, t0));
    // Pruning runs every kWindow admissions; drive it with another session.
    for (size_t i = 1; i <= NotifyDedupWindow::kWindow; ++i) {
        EXPECT_TRUE(window.admit(2, i, t0 + 11min));
    }
    EXPECT_EQ(window.sessions(), 1u);
    // Session 1 was forgotten, so its old number is new again.
    EXPECT_TRUE(window.admit(1, 1, t0 + 11min));
}

TEST(NotifyDedupWindowTest, SlideClearsSkippedBits) {
    NotifyDedupWindow window;
    const uint64_t w = NotifyDedupWindow::kWindow;
    EXPECT_TRUE(window.admit(7, 2));
    EXPECT_TRUE(window.admit(7, w));
    EXPECT_TRUE(window.admit(7, w + 3));  // slides over w + 1 and w + 2
    EXPECT_TRUE(window.admit(7, w + 2));  // not seen, despite 2's old bit
}

TEST(NotifySequencerTest, StampsPerTargetAndKeepsItsOwnStamp) {
    NotifySequencer sequencer;
    Notification notifi;
    notifi.name = "n";
    notifi.msg = "m";
    const Notification a1 = sequencer.stamp("a", notifi);
    const Notification a2 = sequencer.stamp("a", notifi);
    const Notification b1 = sequencer.stamp("b", notifi);
    EXPECT_NE(a1.session, 0u);
    EXPECT_EQ(a2.session, a1.session);
    EXPECT_NE(b1.session, a1.session);  // a session per target
    EXPECT_EQ(a1.seq, 1u);
    EXPECT_EQ(a2.seq, 2u);
    EXPECT_EQ(b1.seq, 1u);
    EXPECT_EQ(a1.msg, "m");
    // A resend keeps its stamp; a stamp from elsewhere is replaced.
    const Notification again = sequencer.stamp("a", a1);
    EXPECT_EQ(again.seq, 1u);
    EXPECT_EQ(again.session, a1.session);
    const Notification forwarded = sequencer.stamp("a", b1);
    EXPECT_EQ(forwarded.session, a1.session);
    EXPECT_EQ(forwarded.seq, 3u);
}

// An idle target is forgotten and comes back under a new session.
TEST(NotifySequencerTest, IdleTargetsGetANewSession) {
    NotifySequencer sequencer;
    const auto t0 = NotifySequencer::Clock::now();
    const Notification first = sequencer.stamp("a", Notification{}, t0);
    const Notification busy = sequencer.stamp("b", Notification{}, t0 + 11min);
    for (size_t i = 0; i < NotifyDedupWindow::kWindow; ++i)
        (void)sequencer.stamp("b", Notification{}, t0 + 11min);
    const Notification later = sequencer.stamp("a", Notification{}, t0 + 11min);
    EXPECT_NE(later.session, first.session);
    EXPECT_EQ(later.seq, 1u);
    EXPECT_EQ(sequencer.stamp("b", Notification{}, t0 + 11min).session,
              busy.session);  // an active target keeps its session
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
