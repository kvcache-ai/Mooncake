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

// The typed frame codec, the sender's stamps and the receiver's filter.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "tent/common/config.h"
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

TEST(NotifyDedupFilterTest, InOrderIsAdmittedOnce) {
    NotifyDedupFilter filter;
    for (uint64_t seq = 1; seq <= 10; ++seq) {
        EXPECT_TRUE(filter.admit(7, seq)) << seq;
        EXPECT_FALSE(filter.admit(7, seq)) << seq;
    }
    EXPECT_EQ(filter.sessions(), 1u);
}

TEST(NotifyDedupFilterTest, ReorderedIsAdmitted) {
    NotifyDedupFilter filter;
    EXPECT_TRUE(filter.admit(7, 5));
    EXPECT_TRUE(filter.admit(7, 3));
    EXPECT_TRUE(filter.admit(7, 4));
    EXPECT_FALSE(filter.admit(7, 4));
    EXPECT_TRUE(filter.admit(7, 1));
    EXPECT_FALSE(filter.admit(7, 5));
}

TEST(NotifyDedupFilterTest, ALateFirstDeliveryFarBehindIsAdmitted) {
    NotifyDedupFilter filter;
    EXPECT_TRUE(filter.admit(7, 1));
    EXPECT_TRUE(filter.admit(7, 100001));
    EXPECT_EQ(filter.holes(), 1u);
    const uint64_t late[] = {50000, 2,     100000, 4097,  99998,
                             3,     77777, 34464,  99999, 65537};
    for (uint64_t seq : late) EXPECT_TRUE(filter.admit(7, seq)) << seq;
    for (uint64_t seq : late) EXPECT_FALSE(filter.admit(7, seq)) << seq;
}

TEST(NotifyDedupFilterTest, AFarJumpIsOneHole) {
    NotifyDedupFilter filter;
    EXPECT_TRUE(filter.admit(7, 1));
    EXPECT_TRUE(filter.admit(7, 1000000000));
    EXPECT_EQ(filter.holes(), 1u);
    EXPECT_TRUE(filter.admit(7, 2));
    EXPECT_TRUE(filter.admit(7, 999999999));
    EXPECT_EQ(filter.holes(), 1u);
}

TEST(NotifyDedupFilterTest, SessionsAreIndependent) {
    NotifyDedupFilter filter;
    EXPECT_TRUE(filter.admit(1, 1));
    EXPECT_TRUE(filter.admit(2, 1));
    EXPECT_FALSE(filter.admit(1, 1));
    EXPECT_FALSE(filter.admit(2, 1));
    EXPECT_EQ(filter.sessions(), 2u);
}

TEST(NotifyDedupFilterTest, UnstampedAlwaysPasses) {
    NotifyDedupFilter filter;
    EXPECT_TRUE(filter.admit(0, 0));
    EXPECT_TRUE(filter.admit(0, 0));
    EXPECT_TRUE(filter.admit(0, 5));
    EXPECT_TRUE(filter.admit(5, 0));
    EXPECT_EQ(filter.sessions(), 0u);
}

TEST(NotifyDedupFilterTest, IdleSessionsAreForgotten) {
    NotifyDedupFilter filter;
    const auto t0 = NotifyDedupFilter::Clock::now();
    EXPECT_TRUE(filter.admit(1, 1, t0));
    // Pruning runs every kPruneEvery admissions; drive it with another session.
    for (size_t i = 1; i <= NotifyDedupFilter::kPruneEvery; ++i) {
        EXPECT_TRUE(filter.admit(2, i, t0 + 11min));
    }
    EXPECT_EQ(filter.sessions(), 1u);
    // Session 1 was forgotten, so its old number is new again.
    EXPECT_TRUE(filter.admit(1, 1, t0 + 11min));
}

TEST(NotifyDedupFilterTest, SkippedSequencesBecomeHolesAndArriveOnce) {
    NotifyDedupFilter filter;
    EXPECT_TRUE(filter.admit(7, 2));
    EXPECT_TRUE(filter.admit(7, 10));
    EXPECT_TRUE(filter.admit(7, 13));
    EXPECT_EQ(filter.holes(), 3u);
    const uint64_t late[] = {12, 1, 5, 3, 9, 11, 4, 6, 8, 7};
    for (uint64_t seq : late) {
        EXPECT_TRUE(filter.admit(7, seq)) << seq;
        EXPECT_FALSE(filter.admit(7, seq)) << seq;
    }
    EXPECT_EQ(filter.holes(), 0u);
    for (uint64_t seq = 1; seq <= 13; ++seq)
        EXPECT_FALSE(filter.admit(7, seq)) << seq;
}

// A receiver that restarted sees each sender mid-stream, while the sender may
// still hold any older one for a retry: all of them get in, once.
TEST(NotifyDedupFilterTest, AnythingBelowAFirstSeqIsAdmittedOnce) {
    NotifyDedupFilter filter;
    EXPECT_TRUE(filter.admit(8, 100000));
    EXPECT_EQ(filter.holes(), 1u);
    const uint64_t late[] = {1, 100000 - 4097, 99999, 2, 50000};
    for (uint64_t seq : late) EXPECT_TRUE(filter.admit(8, seq)) << seq;
    for (uint64_t seq : late) EXPECT_FALSE(filter.admit(8, seq)) << seq;
    EXPECT_FALSE(filter.admit(8, 100000));
}

// Nothing below a first seq is known to be missing, so giving that range up,
// past the ttl or the cap, is not counted; a gap after it still is.
TEST(NotifyDedupFilterTest, TheRangeBelowAFirstSeqIsGivenUpUncounted) {
    NotifyDedupFilter filter;
    NotifyDedupFilter::Config config;
    config.max_holes = 1;
    filter.configure(config);
    const auto t0 = NotifyDedupFilter::Clock::now();
    EXPECT_TRUE(filter.admit(7, 10, t0));
    EXPECT_TRUE(filter.admit(7, 20, t0));  // evicts 1..9
    EXPECT_EQ(filter.evicted(), 0u);
    EXPECT_TRUE(filter.admit(7, 30, t0));  // evicts 11..19
    EXPECT_EQ(filter.evicted(), 1u);
    EXPECT_TRUE(filter.admit(8, 10, t0));
    EXPECT_FALSE(filter.admit(8, 5, t0 + config.ttl + 1ms));
    EXPECT_EQ(filter.expired(), 0u);
}

// A session is forgotten only once its holes have outlived the ttl, and they
// are counted then: a ttl longer than the idle limit keeps the session.
TEST(NotifyDedupFilterTest, AForgottenSessionCountsItsHoles) {
    NotifyDedupFilter filter;
    NotifyDedupFilter::Config config;
    config.ttl = 30min;
    filter.configure(config);
    const auto t0 = NotifyDedupFilter::Clock::now();
    EXPECT_TRUE(filter.admit(7, 1, t0));
    EXPECT_TRUE(filter.admit(7, 10, t0));
    uint64_t other = 0;
    auto prune = [&](NotifyDedupFilter::Clock::time_point now) {
        for (size_t i = 0; i < NotifyDedupFilter::kPruneEvery; ++i)
            EXPECT_TRUE(filter.admit(8, ++other, now));
    };
    prune(t0 + 20min);
    EXPECT_EQ(filter.sessions(), 2u);
    EXPECT_TRUE(filter.admit(7, 5, t0 + 20min));  // leaves 2..4 and 6..9
    prune(t0 + 51min);
    EXPECT_EQ(filter.sessions(), 1u);
    EXPECT_EQ(filter.expired(), 2u);
}

TEST(NotifyDedupFilterTest, AnExpiredHoleIsRefused) {
    NotifyDedupFilter filter;
    const auto ttl = NotifyDedupFilter::Config().ttl;
    const auto t0 = NotifyDedupFilter::Clock::now();
    EXPECT_TRUE(filter.admit(7, 1, t0));
    EXPECT_TRUE(filter.admit(7, 10, t0));
    EXPECT_TRUE(filter.admit(7, 20, t0 + 1min));
    EXPECT_TRUE(filter.admit(7, 2, t0 + ttl));
    EXPECT_FALSE(filter.admit(7, 3, t0 + ttl + 1ms));
    EXPECT_EQ(filter.expired(), 1u);
    EXPECT_FALSE(filter.admit(7, 4, t0 + ttl + 1ms));
    EXPECT_TRUE(filter.admit(7, 15, t0 + ttl + 1ms));
    EXPECT_EQ(filter.expired(), 1u);
    // Pruning gives up the rest of 11..19, though nothing arrived there late.
    for (size_t i = 1; i <= NotifyDedupFilter::kPruneEvery; ++i) {
        EXPECT_TRUE(filter.admit(8, i, t0 + ttl + 2min));
    }
    EXPECT_EQ(filter.holes(), 0u);
    EXPECT_EQ(filter.expired(), 3u);
    EXPECT_FALSE(filter.admit(7, 12, t0 + ttl + 2min));
    EXPECT_EQ(filter.expired(), 3u);
}

// Callers read the clock before the lock, so now can step back.
TEST(NotifyDedupFilterTest, AStaleClockDoesNotAgeANewHole) {
    NotifyDedupFilter filter;
    const auto ttl = NotifyDedupFilter::Config().ttl;
    const auto t0 = NotifyDedupFilter::Clock::now();
    EXPECT_TRUE(filter.admit(7, 1, t0 + 1s));
    EXPECT_TRUE(filter.admit(7, 10, t0));
    EXPECT_TRUE(filter.admit(7, 5, t0 + ttl + 1s));
    EXPECT_EQ(filter.expired(), 0u);
}

TEST(NotifyDedupFilterTest, HolesAreCappedAtTheOldest) {
    NotifyDedupFilter filter;
    NotifyDedupFilter::Config config;
    config.max_holes = 4;
    filter.configure(config);
    EXPECT_TRUE(filter.admit(7, 1));
    for (uint64_t seq = 3; seq <= 21; seq += 2)
        EXPECT_TRUE(filter.admit(7, seq));
    EXPECT_EQ(filter.holes(), 4u);
    EXPECT_EQ(filter.evicted(), 6u);
    for (uint64_t seq = 2; seq <= 12; seq += 2)
        EXPECT_FALSE(filter.admit(7, seq)) << seq;
    for (uint64_t seq = 14; seq <= 20; seq += 2)
        EXPECT_TRUE(filter.admit(7, seq)) << seq;
    EXPECT_EQ(filter.expired(), 0u);
}

// Each refusal and each counted hole reaches on_drop, in order.
TEST(NotifyDedupFilterTest, OnDropSeesEachRefusalAndCountedHole) {
    using Drop = NotifyDedupFilter::Drop;
    std::vector<Drop> drops;
    NotifyDedupFilter filter;
    NotifyDedupFilter::Config config;
    config.max_holes = 1;
    config.on_drop = [&](Drop drop) { drops.push_back(drop); };
    filter.configure(config);
    const auto t0 = NotifyDedupFilter::Clock::now();
    EXPECT_TRUE(filter.admit(7, 10, t0));
    EXPECT_TRUE(filter.admit(7, 20, t0));  // evicts 1..9, below the first
    EXPECT_FALSE(filter.admit(7, 20, t0));
    EXPECT_TRUE(filter.admit(7, 30, t0));  // evicts 11..19
    EXPECT_FALSE(filter.admit(7, 25, t0 + config.ttl + 1ms));
    EXPECT_EQ(drops, (std::vector<Drop>{Drop::Duplicate, Drop::HoleEvicted,
                                        Drop::HoleExpired, Drop::Expired}));
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
    for (size_t i = 0; i < NotifySequencer::kPruneEvery; ++i)
        (void)sequencer.stamp("b", Notification{}, t0 + 11min);
    const Notification later = sequencer.stamp("a", Notification{}, t0 + 11min);
    EXPECT_NE(later.session, first.session);
    EXPECT_EQ(later.seq, 1u);
    EXPECT_EQ(sequencer.stamp("b", Notification{}, t0 + 11min).session,
              busy.session);  // an active target keeps its session
}

TEST(NotifyRecvBacklogTest, OutOfRangeKeepsTheDefault) {
    auto load = [](int64_t value) {
        Config conf;
        conf.set("notification/recv_backlog_max", value);
        return loadNotifyRecvBacklogMax(conf);
    };
    const int64_t limit = kNotifyRecvBacklogLimit;
    EXPECT_EQ(limit, 1048576);
    EXPECT_EQ(loadNotifyRecvBacklogMax(Config{}), 65536u);
    EXPECT_EQ(load(1), 1u);
    EXPECT_EQ(load(limit), kNotifyRecvBacklogLimit);
    const auto warnings = google::LogMessage::num_messages(google::WARNING);
    for (const int64_t value : {int64_t{0}, int64_t{-4}, limit + 1})
        EXPECT_EQ(load(value), 65536u) << value;
    EXPECT_EQ(google::LogMessage::num_messages(google::WARNING) - warnings, 3);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
