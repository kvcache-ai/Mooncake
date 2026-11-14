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

#include "tent/runtime/fenced_session.h"

#include <gtest/gtest.h>

#include <cstring>

namespace mooncake {
namespace tent {
namespace {

constexpr uint64_t kNow = 1'000'000'000ull;

SessionOptions Opts() {
    SessionOptions opts;
    opts.max_writers = 2;
    opts.lease_ttl = std::chrono::milliseconds(1000);
    return opts;
}

Status Open(FencedSessions& sessions, SessionHandle& handle) {
    return sessions.openSession(handle, 7, {{0x1000, 4096}}, Opts());
}

TEST(FencedSession, DisabledRejectsWithoutAdmitting) {
    FencedSessions sessions(false);
    SessionHandle handle = 0;
    EXPECT_TRUE(
        sessions.openSession(handle, 1, {{1, 1}}, Opts()).IsNotImplemented());
    WriterLease lease;
    EXPECT_TRUE(sessions.admitSubmission(lease, kNow, 1).IsNotImplemented());
    EXPECT_TRUE(sessions.submittedBatches(lease.session_id, 1).empty());
}

TEST(FencedSession, MintedLeaseAdmitsAndRecordsTheBatch) {
    FencedSessions sessions(true);
    SessionHandle handle = 0;
    ASSERT_TRUE(Open(sessions, handle).ok());
    WriterLease lease;
    ASSERT_TRUE(sessions.getLease(handle, lease, kNow).ok());
    EXPECT_EQ(lease.writer_id, 1u);
    EXPECT_EQ(lease.epoch, 1u);
    EXPECT_EQ(lease.expires_at_ns, kNow + 1000ull * 1000000ull);
    ASSERT_TRUE(sessions.admitSubmission(lease, kNow, 42).ok());
    EXPECT_EQ(sessions.submittedBatches(lease.session_id, lease.writer_id),
              std::vector<BatchID>({42}));
}

TEST(FencedSession, ExpiredOlderAndRevokedLeasesAreStale) {
    FencedSessions sessions(true);
    SessionHandle handle = 0;
    ASSERT_TRUE(Open(sessions, handle).ok());
    WriterLease lease;
    ASSERT_TRUE(sessions.getLease(handle, lease, kNow).ok());

    WriterLease expired = lease;
    expired.expires_at_ns = kNow;
    EXPECT_TRUE(sessions.admitSubmission(expired, kNow, 1).IsStaleLease());

    ASSERT_TRUE(sessions.admitSubmission(lease, kNow, 2).ok());
    WriterLease newer = lease;
    newer.epoch = 2;
    ASSERT_TRUE(sessions.admitSubmission(newer, kNow, 3).ok());
    EXPECT_TRUE(sessions.admitSubmission(lease, kNow, 4).IsStaleLease());
    lease = newer;

    ASSERT_TRUE(
        sessions.revokeWriter(lease.session_id, lease.writer_id, 2).ok());
    EXPECT_TRUE(sessions.admitSubmission(lease, kNow, 5).IsStaleLease());
    EXPECT_EQ(sessions.submittedBatches(lease.session_id, lease.writer_id),
              std::vector<BatchID>({3}));

    WriterLease renewed = lease;
    renewed.epoch = 3;
    renewed.expires_at_ns = kNow + 5000;
    ASSERT_TRUE(sessions.admitSubmission(renewed, kNow, 6).ok());
    EXPECT_EQ(sessions.submittedBatches(lease.session_id, lease.writer_id),
              std::vector<BatchID>({6}));
}

TEST(FencedSession, FanInWritersAreIndependent) {
    FencedSessions sessions(true);
    SessionHandle handle = 0;
    ASSERT_TRUE(Open(sessions, handle).ok());
    WriterLease first;
    WriterLease second;
    ASSERT_TRUE(sessions.getLease(handle, first, kNow).ok());
    ASSERT_TRUE(sessions.getLease(handle, second, kNow).ok());
    EXPECT_NE(first.writer_id, second.writer_id);
    ASSERT_TRUE(sessions.admitSubmission(first, kNow, 1).ok());
    ASSERT_TRUE(sessions.admitSubmission(second, kNow, 2).ok());
    EXPECT_TRUE(sessions.getLease(handle, first, kNow).IsTooManyRequests());
}

TEST(FencedSession, ZeroTtlDoesNotExpire) {
    FencedSessions sessions(true);
    SessionHandle handle = 0;
    SessionOptions opts = Opts();
    opts.lease_ttl = std::chrono::milliseconds(0);
    ASSERT_TRUE(sessions.openSession(handle, 1, {{1, 8}}, opts).ok());
    WriterLease lease;
    ASSERT_TRUE(sessions.getLease(handle, lease, kNow).ok());
    EXPECT_EQ(lease.expires_at_ns, ~uint64_t{0});
    EXPECT_TRUE(sessions.admitSubmission(lease, ~uint64_t{0} - 1, 9).ok());
}

TEST(FencedSession, HardFenceDoesNotChangeAdmission) {
    FencedSessions sessions(true);
    SessionHandle handle = 0;
    SessionOptions opts = Opts();
    opts.hard_fence = true;
    ASSERT_TRUE(sessions.openSession(handle, 1, {{1, 8}}, opts).ok());
    ASSERT_TRUE(sessions.openSession(handle, 1, {{1, 8}}, opts).ok());
    WriterLease lease;
    ASSERT_TRUE(sessions.getLease(handle, lease, kNow).ok());
    EXPECT_TRUE(sessions.admitSubmission(lease, kNow, 1).ok());
}

TEST(FencedSession, WireRoundTrip) {
    WriterLease lease;
    lease.session_id = {0x1122334455667788ull, 0x99aabbccddeeff00ull};
    lease.epoch = 4;
    lease.writer_id = 3;
    lease.expires_at_ns = 42;
    uint8_t bytes[WriterLease::kWireBytes];
    lease.serialize(bytes);
    EXPECT_EQ(bytes[0], WriterLease::kSchemaVersion);
    WriterLease parsed;
    ASSERT_TRUE(WriterLease::parse(bytes, sizeof(bytes), parsed).ok());
    EXPECT_EQ(parsed.session_id, lease.session_id);
    EXPECT_EQ(parsed.epoch, lease.epoch);
    EXPECT_EQ(parsed.writer_id, lease.writer_id);
    EXPECT_EQ(parsed.expires_at_ns, lease.expires_at_ns);

    bytes[0] = 2;
    EXPECT_TRUE(
        WriterLease::parse(bytes, sizeof(bytes), parsed).IsInvalidArgument());
}

TEST(FencedSession, EmptyRangeAndUnknownHandle) {
    FencedSessions sessions(true);
    SessionHandle handle = 0;
    EXPECT_TRUE(
        sessions.openSession(handle, 1, {}, Opts()).IsInvalidArgument());
    WriterLease lease;
    EXPECT_TRUE(sessions.getLease(99, lease, kNow).IsInvalidArgument());
}

TEST(FencedSession, ForgetDropsOnlyThatBatch) {
    FencedSessions sessions(true);
    SessionHandle handle = 0;
    ASSERT_TRUE(Open(sessions, handle).ok());
    WriterLease lease;
    ASSERT_TRUE(sessions.getLease(handle, lease, kNow).ok());
    ASSERT_TRUE(sessions.admitSubmission(lease, kNow, 1).ok());
    ASSERT_TRUE(sessions.admitSubmission(lease, kNow, 2).ok());
    sessions.forgetSubmission(lease, 1);
    EXPECT_EQ(sessions.submittedBatches(lease.session_id, lease.writer_id),
              std::vector<BatchID>({2}));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
