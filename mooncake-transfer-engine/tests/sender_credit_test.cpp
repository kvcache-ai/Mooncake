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

#include "error.h"
#include "transport/rdma_twosided/sender_credit.h"

using namespace mooncake;

namespace {

const char *kPeer = "127.0.0.1:12345";
constexpr uint64_t kSession = 99;
constexpr uint64_t kEpoch = 7;

void grant(SenderCreditLedger &l, uint64_t seq, uint64_t slots = 10,
           uint64_t bytes = 1000) {
    int disposition = -1;
    ASSERT_EQ(l.applyGrant(kPeer, kSession, kEpoch, seq,
                           {{CreditResource::BounceSlots, slots},
                            {CreditResource::BounceBytes, bytes}},
                           disposition),
              0);
    ASSERT_EQ(disposition, 0);
}

}  // namespace

TEST(SenderCreditTest, ReserveIsAtomicAcrossResources) {
    SenderCreditLedger l;
    ASSERT_EQ(l.activate(kPeer, kSession, kEpoch), 0);
    grant(l, 1, 2, 100);
    ASSERT_EQ(l.tryReserve(kPeer, kSession,
                           {{CreditResource::BounceSlots, 1},
                            {CreditResource::BounceBytes, 60}}),
              0);
    EXPECT_EQ(l.tryReserve(kPeer, kSession,
                           {{CreditResource::BounceSlots, 1},
                            {CreditResource::BounceBytes, 50}}),
              ERR_TOO_MANY_REQUESTS);
    uint64_t avail = 0;
    ASSERT_EQ(l.available(kPeer, kSession, CreditResource::BounceSlots, avail),
              0);
    EXPECT_EQ(avail, 1u);
}

TEST(SenderCreditTest, DuplicateGrantDoesNotMint) {
    SenderCreditLedger l;
    ASSERT_EQ(l.activate(kPeer, kSession, kEpoch), 0);
    grant(l, 2, 10, 100);
    int disposition = -1;
    ASSERT_EQ(l.applyGrant(kPeer, kSession, kEpoch, 2,
                           {{CreditResource::BounceSlots, 999}}, disposition),
              0);
    EXPECT_EQ(disposition, 1);
    uint64_t avail = 0;
    ASSERT_EQ(l.available(kPeer, kSession, CreditResource::BounceSlots, avail),
              0);
    EXPECT_EQ(avail, 10u);
}

TEST(SenderCreditTest, PartialGrantRetainsOmitted) {
    SenderCreditLedger l;
    ASSERT_EQ(l.activate(kPeer, kSession, kEpoch), 0);
    grant(l, 1, 5, 100);
    int disposition = -1;
    ASSERT_EQ(l.applyGrant(kPeer, kSession, kEpoch, 2,
                           {{CreditResource::BounceBytes, 160}}, disposition),
              0);
    EXPECT_EQ(disposition, 0);
    uint64_t bytes = 0, slots = 0;
    ASSERT_EQ(l.available(kPeer, kSession, CreditResource::BounceBytes, bytes),
              0);
    ASSERT_EQ(l.available(kPeer, kSession, CreditResource::BounceSlots, slots),
              0);
    EXPECT_EQ(bytes, 160u);
    EXPECT_EQ(slots, 5u);
}

TEST(SenderCreditTest, RollbackRestores) {
    SenderCreditLedger l;
    ASSERT_EQ(l.activate(kPeer, kSession, kEpoch), 0);
    grant(l, 1, 4, 400);
    ASSERT_EQ(l.tryReserve(kPeer, kSession,
                           {{CreditResource::BounceSlots, 2},
                            {CreditResource::BounceBytes, 200}}),
              0);
    ASSERT_EQ(l.rollbackReservation(kPeer, kSession,
                                    {{CreditResource::BounceSlots, 2},
                                     {CreditResource::BounceBytes, 200}}),
              0);
    uint64_t slots = 0;
    ASSERT_EQ(l.available(kPeer, kSession, CreditResource::BounceSlots, slots),
              0);
    EXPECT_EQ(slots, 4u);
}
