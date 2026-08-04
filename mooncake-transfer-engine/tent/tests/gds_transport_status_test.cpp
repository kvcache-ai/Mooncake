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

#include <cstdint>
#include <vector>

#include "tent/transport/gds/gds_transport.h"

namespace mooncake {
namespace tent {

class GdsTransportTestPeer {
   public:
    static TransferStatus aggregate(const std::vector<CUfileIOEvents_t>& events,
                                    bool& all_terminal) {
        return GdsTransport::aggregateTransferStatus(events, 0, events.size(),
                                                     all_terminal);
    }
};

namespace {

CUfileIOEvents_t makeEvent(CUfileStatus_t status, int64_t bytes = 0) {
    CUfileIOEvents_t event{};
    event.status = status;
    event.ret = bytes;
    return event;
}

TEST(GdsTransportStatusTest, ReportsFailureWhileSiblingIsActive) {
    bool all_terminal = true;
    auto status = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_FAILED), makeEvent(CUFILE_WAITING)}, all_terminal);

    EXPECT_EQ(status.s, FAILED);
    EXPECT_FALSE(all_terminal);
}

TEST(GdsTransportStatusTest, ReportsPendingWhenNoFailureIsKnown) {
    bool all_terminal = true;
    auto status = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_COMPLETE, 1024), makeEvent(CUFILE_PENDING)},
        all_terminal);

    EXPECT_EQ(status.s, PENDING);
    EXPECT_EQ(status.transferred_bytes, 1024);
    EXPECT_FALSE(all_terminal);
}

TEST(GdsTransportStatusTest, AggregatesCompletedBytes) {
    bool all_terminal = false;
    auto status = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_COMPLETE, 1024), makeEvent(CUFILE_COMPLETE, 2048)},
        all_terminal);

    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 3072);
    EXPECT_TRUE(all_terminal);
}

TEST(GdsTransportStatusTest, FailurePrecedenceIsCompletionOrderIndependent) {
    bool all_terminal = false;
    auto first = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_CANCELED), makeEvent(CUFILE_FAILED)}, all_terminal);
    EXPECT_EQ(first.s, FAILED);
    EXPECT_TRUE(all_terminal);

    auto second = GdsTransportTestPeer::aggregate(
        {makeEvent(CUFILE_FAILED), makeEvent(CUFILE_CANCELED)}, all_terminal);
    EXPECT_EQ(second.s, FAILED);
    EXPECT_TRUE(all_terminal);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
