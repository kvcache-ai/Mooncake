// Copyright 2024 KVCache.AI
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

#include <vector>

#include "transport/nvmeof_transport/nvmeof_transport.h"

namespace mooncake {

class CUFileDescPoolTestPeer {
   public:
    static bool cachePolledEvent(std::vector<CUfileIOEvents_t>& events,
                                 const CUfileIOEvents_t& event) {
        return CUFileDescPool::cachePolledEvent(events, event);
    }
};

class NVMeoFTransportTestPeer {
   public:
    static std::unique_ptr<NVMeoFTransport> createWithoutDriver() {
        return std::unique_ptr<NVMeoFTransport>(
            new NVMeoFTransport(std::make_shared<CUFileDescPool>()));
    }

    static Transport::TransferStatus aggregate(
        const std::vector<Transport::TransferStatus>& statuses,
        bool& is_finished) {
        return NVMeoFTransport::aggregateTransferStatus(statuses, is_finished);
    }
};

TEST(NVMeoFStatusTest, RejectsUnsupportedMultiTransportSubmission) {
    auto transport = NVMeoFTransportTestPeer::createWithoutDriver();
    Transport::TransferTask task;

    auto status = transport->submitTransferTask({&task});

    EXPECT_TRUE(status.IsNotImplemented());
    EXPECT_EQ(status.message(),
              "NVMeoFTransport does not support MultiTransport batches");
    EXPECT_TRUE(task.is_finished);
}

TEST(NVMeoFStatusTest, ReportsKnownFailureBeforeAllSlicesFinish) {
    bool is_finished = true;
    auto waiting_status = NVMeoFTransportTestPeer::aggregate(
        {{Transport::FAILED, 0}, {Transport::WAITING, 0}}, is_finished);
    EXPECT_EQ(waiting_status.s, Transport::FAILED);
    EXPECT_FALSE(is_finished);

    auto pending_status = NVMeoFTransportTestPeer::aggregate(
        {{Transport::FAILED, 0}, {Transport::PENDING, 0}}, is_finished);
    EXPECT_EQ(pending_status.s, Transport::FAILED);
    EXPECT_FALSE(is_finished);
}

TEST(NVMeoFStatusTest, ReportsPendingOnlyWhenNoFailureIsKnown) {
    bool is_finished = true;
    auto status = NVMeoFTransportTestPeer::aggregate(
        {{Transport::COMPLETED, 1024}, {Transport::PENDING, 0}}, is_finished);

    EXPECT_EQ(status.s, Transport::PENDING);
    EXPECT_FALSE(is_finished);
}

TEST(NVMeoFStatusTest, AggregatesCompletedBytes) {
    bool is_finished = false;
    auto status = NVMeoFTransportTestPeer::aggregate(
        {{Transport::COMPLETED, 1024}, {Transport::COMPLETED, 2048}},
        is_finished);

    EXPECT_EQ(status.s, Transport::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 3072);
    EXPECT_TRUE(is_finished);
}

TEST(NVMeoFStatusTest, UsesDeterministicTerminalFailurePrecedence) {
    bool first_finished = false;
    auto first = NVMeoFTransportTestPeer::aggregate({{Transport::INVALID, 0},
                                                     {Transport::FAILED, 0},
                                                     {Transport::TIMEOUT, 0}},
                                                    first_finished);

    bool second_finished = false;
    auto second = NVMeoFTransportTestPeer::aggregate({{Transport::TIMEOUT, 0},
                                                      {Transport::FAILED, 0},
                                                      {Transport::INVALID, 0}},
                                                     second_finished);

    EXPECT_EQ(first.s, Transport::FAILED);
    EXPECT_EQ(second.s, Transport::FAILED);
    EXPECT_TRUE(first_finished);
    EXPECT_TRUE(second_finished);
}

TEST(NVMeoFStatusTest, CorrelatesPartialCompletionsByCookie) {
    std::vector<CUfileIOEvents_t> cached = {
        {.cookie = reinterpret_cast<void*>(1),
         .status = CUFILE_WAITING,
         .ret = 0},
        {.cookie = reinterpret_cast<void*>(2),
         .status = CUFILE_WAITING,
         .ret = 0}};
    CUfileIOEvents_t second = {.cookie = reinterpret_cast<void*>(2),
                               .status = CUFILE_COMPLETE,
                               .ret = 4096};

    ASSERT_TRUE(CUFileDescPoolTestPeer::cachePolledEvent(cached, second));
    EXPECT_EQ(cached[0].status, CUFILE_WAITING);
    EXPECT_EQ(cached[1].status, CUFILE_COMPLETE);
    EXPECT_EQ(cached[1].ret, 4096);
}

}  // namespace mooncake
