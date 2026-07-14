// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/control_plane.h"

#include <memory>
#include <string>

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {

constexpr size_t index(CreditResource resource) {
    return static_cast<size_t>(resource) - 1;
}

ReceiverCreditPullRequestV1 request() {
    ReceiverCreditPullRequestV1 value;
    value.sender_peer = 77;
    value.request_sequence = 1;
    value.resources = {
        {CreditResource::DataBytes, 0, 0, 4096, 8192},
        {CreditResource::RequestSlots, 0, 0, 1, 4},
    };
    return value;
}

std::shared_ptr<ReceiverCreditAllocator> allocator() {
    ReceiverCreditAllocatorConfig config;
    config.capacity[index(CreditResource::DataBytes)] = 1ULL << 20;
    config.capacity[index(CreditResource::RequestSlots)] = 64;
    config.max_grant_per_pull[index(CreditResource::DataBytes)] = 64ULL << 10;
    config.max_grant_per_pull[index(CreditResource::RequestSlots)] = 8;
    config.max_entries = 16;
    config.ttl_ms = 1000;
    config.retry_after_us = 100;
    config.receiver_session_id = {11, 22};
    config.epoch = 1;
    std::unique_ptr<ReceiverCreditAllocator> result;
    EXPECT_TRUE(ReceiverCreditAllocator::create(config, result).ok());
    return std::shared_ptr<ReceiverCreditAllocator>(std::move(result));
}

TEST(ReceiverCreditControlService, TypedPullReachesReceiverAllocator) {
    ControlService service("p2p", "", nullptr);
    service.setReceiverCreditAllocator(allocator());
    uint16_t port = 0;
    ASSERT_TRUE(service.start(port).ok());

    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(ControlClient::pullReceiverCredit(
                    "127.0.0.1:" + std::to_string(port), request(), response)
                    .ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Granted);
    EXPECT_EQ(response.activation.receiver_session_id,
              (ReceiverSessionId{11, 22}));
    ASSERT_EQ(response.update.grants.size(), kCreditResourceCount);
    EXPECT_EQ(response.update.grants[0].grant_total, 8192);
    EXPECT_EQ(response.update.grants[1].grant_total, 4);
}

TEST(ReceiverCreditControlService, DisabledReceiverRepliesUnsupported) {
    ControlService service("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(service.start(port).ok());

    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(ControlClient::pullReceiverCredit(
                    "127.0.0.1:" + std::to_string(port), request(), response)
                    .ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Unsupported);
    EXPECT_EQ(response.activation.schema_version, 0);
    EXPECT_TRUE(response.update.grants.empty());
}

TEST(ReceiverCreditControlService, MalformedPullGetsBoundedRejectedReply) {
    ControlService service("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(service.start(port).ok());

    CoroRpcAgent client;
    std::string response_wire;
    ASSERT_TRUE(client
                    .call("127.0.0.1:" + std::to_string(port),
                          PullReceiverCredit, "malformed", response_wire)
                    .ok());
    EXPECT_EQ(response_wire.size(),
              ReceiverCreditPullResponseCodecV1::kWireBytes);
    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(
        ReceiverCreditPullResponseCodecV1::decode(response_wire, response)
            .ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Rejected);
}

}  // namespace
}  // namespace mooncake::tent
