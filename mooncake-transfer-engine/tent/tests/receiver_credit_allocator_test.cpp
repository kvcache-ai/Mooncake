// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_allocator.h"

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {

constexpr size_t index(CreditResource resource) {
    return static_cast<size_t>(resource) - 1;
}

ReceiverCreditAllocatorConfig config(uint64_t bytes = 1000,
                                     uint64_t requests = 10) {
    ReceiverCreditAllocatorConfig config;
    config.capacity[index(CreditResource::DataBytes)] = bytes;
    config.capacity[index(CreditResource::RequestSlots)] = requests;
    config.max_grant_per_pull[index(CreditResource::DataBytes)] = bytes;
    config.max_grant_per_pull[index(CreditResource::RequestSlots)] = requests;
    config.max_entries = 64;
    config.ttl_ms = 1000;
    config.retry_after_us = 50;
    config.receiver_session_id = {11, 22};
    config.epoch = 7;
    return config;
}

std::unique_ptr<ReceiverCreditAllocator> allocator(
    const ReceiverCreditAllocatorConfig& allocator_config = config()) {
    std::unique_ptr<ReceiverCreditAllocator> allocator;
    EXPECT_TRUE(
        ReceiverCreditAllocator::create(allocator_config, allocator).ok());
    return allocator;
}

ReceiverCreditPullRequestV1 pullRequest(uint64_t peer, uint64_t sequence,
                                        uint64_t data_minimum,
                                        uint64_t data_desired,
                                        uint64_t slot_minimum,
                                        uint64_t slot_desired) {
    ReceiverCreditPullRequestV1 request;
    request.sender_peer = peer;
    request.request_sequence = sequence;
    if (sequence > 1) {
        request.expected_receiver_session_id = {11, 22};
        request.expected_epoch = 7;
        request.last_update_sequence = sequence - 1;
    }
    request.resources = {
        {CreditResource::DataBytes, 0, 0, data_minimum, data_desired},
        {CreditResource::RequestSlots, 0, 0, slot_minimum, slot_desired},
    };
    return request;
}

uint64_t grant(const ReceiverCreditPullResponseV1& response,
               CreditResource resource) {
    for (const auto& amount : response.update.grants)
        if (amount.resource == resource) return amount.grant_total;
    ADD_FAILURE() << "missing full credit resource";
    return 0;
}

TEST(ReceiverCreditAllocator, RejectsInvalidConfigurationAtomically) {
    auto valid = config();
    std::unique_ptr<ReceiverCreditAllocator> output;
    ASSERT_TRUE(ReceiverCreditAllocator::create(valid, output).ok());
    auto* original = output.get();

    auto invalid = valid;
    invalid.receiver_session_id = {};
    EXPECT_TRUE(
        ReceiverCreditAllocator::create(invalid, output).IsInvalidArgument());
    EXPECT_EQ(output.get(), original);

    invalid = valid;
    invalid.capacity[index(CreditResource::StagingSlots)] = 1;
    invalid.max_grant_per_pull[index(CreditResource::StagingSlots)] = 1;
    EXPECT_TRUE(
        ReceiverCreditAllocator::create(invalid, output).IsInvalidArgument());
    EXPECT_EQ(output.get(), original);

    invalid = valid;
    invalid.max_grant_per_pull[index(CreditResource::DataBytes)] = 1001;
    EXPECT_TRUE(
        ReceiverCreditAllocator::create(invalid, output).IsInvalidArgument());
    EXPECT_EQ(output.get(), original);
}

TEST(ReceiverCreditAllocator, FirstPullActivatesAndReturnsFullGrantVector) {
    auto receiver = allocator();
    auto request = pullRequest(101, 1, 100, 500, 1, 4);
    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(receiver->pull(request, response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Granted);
    EXPECT_EQ(response.activation.receiver_session_id,
              (ReceiverSessionId{11, 22}));
    EXPECT_EQ(response.activation.epoch, 7);
    EXPECT_EQ(response.update.grants.size(), kCreditResourceCount);
    EXPECT_EQ(grant(response, CreditResource::DataBytes), 500);
    EXPECT_EQ(grant(response, CreditResource::RequestSlots), 4);
    EXPECT_EQ(grant(response, CreditResource::StagingSlots), 0);

    ReceiverCreditAllocatorSnapshot snapshot;
    ASSERT_TRUE(receiver->snapshot(snapshot).ok());
    EXPECT_EQ(snapshot.committed[index(CreditResource::DataBytes)], 500);
    EXPECT_EQ(snapshot.free[index(CreditResource::DataBytes)], 500);
    EXPECT_EQ(snapshot.entries, 1);
}

TEST(ReceiverCreditAllocator, DuplicateIsIdempotentAndChangedBodyIsRejected) {
    auto receiver = allocator();
    auto request = pullRequest(101, 1, 100, 500, 1, 4);
    ReceiverCreditPullResponseV1 first;
    ReceiverCreditPullResponseV1 duplicate;
    ASSERT_TRUE(receiver->pull(request, first).ok());
    ASSERT_TRUE(receiver->pull(request, duplicate).ok());
    EXPECT_EQ(duplicate.status, ReceiverCreditPullStatus::Granted);
    EXPECT_EQ(duplicate.update.sequence, first.update.sequence);
    EXPECT_EQ(grant(duplicate, CreditResource::DataBytes), 500);

    request.resources[0].desired_available = 600;
    ReceiverCreditPullResponseV1 changed;
    ASSERT_TRUE(receiver->pull(request, changed).ok());
    EXPECT_EQ(changed.status, ReceiverCreditPullStatus::Rejected);
    EXPECT_EQ(grant(changed, CreditResource::DataBytes), 500);

    ReceiverCreditAllocatorSnapshot snapshot;
    ASSERT_TRUE(receiver->snapshot(snapshot).ok());
    EXPECT_EQ(snapshot.committed[index(CreditResource::DataBytes)], 500);
}

TEST(ReceiverCreditAllocator, OldSequenceAndFutureUpdateAreRejected) {
    auto receiver = allocator();
    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(
        receiver->pull(pullRequest(101, 1, 1, 100, 1, 1), response).ok());
    auto second = pullRequest(101, 2, 1, 100, 1, 1);
    second.last_update_sequence = 99;
    ASSERT_TRUE(receiver->pull(second, response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Rejected);

    auto old = pullRequest(101, 1, 1, 100, 1, 1);
    old.expected_receiver_session_id = {11, 22};
    old.expected_epoch = 7;
    ASSERT_TRUE(receiver->pull(old, response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Rejected);
}

TEST(ReceiverCreditAllocator, MinimumIsAtomicAcrossResources) {
    auto receiver = allocator();
    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(
        receiver->pull(pullRequest(101, 1, 900, 900, 1, 1), response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Granted);

    ASSERT_TRUE(
        receiver->pull(pullRequest(102, 1, 200, 200, 2, 2), response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Retry);
    EXPECT_EQ(response.retry_after_us, 50);
    EXPECT_EQ(grant(response, CreditResource::DataBytes), 0);
    EXPECT_EQ(grant(response, CreditResource::RequestSlots), 0);

    ReceiverCreditAllocatorSnapshot snapshot;
    ASSERT_TRUE(receiver->snapshot(snapshot).ok());
    EXPECT_EQ(snapshot.committed[index(CreditResource::DataBytes)], 900);
    EXPECT_EQ(snapshot.committed[index(CreditResource::RequestSlots)], 1);
}

TEST(ReceiverCreditAllocator, CompletionIsAppliedBeforeARejectedMinimum) {
    auto receiver = allocator();
    ReceiverCreditPullResponseV1 response;
    auto first = pullRequest(101, 1, 1000, 1000, 0, 0);
    ASSERT_TRUE(receiver->pull(first, response).ok());
    ASSERT_EQ(grant(response, CreditResource::DataBytes), 1000);

    ReceiverCreditPullRequestV1 next;
    next.sender_peer = 101;
    next.expected_receiver_session_id = {11, 22};
    next.expected_epoch = 7;
    next.request_sequence = 2;
    next.last_update_sequence = 1;
    next.resources = {
        {CreditResource::DataBytes, 1000, 500, 600, 600},
    };
    ASSERT_TRUE(receiver->pull(next, response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Retry);
    EXPECT_EQ(grant(response, CreditResource::DataBytes), 1000);

    ReceiverCreditAllocatorSnapshot snapshot;
    ASSERT_TRUE(receiver->snapshot(snapshot).ok());
    EXPECT_EQ(snapshot.committed[index(CreditResource::DataBytes)], 500);
    EXPECT_EQ(snapshot.free[index(CreditResource::DataBytes)], 500);
}

TEST(ReceiverCreditAllocator, CompletionAllowsAnotherSenderToAcquireBudget) {
    auto receiver = allocator();
    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(
        receiver->pull(pullRequest(101, 1, 1000, 1000, 10, 10), response).ok());
    ASSERT_TRUE(
        receiver->pull(pullRequest(102, 1, 1, 500, 1, 5), response).ok());
    ASSERT_EQ(response.status, ReceiverCreditPullStatus::Retry);

    ReceiverCreditPullRequestV1 release;
    release.sender_peer = 101;
    release.expected_receiver_session_id = {11, 22};
    release.expected_epoch = 7;
    release.request_sequence = 2;
    release.last_update_sequence = 1;
    release.resources = {
        {CreditResource::DataBytes, 1000, 1000, 0, 0},
        {CreditResource::RequestSlots, 10, 10, 0, 0},
    };
    ASSERT_TRUE(receiver->pull(release, response).ok());
    ASSERT_EQ(response.status, ReceiverCreditPullStatus::Granted);

    auto retry = pullRequest(102, 2, 1, 500, 1, 5);
    ASSERT_TRUE(receiver->pull(retry, response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Granted);
    EXPECT_EQ(grant(response, CreditResource::DataBytes), 500);
    EXPECT_EQ(grant(response, CreditResource::RequestSlots), 5);
}

TEST(ReceiverCreditAllocator, MaxGrantCapsDesiredButPreservesMinimum) {
    auto limited = config();
    limited.max_grant_per_pull[index(CreditResource::DataBytes)] = 200;
    limited.max_grant_per_pull[index(CreditResource::RequestSlots)] = 2;
    auto receiver = allocator(limited);
    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(
        receiver->pull(pullRequest(101, 1, 100, 900, 1, 9), response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Granted);
    EXPECT_EQ(grant(response, CreditResource::DataBytes), 200);
    EXPECT_EQ(grant(response, CreditResource::RequestSlots), 2);
}

TEST(ReceiverCreditAllocator, RestartAndUnsupportedQosNeverAllocate) {
    auto receiver = allocator();
    auto stale = pullRequest(101, 1, 100, 500, 1, 4);
    stale.expected_receiver_session_id = {99, 100};
    stale.expected_epoch = 1;
    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(receiver->pull(stale, response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::SessionChanged);
    EXPECT_EQ(grant(response, CreditResource::DataBytes), 0);

    auto unsupported = pullRequest(102, 1, 100, 500, 1, 4);
    unsupported.qos_class = 1;
    ASSERT_TRUE(receiver->pull(unsupported, response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Unsupported);

    ReceiverCreditAllocatorSnapshot snapshot;
    ASSERT_TRUE(receiver->snapshot(snapshot).ok());
    EXPECT_EQ(snapshot.entries, 0);
    EXPECT_EQ(snapshot.committed[index(CreditResource::DataBytes)], 0);
}

TEST(ReceiverCreditAllocator, InitialSessionRestartReclaimsOutstandingGrant) {
    auto receiver = allocator();
    ReceiverCreditPullResponseV1 response;

    ASSERT_TRUE(
        receiver->pull(pullRequest(101, 1, 100, 500, 1, 4), response).ok());
    ASSERT_EQ(response.status, ReceiverCreditPullStatus::Granted);
    EXPECT_EQ(grant(response, CreditResource::DataBytes), 500);

    auto restart = pullRequest(101, 2, 100, 600, 1, 5);
    restart.expected_receiver_session_id = {};
    restart.expected_epoch = 0;
    restart.last_update_sequence = 0;
    ASSERT_TRUE(receiver->pull(restart, response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Granted);
    EXPECT_EQ(grant(response, CreditResource::DataBytes), 600);
    EXPECT_EQ(grant(response, CreditResource::RequestSlots), 5);

    ReceiverCreditAllocatorSnapshot snapshot;
    ASSERT_TRUE(receiver->snapshot(snapshot).ok());
    EXPECT_EQ(snapshot.entries, 1);
    EXPECT_EQ(snapshot.committed[index(CreditResource::DataBytes)], 600);
    EXPECT_EQ(snapshot.committed[index(CreditResource::RequestSlots)], 5);
    EXPECT_EQ(snapshot.free[index(CreditResource::DataBytes)], 400);
}

TEST(ReceiverCreditAllocator, EntryTableIsBounded) {
    auto bounded = config();
    bounded.max_entries = 1;
    auto receiver = allocator(bounded);
    ReceiverCreditPullResponseV1 response;
    ASSERT_TRUE(receiver->pull(pullRequest(101, 1, 1, 1, 1, 1), response).ok());
    ASSERT_TRUE(receiver->pull(pullRequest(102, 1, 1, 1, 1, 1), response).ok());
    EXPECT_EQ(response.status, ReceiverCreditPullStatus::Retry);
    EXPECT_EQ(response.retry_after_us, 50);

    ReceiverCreditAllocatorSnapshot snapshot;
    ASSERT_TRUE(receiver->snapshot(snapshot).ok());
    EXPECT_EQ(snapshot.entries, 1);
}

TEST(ReceiverCreditAllocator, ConcurrentPullsNeverExceedGlobalBudget) {
    auto concurrent = config(1000, 1000);
    concurrent.max_entries = 128;
    auto receiver = allocator(concurrent);
    std::atomic<size_t> granted{0};
    std::vector<std::thread> threads;
    for (uint64_t peer = 1; peer <= 64; ++peer) {
        threads.emplace_back([&, peer] {
            ReceiverCreditPullResponseV1 response;
            auto request = pullRequest(peer, 1, 100, 100, 1, 1);
            if (receiver->pull(request, response).ok() &&
                response.status == ReceiverCreditPullStatus::Granted)
                ++granted;
        });
    }
    for (auto& thread : threads) thread.join();

    ReceiverCreditAllocatorSnapshot snapshot;
    ASSERT_TRUE(receiver->snapshot(snapshot).ok());
    EXPECT_EQ(granted, 10);
    EXPECT_EQ(snapshot.committed[index(CreditResource::DataBytes)], 1000);
    EXPECT_EQ(snapshot.free[index(CreditResource::DataBytes)], 0);
    EXPECT_EQ(snapshot.committed[index(CreditResource::RequestSlots)], 10);
    EXPECT_EQ(snapshot.entries, 64);
}

}  // namespace
}  // namespace mooncake::tent
