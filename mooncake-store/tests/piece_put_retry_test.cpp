#include "../src/piece_put_retry.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace mooncake::test {
namespace {

using piece_put_detail::MapFinishResult;
using piece_put_detail::RetryMetadata;

PiecePutStartRequest Start(uint64_t op_id) {
    PiecePutStartRequest request;
    request.key = "kv/" + std::to_string(op_id);
    request.total_size = 8192;
    request.offset = 4096;
    request.length = 4096;
    request.write_id = "computation/attempt1";
    request.op_id = op_id;
    request.config.replica_num = 2;
    return request;
}

PiecePutCommitHandle Handle() {
    PiecePutCommitHandle handle;
    handle.key = "kv/block";
    handle.tenant_id = "tenant-a";
    handle.master_epoch = UUID{1, 2};
    handle.generation = 7;
    handle.write_start_ticks = 11;
    handle.write_id = "computation/attempt1";
    handle.client_id = UUID{3, 4};
    handle.op_id = 9;
    handle.offset = 4096;
    handle.length = 4096;
    return handle;
}

TEST(PiecePutRetryTest, EmptyMetadataBatchDoesNotSend) {
    size_t sends = 0;
    auto results = RetryMetadata<PiecePutStartResponse>(
        std::vector<PiecePutStartRequest>{}, [&](const auto&) {
            ++sends;
            return std::vector<
                tl::expected<PiecePutStartResponse, ErrorCode>>{};
        });
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(sends, 0);
}

TEST(PiecePutRetryTest, OnlyUncertainItemsRepeatWithIdenticalStartIdentity) {
    const std::vector<PiecePutStartRequest> requests{Start(1), Start(2),
                                                     Start(3)};
    std::vector<std::vector<uint64_t>> sent;
    auto results = RetryMetadata<PiecePutStartResponse>(
        requests, [&](const std::vector<PiecePutStartRequest>& retry) {
            std::vector<uint64_t> ids;
            std::vector<tl::expected<PiecePutStartResponse, ErrorCode>> replies;
            for (const auto& request : retry) {
                ids.push_back(request.op_id);
                const auto& original = requests[request.op_id - 1];
                EXPECT_EQ(request.key, original.key);
                EXPECT_EQ(request.write_id, original.write_id);
                EXPECT_EQ(request.total_size, original.total_size);
                EXPECT_EQ(request.offset, original.offset);
                EXPECT_EQ(request.length, original.length);
                EXPECT_EQ(request.config.replica_num, 2);
                if (request.op_id == 3) {
                    replies.emplace_back(
                        tl::unexpected(ErrorCode::INVALID_VERSION));
                } else if (request.op_id == 2 && sent.empty()) {
                    replies.emplace_back(
                        tl::unexpected(ErrorCode::RPC_TIMEOUT));
                } else {
                    PiecePutStartResponse response;
                    response.handle.op_id = request.op_id;
                    replies.emplace_back(std::move(response));
                }
            }
            sent.push_back(std::move(ids));
            return replies;
        });
    ASSERT_EQ(sent.size(), 2);
    EXPECT_EQ(sent[0], (std::vector<uint64_t>{1, 2, 3}));
    EXPECT_EQ(sent[1], (std::vector<uint64_t>{2}));
    ASSERT_EQ(results.size(), 3);
    ASSERT_TRUE(results[0]);
    ASSERT_TRUE(results[1]);
    EXPECT_EQ(results[0]->handle.op_id, 1);
    EXPECT_EQ(results[1]->handle.op_id, 2);
    ASSERT_FALSE(results[2]);
    EXPECT_EQ(results[2].error(), ErrorCode::INVALID_VERSION);
}

TEST(PiecePutRetryTest, StartUnknownStopsAfterThreeMetadataAttempts) {
    size_t sends = 0;
    const std::vector<PiecePutStartRequest> requests{Start(5)};
    auto results =
        RetryMetadata<PiecePutStartResponse>(requests, [&](const auto& retry) {
            ++sends;
            EXPECT_EQ(retry.size(), 1);
            EXPECT_EQ(retry[0].op_id, 5);
            return std::vector<tl::expected<PiecePutStartResponse, ErrorCode>>{
                tl::unexpected(ErrorCode::RPC_TIMEOUT)};
        });
    EXPECT_EQ(sends, 3);
    ASSERT_EQ(results.size(), 1);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::RPC_TIMEOUT);
}

TEST(PiecePutRetryTest, UnconfirmedSuccessfulDataPreservesMetadataOnlyHandle) {
    const auto original = Handle();
    size_t sends = 0;
    const std::vector<PiecePutFinishRequest> requests{{original, true}};
    auto replies =
        RetryMetadata<PiecePutFinishResponse>(requests, [&](const auto& retry) {
            ++sends;
            EXPECT_TRUE(retry[0].applied);
            EXPECT_EQ(retry[0].handle.client_id, original.client_id);
            EXPECT_EQ(retry[0].handle.op_id, original.op_id);
            EXPECT_EQ(retry[0].handle.write_id, original.write_id);
            EXPECT_EQ(retry[0].handle.generation, original.generation);
            EXPECT_EQ(retry[0].handle.master_epoch, original.master_epoch);
            return std::vector<tl::expected<PiecePutFinishResponse, ErrorCode>>{
                tl::unexpected(ErrorCode::RPC_FAIL)};
        });
    EXPECT_EQ(sends, 3);
    auto result = MapFinishResult(original, replies[0]);
    ASSERT_TRUE(result);
    EXPECT_FALSE(result->piece_completed);
    EXPECT_FALSE(result->object_published);
    ASSERT_TRUE(result->pending_commit);
    EXPECT_EQ(result->pending_commit->op_id, original.op_id);
    EXPECT_EQ(result->pending_commit->write_start_ticks,
              original.write_start_ticks);
    EXPECT_EQ(result->pending_commit->offset, original.offset);
    EXPECT_EQ(result->pending_commit->length, original.length);
}

TEST(PiecePutRetryTest, FailedDataNeverProducesACommitHandle) {
    const auto handle = Handle();
    for (auto reply :
         std::vector<tl::expected<PiecePutFinishResponse, ErrorCode>>{
             tl::unexpected(ErrorCode::RPC_TIMEOUT),
             tl::unexpected(ErrorCode::RPC_FAIL),
             tl::unexpected(ErrorCode::WRITE_FAIL),
             PiecePutFinishResponse{true, false}}) {
        auto result = MapFinishResult(handle, reply, ErrorCode::TRANSFER_FAIL);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::TRANSFER_FAIL);
    }
}

TEST(PiecePutRetryTest, SuccessfulConfirmationClearsPendingHandle) {
    const auto handle = Handle();
    for (const bool published : {false, true}) {
        auto result =
            MapFinishResult(handle, PiecePutFinishResponse{true, published});
        ASSERT_TRUE(result);
        EXPECT_TRUE(result->piece_completed);
        EXPECT_EQ(result->object_published, published);
        EXPECT_FALSE(result->pending_commit);
    }
}

TEST(PiecePutRetryTest, CommitTerminalErrorReturnsErrorWithoutRecoveryHandle) {
    const auto handle = Handle();
    for (const auto error :
         {ErrorCode::INVALID_VERSION, ErrorCode::LEASE_EXPIRED,
          ErrorCode::ILLEGAL_CLIENT}) {
        auto result = MapFinishResult(handle, tl::unexpected(error));
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), error);
    }
}

TEST(PiecePutRetryTest, GrantTimingUsesTheSuccessfulAttempt) {
    using Clock = std::chrono::steady_clock;
    using namespace std::chrono_literals;
    size_t attempts = 0;
    Clock::time_point first_entered, second_entered;
    std::vector<Clock::time_point> send_times;
    auto grants = RetryMetadata<PiecePutStartResponse>(
        std::vector<PiecePutStartRequest>{Start(1)},
        [&](const auto& requests) {
            ++attempts;
            EXPECT_EQ(send_times.size(), 1);
            EXPECT_EQ(send_times[0], Clock::time_point{});
            std::vector<tl::expected<PiecePutStartResponse, ErrorCode>> replies;
            if (attempts == 1) {
                first_entered = Clock::now();
                replies.emplace_back(tl::unexpected(ErrorCode::RPC_TIMEOUT));
            } else {
                second_entered = Clock::now();
                PiecePutStartResponse response;
                response.handle.op_id = requests[0].op_id;
                response.remaining_ms = 15000;
                replies.emplace_back(std::move(response));
            }
            return replies;
        },
        &send_times);
    ASSERT_EQ(attempts, 2);
    ASSERT_EQ(grants.size(), 1);
    ASSERT_TRUE(grants[0]);
    EXPECT_GE(send_times[0], first_entered);
    EXPECT_LE(send_times[0], second_entered);
    // Check the selected attempt's budget without sleeping for a timeout.
    EXPECT_FALSE(piece_put_detail::GrantExpired(*grants[0], send_times[0],
                                                send_times[0] + 1s));
    EXPECT_TRUE(piece_put_detail::GrantExpired(*grants[0], send_times[0],
                                               send_times[0] + 15s));
}

TEST(PiecePutRetryTest, EarlierSuccessAgesWhileOtherItemsRetry) {
    using Clock = std::chrono::steady_clock;
    using namespace std::chrono_literals;
    size_t attempts = 0;
    Clock::time_point early_send, second_entered;
    std::vector<Clock::time_point> send_times;
    auto grants = RetryMetadata<PiecePutStartResponse>(
        std::vector<PiecePutStartRequest>{Start(1), Start(2)},
        [&](const auto& requests) {
            ++attempts;
            std::vector<tl::expected<PiecePutStartResponse, ErrorCode>> replies;
            for (const auto& request : requests) {
                if (request.op_id == 2 && attempts == 1) {
                    replies.emplace_back(
                        tl::unexpected(ErrorCode::RPC_TIMEOUT));
                } else {
                    PiecePutStartResponse response;
                    response.handle.op_id = request.op_id;
                    response.remaining_ms = request.op_id == 1 ? 10000 : 15000;
                    replies.emplace_back(std::move(response));
                }
            }
            if (attempts == 2) {
                EXPECT_EQ(requests.size(), 1);
                EXPECT_EQ(requests[0].op_id, 2);
                early_send = send_times[0];
                EXPECT_NE(early_send, Clock::time_point{});
                EXPECT_EQ(send_times[1], Clock::time_point{});
                second_entered = Clock::now();
            }
            return replies;
        },
        &send_times);
    ASSERT_EQ(attempts, 2);
    ASSERT_EQ(grants.size(), 2);
    ASSERT_TRUE(grants[0]);
    ASSERT_TRUE(grants[1]);
    EXPECT_EQ(send_times[0], early_send);
    EXPECT_GE(send_times[1], early_send);
    EXPECT_LE(send_times[1], second_entered);
    // One common synthetic validation time lets the early, shorter grant
    // expire while the later grant remains valid, regardless of CPU scheduling.
    const auto now = std::max(send_times[0] + 11s, send_times[1] + 1s);
    EXPECT_TRUE(piece_put_detail::GrantExpired(*grants[0], send_times[0], now));
    EXPECT_FALSE(
        piece_put_detail::GrantExpired(*grants[1], send_times[1], now));
}

TEST(PiecePutRetryTest, GrantTimingPreservesErrorsAndZeroBudget) {
    using Clock = std::chrono::steady_clock;
    std::vector<Clock::time_point> send_times;
    const auto grants = RetryMetadata<PiecePutStartResponse>(
        std::vector<PiecePutStartRequest>{Start(1), Start(2), Start(3)},
        [](const auto& requests) {
            std::vector<tl::expected<PiecePutStartResponse, ErrorCode>> replies;
            for (const auto& request : requests) {
                if (request.op_id == 1)
                    replies.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
                else if (request.op_id == 2)
                    replies.emplace_back(PiecePutStartResponse{});
                else
                    replies.emplace_back(
                        tl::unexpected(ErrorCode::INVALID_VERSION));
            }
            return replies;
        },
        &send_times);
    ASSERT_EQ(grants.size(), 3);
    ASSERT_EQ(send_times.size(), 3);
    ASSERT_FALSE(grants[0]);
    EXPECT_EQ(grants[0].error(), ErrorCode::RPC_FAIL);
    EXPECT_EQ(send_times[0], Clock::time_point{});
    ASSERT_TRUE(grants[1]);
    EXPECT_NE(send_times[1], Clock::time_point{});
    EXPECT_TRUE(piece_put_detail::GrantExpired(*grants[1], send_times[1],
                                               send_times[1]));
    ASSERT_FALSE(grants[2]);
    EXPECT_EQ(grants[2].error(), ErrorCode::INVALID_VERSION);
    EXPECT_EQ(send_times[2], Clock::time_point{});
}

}  // namespace
}  // namespace mooncake::test
