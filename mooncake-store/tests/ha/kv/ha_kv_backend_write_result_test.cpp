#include "ha/kv/ha_kv_backend.h"
#include "ha/kv/redis_write_ack.h"

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

class LegacyWriteBackend : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view, std::string&) override {
        return ErrorCode::OK;
    }
    ErrorCode Put(std::string_view, std::string_view) override {
        return write_error;
    }
    ErrorCode Range(std::string_view, std::string_view, size_t,
                    std::vector<KvPair>&) override {
        return ErrorCode::OK;
    }
    ErrorCode DeleteRange(std::string_view, std::string_view) override {
        return write_error;
    }
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn&) override { return write_error; }

    ErrorCode write_error{ErrorCode::OK};
};

TEST(HaKvBackendWriteResultTest, LegacyErrorsKeepAmbiguousEtcdSemantics) {
    LegacyWriteBackend backend;

    EXPECT_EQ(KvWriteStatus::kCommitted,
              backend.PutWithResult("key", "value").status);

    backend.write_error = ErrorCode::ETCD_TRANSACTION_FAIL;
    const auto compare_failed = backend.TxnWithResult({});
    EXPECT_EQ(KvWriteStatus::kCompareFailed, compare_failed.status);
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, compare_failed.error);

    backend.write_error = ErrorCode::ETCD_OPERATION_ERROR;
    const auto unknown = backend.TxnWithResult({});
    EXPECT_EQ(KvWriteStatus::kOutcomeUnknown, unknown.status);
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, unknown.error);
}

TEST(HaKvBackendWriteResultTest, UnknownErrorsAreNeverAssumedUncommitted) {
    LegacyWriteBackend backend;
    for (const auto error :
         {ErrorCode::ETCD_OPERATION_ERROR, ErrorCode::ETCD_CTX_CANCELLED,
          ErrorCode::INTERNAL_ERROR}) {
        backend.write_error = error;
        EXPECT_EQ(KvWriteStatus::kOutcomeUnknown,
                  backend.PutWithResult("key", "value").status);
        EXPECT_EQ(KvWriteStatus::kOutcomeUnknown,
                  backend.DeleteRangeWithResult("key", "next").status);
        EXPECT_EQ(KvWriteStatus::kOutcomeUnknown,
                  backend.TxnWithResult({}).status);
    }
}

TEST(HaKvBackendWriteResultTest,
     LegacyAdapterCannotTurnUnknownIntoCompareFailure) {
    const KvWriteResult result{.status = KvWriteStatus::kOutcomeUnknown,
                               .error = ErrorCode::ETCD_TRANSACTION_FAIL};
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.ToLegacyError());
}

TEST(RedisWriteAckTest, AllConfiguredReplicasMustConfirm) {
    const auto result = ExecuteRedisWriteWithAck(
        {.replicas_required = 2}, [] { return KvWriteResult::Committed(); },
        [](size_t, std::chrono::milliseconds)
            -> tl::expected<size_t, ErrorCode> { return 2; });
    EXPECT_EQ(KvWriteStatus::kCommitted, result.status);
    EXPECT_EQ(ErrorCode::OK, result.error);
    EXPECT_EQ(2u, result.replicas_acked);
    EXPECT_EQ(2u, result.replicas_required);
}

TEST(RedisWriteAckTest, FailedOrUnknownExecutionNeverWaitsOrReplays) {
    for (const auto status :
         {KvWriteStatus::kRejected, KvWriteStatus::kFailedBeforeCommit,
          KvWriteStatus::kOutcomeUnknown}) {
        size_t script_calls = 0;
        size_t wait_calls = 0;
        const auto result = ExecuteRedisWriteWithAck(
            {.replicas_required = 1},
            [&] {
                ++script_calls;
                return KvWriteResult{.status = status,
                                     .error = ErrorCode::ETCD_OPERATION_ERROR};
            },
            [&](size_t,
                std::chrono::milliseconds) -> tl::expected<size_t, ErrorCode> {
                ++wait_calls;
                return 1;
            });
        EXPECT_EQ(status, result.status);
        EXPECT_EQ(1u, script_calls);
        EXPECT_EQ(0u, wait_calls);
    }
}

TEST(RedisWriteAckTest, UnsupportedAofPolicyIsRejectedBeforeWriting) {
    size_t script_calls = 0;
    const auto result = ExecuteRedisWriteWithAck(
        {.replicas_required = 1, .require_local_aof = true},
        [&] {
            ++script_calls;
            return KvWriteResult::Committed();
        },
        [](size_t, std::chrono::milliseconds)
            -> tl::expected<size_t, ErrorCode> { return 1; });
    EXPECT_EQ(KvWriteStatus::kRejected, result.status);
    EXPECT_EQ(ErrorCode::NOT_SUPPORTED, result.error);
    EXPECT_EQ(0u, script_calls);
}

TEST(RedisWriteAckTest, CompareFailureDoesNotWaitForReplicas) {
    size_t script_calls = 0;
    size_t wait_calls = 0;
    const auto result = ExecuteRedisWriteWithAck(
        KvWriteDurabilityPolicy{.replicas_required = 2},
        [&] {
            ++script_calls;
            return KvWriteResult::FromError(ErrorCode::ETCD_TRANSACTION_FAIL);
        },
        [&](size_t,
            std::chrono::milliseconds) -> tl::expected<size_t, ErrorCode> {
            ++wait_calls;
            return 2;
        });

    EXPECT_EQ(KvWriteStatus::kCompareFailed, result.status);
    EXPECT_EQ(1u, script_calls);
    EXPECT_EQ(0u, wait_calls);
}

TEST(RedisWriteAckTest, WaitMustReachConfiguredReplicaCount) {
    size_t script_calls = 0;
    size_t wait_calls = 0;
    const auto result = ExecuteRedisWriteWithAck(
        KvWriteDurabilityPolicy{
            .replicas_required = 2,
            .replica_ack_timeout = std::chrono::milliseconds(250)},
        [&] {
            ++script_calls;
            return KvWriteResult::Committed();
        },
        [&](size_t required, std::chrono::milliseconds timeout)
            -> tl::expected<size_t, ErrorCode> {
            ++wait_calls;
            EXPECT_EQ(2u, required);
            EXPECT_EQ(std::chrono::milliseconds(250), timeout);
            return 1;
        });

    EXPECT_EQ(KvWriteStatus::kOutcomeUnknown, result.status);
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.error);
    EXPECT_EQ(1u, result.replicas_acked);
    EXPECT_EQ(2u, result.replicas_required);
    EXPECT_EQ(1u, script_calls);
    EXPECT_EQ(1u, wait_calls);
}

TEST(RedisWriteAckTest, AckTimeoutIsUnknownAndDoesNotReplayScript) {
    size_t script_calls = 0;
    size_t wait_calls = 0;
    const auto result = ExecuteRedisWriteWithAck(
        KvWriteDurabilityPolicy{.replicas_required = 1},
        [&] {
            ++script_calls;
            return KvWriteResult::Committed();
        },
        [&](size_t,
            std::chrono::milliseconds) -> tl::expected<size_t, ErrorCode> {
            ++wait_calls;
            return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
        });

    EXPECT_EQ(KvWriteStatus::kOutcomeUnknown, result.status);
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.error);
    EXPECT_EQ(1u, script_calls);
    EXPECT_EQ(1u, wait_calls);
}

TEST(RedisWriteAckTest, UnexpectedExtraReplicaIsUnknownWithAckCountPreserved) {
    size_t script_calls = 0;
    const auto result = ExecuteRedisWriteWithAck(
        {.replicas_required = 1},
        [&] {
            ++script_calls;
            return KvWriteResult::Committed();
        },
        [](size_t, std::chrono::milliseconds)
            -> tl::expected<size_t, ErrorCode> { return 2; });

    EXPECT_EQ(KvWriteStatus::kOutcomeUnknown, result.status);
    EXPECT_EQ(1u, result.replicas_required);
    EXPECT_EQ(2u, result.replicas_acked);
    EXPECT_EQ(1u, script_calls);
}

TEST(RedisWriteAckTest, ZeroReplicaPolicyIsRejectedBeforeWriting) {
    size_t script_calls = 0;
    size_t wait_calls = 0;
    const auto result = ExecuteRedisWriteWithAck(
        KvWriteDurabilityPolicy{},
        [&] {
            ++script_calls;
            return KvWriteResult::Committed();
        },
        [&](size_t,
            std::chrono::milliseconds) -> tl::expected<size_t, ErrorCode> {
            ++wait_calls;
            return 0;
        });

    EXPECT_EQ(KvWriteStatus::kRejected, result.status);
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error);
    EXPECT_EQ(0u, script_calls);
    EXPECT_EQ(0u, wait_calls);
}

}  // namespace
}  // namespace mooncake::test
