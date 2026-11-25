#include "ha/kv/etcd_ha_kv_backend.h"

#include <gtest/gtest.h>

#include "etcd_helper.h"

namespace mooncake {
namespace {

// Substitute the external helper boundary, keeping the actual etcd backend's
// argument conversion and write-result mapping under test. This binary does
// not link etcd_helper.cpp or the process-wide Go client.
ErrorCode helper_result{ErrorCode::OK};
size_t txn_calls{0};
std::vector<EtcdHelper::TxnCompare> last_compares;
std::vector<EtcdHelper::TxnPut> last_puts;

}  // namespace

ErrorCode EtcdHelper::Put(const char*, size_t, const char*, size_t) {
    return helper_result;
}

ErrorCode EtcdHelper::DeleteRange(const char*, size_t, const char*, size_t) {
    return helper_result;
}

ErrorCode EtcdHelper::TxnCompareAndPut(const std::vector<TxnCompare>& compares,
                                       const std::vector<TxnPut>& puts) {
    ++txn_calls;
    last_compares = compares;
    last_puts = puts;
    return helper_result;
}

ErrorCode EtcdHelper::Get(const char*, size_t, std::string&, EtcdRevisionId&) {
    return ErrorCode::ETCD_KEY_NOT_EXIST;
}

ErrorCode EtcdHelper::GetRangeAsJson(const char*, size_t, const char*, size_t,
                                     size_t, std::string&, EtcdRevisionId&) {
    return ErrorCode::ETCD_OPERATION_ERROR;
}

namespace test {
namespace {

TEST(EtcdHaKvBackendWriteResultTest, SuccessCompareAndAmbiguousWrapperErrors) {
    EtcdHaKvBackend backend;
    const std::vector<std::pair<ErrorCode, KvWriteStatus>> cases{
        {ErrorCode::OK, KvWriteStatus::kCommitted},
        {ErrorCode::ETCD_TRANSACTION_FAIL, KvWriteStatus::kCompareFailed},
        {ErrorCode::ETCD_OPERATION_ERROR, KvWriteStatus::kOutcomeUnknown},
        {ErrorCode::ETCD_CTX_CANCELLED, KvWriteStatus::kOutcomeUnknown},
    };
    for (const auto& [error, expected] : cases) {
        helper_result = error;
        for (const auto& result : {backend.PutWithResult("key", "value"),
                                   backend.DeleteRangeWithResult("a", "b"),
                                   backend.TxnWithResult({})}) {
            EXPECT_EQ(expected, result.status);
            EXPECT_EQ(error, result.error);
        }
    }
}

TEST(EtcdHaKvBackendWriteResultTest,
     UnknownTxnKeepsOneAtomicRequestWithBinaryData) {
    EtcdHaKvBackend backend;
    const std::string binary("a\0b", 3);
    KvTxn txn;
    txn.compares.push_back({.key = "/oplog/cluster/prefix",
                            .kind = KvCompareKind::kValueEquals,
                            .expected_value = "11"});
    txn.compares.push_back({.key = "/oplog/cluster/lease",
                            .kind = KvCompareKind::kCreateRevisionEquals,
                            .expected_revision = 42});
    txn.puts.push_back({.key = "/oplog/cluster/batch12", .value = binary});
    txn.puts.push_back({.key = "/oplog/cluster/prefix", .value = "12"});
    helper_result = ErrorCode::ETCD_OPERATION_ERROR;
    txn_calls = 0;
    const auto result = backend.TxnWithResult(txn);
    EXPECT_EQ(KvWriteStatus::kOutcomeUnknown, result.status);
    EXPECT_EQ(1u, txn_calls);
    ASSERT_EQ(2u, last_compares.size());
    ASSERT_EQ(2u, last_puts.size());
    EXPECT_EQ("11", last_compares[0].expected_value);
    EXPECT_EQ(EtcdHelper::TxnCompareKind::kCreateRevisionEquals,
              last_compares[1].kind);
    EXPECT_EQ(42, last_compares[1].expected_revision);
    EXPECT_EQ(binary, last_puts[0].value);
    EXPECT_EQ("12", last_puts[1].value);
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, backend.Txn(txn));
}

}  // namespace
}  // namespace test
}  // namespace mooncake
