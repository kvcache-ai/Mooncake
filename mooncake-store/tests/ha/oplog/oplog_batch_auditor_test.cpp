#include <gtest/gtest.h>

#include <jsoncpp/json/json.h>

#include <algorithm>
#include <sstream>
#include <string_view>
#include <utility>

#include "ha/oplog/oplog_batch_codec.h"
#include "tools/oplog_batch_auditor.h"

namespace mooncake {
namespace {

class RangeBackend : public HaKvBackend {
   public:
    explicit RangeBackend(std::vector<KvPair> values)
        : values_(std::move(values)) {}

    ErrorCode Get(std::string_view, std::string&) override {
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    ErrorCode Put(std::string_view, std::string_view) override {
        return ErrorCode::OK;
    }
    ErrorCode Range(std::string_view, std::string_view, size_t limit,
                    std::vector<KvPair>& kvs) override {
        observed_limit = limit;
        kvs.assign(values_.begin(),
                   values_.begin() + std::min(limit, values_.size()));
        return ErrorCode::OK;
    }
    bool SupportsTxn() const override { return false; }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::INVALID_PARAMS; }

    size_t observed_limit{0};

   private:
    std::vector<KvPair> values_;
};

OpLogBatchRecord MakeBatch(uint64_t batch_id, uint64_t first_seq,
                           size_t entry_count) {
    OpLogBatchRecord batch;
    batch.batch_id = batch_id;
    batch.first_seq = first_seq;
    batch.last_seq = first_seq + entry_count - 1;
    for (size_t i = 0; i < entry_count; ++i) {
        batch.entries.push_back({.sequence_id = first_seq + i,
                                 .op_type = OpType::REMOVE,
                                 .tenant_id = "default",
                                 .object_key = "key-" + std::to_string(i),
                                 .payload = ""});
    }
    return batch;
}

TEST(OpLogBatchAuditorTest, AcceptsContiguousHistoryAfterLegacyCutover) {
    const std::string cluster = "audit-test";
    std::vector<KvPair> kvs = {
        {.key = "/oplog/audit-test/00000000000000000007", .value = "legacy"},
        {.key = BuildDurablePrefixKey(cluster),
         .value = EncodeDurablePrefix({.batch_id = 2, .last_seq = 10})},
        {.key = BuildBatchRecordKey(cluster, 1),
         .value = EncodeOpLogBatchRecord(MakeBatch(1, 8, 2))},
        {.key = BuildBatchRecordKey(cluster, 2),
         .value = EncodeOpLogBatchRecord(MakeBatch(2, 10, 1))},
    };

    OpLogAuditReport report = AuditOpLogNamespace(cluster, kvs, 100);

    EXPECT_TRUE(report.ok);
    EXPECT_EQ(report.legacy_max_seq, 7);
    ASSERT_TRUE(report.durable_prefix.has_value());
    EXPECT_EQ(report.durable_prefix->batch_id, 2);
    EXPECT_EQ(report.durable_prefix->last_seq, 10);
    EXPECT_EQ(report.batch_count, 2);
    EXPECT_EQ(report.entry_count, 3);
    EXPECT_TRUE(report.errors.empty());
}

TEST(OpLogBatchAuditorTest, SerializesMachineReadableReport) {
    OpLogAuditReport report;
    report.ok = false;
    report.cluster_id = "audit-test";
    report.legacy_max_seq = 7;
    report.durable_prefix = {.batch_id = 2, .last_seq = 10};
    report.batch_count = 2;
    report.entry_count = 3;
    report.orphan_batches = {3};
    report.warnings = {"warning"};
    report.errors = {"error"};
    report.truncated_errors = true;

    const std::string encoded = OpLogAuditReportToJson(report);
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    std::istringstream input(encoded);

    ASSERT_TRUE(Json::parseFromStream(builder, input, &root, &errors))
        << errors;
    EXPECT_EQ(root["schema_version"].asUInt(), 1);
    EXPECT_FALSE(root["ok"].asBool());
    EXPECT_EQ(root["durable_prefix"]["last_seq"].asUInt64(), 10);
    EXPECT_EQ(root["orphan_batches"][0].asUInt64(), 3);
    EXPECT_EQ(root["warnings"][0].asString(), "warning");
    EXPECT_EQ(root["errors"][0].asString(), "error");
    EXPECT_TRUE(root["truncated_errors"].asBool());
}

TEST(OpLogBatchAuditorTest, ReportsUnknownSidecarWithoutFailingAudit) {
    std::vector<KvPair> kvs = {
        {.key = "/oplog/audit-test/custom-sidecar", .value = "value"},
    };

    OpLogAuditReport report = AuditOpLogNamespace("audit-test", kvs, 100);

    EXPECT_TRUE(report.ok);
    ASSERT_EQ(report.warnings.size(), 1);
    EXPECT_NE(report.warnings[0].find("custom-sidecar"), std::string::npos);
}

TEST(OpLogBatchAuditorTest, RejectsMalformedLegacyLikeKey) {
    std::vector<KvPair> kvs = {
        {.key = "/oplog/audit-test/0000000000000000000x", .value = "value"},
    };

    OpLogAuditReport report = AuditOpLogNamespace("audit-test", kvs, 100);

    EXPECT_FALSE(report.ok);
    ASSERT_EQ(report.errors.size(), 1);
    EXPECT_NE(report.errors[0].find("malformed legacy key"), std::string::npos);
}

TEST(OpLogBatchAuditorTest, CapsErrorsAndCountsMalformedBatchKeys) {
    std::vector<KvPair> kvs;
    for (size_t i = 0; i < 105; ++i) {
        kvs.push_back(
            {.key = "/oplog/audit-test/batches/bad-" + std::to_string(i),
             .value = "value"});
    }

    OpLogAuditReport report = AuditOpLogNamespace("audit-test", kvs, 100);

    EXPECT_FALSE(report.ok);
    EXPECT_EQ(report.batch_count, 105);
    EXPECT_EQ(report.errors.size(), 100);
    EXPECT_TRUE(report.truncated_errors);
}

TEST(OpLogBatchAuditorTest, ClassifiesBatchesWithoutPrefixAsOrphans) {
    const std::string cluster = "audit-test";
    std::vector<KvPair> kvs = {
        {.key = BuildBatchRecordKey(cluster, 1),
         .value = EncodeOpLogBatchRecord(MakeBatch(1, 1, 1))},
    };

    OpLogAuditReport report = AuditOpLogNamespace(cluster, kvs, 100);

    EXPECT_FALSE(report.ok);
    EXPECT_EQ(report.batch_count, 1);
    EXPECT_EQ(report.entry_count, 1);
    ASSERT_EQ(report.orphan_batches.size(), 1);
    EXPECT_EQ(report.orphan_batches[0], 1);
}

TEST(OpLogBatchAuditorTest, CapsNamespaceReadBeforeAudit) {
    std::vector<KvPair> values(3);
    RangeBackend backend(std::move(values));
    OpLogNamespaceRead result;

    ErrorCode err = ReadOpLogNamespace("audit-test", backend, 2, &result);

    EXPECT_EQ(err, ErrorCode::OK);
    EXPECT_EQ(backend.observed_limit, 3);
    EXPECT_TRUE(result.truncated);
    EXPECT_EQ(result.kvs.size(), 2);
}

TEST(OpLogBatchAuditorTest, RejectsOverflowingDumpRange) {
    EXPECT_FALSE(ComputeOpLogDumpLimit(1, UINT64_MAX, 100).has_value());
    EXPECT_EQ(ComputeOpLogDumpLimit(1, 0, 100), 100);
    EXPECT_EQ(ComputeOpLogDumpLimit(1, 2, 100), 2);
}

}  // namespace
}  // namespace mooncake
