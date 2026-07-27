#include "ha/oplog/oplog_batch_storage.h"

#include <algorithm>
#include <charconv>

#include <glog/logging.h>

#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_types.h"
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
#include "ha_metric_manager.h"
#endif

namespace mooncake {
namespace {

bool SameBatchRecord(const OpLogBatchRecord& lhs, const OpLogBatchRecord& rhs) {
    if (lhs.schema_version != rhs.schema_version ||
        lhs.batch_id != rhs.batch_id || lhs.first_seq != rhs.first_seq ||
        lhs.last_seq != rhs.last_seq ||
        lhs.entries.size() != rhs.entries.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.entries.size(); ++i) {
        const auto& a = lhs.entries[i];
        const auto& b = rhs.entries[i];
        if (a.sequence_id != b.sequence_id || a.op_type != b.op_type ||
            a.tenant_id != b.tenant_id || a.object_key != b.object_key ||
            a.payload != b.payload) {
            return false;
        }
    }
    return true;
}

bool TryParseBatchIdFromKey(const std::string& key, uint64_t& batch_id) {
    const size_t slash = key.rfind('/');
    if (slash == std::string::npos || key.size() - slash - 1 != 20) {
        return false;
    }
    const std::string_view suffix(key.data() + slash + 1, 20);
    for (char c : suffix) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    auto result =
        std::from_chars(suffix.data(), suffix.data() + suffix.size(), batch_id);
    return result.ec == std::errc() &&
           result.ptr == suffix.data() + suffix.size();
}

}  // namespace

OpLogBatchStorage::OpLogBatchStorage(std::string cluster_id,
                                     HaKvBackend& backend)
    : cluster_id_(std::move(cluster_id)), backend_(backend) {
    cluster_id_valid_ =
        NormalizeAndValidateClusterId(cluster_id_) && !cluster_id_.empty();
}

ErrorCode OpLogBatchStorage::InitDurablePrefix(DurablePrefix& prefix) {
    if (!IsValidClusterId()) {
        return ErrorCode::INVALID_PARAMS;
    }
    ErrorCode err = RejectLegacyLayout();
    if (err != ErrorCode::OK) {
        return err;
    }
    err = ReadDurablePrefix(prefix);
    if (err == ErrorCode::OK) {
        return ErrorCode::OK;
    }
    if (err != ErrorCode::ETCD_KEY_NOT_EXIST) {
        return err;
    }
    if (!backend_.SupportsTxn()) {
        return ErrorCode::INVALID_PARAMS;
    }

    auto batch_range = BuildBatchRecordRange(cluster_id_, 0);
    std::vector<KvPair> existing_batches;
    err = backend_.Range(batch_range.begin_key, batch_range.end_key,
                         /*limit=*/1, existing_batches);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (!existing_batches.empty()) {
        LOG(ERROR) << "Durable prefix is missing but OpLog batch records exist";
        return ErrorCode::INTERNAL_ERROR;
    }

    const std::string durable_key = BuildDurablePrefixKey(cluster_id_);
    DurablePrefix initial{.batch_id = 0, .last_seq = 0};
    KvTxn txn;
    txn.compares.push_back({.key = durable_key,
                            .kind = KvCompareKind::kKeyNotExists,
                            .expected_value = ""});
    txn.puts.push_back(
        {.key = durable_key, .value = EncodeDurablePrefix(initial)});
    err = backend_.Txn(txn);
    if (err == ErrorCode::OK) {
        prefix = initial;
        return ErrorCode::OK;
    }
    if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
        return ReadDurablePrefix(prefix);
    }
    return err;
}

ErrorCode OpLogBatchStorage::ReadDurablePrefix(DurablePrefix& prefix) {
    if (!IsValidClusterId()) {
        return ErrorCode::INVALID_PARAMS;
    }
    std::string value;
    const std::string key = BuildDurablePrefixKey(cluster_id_);
    ErrorCode err = backend_.Get(key, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    std::string reason;
    if (!DecodeDurablePrefix(value, &prefix, &reason)) {
        LOG(ERROR) << "Failed to decode durable prefix: " << reason;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode OpLogBatchStorage::WriteBatchAndAdvancePrefix(
    const OpLogBatchRecord& batch, const DurablePrefix& expected_prefix) {
    if (!IsValidClusterId()) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (!backend_.SupportsTxn()) {
        return ErrorCode::INVALID_PARAMS;
    }
    std::string reason;
    if (!ValidateOpLogBatchRecordShape(batch, &reason)) {
        LOG(ERROR) << "Invalid OpLog batch record: " << reason;
        return ErrorCode::INVALID_PARAMS;
    }
    if (expected_prefix.batch_id == UINT64_MAX ||
        expected_prefix.last_seq == UINT64_MAX) {
        return ErrorCode::INVALID_PARAMS;
    }
    const uint64_t expected_batch_id = expected_prefix.batch_id + 1;
    const uint64_t expected_first_seq = expected_prefix.last_seq + 1;
    if (batch.batch_id != expected_batch_id ||
        batch.first_seq != expected_first_seq) {
        LOG(ERROR) << "OpLog batch does not advance durable prefix "
                      "contiguously: expected_batch_id="
                   << expected_batch_id
                   << ", actual_batch_id=" << batch.batch_id
                   << ", expected_first_seq=" << expected_first_seq
                   << ", actual_first_seq=" << batch.first_seq;
        return ErrorCode::INVALID_PARAMS;
    }

    const std::string durable_key = BuildDurablePrefixKey(cluster_id_);
    const std::string encoded_batch = EncodeOpLogBatchRecord(batch);
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    HAMetricManager::instance().observe_batch_record_batch_bytes(
        encoded_batch.size());
#endif
    KvTxn txn;
    txn.compares.push_back(
        {.key = durable_key,
         .kind = KvCompareKind::kValueEquals,
         .expected_value = EncodeDurablePrefix(expected_prefix)});
    txn.puts.push_back({.key = BuildBatchRecordKey(cluster_id_, batch.batch_id),
                        .value = encoded_batch});
    txn.puts.push_back(
        {.key = durable_key,
         .value = EncodeDurablePrefix(
             {.batch_id = batch.batch_id, .last_seq = batch.last_seq})});
    ErrorCode err = backend_.Txn(txn);
    if (err != ErrorCode::ETCD_TRANSACTION_FAIL) {
        return err;
    }

    DurablePrefix current_prefix;
    if (ReadDurablePrefix(current_prefix) != ErrorCode::OK ||
        current_prefix.batch_id != batch.batch_id ||
        current_prefix.last_seq != batch.last_seq) {
        return err;
    }
    OpLogBatchRecord current_batch;
    if (ReadBatch(batch.batch_id, current_batch) != ErrorCode::OK ||
        !SameBatchRecord(batch, current_batch)) {
        return err;
    }
    return ErrorCode::OK;
}

ErrorCode OpLogBatchStorage::ReadBatch(uint64_t batch_id,
                                       OpLogBatchRecord& batch) {
    if (!IsValidClusterId()) {
        return ErrorCode::INVALID_PARAMS;
    }
    std::string value;
    ErrorCode err =
        backend_.Get(BuildBatchRecordKey(cluster_id_, batch_id), value);
    if (err != ErrorCode::OK) {
        return err;
    }
    std::string reason;
    if (!DecodeOpLogBatchRecord(value, &batch, &reason)) {
        LOG(ERROR) << "Failed to decode OpLog batch record: " << reason;
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode OpLogBatchStorage::ReadBatchesAfter(
    uint64_t after_batch_id, size_t limit,
    std::vector<OpLogBatchRecord>& batches) {
    batches.clear();
    if (!IsValidClusterId()) {
        return ErrorCode::INVALID_PARAMS;
    }
    auto range = BuildBatchRecordRange(cluster_id_, after_batch_id);
    std::string begin_key = range.begin_key;
    do {
        std::vector<KvPair> kvs;
        const size_t remaining = limit == 0 ? 0 : limit - batches.size();
        ErrorCode err =
            backend_.Range(begin_key, range.end_key, remaining, kvs);
        if (err != ErrorCode::OK) {
            return err;
        }
        for (const auto& kv : kvs) {
            uint64_t key_batch_id = 0;
            if (!TryParseBatchIdFromKey(kv.key, key_batch_id)) {
                continue;
            }
            OpLogBatchRecord batch;
            std::string reason;
            if (!DecodeOpLogBatchRecord(kv.value, &batch, &reason)) {
                LOG(ERROR) << "Failed to decode OpLog batch record at key="
                           << kv.key << ": " << reason;
                return ErrorCode::INTERNAL_ERROR;
            }
            if (batch.batch_id != key_batch_id) {
                LOG(ERROR) << "OpLog batch id does not match key at key="
                           << kv.key;
                return ErrorCode::INTERNAL_ERROR;
            }
            batches.push_back(std::move(batch));
        }
        if (limit == 0 || batches.size() >= limit || kvs.size() < remaining) {
            break;
        }
        begin_key = kvs.back().key + '\0';
    } while (begin_key < range.end_key);
    return ErrorCode::OK;
}

bool OpLogBatchStorage::IsValidClusterId() const { return cluster_id_valid_; }

ErrorCode OpLogBatchStorage::RejectLegacyLayout() const {
    const std::string root = "/oplog/" + cluster_id_ + "/";
    std::string ignored;
    ErrorCode err = backend_.Get(root + "latest", ignored);
    if (err == ErrorCode::OK) {
        LOG(ERROR) << "Legacy OpLog latest key exists for cluster="
                   << cluster_id_
                   << "; clear the legacy OpLog namespace before enabling "
                      "batch-record OpLog";
        return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
    }
    if (err != ErrorCode::ETCD_KEY_NOT_EXIST) {
        return err;
    }

    std::vector<KvPair> entries;
    err = backend_.Range(root + "00000000000000000000", root + ":",
                         /*limit=*/1, entries);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (!entries.empty()) {
        LOG(ERROR) << "Legacy per-entry OpLog key exists for cluster="
                   << cluster_id_
                   << "; clear the legacy OpLog namespace before enabling "
                      "batch-record OpLog";
        return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
    }

    entries.clear();
    err = backend_.Range(root + "snapshot/", root + "snapshot0",
                         /*limit=*/1, entries);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (!entries.empty()) {
        LOG(ERROR) << "Legacy OpLog snapshot sidecar exists for cluster="
                   << cluster_id_
                   << "; clear the legacy OpLog namespace before enabling "
                      "batch-record OpLog";
        return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
    }
    return ErrorCode::OK;
}

}  // namespace mooncake
