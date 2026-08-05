#include "ha/oplog/oplog_batch_standby_reader.h"

#include <utility>
#include <vector>

#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_test_failpoint.h"

namespace mooncake {

namespace {

bool IsRetryableBackendError(ErrorCode error) {
    return error == ErrorCode::ETCD_OPERATION_ERROR ||
           error == ErrorCode::ETCD_CTX_CANCELLED;
}

void SetPollError(OpLogBatchStandbyPollResult& result, ErrorCode error,
                  bool retryable) {
    result.error = error;
    result.disposition = retryable ? OpLogBatchStandbyPollDisposition::RETRYABLE
                                   : OpLogBatchStandbyPollDisposition::FATAL;
}

}  // namespace

OpLogBatchStandbyReader::OpLogBatchStandbyReader(std::string cluster_id,
                                                 HaKvBackend& backend,
                                                 OpLogApplier& applier)
    : storage_(std::move(cluster_id), backend), applier_(applier) {}

OpLogBatchStandbyPollResult OpLogBatchStandbyReader::PollOnce(
    size_t max_batches) {
    OpLogBatchStandbyPollResult result;
    DurablePrefix prefix;
    ErrorCode err = storage_.ReadDurablePrefix(prefix);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        if (batch_format_seen_) {
            SetPollError(result, ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, false);
        }
        return result;
    }
    if (err != ErrorCode::OK) {
        SetPollError(result, err, IsRetryableBackendError(err));
        return result;
    }
    batch_format_seen_ = true;
    result.durable_prefix_present = true;
    result.durable_prefix = prefix;

    if (last_observed_prefix_ &&
        (prefix.batch_id < last_observed_prefix_->batch_id ||
         prefix.last_seq < last_observed_prefix_->last_seq ||
         (prefix.batch_id == last_observed_prefix_->batch_id &&
          prefix.last_seq != last_observed_prefix_->last_seq) ||
         (prefix.batch_id > last_observed_prefix_->batch_id &&
          prefix.last_seq == last_observed_prefix_->last_seq))) {
        SetPollError(result, ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, false);
        return result;
    }

    if (prefix.batch_id == 0) {
        last_observed_prefix_ = prefix;
        if (prefix.last_seq != 0) {
            SetPollError(result, ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, false);
        }
        return result;
    }

    TestFailPoint::Wait("standby_prefix_read_before_batch");
    OpLogBatchRecord target_batch;
    err = storage_.ReadBatch(prefix.batch_id, target_batch);
    if (err != ErrorCode::OK) {
        SetPollError(result, err, IsRetryableBackendError(err));
        return result;
    }
    if (target_batch.last_seq != prefix.last_seq) {
        SetPollError(result, ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, false);
        return result;
    }
    last_observed_prefix_ = prefix;
    if (prefix.batch_id <= last_applied_batch_id_) {
        return result;
    }

    std::vector<OpLogBatchRecord> batches;
    err =
        storage_.ReadBatchesAfter(last_applied_batch_id_, max_batches, batches);
    if (err != ErrorCode::OK) {
        SetPollError(result, err, IsRetryableBackendError(err));
        return result;
    }
    for (const auto& batch : batches) {
        if (last_scanned_batch_last_seq_ &&
            (last_applied_batch_id_ == UINT64_MAX ||
             batch.batch_id != last_applied_batch_id_ + 1)) {
            SetPollError(result, ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, false);
            return result;
        }
        if (last_scanned_batch_last_seq_ &&
            (*last_scanned_batch_last_seq_ == UINT64_MAX ||
             batch.first_seq != *last_scanned_batch_last_seq_ + 1)) {
            SetPollError(result, ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, false);
            return result;
        }
        for (const auto& entry : batch.entries) {
            if (entry.sequence_id > prefix.last_seq) {
                break;
            }
            const uint64_t expected = applier_.GetExpectedSequenceId();
            if (IsSequenceOlder(entry.sequence_id, expected)) {
                continue;
            }
            if (IsSequenceNewer(entry.sequence_id, expected)) {
                SetPollError(result, ErrorCode::INCOMPLETE_OPLOG_CATCH_UP,
                             false);
                return result;
            }
            if (!applier_.ApplyOpLogEntry(entry)) {
                SetPollError(result, ErrorCode::INCOMPLETE_OPLOG_CATCH_UP,
                             false);
                return result;
            }
            ++result.applied_entries;
        }
        last_applied_batch_id_ = batch.batch_id;
        last_scanned_batch_last_seq_ = batch.last_seq;
        if (last_applied_batch_id_ >= prefix.batch_id) {
            break;
        }
    }
    const bool has_more_pages = max_batches != 0 &&
                                batches.size() >= max_batches &&
                                last_applied_batch_id_ < prefix.batch_id;
    if (!has_more_pages &&
        IsSequenceOlderOrEqual(applier_.GetExpectedSequenceId(),
                               prefix.last_seq)) {
        SetPollError(result, ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, false);
    }
    return result;
}

}  // namespace mooncake
