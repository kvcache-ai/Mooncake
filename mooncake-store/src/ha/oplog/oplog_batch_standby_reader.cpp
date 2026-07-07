#include "ha/oplog/oplog_batch_standby_reader.h"

#include <utility>
#include <vector>

#include "ha/oplog/oplog_applier.h"

namespace mooncake {

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
        result.used_legacy_path = true;
        return result;
    }
    if (err != ErrorCode::OK) {
        result.error = err;
        return result;
    }
    result.durable_prefix = prefix;
    if (prefix.batch_id <= last_applied_batch_id_) {
        return result;
    }

    std::vector<OpLogBatchRecord> batches;
    err =
        storage_.ReadBatchesAfter(last_applied_batch_id_, max_batches, batches);
    if (err != ErrorCode::OK) {
        result.error = err;
        return result;
    }
    for (const auto& batch : batches) {
        for (const auto& entry : batch.entries) {
            if (entry.sequence_id > prefix.last_seq) {
                break;
            }
            const uint64_t expected = applier_.GetExpectedSequenceId();
            if (IsSequenceOlder(entry.sequence_id, expected)) {
                continue;
            }
            if (IsSequenceNewer(entry.sequence_id, expected)) {
                result.error = ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
                return result;
            }
            if (!applier_.ApplyOpLogEntry(entry)) {
                result.error = ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
                return result;
            }
            ++result.applied_entries;
        }
        last_applied_batch_id_ = batch.batch_id;
        if (last_applied_batch_id_ >= prefix.batch_id) {
            break;
        }
    }
    if (IsSequenceOlderOrEqual(applier_.GetExpectedSequenceId(),
                               prefix.last_seq)) {
        result.error = ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
    }
    return result;
}

}  // namespace mooncake
