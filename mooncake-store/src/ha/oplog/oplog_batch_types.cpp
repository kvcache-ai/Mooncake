#include "ha/oplog/oplog_batch_types.h"

#include <iomanip>
#include <sstream>

#include "ha/oplog/oplog_store.h"

namespace mooncake {

namespace {

void SetReason(std::string* reason, const std::string& value) {
    if (reason != nullptr) {
        *reason = value;
    }
}

std::string PrefixEnd(std::string prefix) {
    for (int i = static_cast<int>(prefix.size()) - 1; i >= 0; --i) {
        unsigned char c = static_cast<unsigned char>(prefix[i]);
        if (c < 0xFF) {
            prefix[i] = static_cast<char>(c + 1);
            prefix.resize(i + 1);
            return prefix;
        }
    }
    return std::string(1, '\0');
}

std::string BatchPrefix(const std::string& cluster_id) {
    return "/oplog/" + cluster_id + "/batches/";
}

}  // namespace

bool ValidateOpLogBatchRecordShape(const OpLogBatchRecord& batch,
                                   std::string* reason) {
    if (reason != nullptr) {
        reason->clear();
    }
    if (batch.schema_version != kOpLogBatchRecordSchemaVersion) {
        SetReason(reason, "unsupported schema_version");
        return false;
    }
    if (batch.batch_id == 0) {
        SetReason(reason, "batch_id must be non-zero");
        return false;
    }
    if (batch.first_seq == 0) {
        SetReason(reason, "first_seq must be non-zero");
        return false;
    }
    if (batch.entries.empty()) {
        SetReason(reason, "batch entries must not be empty");
        return false;
    }
    if (batch.first_seq != batch.entries.front().sequence_id) {
        SetReason(reason, "first_seq does not match first entry sequence");
        return false;
    }
    if (batch.last_seq != batch.entries.back().sequence_id) {
        SetReason(reason, "last_seq does not match last entry sequence");
        return false;
    }
    if (batch.last_seq < batch.first_seq ||
        batch.last_seq - batch.first_seq != batch.entries.size() - 1) {
        SetReason(reason, "batch sequence range does not match entry count");
        return false;
    }
    for (size_t i = 0; i < batch.entries.size(); ++i) {
        const uint64_t expected = batch.first_seq + i;
        if (batch.entries[i].sequence_id != expected) {
            SetReason(reason, "entry sequences must be contiguous");
            return false;
        }
        if (!ValidateOpLogBatchEntry(batch.entries[i], reason)) {
            return false;
        }
    }
    return true;
}

bool ValidateOpLogBatchEntry(const OpLogEntry& entry, std::string* reason) {
    if (reason != nullptr) {
        reason->clear();
    }
    const auto op_type = static_cast<uint32_t>(entry.op_type);
    if (op_type == 0 || op_type >= static_cast<uint32_t>(OpType::OP_TYPE_MAX)) {
        SetReason(reason, "op_type is outside the valid enum range");
        return false;
    }
    if (!IsValidTenantId(entry.tenant_id)) {
        SetReason(reason, "tenant_id is empty or invalid");
        return false;
    }
    return OpLogManager::ValidateEntrySize(entry, reason);
}

bool ValidateOpLogBatchClusterId(const std::string& cluster_id,
                                 std::string* reason) {
    std::string normalized = cluster_id;
    if (!NormalizeAndValidateClusterId(normalized) || normalized.empty()) {
        SetReason(reason, "invalid cluster_id");
        return false;
    }
    if (reason != nullptr) {
        reason->clear();
    }
    return true;
}

std::string FormatOpLogBatchId(uint64_t batch_id) {
    std::ostringstream oss;
    oss << std::setw(kOpLogBatchIdWidth) << std::setfill('0') << batch_id;
    return oss.str();
}

std::string BuildBatchRecordKey(const std::string& cluster_id,
                                uint64_t batch_id) {
    std::string normalized = cluster_id;
    if (!NormalizeAndValidateClusterId(normalized) || normalized.empty()) {
        return {};
    }
    return BatchPrefix(normalized) + FormatOpLogBatchId(batch_id);
}

std::string BuildDurablePrefixKey(const std::string& cluster_id) {
    std::string normalized = cluster_id;
    if (!NormalizeAndValidateClusterId(normalized) || normalized.empty()) {
        return {};
    }
    return "/oplog/" + normalized + "/durable_prefix";
}

BatchRecordRange BuildBatchRecordRange(const std::string& cluster_id,
                                       uint64_t after_batch_id) {
    std::string normalized = cluster_id;
    if (!NormalizeAndValidateClusterId(normalized) || normalized.empty()) {
        return {};
    }
    const std::string prefix = BatchPrefix(normalized);
    if (after_batch_id == UINT64_MAX) {
        return {.begin_key = PrefixEnd(prefix), .end_key = PrefixEnd(prefix)};
    }
    return {.begin_key = prefix + FormatOpLogBatchId(after_batch_id + 1),
            .end_key = PrefixEnd(prefix)};
}

}  // namespace mooncake
