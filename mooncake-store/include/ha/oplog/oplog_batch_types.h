#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "ha/oplog/oplog_types.h"

namespace mooncake {

static constexpr uint32_t kOpLogBatchRecordSchemaVersion = 1;
static constexpr uint32_t kDurablePrefixSchemaVersion = 1;
static constexpr int kOpLogBatchIdWidth = 20;

struct DurablePrefix {
    uint64_t batch_id{0};
    uint64_t last_seq{0};
};

struct BatchRecordRange {
    std::string begin_key;
    std::string end_key;
};

struct OpLogBatchRecord {
    uint32_t schema_version{kOpLogBatchRecordSchemaVersion};
    uint64_t batch_id{0};
    uint64_t first_seq{0};
    uint64_t last_seq{0};
    std::vector<OpLogEntry> entries;
    uint32_t checksum{0};
};

bool ValidateOpLogBatchRecordShape(const OpLogBatchRecord& batch,
                                   std::string* reason = nullptr);

bool ValidateOpLogBatchEntry(const OpLogEntry& entry,
                             std::string* reason = nullptr);

bool ValidateOpLogBatchClusterId(const std::string& cluster_id,
                                 std::string* reason = nullptr);

std::string FormatOpLogBatchId(uint64_t batch_id);
std::string BuildBatchRecordKey(const std::string& cluster_id,
                                uint64_t batch_id);
std::string BuildDurablePrefixKey(const std::string& cluster_id);
BatchRecordRange BuildBatchRecordRange(const std::string& cluster_id,
                                       uint64_t after_batch_id);

}  // namespace mooncake
