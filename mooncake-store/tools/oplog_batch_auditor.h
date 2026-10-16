#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_types.h"

namespace mooncake {

struct OpLogAuditReport {
    bool ok{false};
    std::string cluster_id;
    uint64_t legacy_max_seq{0};
    std::optional<DurablePrefix> durable_prefix;
    size_t batch_count{0};
    size_t entry_count{0};
    std::vector<uint64_t> orphan_batches;
    std::vector<std::string> warnings;
    std::vector<std::string> errors;
    bool truncated_errors{false};
};

struct OpLogNamespaceRead {
    std::vector<KvPair> kvs;
    bool truncated{false};
};

ErrorCode ReadOpLogNamespace(const std::string& cluster_id,
                             HaKvBackend& backend, size_t max_keys,
                             OpLogNamespaceRead* result);
std::optional<size_t> ComputeOpLogDumpLimit(uint64_t from_batch,
                                            uint64_t to_batch,
                                            size_t max_batches);
OpLogAuditReport AuditOpLogNamespace(const std::string& cluster_id,
                                     const std::vector<KvPair>& kvs,
                                     size_t max_batches);
std::string OpLogAuditReportToJson(const OpLogAuditReport& report);

}  // namespace mooncake
