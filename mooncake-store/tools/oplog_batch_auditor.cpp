#include "tools/oplog_batch_auditor.h"

#include <algorithm>
#include <charconv>
#include <limits>
#include <map>
#include <string_view>

#include <jsoncpp/json/json.h>

#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_store.h"

namespace mooncake {
namespace {

constexpr size_t kMaxAuditErrors = 100;

void AddError(OpLogAuditReport* report, std::string error) {
    if (report->errors.size() < kMaxAuditErrors) {
        report->errors.push_back(std::move(error));
    } else {
        report->truncated_errors = true;
    }
}

bool ParsePaddedId(std::string_view value, uint64_t* id) {
    if (value.size() != kOpLogBatchIdWidth ||
        !std::all_of(value.begin(), value.end(),
                     [](char c) { return c >= '0' && c <= '9'; })) {
        return false;
    }
    auto result =
        std::from_chars(value.data(), value.data() + value.size(), *id);
    return result.ec == std::errc() &&
           result.ptr == value.data() + value.size();
}

std::string PrefixEnd(std::string prefix) {
    for (size_t i = prefix.size(); i > 0; --i) {
        auto value = static_cast<unsigned char>(prefix[i - 1]);
        if (value != 0xFF) {
            prefix[i - 1] = static_cast<char>(value + 1);
            prefix.resize(i);
            return prefix;
        }
    }
    return std::string(1, '\0');
}

}  // namespace

ErrorCode ReadOpLogNamespace(const std::string& cluster_id,
                             HaKvBackend& backend, size_t max_keys,
                             OpLogNamespaceRead* result) {
    if (result == nullptr || max_keys == 0 ||
        max_keys == std::numeric_limits<size_t>::max()) {
        return ErrorCode::INVALID_PARAMS;
    }
    result->kvs.clear();
    result->truncated = false;

    std::string normalized = cluster_id;
    if (!NormalizeAndValidateClusterId(normalized) || normalized.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }
    const std::string prefix = "/oplog/" + normalized + "/";
    const ErrorCode err =
        backend.Range(prefix, PrefixEnd(prefix), max_keys + 1, result->kvs);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (result->kvs.size() > max_keys) {
        result->kvs.resize(max_keys);
        result->truncated = true;
    }
    return ErrorCode::OK;
}

std::optional<size_t> ComputeOpLogDumpLimit(uint64_t from_batch,
                                            uint64_t to_batch,
                                            size_t max_batches) {
    if (from_batch == 0 || max_batches == 0 ||
        (to_batch != 0 && to_batch < from_batch)) {
        return std::nullopt;
    }
    if (to_batch == 0) {
        return max_batches;
    }
    const uint64_t difference = to_batch - from_batch;
    if (difference >= max_batches) {
        return std::nullopt;
    }
    return static_cast<size_t>(difference + 1);
}

OpLogAuditReport AuditOpLogNamespace(const std::string& cluster_id,
                                     const std::vector<KvPair>& kvs,
                                     size_t max_batches) {
    OpLogAuditReport report;
    report.cluster_id = cluster_id;

    std::string normalized = cluster_id;
    if (!NormalizeAndValidateClusterId(normalized) || normalized.empty()) {
        AddError(&report, "invalid cluster_id");
        return report;
    }
    report.cluster_id = normalized;

    const std::string prefix = "/oplog/" + normalized + "/";
    const std::string batch_prefix = prefix + "batches/";
    const std::string durable_key = prefix + "durable_prefix";
    std::map<uint64_t, OpLogBatchRecord> batches;
    size_t raw_batch_count = 0;

    for (const auto& kv : kvs) {
        if (kv.key == durable_key) {
            DurablePrefix durable;
            std::string reason;
            if (!DecodeDurablePrefix(kv.value, &durable, &reason)) {
                AddError(&report, "invalid durable_prefix: " + reason);
            } else {
                report.durable_prefix = durable;
            }
            continue;
        }

        if (kv.key.starts_with(batch_prefix)) {
            ++raw_batch_count;
            uint64_t key_id = 0;
            const std::string_view suffix(kv.key.data() + batch_prefix.size(),
                                          kv.key.size() - batch_prefix.size());
            if (!ParsePaddedId(suffix, &key_id)) {
                AddError(&report, "malformed batch key: " + kv.key);
                continue;
            }
            OpLogBatchRecord batch;
            std::string reason;
            if (!DecodeOpLogBatchRecord(kv.value, &batch, &reason)) {
                AddError(&report, "invalid batch " + std::to_string(key_id) +
                                      ": " + reason);
                continue;
            }
            if (batch.batch_id != key_id) {
                AddError(&report, "batch id does not match key: " + kv.key);
                continue;
            }
            batches.emplace(key_id, std::move(batch));
            continue;
        }

        if (kv.key.starts_with(prefix)) {
            uint64_t legacy_seq = 0;
            const std::string_view suffix(kv.key.data() + prefix.size(),
                                          kv.key.size() - prefix.size());
            if (ParsePaddedId(suffix, &legacy_seq)) {
                report.legacy_max_seq =
                    std::max(report.legacy_max_seq, legacy_seq);
            } else if (suffix.size() == kOpLogBatchIdWidth) {
                AddError(&report, "malformed legacy key: " + kv.key);
            } else if (suffix != "latest" && !suffix.starts_with("snapshot/") &&
                       !suffix.starts_with("cleanup/")) {
                report.warnings.push_back("unknown OpLog key: " + kv.key);
            }
        }
    }

    report.batch_count = raw_batch_count;
    if (raw_batch_count > max_batches) {
        AddError(&report, "batch count exceeds audit limit");
        return report;
    }
    if (!batches.empty() && !report.durable_prefix.has_value()) {
        for (const auto& [batch_id, batch] : batches) {
            report.orphan_batches.push_back(batch_id);
            report.entry_count += batch.entries.size();
        }
        AddError(&report, "batch records exist without durable_prefix");
        return report;
    }

    uint64_t expected_batch_id = 1;
    uint64_t expected_first_seq = report.legacy_max_seq + 1;
    for (const auto& [batch_id, batch] : batches) {
        if (report.durable_prefix.has_value() &&
            batch_id > report.durable_prefix->batch_id) {
            report.orphan_batches.push_back(batch_id);
            continue;
        }
        if (batch_id != expected_batch_id) {
            AddError(&report,
                     "batch id gap before " + std::to_string(batch_id));
            break;
        }
        if (batch.first_seq != expected_first_seq) {
            AddError(&report,
                     "sequence gap before batch " + std::to_string(batch_id));
            break;
        }
        report.entry_count += batch.entries.size();
        expected_batch_id = batch_id + 1;
        expected_first_seq = batch.last_seq + 1;
    }

    if (!report.orphan_batches.empty()) {
        AddError(&report, "batch records exist beyond durable_prefix");
    }
    if (report.durable_prefix.has_value() &&
        (expected_batch_id != report.durable_prefix->batch_id + 1 ||
         expected_first_seq != report.durable_prefix->last_seq + 1)) {
        AddError(&report, "durable_prefix does not match batch history");
    }

    report.ok = report.errors.empty();
    return report;
}

std::string OpLogAuditReportToJson(const OpLogAuditReport& report) {
    Json::Value root;
    root["schema_version"] = 1;
    root["ok"] = report.ok;
    root["cluster_id"] = report.cluster_id;
    root["legacy_max_seq"] = Json::UInt64(report.legacy_max_seq);
    if (report.durable_prefix.has_value()) {
        root["durable_prefix"]["batch_id"] =
            Json::UInt64(report.durable_prefix->batch_id);
        root["durable_prefix"]["last_seq"] =
            Json::UInt64(report.durable_prefix->last_seq);
    } else {
        root["durable_prefix"] = Json::nullValue;
    }
    root["batch_count"] = Json::UInt64(report.batch_count);
    root["entry_count"] = Json::UInt64(report.entry_count);
    root["orphan_batches"] = Json::arrayValue;
    for (uint64_t batch_id : report.orphan_batches) {
        root["orphan_batches"].append(Json::UInt64(batch_id));
    }
    root["warnings"] = Json::arrayValue;
    for (const auto& warning : report.warnings) {
        root["warnings"].append(warning);
    }
    root["errors"] = Json::arrayValue;
    for (const auto& error : report.errors) {
        root["errors"].append(error);
    }
    root["truncated_errors"] = report.truncated_errors;

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

}  // namespace mooncake
