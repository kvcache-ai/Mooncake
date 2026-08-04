#include "ha/oplog/oplog_batch_codec.h"

#include <memory>
#include <sstream>

#include <xxhash.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "utils/base64.h"

namespace mooncake {

namespace {

void SetReason(std::string* reason, const std::string& value) {
    if (reason != nullptr) {
        *reason = value;
    }
}

std::string WriteJson(const Json::Value& root) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
    std::ostringstream oss;
    writer->write(root, &oss);
    return oss.str();
}

bool ParseJson(const std::string& value, Json::Value* root,
               std::string* reason) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errors;
    if (!reader->parse(value.data(), value.data() + value.size(), root,
                       &errors)) {
        SetReason(reason, errors.empty() ? "malformed json" : errors);
        return false;
    }
    return true;
}

bool GetUIntField(const Json::Value& root, const char* field, uint32_t* out,
                  std::string* reason) {
    if (!root.isMember(field)) {
        SetReason(reason, std::string("missing field: ") + field);
        return false;
    }
    if (!root[field].isUInt()) {
        SetReason(reason, std::string("field must be uint32: ") + field);
        return false;
    }
    *out = root[field].asUInt();
    return true;
}

bool GetUInt64Field(const Json::Value& root, const char* field, uint64_t* out,
                    std::string* reason) {
    if (!root.isMember(field)) {
        SetReason(reason, std::string("missing field: ") + field);
        return false;
    }
    if (!root[field].isUInt64()) {
        SetReason(reason, std::string("field must be uint64: ") + field);
        return false;
    }
    *out = root[field].asUInt64();
    return true;
}

Json::Value BatchRecordToJson(const OpLogBatchRecord& batch,
                              bool include_checksum) {
    Json::Value root(Json::arrayValue);
    root.append(static_cast<Json::UInt>(batch.schema_version));
    root.append(static_cast<Json::UInt64>(batch.batch_id));
    root.append(static_cast<Json::UInt64>(batch.first_seq));
    root.append(static_cast<Json::UInt64>(batch.last_seq));
    Json::Value entries(Json::arrayValue);
    for (const auto& entry : batch.entries) {
        Json::Value entry_root(Json::arrayValue);
        entry_root.append(static_cast<Json::UInt>(entry.op_type));
        entry_root.append(entry.tenant_id);
        entry_root.append(entry.object_key);
        entry_root.append(base64::Encode(entry.payload));
        entries.append(entry_root);
    }
    root.append(std::move(entries));
    if (include_checksum) {
        root.append(static_cast<Json::UInt>(batch.checksum));
    }
    return root;
}

uint32_t ComputeJsonChecksum(const Json::Value& value) {
    const std::string stable_payload = WriteJson(value);
    return static_cast<uint32_t>(
        XXH32(stable_payload.data(), stable_payload.size(), 0));
}

uint32_t ComputeBatchChecksum(const OpLogBatchRecord& batch) {
    return ComputeJsonChecksum(
        BatchRecordToJson(batch, /*include_checksum=*/false));
}

bool JsonEntryToOpLogEntry(const Json::Value& root, uint64_t sequence_id,
                           OpLogEntry* entry, std::string* reason) {
    if (!root.isArray() || root.size() != 4) {
        SetReason(reason, "oplog entry must be a four-element array");
        return false;
    }
    if (!root[0].isUInt() || !root[1].isString() || !root[2].isString() ||
        !root[3].isString()) {
        SetReason(reason, "oplog entry has invalid field types");
        return false;
    }
    const uint32_t op_type = root[0].asUInt();
    if (op_type == 0 || op_type >= static_cast<uint32_t>(OpType::OP_TYPE_MAX)) {
        SetReason(reason, "oplog entry op_type is outside the enum range");
        return false;
    }

    const std::string encoded_payload = root[3].asString();
    std::string payload = base64::Decode(encoded_payload);
    if (base64::Encode(payload) != encoded_payload) {
        SetReason(reason, "oplog entry payload is not canonical base64");
        return false;
    }

    entry->sequence_id = sequence_id;
    entry->timestamp_ms = 0;
    entry->op_type = static_cast<OpType>(op_type);
    entry->tenant_id = root[1].asString();
    entry->object_key = root[2].asString();
    entry->payload = std::move(payload);
    entry->checksum = ComputeOpLogChecksum(entry->payload);
    entry->prefix_hash = 0;
    if (!ValidateOpLogBatchEntry(*entry, reason)) {
        return false;
    }
    return true;
}

}  // namespace

std::string EncodeDurablePrefix(const DurablePrefix& prefix) {
    Json::Value root;
    root["schema_version"] =
        static_cast<Json::UInt>(kDurablePrefixSchemaVersion);
    root["batch_id"] = static_cast<Json::UInt64>(prefix.batch_id);
    root["last_seq"] = static_cast<Json::UInt64>(prefix.last_seq);
    return WriteJson(root);
}

bool DecodeDurablePrefix(const std::string& value, DurablePrefix* prefix,
                         std::string* reason) {
    if (reason != nullptr) {
        reason->clear();
    }
    if (prefix == nullptr) {
        SetReason(reason, "prefix output is null");
        return false;
    }

    Json::Value root;
    if (!ParseJson(value, &root, reason)) {
        return false;
    }
    if (!root.isObject()) {
        SetReason(reason, "durable prefix must be a JSON object");
        return false;
    }
    uint32_t schema_version = 0;
    if (!GetUIntField(root, "schema_version", &schema_version, reason)) {
        return false;
    }
    if (schema_version != kDurablePrefixSchemaVersion) {
        SetReason(reason, "unsupported durable prefix schema_version");
        return false;
    }
    if (!GetUInt64Field(root, "batch_id", &prefix->batch_id, reason)) {
        return false;
    }
    if (!GetUInt64Field(root, "last_seq", &prefix->last_seq, reason)) {
        return false;
    }
    return true;
}

std::string EncodeOpLogBatchRecord(const OpLogBatchRecord& batch) {
    OpLogBatchRecord encoded = batch;
    encoded.schema_version = kOpLogBatchRecordSchemaVersion;
    encoded.checksum = ComputeBatchChecksum(encoded);
    return WriteJson(BatchRecordToJson(encoded, /*include_checksum=*/true));
}

bool DecodeOpLogBatchRecord(const std::string& value, OpLogBatchRecord* batch,
                            std::string* reason) {
    if (reason != nullptr) {
        reason->clear();
    }
    if (batch == nullptr) {
        SetReason(reason, "batch output is null");
        return false;
    }

    Json::Value root;
    if (!ParseJson(value, &root, reason)) {
        return false;
    }
    if (!root.isArray() || root.size() != 6) {
        SetReason(reason, "batch record must be a six-element array");
        return false;
    }
    if (!root[0].isUInt() || !root[1].isUInt64() || !root[2].isUInt64() ||
        !root[3].isUInt64() || !root[4].isArray() || !root[5].isUInt()) {
        SetReason(reason, "batch record has invalid field types");
        return false;
    }
    const uint32_t schema_version = root[0].asUInt();
    if (schema_version != kOpLogBatchRecordSchemaVersion) {
        SetReason(reason, "unsupported batch record schema_version");
        return false;
    }

    OpLogBatchRecord decoded;
    decoded.schema_version = schema_version;
    decoded.batch_id = root[1].asUInt64();
    decoded.first_seq = root[2].asUInt64();
    decoded.last_seq = root[3].asUInt64();
    decoded.checksum = root[5].asUInt();
    const auto& encoded_entries = root[4];
    if (encoded_entries.empty() || decoded.first_seq == 0 ||
        decoded.last_seq < decoded.first_seq ||
        decoded.last_seq - decoded.first_seq != encoded_entries.size() - 1) {
        SetReason(reason, "batch sequence range does not match entry count");
        return false;
    }

    Json::Value checksum_payload(Json::arrayValue);
    for (Json::ArrayIndex i = 0; i < 5; ++i) {
        checksum_payload.append(root[i]);
    }
    const uint32_t expected = ComputeJsonChecksum(checksum_payload);
    if (decoded.checksum != expected) {
        SetReason(reason, "batch record checksum mismatch");
        return false;
    }

    decoded.entries.reserve(encoded_entries.size());
    for (Json::ArrayIndex i = 0; i < encoded_entries.size(); ++i) {
        OpLogEntry entry;
        if (!JsonEntryToOpLogEntry(encoded_entries[i], decoded.first_seq + i,
                                   &entry, reason)) {
            return false;
        }
        decoded.entries.push_back(std::move(entry));
    }

    if (!ValidateOpLogBatchRecordShape(decoded, reason)) {
        return false;
    }
    *batch = std::move(decoded);
    return true;
}

}  // namespace mooncake
